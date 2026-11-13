from __future__ import annotations

import csv
import math
import re
import sys
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

"""STEP 29A - Local wall-clock timestamp resolution audit.

Reads the 12 approved BTS CLEAN CSVs and creates audit outputs only.
It does not modify RAW, STAGING, or CLEAN. No pandas/PyArrow/Parquet.

Key rule: apply BTS DepDelay / ArrDelay to the relevant LOCAL scheduled
wall-clock datetime, then localize using the airport's IANA timezone.
ActualElapsedTime is an independent UTC-duration cross-check, not a repair.
"""

ROOT = Path(__file__).resolve().parents[2]
CLEAN_DIR = ROOT / "04_CLEAN_DATA" / "BTS"
OUT_DIR = ROOT / "06_DW_DESIGN" / "01_LOGICAL_MODEL"
EXPECTED_ROWS = 7_001_619
EXPECTED_MONTHS = {f"{m:02d}" for m in range(1, 13)}
SAMPLE_LIMIT_PER_EVENT_STATUS = 30
UTC = timezone.utc
MISSING = {"", "NA", "N/A", "NULL", "NONE", "NAN"}
CLOCK_TOLERANCE_MINUTES = 1

REQUIRED_COLUMNS = [
    "SourceFile", "SourceMonth", "SourceRowNumber",
    "FlightDate_Clean", "CRSDepTime_Clean", "DepTime_Clean",
    "CRSArrTime_Clean", "ArrTime_Clean",
    "OriginTimeZoneIANA", "DestinationTimeZoneIANA",
    "Origin", "Dest", "Reporting_Airline",
    "Flight_Number_Reporting_Airline", "Cancelled", "Diverted",
    "DepDelay", "ArrDelay", "CRSElapsedTime", "ActualElapsedTime",
]

ROUTINE_STATUSES = {
    "SCHEDULED_DEP_UNIQUE_LOCAL_TIME",
    "SCHEDULED_DEP_DST_FOLD_RESOLVED_BY_CRS_ARRIVAL",
    "SCHEDULED_ARRIVAL_MATCH_WITHIN_1_MINUTE",
    "ACTUAL_DEP_LOCAL_DELAY_CLOCK_MATCH_UNIQUE_UTC",
    "ACTUAL_DEP_DST_FOLD_RESOLVED_BY_ARRIVAL_EVIDENCE",
    "ACTUAL_ARRIVAL_BOTH_METHODS_MATCH",
}


def text(value: object) -> str:
    return "" if value is None else str(value).strip()


def is_missing(value: object) -> bool:
    return text(value).upper() in MISSING


def number(value: object) -> float | None:
    raw = text(value)
    if raw.upper() in MISSING:
        return None
    try:
        result = float(raw)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def flag(value: object) -> bool | None:
    val = number(value)
    if val is None:
        return None
    if abs(val - 1.0) < 1e-9:
        return True
    if abs(val) < 1e-9:
        return False
    return None


def parse_date(value: object) -> date | None:
    raw = text(value)
    if raw.upper() in MISSING:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            pass
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def parse_clock(value: object) -> tuple[int, int] | None:
    """Parse HHMM/HH:MM. 2400 means next-day 00:00."""
    raw = text(value)
    if raw.upper() in MISSING:
        return None
    raw = raw.replace(":", "").replace(" ", "")
    if re.fullmatch(r"\d+\.0+", raw):
        raw = raw.split(".", 1)[0]
    if not re.fullmatch(r"\d{1,4}", raw):
        return None
    raw = raw.zfill(4)
    hour, minute = int(raw[:2]), int(raw[2:])
    if hour == 24 and minute == 0:
        return 0, 1
    if hour > 23 or minute > 59:
        return None
    return hour * 60 + minute, 0


def local_datetime(day: date | None, clock: object) -> datetime | None:
    parsed = parse_clock(clock)
    if day is None or parsed is None:
        return None
    mins, day_offset = parsed
    target_day = day + timedelta(days=day_offset)
    return datetime.combine(target_day, time(mins // 60, mins % 60))


def clock_residual(actual: datetime, reported_clock: object) -> int | None:
    parsed = parse_clock(reported_clock)
    if parsed is None:
        return None
    actual_min = actual.hour * 60 + actual.minute
    reported_min = parsed[0]
    return int((actual_min - reported_min + 720) % 1440 - 720)


def clock_match(actual: datetime, reported_clock: object) -> tuple[str, int | None]:
    residual = clock_residual(actual, reported_clock)
    if residual is None:
        return "REPORTED_CLOCK_MISSING_OR_INVALID", None
    if abs(residual) <= CLOCK_TOLERANCE_MINUTES:
        return "CLOCK_MATCH_WITHIN_1_MINUTE", residual
    return "CLOCK_MISMATCH", residual


def zone_for(name: object, cache: dict[str, ZoneInfo]) -> ZoneInfo | None:
    key = text(name)
    if not key or key.upper() in MISSING:
        return None
    if key not in cache:
        try:
            cache[key] = ZoneInfo(key)
        except (ZoneInfoNotFoundError, ValueError):
            return None
    return cache[key]


def utc_candidates(local_naive: datetime, zone: ZoneInfo) -> list[datetime]:
    """Return zero (gap), one (unique), or two (fold) valid UTC instants."""
    found: dict[datetime, datetime] = {}
    for fold in (0, 1):
        aware = local_naive.replace(tzinfo=zone, fold=fold)
        candidate = aware.astimezone(UTC)
        roundtrip = candidate.astimezone(zone).replace(tzinfo=None)
        if roundtrip == local_naive:
            found[candidate] = candidate
    return sorted(found.values())


def iso_utc(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def iso_local(value: datetime | None) -> str:
    return "" if value is None else value.isoformat(sep="T", timespec="seconds")


def scheduled_departure(
    scheduled_local: datetime | None,
    origin_zone: ZoneInfo | None,
    dest_zone: ZoneInfo | None,
    crs_arr_clock: object,
    crs_elapsed: float | None,
) -> tuple[datetime | None, str, datetime | None]:
    if scheduled_local is None:
        return None, "SCHEDULED_DEP_LOCAL_DATETIME_MISSING_OR_INVALID", None
    if origin_zone is None:
        return None, "ORIGIN_TIMEZONE_MISSING_OR_UNKNOWN", None
    candidates = utc_candidates(scheduled_local, origin_zone)
    if not candidates:
        return None, "SCHEDULED_DEP_NONEXISTENT_LOCAL_TIME", None
    if len(candidates) == 1:
        return candidates[0], "SCHEDULED_DEP_UNIQUE_LOCAL_TIME", None

    # Fall-back fold: scheduled elapsed duration and destination CRSArrTime
    # can resolve the fold only when exactly one candidate matches.
    if dest_zone is not None and crs_elapsed is not None and crs_elapsed >= 0:
        matches: list[tuple[datetime, datetime]] = []
        for candidate in candidates:
            arr_local = (candidate + timedelta(minutes=crs_elapsed)).astimezone(dest_zone)
            status, _ = clock_match(arr_local, crs_arr_clock)
            if status == "CLOCK_MATCH_WITHIN_1_MINUTE":
                matches.append((candidate, arr_local.replace(tzinfo=None)))
        if len(matches) == 1:
            return matches[0][0], "SCHEDULED_DEP_DST_FOLD_RESOLVED_BY_CRS_ARRIVAL", matches[0][1]
    return None, "SCHEDULED_DEP_AMBIGUOUS_LOCAL_TIME_UNRESOLVED", None


def actual_departure(
    scheduled_dep_local: datetime | None,
    dep_delay: float | None,
    dep_clock: object,
    origin_zone: ZoneInfo | None,
    actual_elapsed: float | None,
    arr_clock: object,
    dest_zone: ZoneInfo | None,
    cancelled: bool | None,
    diverted: bool | None,
) -> tuple[datetime | None, str, datetime | None, int | None]:
    """Apply DepDelay to local scheduled wall time, then localize."""
    if scheduled_dep_local is None:
        return None, "ACTUAL_DEP_NO_SCHEDULED_LOCAL_BASELINE", None, None
    if dep_delay is None:
        return None, "ACTUAL_DEP_DEPDELAY_MISSING", None, None
    if origin_zone is None:
        return None, "ORIGIN_TIMEZONE_MISSING_OR_UNKNOWN", None, None

    local_candidate = scheduled_dep_local + timedelta(minutes=dep_delay)
    clock_status, residual = clock_match(local_candidate, dep_clock)
    if clock_status == "REPORTED_CLOCK_MISSING_OR_INVALID":
        return None, "ACTUAL_DEP_REPORTED_DEPTIME_MISSING_OR_INVALID", local_candidate, residual
    if clock_status == "CLOCK_MISMATCH":
        return None, "ACTUAL_DEP_LOCAL_DELAY_CLOCK_MISMATCH", local_candidate, residual

    candidates = utc_candidates(local_candidate, origin_zone)
    if len(candidates) == 0:
        return None, "ACTUAL_DEP_NONEXISTENT_LOCAL_TIME", local_candidate, residual
    if len(candidates) == 1:
        return candidates[0], "ACTUAL_DEP_LOCAL_DELAY_CLOCK_MATCH_UNIQUE_UTC", local_candidate, residual

    # Use ActualElapsedTime + destination ArrTime as independent evidence to
    # identify one repeated-hour fold. Do not use cancelled/diverted arrivals.
    if (actual_elapsed is not None and actual_elapsed >= 0 and dest_zone is not None
            and cancelled is False and diverted is False and not is_missing(arr_clock)):
        matches = []
        for candidate in candidates:
            arrival_local = (candidate + timedelta(minutes=actual_elapsed)).astimezone(dest_zone)
            status, _ = clock_match(arrival_local, arr_clock)
            if status == "CLOCK_MATCH_WITHIN_1_MINUTE":
                matches.append(candidate)
        if len(matches) == 1:
            return matches[0], "ACTUAL_DEP_DST_FOLD_RESOLVED_BY_ARRIVAL_EVIDENCE", local_candidate, residual
    return None, "ACTUAL_DEP_AMBIGUOUS_LOCAL_TIME_UNRESOLVED", local_candidate, residual


def evaluate_actual_arrival(
    *, cancelled: bool | None, diverted: bool | None,
    scheduled_arr_utc: datetime | None, scheduled_arr_status: str,
    arr_delay: float | None, arr_clock: object,
    actual_dep_utc: datetime | None, actual_elapsed: float | None,
    dest_zone: ZoneInfo | None,
) -> dict[str, object]:
    out: dict[str, object] = {
        "scheduled_arr_local": None,
        "arr_local_from_arrdelay": None,
        "arrdelay_utc": None,
        "arrdelay_status": "NOT_EVALUATED",
        "arrdelay_residual": None,
        "elapsed_utc": None,
        "elapsed_status": "NOT_EVALUATED",
        "elapsed_local": None,
        "elapsed_residual": None,
        "delta_minutes": None,
        "consensus_utc": None,
        "status": "NOT_EVALUATED",
    }

    if cancelled is True:
        out["status"] = "CANCELLED_SEPARATE_HANDLING"
        return out
    if diverted is True:
        out["status"] = "DIVERTED_SCHEDULED_DESTINATION_NOT_CONFIRMED"
        return out
    if is_missing(arr_clock):
        out["status"] = "ARRTIME_MISSING"
        return out
    if dest_zone is None:
        out["status"] = "DESTINATION_TIMEZONE_MISSING_OR_UNKNOWN"
        return out

    # Candidate A: construct actual-arrival LOCAL wall time by adding ArrDelay
    # to a scheduled-arrival local baseline. Baseline is usable only when the
    # CRS elapsed-derived instant passes the destination CRSArrTime check.
    arrdelay_candidates: list[datetime] = []
    arr_local_candidate: datetime | None = None
    if scheduled_arr_utc is not None and scheduled_arr_status == "SCHEDULED_ARRIVAL_MATCH_WITHIN_1_MINUTE":
        scheduled_arr_local_aware = scheduled_arr_utc.astimezone(dest_zone)
        scheduled_arr_local = scheduled_arr_local_aware.replace(tzinfo=None)
        out["scheduled_arr_local"] = scheduled_arr_local
        if arr_delay is not None:
            arr_local_candidate = scheduled_arr_local + timedelta(minutes=arr_delay)
            out["arr_local_from_arrdelay"] = arr_local_candidate
            arrdelay_candidates = utc_candidates(arr_local_candidate, dest_zone)
            if not arrdelay_candidates:
                out["arrdelay_status"] = "ARRDELAY_LOCAL_TIME_NONEXISTENT_DST_GAP"
            elif len(arrdelay_candidates) == 1:
                out["arrdelay_utc"] = arrdelay_candidates[0]
                status, residual = clock_match(arr_local_candidate, arr_clock)
                out["arrdelay_residual"] = residual
                out["arrdelay_status"] = (
                    "ARRDELAY_LOCAL_CLOCK_MATCH_UNIQUE_UTC" if status == "CLOCK_MATCH_WITHIN_1_MINUTE"
                    else "ARRDELAY_LOCAL_CLOCK_MISMATCH"
                )
            else:
                out["arrdelay_status"] = "ARRDELAY_AMBIGUOUS_DST_FOLD_PENDING_ELAPSED_EVIDENCE"
        else:
            out["arrdelay_status"] = "ARRDELAY_MISSING"
    elif scheduled_arr_utc is None:
        out["arrdelay_status"] = "SCHEDULED_ARRIVAL_BASELINE_UNAVAILABLE"
    else:
        out["arrdelay_status"] = "SCHEDULED_ARRIVAL_BASELINE_UNVERIFIED"

    # Candidate B: actual departure UTC + ActualElapsedTime. This is an elapsed
    # duration path and is independent of ArrDelay, but only usable when actual
    # departure UTC was resolved from a local clock candidate.
    if actual_dep_utc is not None and actual_elapsed is not None and actual_elapsed >= 0:
        elapsed_utc = actual_dep_utc + timedelta(minutes=actual_elapsed)
        elapsed_local_aware = elapsed_utc.astimezone(dest_zone)
        elapsed_local = elapsed_local_aware.replace(tzinfo=None)
        status, residual = clock_match(elapsed_local, arr_clock)
        out["elapsed_utc"] = elapsed_utc
        out["elapsed_local"] = elapsed_local
        out["elapsed_residual"] = residual
        out["elapsed_status"] = (
            "ELAPSED_UTC_CLOCK_MATCH" if status == "CLOCK_MATCH_WITHIN_1_MINUTE"
            else "ELAPSED_UTC_CLOCK_MISMATCH" if status == "CLOCK_MISMATCH"
            else "ELAPSED_ARRTIME_MISSING_OR_INVALID"
        )
    elif actual_elapsed is None or actual_elapsed < 0:
        out["elapsed_status"] = "ACTUALELAPSEDTIME_MISSING_OR_INVALID"
    else:
        out["elapsed_status"] = "ACTUAL_DEPARTURE_UTC_UNAVAILABLE_OR_UNRESOLVED"

    elapsed_utc = out["elapsed_utc"]
    elapsed_match = out["elapsed_status"] == "ELAPSED_UTC_CLOCK_MATCH"

    # Resolve a repeated arrival wall-clock only if the elapsed UTC instant
    # matches exactly one of its two valid UTC folds.
    if len(arrdelay_candidates) == 2:
        if elapsed_utc is not None:
            matches = [c for c in arrdelay_candidates
                       if abs((c - elapsed_utc).total_seconds()) <= CLOCK_TOLERANCE_MINUTES * 60]
            if len(matches) == 1:
                out["arrdelay_utc"] = matches[0]
                out["arrdelay_status"] = "ARRDELAY_DST_FOLD_RESOLVED_BY_ACTUAL_ELAPSED_TIME"
            else:
                out["arrdelay_status"] = "ARRDELAY_AMBIGUOUS_DST_FOLD_UNRESOLVED"
        else:
            out["arrdelay_status"] = "ARRDELAY_AMBIGUOUS_DST_FOLD_UNRESOLVED"

    a_utc = out["arrdelay_utc"]
    a_clock_match = out["arrdelay_status"] in {
        "ARRDELAY_LOCAL_CLOCK_MATCH_UNIQUE_UTC",
        "ARRDELAY_DST_FOLD_RESOLVED_BY_ACTUAL_ELAPSED_TIME",
    }

    if a_utc is not None and elapsed_utc is not None:
        delta = (a_utc - elapsed_utc).total_seconds() / 60.0
        out["delta_minutes"] = round(delta, 3)
        pair_match = abs(delta) <= CLOCK_TOLERANCE_MINUTES
        if a_clock_match and elapsed_match and pair_match:
            out["consensus_utc"] = a_utc
            out["status"] = "ACTUAL_ARRIVAL_BOTH_METHODS_MATCH"
        elif a_clock_match and elapsed_match and not pair_match:
            out["status"] = "ACTUAL_ARRIVAL_METHODS_CONFLICT"
        elif a_clock_match and not elapsed_match:
            out["status"] = "ACTUAL_ARRIVAL_ARRDELAY_MATCH_ELAPSED_CLOCK_CONFLICT"
        elif not a_clock_match and elapsed_match:
            out["status"] = "ACTUAL_ARRIVAL_ELAPSED_MATCH_ARRDELAY_CONFLICT"
        else:
            out["status"] = "ACTUAL_ARRIVAL_BOTH_METHODS_NOT_CONFIRMED"
    elif a_utc is not None:
        if a_clock_match:
            out["status"] = "ACTUAL_ARRIVAL_ARRDELAY_ONLY_NEEDS_ELAPSED_CROSSCHECK"
        else:
            out["status"] = "ACTUAL_ARRIVAL_ARRDELAY_CANDIDATE_NOT_CONFIRMED"
    elif elapsed_utc is not None:
        if elapsed_match:
            if out["arrdelay_status"] == "ARRDELAY_LOCAL_TIME_NONEXISTENT_DST_GAP":
                out["status"] = "ACTUAL_ARRIVAL_ELAPSED_MATCH_ARRDELAY_DST_GAP"
            else:
                out["status"] = "ACTUAL_ARRIVAL_ELAPSED_ONLY_NEEDS_ARRDELAY_CROSSCHECK"
        else:
            out["status"] = "ACTUAL_ARRIVAL_ELAPSED_CANDIDATE_CLOCK_MISMATCH"
    else:
        out["status"] = "ACTUAL_ARRIVAL_NO_RESOLVED_UTC_CANDIDATE"

    return out


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    if not CLEAN_DIR.exists():
        raise FileNotFoundError(f"CLEAN directory not found: {CLEAN_DIR}")
    files = sorted(CLEAN_DIR.glob("BTS_CLEAN_2025_*.csv"))
    if len(files) != 12:
        raise RuntimeError(f"Expected 12 BTS CLEAN files, found {len(files)}: {CLEAN_DIR}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_path = OUT_DIR / f"29A_local_wallclock_timestamp_summary_{run_id}.txt"
    monthly_path = OUT_DIR / f"29A_timestamp_status_by_month_{run_id}.csv"
    sample_path = OUT_DIR / f"29A_timestamp_exception_samples_{run_id}.csv"

    zone_cache: dict[str, ZoneInfo] = {}
    row_total = 0
    rows_by_month: Counter[str] = Counter()
    status_counts: Counter[tuple[str, str]] = Counter()
    monthly_counts: Counter[tuple[str, str, str]] = Counter()
    samples: list[dict[str, object]] = []
    sample_counts: Counter[tuple[str, str]] = Counter()
    invalid_timezones: Counter[str] = Counter()
    required_schema_errors: list[str] = []
    file_row_counts: dict[str, int] = {}

    for path in files:
        match = re.search(r"2025_(\d{2})\.csv$", path.name, re.IGNORECASE)
        if not match:
            raise RuntimeError(f"Cannot identify month from filename: {path.name}")
        filename_month = match.group(1)
        print(f"Reading {path.name} ...", flush=True)
        file_rows = 0

        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            missing = [col for col in REQUIRED_COLUMNS if col not in headers]
            if missing:
                required_schema_errors.append(f"{path.name}: {missing}")
                raise RuntimeError(f"Required columns missing in {path.name}: {missing}")

            for row in reader:
                file_rows += 1
                row_total += 1
                month = text(row.get("SourceMonth")) or filename_month
                if month.isdigit():
                    month = f"{int(month):02d}"
                rows_by_month[month] += 1

                flight_day = parse_date(row.get("FlightDate_Clean"))
                scheduled_dep_local = local_datetime(flight_day, row.get("CRSDepTime_Clean"))
                origin_name = text(row.get("OriginTimeZoneIANA"))
                dest_name = text(row.get("DestinationTimeZoneIANA"))
                origin_zone = zone_for(origin_name, zone_cache)
                dest_zone = zone_for(dest_name, zone_cache)
                if origin_name and origin_zone is None:
                    invalid_timezones[origin_name] += 1
                if dest_name and dest_zone is None:
                    invalid_timezones[dest_name] += 1

                crs_elapsed = number(row.get("CRSElapsedTime"))
                actual_elapsed = number(row.get("ActualElapsedTime"))
                dep_delay = number(row.get("DepDelay"))
                arr_delay = number(row.get("ArrDelay"))
                cancelled = flag(row.get("Cancelled"))
                diverted = flag(row.get("Diverted"))

                sched_dep_utc, sched_dep_status, _ = scheduled_departure(
                    scheduled_dep_local, origin_zone, dest_zone,
                    row.get("CRSArrTime_Clean"), crs_elapsed,
                )

                sched_arr_utc: datetime | None = None
                if sched_dep_utc is None:
                    sched_arr_status = "SCHEDULED_ARR_NO_RESOLVED_DEP"
                elif crs_elapsed is None:
                    sched_arr_status = "CRSElapsedTime_MISSING_OR_INVALID"
                elif crs_elapsed < 0:
                    sched_arr_status = "CRSElapsedTime_INVALID_NEGATIVE"
                elif dest_zone is None:
                    sched_arr_status = "DESTINATION_TIMEZONE_MISSING_OR_UNKNOWN"
                else:
                    sched_arr_utc = sched_dep_utc + timedelta(minutes=crs_elapsed)
                    sched_arr_local = sched_arr_utc.astimezone(dest_zone).replace(tzinfo=None)
                    clock_status, _ = clock_match(sched_arr_local, row.get("CRSArrTime_Clean"))
                    if clock_status == "CLOCK_MATCH_WITHIN_1_MINUTE":
                        sched_arr_status = "SCHEDULED_ARRIVAL_MATCH_WITHIN_1_MINUTE"
                    elif clock_status == "CLOCK_MISMATCH":
                        sched_arr_status = "SCHEDULED_ARRIVAL_CLOCK_MISMATCH"
                    else:
                        sched_arr_status = "SCHEDULED_ARRIVAL_REPORTED_CLOCK_MISSING_OR_INVALID"

                actual_dep_utc, actual_dep_status, actual_dep_local, dep_residual = actual_departure(
                    scheduled_dep_local, dep_delay, row.get("DepTime_Clean"),
                    origin_zone, actual_elapsed, row.get("ArrTime_Clean"),
                    dest_zone, cancelled, diverted,
                )

                arrival = evaluate_actual_arrival(
                    cancelled=cancelled,
                    diverted=diverted,
                    scheduled_arr_utc=sched_arr_utc,
                    scheduled_arr_status=sched_arr_status,
                    arr_delay=arr_delay,
                    arr_clock=row.get("ArrTime_Clean"),
                    actual_dep_utc=actual_dep_utc,
                    actual_elapsed=actual_elapsed,
                    dest_zone=dest_zone,
                )

                event_statuses = {
                    "SCHEDULED_DEPARTURE": sched_dep_status,
                    "SCHEDULED_ARRIVAL": sched_arr_status,
                    "ACTUAL_DEPARTURE": actual_dep_status,
                    "ACTUAL_ARRIVAL": str(arrival["status"]),
                }
                for event, status in event_statuses.items():
                    status_counts[(event, status)] += 1
                    monthly_counts[(month, event, status)] += 1

                for event, status in event_statuses.items():
                    key = (event, status)
                    if status in ROUTINE_STATUSES or sample_counts[key] >= SAMPLE_LIMIT_PER_EVENT_STATUS:
                        continue
                    sample_counts[key] += 1
                    samples.append({
                        "EventType": event,
                        "Status": status,
                        "SourceFile": text(row.get("SourceFile")) or path.name,
                        "SourceMonth": month,
                        "SourceRowNumber": text(row.get("SourceRowNumber")),
                        "FlightDate_Clean": text(row.get("FlightDate_Clean")),
                        "Reporting_Airline": text(row.get("Reporting_Airline")),
                        "Flight_Number_Reporting_Airline": text(row.get("Flight_Number_Reporting_Airline")),
                        "Origin": text(row.get("Origin")),
                        "Dest": text(row.get("Dest")),
                        "OriginTimeZoneIANA": origin_name,
                        "DestinationTimeZoneIANA": dest_name,
                        "Cancelled": text(row.get("Cancelled")),
                        "Diverted": text(row.get("Diverted")),
                        "CRSDepTime_Clean": text(row.get("CRSDepTime_Clean")),
                        "DepTime_Clean": text(row.get("DepTime_Clean")),
                        "DepDelay": text(row.get("DepDelay")),
                        "CRSArrTime_Clean": text(row.get("CRSArrTime_Clean")),
                        "ArrTime_Clean": text(row.get("ArrTime_Clean")),
                        "ArrDelay": text(row.get("ArrDelay")),
                        "CRSElapsedTime": text(row.get("CRSElapsedTime")),
                        "ActualElapsedTime": text(row.get("ActualElapsedTime")),
                        "ScheduledDepLocal": iso_local(scheduled_dep_local),
                        "ScheduledDepUTC_Candidate": iso_utc(sched_dep_utc),
                        "ScheduledDepStatus": sched_dep_status,
                        "ScheduledArrLocalFromElapsed": iso_local(
                            sched_arr_utc.astimezone(dest_zone).replace(tzinfo=None)
                            if sched_arr_utc is not None and dest_zone is not None else None
                        ),
                        "ScheduledArrUTC_Candidate": iso_utc(sched_arr_utc),
                        "ScheduledArrStatus": sched_arr_status,
                        "ActualDepLocalFromDelay": iso_local(actual_dep_local),
                        "ActualDepUTC_Candidate": iso_utc(actual_dep_utc),
                        "ActualDepStatus": actual_dep_status,
                        "ActualDepClockResidualMin": dep_residual,
                        "ActualArrScheduledLocalBaseline": iso_local(arrival["scheduled_arr_local"]),
                        "ActualArrLocalFromArrDelay": iso_local(arrival["arr_local_from_arrdelay"]),
                        "ActualArrUTC_FromArrDelay": iso_utc(arrival["arrdelay_utc"]),
                        "ArrDelayCandidateStatus": arrival["arrdelay_status"],
                        "ActualArrUTC_FromElapsed": iso_utc(arrival["elapsed_utc"]),
                        "ActualArrLocal_FromElapsed": iso_local(arrival["elapsed_local"]),
                        "ElapsedCandidateStatus": arrival["elapsed_status"],
                        "ArrDelayClockResidualMin": arrival["arrdelay_residual"],
                        "ElapsedClockResidualMin": arrival["elapsed_residual"],
                        "CandidateDeltaMinutes": arrival["delta_minutes"],
                        "ActualArrUTC_Consensus": iso_utc(arrival["consensus_utc"]),
                        "ActualArrivalStatus": arrival["status"],
                        "DQ_FlightDate_Status": text(row.get("DQ_FlightDate_Status")),
                        "DQ_CRSDepTime_Status": text(row.get("DQ_CRSDepTime_Status")),
                        "DQ_DepTime_Status": text(row.get("DQ_DepTime_Status")),
                        "DQ_CRSArrTime_Status": text(row.get("DQ_CRSArrTime_Status")),
                        "DQ_ArrTime_Status": text(row.get("DQ_ArrTime_Status")),
                        "DQ_OriginAirportTimezone_Status": text(row.get("DQ_OriginAirportTimezone_Status")),
                        "DQ_DestinationAirportTimezone_Status": text(row.get("DQ_DestinationAirportTimezone_Status")),
                    })

        file_row_counts[path.name] = file_rows
        print(f"  rows: {file_rows:,}", flush=True)

    monthly_rows = [
        {"SourceMonth": month, "EventType": event, "Status": status, "RecordCount": count}
        for (month, event, status), count in sorted(monthly_counts.items())
    ]
    sample_fields = list(samples[0].keys()) if samples else ["EventType", "Status", "SourceFile"]
    write_csv(monthly_path, ["SourceMonth", "EventType", "Status", "RecordCount"], monthly_rows)
    write_csv(sample_path, sample_fields, samples)

    lines = [
        "STEP 29A - LOCAL WALL-CLOCK FLIGHT EVENT TIMESTAMP RESOLUTION AUDIT",
        "=" * 92,
        f"Run timestamp: {run_id}",
        f"CLEAN input directory: {CLEAN_DIR}",
        f"Monthly files found: {len(files)}",
        f"Rows processed: {row_total:,}",
        f"Expected rows: {EXPECTED_ROWS:,}",
        f"Row count check: {'PASS' if row_total == EXPECTED_ROWS else 'REVIEW'}",
        f"Months present: {sorted(rows_by_month.keys())}",
        f"Missing months: {sorted(EXPECTED_MONTHS - set(rows_by_month.keys()))}",
        "",
        "FILE ROW COUNTS",
        "-" * 92,
    ]
    for filename, count in sorted(file_row_counts.items()):
        lines.append(f"{filename}: {count:,}")

    lines += ["", "STATUS COUNTS BY EVENT", "-" * 92]
    for event in ("SCHEDULED_DEPARTURE", "SCHEDULED_ARRIVAL", "ACTUAL_DEPARTURE", "ACTUAL_ARRIVAL"):
        lines.append(f"[{event}]")
        for (event_name, status), count in sorted(status_counts.items()):
            if event_name == event:
                lines.append(f"{status}: {count:,}")
        lines.append("")

    lines += [
        "TIMEZONE LOOKUP ISSUES",
        "-" * 92,
        f"Distinct unrecognized timezone names: {len(invalid_timezones)}",
    ]
    for name, count in invalid_timezones.most_common(30):
        lines.append(f"{name}: {count:,}")

    lines += [
        "", "METHOD AND LIMITATIONS", "-" * 92,
        "1. Scheduled departure local datetime = FlightDate_Clean + CRSDepTime_Clean at origin.",
        "2. Scheduled departure UTC is localized from IANA timezone; repeated-hour fold is resolved only by unique CRS arrival evidence.",
        "3. Scheduled arrival UTC candidate = ScheduledDepUTC + CRSElapsedTime; CRSArrTime is a destination-local clock cross-check.",
        "4. Actual departure local candidate = scheduled departure LOCAL wall time + DepDelay; it is then localized to UTC.",
        "5. Actual departure fold is resolved only when ActualElapsedTime + destination ArrTime uniquely identifies one fold.",
        "6. Actual arrival local candidate = scheduled arrival LOCAL wall time + ArrDelay, only if scheduled-arrival baseline passes CRSArrTime check.",
        "7. Independent actual-arrival UTC candidate = resolved ActualDepUTC + ActualElapsedTime.",
        "8. Actual arrival consensus UTC is populated only when both methods agree within one minute and both clock checks pass.",
        "9. Cancelled/diverted actual destination arrivals are not inferred; their flight records remain in scope.",
        "10. DST gaps are not shifted; unresolved folds remain unresolved. No fabricated timestamps or imputation.",
        "11. ArrTime/DepTime/CRS clock checks use HHMM only; they do not prove local calendar date by themselves.",
        "12. This is an audit output, not a CLEAN timestamp write-back or blanket approval for weather joins.",
        "13. RAW, STAGING, and CLEAN are read-only. Python standard library only; no pandas/PyArrow/Parquet.",
        "",
        "OUTPUT FILES",
        str(summary_path),
        str(monthly_path),
        str(sample_path),
    ]
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("\nSTEP 29A FINISHED")
    print(f"Rows processed: {row_total:,} / {EXPECTED_ROWS:,}")
    print(f"Summary: {summary_path}")
    print(f"Monthly status: {monthly_path}")
    print(f"Exception samples: {sample_path}")
    print("RAW / STAGING / CLEAN data were read-only.")

    if row_total != EXPECTED_ROWS or set(rows_by_month) != EXPECTED_MONTHS:
        print("WARNING: row or month inventory differs from expected.")
        return 2
    if invalid_timezones:
        print("WARNING: some timezone names could not be resolved.")
        return 3
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"STEP 29A FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise
