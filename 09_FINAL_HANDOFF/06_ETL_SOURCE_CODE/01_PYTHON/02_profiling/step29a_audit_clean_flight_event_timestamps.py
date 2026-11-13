from __future__ import annotations

import csv
import re
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# STEP 27D2A
# Audit UTC timestamp candidates from approved BTS CLEAN CSV files.
# This script is intentionally READ-ONLY against RAW, STAGING, and CLEAN.
# It does NOT write timestamps back into CLEAN and does NOT automatically
# approve a UTC timestamp when source evidence conflicts.

ROOT = Path(__file__).resolve().parents[2]
CLEAN_DIR = ROOT / "04_CLEAN_DATA" / "BTS"
OUT_DIR = ROOT / "06_DW_DESIGN" / "01_LOGICAL_MODEL"
EXPECTED_ROWS = 7_001_619
EXPECTED_MONTHS = {f"{m:02d}" for m in range(1, 13)}
SAMPLE_LIMIT_PER_STATUS = 25
UTC = timezone.utc
MISSING = {"", "NA", "N/A", "NULL", "NONE", "NAN"}

REQUIRED_COLUMNS = [
    "SourceFile", "SourceMonth", "SourceRowNumber",
    "FlightDate_Clean", "CRSDepTime_Clean", "DepTime_Clean",
    "CRSArrTime_Clean", "ArrTime_Clean",
    "OriginTimeZoneIANA", "DestinationTimeZoneIANA",
    "Origin", "Dest", "Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "Cancelled", "Diverted", "DepDelay", "ArrDelay",
    "CRSElapsedTime", "ActualElapsedTime",
]


def s(value: object) -> str:
    return "" if value is None else str(value).strip()


def is_missing(value: object) -> bool:
    return s(value).upper() in MISSING


def parse_number(value: object) -> float | None:
    text = s(value)
    if text.upper() in MISSING:
        return None
    try:
        number = float(text)
        if number != number or number in (float("inf"), float("-inf")):
            return None
        return number
    except (TypeError, ValueError):
        return None


def parse_flag(value: object) -> bool | None:
    number = parse_number(value)
    if number is None:
        return None
    if abs(number - 1.0) < 1e-9:
        return True
    if abs(number) < 1e-9:
        return False
    return None


def parse_date(value: object) -> date | None:
    text = s(value)
    if text.upper() in MISSING:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def parse_clock(value: object) -> tuple[int, int] | None:
    """Return (minute_of_day, day_offset); 2400 is represented as next-day 00:00."""
    text = s(value)
    if text.upper() in MISSING:
        return None

    # Accept 08:00, 800, 0800, and CSV numeric renderings such as 800.0.
    text = text.replace(":", "").replace(" ", "")
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".", 1)[0]
    if not re.fullmatch(r"\d{1,4}", text):
        return None

    text = text.zfill(4)
    hour = int(text[:2])
    minute = int(text[2:])

    if hour == 24 and minute == 0:
        return 0, 1
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return hour * 60 + minute, 0


def hhmm_minutes(value: object) -> int | None:
    parsed = parse_clock(value)
    return None if parsed is None else parsed[0]


def clock_residual_minutes(actual_minute: int, reported_minute: int) -> int:
    """Circular clock residual in [-720, 719], because BTS reports HHMM only."""
    return int((actual_minute - reported_minute + 720) % 1440 - 720)


def clock_match(local_dt: datetime, reported_clock: object) -> tuple[str, int | None]:
    reported = hhmm_minutes(reported_clock)
    if reported is None:
        return "REPORTED_CLOCK_MISSING_OR_INVALID", None
    residual = clock_residual_minutes(local_dt.hour * 60 + local_dt.minute, reported)
    if abs(residual) <= 1:
        return "CLOCK_MATCH_WITHIN_1_MINUTE", residual
    return "CLOCK_MISMATCH", residual


def get_zone(name: object, cache: dict[str, ZoneInfo]) -> ZoneInfo | None:
    key = s(name)
    if not key or key.upper() in MISSING:
        return None
    if key not in cache:
        try:
            cache[key] = ZoneInfo(key)
        except ZoneInfoNotFoundError:
            return None
    return cache[key]


def local_utc_candidates(local_naive: datetime, zone: ZoneInfo) -> list[datetime]:
    """Resolve valid UTC candidates by round-tripping both DST folds."""
    candidates: dict[datetime, datetime] = {}
    for fold in (0, 1):
        aware = local_naive.replace(tzinfo=zone, fold=fold)
        utc_dt = aware.astimezone(UTC)
        back = utc_dt.astimezone(zone).replace(tzinfo=None)
        if back == local_naive:
            candidates[utc_dt] = utc_dt
    return sorted(candidates.values())


def iso_utc(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def make_local_datetime(day: date | None, clock_value: object) -> datetime | None:
    parsed = parse_clock(clock_value)
    if day is None or parsed is None:
        return None
    minute, day_offset = parsed
    target_day = day + timedelta(days=day_offset)
    return datetime.combine(target_day, time(minute // 60, minute % 60))


def resolve_scheduled_departure(
    local_dep: datetime | None,
    origin_zone: ZoneInfo | None,
    dest_zone: ZoneInfo | None,
    crs_arr_clock: object,
    crs_elapsed: float | None,
) -> tuple[datetime | None, str]:
    if local_dep is None:
        return None, "SCHEDULED_DEP_LOCAL_DATETIME_MISSING_OR_INVALID"
    if origin_zone is None:
        return None, "ORIGIN_TIMEZONE_MISSING_OR_UNKNOWN"

    candidates = local_utc_candidates(local_dep, origin_zone)
    if len(candidates) == 0:
        return None, "SCHEDULED_DEP_NONEXISTENT_LOCAL_TIME"
    if len(candidates) == 1:
        return candidates[0], "SCHEDULED_DEP_UNIQUE_LOCAL_TIME"

    # For a repeated fall-back clock, scheduled elapsed time + destination
    # scheduled clock may identify one fold. Otherwise preserve ambiguity.
    arr_clock = hhmm_minutes(crs_arr_clock)
    if dest_zone is not None and crs_elapsed is not None and crs_elapsed >= 0 and arr_clock is not None:
        matching = []
        for candidate in candidates:
            dest_local = (candidate + timedelta(minutes=crs_elapsed)).astimezone(dest_zone)
            residual = clock_residual_minutes(dest_local.hour * 60 + dest_local.minute, arr_clock)
            if abs(residual) <= 1:
                matching.append(candidate)
        if len(matching) == 1:
            return matching[0], "SCHEDULED_DEP_DST_FOLD_RESOLVED_BY_CRS_ARRIVAL"

    return None, "SCHEDULED_DEP_AMBIGUOUS_LOCAL_TIME_UNRESOLVED"


def actual_departure_candidate(
    scheduled_dep_utc: datetime | None,
    dep_delay: float | None,
    dep_clock: object,
    origin_zone: ZoneInfo | None,
) -> tuple[datetime | None, str, int | None]:
    if scheduled_dep_utc is None:
        return None, "ACTUAL_DEP_NO_RESOLVED_SCHEDULED_DEP", None
    if dep_delay is None:
        return None, "ACTUAL_DEP_DEPDELAY_MISSING", None
    candidate = scheduled_dep_utc + timedelta(minutes=dep_delay)
    if origin_zone is None:
        return candidate, "ACTUAL_DEP_ORIGIN_TIMEZONE_MISSING_OR_UNKNOWN", None
    local_candidate = candidate.astimezone(origin_zone).replace(tzinfo=None)
    status, residual = clock_match(local_candidate, dep_clock)
    if status == "CLOCK_MATCH_WITHIN_1_MINUTE":
        return candidate, "ACTUAL_DEP_UTC_CANDIDATE_CLOCK_MATCH", residual
    if status == "REPORTED_CLOCK_MISSING_OR_INVALID":
        return candidate, "ACTUAL_DEP_DEPTIME_MISSING_CANDIDATE_ONLY", residual
    return candidate, "ACTUAL_DEP_UTC_CANDIDATE_CLOCK_MISMATCH", residual


def evaluate_arrival(
    *,
    cancelled: bool | None,
    diverted: bool | None,
    scheduled_arr_utc: datetime | None,
    actual_dep_utc: datetime | None,
    actual_dep_status: str,
    arr_delay: float | None,
    actual_elapsed: float | None,
    arr_clock: object,
    dest_zone: ZoneInfo | None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "arr_from_arrdelay": None,
        "arr_from_elapsed": None,
        "arrdelay_clock_residual": None,
        "elapsed_clock_residual": None,
        "candidate_delta_minutes": None,
        "arrdelay_candidate_status": "NOT_EVALUATED",
        "elapsed_candidate_status": "NOT_EVALUATED",
        "overall_status": "NOT_EVALUATED",
    }

    # BTS cancelled/diverted rows remain in Fact_Flight, but scheduled-destination
    # actual arrival is not inferred for these states.
    if cancelled is True:
        result["overall_status"] = "CANCELLED_SEPARATE_HANDLING"
        return result
    if diverted is True:
        result["overall_status"] = "DIVERTED_SCHEDULED_DESTINATION_NOT_CONFIRMED"
        return result
    if arr_clock is None or is_missing(arr_clock):
        result["overall_status"] = "ARRTIME_MISSING"
        return result
    if dest_zone is None:
        result["overall_status"] = "DESTINATION_TIMEZONE_MISSING_OR_UNKNOWN"
        return result

    cand_a = None
    cand_b = None
    residual_a = None
    residual_b = None

    if scheduled_arr_utc is not None and arr_delay is not None:
        cand_a = scheduled_arr_utc + timedelta(minutes=arr_delay)
        local_a = cand_a.astimezone(dest_zone).replace(tzinfo=None)
        stat_a, residual_a = clock_match(local_a, arr_clock)
        result["arrdelay_candidate_status"] = (
            "ARRDELAY_CANDIDATE_CLOCK_MATCH" if stat_a == "CLOCK_MATCH_WITHIN_1_MINUTE"
            else "ARRDELAY_CANDIDATE_CLOCK_MISMATCH" if stat_a == "CLOCK_MISMATCH"
            else "ARRDELAY_CANDIDATE_NO_ARRTIME"
        )
    else:
        result["arrdelay_candidate_status"] = "ARRDELAY_CANDIDATE_UNAVAILABLE"

    if (actual_dep_utc is not None and actual_elapsed is not None
            and actual_dep_status == "ACTUAL_DEP_UTC_CANDIDATE_CLOCK_MATCH"):
        cand_b = actual_dep_utc + timedelta(minutes=actual_elapsed)
        local_b = cand_b.astimezone(dest_zone).replace(tzinfo=None)
        stat_b, residual_b = clock_match(local_b, arr_clock)
        result["elapsed_candidate_status"] = (
            "ELAPSED_CANDIDATE_CLOCK_MATCH" if stat_b == "CLOCK_MATCH_WITHIN_1_MINUTE"
            else "ELAPSED_CANDIDATE_CLOCK_MISMATCH" if stat_b == "CLOCK_MISMATCH"
            else "ELAPSED_CANDIDATE_NO_ARRTIME"
        )
    else:
        result["elapsed_candidate_status"] = "ELAPSED_CANDIDATE_UNAVAILABLE_OR_DEP_UNRESOLVED"

    result["arr_from_arrdelay"] = cand_a
    result["arr_from_elapsed"] = cand_b
    result["arrdelay_clock_residual"] = residual_a
    result["elapsed_clock_residual"] = residual_b

    if cand_a is not None and cand_b is not None:
        delta = (cand_a - cand_b).total_seconds() / 60.0
        result["candidate_delta_minutes"] = round(delta, 3)
    else:
        delta = None

    a_match = result["arrdelay_candidate_status"] == "ARRDELAY_CANDIDATE_CLOCK_MATCH"
    b_match = result["elapsed_candidate_status"] == "ELAPSED_CANDIDATE_CLOCK_MATCH"
    pair_match = delta is not None and abs(delta) <= 1

    if a_match and b_match and pair_match:
        overall = "ARRIVAL_BOTH_UTC_CANDIDATES_AND_ARRTIME_MATCH"
    elif b_match and cand_a is not None:
        overall = "ARRIVAL_ELAPSED_AND_ARRTIME_MATCH_ARRDELAY_CONFLICT"
    elif b_match and cand_a is None:
        overall = "ARRIVAL_ELAPSED_AND_ARRTIME_MATCH_NO_ARRDELAY_CROSSCHECK"
    elif a_match and cand_b is None:
        overall = "ARRIVAL_ARRDELAY_AND_ARRTIME_MATCH_NO_ELAPSED_CROSSCHECK"
    elif a_match and cand_b is not None:
        overall = "ARRIVAL_ARRDELAY_MATCH_ELAPSED_CONFLICT"
    elif cand_a is None and cand_b is None:
        overall = "ARRIVAL_NO_UTC_CANDIDATE"
    elif (result["arrdelay_candidate_status"] == "ARRDELAY_CANDIDATE_CLOCK_MISMATCH"
          or result["elapsed_candidate_status"] == "ELAPSED_CANDIDATE_CLOCK_MISMATCH"):
        overall = "ARRIVAL_CLOCK_MISMATCH_OR_CANDIDATE_CONFLICT"
    else:
        overall = "ARRIVAL_EVIDENCE_INCOMPLETE_OR_UNRESOLVED"

    result["overall_status"] = overall
    return result


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(CLEAN_DIR.glob("BTS_CLEAN_2025_*.csv"))
    if not CLEAN_DIR.exists():
        raise FileNotFoundError(f"CLEAN directory not found: {CLEAN_DIR}")
    if len(files) != 12:
        raise RuntimeError(f"Expected 12 monthly CLEAN files, found {len(files)} in {CLEAN_DIR}")

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_path = OUT_DIR / f"27D2A_timestamp_audit_summary_{run_id}.txt"
    monthly_path = OUT_DIR / f"27D2A_timestamp_status_by_month_{run_id}.csv"
    samples_path = OUT_DIR / f"27D2A_timestamp_exception_samples_{run_id}.csv"

    zone_cache: dict[str, ZoneInfo] = {}
    total_rows = 0
    rows_by_month: Counter[str] = Counter()
    statuses: Counter[tuple[str, str]] = Counter()
    monthly_statuses: Counter[tuple[str, str, str]] = Counter()
    sample_counts: Counter[tuple[str, str]] = Counter()
    samples: list[dict[str, object]] = []
    missing_required_by_file: list[str] = []
    invalid_timezone_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()

    for file_path in files:
        match = re.search(r"2025_(\d{2})\.csv$", file_path.name, re.IGNORECASE)
        if not match:
            raise RuntimeError(f"Cannot infer month from filename: {file_path.name}")
        filename_month = match.group(1)

        print(f"Reading {file_path.name} ...", flush=True)
        file_rows = 0
        with file_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            missing = [c for c in REQUIRED_COLUMNS if c not in fieldnames]
            if missing:
                missing_required_by_file.append(f"{file_path.name}: {missing}")
                raise RuntimeError(f"Required CLEAN columns missing in {file_path.name}: {missing}")

            for row in reader:
                file_rows += 1
                total_rows += 1
                month = s(row.get("SourceMonth")) or filename_month
                if month.isdigit():
                    month = f"{int(month):02d}"
                rows_by_month[month] += 1
                source_counts[s(row.get("SourceFile")) or file_path.name] += 1

                flight_date = parse_date(row.get("FlightDate_Clean"))
                crs_dep_local = make_local_datetime(flight_date, row.get("CRSDepTime_Clean"))
                crs_arr_clock = row.get("CRSArrTime_Clean")
                dep_clock = row.get("DepTime_Clean")
                arr_clock = row.get("ArrTime_Clean")

                origin_zone = get_zone(row.get("OriginTimeZoneIANA"), zone_cache)
                dest_zone = get_zone(row.get("DestinationTimeZoneIANA"), zone_cache)
                if not is_missing(row.get("OriginTimeZoneIANA")) and origin_zone is None:
                    invalid_timezone_counts[s(row.get("OriginTimeZoneIANA"))] += 1
                if not is_missing(row.get("DestinationTimeZoneIANA")) and dest_zone is None:
                    invalid_timezone_counts[s(row.get("DestinationTimeZoneIANA"))] += 1

                crs_elapsed = parse_number(row.get("CRSElapsedTime"))
                actual_elapsed = parse_number(row.get("ActualElapsedTime"))
                dep_delay = parse_number(row.get("DepDelay"))
                arr_delay = parse_number(row.get("ArrDelay"))
                cancelled = parse_flag(row.get("Cancelled"))
                diverted = parse_flag(row.get("Diverted"))

                scheduled_dep_utc, scheduled_dep_status = resolve_scheduled_departure(
                    crs_dep_local, origin_zone, dest_zone, crs_arr_clock, crs_elapsed
                )

                scheduled_arr_utc = None
                if scheduled_dep_utc is None:
                    scheduled_arr_status = "SCHEDULED_ARR_NO_RESOLVED_DEP"
                elif crs_elapsed is None:
                    scheduled_arr_status = "CRSElapsedTime_MISSING_OR_INVALID"
                elif crs_elapsed < 0:
                    scheduled_arr_status = "CRSElapsedTime_INVALID_NEGATIVE"
                else:
                    scheduled_arr_utc = scheduled_dep_utc + timedelta(minutes=crs_elapsed)
                    if dest_zone is None:
                        scheduled_arr_status = "DESTINATION_TIMEZONE_MISSING_OR_UNKNOWN"
                    else:
                        local_sched_arr = scheduled_arr_utc.astimezone(dest_zone).replace(tzinfo=None)
                        clock_status, _ = clock_match(local_sched_arr, crs_arr_clock)
                        scheduled_arr_status = (
                            "SCHEDULED_ARRIVAL_MATCH_WITHIN_1_MINUTE"
                            if clock_status == "CLOCK_MATCH_WITHIN_1_MINUTE"
                            else "SCHEDULED_ARRIVAL_CLOCK_MISMATCH"
                            if clock_status == "CLOCK_MISMATCH"
                            else "SCHEDULED_ARRIVAL_REPORTED_CLOCK_MISSING_OR_INVALID"
                        )

                actual_dep_utc, actual_dep_status, actual_dep_residual = actual_departure_candidate(
                    scheduled_dep_utc, dep_delay, dep_clock, origin_zone
                )

                arrival = evaluate_arrival(
                    cancelled=cancelled,
                    diverted=diverted,
                    scheduled_arr_utc=scheduled_arr_utc,
                    actual_dep_utc=actual_dep_utc,
                    actual_dep_status=actual_dep_status,
                    arr_delay=arr_delay,
                    actual_elapsed=actual_elapsed,
                    arr_clock=arr_clock,
                    dest_zone=dest_zone,
                )

                event_statuses = {
                    "SCHEDULED_DEPARTURE": scheduled_dep_status,
                    "SCHEDULED_ARRIVAL": scheduled_arr_status,
                    "ACTUAL_DEPARTURE": actual_dep_status,
                    "ACTUAL_ARRIVAL": str(arrival["overall_status"]),
                }
                for event_type, status in event_statuses.items():
                    statuses[(event_type, status)] += 1
                    monthly_statuses[(month, event_type, status)] += 1

                for event_type, status in event_statuses.items():
                    key = (event_type, status)
                    if sample_counts[key] >= SAMPLE_LIMIT_PER_STATUS:
                        continue
                    # Keep examples of non-routine statuses; routine statuses
                    # are counted but do not flood the sample output.
                    routine = {
                        "SCHEDULED_DEP_UNIQUE_LOCAL_TIME",
                        "SCHEDULED_DEP_DST_FOLD_RESOLVED_BY_CRS_ARRIVAL",
                        "SCHEDULED_ARRIVAL_MATCH_WITHIN_1_MINUTE",
                        "ACTUAL_DEP_UTC_CANDIDATE_CLOCK_MATCH",
                        "ARRIVAL_BOTH_UTC_CANDIDATES_AND_ARRTIME_MATCH",
                    }
                    if status in routine:
                        continue
                    sample_counts[key] += 1
                    samples.append({
                        "EventType": event_type,
                        "Status": status,
                        "SourceFile": s(row.get("SourceFile")) or file_path.name,
                        "SourceMonth": month,
                        "SourceRowNumber": s(row.get("SourceRowNumber")),
                        "FlightDate_Clean": s(row.get("FlightDate_Clean")),
                        "Reporting_Airline": s(row.get("Reporting_Airline")),
                        "Flight_Number_Reporting_Airline": s(row.get("Flight_Number_Reporting_Airline")),
                        "Origin": s(row.get("Origin")),
                        "Dest": s(row.get("Dest")),
                        "OriginTimeZoneIANA": s(row.get("OriginTimeZoneIANA")),
                        "DestinationTimeZoneIANA": s(row.get("DestinationTimeZoneIANA")),
                        "Cancelled": s(row.get("Cancelled")),
                        "Diverted": s(row.get("Diverted")),
                        "CRSDepTime_Clean": s(row.get("CRSDepTime_Clean")),
                        "DepTime_Clean": s(row.get("DepTime_Clean")),
                        "DepDelay": s(row.get("DepDelay")),
                        "CRSArrTime_Clean": s(row.get("CRSArrTime_Clean")),
                        "ArrTime_Clean": s(row.get("ArrTime_Clean")),
                        "ArrDelay": s(row.get("ArrDelay")),
                        "CRSElapsedTime": s(row.get("CRSElapsedTime")),
                        "ActualElapsedTime": s(row.get("ActualElapsedTime")),
                        "ScheduledDepUTC_Candidate": iso_utc(scheduled_dep_utc),
                        "ScheduledDepStatus": scheduled_dep_status,
                        "ScheduledArrUTC_Candidate": iso_utc(scheduled_arr_utc),
                        "ScheduledArrStatus": scheduled_arr_status,
                        "ActualDepUTC_Candidate": iso_utc(actual_dep_utc),
                        "ActualDepStatus": actual_dep_status,
                        "ActualDepClockResidualMin": actual_dep_residual,
                        "ActualArrUTC_FromArrDelay": iso_utc(arrival["arr_from_arrdelay"]),
                        "ActualArrUTC_FromElapsed": iso_utc(arrival["arr_from_elapsed"]),
                        "ArrDelayCandidateStatus": arrival["arrdelay_candidate_status"],
                        "ElapsedCandidateStatus": arrival["elapsed_candidate_status"],
                        "ArrDelayClockResidualMin": arrival["arrdelay_clock_residual"],
                        "ElapsedClockResidualMin": arrival["elapsed_clock_residual"],
                        "CandidateDeltaMinutes": arrival["candidate_delta_minutes"],
                        "ActualArrivalStatus": arrival["overall_status"],
                        "DQ_FlightDate_Status": s(row.get("DQ_FlightDate_Status")),
                        "DQ_CRSDepTime_Status": s(row.get("DQ_CRSDepTime_Status")),
                        "DQ_DepTime_Status": s(row.get("DQ_DepTime_Status")),
                        "DQ_CRSArrTime_Status": s(row.get("DQ_CRSArrTime_Status")),
                        "DQ_ArrTime_Status": s(row.get("DQ_ArrTime_Status")),
                        "DQ_OriginAirportTimezone_Status": s(row.get("DQ_OriginAirportTimezone_Status")),
                        "DQ_DestinationAirportTimezone_Status": s(row.get("DQ_DestinationAirportTimezone_Status")),
                    })

        print(f"  rows: {file_rows:,}", flush=True)

    # Count month inventory and write concise control outputs.
    monthly_rows = []
    for (month, event_type, status), count in sorted(monthly_statuses.items()):
        monthly_rows.append({
            "SourceMonth": month,
            "EventType": event_type,
            "Status": status,
            "RecordCount": count,
        })

    sample_fields = list(samples[0].keys()) if samples else [
        "EventType", "Status", "SourceFile", "SourceMonth", "SourceRowNumber"
    ]
    write_csv(monthly_path, ["SourceMonth", "EventType", "Status", "RecordCount"], monthly_rows)
    write_csv(samples_path, sample_fields, samples)

    lines = [
        "STEP 27D2A - CLEAN FLIGHT EVENT UTC CANDIDATE AUDIT",
        "=" * 88,
        f"Run timestamp: {run_id}",
        f"CLEAN input directory: {CLEAN_DIR}",
        f"Monthly files found: {len(files)}",
        f"Rows processed: {total_rows:,}",
        f"Expected rows: {EXPECTED_ROWS:,}",
        f"Row count check: {'PASS' if total_rows == EXPECTED_ROWS else 'REVIEW'}",
        f"Months present: {sorted(rows_by_month.keys())}",
        f"Missing months: {sorted(EXPECTED_MONTHS - set(rows_by_month.keys()))}",
        "",
        "ROWS BY MONTH",
        "-" * 88,
    ]
    for month in sorted(rows_by_month):
        lines.append(f"2025-{month}: {rows_by_month[month]:,}")

    lines += ["", "STATUS COUNTS BY EVENT", "-" * 88]
    for event_type in ("SCHEDULED_DEPARTURE", "SCHEDULED_ARRIVAL", "ACTUAL_DEPARTURE", "ACTUAL_ARRIVAL"):
        lines.append(f"[{event_type}]")
        for (ev, status), count in sorted(statuses.items()):
            if ev == event_type:
                lines.append(f"{status}: {count:,}")
        lines.append("")

    lines += [
        "TIMEZONE LOOKUP ISSUES",
        "-" * 88,
        f"Distinct unrecognized timezone names: {len(invalid_timezone_counts)}",
    ]
    for name, count in invalid_timezone_counts.most_common(30):
        lines.append(f"{name}: {count:,}")

    lines += [
        "", "INTERPRETATION AND LIMITATIONS", "-" * 88,
        "1. This is a timestamp candidate audit, not an automatic correction step.",
        "2. ScheduledDepUTC is localized from FlightDate_Clean + CRSDepTime_Clean at origin.",
        "3. ScheduledArrUTC candidate is ScheduledDepUTC + CRSElapsedTime; CRSArrTime is a clock cross-check.",
        "4. ActualDepUTC candidate is ScheduledDepUTC + DepDelay; it is retained only as a candidate and checked against DepTime_Clean.",
        "5. ActualArrUTC candidates are ScheduledArrUTC + ArrDelay and ActualDepUTC candidate + ActualElapsedTime.",
        "6. Actual arrival at the scheduled destination is not inferred for cancelled or diverted records.",
        "7. DST ambiguous and nonexistent local timestamps are not fabricated or shifted.",
        "8. Clock comparisons use HHMM only and allow a tolerance of one minute; they do not validate local calendar date.",
        "9. No timestamp columns are written into CLEAN. No RAW, STAGING, or CLEAN file is modified.",
        "10. The audit uses Python standard library only; no pandas, PyArrow, or Parquet access.",
        "",
        "OUTPUT FILES",
        str(summary_path),
        str(monthly_path),
        str(samples_path),
    ]

    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("\nSTEP 27D2A FINISHED")
    print(f"Rows processed: {total_rows:,} / {EXPECTED_ROWS:,}")
    print(f"Summary: {summary_path}")
    print(f"Monthly status: {monthly_path}")
    print(f"Exception samples: {samples_path}")
    print("RAW / STAGING / CLEAN data were read-only.")

    if total_rows != EXPECTED_ROWS or set(rows_by_month) != EXPECTED_MONTHS:
        print("WARNING: row or month inventory differs from expected; investigate before continuing.")
        return 2
    if invalid_timezone_counts:
        print("WARNING: at least one timezone name could not be resolved; inspect summary.")
        return 3
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"STEP 27D2A FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise
