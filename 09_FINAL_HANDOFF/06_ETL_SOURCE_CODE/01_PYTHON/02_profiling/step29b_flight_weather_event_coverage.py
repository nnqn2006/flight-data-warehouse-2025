from __future__ import annotations

import csv
import math
import re
import sys
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

"""STEP 29B - Flight-weather event coverage audit.

Reads BTS CLEAN and Open-Meteo CLEAN CSVs only. Re-applies the approved STEP 29A
local-wall-clock resolution logic in memory, then checks whether the event's
UTC hour bucket exists in a validated, complete Open-Meteo airport-year file.

Outputs aggregate coverage and exception samples only. It does NOT create the
final flight-weather bridge or modify RAW, STAGING, or CLEAN.
No pandas, PyArrow, Parquet, network, or third-party packages are required.
"""

ROOT = Path(__file__).resolve().parents[2]
BTS_DIR = ROOT / "04_CLEAN_DATA" / "BTS"
WEATHER_DIR = ROOT / "04_CLEAN_DATA" / "OpenMeteo"
OUT_DIR = ROOT / "06_DW_DESIGN" / "01_LOGICAL_MODEL"
EXPECTED_FLIGHTS = 7_001_619
EXPECTED_BTS_MONTHS = {f"{m:02d}" for m in range(1, 13)}
EXPECTED_WEATHER_AIRPORTS = 240
EXPECTED_HOURS_PER_AIRPORT = 8_760
UTC = timezone.utc
MISSING = {"", "NA", "N/A", "NULL", "NONE", "NAN"}
CLOCK_TOLERANCE_MINUTES = 1
WEATHER_START = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
WEATHER_END = datetime(2025, 12, 31, 23, 0, tzinfo=UTC)
SAMPLE_LIMIT_PER_GROUP = 12

REQUIRED_BTS_COLUMNS = [
    "SourceFile", "SourceMonth", "SourceRowNumber", "FlightDate_Clean",
    "CRSDepTime_Clean", "DepTime_Clean", "CRSArrTime_Clean", "ArrTime_Clean",
    "OriginTimeZoneIANA", "DestinationTimeZoneIANA", "Origin", "Dest",
    "Reporting_Airline", "Flight_Number_Reporting_Airline", "Cancelled",
    "Diverted", "DepDelay", "ArrDelay", "CRSElapsedTime", "ActualElapsedTime",
]

ROUTINE_EVENT_STATUSES = {
    "SCHEDULED_DEPARTURE": {
        "SCHEDULED_DEP_UNIQUE_LOCAL_TIME",
        "SCHEDULED_DEP_DST_FOLD_RESOLVED_BY_CRS_ARRIVAL",
    },
    "SCHEDULED_ARRIVAL": {"SCHEDULED_ARRIVAL_MATCH_WITHIN_1_MINUTE"},
    "ACTUAL_DEPARTURE": {
        "ACTUAL_DEP_LOCAL_DELAY_CLOCK_MATCH_UNIQUE_UTC",
        "ACTUAL_DEP_DST_FOLD_RESOLVED_BY_ARRIVAL_EVIDENCE",
    },
    "ACTUAL_ARRIVAL": {"ACTUAL_ARRIVAL_BOTH_METHODS_MATCH"},
}


def txt(value: object) -> str:
    return "" if value is None else str(value).strip()


def missing(value: object) -> bool:
    return txt(value).upper() in MISSING


def num(value: object) -> float | None:
    raw = txt(value)
    if raw.upper() in MISSING:
        return None
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return None
    return val if math.isfinite(val) else None


def flag(value: object) -> bool | None:
    val = num(value)
    if val is None:
        return None
    if abs(val - 1.0) < 1e-9:
        return True
    if abs(val) < 1e-9:
        return False
    return None


def parse_day(value: object) -> date | None:
    raw = txt(value)
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
    """Return (minutes after midnight, day offset); HHMM 2400 = next day 00:00."""
    raw = txt(value)
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


def local_dt(day: date | None, clock: object) -> datetime | None:
    parsed = parse_clock(clock)
    if day is None or parsed is None:
        return None
    mins, day_offset = parsed
    return datetime.combine(day + timedelta(days=day_offset), time(mins // 60, mins % 60))


def clock_residual(actual: datetime, reported_clock: object) -> int | None:
    parsed = parse_clock(reported_clock)
    if parsed is None:
        return None
    actual_min = actual.hour * 60 + actual.minute
    return int((actual_min - parsed[0] + 720) % 1440 - 720)


def clock_match(actual: datetime, reported_clock: object) -> tuple[str, int | None]:
    residual = clock_residual(actual, reported_clock)
    if residual is None:
        return "REPORTED_CLOCK_MISSING_OR_INVALID", None
    if abs(residual) <= CLOCK_TOLERANCE_MINUTES:
        return "CLOCK_MATCH_WITHIN_1_MINUTE", residual
    return "CLOCK_MISMATCH", residual


def get_zone(name: object, cache: dict[str, ZoneInfo]) -> ZoneInfo | None:
    key = txt(name)
    if not key or key.upper() in MISSING:
        return None
    if key not in cache:
        try:
            cache[key] = ZoneInfo(key)
        except (ZoneInfoNotFoundError, ValueError):
            return None
    return cache[key]


def utc_candidates(local_naive: datetime, zone: ZoneInfo) -> list[datetime]:
    """Zero candidates = DST gap; one = unique; two = DST fold."""
    found: dict[datetime, datetime] = {}
    for fold in (0, 1):
        candidate = local_naive.replace(tzinfo=zone, fold=fold).astimezone(UTC)
        if candidate.astimezone(zone).replace(tzinfo=None) == local_naive:
            found[candidate] = candidate
    return sorted(found.values())


def scheduled_departure(
    sched_local: datetime | None, origin_zone: ZoneInfo | None,
    dest_zone: ZoneInfo | None, crs_arr_clock: object,
    crs_elapsed: float | None,
) -> tuple[datetime | None, str]:
    if sched_local is None:
        return None, "SCHEDULED_DEP_LOCAL_DATETIME_MISSING_OR_INVALID"
    if origin_zone is None:
        return None, "ORIGIN_TIMEZONE_MISSING_OR_UNKNOWN"
    candidates = utc_candidates(sched_local, origin_zone)
    if not candidates:
        return None, "SCHEDULED_DEP_NONEXISTENT_LOCAL_TIME"
    if len(candidates) == 1:
        return candidates[0], "SCHEDULED_DEP_UNIQUE_LOCAL_TIME"
    if dest_zone is not None and crs_elapsed is not None and crs_elapsed >= 0:
        matches = []
        for candidate in candidates:
            arr_local = (candidate + timedelta(minutes=crs_elapsed)).astimezone(dest_zone).replace(tzinfo=None)
            status, _ = clock_match(arr_local, crs_arr_clock)
            if status == "CLOCK_MATCH_WITHIN_1_MINUTE":
                matches.append(candidate)
        if len(matches) == 1:
            return matches[0], "SCHEDULED_DEP_DST_FOLD_RESOLVED_BY_CRS_ARRIVAL"
    return None, "SCHEDULED_DEP_AMBIGUOUS_LOCAL_TIME_UNRESOLVED"


def actual_departure(
    sched_local: datetime | None, dep_delay: float | None, dep_clock: object,
    origin_zone: ZoneInfo | None, actual_elapsed: float | None,
    arr_clock: object, dest_zone: ZoneInfo | None,
    cancelled: bool | None, diverted: bool | None,
) -> tuple[datetime | None, str]:
    if sched_local is None:
        return None, "ACTUAL_DEP_NO_SCHEDULED_LOCAL_BASELINE"
    if dep_delay is None:
        return None, "ACTUAL_DEP_DEPDELAY_MISSING"
    if origin_zone is None:
        return None, "ORIGIN_TIMEZONE_MISSING_OR_UNKNOWN"
    local_candidate = sched_local + timedelta(minutes=dep_delay)
    clock_status, _ = clock_match(local_candidate, dep_clock)
    if clock_status == "REPORTED_CLOCK_MISSING_OR_INVALID":
        return None, "ACTUAL_DEP_REPORTED_DEPTIME_MISSING_OR_INVALID"
    if clock_status == "CLOCK_MISMATCH":
        return None, "ACTUAL_DEP_LOCAL_DELAY_CLOCK_MISMATCH"
    candidates = utc_candidates(local_candidate, origin_zone)
    if len(candidates) == 0:
        return None, "ACTUAL_DEP_NONEXISTENT_LOCAL_TIME"
    if len(candidates) == 1:
        return candidates[0], "ACTUAL_DEP_LOCAL_DELAY_CLOCK_MATCH_UNIQUE_UTC"
    if (actual_elapsed is not None and actual_elapsed >= 0 and dest_zone is not None
            and cancelled is False and diverted is False and not missing(arr_clock)):
        matches = []
        for candidate in candidates:
            arrival_local = (candidate + timedelta(minutes=actual_elapsed)).astimezone(dest_zone)
            status, _ = clock_match(arrival_local, arr_clock)
            if status == "CLOCK_MATCH_WITHIN_1_MINUTE":
                matches.append(candidate)
        if len(matches) == 1:
            return matches[0], "ACTUAL_DEP_DST_FOLD_RESOLVED_BY_ARRIVAL_EVIDENCE"
    return None, "ACTUAL_DEP_AMBIGUOUS_LOCAL_TIME_UNRESOLVED"


def scheduled_arrival(
    sched_dep_utc: datetime | None, crs_elapsed: float | None,
    dest_zone: ZoneInfo | None, crs_arr_clock: object,
) -> tuple[datetime | None, str]:
    if sched_dep_utc is None:
        return None, "SCHEDULED_ARR_NO_RESOLVED_DEP"
    if crs_elapsed is None:
        return None, "CRSElapsedTime_MISSING_OR_INVALID"
    if crs_elapsed < 0:
        return None, "CRSElapsedTime_INVALID_NEGATIVE"
    if dest_zone is None:
        return None, "DESTINATION_TIMEZONE_MISSING_OR_UNKNOWN"
    arr_utc = sched_dep_utc + timedelta(minutes=crs_elapsed)
    arr_local = arr_utc.astimezone(dest_zone).replace(tzinfo=None)
    clock_status, _ = clock_match(arr_local, crs_arr_clock)
    if clock_status == "CLOCK_MATCH_WITHIN_1_MINUTE":
        return arr_utc, "SCHEDULED_ARRIVAL_MATCH_WITHIN_1_MINUTE"
    if clock_status == "CLOCK_MISMATCH":
        return None, "SCHEDULED_ARRIVAL_CLOCK_MISMATCH"
    return None, "SCHEDULED_ARRIVAL_REPORTED_CLOCK_MISSING_OR_INVALID"


def actual_arrival(
    *, cancelled: bool | None, diverted: bool | None,
    scheduled_arr_utc: datetime | None, scheduled_arr_status: str,
    arr_delay: float | None, arr_clock: object,
    actual_dep_utc: datetime | None, actual_elapsed: float | None,
    dest_zone: ZoneInfo | None,
) -> tuple[datetime | None, str]:
    if cancelled is True:
        return None, "CANCELLED_SEPARATE_HANDLING"
    if diverted is True:
        return None, "DIVERTED_SCHEDULED_DESTINATION_NOT_CONFIRMED"
    if missing(arr_clock):
        return None, "ARRTIME_MISSING"
    if dest_zone is None:
        return None, "DESTINATION_TIMEZONE_MISSING_OR_UNKNOWN"

    arrdelay_utc: datetime | None = None
    arrdelay_status = "NOT_EVALUATED"
    arrdelay_clock_match = False
    arrdelay_candidates: list[datetime] = []

    if scheduled_arr_utc is not None and scheduled_arr_status == "SCHEDULED_ARRIVAL_MATCH_WITHIN_1_MINUTE":
        sched_arr_local = scheduled_arr_utc.astimezone(dest_zone).replace(tzinfo=None)
        if arr_delay is not None:
            arr_local_candidate = sched_arr_local + timedelta(minutes=arr_delay)
            arrdelay_candidates = utc_candidates(arr_local_candidate, dest_zone)
            if not arrdelay_candidates:
                arrdelay_status = "ARRDELAY_LOCAL_TIME_NONEXISTENT_DST_GAP"
            elif len(arrdelay_candidates) == 1:
                arrdelay_utc = arrdelay_candidates[0]
                status, _ = clock_match(arr_local_candidate, arr_clock)
                arrdelay_status = (
                    "ARRDELAY_LOCAL_CLOCK_MATCH_UNIQUE_UTC"
                    if status == "CLOCK_MATCH_WITHIN_1_MINUTE" else "ARRDELAY_LOCAL_CLOCK_MISMATCH"
                )
            else:
                arrdelay_status = "ARRDELAY_AMBIGUOUS_DST_FOLD_PENDING_ELAPSED_EVIDENCE"
        else:
            arrdelay_status = "ARRDELAY_MISSING"
    elif scheduled_arr_utc is None:
        arrdelay_status = "SCHEDULED_ARRIVAL_BASELINE_UNAVAILABLE"
    else:
        arrdelay_status = "SCHEDULED_ARRIVAL_BASELINE_UNVERIFIED"

    elapsed_utc: datetime | None = None
    elapsed_status = "NOT_EVALUATED"
    elapsed_clock_match = False
    if actual_dep_utc is not None and actual_elapsed is not None and actual_elapsed >= 0:
        elapsed_utc = actual_dep_utc + timedelta(minutes=actual_elapsed)
        elapsed_local = elapsed_utc.astimezone(dest_zone).replace(tzinfo=None)
        status, _ = clock_match(elapsed_local, arr_clock)
        elapsed_status = "ELAPSED_UTC_CLOCK_MATCH" if status == "CLOCK_MATCH_WITHIN_1_MINUTE" else (
            "ELAPSED_UTC_CLOCK_MISMATCH" if status == "CLOCK_MISMATCH" else "ELAPSED_ARRTIME_MISSING_OR_INVALID"
        )
        elapsed_clock_match = elapsed_status == "ELAPSED_UTC_CLOCK_MATCH"
    elif actual_elapsed is None or actual_elapsed < 0:
        elapsed_status = "ACTUALELAPSEDTIME_MISSING_OR_INVALID"
    else:
        elapsed_status = "ACTUAL_DEPARTURE_UTC_UNAVAILABLE_OR_UNRESOLVED"

    if len(arrdelay_candidates) == 2:
        if elapsed_utc is not None:
            matches = [c for c in arrdelay_candidates if abs((c - elapsed_utc).total_seconds()) <= 60]
            if len(matches) == 1:
                arrdelay_utc = matches[0]
                arrdelay_status = "ARRDELAY_DST_FOLD_RESOLVED_BY_ACTUAL_ELAPSED_TIME"
            else:
                arrdelay_status = "ARRDELAY_AMBIGUOUS_DST_FOLD_UNRESOLVED"
        else:
            arrdelay_status = "ARRDELAY_AMBIGUOUS_DST_FOLD_UNRESOLVED"

    arrdelay_clock_match = arrdelay_status in {
        "ARRDELAY_LOCAL_CLOCK_MATCH_UNIQUE_UTC",
        "ARRDELAY_DST_FOLD_RESOLVED_BY_ACTUAL_ELAPSED_TIME",
    }
    if arrdelay_utc is not None and elapsed_utc is not None:
        delta_min = (arrdelay_utc - elapsed_utc).total_seconds() / 60.0
        if arrdelay_clock_match and elapsed_clock_match and abs(delta_min) <= 1:
            return arrdelay_utc, "ACTUAL_ARRIVAL_BOTH_METHODS_MATCH"
        if arrdelay_clock_match and elapsed_clock_match:
            return None, "ACTUAL_ARRIVAL_METHODS_CONFLICT"
        if arrdelay_clock_match:
            return None, "ACTUAL_ARRIVAL_ARRDELAY_MATCH_ELAPSED_CLOCK_CONFLICT"
        if elapsed_clock_match:
            return None, "ACTUAL_ARRIVAL_ELAPSED_MATCH_ARRDELAY_CONFLICT"
        return None, "ACTUAL_ARRIVAL_BOTH_METHODS_NOT_CONFIRMED"
    if arrdelay_utc is not None:
        if arrdelay_clock_match:
            return None, "ACTUAL_ARRIVAL_ARRDELAY_ONLY_NEEDS_ELAPSED_CROSSCHECK"
        return None, "ACTUAL_ARRIVAL_ARRDELAY_CANDIDATE_NOT_CONFIRMED"
    if elapsed_utc is not None:
        if elapsed_clock_match:
            if arrdelay_status == "ARRDELAY_LOCAL_TIME_NONEXISTENT_DST_GAP":
                return None, "ACTUAL_ARRIVAL_ELAPSED_MATCH_ARRDELAY_DST_GAP"
            return None, "ACTUAL_ARRIVAL_ELAPSED_ONLY_NEEDS_ARRDELAY_CROSSCHECK"
        return None, "ACTUAL_ARRIVAL_ELAPSED_CANDIDATE_CLOCK_MISMATCH"
    return None, "ACTUAL_ARRIVAL_NO_RESOLVED_UTC_CANDIDATE"


def parse_utc(value: object) -> datetime | None:
    raw = txt(value)
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
        return None
    return dt.astimezone(UTC)


def detect_weather_columns(headers: list[str], first_row: dict[str, str], path: Path) -> tuple[str, str | None]:
    timestamp_candidates = [
        "TimestampUTC", "WeatherTimestampUTC", "TimeUTC", "time_utc",
        "timestamp_utc", "datetime_utc", "time", "timestamp", "datetime", "DateTimeUTC",
    ]
    timestamp_col = None
    for name in timestamp_candidates:
        if name in headers and parse_utc(first_row.get(name)) is not None:
            timestamp_col = name
            break
    if timestamp_col is None:
        for name in headers:
            if parse_utc(first_row.get(name)) is not None:
                timestamp_col = name
                break
    if timestamp_col is None:
        raise ValueError(f"Cannot identify a UTC timestamp column in {path.name}. Headers={headers}")

    airport_candidates = ["AirportCode", "IATACode", "airport_code", "airport", "IATA"]
    airport_col = next((x for x in airport_candidates if x in headers), None)
    return timestamp_col, airport_col


def validate_weather_files(weather_dir: Path) -> tuple[dict[str, tuple[datetime, datetime]], list[str], str, str | None]:
    paths = sorted(weather_dir.glob("weather_*_2025.csv"))
    issues: list[str] = []
    ranges: dict[str, tuple[datetime, datetime]] = {}
    timestamp_col: str | None = None
    airport_col: str | None = None

    if len(paths) != EXPECTED_WEATHER_AIRPORTS:
        issues.append(f"Expected {EXPECTED_WEATHER_AIRPORTS} weather CSVs; found {len(paths)}")

    for path in paths:
        match = re.fullmatch(r"weather_([A-Z0-9]{3})_2025\.csv", path.name, re.IGNORECASE)
        if not match:
            issues.append(f"Unexpected weather filename: {path.name}")
            continue
        code = match.group(1).upper()
        file_count = 0
        first_ts: datetime | None = None
        last_ts: datetime | None = None
        file_issues = 0
        expected = WEATHER_START

        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            first_row = next(reader, None)
            if first_row is None:
                issues.append(f"{path.name}: empty CSV")
                continue
            if timestamp_col is None:
                timestamp_col, airport_col = detect_weather_columns(headers, first_row, path)
            elif timestamp_col not in headers:
                issues.append(f"{path.name}: timestamp column {timestamp_col!r} missing")
                continue
            rows = [first_row]
            rows_iter = iter(rows)
            # Process first row and remaining rows without loading the file.
            for row in list(rows_iter) + list(reader):
                file_count += 1
                ts = parse_utc(row.get(timestamp_col))
                if ts is None:
                    if file_issues < 3:
                        issues.append(f"{path.name}: non-UTC/invalid timestamp at data row {file_count}")
                    file_issues += 1
                    continue
                if first_ts is None:
                    first_ts = ts
                last_ts = ts
                if ts != expected:
                    if file_issues < 3:
                        issues.append(
                            f"{path.name}: hour sequence mismatch at data row {file_count}; "
                            f"expected {expected.isoformat()}, got {ts.isoformat()}"
                        )
                    file_issues += 1
                expected += timedelta(hours=1)
                if airport_col:
                    row_code = txt(row.get(airport_col)).upper()
                    if row_code != code:
                        if file_issues < 3:
                            issues.append(f"{path.name}: AirportCode {row_code!r} != filename code {code}")
                        file_issues += 1
        if file_count != EXPECTED_HOURS_PER_AIRPORT:
            issues.append(f"{path.name}: expected {EXPECTED_HOURS_PER_AIRPORT} rows; found {file_count}")
            file_issues += 1
        if first_ts != WEATHER_START or last_ts != WEATHER_END:
            issues.append(f"{path.name}: expected UTC range {WEATHER_START.isoformat()}..{WEATHER_END.isoformat()}, got {first_ts}..{last_ts}")
            file_issues += 1
        if file_issues == 0 and first_ts is not None and last_ts is not None:
            ranges[code] = (first_ts, last_ts)
        else:
            issues.append(f"{path.name}: INVALID weather time series; excluded from join coverage")

    return ranges, issues, timestamp_col or "", airport_col


def main() -> int:
    if not BTS_DIR.exists():
        raise FileNotFoundError(f"BTS CLEAN directory not found: {BTS_DIR}")
    if not WEATHER_DIR.exists():
        raise FileNotFoundError(f"Open-Meteo CLEAN directory not found: {WEATHER_DIR}")

    bts_files = sorted(BTS_DIR.glob("BTS_CLEAN_2025_*.csv"))
    if len(bts_files) != 12:
        raise RuntimeError(f"Expected 12 BTS CLEAN monthly files; found {len(bts_files)}")

    weather_ranges, weather_issues, weather_ts_col, weather_airport_col = validate_weather_files(WEATHER_DIR)
    if not weather_ts_col:
        raise RuntimeError("Could not detect Open-Meteo UTC timestamp column")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_path = OUT_DIR / f"29B_flight_weather_event_coverage_summary_{run_id}.txt"
    monthly_path = OUT_DIR / f"29B_flight_weather_event_coverage_by_month_{run_id}.csv"
    samples_path = OUT_DIR / f"29B_flight_weather_event_exception_samples_{run_id}.csv"

    zone_cache: dict[str, ZoneInfo] = {}
    row_total = 0
    rows_by_month: Counter[str] = Counter()
    aggregate: Counter[tuple[str, str, str, str]] = Counter()
    event_aggregate: Counter[tuple[str, str]] = Counter()
    samples: list[dict[str, object]] = []
    sample_counts: Counter[tuple[str, str, str]] = Counter()
    invalid_timezone_counts: Counter[str] = Counter()
    file_rows: dict[str, int] = {}

    for path in bts_files:
        m = re.search(r"2025_(\d{2})\.csv$", path.name, re.IGNORECASE)
        if not m:
            raise RuntimeError(f"Cannot identify source month from {path.name}")
        filename_month = m.group(1)
        print(f"Reading {path.name} ...", flush=True)
        count_this_file = 0
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            missing_cols = [c for c in REQUIRED_BTS_COLUMNS if c not in headers]
            if missing_cols:
                raise RuntimeError(f"Required BTS CLEAN columns missing in {path.name}: {missing_cols}")

            for row in reader:
                count_this_file += 1
                row_total += 1
                month = txt(row.get("SourceMonth")) or filename_month
                month = f"{int(month):02d}" if month.isdigit() else filename_month
                rows_by_month[month] += 1

                flight_day = parse_day(row.get("FlightDate_Clean"))
                sched_dep_local = local_dt(flight_day, row.get("CRSDepTime_Clean"))
                origin = txt(row.get("Origin")).upper()
                dest = txt(row.get("Dest")).upper()
                origin_tz_name = txt(row.get("OriginTimeZoneIANA"))
                dest_tz_name = txt(row.get("DestinationTimeZoneIANA"))
                origin_zone = get_zone(origin_tz_name, zone_cache)
                dest_zone = get_zone(dest_tz_name, zone_cache)
                if origin_tz_name and origin_zone is None:
                    invalid_timezone_counts[origin_tz_name] += 1
                if dest_tz_name and dest_zone is None:
                    invalid_timezone_counts[dest_tz_name] += 1

                crs_elapsed = num(row.get("CRSElapsedTime"))
                actual_elapsed = num(row.get("ActualElapsedTime"))
                dep_delay = num(row.get("DepDelay"))
                arr_delay = num(row.get("ArrDelay"))
                cancelled = flag(row.get("Cancelled"))
                diverted = flag(row.get("Diverted"))

                sched_dep_utc, sched_dep_status = scheduled_departure(
                    sched_dep_local, origin_zone, dest_zone,
                    row.get("CRSArrTime_Clean"), crs_elapsed,
                )
                sched_arr_utc, sched_arr_status = scheduled_arrival(
                    sched_dep_utc, crs_elapsed, dest_zone, row.get("CRSArrTime_Clean"),
                )
                actual_dep_utc, actual_dep_status = actual_departure(
                    sched_dep_local, dep_delay, row.get("DepTime_Clean"),
                    origin_zone, actual_elapsed, row.get("ArrTime_Clean"),
                    dest_zone, cancelled, diverted,
                )
                actual_arr_utc, actual_arr_status = actual_arrival(
                    cancelled=cancelled, diverted=diverted,
                    scheduled_arr_utc=sched_arr_utc,
                    scheduled_arr_status=sched_arr_status,
                    arr_delay=arr_delay, arr_clock=row.get("ArrTime_Clean"),
                    actual_dep_utc=actual_dep_utc, actual_elapsed=actual_elapsed,
                    dest_zone=dest_zone,
                )

                events = [
                    ("SCHEDULED_DEPARTURE", origin, sched_dep_utc, sched_dep_status),
                    ("SCHEDULED_ARRIVAL", dest, sched_arr_utc, sched_arr_status),
                    ("ACTUAL_DEPARTURE", origin, actual_dep_utc, actual_dep_status),
                    ("ACTUAL_ARRIVAL", dest, actual_arr_utc, actual_arr_status),
                ]

                for event_type, airport_code, event_utc, timestamp_status in events:
                    approved = timestamp_status in ROUTINE_EVENT_STATUSES[event_type] and event_utc is not None
                    weather_hour = None
                    offset_min = None
                    if event_type == "ACTUAL_ARRIVAL" and timestamp_status == "DIVERTED_SCHEDULED_DESTINATION_NOT_CONFIRMED":
                        coverage_status = "DIVERTED_SEPARATE_HANDLING"
                    elif event_type == "ACTUAL_ARRIVAL" and timestamp_status == "CANCELLED_SEPARATE_HANDLING":
                        coverage_status = "CANCELLED_SEPARATE_HANDLING"
                    elif not approved:
                        coverage_status = "TIMESTAMP_NOT_APPROVED_OR_UNRESOLVED"
                    elif not airport_code or airport_code.upper() in MISSING:
                        coverage_status = "AIRPORT_CODE_MISSING"
                    elif airport_code not in weather_ranges:
                        coverage_status = "NO_VALIDATED_WEATHER_FILE_FOR_AIRPORT"
                    else:
                        weather_hour = event_utc.replace(minute=0, second=0, microsecond=0)
                        offset_min = int((event_utc - weather_hour).total_seconds() // 60)
                        start, end = weather_ranges[airport_code]
                        if weather_hour < start or weather_hour > end:
                            coverage_status = "EVENT_OUTSIDE_WEATHER_UTC_WINDOW"
                        else:
                            coverage_status = "MATCHED_WEATHER_HOUR"

                    aggregate[(month, event_type, timestamp_status, coverage_status)] += 1
                    event_aggregate[(event_type, coverage_status)] += 1

                    if coverage_status != "MATCHED_WEATHER_HOUR":
                        sample_key = (event_type, timestamp_status, coverage_status)
                        if sample_counts[sample_key] < SAMPLE_LIMIT_PER_GROUP:
                            sample_counts[sample_key] += 1
                            samples.append({
                                "EventType": event_type,
                                "SourceMonth": month,
                                "SourceFile": txt(row.get("SourceFile")) or path.name,
                                "SourceRowNumber": txt(row.get("SourceRowNumber")),
                                "FlightDate_Clean": txt(row.get("FlightDate_Clean")),
                                "Reporting_Airline": txt(row.get("Reporting_Airline")),
                                "Flight_Number_Reporting_Airline": txt(row.get("Flight_Number_Reporting_Airline")),
                                "Origin": origin,
                                "Destination": dest,
                                "ExposureAirportCode": airport_code,
                                "TimestampStatus": timestamp_status,
                                "EventTimestampUTC": event_utc.isoformat().replace("+00:00", "Z") if event_utc else "",
                                "WeatherHourUTC": weather_hour.isoformat().replace("+00:00", "Z") if weather_hour else "",
                                "MinutesAfterWeatherHour": offset_min if offset_min is not None else "",
                                "WeatherCoverageStatus": coverage_status,
                                "Cancelled": txt(row.get("Cancelled")),
                                "Diverted": txt(row.get("Diverted")),
                            })

        file_rows[path.name] = count_this_file
        print(f"  rows: {count_this_file:,}", flush=True)

    # Ensure every month and every event has an auditable total.
    monthly_rows = [
        {
            "SourceMonth": month,
            "EventType": event,
            "TimestampStatus": ts_status,
            "WeatherCoverageStatus": weather_status,
            "RecordCount": count,
        }
        for (month, event, ts_status, weather_status), count in sorted(aggregate.items())
    ]
    sample_fields = [
        "EventType", "SourceMonth", "SourceFile", "SourceRowNumber", "FlightDate_Clean",
        "Reporting_Airline", "Flight_Number_Reporting_Airline", "Origin", "Destination",
        "ExposureAirportCode", "TimestampStatus", "EventTimestampUTC", "WeatherHourUTC",
        "MinutesAfterWeatherHour", "WeatherCoverageStatus", "Cancelled", "Diverted",
    ]
    with monthly_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(monthly_rows[0].keys()) if monthly_rows else [
            "SourceMonth", "EventType", "TimestampStatus", "WeatherCoverageStatus", "RecordCount"
        ])
        writer.writeheader()
        writer.writerows(monthly_rows)
    with samples_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=sample_fields)
        writer.writeheader()
        writer.writerows(samples)

    all_months = set(rows_by_month)
    event_names = ("SCHEDULED_DEPARTURE", "SCHEDULED_ARRIVAL", "ACTUAL_DEPARTURE", "ACTUAL_ARRIVAL")
    event_totals = {event: sum(v for (e, _), v in event_aggregate.items() if e == event) for event in event_names}
    lines = [
        "STEP 29B - FLIGHT / WEATHER EVENT COVERAGE AUDIT",
        "=" * 96,
        f"Run timestamp: {run_id}",
        f"BTS CLEAN directory: {BTS_DIR}",
        f"Open-Meteo CLEAN directory: {WEATHER_DIR}",
        f"BTS monthly files: {len(bts_files)}",
        f"BTS rows processed: {row_total:,}",
        f"Expected BTS rows: {EXPECTED_FLIGHTS:,}",
        f"BTS row count check: {'PASS' if row_total == EXPECTED_FLIGHTS else 'REVIEW'}",
        f"Months present: {sorted(all_months)}",
        f"Missing months: {sorted(EXPECTED_BTS_MONTHS - all_months)}",
        "",
        "OPEN-METEO CLEAN VALIDATION",
        "-" * 96,
        f"Weather files discovered: {len(list(WEATHER_DIR.glob('weather_*_2025.csv')))}",
        f"Weather files passing full-year UTC sequence validation: {len(weather_ranges)}",
        f"Expected hours per airport: {EXPECTED_HOURS_PER_AIRPORT:,}",
        f"Detected UTC timestamp column: {weather_ts_col}",
        f"Detected AirportCode column: {weather_airport_col or '[filename used]'}",
        f"Expected UTC window: {WEATHER_START.isoformat()} .. {WEATHER_END.isoformat()}",
        f"Weather validation issues: {len(weather_issues)}",
    ]
    for issue in weather_issues[:100]:
        lines.append(f"WEATHER ISSUE: {issue}")
    if len(weather_issues) > 100:
        lines.append(f"... {len(weather_issues) - 100} additional weather issues omitted from summary")

    lines += ["", "EVENT COVERAGE TOTALS", "-" * 96]
    for event in event_names:
        lines.append(f"[{event}] total event rows: {event_totals[event]:,}")
        statuses = sorted((status, count) for (e, status), count in event_aggregate.items() if e == event)
        matched = event_aggregate[(event, "MATCHED_WEATHER_HOUR")]
        lines.append(f"  MATCHED_WEATHER_HOUR: {matched:,}")
        for status, count in statuses:
            if status != "MATCHED_WEATHER_HOUR":
                lines.append(f"  {status}: {count:,}")
        eligible = event_totals[event] - event_aggregate[(event, "TIMESTAMP_NOT_APPROVED_OR_UNRESOLVED")]
        if eligible > 0:
            lines.append(f"  Match rate over all event rows: {matched / event_totals[event] * 100:.4f}%")

    lines += ["", "TIMEZONE LOOKUP", "-" * 96,
              f"Unrecognized timezone names encountered in BTS CLEAN: {len(invalid_timezone_counts)}"]
    for name, count in sorted(invalid_timezone_counts.items()):
        lines.append(f"{name}: {count}")

    lines += ["", "METHOD AND INTERPRETATION", "-" * 96,
        "1. Timestamp resolution repeats STEP 29A local-wall-clock rules; no timestamp is written to CLEAN.",
        "2. Scheduled departure uses FlightDate_Clean + CRSDepTime_Clean at Origin.",
        "3. Scheduled arrival uses ScheduledDepUTC + CRSElapsedTime and passes CRSArrTime local-clock check.",
        "4. Actual departure uses scheduled origin-local wall time + DepDelay, then IANA localization.",
        "5. Actual arrival is approved only when ArrDelay and ActualElapsedTime UTC methods agree within one minute and both clock checks pass.",
        "6. Cancelled and diverted actual-arrival destination exposures are kept as separate statuses, not joined to scheduled destination weather.",
        "7. Weather files are validated for exactly 8,760 consecutive UTC hours from 2025-01-01 00:00Z to 2025-12-31 23:00Z.",
        "8. EventTimestampUTC is assigned to the containing UTC hour using floor-to-hour (not nearest-hour) matching.",
        "9. MinutesAfterWeatherHour records how far into the weather hour the flight event occurred.",
        "10. An event after the 2025 weather window or at an airport without a validated weather file is not counted as matched.",
        "11. This step creates aggregate coverage and exception samples only; it does not create the final bridge/fact table.",
        "12. Exception samples are capped per EventType x TimestampStatus x WeatherCoverageStatus; they are not a complete exception export.",
        "13. RAW, STAGING, and CLEAN are read-only. Python standard library only; no pandas/PyArrow/Parquet.",
        "",
        "OUTPUT FILES",
        str(summary_path),
        str(monthly_path),
        str(samples_path),
    ]
    summary_path.write_text("\n".join(lines), encoding="utf-8")

    print("STEP 29B FINISHED")
    print(f"BTS rows processed: {row_total:,}")
    print(f"Weather files validated: {len(weather_ranges):,}")
    for event in event_names:
        print(f"{event}: matched {event_aggregate[(event, 'MATCHED_WEATHER_HOUR')]:,} / {event_totals[event]:,}")
    print(f"Summary: {summary_path}")
    print(f"Monthly coverage: {monthly_path}")
    print(f"Exception samples: {samples_path}")
    print("RAW / STAGING / CLEAN were read-only.")
    if row_total != EXPECTED_FLIGHTS or len(weather_ranges) != EXPECTED_WEATHER_AIRPORTS or weather_issues:
        print("STEP 29B RESULT: REVIEW - investigate inventory/weather validation issues before approval")
        return 2
    print("STEP 29B RESULT: PASS - coverage audit completed; this is not final Gold loading")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"STEP 29B FAILED: {type(exc).__name__}: {exc}")
        raise
