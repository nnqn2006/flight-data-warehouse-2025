from __future__ import annotations

import argparse
import csv
import hashlib
import io
import shutil
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import step29b_flight_weather_event_coverage as audit

"""STEP 29C - Build a complete Flight-Weather Exposure bridge candidate.

Grain:
    One row per FlightKey x ExposureType x SegmentSequence.
    For the four main flight events, SegmentSequence = 1.
    Every BTS flight receives four event rows, including unresolved / unmatched
    cases. This preserves flight-level coverage status and avoids survivorship
    bias from writing only successful weather matches.

Inputs:
    - 12 monthly BTS CLEAN CSV files
    - 352 Open-Meteo CLEAN airport-year CSV files
    - 352-row resolved BTS IATA -> OurAirports ID crosswalk

Outputs (created only with --execute):
    - Four event-specific CSVs, each containing all 7,001,619 flight events
    - FlightKey month-range manifest
    - Bridge output manifest with row counts, file sizes, SHA-256
    - Event coverage/status reconciliation summary

FlightKey:
    Deterministic sequential surrogate across sorted BTS_CLEAN_2025_MM.csv
    files and source row order. Future Fact_Flight loading MUST reproduce this
    order, or use SourceFile + SourceRowNumber to join to this mapping.

Safety:
    RAW, STAGING, and CLEAN are read-only. No Parquet/PyArrow, pandas, network,
    or third-party packages. Preview is the default; --execute writes outputs.
    Outputs are written to a temporary run folder and promoted only after all
    count/read-back validations pass. Failed runs are retained as REVIEW runs.
"""

ROOT = Path(__file__).resolve().parents[2]
BTS_DIR = ROOT / "04_CLEAN_DATA" / "BTS"
WEATHER_DIR = ROOT / "04_CLEAN_DATA" / "OpenMeteo"
AIRPORT_MAP_PATH = ROOT / "02_DATA_PROFILING" / "02_OurAirports" / "13_bts_airport_mapping_resolved.csv"
OUT_BASE = ROOT / "07_GOLD_DATA" / "01_FLIGHT_WEATHER_EXPOSURE_BRIDGE"

EXPECTED_FLIGHTS = 7_001_619
EXPECTED_WEATHER_FILES = 352
EXPECTED_AIRPORT_CODES = 352
EXPECTED_HOURS_PER_AIRPORT = 8_760
EXPECTED_EVENT_ROWS = EXPECTED_FLIGHTS
EXPECTED_TOTAL_EXPOSURE_ROWS = EXPECTED_FLIGHTS * 4

EXPECTED_BTS_ROWS_BY_MONTH = {
    "01": 539_747, "02": 504_884, "03": 600_872, "04": 583_950,
    "05": 605_648, "06": 611_575, "07": 631_428, "08": 602_378,
    "09": 562_439, "10": 605_844, "11": 570_550, "12": 582_304,
}

# STEP 29B4 authoritative status counts. Each event type must reconcile to
# 7,001,619 BTS flight rows.
EXPECTED_STATUS_COUNTS = {
    "SCHEDULED_DEPARTURE": {
        "MATCHED_WEATHER_HOUR": 6_998_688,
        "EVENT_OUTSIDE_WEATHER_UTC_WINDOW": 2_931,
    },
    "SCHEDULED_ARRIVAL": {
        "MATCHED_WEATHER_HOUR": 6_996_110,
        "EVENT_OUTSIDE_WEATHER_UTC_WINDOW": 5_302,
        "TIMESTAMP_NOT_APPROVED_OR_UNRESOLVED": 207,
    },
    "ACTUAL_DEPARTURE": {
        "MATCHED_WEATHER_HOUR": 6_899_805,
        "EVENT_OUTSIDE_WEATHER_UTC_WINDOW": 3_196,
        "TIMESTAMP_NOT_APPROVED_OR_UNRESOLVED": 98_618,
    },
    "ACTUAL_ARRIVAL": {
        "MATCHED_WEATHER_HOUR": 6_873_776,
        "EVENT_OUTSIDE_WEATHER_UTC_WINDOW": 5_455,
        "TIMESTAMP_NOT_APPROVED_OR_UNRESOLVED": 254,
        "CANCELLED_SEPARATE_HANDLING": 102_876,
        "DIVERTED_SEPARATE_HANDLING": 19_258,
    },
}

EVENT_NAMES = (
    "SCHEDULED_DEPARTURE",
    "SCHEDULED_ARRIVAL",
    "ACTUAL_DEPARTURE",
    "ACTUAL_ARRIVAL",
)

EXPECTED_WEATHER_COLUMNS = [
    "AirportCode", "time", "temperature_2m", "relative_humidity_2m",
    "precipitation", "wind_speed_10m", "wind_direction_10m",
    "wind_gusts_10m", "cloud_cover", "weather_code", "Latitude",
    "Longitude", "TimezoneRequested", "WeatherModelRequested",
]

OUTPUT_FIELDS = [
    "FlightKey",
    "SourceFile",
    "SourceMonth",
    "SourceRowNumber",
    "ExposureType",
    "SegmentSequence",
    "ExposureAirportCode",
    "OurAirportsID",
    "TimezoneIANA",
    "EventTimestampUTC",
    "WeatherHourUTC",
    "MinutesAfterWeatherHour",
    "TimestampStatus",
    "WeatherNaturalKey",
    "WeatherCoverageStatus",
]


def iso_utc(value):
    return value.isoformat().replace("+00:00", "Z") if value is not None else ""


def minute_text(value: float | None) -> str:
    if value is None:
        return ""
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return f"{value:.6f}".rstrip("0").rstrip(".")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_airport_crosswalk(path: Path) -> dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(f"Resolved BTS airport mapping not found: {path}")
    mapping: dict[str, str] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"IATACode", "AirportID"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(f"Airport crosswalk missing columns: {sorted(missing)}")
        for row in reader:
            code = audit.txt(row.get("IATACode")).upper()
            airport_id = audit.txt(row.get("AirportID"))
            if not code or not airport_id:
                raise RuntimeError(f"Blank code / AirportID in resolved crosswalk: {row}")
            if code in mapping and mapping[code] != airport_id:
                raise RuntimeError(
                    f"Conflicting OurAirports IDs for IATA code {code}: "
                    f"{mapping[code]} vs {airport_id}"
                )
            mapping[code] = airport_id
    if len(mapping) != EXPECTED_AIRPORT_CODES:
        raise RuntimeError(
            f"Expected {EXPECTED_AIRPORT_CODES} resolved BTS airport codes; found {len(mapping)}"
        )
    if mapping.get("PBI") != "3766":
        raise RuntimeError(
            "Historical alias control failed: expected PBI -> OurAirports_ID 3766; "
            f"got {mapping.get('PBI')!r}"
        )
    return mapping


def validate_weather_inventory() -> tuple[dict[str, tuple[datetime, datetime]], str, str | None]:
    # The STEP 29B helper was originally calibrated for the first 240 airports.
    # Override only its in-memory inventory constant for this 352-airport pass;
    # the historical STEP 29B script file itself is not modified.
    audit.EXPECTED_WEATHER_AIRPORTS = EXPECTED_WEATHER_FILES
    ranges, issues, timestamp_col, airport_col = audit.validate_weather_files(WEATHER_DIR)
    if issues:
        preview = "\n".join(issues[:25])
        raise RuntimeError(
            f"Open-Meteo CLEAN validation reported {len(issues)} issue(s). First issues:\n{preview}"
        )
    if len(ranges) != EXPECTED_WEATHER_FILES:
        raise RuntimeError(
            f"Expected {EXPECTED_WEATHER_FILES} fully valid weather files; got {len(ranges)}"
        )

    discovered = set(ranges)

    # Validate canonical 14-column schema/order on every CLEAN file.
    for code in sorted(discovered):
        path = WEATHER_DIR / f"weather_{code}_2025.csv"
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
        if header != EXPECTED_WEATHER_COLUMNS:
            raise RuntimeError(
                f"{path.name}: canonical 14-column schema/order mismatch. "
                f"Expected={EXPECTED_WEATHER_COLUMNS}; got={header}"
            )

    return ranges, timestamp_col, airport_col


def resolve_event_rows(row: dict[str, str], airport_map: dict[str, str], zone_cache: dict[str, ZoneInfo]):
    flight_day = audit.parse_day(row.get("FlightDate_Clean"))
    scheduled_dep_local = audit.local_dt(flight_day, row.get("CRSDepTime_Clean"))
    origin = audit.txt(row.get("Origin")).upper()
    destination = audit.txt(row.get("Dest")).upper()
    origin_zone_name = audit.txt(row.get("OriginTimeZoneIANA"))
    destination_zone_name = audit.txt(row.get("DestinationTimeZoneIANA"))
    origin_zone = audit.get_zone(origin_zone_name, zone_cache)
    destination_zone = audit.get_zone(destination_zone_name, zone_cache)

    crs_elapsed = audit.num(row.get("CRSElapsedTime"))
    actual_elapsed = audit.num(row.get("ActualElapsedTime"))
    dep_delay = audit.num(row.get("DepDelay"))
    arr_delay = audit.num(row.get("ArrDelay"))
    cancelled = audit.flag(row.get("Cancelled"))
    diverted = audit.flag(row.get("Diverted"))

    scheduled_dep_utc, scheduled_dep_status = audit.scheduled_departure(
        scheduled_dep_local, origin_zone, destination_zone,
        row.get("CRSArrTime_Clean"), crs_elapsed,
    )
    scheduled_arr_utc, scheduled_arr_status = audit.scheduled_arrival(
        scheduled_dep_utc, crs_elapsed, destination_zone,
        row.get("CRSArrTime_Clean"),
    )
    actual_dep_utc, actual_dep_status = audit.actual_departure(
        scheduled_dep_local, dep_delay, row.get("DepTime_Clean"),
        origin_zone, actual_elapsed, row.get("ArrTime_Clean"),
        destination_zone, cancelled, diverted,
    )
    actual_arr_utc, actual_arr_status = audit.actual_arrival(
        cancelled=cancelled,
        diverted=diverted,
        scheduled_arr_utc=scheduled_arr_utc,
        scheduled_arr_status=scheduled_arr_status,
        arr_delay=arr_delay,
        arr_clock=row.get("ArrTime_Clean"),
        actual_dep_utc=actual_dep_utc,
        actual_elapsed=actual_elapsed,
        dest_zone=destination_zone,
    )

    return (
        ("SCHEDULED_DEPARTURE", origin, origin_zone_name, scheduled_dep_utc, scheduled_dep_status),
        ("SCHEDULED_ARRIVAL", destination, destination_zone_name, scheduled_arr_utc, scheduled_arr_status),
        ("ACTUAL_DEPARTURE", origin, origin_zone_name, actual_dep_utc, actual_dep_status),
        ("ACTUAL_ARRIVAL", destination, destination_zone_name, actual_arr_utc, actual_arr_status),
    )


def classify_coverage(event_type: str, airport_code: str, event_utc, timestamp_status: str,
                      weather_ranges: dict[str, tuple[datetime, datetime]]) -> tuple[str, datetime | None]:
    if event_type == "ACTUAL_ARRIVAL" and timestamp_status == "CANCELLED_SEPARATE_HANDLING":
        return "CANCELLED_SEPARATE_HANDLING", None
    if event_type == "ACTUAL_ARRIVAL" and timestamp_status == "DIVERTED_SCHEDULED_DESTINATION_NOT_CONFIRMED":
        return "DIVERTED_SEPARATE_HANDLING", None

    approved = (
        timestamp_status in audit.ROUTINE_EVENT_STATUSES[event_type]
        and event_utc is not None
    )
    if not approved:
        return "TIMESTAMP_NOT_APPROVED_OR_UNRESOLVED", None
    if not airport_code or airport_code not in weather_ranges:
        return "NO_VALIDATED_WEATHER_FILE_FOR_AIRPORT", None

    weather_hour = event_utc.replace(minute=0, second=0, microsecond=0)
    start, end = weather_ranges[airport_code]
    if weather_hour < start or weather_hour > end:
        return "EVENT_OUTSIDE_WEATHER_UTC_WINDOW", weather_hour
    return "MATCHED_WEATHER_HOUR", weather_hour


def build_row(flight_key: int, source_file: str, source_month: str, source_row_number: str,
              event_tuple, airport_map, weather_ranges):
    event_type, airport_code, timezone_name, event_utc, timestamp_status = event_tuple
    airport_code = audit.txt(airport_code).upper()
    our_airports_id = airport_map.get(airport_code, "")
    if airport_code and not our_airports_id:
        raise RuntimeError(f"Exposure airport code {airport_code} has no OurAirports ID")

    coverage_status, weather_hour = classify_coverage(
        event_type, airport_code, event_utc, timestamp_status, weather_ranges
    )
    # Keep the timestamp/hour even when the hour is outside the collected
    # weather window. Do not create a join key unless the weather row exists.
    if event_utc is not None and weather_hour is None:
        weather_hour = event_utc.replace(minute=0, second=0, microsecond=0)
    minutes_after = None
    if event_utc is not None and weather_hour is not None:
        minutes_after = (event_utc - weather_hour).total_seconds() / 60.0

    natural_key = ""
    if coverage_status == "MATCHED_WEATHER_HOUR":
        natural_key = f"{our_airports_id}|{iso_utc(weather_hour)}"

    return {
        "FlightKey": flight_key,
        "SourceFile": source_file,
        "SourceMonth": source_month,
        "SourceRowNumber": source_row_number,
        "ExposureType": event_type,
        "SegmentSequence": 1,
        "ExposureAirportCode": airport_code,
        "OurAirportsID": our_airports_id,
        "TimezoneIANA": timezone_name,
        "EventTimestampUTC": iso_utc(event_utc),
        "WeatherHourUTC": iso_utc(weather_hour),
        "MinutesAfterWeatherHour": minute_text(minutes_after),
        "TimestampStatus": timestamp_status,
        "WeatherNaturalKey": natural_key,
        "WeatherCoverageStatus": coverage_status,
    }


def estimate_output_size(bts_files, airport_map, weather_ranges) -> tuple[int, float, int]:
    """Estimate four plain-CSV outputs from a small sample across all months."""
    sample_bytes: Counter[str] = Counter()
    sample_rows: Counter[str] = Counter()
    zone_cache: dict[str, ZoneInfo] = {}
    sample_per_month = 500

    for path in bts_files:
        month = path.stem[-2:]
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= sample_per_month:
                    break
                events = resolve_event_rows(row, airport_map, zone_cache)
                source_file = audit.txt(row.get("SourceFile")) or path.name
                source_month = audit.txt(row.get("SourceMonth")) or month
                source_month = f"{int(source_month):02d}" if source_month.isdigit() else month
                source_row_number = audit.txt(row.get("SourceRowNumber"))
                for event_tuple in events:
                    event_type = event_tuple[0]
                    record = build_row(
                        1, source_file, source_month, source_row_number,
                        event_tuple, airport_map, weather_ranges,
                    )
                    buf = io.StringIO()
                    csv.DictWriter(buf, fieldnames=OUTPUT_FIELDS).writerow(record)
                    sample_bytes[event_type] += len(buf.getvalue().encode("utf-8"))
                    sample_rows[event_type] += 1

    projected_bytes = 0
    total_sampled = 0
    for event in EVENT_NAMES:
        if not sample_rows[event]:
            raise RuntimeError(f"Could not estimate output size for {event}")
        projected_bytes += int(
            sample_bytes[event] / sample_rows[event] * EXPECTED_EVENT_ROWS
        )
        total_sampled += sample_rows[event]
    # Include CSV headers and a modest line-ending / variance allowance.
    projected_bytes = int(projected_bytes * 1.05) + 4 * 1024
    required_free_bytes = max(4 * 1024**3, int(projected_bytes * 1.25 + 1 * 1024**3))
    return projected_bytes, required_free_bytes, total_sampled


def process_all(execute: bool, run_id: str):
    if not BTS_DIR.exists():
        raise FileNotFoundError(f"BTS CLEAN directory not found: {BTS_DIR}")
    if not WEATHER_DIR.exists():
        raise FileNotFoundError(f"Open-Meteo CLEAN directory not found: {WEATHER_DIR}")

    bts_files = sorted(BTS_DIR.glob("BTS_CLEAN_2025_*.csv"))
    if len(bts_files) != 12:
        raise RuntimeError(f"Expected 12 BTS CLEAN files; found {len(bts_files)}")

    airport_map = read_airport_crosswalk(AIRPORT_MAP_PATH)
    weather_ranges, weather_ts_col, weather_airport_col = validate_weather_inventory()
    weather_codes = set(weather_ranges)
    if weather_codes != set(airport_map):
        raise RuntimeError(
            "Weather airport scope does not exactly equal resolved BTS airport scope. "
            f"Weather-only={sorted(weather_codes - set(airport_map))}; "
            f"Mapping-only={sorted(set(airport_map) - weather_codes)}"
        )

    # Preflight all BTS schemas and monthly file names before any output creation.
    for path in bts_files:
        month_match = path.stem[-2:]
        if month_match not in EXPECTED_BTS_ROWS_BY_MONTH:
            raise RuntimeError(f"Unexpected BTS CLEAN monthly filename: {path.name}")
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            missing = [c for c in audit.REQUIRED_BTS_COLUMNS if c not in (reader.fieldnames or [])]
            if missing:
                raise RuntimeError(f"Required BTS CLEAN columns missing in {path.name}: {missing}")

    total_rows = 0
    month_rows: Counter[str] = Counter()
    month_first_key: dict[str, int] = {}
    month_last_key: dict[str, int] = {}
    month_source_file: dict[str, str] = {}
    event_total_counts: Counter[str] = Counter()
    status_counts: Counter[tuple[str, str]] = Counter()
    status_counts_by_month: Counter[tuple[str, str, str]] = Counter()
    blank_source_lineage = 0
    zone_cache: dict[str, ZoneInfo] = {}

    run_temp_dir = OUT_BASE / f"_building_{run_id}"
    final_dir = OUT_BASE / f"run_{run_id}"
    review_dir = OUT_BASE / f"_REVIEW_{run_id}"
    output_paths = {e: run_temp_dir / f"29C_bridge_{e.lower()}_{run_id}.csv" for e in EVENT_NAMES}

    if OUT_BASE.exists() and (run_temp_dir.exists() or final_dir.exists() or review_dir.exists()):
        raise FileExistsError(f"Run output directory already exists for {run_id}")

    projected_bytes, required_free_bytes, size_sample_rows = estimate_output_size(
        bts_files, airport_map, weather_ranges
    )
    projected_gib = projected_bytes / 1024**3
    if execute:
        # Check the project volume before creating the run/output directory.
        free_bytes = shutil.disk_usage(ROOT).free
        if free_bytes < required_free_bytes:
            raise RuntimeError(
                f"Insufficient free disk space before --execute. Estimated bridge CSV size="
                f"{projected_gib:.2f} GiB; required free space including 25% headroom + 1 GiB="
                f"{required_free_bytes / 1024**3:.2f} GiB; available={free_bytes / 1024**3:.2f} GiB. "
                "No bridge output files were opened."
            )
        OUT_BASE.mkdir(parents=True, exist_ok=True)
        run_temp_dir.mkdir(parents=True, exist_ok=False)
        for event, path in output_paths.items():
            with path.open("x", encoding="utf-8-sig", newline="") as f:
                csv.DictWriter(f, fieldnames=OUTPUT_FIELDS).writeheader()

    try:
        handles = {}
        writers = {}
        if execute:
            for event, path in output_paths.items():
                handles[event] = path.open("a", encoding="utf-8-sig", newline="")
                writers[event] = csv.DictWriter(handles[event], fieldnames=OUTPUT_FIELDS, extrasaction="raise")

        try:
            for path in bts_files:
                filename_month = path.stem[-2:]
                print(f"Processing {path.name} ...", flush=True)
                rows_this_file = 0
                with path.open("r", encoding="utf-8-sig", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        rows_this_file += 1
                        total_rows += 1
                        flight_key = total_rows
                        source_month_raw = audit.txt(row.get("SourceMonth")) or filename_month
                        source_month = f"{int(source_month_raw):02d}" if source_month_raw.isdigit() else filename_month
                        if source_month != filename_month:
                            raise RuntimeError(
                                f"SourceMonth mismatch in {path.name}: row says {source_month!r}"
                            )
                        source_file = audit.txt(row.get("SourceFile")) or path.name
                        source_row_number = audit.txt(row.get("SourceRowNumber"))
                        if not source_row_number:
                            blank_source_lineage += 1
                        month_rows[source_month] += 1
                        month_first_key.setdefault(source_month, flight_key)
                        month_last_key[source_month] = flight_key
                        month_source_file[source_month] = source_file

                        events = resolve_event_rows(row, airport_map, zone_cache)
                        if len(events) != 4:
                            raise RuntimeError(f"Expected exactly four exposure events for FlightKey={flight_key}")
                        for event_tuple in events:
                            event_type = event_tuple[0]
                            record = build_row(
                                flight_key, source_file, source_month, source_row_number,
                                event_tuple, airport_map, weather_ranges,
                            )
                            event_total_counts[event_type] += 1
                            status = record["WeatherCoverageStatus"]
                            status_counts[(event_type, status)] += 1
                            status_counts_by_month[(source_month, event_type, status)] += 1
                            if execute:
                                writers[event_type].writerow(record)

                if rows_this_file != EXPECTED_BTS_ROWS_BY_MONTH[filename_month]:
                    raise RuntimeError(
                        f"{path.name}: expected {EXPECTED_BTS_ROWS_BY_MONTH[filename_month]:,} rows; "
                        f"found {rows_this_file:,}"
                    )
                print(f"  rows={rows_this_file:,} | cumulative FlightKey={total_rows:,}", flush=True)
        finally:
            if execute:
                for handle in handles.values():
                    handle.close()

        # Reconcile the complete 4-event bridge against the authoritative STEP 29B4.
        failed = []
        if total_rows != EXPECTED_FLIGHTS:
            failed.append(f"BTS rows expected {EXPECTED_FLIGHTS:,}; got {total_rows:,}")
        if set(month_rows) != audit.EXPECTED_BTS_MONTHS:
            failed.append(f"BTS month inventory mismatch: {sorted(month_rows)}")
        if blank_source_lineage:
            failed.append(f"Blank SourceRowNumber count={blank_source_lineage:,}")
        for event in EVENT_NAMES:
            if event_total_counts[event] != EXPECTED_EVENT_ROWS:
                failed.append(
                    f"{event}: event rows expected {EXPECTED_EVENT_ROWS:,}; got {event_total_counts[event]:,}"
                )
            actual_statuses = {k[1]: v for k, v in status_counts.items() if k[0] == event}
            expected_statuses = EXPECTED_STATUS_COUNTS[event]
            for status_name, expected_count in expected_statuses.items():
                actual_count = actual_statuses.get(status_name, 0)
                if actual_count != expected_count:
                    failed.append(
                        f"{event}/{status_name}: expected {expected_count:,}; got {actual_count:,}"
                    )
            for status_name, actual_count in actual_statuses.items():
                if status_name not in expected_statuses and actual_count:
                    failed.append(f"{event}: unexpected status {status_name}={actual_count:,}")

        total_exposure_rows = sum(event_total_counts.values())
        expected_matched_total = sum(
            v.get("MATCHED_WEATHER_HOUR", 0) for v in EXPECTED_STATUS_COUNTS.values()
        )
        expected_nonmatched_total = EXPECTED_TOTAL_EXPOSURE_ROWS - expected_matched_total

        output_manifest_rows = []
        if execute:
            for event, path in output_paths.items():
                readback_count = 0
                bad_schema = False
                with path.open("r", encoding="utf-8-sig", newline="") as f:
                    reader = csv.DictReader(f)
                    if reader.fieldnames != OUTPUT_FIELDS:
                        bad_schema = True
                    for _ in reader:
                        readback_count += 1
                expected_count = EXPECTED_EVENT_ROWS
                valid = (not bad_schema and readback_count == expected_count == event_total_counts[event])
                if not valid:
                    failed.append(
                        f"{event}: schema_ok={not bad_schema}; readback={readback_count:,}; "
                        f"written={event_total_counts[event]:,}; expected={expected_count:,}"
                    )
                output_manifest_rows.append({
                    "ExposureType": event,
                    "Filename": path.name,
                    "RowsWritten": event_total_counts[event],
                    "RowsReadBack": readback_count,
                    "ExpectedRows": expected_count,
                    "MatchedRows": status_counts[(event, "MATCHED_WEATHER_HOUR")],
                    "UnmatchedRows": expected_count - status_counts[(event, "MATCHED_WEATHER_HOUR")],
                    "FileSizeBytes": path.stat().st_size,
                    "SHA256": sha256_file(path),
                    "Validation": "PASS" if valid else "FAIL",
                })

        result = "PASS" if not failed else "REVIEW"
        target_dir = final_dir if result == "PASS" and execute else (review_dir if execute else None)

        if execute:
            manifest_path = run_temp_dir / f"29C_bridge_output_manifest_{run_id}.csv"
            with manifest_path.open("x", encoding="utf-8-sig", newline="") as f:
                fields = [
                    "ExposureType", "Filename", "RowsWritten", "RowsReadBack", "ExpectedRows",
                    "MatchedRows", "UnmatchedRows", "FileSizeBytes", "SHA256", "Validation",
                ]
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader()
                writer.writerows(output_manifest_rows)

        flight_manifest_rows = []
        expected_first = 1
        for month in sorted(month_rows):
            first_key = month_first_key[month]
            last_key = month_last_key[month]
            if first_key != expected_first:
                failed.append(
                    f"Non-contiguous FlightKey range for month {month}: expected {expected_first}, got {first_key}"
                )
            if last_key - first_key + 1 != month_rows[month]:
                failed.append(f"FlightKey range length mismatch for month {month}")
            flight_manifest_rows.append({
                "SourceMonth": month,
                "SourceFile": month_source_file[month],
                "RowsProcessed": month_rows[month],
                "FirstFlightKey": first_key,
                "LastFlightKey": last_key,
            })
            expected_first = last_key + 1
        if total_rows and expected_first != total_rows + 1:
            failed.append("Final FlightKey month ranges do not end at the final BTS row")
        result = "PASS" if not failed else "REVIEW"

        if execute:
            flight_manifest_path = run_temp_dir / f"29C_flightkey_month_manifest_{run_id}.csv"
            with flight_manifest_path.open("x", encoding="utf-8-sig", newline="") as f:
                fields = ["SourceMonth", "SourceFile", "RowsProcessed", "FirstFlightKey", "LastFlightKey"]
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader()
                writer.writerows(flight_manifest_rows)

        # Build detailed status totals and monthly validation table in the summary.
        lines = [
            "STEP 29C - COMPLETE FLIGHT / WEATHER EXPOSURE BRIDGE CANDIDATE",
            "=" * 100,
            f"Run timestamp: {run_id}",
            f"Mode: {'EXECUTE' if execute else 'PREVIEW'}",
            f"BTS CLEAN: {BTS_DIR}",
            f"Open-Meteo CLEAN: {WEATHER_DIR}",
            f"Airport crosswalk: {AIRPORT_MAP_PATH}",
            f"Output base: {OUT_BASE}",
            "",
            "INPUT VALIDATION",
            "-" * 100,
            f"BTS monthly files: {len(bts_files)}",
            f"BTS rows processed: {total_rows:,} / {EXPECTED_FLIGHTS:,}",
            f"Weather airport files validated: {len(weather_ranges):,} / {EXPECTED_WEATHER_FILES}",
            f"Weather timestamp column: {weather_ts_col}",
            f"Weather airport column: {weather_airport_col or '[filename used]'}",
            f"Resolved airport crosswalk rows: {len(airport_map):,}",
            f"Weather codes equal resolved BTS codes: {weather_codes == set(airport_map)}",
            f"Historical alias PBI -> OurAirports_ID 3766: {airport_map.get('PBI') == '3766'}",
            f"Blank SourceRowNumber count: {blank_source_lineage:,}",
            "",
            "BRIDGE GRAIN AND KEY POLICY",
            "-" * 100,
            "Grain: one row per FlightKey x ExposureType x SegmentSequence.",
            "The four main event types are emitted for every BTS flight; SegmentSequence=1.",
            "Matched and unmatched statuses are both retained; WeatherNaturalKey is populated only for matched rows.",
            "WeatherNaturalKey = OurAirportsID + '|' + WeatherHourUTC; surrogate WeatherKey is assigned during physical DW loading.",
            "FlightKey = deterministic sequential key over sorted monthly BTS CLEAN files and each file's row order.",
            "Future Fact_Flight load must reproduce this ordering or join by SourceFile + SourceRowNumber.",
            "SourceFile + SourceRowNumber are retained as lineage columns.",
            "",
            "EVENT / STATUS COUNTS (STEP 29B4 RECONCILIATION)",
            "-" * 100,
        ]
        for event in EVENT_NAMES:
            lines.append(f"{event}: total={event_total_counts[event]:,}; expected={EXPECTED_EVENT_ROWS:,}")
            for status_name in sorted(set(EXPECTED_STATUS_COUNTS[event]) | {
                k[1] for k in status_counts if k[0] == event
            }):
                expected_n = EXPECTED_STATUS_COUNTS[event].get(status_name, 0)
                actual_n = status_counts[(event, status_name)]
                lines.append(f"  {status_name}: {actual_n:,} (STEP 29B4 expected {expected_n:,})")
        lines += [
            "",
            f"Total exposure bridge rows: {total_exposure_rows:,} / {EXPECTED_TOTAL_EXPOSURE_ROWS:,}",
            f"Matched exposure rows: {expected_matched_total:,}",
            f"Unmatched / separately handled exposure rows: {expected_nonmatched_total:,}",
            f"Estimated plain CSV size (sample rows={size_sample_rows:,}): {projected_gib:.2f} GiB",
            f"Required free space before execution (25% headroom + 1 GiB): {required_free_bytes / 1024**3:.2f} GiB",
            "",
            "FLIGHTKEY MONTH RANGES",
            "-" * 100,
        ]
        for item in flight_manifest_rows:
            lines.append(
                f"{item['SourceMonth']}: rows={item['RowsProcessed']:,}; "
                f"FlightKey={item['FirstFlightKey']:,}..{item['LastFlightKey']:,}"
            )
        if execute:
            lines += ["", "OUTPUT READ-BACK VALIDATION", "-" * 100]
            for item in output_manifest_rows:
                lines.append(
                    f"{item['ExposureType']}: readback={item['RowsReadBack']:,}; "
                    f"matched={item['MatchedRows']:,}; unmatched={item['UnmatchedRows']:,}; "
                    f"{item['Validation']}; size={item['FileSizeBytes']:,}; sha256={item['SHA256']}"
                )
        lines += [
            "",
            "SAFETY / INTERPRETATION",
            "-" * 100,
            "1. RAW, STAGING, and CLEAN are read-only.",
            "2. Actual arrival at the scheduled destination is separately classified for cancelled/diverted flights; no false weather join is made.",
            "3. Diversion airport/segment exposures are not fabricated here; they belong in a separate diversion-segment fact/bridge.",
            "4. Event timestamps are assigned to the containing UTC hour by floor-to-hour matching, not nearest-hour matching.",
            "5. Out-of-window rows retain event and hour timestamps but do not receive WeatherNaturalKey.",
            "6. Unresolved events retain their timestamp status and blank event/weather timestamps where no approved UTC instant exists.",
            "7. No weather measures are copied into this bridge; measures remain in Fact_Airport_Weather_Hourly.",
            "8. WeatherModelRequested is request metadata, not independent proof of served-model provenance.",
            "9. No pandas, PyArrow, Parquet, external network, or third-party packages are used.",
            "",
            f"FINAL RESULT: {result}",
        ]
        if failed:
            lines += ["FAILED CHECKS:"] + [f"- {item}" for item in failed]

        if execute:
            summary_path = run_temp_dir / f"29C_bridge_validation_summary_{run_id}.txt"
            summary_path.write_text("\n".join(lines), encoding="utf-8")
            # Promote only after complete validation. REVIEW runs are retained separately.
            if result == "PASS":
                run_temp_dir.rename(final_dir)
                print(f"Outputs promoted to: {final_dir}")
            else:
                run_temp_dir.rename(review_dir)
                print(f"REVIEW outputs retained at: {review_dir}")
        else:
            print("\n".join(lines))
            print("\nPREVIEW ONLY: no bridge files were created.")

        print(f"STEP 29C {'PREVIEW' if not execute else 'EXECUTE'} RESULT: {result}")
        return 0 if result == "PASS" else 2

    except Exception:
        if execute and run_temp_dir.exists():
            # Preserve incomplete files for forensic review; clearly quarantine them.
            if not review_dir.exists():
                run_temp_dir.rename(review_dir)
                print(f"Incomplete run quarantined at: {review_dir}", file=sys.stderr)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description="STEP 29C Flight-Weather Exposure Bridge")
    parser.add_argument(
        "--execute", action="store_true",
        help="Write bridge CSVs. Without this flag, run a full PREVIEW only.",
    )
    args = parser.parse_args()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    return process_all(execute=args.execute, run_id=run_id)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"STEP 29C FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise
