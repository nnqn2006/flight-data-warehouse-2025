#!/usr/bin/env python3
"""STEP 30E3 - Load the 29C2 Flight-Weather Exposure Bridge into MySQL.

Default mode is PREVIEW. It validates the complete 48-partition 29C2 output,
its manifests, SHA256 values, FlightKey ranges, and STEP 29B4 status counts.
PREVIEW does not connect to MySQL or modify any source/target data.

EXECUTE mode loads in bounded batches through a temporary staging table and
resolves WeatherKey by joining (OurAirportsID, WeatherHourUTC) to the unique
key in Fact_Airport_Weather_Hourly. Checkpoints are append-only JSONL records;
no Windows file replacement/rename is used for checkpoint updates.

The loader never modifies RAW, STAGING, CLEAN, GOLD bridge CSV files, or
existing Dimension/Fact data. It does not DROP/TRUNCATE/DELETE target rows.
"""
from __future__ import annotations

import argparse
import csv
import getpass
import hashlib
import json
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path.cwd()
DB_NAME = "flight_dw_2025"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 3306
DEFAULT_USER = "root"
BRIDGE_RUN_ID = "20261110_030433_504879"
BRIDGE_BASE = ROOT / "07_GOLD_DATA" / "01_FLIGHT_WEATHER_EXPOSURE_BRIDGE"
BRIDGE_DIR = BRIDGE_BASE / f"run_{BRIDGE_RUN_ID}"
OUTPUT_MANIFEST = BRIDGE_DIR / f"29C2_bridge_output_manifest_{BRIDGE_RUN_ID}.csv"
FLIGHTKEY_MANIFEST = BRIDGE_DIR / f"29C2_flightkey_month_manifest_{BRIDGE_RUN_ID}.csv"
BRIDGE_SUMMARY = BRIDGE_DIR / f"29C2_bridge_validation_summary_{BRIDGE_RUN_ID}.txt"
AIRPORT_MAPPING_FILE = ROOT / "02_DATA_PROFILING" / "02_OurAirports" / "13_bts_airport_mapping_resolved.csv"
PHYSICAL_DIR = ROOT / "06_DW_DESIGN" / "02_PHYSICAL_MODEL"
CHECKPOINT_FILE = PHYSICAL_DIR / "_checkpoints" / "30E3_flight_weather_bridge_checkpoint.json"
EXPECTED_FLIGHTS = 7_001_619
EXPECTED_ROWS = EXPECTED_FLIGHTS * 4
EXPECTED_MATCHED = 27_768_379
EXPECTED_UNMATCHED = 238_097
DEFAULT_BATCH_SIZE = 5_000
EVENTS = (
    "SCHEDULED_DEPARTURE",
    "SCHEDULED_ARRIVAL",
    "ACTUAL_DEPARTURE",
    "ACTUAL_ARRIVAL",
)
MONTHS = ("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
MONTH_ROWS = {
    "01": 539_747, "02": 504_884, "03": 600_872, "04": 583_950,
    "05": 605_648, "06": 611_575, "07": 631_428, "08": 602_378,
    "09": 562_439, "10": 605_844, "11": 570_550, "12": 582_304,
}
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
OUTPUT_FIELDS = [
    "FlightKey", "SourceFile", "SourceMonth", "SourceRowNumber",
    "ExposureType", "SegmentSequence", "ExposureAirportCode",
    "OurAirportsID", "TimezoneIANA", "EventTimestampUTC", "WeatherHourUTC",
    "MinutesAfterWeatherHour", "TimestampStatus", "WeatherNaturalKey",
    "WeatherCoverageStatus",
]
MANIFEST_FIELDS = [
    "SourceMonth", "ExposureType", "Filename", "RowsWritten", "RowsReadBack",
    "ExpectedRows", "FirstFlightKey", "LastFlightKey", "MatchedRows",
    "UnmatchedRows", "FileSizeBytes", "SHA256", "Validation",
]
FLIGHTKEY_FIELDS = ["SourceMonth", "SourceFile", "RowsProcessed", "FirstFlightKey", "LastFlightKey"]


def say(message: str) -> None:
    print(message, flush=True)


def clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def require_file(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"Required file not found: {path}")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_uint(raw: Any, field: str, context: str, minimum: int = 0, maximum: int = 2**64 - 1) -> int:
    text = clean(raw)
    if not text:
        raise ValueError(f"Blank {field} at {context}")
    try:
        value = int(text)
    except ValueError as exc:
        raise ValueError(f"Invalid integer {field}={text!r} at {context}") from exc
    if str(value) != text or not minimum <= value <= maximum:
        raise ValueError(f"Invalid/range {field}={text!r} at {context}")
    return value


def parse_utc_datetime(raw: Any, field: str, context: str, required: bool) -> datetime | None:
    text = clean(raw)
    if not text:
        if required:
            raise ValueError(f"Required {field} is blank at {context}")
        return None
    if not text.endswith("Z"):
        raise ValueError(f"{field} must use explicit UTC trailing Z at {context}: {text!r}")
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"Invalid {field}={text!r} at {context}") from exc
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise ValueError(f"{field} is not UTC at {context}: {text!r}")
    # MySQL DATETIME is stored as a naive UTC clock value by project convention.
    return parsed.replace(tzinfo=None)


def file_signature(path: Path, digest: str | None = None) -> dict[str, Any]:
    stat = path.stat()
    return {
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "sha256": digest if digest is not None else sha256_file(path),
    }


def expected_first_keys(month_manifest: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {item["SourceMonth"]: item for item in month_manifest}


def read_flightkey_manifest() -> list[dict[str, Any]]:
    require_file(FLIGHTKEY_MANIFEST)
    result: list[dict[str, Any]] = []
    with FLIGHTKEY_MANIFEST.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != FLIGHTKEY_FIELDS:
            raise ValueError(f"FlightKey manifest schema mismatch: {reader.fieldnames}")
        for row in reader:
            month = clean(row["SourceMonth"])
            if month not in MONTH_ROWS:
                raise ValueError(f"Unexpected SourceMonth in FlightKey manifest: {month!r}")
            item = {
                "SourceMonth": month,
                "SourceFile": clean(row["SourceFile"]),
                "RowsProcessed": parse_uint(row["RowsProcessed"], "RowsProcessed", FLIGHTKEY_MANIFEST.name, 1),
                "FirstFlightKey": parse_uint(row["FirstFlightKey"], "FirstFlightKey", FLIGHTKEY_MANIFEST.name, 1),
                "LastFlightKey": parse_uint(row["LastFlightKey"], "LastFlightKey", FLIGHTKEY_MANIFEST.name, 1),
            }
            if item["SourceFile"] != f"BTS_CLEAN_2025_{month}.csv":
                raise ValueError(f"FlightKey manifest CLEAN filename mismatch for month {month}: {item['SourceFile']!r}")
            if item["RowsProcessed"] != MONTH_ROWS[month]:
                raise ValueError(f"FlightKey manifest row count mismatch for month {month}")
            if item["LastFlightKey"] - item["FirstFlightKey"] + 1 != item["RowsProcessed"]:
                raise ValueError(f"FlightKey manifest range mismatch for month {month}")
            result.append(item)
    result.sort(key=lambda x: x["SourceMonth"])
    if len(result) != 12 or [x["SourceMonth"] for x in result] != list(MONTHS):
        raise ValueError("FlightKey manifest must contain exactly months 01..12")
    expected_next = 1
    for item in result:
        if item["FirstFlightKey"] != expected_next:
            raise ValueError(f"Non-contiguous FlightKey range at month {item['SourceMonth']}")
        expected_next = item["LastFlightKey"] + 1
    if expected_next != EXPECTED_FLIGHTS + 1:
        raise ValueError("FlightKey manifest does not end at 7,001,619")
    return result


def read_airport_mapping() -> dict[str, int]:
    require_file(AIRPORT_MAPPING_FILE)
    mapping: dict[str, int] = {}
    with AIRPORT_MAPPING_FILE.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"IATACode", "AirportID"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError(f"Resolved airport mapping missing columns: {sorted(required - set(reader.fieldnames or []))}")
        for row_number, row in enumerate(reader, start=2):
            code = clean(row.get("IATACode")).upper()
            airport_id = parse_uint(row.get("AirportID"), "AirportID", f"{AIRPORT_MAPPING_FILE.name}:{row_number}", 1, 2**32 - 1)
            if len(code) != 3 or code in mapping:
                raise ValueError(f"Invalid/duplicate IATA mapping code {code!r} at row {row_number}")
            mapping[code] = airport_id
    if len(mapping) != 352 or mapping.get("PBI") != 3766:
        raise ValueError(f"Resolved airport mapping control failed: codes={len(mapping)}, PBI={mapping.get('PBI')}")
    return mapping


def load_source_manifests() -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    for path in (OUTPUT_MANIFEST, FLIGHTKEY_MANIFEST, BRIDGE_SUMMARY):
        require_file(path)
    summary_text = BRIDGE_SUMMARY.read_text(encoding="utf-8-sig")
    if f"Run ID: {BRIDGE_RUN_ID}" not in summary_text or "FINAL RESULT: PASS" not in summary_text:
        raise ValueError("STEP 29C2 summary does not identify the expected run and FINAL RESULT: PASS")
    if f"Exposure rows: {EXPECTED_ROWS:,} / {EXPECTED_ROWS:,}" not in summary_text:
        raise ValueError("STEP 29C2 summary does not confirm 28,006,476 exposure rows")

    month_manifest = read_flightkey_manifest()
    month_by = expected_first_keys(month_manifest)
    partitions: list[dict[str, Any]] = []
    with OUTPUT_MANIFEST.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != MANIFEST_FIELDS:
            raise ValueError(f"Bridge output manifest schema mismatch: {reader.fieldnames}")
        for row in reader:
            month = clean(row["SourceMonth"])
            event = clean(row["ExposureType"])
            if month not in month_by or event not in EVENTS:
                raise ValueError(f"Unexpected Bridge manifest partition: {month}/{event}")
            month_info = month_by[month]
            expected_rows = MONTH_ROWS[month]
            item = {
                "month": month,
                "event": event,
                "filename": clean(row["Filename"]),
                "path": BRIDGE_DIR / clean(row["Filename"]),
                "rows_written": parse_uint(row["RowsWritten"], "RowsWritten", f"manifest {month}/{event}", 1),
                "rows_readback": parse_uint(row["RowsReadBack"], "RowsReadBack", f"manifest {month}/{event}", 1),
                "expected_rows": parse_uint(row["ExpectedRows"], "ExpectedRows", f"manifest {month}/{event}", 1),
                "first_key": parse_uint(row["FirstFlightKey"], "FirstFlightKey", f"manifest {month}/{event}", 1),
                "last_key": parse_uint(row["LastFlightKey"], "LastFlightKey", f"manifest {month}/{event}", 1),
                "matched_rows": parse_uint(row["MatchedRows"], "MatchedRows", f"manifest {month}/{event}"),
                "unmatched_rows": parse_uint(row["UnmatchedRows"], "UnmatchedRows", f"manifest {month}/{event}"),
                "file_size_bytes": parse_uint(row["FileSizeBytes"], "FileSizeBytes", f"manifest {month}/{event}", 1),
                "sha256": clean(row["SHA256"]).lower(),
                "validation": clean(row["Validation"]),
                # 29C2 FlightKey manifest names the CLEAN input file; the row-level
                # SourceFile lineage remains the original BTS source filename.
                "source_file": f"BTS_2025_{month}.csv",
                "clean_source_file": month_info["SourceFile"],
            }
            if item["validation"] != "PASS":
                raise ValueError(f"29C2 partition is not PASS: {month}/{event}")
            if (item["rows_written"] != expected_rows or item["rows_readback"] != expected_rows
                    or item["expected_rows"] != expected_rows):
                raise ValueError(f"29C2 row-count manifest mismatch: {month}/{event}")
            if item["first_key"] != month_info["FirstFlightKey"] or item["last_key"] != month_info["LastFlightKey"]:
                raise ValueError(f"29C2 FlightKey range mismatch: {month}/{event}")
            if item["matched_rows"] + item["unmatched_rows"] != expected_rows:
                raise ValueError(f"29C2 matched/unmatched counts do not add up: {month}/{event}")
            if len(item["sha256"]) != 64:
                raise ValueError(f"Invalid SHA256 in 29C2 manifest: {month}/{event}")
            if item["path"].name != item["filename"]:
                raise ValueError(f"Invalid filename in 29C2 manifest: {month}/{event}")
            partitions.append(item)
    partitions.sort(key=lambda x: (x["month"], EVENTS.index(x["event"])))
    expected_pairs = {(m, e) for m in MONTHS for e in EVENTS}
    actual_pairs = {(p["month"], p["event"]) for p in partitions}
    if len(partitions) != 48 or actual_pairs != expected_pairs:
        raise ValueError(f"29C2 output manifest must contain exactly 48 unique month/event partitions; got {len(partitions)}")
    return partitions, month_manifest, {
        "BridgeRunID": BRIDGE_RUN_ID,
        "OutputManifestSHA256": sha256_file(OUTPUT_MANIFEST),
        "FlightKeyManifestSHA256": sha256_file(FLIGHTKEY_MANIFEST),
        "BridgeSummarySHA256": sha256_file(BRIDGE_SUMMARY),
        "AirportMappingSHA256": sha256_file(AIRPORT_MAPPING_FILE),
    }


def validate_partition_source(item: dict[str, Any], airport_mapping: dict[str, int]) -> dict[str, Any]:
    path = item["path"]
    require_file(path)
    if path.stat().st_size != item["file_size_bytes"]:
        raise ValueError(f"File size differs from 29C2 manifest: {path.name}")
    digest = sha256_file(path)
    if digest != item["sha256"]:
        raise ValueError(f"SHA256 differs from 29C2 manifest: {path.name}")

    status_counts: Counter[str] = Counter()
    matched = 0
    count = 0
    prior_row_number = None
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != OUTPUT_FIELDS:
            raise ValueError(f"Bridge partition schema mismatch: {path.name}; got={reader.fieldnames}")
        for row in reader:
            count += 1
            ctx = f"{path.name}:data row {count}"
            if None in row or any(row.get(field) is None for field in OUTPUT_FIELDS):
                raise ValueError(f"Malformed CSV row width at {ctx}")
            flight_key = parse_uint(row["FlightKey"], "FlightKey", ctx, 1, EXPECTED_FLIGHTS)
            expected_key = item["first_key"] + count - 1
            if flight_key != expected_key:
                raise ValueError(f"FlightKey sequence mismatch at {ctx}: {flight_key} != {expected_key}")
            if clean(row["SourceMonth"]) != item["month"]:
                raise ValueError(f"SourceMonth mismatch at {ctx}")
            if clean(row["SourceFile"]) != item["source_file"]:
                raise ValueError(f"SourceFile mismatch at {ctx}: {row['SourceFile']!r} != {item['source_file']!r}")
            if clean(row["ExposureType"]) != item["event"]:
                raise ValueError(f"ExposureType mismatch at {ctx}")
            segment = parse_uint(row["SegmentSequence"], "SegmentSequence", ctx, 1, 255)
            if segment != 1:
                raise ValueError(f"Expected SegmentSequence=1 at {ctx}; got {segment}")
            source_row = parse_uint(row["SourceRowNumber"], "SourceRowNumber", ctx, 1, 2**32 - 1)
            if prior_row_number is not None and source_row <= prior_row_number:
                raise ValueError(f"SourceRowNumber is not strictly increasing at {ctx}")
            prior_row_number = source_row
            code = clean(row["ExposureAirportCode"]).upper()
            if len(code) != 3 or code not in airport_mapping:
                raise ValueError(f"Invalid/unmapped ExposureAirportCode={code!r} at {ctx}")
            our_id = parse_uint(row["OurAirportsID"], "OurAirportsID", ctx, 1, 2**32 - 1)
            if airport_mapping[code] != our_id:
                raise ValueError(f"Airport code/OurAirportsID conflict at {ctx}: {code}/{our_id}")
            timezone = clean(row["TimezoneIANA"])
            if not timezone or len(timezone) > 64:
                raise ValueError(f"Invalid TimezoneIANA at {ctx}")
            event_dt = parse_utc_datetime(row["EventTimestampUTC"], "EventTimestampUTC", ctx, required=False)
            weather_dt = parse_utc_datetime(row["WeatherHourUTC"], "WeatherHourUTC", ctx, required=False)
            minutes_text = clean(row["MinutesAfterWeatherHour"])
            minutes = None if not minutes_text else parse_uint(minutes_text, "MinutesAfterWeatherHour", ctx, 0, 59)
            timestamp_status = clean(row["TimestampStatus"])
            coverage_status = clean(row["WeatherCoverageStatus"])
            status_counts[coverage_status] += 1
            natural_key = clean(row["WeatherNaturalKey"])
            if not timestamp_status or len(timestamp_status) > 96:
                raise ValueError(f"Invalid TimestampStatus at {ctx}")
            if coverage_status not in EXPECTED_STATUS_COUNTS[item["event"]]:
                raise ValueError(f"Unexpected WeatherCoverageStatus={coverage_status!r} at {ctx}")
            if coverage_status == "MATCHED_WEATHER_HOUR":
                matched += 1
                if event_dt is None or weather_dt is None or minutes is None:
                    raise ValueError(f"Matched row missing timestamp/hour/minutes at {ctx}")
                if weather_dt.minute != 0 or weather_dt.second != 0 or weather_dt.microsecond != 0:
                    raise ValueError(f"WeatherHourUTC is not an exact floor-to-hour at {ctx}")
                expected_natural_key = f"{our_id}|{weather_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}"
                if natural_key != expected_natural_key:
                    raise ValueError(f"WeatherNaturalKey mismatch at {ctx}: {natural_key!r} != {expected_natural_key!r}")
                if len(natural_key) > 64:
                    raise ValueError(f"WeatherNaturalKey exceeds target width at {ctx}")
            else:
                if natural_key:
                    raise ValueError(f"Unmatched/separate row must not carry WeatherNaturalKey at {ctx}")
                if coverage_status in ("CANCELLED_SEPARATE_HANDLING", "DIVERTED_SEPARATE_HANDLING"):
                    if item["event"] != "ACTUAL_ARRIVAL" or event_dt is not None or weather_dt is not None:
                        raise ValueError(f"Cancelled/diverted separate handling has unexpected timestamp at {ctx}")
        if count != item["expected_rows"]:
            raise ValueError(f"Partition row count mismatch: {path.name}: {count} != {item['expected_rows']}")
    if matched != item["matched_rows"] or count - matched != item["unmatched_rows"]:
        raise ValueError(f"Partition matched/unmatched counts differ from manifest: {path.name}")
    return {
        "Rows": count,
        "MatchedRows": matched,
        "UnmatchedRows": count - matched,
        "StatusCounts": dict(status_counts),
        "SHA256": digest,
        "FileSizeBytes": path.stat().st_size,
        "FileSignature": file_signature(path, digest),
    }


def preflight_sources() -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    say("STEP 30E3 - FLIGHT / WEATHER BRIDGE PREFLIGHT")
    say("=" * 78)
    partitions, month_manifest, manifest_meta = load_source_manifests()
    airport_mapping = read_airport_mapping()
    say(f"[1/2] 29C2 inventory: {len(partitions)} partitions; run ID {BRIDGE_RUN_ID}.")
    status_totals: dict[str, Counter[str]] = {event: Counter() for event in EVENTS}
    total_rows = total_matched = 0
    summaries: list[dict[str, Any]] = []
    for index, item in enumerate(partitions, start=1):
        result = validate_partition_source(item, airport_mapping)
        item.update(result)
        for status, count in result["StatusCounts"].items():
            status_totals[item["event"]][status] += count
        total_rows += result["Rows"]
        total_matched += result["MatchedRows"]
        summaries.append({
            "SourceMonth": item["month"],
            "ExposureType": item["event"],
            "RowsValidated": result["Rows"],
            "MatchedRows": result["MatchedRows"],
            "UnmatchedRows": result["UnmatchedRows"],
            "SHA256": result["SHA256"],
        })
        if index == 1 or index % 4 == 0 or index == len(partitions):
            say(f"      Validated {index}/48 partitions; {total_rows:,}/{EXPECTED_ROWS:,} exposure rows.")

    failures: list[str] = []
    if total_rows != EXPECTED_ROWS:
        failures.append(f"Total rows {total_rows:,} != {EXPECTED_ROWS:,}")
    if total_matched != EXPECTED_MATCHED:
        failures.append(f"Matched rows {total_matched:,} != {EXPECTED_MATCHED:,}")
    if total_rows - total_matched != EXPECTED_UNMATCHED:
        failures.append(f"Unmatched/separate rows {total_rows - total_matched:,} != {EXPECTED_UNMATCHED:,}")
    for event in EVENTS:
        if sum(status_totals[event].values()) != EXPECTED_FLIGHTS:
            failures.append(f"{event}: total status rows do not equal {EXPECTED_FLIGHTS:,}")
        expected = EXPECTED_STATUS_COUNTS[event]
        actual = dict(status_totals[event])
        if actual != expected:
            failures.append(f"{event}: status counts differ from STEP 29B4: {actual} != {expected}")
    if failures:
        raise ValueError("Source preflight reconciliation failed: " + "; ".join(failures))

    say("[2/2] Source validation PASS: all 48 partitions; 28,006,476 rows; exact STEP 29B4 statuses.")
    meta = {
        **manifest_meta,
        "Partitions": len(partitions),
        "RowsValidated": total_rows,
        "MatchedRows": total_matched,
        "UnmatchedRows": total_rows - total_matched,
        "ExpectedAirportCodes": len(airport_mapping),
        "FileSummaries": summaries,
        "StatusCounts": {event: dict(status_totals[event]) for event in EVENTS},
    }
    return partitions, month_manifest, meta


def write_reports(run_id: str, mode: str, result: str, summary: dict[str, Any]) -> tuple[Path, Path]:
    PHYSICAL_DIR.mkdir(parents=True, exist_ok=True)
    txt_path = PHYSICAL_DIR / f"30E3_flight_weather_bridge_{mode.lower()}_{run_id}.txt"
    json_path = PHYSICAL_DIR / f"30E3_flight_weather_bridge_{mode.lower()}_{run_id}.json"
    payload = {
        "Step": "30E3", "Mode": mode, "Result": result, "RunID": run_id,
        "TargetDatabase": DB_NAME, "TargetTable": "Bridge_Flight_Weather_Exposure",
        **summary,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    lines = [
        "STEP 30E3 - FLIGHT / WEATHER EXPOSURE BRIDGE",
        "=" * 78,
        f"RunID: {run_id}", f"Mode: {mode}", f"Result: {result}",
        f"Target: {DB_NAME}.Bridge_Flight_Weather_Exposure",
        f"Partitions validated: {summary.get('PartitionsValidated', summary.get('Partitions', 0)):,} / 48",
        f"Rows validated: {summary.get('RowsValidated', 0):,} / {EXPECTED_ROWS:,}",
        f"Matched rows: {summary.get('MatchedRows', 0):,} / {EXPECTED_MATCHED:,}",
        f"Unmatched/separate rows: {summary.get('UnmatchedRows', 0):,} / {EXPECTED_UNMATCHED:,}",
        "Source files: READ ONLY",
    ]
    if summary.get("DatabaseValidation"):
        lines += ["", "DATABASE VALIDATION"]
        for key, value in summary["DatabaseValidation"].items():
            lines.append(f"{key}: {value}")
    lines += ["", f"FINAL RESULT: {result}"]
    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return txt_path, json_path


def checkpoint_journal_path() -> Path:
    return CHECKPOINT_FILE.with_name(CHECKPOINT_FILE.stem + "_journal.jsonl")


def append_checkpoint(payload: dict[str, Any]) -> None:
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    journal = checkpoint_journal_path()
    line = (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
    fd = os.open(str(journal), os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o666)
    try:
        view = memoryview(line)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("Could not append complete STEP 30E3 checkpoint journal record")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


def make_fingerprint(partitions: list[dict[str, Any]], manifest_meta: dict[str, Any], batch_size: int) -> dict[str, Any]:
    return {
        "BridgeRunID": BRIDGE_RUN_ID,
        "OutputManifestSHA256": manifest_meta["OutputManifestSHA256"],
        "FlightKeyManifestSHA256": manifest_meta["FlightKeyManifestSHA256"],
        "BridgeSummarySHA256": manifest_meta["BridgeSummarySHA256"],
        "AirportMappingSHA256": manifest_meta["AirportMappingSHA256"],
        "BatchSize": batch_size,
        "Partitions": {
            f"{p['month']}|{p['event']}": {
                "Filename": p["filename"],
                "SHA256": p["SHA256"],
                "Rows": p["expected_rows"],
                "FirstFlightKey": p["first_key"],
                "LastFlightKey": p["last_key"],
            } for p in partitions
        },
    }


def load_checkpoint(fingerprint: dict[str, Any], partitions: list[dict[str, Any]]) -> dict[str, Any]:
    candidates: list[tuple[int, int, str, dict[str, Any]]] = []
    sequence = 0
    partition_by_key = {f"{p['month']}|{p['event']}": p for p in partitions}

    def consider(label: str, text: str) -> None:
        nonlocal sequence
        try:
            state = json.loads(text)
        except (json.JSONDecodeError, UnicodeDecodeError):
            say(f"[CHECKPOINT] Ignoring incomplete/corrupt snapshot: {label}")
            return
        if state.get("SchemaVersion") != 1:
            raise ValueError(f"Unsupported STEP 30E3 checkpoint version in {label}")
        if state.get("Fingerprint") != fingerprint:
            raise ValueError(f"STEP 30E3 checkpoint fingerprint mismatch in {label}; preserve and investigate")
        done = state.get("Partitions", {})
        if not isinstance(done, dict):
            raise ValueError(f"Invalid checkpoint partition map in {label}")
        progress = 0
        for key, part_state in done.items():
            if key not in partition_by_key or not isinstance(part_state, dict):
                raise ValueError(f"Unexpected checkpoint partition {key!r}")
            rows = int(part_state.get("CompletedSourceRows", 0))
            expected = partition_by_key[key]["expected_rows"]
            if rows < 0 or rows > expected:
                raise ValueError(f"Checkpoint rows out of range for {key}: {rows}")
            progress += rows
        sequence += 1
        candidates.append((progress, sequence, label, state))

    for path in (CHECKPOINT_FILE, CHECKPOINT_FILE.with_suffix(CHECKPOINT_FILE.suffix + ".tmp")):
        if path.is_file():
            try:
                consider(path.name, path.read_text(encoding="utf-8"))
            except OSError as exc:
                say(f"[CHECKPOINT] Could not read {path.name}: {exc}")
    journal = checkpoint_journal_path()
    if journal.is_file():
        with journal.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                if line.strip():
                    consider(f"{journal.name}:line {line_no}", line)
    if candidates:
        progress, _, label, state = max(candidates, key=lambda x: (x[0], x[1]))
        say(f"[CHECKPOINT] Restored latest valid snapshot: {label}; completed source rows={progress:,}")
    else:
        state = {"SchemaVersion": 1, "Fingerprint": fingerprint, "Partitions": {}}
        say("[CHECKPOINT] No prior valid STEP 30E3 snapshot; starting with an empty journal.")
    say(f"[CHECKPOINT] Future updates append to: {journal}")
    return state


def validate_db_prerequisites(cursor) -> dict[str, Any]:
    cursor.execute("SELECT DATABASE(), VERSION(), @@datadir")
    database, version, datadir = cursor.fetchone()
    if database != DB_NAME or not str(version).startswith("8.0."):
        raise RuntimeError(f"Unexpected MySQL target/version: {database}/{version}")
    say(f"Connected database: {database}; MySQL: {version}")
    cursor.execute("SELECT COUNT(*), MIN(FlightKey), MAX(FlightKey), COUNT(DISTINCT FlightKey), COUNT(DISTINCT SourceFile, SourceRowNumber) FROM Fact_Flight")
    flight = cursor.fetchone()
    expected_flight = (EXPECTED_FLIGHTS, 1, EXPECTED_FLIGHTS, EXPECTED_FLIGHTS, EXPECTED_FLIGHTS)
    if tuple(None if v is None else int(v) for v in flight) != expected_flight:
        raise RuntimeError(f"Fact_Flight prerequisite validation failed: {flight} != {expected_flight}")
    cursor.execute("SELECT COUNT(*), COUNT(DISTINCT OurAirportsID, WeatherHourUTC), COUNT(DISTINCT WeatherKey) FROM Fact_Airport_Weather_Hourly")
    weather = tuple(int(v) for v in cursor.fetchone())
    if weather != (3_083_520, 3_083_520, 3_083_520):
        raise RuntimeError(f"Weather Fact prerequisite validation failed: {weather}")
    cursor.execute("SELECT COUNT(*) FROM Map_BTS_Airport")
    map_count = int(cursor.fetchone()[0])
    if map_count != 352:
        raise RuntimeError(f"Map_BTS_Airport expected 352 rows; found {map_count}")
    cursor.execute("""
        SELECT m.BTSAirportCode, m.AirportKey, m.OurAirportsID, a.TimeZoneIANA
        FROM Map_BTS_Airport m
        JOIN Dim_Airport a
          ON a.AirportKey = m.AirportKey
         AND a.OurAirportsID = m.OurAirportsID
        ORDER BY m.BTSAirportCode
    """)
    airport_map = {
        str(code): (int(airport_key), int(our_id), str(tz))
        for code, airport_key, our_id, tz in cursor.fetchall()
    }
    if len(airport_map) != 352 or any(not v[2] for v in airport_map.values()):
        raise RuntimeError("Map_BTS_Airport/Dim_Airport timezone join did not produce 352 complete rows")
    cursor.execute("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME='bridge_flight_weather_exposure'", (DB_NAME,))
    if int(cursor.fetchone()[0]) != 1:
        raise RuntimeError("Target Bridge_Flight_Weather_Exposure table is missing")
    return {
        "Database": database,
        "MySQLVersion": version,
        "MySQLDataDirectory": str(datadir),
        "FactFlightRows": int(flight[0]),
        "WeatherFactRows": weather[0],
        "AirportMappings": map_count,
        "AirportMap": airport_map,
    }


STAGE_CREATE_SQL = """
CREATE TEMPORARY TABLE tmp_30e3_bridge_stage (
    FlightKey BIGINT UNSIGNED NOT NULL,
    ExposureType VARCHAR(32) NOT NULL,
    SegmentSequence TINYINT UNSIGNED NOT NULL,
    SourceFile VARCHAR(32) NOT NULL,
    SourceMonth CHAR(2) NOT NULL,
    SourceRowNumber INT UNSIGNED NOT NULL,
    ExposureAirportCode CHAR(3) NOT NULL,
    AirportKey INT UNSIGNED NOT NULL,
    OurAirportsID INT UNSIGNED NOT NULL,
    TimezoneIANA VARCHAR(64) NOT NULL,
    EventTimestampUTC DATETIME NULL,
    WeatherHourUTC DATETIME NULL,
    MinutesAfterWeatherHour TINYINT UNSIGNED NULL,
    TimestampStatus VARCHAR(96) NOT NULL,
    WeatherNaturalKey VARCHAR(64) NULL,
    WeatherCoverageStatus VARCHAR(48) NOT NULL,
    PRIMARY KEY (FlightKey, ExposureType, SegmentSequence),
    KEY ix_tmp_30e3_weather (OurAirportsID, WeatherHourUTC)
) ENGINE=InnoDB
"""
STAGE_INSERT_SQL = """
INSERT INTO tmp_30e3_bridge_stage (
    FlightKey, ExposureType, SegmentSequence, SourceFile, SourceMonth, SourceRowNumber,
    ExposureAirportCode, AirportKey, OurAirportsID, TimezoneIANA,
    EventTimestampUTC, WeatherHourUTC, MinutesAfterWeatherHour,
    TimestampStatus, WeatherNaturalKey, WeatherCoverageStatus
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
"""

BRIDGE_INSERT_SELECT_SQL = """
INSERT INTO Bridge_Flight_Weather_Exposure (
    FlightKey, ExposureType, SegmentSequence, SourceFile, SourceMonth, SourceRowNumber,
    ExposureAirportCode, AirportKey, OurAirportsID, TimezoneIANA,
    EventTimestampUTC, WeatherHourUTC, MinutesAfterWeatherHour,
    TimestampStatus, WeatherNaturalKey, WeatherKey, WeatherCoverageStatus
)
SELECT
    s.FlightKey, s.ExposureType, s.SegmentSequence, s.SourceFile, s.SourceMonth, s.SourceRowNumber,
    s.ExposureAirportCode, s.AirportKey, s.OurAirportsID, s.TimezoneIANA,
    s.EventTimestampUTC, s.WeatherHourUTC, s.MinutesAfterWeatherHour,
    s.TimestampStatus, s.WeatherNaturalKey, w.WeatherKey, s.WeatherCoverageStatus
FROM tmp_30e3_bridge_stage s
LEFT JOIN Fact_Airport_Weather_Hourly w
  ON s.WeatherCoverageStatus = 'MATCHED_WEATHER_HOUR'
 AND w.OurAirportsID = s.OurAirportsID
 AND w.WeatherHourUTC = s.WeatherHourUTC
ON DUPLICATE KEY UPDATE
    SourceFile = VALUES(SourceFile),
    SourceMonth = VALUES(SourceMonth),
    SourceRowNumber = VALUES(SourceRowNumber),
    ExposureAirportCode = VALUES(ExposureAirportCode),
    AirportKey = VALUES(AirportKey),
    OurAirportsID = VALUES(OurAirportsID),
    TimezoneIANA = VALUES(TimezoneIANA),
    EventTimestampUTC = VALUES(EventTimestampUTC),
    WeatherHourUTC = VALUES(WeatherHourUTC),
    MinutesAfterWeatherHour = VALUES(MinutesAfterWeatherHour),
    TimestampStatus = VALUES(TimestampStatus),
    WeatherNaturalKey = VALUES(WeatherNaturalKey),
    WeatherKey = VALUES(WeatherKey),
    WeatherCoverageStatus = VALUES(WeatherCoverageStatus)
"""


def make_stage_tuple(row: dict[str, str], item: dict[str, Any], airport_map: dict[str, tuple[int, int, str]], row_index: int) -> tuple[Any, ...]:
    ctx = f"{item['filename']}:{row_index}"
    code = clean(row["ExposureAirportCode"]).upper()
    if code not in airport_map:
        raise ValueError(f"Exposure airport code {code!r} absent from Map_BTS_Airport at {ctx}")
    airport_key, our_id, timezone = airport_map[code]
    source_id = parse_uint(row["OurAirportsID"], "OurAirportsID", ctx, 1, 2**32 - 1)
    if source_id != our_id:
        raise ValueError(f"Source OurAirportsID does not match database mapping for {code} at {ctx}")
    source_timezone = clean(row["TimezoneIANA"])
    if source_timezone != timezone:
        raise ValueError(f"TimezoneIANA does not match Dim_Airport for {code} at {ctx}")
    source_file = clean(row["SourceFile"])
    if len(source_file) > 32:
        raise ValueError(f"SourceFile exceeds target width at {ctx}")
    source_row = parse_uint(row["SourceRowNumber"], "SourceRowNumber", ctx, 1, 2**32 - 1)
    event_dt = parse_utc_datetime(row["EventTimestampUTC"], "EventTimestampUTC", ctx, required=False)
    weather_dt = parse_utc_datetime(row["WeatherHourUTC"], "WeatherHourUTC", ctx, required=False)
    minutes_text = clean(row["MinutesAfterWeatherHour"])
    minutes = None if not minutes_text else parse_uint(minutes_text, "MinutesAfterWeatherHour", ctx, 0, 59)
    natural = clean(row["WeatherNaturalKey"])
    coverage = clean(row["WeatherCoverageStatus"])
    if coverage == "MATCHED_WEATHER_HOUR":
        if weather_dt is None or event_dt is None or minutes is None:
            raise ValueError(f"Matched row missing event/hour/minutes at {ctx}")
        expected_natural = f"{our_id}|{weather_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        if natural != expected_natural:
            raise ValueError(f"WeatherNaturalKey mismatch at {ctx}")
        natural_value = natural
    else:
        if natural:
            raise ValueError(f"Unmatched row unexpectedly has WeatherNaturalKey at {ctx}")
        natural_value = None
    return (
        parse_uint(row["FlightKey"], "FlightKey", ctx, 1, EXPECTED_FLIGHTS),
        clean(row["ExposureType"]),
        parse_uint(row["SegmentSequence"], "SegmentSequence", ctx, 1, 255),
        source_file,
        clean(row["SourceMonth"]),
        source_row,
        code,
        airport_key,
        our_id,
        timezone,
        event_dt,
        weather_dt,
        minutes,
        clean(row["TimestampStatus"]),
        natural_value,
        coverage,
    )


def create_stage(cursor) -> None:
    cursor.execute(STAGE_CREATE_SQL)


def validate_batch_weather_links(cursor, expected_matched: int) -> None:
    cursor.execute("""
        SELECT
            SUM(s.WeatherCoverageStatus = 'MATCHED_WEATHER_HOUR') AS MatchedStageRows,
            SUM(s.WeatherCoverageStatus = 'MATCHED_WEATHER_HOUR' AND w.WeatherKey IS NULL) AS MissingWeatherKeys,
            SUM(s.WeatherCoverageStatus <> 'MATCHED_WEATHER_HOUR' AND w.WeatherKey IS NOT NULL) AS UnexpectedWeatherKeys
        FROM tmp_30e3_bridge_stage s
        LEFT JOIN Fact_Airport_Weather_Hourly w
          ON s.WeatherCoverageStatus = 'MATCHED_WEATHER_HOUR'
         AND w.OurAirportsID = s.OurAirportsID
         AND w.WeatherHourUTC = s.WeatherHourUTC
    """)
    matched, missing, unexpected = cursor.fetchone()
    matched = int(matched or 0)
    missing = int(missing or 0)
    unexpected = int(unexpected or 0)
    if matched != expected_matched or missing != 0 or unexpected != 0:
        raise RuntimeError(f"Batch WeatherKey resolution failed: matched={matched}, missing={missing}, unexpected={unexpected}")


def validate_existing_batch_conflicts(cursor) -> None:
    cursor.execute("""
        SELECT COUNT(*)
        FROM tmp_30e3_bridge_stage s
        JOIN Bridge_Flight_Weather_Exposure b
          ON b.FlightKey = s.FlightKey
         AND b.ExposureType = s.ExposureType
         AND b.SegmentSequence = s.SegmentSequence
        LEFT JOIN Fact_Airport_Weather_Hourly w
          ON s.WeatherCoverageStatus = 'MATCHED_WEATHER_HOUR'
         AND w.OurAirportsID = s.OurAirportsID
         AND w.WeatherHourUTC = s.WeatherHourUTC
        WHERE NOT (
            b.SourceFile <=> s.SourceFile
            AND b.SourceMonth <=> s.SourceMonth
            AND b.SourceRowNumber <=> s.SourceRowNumber
            AND b.ExposureAirportCode <=> s.ExposureAirportCode
            AND b.AirportKey <=> s.AirportKey
            AND b.OurAirportsID <=> s.OurAirportsID
            AND b.TimezoneIANA <=> s.TimezoneIANA
            AND b.EventTimestampUTC <=> s.EventTimestampUTC
            AND b.WeatherHourUTC <=> s.WeatherHourUTC
            AND b.MinutesAfterWeatherHour <=> s.MinutesAfterWeatherHour
            AND b.TimestampStatus <=> s.TimestampStatus
            AND b.WeatherNaturalKey <=> s.WeatherNaturalKey
            AND b.WeatherKey <=> w.WeatherKey
            AND b.WeatherCoverageStatus <=> s.WeatherCoverageStatus
        )
    """)
    conflicts = int(cursor.fetchone()[0])
    if conflicts:
        raise RuntimeError(f"Found {conflicts:,} existing Bridge rows with conflicting source values; refusing upsert")


def query_partition_state(cursor, item: dict[str, Any]) -> tuple[int, int | None, int | None]:
    cursor.execute("""
        SELECT COUNT(*), MIN(FlightKey), MAX(FlightKey),
               COUNT(DISTINCT FlightKey), COUNT(DISTINCT SourceRowNumber),
               MIN(SourceFile), MAX(SourceFile)
        FROM Bridge_Flight_Weather_Exposure
        WHERE SourceMonth = %s AND ExposureType = %s AND SegmentSequence = 1
    """, (item["month"], item["event"]))
    count, min_key, max_key, distinct_keys, distinct_rows, min_file, max_file = cursor.fetchone()
    count = int(count)
    if count == 0:
        return 0, None, None
    if (int(distinct_keys) != count or int(distinct_rows) != count
            or int(min_key) != item["first_key"] or int(max_key) != item["first_key"] + count - 1
            or min_file != item["source_file"] or max_file != item["source_file"]):
        raise RuntimeError(f"Existing Bridge partition has inconsistent key/lineage: {item['month']}/{item['event']}")
    if count > item["expected_rows"]:
        raise RuntimeError(f"Existing Bridge partition exceeds expected rows: {item['month']}/{item['event']}")
    return count, int(min_key), int(max_key)


def load_one_partition(connection, cursor, item: dict[str, Any], checkpoint: dict[str, Any],
                        batch_size: int, airport_map: dict[str, tuple[int, int, str]]) -> dict[str, Any]:
    key = f"{item['month']}|{item['event']}"
    state = checkpoint["Partitions"].get(key, {})
    checkpoint_rows = int(state.get("CompletedSourceRows", 0))
    db_rows, _, _ = query_partition_state(cursor, item)
    if db_rows < checkpoint_rows or db_rows > checkpoint_rows + batch_size:
        raise RuntimeError(f"Bridge DB/checkpoint mismatch for {key}: DB={db_rows:,}, checkpoint={checkpoint_rows:,}, batch={batch_size:,}")
    if checkpoint_rows == item["expected_rows"] and db_rows == item["expected_rows"]:
        say(f"      [CHECKPOINT] {key}: fully loaded and verified; skipping.")
        return {"RowsPresent": db_rows, "RowsLoadedThisRun": 0, "Status": "VERIFIED_CHECKPOINT_SKIP"}

    start_stat = item["path"].stat()
    if start_stat.st_size != item["FileSignature"]["size_bytes"] or start_stat.st_mtime_ns != item["FileSignature"]["mtime_ns"]:
        raise RuntimeError(f"Bridge source partition changed after preflight: {item['filename']}")
    loaded_this_run = 0
    processed = 0
    batch: list[tuple[Any, ...]] = []
    batch_matched = 0
    cursor.execute("DELETE FROM tmp_30e3_bridge_stage")
    with item["path"].open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != OUTPUT_FIELDS:
            raise ValueError(f"Bridge schema changed before execute: {item['filename']}")
        for row_index, row in enumerate(reader, start=1):
            processed += 1
            flight_key = item["first_key"] + row_index - 1
            if parse_uint(row["FlightKey"], "FlightKey", f"{item['filename']}:{row_index}", 1, EXPECTED_FLIGHTS) != flight_key:
                raise ValueError(f"FlightKey changed between preflight and execute: {item['filename']}:{row_index}")
            if processed <= checkpoint_rows:
                continue
            if clean(row["SourceMonth"]) != item["month"] or clean(row["ExposureType"]) != item["event"]:
                raise ValueError(f"Partition identity changed after preflight: {item['filename']}:{row_index}")
            if clean(row["SourceFile"]) != item["source_file"]:
                raise ValueError(f"SourceFile changed after preflight: {item['filename']}:{row_index}")
            if parse_uint(row["SegmentSequence"], "SegmentSequence", f"{item['filename']}:{row_index}", 1, 255) != 1:
                raise ValueError(f"SegmentSequence changed after preflight: {item['filename']}:{row_index}")
            rec = make_stage_tuple(row, item, airport_map, row_index)
            batch.append(rec)
            if rec[-1] == "MATCHED_WEATHER_HOUR":
                batch_matched += 1
            if len(batch) >= batch_size:
                cursor.executemany(STAGE_INSERT_SQL, batch)
                validate_batch_weather_links(cursor, batch_matched)
                validate_existing_batch_conflicts(cursor)
                cursor.execute(BRIDGE_INSERT_SELECT_SQL)
                connection.commit()
                loaded_this_run += len(batch)
                checkpoint["Partitions"][key] = {
                    "CompletedSourceRows": processed,
                    "LastCommittedFlightKey": item["first_key"] + processed - 1,
                    "ExpectedRows": item["expected_rows"],
                    "SourceFileSignature": item["FileSignature"],
                }
                append_checkpoint(checkpoint)
                batch.clear()
                batch_matched = 0
                cursor.execute("DELETE FROM tmp_30e3_bridge_stage")
                if loaded_this_run % (batch_size * 10) == 0:
                    say(f"      Bridge {item['month']}/{item['event']}: {processed:,}/{item['expected_rows']:,} rows processed; {loaded_this_run:,} newly committed this run")
        if batch:
            cursor.executemany(STAGE_INSERT_SQL, batch)
            validate_batch_weather_links(cursor, batch_matched)
            validate_existing_batch_conflicts(cursor)
            cursor.execute(BRIDGE_INSERT_SELECT_SQL)
            connection.commit()
            loaded_this_run += len(batch)
            checkpoint["Partitions"][key] = {
                "CompletedSourceRows": processed,
                "LastCommittedFlightKey": item["last_key"],
                "ExpectedRows": item["expected_rows"],
                "SourceFileSignature": item["FileSignature"],
            }
            append_checkpoint(checkpoint)
            batch.clear()
            cursor.execute("DELETE FROM tmp_30e3_bridge_stage")
    if processed != item["expected_rows"]:
        raise RuntimeError(f"Bridge partition source rows changed: {item['filename']}: {processed} != {item['expected_rows']}")
    end_stat = item["path"].stat()
    if end_stat.st_size != start_stat.st_size or end_stat.st_mtime_ns != start_stat.st_mtime_ns:
        raise RuntimeError(f"Bridge source partition changed during execute: {item['filename']}")

    count, min_key, max_key = query_partition_state(cursor, item)
    if count != item["expected_rows"] or min_key != item["first_key"] or max_key != item["last_key"]:
        raise RuntimeError(f"Post-load Bridge partition validation failed for {key}: count={count}, range={min_key}..{max_key}")
    checkpoint["Partitions"][key] = {
        "CompletedSourceRows": item["expected_rows"],
        "LastCommittedFlightKey": item["last_key"],
        "ExpectedRows": item["expected_rows"],
        "SourceFileSignature": item["FileSignature"],
        "DatabaseValidation": "PASS",
    }
    append_checkpoint(checkpoint)
    say(f"      Bridge {item['month']}/{item['event']}: {count:,}/{item['expected_rows']:,} rows committed and verified; FlightKey {item['first_key']:,}..{item['last_key']:,}")
    return {"RowsPresent": count, "RowsLoadedThisRun": loaded_this_run, "Status": "PASS"}


def final_database_validation(cursor) -> dict[str, Any]:
    validation: dict[str, Any] = {}
    cursor.execute("""
        SELECT COUNT(*), COUNT(DISTINCT FlightKey),
               COUNT(DISTINCT FlightKey, ExposureType, SegmentSequence),
               SUM(WeatherCoverageStatus = 'MATCHED_WEATHER_HOUR'),
               SUM(WeatherKey IS NOT NULL),
               SUM(WeatherCoverageStatus <> 'MATCHED_WEATHER_HOUR' AND WeatherKey IS NOT NULL),
               MIN(FlightKey), MAX(FlightKey), COUNT(DISTINCT SourceFile, SourceRowNumber, ExposureType)
        FROM Bridge_Flight_Weather_Exposure
    """)
    row = cursor.fetchone()
    names = ["FinalDatabaseRows", "DistinctFlightKeys", "DistinctBridgePrimaryKeys", "MatchedRowsByStatus",
             "RowsWithWeatherKey", "UnmatchedRowsWithWeatherKey", "MinFlightKey", "MaxFlightKey", "DistinctLineageEventKeys"]
    for name, value in zip(names, row):
        validation[name] = None if value is None else int(value)
    expected = {
        "FinalDatabaseRows": EXPECTED_ROWS,
        "DistinctFlightKeys": EXPECTED_FLIGHTS,
        "DistinctBridgePrimaryKeys": EXPECTED_ROWS,
        "MatchedRowsByStatus": EXPECTED_MATCHED,
        "RowsWithWeatherKey": EXPECTED_MATCHED,
        "UnmatchedRowsWithWeatherKey": 0,
        "MinFlightKey": 1,
        "MaxFlightKey": EXPECTED_FLIGHTS,
        "DistinctLineageEventKeys": EXPECTED_ROWS,
    }
    failures = {k: (validation.get(k), v) for k, v in expected.items() if validation.get(k) != v}
    if failures:
        raise RuntimeError(f"Bridge global count/key validation failed: {failures}")

    cursor.execute("SELECT ExposureType, WeatherCoverageStatus, COUNT(*) FROM Bridge_Flight_Weather_Exposure GROUP BY ExposureType, WeatherCoverageStatus")
    actual_status: dict[str, dict[str, int]] = {event: {} for event in EVENTS}
    for event, status, count in cursor.fetchall():
        if event not in actual_status:
            raise RuntimeError(f"Unexpected ExposureType in Bridge: {event}")
        actual_status[event][status] = int(count)
    for event in EVENTS:
        if actual_status[event] != EXPECTED_STATUS_COUNTS[event]:
            raise RuntimeError(f"Bridge status reconciliation failed for {event}: {actual_status[event]} != {EXPECTED_STATUS_COUNTS[event]}")
    validation["StatusCounts"] = actual_status

    monthly: dict[str, Any] = {}
    for month in MONTHS:
        for event in EVENTS:
            first = next(x["FirstFlightKey"] for x in read_flightkey_manifest() if x["SourceMonth"] == month)
            last = next(x["LastFlightKey"] for x in read_flightkey_manifest() if x["SourceMonth"] == month)
            cursor.execute("SELECT COUNT(*), MIN(FlightKey), MAX(FlightKey), COUNT(DISTINCT SourceRowNumber) FROM Bridge_Flight_Weather_Exposure WHERE SourceMonth=%s AND ExposureType=%s", (month, event))
            count, min_key, max_key, distinct_rows = cursor.fetchone()
            actual = {"Rows": int(count), "MinFlightKey": int(min_key), "MaxFlightKey": int(max_key), "DistinctSourceRows": int(distinct_rows)}
            wanted = {"Rows": MONTH_ROWS[month], "MinFlightKey": int(first), "MaxFlightKey": int(last), "DistinctSourceRows": MONTH_ROWS[month]}
            if actual != wanted:
                raise RuntimeError(f"Monthly Bridge validation failed for {month}/{event}: {actual} != {wanted}")
            monthly[f"{month}|{event}"] = actual
    validation["MonthlyPartitionValidation"] = monthly

    cursor.execute("""
        SELECT COUNT(*)
        FROM Bridge_Flight_Weather_Exposure b
        LEFT JOIN Fact_Flight f
          ON f.FlightKey = b.FlightKey
         AND f.SourceFile = b.SourceFile
         AND f.SourceMonth = b.SourceMonth
         AND f.SourceRowNumber = b.SourceRowNumber
        WHERE f.FlightKey IS NULL
    """)
    flight_lineage_orphans = int(cursor.fetchone()[0])
    validation["FlightLineageOrphans"] = flight_lineage_orphans
    if flight_lineage_orphans != 0:
        raise RuntimeError(f"Bridge -> Fact_Flight lineage mismatch: {flight_lineage_orphans:,}")

    cursor.execute("""
        SELECT COUNT(*)
        FROM Bridge_Flight_Weather_Exposure b
        JOIN Fact_Airport_Weather_Hourly w
          ON w.WeatherKey = b.WeatherKey
        WHERE b.WeatherCoverageStatus = 'MATCHED_WEATHER_HOUR'
          AND (w.OurAirportsID <> b.OurAirportsID OR w.WeatherHourUTC <> b.WeatherHourUTC)
    """)
    weather_key_mismatches = int(cursor.fetchone()[0])
    validation["WeatherKeyNaturalKeyMismatches"] = weather_key_mismatches
    if weather_key_mismatches != 0:
        raise RuntimeError(f"Bridge WeatherKey/natural-key mismatch: {weather_key_mismatches:,}")

    return validation


def connect_mysql(args: argparse.Namespace):
    try:
        import mysql.connector  # type: ignore
    except ImportError as exc:
        raise RuntimeError("mysql-connector-python is not installed. Run: python -m pip install mysql-connector-python") from exc
    user = args.user or DEFAULT_USER
    password = os.getenv("FLIGHT_DW_DB_PASSWORD")
    if password is None:
        password = getpass.getpass(f"MySQL password for {user}@{args.host}:{args.port}: ")
    return mysql.connector.connect(
        host=args.host, port=args.port, user=user, password=password, database=DB_NAME,
        charset="utf8mb4", collation="utf8mb4_0900_ai_ci", autocommit=False,
        connection_timeout=30,
    )


def execute_load(args: argparse.Namespace, partitions: list[dict[str, Any]], month_manifest: list[dict[str, Any]],
                 manifest_meta: dict[str, Any]) -> dict[str, Any]:
    connection = connect_mysql(args)
    cursor = None
    try:
        cursor = connection.cursor()
        db_info = validate_db_prerequisites(cursor)
        say("[DB] Prerequisites PASS: Fact_Flight=7,001,619; Weather Fact=3,083,520; airport mappings=352.")
        fingerprint = make_fingerprint(partitions, manifest_meta, args.batch_size)
        checkpoint = load_checkpoint(fingerprint, partitions)
        create_stage(cursor)
        monthly_results: dict[str, Any] = {}
        for item in partitions:
            key = f"{item['month']}|{item['event']}"
            checkpoint_rows = int(checkpoint["Partitions"].get(key, {}).get("CompletedSourceRows", 0))
            db_rows, _, _ = query_partition_state(cursor, item)
            if db_rows > checkpoint_rows:
                say(f"[RESUME] {key}: replaying up to {db_rows - checkpoint_rows:,} committed row(s) after the last checkpoint.")
            if checkpoint_rows == 0 and db_rows > args.batch_size:
                raise RuntimeError(f"{key}: database has {db_rows:,} rows but no valid checkpoint; refusing unverified resume")
            result = load_one_partition(connection, cursor, item, checkpoint, args.batch_size, db_info["AirportMap"])
            monthly_results[key] = result
        validation = final_database_validation(cursor)
        connection.commit()
        return {
            "Database": db_info["Database"],
            "MySQLVersion": db_info["MySQLVersion"],
            "PartitionsLoaded": len(partitions),
            "ExpectedRows": EXPECTED_ROWS,
            "ExpectedMatchedRows": EXPECTED_MATCHED,
            "ExpectedUnmatchedRows": EXPECTED_UNMATCHED,
            "DatabaseValidation": validation,
            "PartitionLoadResults": monthly_results,
            "CheckpointFile": str(CHECKPOINT_FILE),
            "CheckpointJournalFile": str(checkpoint_journal_path()),
            "BatchSize": args.batch_size,
            "NoSourceFilesModified": True,
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        if cursor is not None:
            cursor.close()
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="STEP 30E3 - Load Bridge_Flight_Weather_Exposure")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--preview", action="store_true", help="Full source preflight only; default")
    mode.add_argument("--execute", action="store_true", help="Load 29C2 bridge partitions into MySQL")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--user", default=DEFAULT_USER)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args()
    if not (ROOT / "04_CLEAN_DATA").is_dir() or not (ROOT / "07_GOLD_DATA").is_dir():
        say("ERROR: Run this script from D:\\DATA_WAREHOUSE project root.")
        return 2
    if args.batch_size < 500 or args.batch_size > 20_000:
        say("ERROR: --batch-size must be between 500 and 20,000.")
        return 2
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        partitions, month_manifest, meta = preflight_sources()
        say("\nPREFLIGHT SUMMARY")
        say("-" * 78)
        for key in ("BridgeRunID", "Partitions", "RowsValidated", "MatchedRows", "UnmatchedRows", "ExpectedAirportCodes"):
            say(f"{key}: {meta[key]}")
        say("StatusCounts:")
        for event in EVENTS:
            say(f"  {event}: {meta['StatusCounts'][event]}")
        say("\nSOURCE PREFLIGHT RESULT: PASS")
        if args.execute:
            say("\nSTEP 30E3 - EXECUTE BRIDGE LOAD")
            say("=" * 78)
            database_result = execute_load(args, partitions, month_manifest, meta)
            summary = {**meta, **database_result, "PartitionsValidated": len(partitions)}
            result = "PASS"
            say("\nSTEP 30E3 EXECUTE RESULT: PASS")
        else:
            summary = {**meta, "PartitionsValidated": len(partitions), "Message": "Source preflight only; no MySQL connection or writes performed."}
            result = "PASS"
            say("\nSTEP 30E3 PREVIEW RESULT: PASS")
            say("No MySQL rows were inserted or updated.")
        txt_path, json_path = write_reports(run_id, "EXECUTE" if args.execute else "PREVIEW", result, summary)
        say(f"Text report: {txt_path}")
        say(f"JSON report: {json_path}")
        say("STEP 30E3 COMPLETE")
        return 0
    except Exception as exc:
        say("\nSTEP 30E3 RESULT: FAIL")
        say(f"ERROR: {type(exc).__name__}: {exc}")
        say("RAW/STAGING/CLEAN/29C2 Bridge source files were not modified.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
