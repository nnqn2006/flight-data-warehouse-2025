#!/usr/bin/env python3
"""STEP 30E2 - Load BTS CLEAN flight rows into MySQL Fact_Flight.

Default mode is PREVIEW. PREVIEW scans and validates all 7,001,619 BTS CLEAN
rows but does not connect to MySQL or modify any source/target data.

EXECUTE mode:
  * Uses the exact STEP 29C2 monthly FlightKey manifest and source row order.
  * Loads in bounded batches and commits each batch.
  * Appends a durable checkpoint snapshot after every committed batch to an
    append-only JSONL journal (no Windows rename/replace of a locked file).
  * Recovers from the newest valid snapshot among the prior JSON, prior .tmp,
    and journal; committed rows may be replayed idempotently after interruption.
  * Refuses a conflicting FlightKey <-> (SourceFile, SourceRowNumber) mapping.
  * Validates database dimensions and target row counts before/after loading.

It never modifies RAW, STAGING, CLEAN, GOLD source data, and never DROP,
TRUNCATE, or DELETEs target rows.
"""
from __future__ import annotations

import argparse
import csv
import getpass
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator

ROOT = Path.cwd()
DB_NAME = "flight_dw_2025"
EXPECTED_ROWS = 7_001_619
EXPECTED_AIRLINES = 14
EXPECTED_AIRPORT_CODES = 352
EXPECTED_CANCELLED = 102_876
EXPECTED_DIVERTED = 19_258
DEFAULT_BATCH_SIZE = 1_000

BTS_DIR = ROOT / "04_CLEAN_DATA" / "BTS"
PHYSICAL_DIR = ROOT / "06_DW_DESIGN" / "02_PHYSICAL_MODEL"
BRIDGE_RUN_ID = "20261110_030433_504879"
BRIDGE_RUN_DIR = ROOT / "07_GOLD_DATA" / "01_FLIGHT_WEATHER_EXPOSURE_BRIDGE" / f"run_{BRIDGE_RUN_ID}"
FLIGHTKEY_MANIFEST = BRIDGE_RUN_DIR / f"29C2_flightkey_month_manifest_{BRIDGE_RUN_ID}.csv"
AIRPORT_MAPPING_FILE = ROOT / "02_DATA_PROFILING" / "02_OurAirports" / "13_bts_airport_mapping_resolved.csv"
CHECKPOINT_FILE = PHYSICAL_DIR / "_checkpoints" / "30E2_flight_fact_checkpoint.json"

EXPECTED_MONTH_ROWS = {
    "01": 539_747, "02": 504_884, "03": 600_872, "04": 583_950,
    "05": 605_648, "06": 611_575, "07": 631_428, "08": 602_378,
    "09": 562_439, "10": 605_844, "11": 570_550, "12": 582_304,
}

REQUIRED_COLUMNS = [
    "SourceFile", "SourceMonth", "SourceRowNumber",
    "DOT_ID_Reporting_Airline", "Reporting_Airline", "IATA_CODE_Reporting_Airline",
    "Flight_Number_Reporting_Airline", "Tail_Number", "Origin", "Dest",
    "FlightDate_Clean", "DQ_FlightDate_Status",
    "CRSDepTime_Clean", "DQ_CRSDepTime_Status",
    "DepTime_Clean", "DQ_DepTime_Status",
    "CRSArrTime_Clean", "DQ_CRSArrTime_Status",
    "ArrTime_Clean", "DQ_ArrTime_Status",
    "DepDelay", "DepDelayMinutes", "DepDel15", "DepartureDelayGroups", "TaxiOut",
    "ArrDelay", "ArrDelayMinutes", "ArrDel15", "ArrivalDelayGroups", "TaxiIn",
    "Cancelled", "CancellationCode", "Diverted",
    "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Flights", "Distance", "DistanceGroup",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
    "DivAirportLandings", "DivReachedDest", "DivActualElapsedTime", "DivArrDelay", "DivDistance",
    "DQ_Cancelled_Status", "DQ_Diverted_Status",
]

INSERT_COLUMNS = [
    "FlightKey", "FlightDateKey", "AirlineKey", "OriginAirportKey", "DestinationAirportKey",
    "CRSDepTimeKey", "DepTimeKey", "CRSArrTimeKey", "ArrTimeKey",
    "SourceFile", "SourceMonth", "SourceRowNumber",
    "FlightNumberReportingAirline", "TailNumber",
    "DepDelay", "DepDelayMinutes", "DepDel15", "DepartureDelayGroups", "TaxiOut",
    "ArrDelay", "ArrDelayMinutes", "ArrDel15", "ArrivalDelayGroups", "TaxiIn",
    "Cancelled", "CancellationCode", "Diverted",
    "CRSElapsedTime", "ActualElapsedTime", "AirTime", "FlightCount", "Distance", "DistanceGroup",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
    "DivAirportLandings", "DivReachedDest", "DivActualElapsedTime", "DivArrDelay", "DivDistance",
    "DQ_FlightDate_Status", "DQ_CRSDepTime_Status", "DQ_DepTime_Status",
    "DQ_CRSArrTime_Status", "DQ_ArrTime_Status", "DQ_Cancelled_Status", "DQ_Diverted_Status",
]

# Source numeric column -> (nullable, signed, SQL maximum magnitude / range).
NUMERIC_RULES: dict[str, tuple[bool, int, int]] = {
    # signed INT
    "DepDelay": (True, -2_147_483_648, 2_147_483_647),
    "ArrDelay": (True, -2_147_483_648, 2_147_483_647),
    # unsigned INT
    "DepDelayMinutes": (True, 0, 4_294_967_295),
    "ArrDelayMinutes": (True, 0, 4_294_967_295),
    "CarrierDelay": (True, 0, 4_294_967_295),
    "WeatherDelay": (True, 0, 4_294_967_295),
    "NASDelay": (True, 0, 4_294_967_295),
    "SecurityDelay": (True, 0, 4_294_967_295),
    "LateAircraftDelay": (True, 0, 4_294_967_295),
    # signed SMALLINT
    "DepartureDelayGroups": (True, -32_768, 32_767),
    "ArrivalDelayGroups": (True, -32_768, 32_767),
    "CRSElapsedTime": (True, -32_768, 32_767),
    "DivActualElapsedTime": (True, -32_768, 32_767),
    "DivArrDelay": (True, -32_768, 32_767),
    # unsigned SMALLINT
    "TaxiOut": (True, 0, 65_535),
    "TaxiIn": (True, 0, 65_535),
    "ActualElapsedTime": (True, 0, 65_535),
    "AirTime": (True, 0, 65_535),
    "Distance": (False, 0, 65_535),
    "DivDistance": (True, 0, 65_535),
    # TINYINT fields
    "DepDel15": (True, 0, 255),
    "ArrDel15": (True, 0, 255),
    "DistanceGroup": (False, 0, 255),
    "Flights": (False, 0, 255),
    "DivAirportLandings": (True, 0, 255),
    "DivReachedDest": (True, 0, 255),
    "Cancelled": (False, 0, 255),
    "Diverted": (False, 0, 255),
}


def say(message: str) -> None:
    print(message, flush=True)


def clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def require_file(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"Required file not found: {path}")


def parse_int_cell(raw: Any, field: str, *, nullable: bool, minimum: int, maximum: int, context: str) -> int | None:
    text = clean(raw)
    if text == "":
        if nullable:
            return None
        raise ValueError(f"Required numeric field {field} is blank at {context}")
    try:
        value_decimal = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid numeric value for {field} at {context}: {text!r}") from exc
    if not value_decimal.is_finite() or value_decimal != value_decimal.to_integral_value():
        raise ValueError(f"Non-integral numeric value for {field} at {context}: {text!r}")
    value = int(value_decimal)
    if value < minimum or value > maximum:
        raise ValueError(f"{field} out of target SQL range at {context}: {value}")
    return value


def parse_date_key(raw: Any, status: Any, context: str) -> tuple[date, int]:
    value = clean(raw)
    dq = clean(status)
    if dq != "VALID":
        raise ValueError(f"Unexpected DQ_FlightDate_Status={dq!r} at {context}")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid FlightDate_Clean={value!r} at {context}") from exc
    if parsed.year != 2025:
        raise ValueError(f"FlightDate_Clean outside 2025 at {context}: {value}")
    return parsed, int(parsed.strftime("%Y%m%d"))


def parse_time_key(raw: Any, status: Any, field: str, context: str) -> tuple[int, str]:
    """Return TimeKey (MinuteOfDay+1; 0=Unknown) and normalized status.

    Valid clean times are four-digit HHMM strings. 2400 maps to TimeKey 1441.
    MISSING must have a blank clean value and maps to Unknown TimeKey 0.
    """
    value = clean(raw)
    dq = clean(status)
    if dq == "MISSING":
        if value != "":
            raise ValueError(f"{field} is nonblank while DQ status is MISSING at {context}: {value!r}")
        return 0, dq
    if dq == "VALID_2400":
        if value != "2400":
            raise ValueError(f"{field} status VALID_2400 but value is {value!r} at {context}")
        return 1441, dq
    if dq != "VALID":
        raise ValueError(f"Unexpected {field} DQ status {dq!r} at {context}")
    if not re.fullmatch(r"\d{4}", value):
        raise ValueError(f"{field} must be four-digit HHMM at {context}: {value!r}")
    hour = int(value[:2])
    minute = int(value[2:])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"{field} invalid HHMM at {context}: {value!r}")
    minute_of_day = hour * 60 + minute
    return minute_of_day + 1, dq


def load_month_manifest() -> list[dict[str, Any]]:
    require_file(FLIGHTKEY_MANIFEST)
    items: list[dict[str, Any]] = []
    with FLIGHTKEY_MANIFEST.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        expected_header = {"SourceMonth", "SourceFile", "RowsProcessed", "FirstFlightKey", "LastFlightKey"}
        if not reader.fieldnames or not expected_header.issubset(set(reader.fieldnames)):
            raise ValueError(f"Unexpected STEP 29C2 FlightKey manifest header: {reader.fieldnames}")
        for row in reader:
            month = clean(row["SourceMonth"])
            if month not in EXPECTED_MONTH_ROWS:
                raise ValueError(f"Unexpected SourceMonth in FlightKey manifest: {month!r}")
            item = {
                "month": month,
                "manifest_source_file": clean(row["SourceFile"]),
                "rows": int(clean(row["RowsProcessed"])),
                "first_key": int(clean(row["FirstFlightKey"])),
                "last_key": int(clean(row["LastFlightKey"])),
                "clean_path": BTS_DIR / f"BTS_CLEAN_2025_{month}.csv",
                "source_file_value": f"BTS_2025_{month}.csv",
            }
            if item["rows"] != EXPECTED_MONTH_ROWS[month]:
                raise ValueError(f"Manifest row count mismatch for month {month}: {item['rows']} != {EXPECTED_MONTH_ROWS[month]}")
            if item["manifest_source_file"] != f"BTS_CLEAN_2025_{month}.csv":
                raise ValueError(f"Manifest source file mismatch for month {month}: {item['manifest_source_file']!r}")
            items.append(item)
    items.sort(key=lambda x: x["month"])
    if len(items) != 12 or [x["month"] for x in items] != [f"{i:02d}" for i in range(1, 13)]:
        raise ValueError("FlightKey manifest must contain exactly months 01..12")
    expected_first = 1
    for item in items:
        if item["first_key"] != expected_first:
            raise ValueError(f"FlightKey range is not contiguous at month {item['month']}: {item['first_key']} != {expected_first}")
        if item["last_key"] != item["first_key"] + item["rows"] - 1:
            raise ValueError(f"FlightKey last-key mismatch in month {item['month']}")
        expected_first = item["last_key"] + 1
        require_file(item["clean_path"])
    if expected_first - 1 != EXPECTED_ROWS:
        raise ValueError(f"Manifest total FlightKey rows mismatch: {expected_first - 1:,} != {EXPECTED_ROWS:,}")
    return items


def load_airport_code_source_map() -> dict[str, int]:
    require_file(AIRPORT_MAPPING_FILE)
    result: dict[str, int] = {}
    with AIRPORT_MAPPING_FILE.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row_number, row in enumerate(reader, start=2):
            code = clean(row.get("IATACode")).upper()
            airport_id = parse_int_cell(row.get("AirportID"), "AirportID", nullable=False,
                                        minimum=1, maximum=4_294_967_295,
                                        context=f"{AIRPORT_MAPPING_FILE.name}:{row_number}")
            if not re.fullmatch(r"[A-Z0-9]{3}", code):
                raise ValueError(f"Invalid IATA airport code in resolved mapping at row {row_number}: {code!r}")
            if code in result:
                raise ValueError(f"Duplicate IATA code in resolved mapping: {code}")
            result[code] = int(airport_id)
    if len(result) != EXPECTED_AIRPORT_CODES:
        raise ValueError(f"Expected {EXPECTED_AIRPORT_CODES} resolved airport codes; found {len(result)}")
    return result


def file_signature(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {"name": path.name, "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def get_header_and_indexes(path: Path) -> tuple[list[str], dict[str, int]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise ValueError(f"Empty BTS CLEAN file: {path}") from exc
    if len(header) != 154 or len(set(header)) != len(header):
        raise ValueError(f"Unexpected BTS CLEAN header width/duplicates in {path.name}: {len(header)}")
    missing = [name for name in REQUIRED_COLUMNS if name not in header]
    if missing:
        raise ValueError(f"Missing required BTS CLEAN columns in {path.name}: {missing}")
    return header, {name: header.index(name) for name in REQUIRED_COLUMNS}


def build_record(row: list[str], ix: dict[str, int], month_item: dict[str, Any],
                 flight_key: int, db_maps: dict[str, dict[Any, Any]] | None,
                 counters: Counter[str], context: str) -> tuple[Any, ...]:
    """Validate one source row and map it to the exact Fact_Flight INSERT order."""
    def v(name: str) -> str:
        return clean(row[ix[name]])

    expected_source_file = month_item["source_file_value"]
    source_file = v("SourceFile")
    source_month = v("SourceMonth")
    if source_file != expected_source_file:
        raise ValueError(f"SourceFile mismatch at {context}: {source_file!r} != {expected_source_file!r}")
    if source_month != month_item["month"]:
        raise ValueError(f"SourceMonth mismatch at {context}: {source_month!r} != {month_item['month']!r}")
    if len(source_file) > 32:
        raise ValueError(f"SourceFile exceeds VARCHAR(32) at {context}")
    source_row_number = parse_int_cell(v("SourceRowNumber"), "SourceRowNumber", nullable=False,
                                       minimum=1, maximum=4_294_967_295, context=context)

    dot_id = parse_int_cell(v("DOT_ID_Reporting_Airline"), "DOT_ID_Reporting_Airline",
                            nullable=False, minimum=1, maximum=4_294_967_295, context=context)
    reporting_code = v("Reporting_Airline")
    iata_airline_code = v("IATA_CODE_Reporting_Airline")
    if not reporting_code or len(reporting_code) > 2 or not iata_airline_code or len(iata_airline_code) > 2:
        raise ValueError(f"Invalid airline code at {context}: {reporting_code!r}/{iata_airline_code!r}")

    flight_number = v("Flight_Number_Reporting_Airline")
    if not flight_number or len(flight_number) > 8:
        raise ValueError(f"Flight number blank or exceeds VARCHAR(8) at {context}: {flight_number!r}")
    tail_number = v("Tail_Number") or None
    if tail_number is not None and len(tail_number) > 8:
        raise ValueError(f"Tail_Number exceeds VARCHAR(8) at {context}")

    origin = v("Origin").upper()
    destination = v("Dest").upper()
    if len(origin) != 3 or len(destination) != 3:
        raise ValueError(f"Invalid Origin/Dest IATA code at {context}: {origin!r}/{destination!r}")

    flight_date, date_key = parse_date_key(v("FlightDate_Clean"), v("DQ_FlightDate_Status"), context)
    if flight_date.month != int(month_item["month"]):
        raise ValueError(f"FlightDate_Clean month disagrees with source month at {context}: {flight_date}")

    crs_dep_key, crs_dep_status = parse_time_key(v("CRSDepTime_Clean"), v("DQ_CRSDepTime_Status"), "CRSDepTime_Clean", context)
    dep_key, dep_status = parse_time_key(v("DepTime_Clean"), v("DQ_DepTime_Status"), "DepTime_Clean", context)
    crs_arr_key, crs_arr_status = parse_time_key(v("CRSArrTime_Clean"), v("DQ_CRSArrTime_Status"), "CRSArrTime_Clean", context)
    arr_key, arr_status = parse_time_key(v("ArrTime_Clean"), v("DQ_ArrTime_Status"), "ArrTime_Clean", context)
    counters["CRSDepTimeKey_Unknown"] += int(crs_dep_key == 0)
    counters["DepTimeKey_Unknown"] += int(dep_key == 0)
    counters["CRSArrTimeKey_Unknown"] += int(crs_arr_key == 0)
    counters["ArrTimeKey_Unknown"] += int(arr_key == 0)
    counters["CRSDepTimeKey_2400"] += int(crs_dep_key == 1441)
    counters["DepTimeKey_2400"] += int(dep_key == 1441)
    counters["CRSArrTimeKey_2400"] += int(crs_arr_key == 1441)
    counters["ArrTimeKey_2400"] += int(arr_key == 1441)

    numeric: dict[str, int | None] = {}
    for field, (nullable, minimum, maximum) in NUMERIC_RULES.items():
        numeric[field] = parse_int_cell(v(field), field, nullable=nullable,
                                        minimum=minimum, maximum=maximum, context=context)
    if numeric["Cancelled"] not in (0, 1):
        raise ValueError(f"Cancelled must be 0/1 at {context}")
    if numeric["Diverted"] not in (0, 1):
        raise ValueError(f"Diverted must be 0/1 at {context}")
    if numeric["DepDel15"] not in (None, 0, 1):
        raise ValueError(f"DepDel15 must be NULL/0/1 at {context}")
    if numeric["ArrDel15"] not in (None, 0, 1):
        raise ValueError(f"ArrDel15 must be NULL/0/1 at {context}")
    if numeric["DivReachedDest"] not in (None, 0, 1):
        raise ValueError(f"DivReachedDest must be NULL/0/1 at {context}")
    if numeric["Flights"] <= 0:
        raise ValueError(f"Flights must be positive at {context}")

    cancellation_code = v("CancellationCode") or None
    if cancellation_code is not None and len(cancellation_code) > 1:
        raise ValueError(f"CancellationCode exceeds CHAR(1) at {context}")

    dq_cancelled = v("DQ_Cancelled_Status")
    dq_diverted = v("DQ_Diverted_Status")
    if dq_cancelled not in ("VALID_0", "VALID_1") or dq_diverted not in ("VALID_0", "VALID_1"):
        raise ValueError(f"Unexpected Cancelled/Diverted DQ status at {context}: {dq_cancelled}/{dq_diverted}")
    if dq_cancelled != f"VALID_{numeric['Cancelled']}" or dq_diverted != f"VALID_{numeric['Diverted']}":
        raise ValueError(f"Cancelled/Diverted values disagree with DQ status at {context}")

    dq_values = {
        "DQ_FlightDate_Status": v("DQ_FlightDate_Status"),
        "DQ_CRSDepTime_Status": v("DQ_CRSDepTime_Status"),
        "DQ_DepTime_Status": v("DQ_DepTime_Status"),
        "DQ_CRSArrTime_Status": v("DQ_CRSArrTime_Status"),
        "DQ_ArrTime_Status": v("DQ_ArrTime_Status"),
        "DQ_Cancelled_Status": dq_cancelled,
        "DQ_Diverted_Status": dq_diverted,
    }
    for field, text in dq_values.items():
        if not text or len(text) > 24:
            raise ValueError(f"{field} blank or exceeds VARCHAR(24) at {context}: {text!r}")

    if db_maps is None:
        # PREVIEW uses harmless placeholder surrogate keys; no SQL is executed.
        # Airport-code membership and airline business-key consistency are
        # validated by the full-file preflight loop.
        airline_key = 1
        origin_key = 1
        destination_key = 1
        actual_date_key = date_key
        actual_time_keys = (crs_dep_key, dep_key, crs_arr_key, arr_key)
    else:
        try:
            airline_key = db_maps["airline"][dot_id]
            origin_key = db_maps["airport"][origin][0]
            destination_key = db_maps["airport"][destination][0]
            actual_date_key = db_maps["date"][flight_date]
            actual_time_keys = (
                db_maps["time"][crs_dep_key - 1] if crs_dep_key else 0,
                db_maps["time"][dep_key - 1] if dep_key else 0,
                db_maps["time"][crs_arr_key - 1] if crs_arr_key else 0,
                db_maps["time"][arr_key - 1] if arr_key else 0,
            )
        except KeyError as exc:
            raise ValueError(f"Missing MySQL dimension mapping for {exc} at {context}") from exc

    counters["RowsValidated"] += 1
    counters["CancelledRows"] += int(numeric["Cancelled"] == 1)
    counters["DivertedRows"] += int(numeric["Diverted"] == 1)
    counters[f"Month_{month_item['month']}_Rows"] += 1

    return (
        flight_key, actual_date_key, airline_key, origin_key, destination_key,
        *actual_time_keys,
        source_file, source_month, source_row_number,
        flight_number, tail_number,
        numeric["DepDelay"], numeric["DepDelayMinutes"], numeric["DepDel15"], numeric["DepartureDelayGroups"], numeric["TaxiOut"],
        numeric["ArrDelay"], numeric["ArrDelayMinutes"], numeric["ArrDel15"], numeric["ArrivalDelayGroups"], numeric["TaxiIn"],
        numeric["Cancelled"], cancellation_code, numeric["Diverted"],
        numeric["CRSElapsedTime"], numeric["ActualElapsedTime"], numeric["AirTime"], numeric["Flights"], numeric["Distance"], numeric["DistanceGroup"],
        numeric["CarrierDelay"], numeric["WeatherDelay"], numeric["NASDelay"], numeric["SecurityDelay"], numeric["LateAircraftDelay"],
        numeric["DivAirportLandings"], numeric["DivReachedDest"], numeric["DivActualElapsedTime"], numeric["DivArrDelay"], numeric["DivDistance"],
        dq_values["DQ_FlightDate_Status"], dq_values["DQ_CRSDepTime_Status"], dq_values["DQ_DepTime_Status"],
        dq_values["DQ_CRSArrTime_Status"], dq_values["DQ_ArrTime_Status"], dq_values["DQ_Cancelled_Status"], dq_values["DQ_Diverted_Status"],
    )



def preflight_source(manifest: list[dict[str, Any]], airport_codes: set[str]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    say("STEP 30E2 - BTS FLIGHT FACT PREFLIGHT")
    say("=" * 78)
    say(f"[1/2] Validating 12 BTS CLEAN monthly files against STEP 29C2 FlightKey manifest...")
    counters: Counter[str] = Counter()
    month_signatures: dict[str, dict[str, Any]] = {}
    distinct_airlines: dict[int, tuple[str, str]] = {}
    observed_airport_codes: set[str] = set()
    total_rows = 0

    for item in manifest:
        path = item["clean_path"]
        header, ix = get_header_and_indexes(path)
        file_rows = 0
        seen_source_row_numbers: set[int] = set()
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            next(reader)
            for csv_line_number, row in enumerate(reader, start=2):
                context = f"{path.name}:{csv_line_number}"
                if len(row) != len(header):
                    raise ValueError(f"Bad row width at {context}: expected {len(header)}, got {len(row)}")
                # Validate and map source-side fields. Preview keys are placeholders only.
                build_record(row, ix, item, item["first_key"] + file_rows, None, counters, context)
                source_row_number = int(clean(row[ix["SourceRowNumber"]]))
                if source_row_number in seen_source_row_numbers:
                    raise ValueError(f"Duplicate SourceRowNumber within source file at {context}: {source_row_number}")
                seen_source_row_numbers.add(source_row_number)
                dot_id = int(Decimal(clean(row[ix["DOT_ID_Reporting_Airline"]])))
                pair = (clean(row[ix["Reporting_Airline"]]), clean(row[ix["IATA_CODE_Reporting_Airline"]]))
                if dot_id in distinct_airlines and distinct_airlines[dot_id] != pair:
                    raise ValueError(f"DOT Airline ID {dot_id} has conflicting codes at {context}")
                distinct_airlines[dot_id] = pair
                origin = clean(row[ix["Origin"]]).upper()
                destination = clean(row[ix["Dest"]]).upper()
                observed_airport_codes.add(origin)
                observed_airport_codes.add(destination)
                if origin not in airport_codes or destination not in airport_codes:
                    raise ValueError(f"Origin/Dest airport code missing from resolved mapping at {context}: {origin}/{destination}")
                file_rows += 1
                total_rows += 1
        if file_rows != item["rows"]:
            raise ValueError(f"Monthly row count mismatch for {item['month']}: {file_rows:,} != {item['rows']:,}")
        month_signatures[item["month"]] = file_signature(path)
        say(f"      {path.name}: {file_rows:,} rows validated; FlightKey {item['first_key']:,}..{item['last_key']:,}")

    if total_rows != EXPECTED_ROWS:
        raise ValueError(f"Total BTS rows mismatch: {total_rows:,} != {EXPECTED_ROWS:,}")
    if len(distinct_airlines) != EXPECTED_AIRLINES:
        raise ValueError(f"Expected {EXPECTED_AIRLINES} distinct DOT Airline IDs; found {len(distinct_airlines)}")
    if observed_airport_codes != airport_codes:
        missing = sorted(observed_airport_codes - airport_codes)
        unused = sorted(airport_codes - observed_airport_codes)
        raise ValueError(f"Observed airport code/mapping set mismatch; missing={missing[:10]}, unused={unused[:10]}")
    if counters["CancelledRows"] != EXPECTED_CANCELLED or counters["DivertedRows"] != EXPECTED_DIVERTED:
        raise ValueError(f"Cancelled/Diverted totals mismatch: {counters['CancelledRows']:,}/{counters['DivertedRows']:,}")

    summary: dict[str, Any] = {
        "Step": "30E2", "Mode": "PREVIEW", "Result": "PASS", "TargetDatabase": DB_NAME,
        "TargetTable": "Fact_Flight", "BTSFilesValidated": len(manifest), "BTSRowsValidated": total_rows,
        "DistinctAirlines": len(distinct_airlines), "DistinctAirportCodesObserved": len(observed_airport_codes),
        "ExpectedFlightKeyMin": 1, "ExpectedFlightKeyMax": EXPECTED_ROWS,
        "CancelledRows": counters["CancelledRows"], "DivertedRows": counters["DivertedRows"],
        "CRSDepTimeUnknownRows": counters["CRSDepTimeKey_Unknown"],
        "DepTimeUnknownRows": counters["DepTimeKey_Unknown"],
        "CRSArrTimeUnknownRows": counters["CRSArrTimeKey_Unknown"],
        "ArrTimeUnknownRows": counters["ArrTimeKey_Unknown"],
        "CRSDepTime2400Rows": counters["CRSDepTimeKey_2400"],
        "DepTime2400Rows": counters["DepTimeKey_2400"],
        "CRSArrTime2400Rows": counters["CRSArrTimeKey_2400"],
        "ArrTime2400Rows": counters["ArrTimeKey_2400"],
        "MonthlyRows": {m: counters[f"Month_{m}_Rows"] for m in EXPECTED_MONTH_ROWS},
        "BatchSize": DEFAULT_BATCH_SIZE, "NoSourceFilesModified": True,
        "FlightKeyManifest": str(FLIGHTKEY_MANIFEST),
        "FlightKeyManifestRunID": BRIDGE_RUN_ID,
        "CheckpointFile": str(CHECKPOINT_FILE),
    }
    say("\nPREFLIGHT SUMMARY")
    say("-" * 78)
    for key, value in summary.items():
        say(f"{key}: {value}")
    say("\nSOURCE PREFLIGHT RESULT: PASS")
    return summary, month_signatures


def write_reports(summary: dict[str, Any], mode: str) -> tuple[Path, Path]:
    PHYSICAL_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    payload = dict(summary)
    payload["Mode"] = mode
    payload["RunID"] = run_id
    json_path = PHYSICAL_DIR / f"30E2_flight_fact_{mode.lower()}_{run_id}.json"
    txt_path = PHYSICAL_DIR / f"30E2_flight_fact_{mode.lower()}_{run_id}.txt"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    lines = [
        "STEP 30E2 - BTS FLIGHT FACT " + mode,
        "=" * 78,
        f"Result: {payload.get('Result')}",
        f"BTS files validated: {payload.get('BTSFilesValidated', 0):,}",
        f"BTS rows validated: {payload.get('BTSRowsValidated', 0):,}",
        f"Final database rows: {payload.get('FinalDatabaseRows', 'not executed')}",
        f"Distinct FlightKeys: {payload.get('FinalDistinctFlightKeys', 'not executed')}",
        f"Distinct source lineage keys: {payload.get('FinalDistinctLineageKeys', 'not executed')}",
        f"Cancelled rows: {payload.get('CancelledRows', 0):,}",
        f"Diverted rows: {payload.get('DivertedRows', 0):,}",
        f"FlightKey manifest: {FLIGHTKEY_MANIFEST}",
        "Source files: READ ONLY",
    ]
    if payload.get("DatabaseValidation"):
        lines += ["", "DATABASE VALIDATION"]
        for key, value in payload["DatabaseValidation"].items():
            lines.append(f"{key}: {value}")
    if payload.get("MonthlyDatabaseValidation"):
        lines += ["", "MONTHLY DATABASE VALIDATION"]
        for month, value in payload["MonthlyDatabaseValidation"].items():
            lines.append(f"{month}: {value}")
    lines += ["", f"JSON report: {json_path}"]
    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return txt_path, json_path


def checkpoint_journal_path(path: Path) -> Path:
    """Append-only checkpoint journal; avoids Windows replace/rename locking."""
    return path.with_name(path.stem + "_journal.jsonl")


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Persist a checkpoint snapshot without replacing an existing file.

    The previous implementation wrote a fixed .tmp file and called os.replace().
    On this Windows machine, replacement intermittently fails with WinError 5
    after MySQL has already committed the batch.  An append-only JSONL journal
    avoids replacing/renaming a locked checkpoint target.  Each line is a full
    self-contained snapshot.  A torn final line is ignored by the loader.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    journal = checkpoint_journal_path(path)
    encoded = (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
    fd = os.open(str(journal), os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o666)
    try:
        view = memoryview(encoded)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("Could not append complete checkpoint journal record")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


def _checkpoint_progress(payload: dict[str, Any], manifest_by_month: dict[str, dict[str, Any]],
                         month_signatures: dict[str, dict[str, Any]]) -> int:
    months = payload.get("Months", {})
    if not isinstance(months, dict):
        raise ValueError("Checkpoint Months must be a JSON object")
    total = 0
    for month, state in months.items():
        if month not in manifest_by_month or not isinstance(state, dict):
            raise ValueError(f"Checkpoint contains an unexpected month/state: {month!r}")
        rows = int(state.get("CompletedSourceRows", 0))
        item = manifest_by_month[month]
        if rows < 0 or rows > item["rows"]:
            raise ValueError(f"Checkpoint progress outside expected range for month {month}: {rows}")
        if rows:
            expected_last = item["first_key"] + rows - 1
            if int(state.get("LastCommittedFlightKey", -1)) != expected_last:
                raise ValueError(f"Checkpoint FlightKey does not match completed rows for month {month}")
            if state.get("SourceFileSignature") != month_signatures[month]:
                raise ValueError(f"Checkpoint source signature mismatch for month {month}")
        total += rows
    return total


def load_checkpoint(month_signatures: dict[str, dict[str, Any]], manifest: list[dict[str, Any]], batch_size: int) -> dict[str, Any]:
    fingerprint = {
        "BridgeRunID": BRIDGE_RUN_ID,
        "FlightKeyManifest": file_signature(FLIGHTKEY_MANIFEST),
        "AirportMapping": file_signature(AIRPORT_MAPPING_FILE),
        "BTSMonthlyFiles": month_signatures,
        "ExpectedRows": EXPECTED_ROWS,
        "BatchSize": batch_size,
    }
    manifest_by_month = {item["month"]: item for item in manifest}
    candidates: list[tuple[int, int, str, dict[str, Any]]] = []
    sequence = 0

    def consider(label: str, text: str) -> None:
        nonlocal sequence
        try:
            payload = json.loads(text)
        except (json.JSONDecodeError, UnicodeDecodeError):
            say(f"[CHECKPOINT] Ignoring incomplete/corrupt snapshot: {label}")
            return
        if payload.get("SchemaVersion") != 1:
            raise ValueError(f"Unsupported STEP 30E2 checkpoint schema version in {label}")
        if payload.get("Fingerprint") != fingerprint:
            raise ValueError(f"Checkpoint fingerprint differs from current sources/manifest in {label}; preserve and investigate")
        progress = _checkpoint_progress(payload, manifest_by_month, month_signatures)
        sequence += 1
        candidates.append((progress, sequence, label, payload))

    # Recover from the canonical file and the .tmp left behind by the old
    # os.replace() implementation. Prefer whichever valid snapshot records the
    # greatest committed source-row prefix.
    for candidate_path in (CHECKPOINT_FILE, CHECKPOINT_FILE.with_suffix(CHECKPOINT_FILE.suffix + ".tmp")):
        if candidate_path.is_file():
            try:
                consider(str(candidate_path), candidate_path.read_text(encoding="utf-8"))
            except OSError as exc:
                say(f"[CHECKPOINT] Could not read {candidate_path.name}: {exc}")

    journal = checkpoint_journal_path(CHECKPOINT_FILE)
    if journal.is_file():
        with journal.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                consider(f"{journal.name}:line {line_number}", line)

    if candidates:
        progress, _, label, payload = max(candidates, key=lambda item: (item[0], item[1]))
        payload.setdefault("Months", {})
        say(f"[CHECKPOINT] Restored latest valid snapshot: {label}; completed source rows={progress:,}")
        say(f"[CHECKPOINT] Future updates will append to: {journal}")
        return payload

    say("[CHECKPOINT] No prior valid snapshot found; starting with an empty checkpoint journal.")
    say(f"[CHECKPOINT] Future updates will append to: {journal}")
    return {"SchemaVersion": 1, "Fingerprint": fingerprint, "Months": {}}


def connect_mysql(args: argparse.Namespace):
    try:
        import mysql.connector  # type: ignore
    except ImportError as exc:
        raise RuntimeError("mysql-connector-python missing. Run: python -m pip install mysql-connector-python") from exc
    host = args.host or os.getenv("FLIGHT_DW_DB_HOST", "127.0.0.1")
    port = args.port or int(os.getenv("FLIGHT_DW_DB_PORT", "3306"))
    user = args.user or os.getenv("FLIGHT_DW_DB_USER", "root")
    password = os.getenv("FLIGHT_DW_DB_PASSWORD")
    if password is None:
        password = getpass.getpass(f"MySQL password for {user}@{host}:{port}: ")
    connection = mysql.connector.connect(
        host=host, port=port, user=user, password=password, database=DB_NAME,
        charset="utf8mb4", collation="utf8mb4_0900_ai_ci", autocommit=False,
        connection_timeout=30,
    )
    return connection


def load_dimension_maps(cursor) -> dict[str, dict[Any, Any]]:
    say("[DB] Loading and validating dimension surrogate-key maps...")
    cursor.execute("SELECT AirlineKey, DOT_ID_Reporting_Airline, ReportingAirlineCode, IATAReportingAirlineCode FROM Dim_Airline")
    airline: dict[int, int] = {}
    airline_pairs: dict[int, tuple[str, str]] = {}
    for key, dot, reporting, iata in cursor.fetchall():
        airline[int(dot)] = int(key)
        airline_pairs[int(dot)] = (str(reporting).strip(), str(iata).strip())
    if len(airline) != EXPECTED_AIRLINES:
        raise ValueError(f"Dim_Airline expected {EXPECTED_AIRLINES} rows; found {len(airline)}")

    cursor.execute("SELECT BTSAirportCode, AirportKey, OurAirportsID FROM Map_BTS_Airport")
    airport: dict[str, tuple[int, int]] = {}
    for code, key, our_id in cursor.fetchall():
        airport[str(code).strip().upper()] = (int(key), int(our_id))
    if len(airport) != EXPECTED_AIRPORT_CODES:
        raise ValueError(f"Map_BTS_Airport expected {EXPECTED_AIRPORT_CODES} rows; found {len(airport)}")

    cursor.execute("SELECT CalendarDate, DateKey FROM Dim_Date WHERE IsUnknown = 0")
    date_map = {d: int(k) for d, k in cursor.fetchall()}
    if len(date_map) != 369:
        raise ValueError(f"Dim_Date expected 369 calendar dates excluding Unknown; found {len(date_map)}")

    cursor.execute("SELECT MinuteOfDay, TimeKey FROM Dim_Time WHERE IsUnknown = 0")
    time_map = {int(m): int(k) for m, k in cursor.fetchall()}
    if len(time_map) != 1441 or time_map.get(1440) != 1441:
        raise ValueError("Dim_Time does not contain all 1,441 minute positions plus the 24:00 sentinel")

    cursor.execute("SELECT COUNT(*) FROM Dim_Airport")
    airport_rows = int(cursor.fetchone()[0])
    cursor.execute("SELECT COUNT(*) FROM Fact_Airport_Weather_Hourly")
    weather_rows = int(cursor.fetchone()[0])
    if airport_rows != 86_335:
        raise ValueError(f"Dim_Airport expected 86,335 rows; found {airport_rows}")
    if weather_rows != 3_083_520:
        raise ValueError(f"Weather Fact must be loaded before Flight Fact; expected 3,083,520 rows, found {weather_rows}")

    say(f"      Dim_Airline keys: {len(airline):,}")
    say(f"      Map_BTS_Airport keys: {len(airport):,}")
    say(f"      Dim_Date keys: {len(date_map):,}")
    say(f"      Dim_Time keys (excluding Unknown): {len(time_map):,}")
    say(f"      Weather Fact prerequisite rows: {weather_rows:,}")
    return {"airline": airline, "airline_pairs": airline_pairs, "airport": airport, "date": date_map, "time": time_map}


def make_insert_sql() -> str:
    cols = ", ".join(INSERT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(INSERT_COLUMNS))
    updates = [
        # Fail closed if either unique key resolves to a different source/FlightKey pair.
        "FlightKey = IF(FlightKey = VALUES(FlightKey), FlightKey, NULL)",
        "SourceFile = IF(FlightKey = VALUES(FlightKey) AND SourceFile = VALUES(SourceFile) AND SourceRowNumber = VALUES(SourceRowNumber), SourceFile, NULL)",
    ]
    protected = {"FlightKey", "SourceFile", "SourceRowNumber", "CreatedAt"}
    for col in INSERT_COLUMNS:
        if col not in protected:
            updates.append(f"{col} = VALUES({col})")
    return f"INSERT INTO Fact_Flight ({cols}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE " + ", ".join(updates)


def validate_month_target(cursor, item: dict[str, Any], checkpoint_rows: int, batch_size: int) -> int:
    month = item["month"]
    cursor.execute(
        "SELECT COUNT(*), MIN(FlightKey), MAX(FlightKey), COUNT(DISTINCT FlightKey), "
        "COUNT(DISTINCT SourceRowNumber), MIN(SourceFile), MAX(SourceFile), MIN(SourceMonth), MAX(SourceMonth) "
        "FROM Fact_Flight WHERE SourceMonth = %s",
        (month,),
    )
    count, min_key, max_key, distinct_keys, distinct_rows, min_file, max_file, min_month, max_month = cursor.fetchone()
    count = int(count)
    if count == 0:
        if checkpoint_rows != 0:
            raise ValueError(f"Checkpoint says month {month} has {checkpoint_rows:,} committed rows, but DB has 0")
        return 0
    expected_file = item["source_file_value"]
    if min_file != expected_file or max_file != expected_file or min_month != month or max_month != month:
        raise ValueError(f"Existing Fact_Flight rows have unexpected source lineage for month {month}")
    if int(distinct_keys) != count or int(distinct_rows) != count:
        raise ValueError(f"Existing month {month} has duplicate FlightKey or SourceRowNumber lineage")
    if int(min_key) != item["first_key"] or int(max_key) != item["first_key"] + count - 1:
        raise ValueError(f"Existing FlightKey range is not a contiguous prefix for month {month}: {min_key}..{max_key}, rows={count}")
    if checkpoint_rows == 0:
        if count > batch_size:
            raise ValueError(f"Month {month} has {count:,} rows but no checkpoint exists; refusing an unverified resume")
    else:
        if count < checkpoint_rows or count > checkpoint_rows + batch_size:
            raise ValueError(f"Month {month} DB/checkpoint row mismatch: DB={count:,}, checkpoint={checkpoint_rows:,}")
    return count


def verify_airline_source_pair(row: list[str], ix: dict[str, int], maps: dict[str, dict[Any, Any]], dot_id: int, context: str) -> None:
    expected_pair = maps["airline_pairs"].get(dot_id)
    actual_pair = (clean(row[ix["Reporting_Airline"]]), clean(row[ix["IATA_CODE_Reporting_Airline"]]))
    if expected_pair != actual_pair:
        raise ValueError(f"Airline business-key/code mismatch at {context}: DOT_ID={dot_id}, {actual_pair} != {expected_pair}")


def execute_month(connection, cursor, item: dict[str, Any], ix: dict[str, int], maps: dict[str, dict[Any, Any]],
                  checkpoint: dict[str, Any], batch_size: int) -> dict[str, Any]:
    month = item["month"]
    month_state = checkpoint["Months"].get(month, {})
    resume_rows = int(month_state.get("CompletedSourceRows", 0))
    db_rows = validate_month_target(cursor, item, resume_rows, batch_size)
    if db_rows > resume_rows:
        say(f"      [RESUME] Month {month}: {db_rows - resume_rows:,} committed row(s) may be replayed after last checkpoint.")
    if resume_rows == item["rows"] and db_rows == item["rows"]:
        say(f"      [CHECKPOINT] Month {month}: fully loaded and verified; skipping.")
        return {"RowsLoadedThisRun": 0, "RowsPresent": db_rows, "Status": "VERIFIED_CHECKPOINT_SKIP"}

    insert_sql = make_insert_sql()
    batch: list[tuple[Any, ...]] = []
    processed_in_file = 0
    loaded_this_run = 0
    counters: Counter[str] = Counter()
    path = item["clean_path"]
    header, current_ix = get_header_and_indexes(path)
    # The index layout must remain the same as it was during full preflight.
    if current_ix != ix:
        raise ValueError(f"BTS CLEAN header index map changed after preflight for {path.name}")

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        next(reader)
        for csv_line_number, row in enumerate(reader, start=2):
            context = f"{path.name}:{csv_line_number}"
            if len(row) != len(header):
                raise ValueError(f"Bad row width during execute at {context}")
            flight_key = item["first_key"] + processed_in_file
            # Transform every row (including skipped prefix rows) to revalidate file stability.
            dot_id = int(Decimal(clean(row[ix["DOT_ID_Reporting_Airline"]])))
            verify_airline_source_pair(row, ix, maps, dot_id, context)
            record = build_record(row, ix, item, flight_key, maps, counters, context)
            processed_in_file += 1
            if processed_in_file <= resume_rows:
                continue
            batch.append(record)
            if len(batch) >= batch_size:
                cursor.executemany(insert_sql, batch)
                connection.commit()
                loaded_this_run += len(batch)
                checkpoint["Months"][month] = {
                    "CompletedSourceRows": processed_in_file,
                    "LastCommittedFlightKey": item["first_key"] + processed_in_file - 1,
                    "ExpectedRows": item["rows"],
                    "SourceFileSignature": file_signature(path),
                }
                atomic_write_json(CHECKPOINT_FILE, checkpoint)
                batch.clear()
                if loaded_this_run % (batch_size * 25) == 0:
                    say(f"      Fact_Flight {month}: {processed_in_file:,}/{item['rows']:,} source rows processed; {loaded_this_run:,} newly committed this run")

    if batch:
        cursor.executemany(insert_sql, batch)
        connection.commit()
        loaded_this_run += len(batch)
        batch.clear()
        checkpoint["Months"][month] = {
            "CompletedSourceRows": processed_in_file,
            "LastCommittedFlightKey": item["last_key"],
            "ExpectedRows": item["rows"],
            "SourceFileSignature": file_signature(path),
        }
        atomic_write_json(CHECKPOINT_FILE, checkpoint)
    elif processed_in_file == resume_rows and processed_in_file == item["rows"]:
        # Nothing new needed; existing fully loaded month.
        pass

    if processed_in_file != item["rows"]:
        raise ValueError(f"Execute pass row count mismatch for month {month}: {processed_in_file:,} != {item['rows']:,}")

    cursor.execute(
        "SELECT COUNT(*), MIN(FlightKey), MAX(FlightKey), COUNT(DISTINCT FlightKey), "
        "COUNT(DISTINCT SourceRowNumber), MIN(SourceFile), MAX(SourceFile) "
        "FROM Fact_Flight WHERE SourceMonth = %s",
        (month,),
    )
    count, min_key, max_key, distinct_keys, distinct_rows, min_file, max_file = cursor.fetchone()
    expected_file = item["source_file_value"]
    if (int(count) != item["rows"] or int(min_key) != item["first_key"] or int(max_key) != item["last_key"]
            or int(distinct_keys) != item["rows"] or int(distinct_rows) != item["rows"]
            or min_file != expected_file or max_file != expected_file):
        raise RuntimeError(f"Post-load month validation failed for {month}: count/range/lineage mismatch")

    checkpoint["Months"][month] = {
        "CompletedSourceRows": item["rows"],
        "LastCommittedFlightKey": item["last_key"],
        "ExpectedRows": item["rows"],
        "SourceFileSignature": file_signature(path),
        "DatabaseValidation": "PASS",
    }
    atomic_write_json(CHECKPOINT_FILE, checkpoint)
    say(f"      Fact_Flight {month}: {item['rows']:,}/{item['rows']:,} rows committed and verified; FlightKey {item['first_key']:,}..{item['last_key']:,}")
    return {"RowsLoadedThisRun": loaded_this_run, "RowsPresent": int(count), "Status": "PASS"}


def final_database_validation(cursor, manifest: list[dict[str, Any]]) -> dict[str, Any]:
    validation: dict[str, Any] = {}
    cursor.execute("""
        SELECT COUNT(*), MIN(FlightKey), MAX(FlightKey), COUNT(DISTINCT FlightKey),
               COUNT(DISTINCT SourceFile, SourceRowNumber),
               COUNT(DISTINCT SourceMonth), SUM(Cancelled = 1), SUM(Diverted = 1),
               SUM(CRSDepTimeKey = 0), SUM(DepTimeKey = 0),
               SUM(CRSArrTimeKey = 0), SUM(ArrTimeKey = 0),
               SUM(CRSDepTimeKey = 1441), SUM(DepTimeKey = 1441),
               SUM(CRSArrTimeKey = 1441), SUM(ArrTimeKey = 1441)
        FROM Fact_Flight
    """)
    row = cursor.fetchone()
    names = [
        "FinalDatabaseRows", "MinFlightKey", "MaxFlightKey", "FinalDistinctFlightKeys",
        "FinalDistinctLineageKeys", "DistinctSourceMonths", "CancelledRows", "DivertedRows",
        "CRSDepTimeUnknownRows", "DepTimeUnknownRows", "CRSArrTimeUnknownRows", "ArrTimeUnknownRows",
        "CRSDepTime2400Rows", "DepTime2400Rows", "CRSArrTime2400Rows", "ArrTime2400Rows",
    ]
    for name, value in zip(names, row):
        validation[name] = None if value is None else int(value)
    expected = {
        "FinalDatabaseRows": EXPECTED_ROWS,
        "MinFlightKey": 1,
        "MaxFlightKey": EXPECTED_ROWS,
        "FinalDistinctFlightKeys": EXPECTED_ROWS,
        "FinalDistinctLineageKeys": EXPECTED_ROWS,
        "DistinctSourceMonths": 12,
        "CancelledRows": EXPECTED_CANCELLED,
        "DivertedRows": EXPECTED_DIVERTED,
        "CRSDepTimeUnknownRows": 0,
        "DepTimeUnknownRows": 98_170,
        "CRSArrTimeUnknownRows": 0,
        "ArrTimeUnknownRows": 104_608,
        "CRSDepTime2400Rows": 1,
        "DepTime2400Rows": 747,
        "CRSArrTime2400Rows": 2,
        "ArrTime2400Rows": 3_720,
    }
    failures = {key: (validation.get(key), target) for key, target in expected.items() if validation.get(key) != target}
    if failures:
        raise RuntimeError(f"Fact_Flight global read-back validation failed: {failures}")

    monthly: dict[str, Any] = {}
    for item in manifest:
        cursor.execute(
            "SELECT COUNT(*), MIN(FlightKey), MAX(FlightKey), COUNT(DISTINCT SourceRowNumber) "
            "FROM Fact_Flight WHERE SourceMonth = %s",
            (item["month"],),
        )
        count, min_key, max_key, distinct_rows = cursor.fetchone()
        actual = {"Rows": int(count), "MinFlightKey": int(min_key), "MaxFlightKey": int(max_key), "DistinctSourceRows": int(distinct_rows)}
        wanted = {"Rows": item["rows"], "MinFlightKey": item["first_key"], "MaxFlightKey": item["last_key"], "DistinctSourceRows": item["rows"]}
        if actual != wanted:
            raise RuntimeError(f"Monthly Fact_Flight validation failed for {item['month']}: {actual} != {wanted}")
        monthly[item["month"]] = actual
    validation["MonthlyValidation"] = monthly
    return validation


def execute_load(args: argparse.Namespace, summary: dict[str, Any], manifest: list[dict[str, Any]],
                 month_signatures: dict[str, dict[str, Any]], header_indexes: dict[str, dict[str, int]]) -> dict[str, Any]:
    connection = connect_mysql(args)
    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT DATABASE(), VERSION()")
        database, version = cursor.fetchone()
        if database != DB_NAME or not str(version).startswith("8.0."):
            raise RuntimeError(f"Unexpected MySQL target/version: {database}/{version}")
        say(f"Connected database: {database}; MySQL: {version}")
        maps = load_dimension_maps(cursor)

        # Verify source airport codes and airline business-key/code combinations against loaded dimensions.
        source_airport_map = load_airport_code_source_map()
        cursor.execute("SELECT OurAirportsID, AirportKey FROM Dim_Airport")
        db_airport_ids = {int(our_id): int(key) for our_id, key in cursor.fetchall()}
        for code, our_id in source_airport_map.items():
            if code not in maps["airport"] or maps["airport"][code][1] != our_id:
                raise ValueError(f"Airport mapping mismatch in MySQL for {code}: expected OurAirportsID {our_id}")
            if our_id not in db_airport_ids or db_airport_ids[our_id] != maps["airport"][code][0]:
                raise ValueError(f"Dim_Airport surrogate-key mismatch for {code}/{our_id}")

        # Verify Fact_Flight table exists and there are no rows from an unplanned source month.
        cursor.execute("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME='fact_flight'", (DB_NAME,))
        if int(cursor.fetchone()[0]) != 1:
            raise RuntimeError("Target table flight_dw_2025.Fact_Flight does not exist")
        cursor.execute("SELECT COUNT(*) FROM Fact_Flight WHERE SourceMonth NOT IN ('01','02','03','04','05','06','07','08','09','10','11','12')")
        if int(cursor.fetchone()[0]) != 0:
            raise RuntimeError("Fact_Flight contains rows with unexpected SourceMonth values")

        checkpoint = load_checkpoint(month_signatures, manifest, args.batch_size)
        say(f"[DB] Checkpoint recovery source: {CHECKPOINT_FILE} and {CHECKPOINT_FILE}.tmp")
        say(f"[DB] Append-only checkpoint journal: {checkpoint_journal_path(CHECKPOINT_FILE)}")
        monthly_results: dict[str, Any] = {}
        for item in manifest:
            # Revalidate each month against the database/checkpoint before processing it.
            checkpoint_rows = int(checkpoint["Months"].get(item["month"], {}).get("CompletedSourceRows", 0))
            db_count = validate_month_target(cursor, item, checkpoint_rows, args.batch_size)
            if db_count > checkpoint_rows:
                say(f"[RESUME] Month {item['month']}: replaying {db_count - checkpoint_rows:,} committed row(s) after checkpoint.")
            month_result = execute_month(connection, cursor, item, header_indexes[item["month"]], maps, checkpoint, args.batch_size)
            monthly_results[item["month"]] = month_result

        validation = final_database_validation(cursor, manifest)
        summary.update({
            "Mode": "EXECUTE", "Result": "PASS", "MySQLVersion": version,
            "FinalDatabaseRows": validation["FinalDatabaseRows"],
            "FinalDistinctFlightKeys": validation["FinalDistinctFlightKeys"],
            "FinalDistinctLineageKeys": validation["FinalDistinctLineageKeys"],
            "MonthlyDatabaseValidation": validation.pop("MonthlyValidation"),
            "DatabaseValidation": validation,
            "MonthlyLoadResults": monthly_results,
            "CheckpointFile": str(CHECKPOINT_FILE),
            "CheckpointJournalFile": str(checkpoint_journal_path(CHECKPOINT_FILE)),
            "NoSourceFilesModified": True,
        })
        connection.commit()
        return summary
    except Exception:
        connection.rollback()
        raise
    finally:
        if cursor is not None:
            cursor.close()
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="STEP 30E2 - load BTS CLEAN rows into Fact_Flight")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--preview", action="store_true", help="Full source validation only; no MySQL access (default)")
    group.add_argument("--execute", action="store_true", help="Load Fact_Flight into MySQL 8.0")
    parser.add_argument("--host", default=None, help="MySQL host (default 127.0.0.1)")
    parser.add_argument("--port", type=int, default=None, help="MySQL port (default 3306)")
    parser.add_argument("--user", default=None, help="MySQL user (default root)")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Rows per transaction (default 1000; max 5000)")
    args = parser.parse_args()
    if args.batch_size < 100 or args.batch_size > 5_000:
        say("ERROR: --batch-size must be between 100 and 5000")
        return 2
    if not (ROOT / "04_CLEAN_DATA" / "BTS").is_dir() or not (ROOT / "07_GOLD_DATA").is_dir():
        say("ERROR: Run this script from project root D:\\DATA_WAREHOUSE")
        return 2

    try:
        manifest = load_month_manifest()
        airport_map = load_airport_code_source_map()
        summary, month_signatures = preflight_source(manifest, set(airport_map))
        header_indexes = {}
        for item in manifest:
            _, header_indexes[item["month"]] = get_header_and_indexes(item["clean_path"])
        summary["BatchSize"] = args.batch_size

        if args.execute:
            say("\nSTEP 30E2 - EXECUTE FLIGHT FACT LOAD")
            say("=" * 78)
            result = execute_load(args, summary, manifest, month_signatures, header_indexes)
            txt_path, json_path = write_reports(result, "EXECUTE")
            say("\nSTEP 30E2 EXECUTE RESULT: PASS")
        else:
            summary["MySQLRowsInsertedOrUpdated"] = 0
            summary["Message"] = "Source preflight only; no MySQL connection or writes performed."
            txt_path, json_path = write_reports(summary, "PREVIEW")
            say("\nSTEP 30E2 PREVIEW RESULT: PASS")
            say("No MySQL rows were inserted or updated.")

        say(f"Text report: {txt_path}")
        say(f"JSON report: {json_path}")
        say("STEP 30E2 COMPLETE")
        return 0
    except Exception as exc:
        say("\nSTEP 30E2 RESULT: FAIL")
        say(f"ERROR: {type(exc).__name__}: {exc}")
        say("No RAW/STAGING/CLEAN source files were modified.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
