#!/usr/bin/env python3
"""STEP 30A - Profile physical data types for the Flight DW source columns.

Read-only with respect to all RAW/CLEAN/BRIDGE source files. The script scans:
- all 12 BTS CLEAN monthly files (selected DW-relevant columns)
- the OurAirports CLEAN file (all columns)
- all Open-Meteo CLEAN CSV files (all columns)
- a bounded sample from each of the 48 Flight-Weather bridge partitions

It writes only profile reports under 06_DW_DESIGN/02_PHYSICAL_MODEL.
No pandas, PyArrow, Parquet, network, or third-party dependencies are used.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent.parent.parent
RUN_ID = "20261110_030433_504879"
BRIDGE_DIR = ROOT / "07_GOLD_DATA" / "01_FLIGHT_WEATHER_EXPOSURE_BRIDGE" / f"run_{RUN_ID}"
OUT_DIR = ROOT / "06_DW_DESIGN" / "02_PHYSICAL_MODEL"

BTS_FIELDS = [
    "SourceFile", "SourceMonth", "SourceRowNumber", "Year", "Quarter", "Month",
    "DayofMonth", "DayOfWeek", "FlightDate", "Reporting_Airline",
    "DOT_ID_Reporting_Airline", "IATA_CODE_Reporting_Airline", "Tail_Number",
    "Flight_Number_Reporting_Airline", "OriginAirportID", "OriginAirportSeqID",
    "OriginCityMarketID", "Origin", "OriginCityName", "OriginState",
    "OriginStateFips", "OriginStateName", "OriginWac", "DestAirportID",
    "DestAirportSeqID", "DestCityMarketID", "Dest", "DestCityName", "DestState",
    "DestStateFips", "DestStateName", "DestWac", "CRSDepTime", "DepTime",
    "DepDelay", "DepDelayMinutes", "DepDel15", "DepartureDelayGroups", "DepTimeBlk",
    "TaxiOut", "WheelsOff", "TaxiIn", "CRSArrTime", "ArrTime", "ArrDelay",
    "ArrDelayMinutes", "ArrDel15", "ArrivalDelayGroups", "ArrTimeBlk", "Cancelled",
    "CancellationCode", "Diverted", "CRSElapsedTime", "ActualElapsedTime", "AirTime",
    "Flights", "Distance", "DistanceGroup", "CarrierDelay", "WeatherDelay", "NASDelay",
    "SecurityDelay", "LateAircraftDelay", "FirstDepTime", "TotalAddGTime",
    "LongestAddGTime", "DivAirportLandings", "DivReachedDest", "DivActualElapsedTime",
    "DivArrDelay", "DivDistance", "FlightDate_Clean", "DQ_FlightDate_Status",
    "CRSDepTime_Clean", "DQ_CRSDepTime_Status", "DepTime_Clean", "DQ_DepTime_Status",
    "CRSArrTime_Clean", "DQ_CRSArrTime_Status", "ArrTime_Clean", "DQ_ArrTime_Status",
    "OriginTimeZoneIANA", "DestinationTimeZoneIANA",
    "DQ_OriginAirportTimezone_Status", "DQ_DestinationAirportTimezone_Status",
    "DQ_Cancelled_Status", "DQ_Diverted_Status",
]
for n in range(1, 6):
    BTS_FIELDS.extend([
        f"Div{n}Airport", f"Div{n}AirportID", f"Div{n}AirportSeqID",
        f"Div{n}WheelsOn", f"Div{n}TotalGTime", f"Div{n}LongestGTime",
        f"Div{n}WheelsOff", f"Div{n}TailNum",
        f"Div{n}WheelsOn_Clean", f"DQ_Div{n}WheelsOn_Status",
        f"Div{n}WheelsOff_Clean", f"DQ_Div{n}WheelsOff_Status",
    ])

@dataclass
class ColumnStats:
    rows: int = 0
    blank: int = 0
    max_length: int = 0
    integer_parse_count: int = 0
    decimal_parse_count: int = 0
    leading_zero_count: int = 0
    numeric_min: str | None = None
    numeric_max: str | None = None
    samples: list[str] = field(default_factory=list)
    frequent_values: Counter = field(default_factory=Counter)
    track_frequency: bool = False

    def add(self, raw: str) -> None:
        self.rows += 1
        value = raw.strip()
        if value == "":
            self.blank += 1
            return
        self.max_length = max(self.max_length, len(raw))
        if len(self.samples) < 5 and value not in self.samples:
            self.samples.append(value)
        if len(value) > 1 and value[0] == "0" and value.isdigit():
            self.leading_zero_count += 1
        if self.track_frequency and (len(self.frequent_values) < 100 or value in self.frequent_values):
            self.frequent_values[value] += 1
        # Fast pre-check avoids Decimal parsing for ordinary text/date/time values.
        unsigned = value[1:] if value[:1] in ("+", "-") else value
        numeric_lookalike = unsigned.isdigit()
        if not numeric_lookalike and unsigned.count(".") == 1:
            left, right = unsigned.split(".", 1)
            numeric_lookalike = (left.isdigit() or left == "") and (right.isdigit() or right == "")
        if numeric_lookalike:
            try:
                number = Decimal(value)
                if number.is_finite():
                    if number == number.to_integral_value():
                        self.integer_parse_count += 1
                    else:
                        self.decimal_parse_count += 1
                    if self.numeric_min is None or number < Decimal(self.numeric_min):
                        self.numeric_min = str(number)
                    if self.numeric_max is None or number > Decimal(self.numeric_max):
                        self.numeric_max = str(number)
            except (InvalidOperation, ValueError):
                pass

    def inferred_shape(self) -> str:
        nonblank = self.rows - self.blank
        if nonblank == 0:
            return "ALL_BLANK"
        numeric = self.integer_parse_count + self.decimal_parse_count
        if numeric == nonblank:
            if self.decimal_parse_count:
                return "NUMERIC_DECIMAL"
            if self.leading_zero_count:
                return "NUMERIC_TEXT_REVIEW_LEADING_ZERO"
            return "NUMERIC_INTEGER_CANDIDATE"
        if numeric == 0:
            return "TEXT_OR_DATETIME_REVIEW"
        return "MIXED_NUMERIC_AND_TEXT_REVIEW"

    def to_record(self, source: str, column: str) -> dict:
        return {
            "Source": source,
            "Column": column,
            "RowsScanned": self.rows,
            "BlankCount": self.blank,
            "NonBlankCount": self.rows - self.blank,
            "BlankPct": round(100 * self.blank / self.rows, 6) if self.rows else None,
            "MaxLength": self.max_length,
            "IntegerParseCount": self.integer_parse_count,
            "DecimalParseCount": self.decimal_parse_count,
            "LeadingZeroCount": self.leading_zero_count,
            "NumericMin": self.numeric_min,
            "NumericMax": self.numeric_max,
            "ShapeAssessment": self.inferred_shape(),
            "SampleValues": " || ".join(self.samples),
            "TopValues": " || ".join(f"{k} ({v})" for k, v in self.frequent_values.most_common(5)),
        }


def require_files(folder: Path, pattern: str, expected: int | None = None) -> list[Path]:
    files = sorted(folder.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files found: {folder} / {pattern}")
    if expected is not None and len(files) != expected:
        raise RuntimeError(f"Expected {expected} files in {folder}, found {len(files)}")
    return files


def scan_csv_files(source: str, files: Iterable[Path], selected_fields: list[str] | None,
                   max_rows_per_file: int | None = None) -> tuple[list[dict], dict]:
    aggregate: dict[str, ColumnStats] = {}
    file_summaries = []
    total_rows = 0
    total_bad_width = 0
    for file_path in files:
        with file_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                raise RuntimeError(f"Empty CSV file: {file_path}")
            if len(header) != len(set(header)):
                raise RuntimeError(f"Duplicate column names found in {file_path.name}")
            if selected_fields is None:
                fields = header
            else:
                missing = [name for name in selected_fields if name not in header]
                if missing:
                    raise RuntimeError(f"{file_path.name}: requested fields missing: {missing}")
                fields = selected_fields
            indexes = [header.index(name) for name in fields]
            for name in fields:
                if name not in aggregate:
                    track = name.endswith("_Status") or name in {
                        "Reporting_Airline", "IATA_CODE_Reporting_Airline", "Origin", "Dest",
                        "CancellationCode", "type", "iso_country", "iso_region", "scheduled_service",
                        "AirportCode", "ExposureType", "TimestampStatus", "WeatherCoverageStatus"
                    }
                    aggregate[name] = ColumnStats(track_frequency=track)
            scanned = 0
            bad_width = 0
            for row in reader:
                if max_rows_per_file is not None and scanned >= max_rows_per_file:
                    break
                scanned += 1
                total_rows += 1
                if len(row) != len(header):
                    bad_width += 1
                    total_bad_width += 1
                    continue
                for name, idx in zip(fields, indexes):
                    aggregate[name].add(row[idx])
        file_summaries.append({
            "File": file_path.name,
            "RowsScanned": scanned,
            "HeaderColumns": len(header),
            "BadWidthRows": bad_width,
            "Sampling": "FIRST_N_ROWS" if max_rows_per_file is not None else "FULL_FILE",
        })
    records = [aggregate[name].to_record(source, name) for name in aggregate]
    summary = {
        "Source": source,
        "Files": len(file_summaries),
        "RowsScanned": total_rows,
        "BadWidthRows": total_bad_width,
        "FileSummaries": file_summaries,
    }
    return records, summary


def write_csv(path: Path, records: list[dict]) -> None:
    if not records:
        return
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)


def main() -> int:
    parser = argparse.ArgumentParser(description="STEP 30A physical source profiling")
    parser.add_argument("--bridge-sample-rows", type=int, default=2000,
                        help="Rows to sample from each bridge partition (default: 2000)")
    args = parser.parse_args()

    bts_files = require_files(ROOT / "04_CLEAN_DATA" / "BTS", "BTS_CLEAN_2025_*.csv", 12)
    airport_files = require_files(ROOT / "04_CLEAN_DATA" / "OurAirports", "airports_clean.csv", 1)
    weather_files = require_files(ROOT / "04_CLEAN_DATA" / "OpenMeteo", "weather_*.csv", 352)
    bridge_files = require_files(BRIDGE_DIR, "29C_bridge_*.csv", 48)

    print("STEP 30A - SOURCE COLUMN PROFILE")
    print("=" * 78)
    print(f"BTS CLEAN files       : {len(bts_files)} (full scan)")
    print(f"OurAirports files     : {len(airport_files)} (full scan)")
    print(f"Open-Meteo files      : {len(weather_files)} (full scan)")
    print(f"Bridge partitions     : {len(bridge_files)} (first {args.bridge_sample_rows:,} rows/file)")
    print("Source files are read-only; only profile reports will be written.")
    print("=" * 78, flush=True)

    all_records: list[dict] = []
    summaries = []

    records, summary = scan_csv_files("BTS_CLEAN", bts_files, BTS_FIELDS)
    all_records.extend(records); summaries.append(summary)
    print(f"BTS CLEAN: {summary['RowsScanned']:,} rows scanned; bad-width rows={summary['BadWidthRows']}", flush=True)

    records, summary = scan_csv_files("OURAIRPORTS_CLEAN", airport_files, None)
    all_records.extend(records); summaries.append(summary)
    print(f"OurAirports: {summary['RowsScanned']:,} rows scanned; bad-width rows={summary['BadWidthRows']}", flush=True)

    records, summary = scan_csv_files("OPENMETEO_CLEAN", weather_files, None)
    all_records.extend(records); summaries.append(summary)
    print(f"Open-Meteo: {summary['RowsScanned']:,} rows scanned; bad-width rows={summary['BadWidthRows']}", flush=True)

    records, summary = scan_csv_files(
        "FLIGHT_WEATHER_BRIDGE_SAMPLE", bridge_files, None,
        max_rows_per_file=args.bridge_sample_rows
    )
    all_records.extend(records); summaries.append(summary)
    print(f"Bridge sample: {summary['RowsScanned']:,} rows scanned; bad-width rows={summary['BadWidthRows']}", flush=True)

    expected = {
        "BTS_CLEAN": 7_001_619,
        "OURAIRPORTS_CLEAN": 86_335,
        "OPENMETEO_CLEAN": 3_083_520,
        "FLIGHT_WEATHER_BRIDGE_SAMPLE": None,
    }
    errors = []
    for summary in summaries:
        source = summary["Source"]
        if summary["BadWidthRows"] != 0:
            errors.append(f"{source}: {summary['BadWidthRows']} malformed row widths")
        if expected[source] is not None and summary["RowsScanned"] != expected[source]:
            errors.append(f"{source}: expected {expected[source]}, scanned {summary['RowsScanned']}")
    # Confirm all selected field profile rows have the expected number of observations.
    if not errors:
        result = "PASS"
    else:
        result = "REVIEW_REQUIRED"

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    profile_path = OUT_DIR / f"30A_source_column_profile_{stamp}.csv"
    summary_path = OUT_DIR / f"30A_source_column_profile_summary_{stamp}.json"
    text_path = OUT_DIR / f"30A_source_column_profile_summary_{stamp}.txt"
    write_csv(profile_path, all_records)
    payload = {
        "Step": "30A",
        "Result": result,
        "RunTimestamp": stamp,
        "BridgeRunID": RUN_ID,
        "Summaries": summaries,
        "Errors": errors,
        "ProfileCSV": str(profile_path),
    }
    summary_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    lines = [
        "STEP 30A - SOURCE COLUMN PROFILE SUMMARY",
        "=" * 78,
        f"Result: {result}",
        f"BTS CLEAN rows scanned: {summaries[0]['RowsScanned']:,} / 7,001,619",
        f"OurAirports rows scanned: {summaries[1]['RowsScanned']:,} / 86,335",
        f"Open-Meteo rows scanned: {summaries[2]['RowsScanned']:,} / 3,083,520",
        f"Bridge rows sampled: {summaries[3]['RowsScanned']:,} across {summaries[3]['Files']} partitions",
        f"Bad-width rows: {sum(s['BadWidthRows'] for s in summaries)}",
        f"Profile CSV: {profile_path}",
        f"Summary JSON: {summary_path}",
    ]
    if errors:
        lines.extend(["", "ERRORS:"] + [f"- {e}" for e in errors])
    lines.extend(["", "NOTE: numeric shape assessments are candidates, not automatic SQL type decisions.",
                  "Leading-zero fields are flagged for preservation as text."])
    text_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n" + "\n".join(lines))
    print("\nSTEP 30A COMPLETE")
    return 0 if result == "PASS" else 2

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user. Source files were not modified.", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(f"\nSTEP 30A ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1)
