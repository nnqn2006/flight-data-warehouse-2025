#!/usr/bin/env python3
"""STEP 30C - Load Flight DW 2025 dimensions into MySQL 8.0.

Default mode is PREVIEW (read-only with respect to MySQL and source files).
Use --execute to upsert Dim_Airline, Dim_Airport and Map_BTS_Airport.

The script does not modify RAW, STAGING, CLEAN, GOLD CSV files and does not
DROP/TRUNCATE target tables. It is restart-safe for the intended source keys.
"""
from __future__ import annotations

import argparse
import csv
import getpass
import json
import os
import sys
from collections import Counter
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

ROOT = Path.cwd()
DB_NAME = "flight_dw_2025"
BATCH_SIZE = 1000
EXPECTED_BTS_ROWS = 7_001_619
EXPECTED_AIRPORT_ROWS = 86_335
EXPECTED_BTS_CODES = 352
EXPECTED_DIRECT_CODES = 351
EXPECTED_HISTORICAL_CODES = 1
EXPECTED_OCCURRENCES = 14_003_238

PHYSICAL_DIR = ROOT / "06_DW_DESIGN" / "02_PHYSICAL_MODEL"
BTS_DIR = ROOT / "04_CLEAN_DATA" / "BTS"
OURAIRPORTS_FILE = ROOT / "04_CLEAN_DATA" / "OurAirports" / "airports_clean.csv"
MAPPING_DIR = ROOT / "02_DATA_PROFILING" / "02_OurAirports"
MAPPING_FILE = MAPPING_DIR / "13_bts_airport_mapping_resolved.csv"
MAPPING_SUMMARY_FILE = MAPPING_DIR / "14_bts_airport_mapping_resolved_summary.csv"
CROSSWALK_FILE = MAPPING_DIR / "12_historical_airport_code_crosswalk.csv"
TIMEZONE_FILE = ROOT / "06_DW_DESIGN" / "01_LOGICAL_MODEL" / "27B3B_airport_timezone_mapping_candidates.csv"
OPENMETEO_DIR = ROOT / "04_CLEAN_DATA" / "OpenMeteo"
TIMEZONE_ISSUES_FILE = ROOT / "06_DW_DESIGN" / "01_LOGICAL_MODEL" / "27B3B_airport_timezone_mapping_issues.csv"


def say(message: str) -> None:
    print(message, flush=True)


def require_file(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"Required source file not found: {path}")


def clean_text(value: Any) -> str:
    """Trim outer whitespace only. Literal text such as 'NA' is preserved."""
    return "" if value is None else str(value).strip()


def blank_to_none(value: Any) -> str | None:
    value = clean_text(value)
    return None if value == "" else value


def parse_int(value: Any, field: str, *, nullable: bool = False) -> int | None:
    raw = clean_text(value)
    if raw == "" and nullable:
        return None
    try:
        # int('12.0') is invalid; this is intentional for identifier fields.
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid integer in {field}: {raw!r}") from exc


def parse_decimal(value: Any, field: str, *, nullable: bool = False) -> Decimal | None:
    raw = clean_text(value)
    if raw == "" and nullable:
        return None
    try:
        return Decimal(raw)
    except (InvalidOperation, TypeError, ValueError) as exc:
        if nullable:
            return None
        raise ValueError(f"Invalid decimal in {field}: {raw!r}") from exc


def decimal_7(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return value.quantize(Decimal("0.0000001"), rounding=ROUND_HALF_UP)


def read_dict_rows(path: Path):
    require_file(path)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"Missing CSV header: {path}")
        for row_number, row in enumerate(reader, start=2):
            yield row_number, row


def locate_bts_files() -> list[Path]:
    files = sorted(BTS_DIR.glob("BTS_CLEAN_2025_*.csv"))
    if len(files) != 12:
        raise ValueError(f"Expected 12 BTS CLEAN monthly files; found {len(files)} in {BTS_DIR}")
    return files


def load_airline_source() -> tuple[list[tuple[int, str, str]], int]:
    """Scan all BTS CLEAN rows and derive one governed record per DOT ID."""
    files = locate_bts_files()
    airlines: dict[int, tuple[str, str]] = {}
    total_rows = 0
    say(f"[1/4] Scanning airline business keys in {len(files)} BTS CLEAN files...")

    for file_path in files:
        with file_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            try:
                header = next(reader)
            except StopIteration:
                raise ValueError(f"Empty BTS CLEAN file: {file_path}")
            required = ["DOT_ID_Reporting_Airline", "Reporting_Airline", "IATA_CODE_Reporting_Airline"]
            missing = [name for name in required if name not in header]
            if missing:
                raise ValueError(f"Missing BTS columns in {file_path.name}: {missing}")
            ix_dot, ix_reporting, ix_iata = (header.index(x) for x in required)

            file_rows = 0
            for row_number, row in enumerate(reader, start=2):
                if len(row) != len(header):
                    raise ValueError(
                        f"Bad row width in {file_path.name}:{row_number}; "
                        f"expected {len(header)}, got {len(row)}"
                    )
                dot_id = parse_int(row[ix_dot], f"{file_path.name}.DOT_ID_Reporting_Airline")
                reporting = clean_text(row[ix_reporting])
                iata = clean_text(row[ix_iata])
                if dot_id <= 0:
                    raise ValueError(f"Non-positive DOT Airline ID in {file_path.name}:{row_number}")
                if not reporting or not iata:
                    raise ValueError(f"Blank airline code in {file_path.name}:{row_number}")
                if len(reporting) > 2 or len(iata) > 2:
                    raise ValueError(f"Airline code exceeds CHAR(2) in {file_path.name}:{row_number}")
                pair = (reporting, iata)
                previous = airlines.get(dot_id)
                if previous is not None and previous != pair:
                    raise ValueError(
                        f"DOT_ID {dot_id} maps to conflicting airline codes: {previous} vs {pair}"
                    )
                airlines[dot_id] = pair
                file_rows += 1
            total_rows += file_rows
            say(f"      {file_path.name}: {file_rows:,} rows scanned; {len(airlines):,} distinct DOT IDs so far")

    if total_rows != EXPECTED_BTS_ROWS:
        raise ValueError(f"BTS row count mismatch: {total_rows:,} != {EXPECTED_BTS_ROWS:,}")
    records = [(dot_id, pair[0], pair[1]) for dot_id, pair in sorted(airlines.items())]
    if not records:
        raise ValueError("No airline records derived from BTS CLEAN")
    return records, total_rows


def load_timezone_lookup() -> tuple[dict[int, str], dict[str, dict[str, str]], dict[str, Any]]:
    """Load the audited timezone map and validate weather files against current CLEAN inventory.

    HasOpenMeteo2025File in the timezone-candidate CSV is a historical snapshot.
    It may predate the STEP 29B2 incremental crawl. Therefore, current CLEAN file
    presence is authoritative; the old flag is retained as a reconciliation metric.
    """
    require_file(TIMEZONE_FILE)
    if not OPENMETEO_DIR.is_dir():
        raise FileNotFoundError(f"Open-Meteo CLEAN directory not found: {OPENMETEO_DIR}")

    by_id: dict[int, str] = {}
    by_code: dict[str, dict[str, str]] = {}
    stale_false_flags: list[str] = []
    invalid_file_flags: list[str] = []

    for row_number, row in read_dict_rows(TIMEZONE_FILE):
        airport_id = parse_int(row.get("OurAirportsID"), f"timezone.OurAirportsID row {row_number}")
        code = clean_text(row.get("BTSAirportCode")).upper()
        timezone = clean_text(row.get("TimeZoneIANA"))
        if not code or len(code) != 3 or not timezone:
            raise ValueError(f"Invalid airport/timezone mapping at {TIMEZONE_FILE.name}:{row_number}")
        if clean_text(row.get("CandidateStatus")) != "CANDIDATE_READY_FOR_REVIEW":
            raise ValueError(f"Unexpected timezone candidate status for {code}: {row.get('CandidateStatus')}")
        if clean_text(row.get("TimezoneLookupStatus")) != "LOOKUP_OK_UNAMBIGUOUS":
            raise ValueError(f"Timezone is not unambiguous for {code}")

        # The candidate-file boolean may be stale after the incremental weather
        # crawl. Validate actual 2025 CLEAN files below instead of rejecting a
        # code solely because this earlier snapshot says False.
        historical_flag = clean_text(row.get("HasOpenMeteo2025File")).lower()
        if historical_flag == "false":
            stale_false_flags.append(code)
        elif historical_flag != "true":
            invalid_file_flags.append(f"{code}={historical_flag!r}")

        if airport_id in by_id and by_id[airport_id] != timezone:
            raise ValueError(f"Conflicting timezone values for OurAirportsID {airport_id}")
        if code in by_code:
            raise ValueError(f"Duplicate BTS airport code in timezone candidates: {code}")
        by_id[airport_id] = timezone
        by_code[code] = row

    if len(by_code) != EXPECTED_BTS_CODES:
        raise ValueError(f"Expected {EXPECTED_BTS_CODES} timezone candidate rows; found {len(by_code)}")
    if invalid_file_flags:
        raise ValueError(
            "Unexpected HasOpenMeteo2025File flag values: " + ", ".join(invalid_file_flags[:20])
        )

    # Compare the 352 expected BTS airport codes to the current CLEAN folder.
    actual_files: dict[str, Path] = {}
    duplicate_file_codes: list[str] = []
    for path in OPENMETEO_DIR.glob("weather_*_2025.csv"):
        parts = path.stem.split("_")
        if len(parts) != 3 or parts[0].lower() != "weather" or len(parts[1]) != 3:
            continue
        code = parts[1].upper()
        if code in actual_files:
            duplicate_file_codes.append(code)
        else:
            actual_files[code] = path

    if duplicate_file_codes:
        raise ValueError(
            "Duplicate Open-Meteo CLEAN files for airport codes: "
            + ", ".join(sorted(set(duplicate_file_codes)))
        )

    expected_codes = set(by_code)
    actual_codes = set(actual_files)
    missing_files = sorted(expected_codes - actual_codes)
    if missing_files:
        raise ValueError(
            f"Missing {len(missing_files)} required Open-Meteo CLEAN 2025 files: "
            + ", ".join(missing_files[:30])
        )

    present_but_old_flag_false = sorted(set(stale_false_flags) & actual_codes)
    say(
        f"[Open-Meteo] Current CLEAN inventory: {len(actual_codes)} file(s); "
        f"all {len(expected_codes)} BTS airport files are present."
    )
    if present_but_old_flag_false:
        say(
            "[Open-Meteo] Historical candidate-file flag is stale for "
            f"{len(present_but_old_flag_false)} code(s), but current CLEAN files exist: "
            + ", ".join(present_but_old_flag_false[:20])
        )

    require_file(TIMEZONE_ISSUES_FILE)
    with TIMEZONE_ISSUES_FILE.open("r", encoding="utf-8-sig", newline="") as handle:
        issue_reader = csv.DictReader(handle)
        issue_count = sum(1 for _ in issue_reader)
    if issue_count != 0:
        raise ValueError(f"Timezone mapping issue file is not empty: {issue_count} rows")

    inventory = {
        "OpenMeteoCleanFilesFound": len(actual_codes),
        "BTSAirportWeatherFilesRequired": len(expected_codes),
        "RequiredOpenMeteoFilesPresent": len(expected_codes) - len(missing_files),
        "HistoricalCandidateFlagFalseCount": len(stale_false_flags),
        "HistoricalFlagFalseButCurrentFilePresentCount": len(present_but_old_flag_false),
        "HistoricalFlagFalseButCurrentFilePresentCodes": present_but_old_flag_false,
        "TimezoneIssueRows": issue_count,
    }
    return by_id, by_code, inventory


def load_airport_source(timezones_by_id: dict[int, str]) -> tuple[list[tuple[Any, ...]], set[int]]:
    require_file(OURAIRPORTS_FILE)
    records: list[tuple[Any, ...]] = []
    ids: set[int] = set()
    ident_seen: dict[str, int] = {}
    say("[2/4] Reading and validating all OurAirports CLEAN records...")

    length_limits = {
        "ident": 8, "type": 20, "name": 150, "latitude_deg": 32,
        "continent": 2, "iso_country": 2, "iso_region": 8,
        "municipality": 80, "scheduled_service": 3, "icao_code": 4,
        "iata_code": 3, "gps_code": 4, "local_code": 10,
        "home_link": 255, "wikipedia_link": 255, "keywords": 512,
    }

    for row_number, row in read_dict_rows(OURAIRPORTS_FILE):
        airport_id = parse_int(row.get("id"), f"OurAirports.id row {row_number}")
        ident = clean_text(row.get("ident"))
        airport_type = clean_text(row.get("type"))
        name = clean_text(row.get("name"))
        latitude_raw = clean_text(row.get("latitude_deg"))
        if airport_id <= 0 or not ident or not airport_type or not name or latitude_raw == "":
            raise ValueError(f"Missing required airport attribute at airports_clean.csv:{row_number}")
        if airport_id in ids:
            raise ValueError(f"Duplicate OurAirports id: {airport_id}")
        folded_ident = ident.casefold()
        if folded_ident in ident_seen:
            raise ValueError(
                f"Duplicate/case-insensitive Ident {ident!r} for IDs {ident_seen[folded_ident]} and {airport_id}"
            )
        ids.add(airport_id)
        ident_seen[folded_ident] = airport_id

        for field, limit in length_limits.items():
            value = clean_text(row.get(field))
            if len(value) > limit:
                raise ValueError(
                    f"{field} exceeds target column length {limit} at row {row_number}: {len(value)} chars"
                )

        latitude_decimal = parse_decimal(latitude_raw, f"latitude_deg row {row_number}", nullable=True)
        latitude_deg = decimal_7(latitude_decimal)
        if latitude_deg is not None and not (Decimal("-90") <= latitude_deg <= Decimal("90")):
            raise ValueError(f"Latitude outside valid range at row {row_number}: {latitude_raw}")

        longitude_raw = clean_text(row.get("longitude_deg"))
        longitude_decimal = parse_decimal(longitude_raw, f"longitude_deg row {row_number}")
        longitude_deg = decimal_7(longitude_decimal)
        if not (Decimal("-180") <= longitude_deg <= Decimal("180")):
            raise ValueError(f"Longitude outside valid range at row {row_number}: {longitude_raw}")

        elevation_raw = clean_text(row.get("elevation_ft"))
        elevation_ft = None
        if elevation_raw:
            elevation_dec = parse_decimal(elevation_raw, f"elevation_ft row {row_number}")
            if elevation_dec != elevation_dec.to_integral_value():
                raise ValueError(f"Non-integer elevation cannot fit SMALLINT at row {row_number}: {elevation_raw}")
            elevation_ft = int(elevation_dec)
            if not (-32768 <= elevation_ft <= 32767):
                raise ValueError(f"Elevation outside SMALLINT range at row {row_number}: {elevation_raw}")

        # Blank CSV cells become SQL NULL. Literal values such as 'NA' stay text.
        timezone = timezones_by_id.get(airport_id)
        records.append((
            airport_id,
            ident,
            airport_type,
            name,
            latitude_raw,
            latitude_deg,
            longitude_deg,
            elevation_ft,
            blank_to_none(row.get("continent")),
            blank_to_none(row.get("iso_country")),
            blank_to_none(row.get("iso_region")),
            blank_to_none(row.get("municipality")),
            blank_to_none(row.get("scheduled_service")),
            blank_to_none(row.get("icao_code")),
            blank_to_none(row.get("iata_code")),
            blank_to_none(row.get("gps_code")),
            blank_to_none(row.get("local_code")),
            blank_to_none(row.get("home_link")),
            blank_to_none(row.get("wikipedia_link")),
            blank_to_none(row.get("keywords")),
            timezone,
        ))

    if len(records) != EXPECTED_AIRPORT_ROWS:
        raise ValueError(f"OurAirports row count mismatch: {len(records):,} != {EXPECTED_AIRPORT_ROWS:,}")
    return records, ids


def load_mapping_source(airport_ids: set[int], timezone_by_code: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    require_file(MAPPING_FILE)
    require_file(MAPPING_SUMMARY_FILE)
    require_file(CROSSWALK_FILE)

    summary_rows = list(read_dict_rows(MAPPING_SUMMARY_FILE))
    if len(summary_rows) != 1:
        raise ValueError(f"Expected exactly one mapping summary row; found {len(summary_rows)}")
    summary = summary_rows[0][1]
    summary_expected = {
        "TotalDistinctCodes": EXPECTED_BTS_CODES,
        "DirectlyMatchedCodes": EXPECTED_DIRECT_CODES,
        "HistoricallyMatchedCodes": EXPECTED_HISTORICAL_CODES,
        "UnmatchedCodes": 0,
        "TotalOccurrences": EXPECTED_OCCURRENCES,
        "MatchedOccurrences": EXPECTED_OCCURRENCES,
        "UnmatchedOccurrences": 0,
    }
    for field, expected in summary_expected.items():
        actual = parse_int(summary.get(field), f"mapping summary {field}")
        if actual != expected:
            raise ValueError(f"Mapping summary mismatch for {field}: {actual} != {expected}")
    if Decimal(clean_text(summary.get("DistinctMatchPercent"))) != Decimal("100.0"):
        raise ValueError("DistinctMatchPercent is not 100.0")
    if Decimal(clean_text(summary.get("OccurrenceMatchPercent"))) != Decimal("100.0"):
        raise ValueError("OccurrenceMatchPercent is not 100.0")

    crosswalk: dict[str, dict[str, str]] = {}
    for row_number, row in read_dict_rows(CROSSWALK_FILE):
        code = clean_text(row.get("AirportCode"))
        if code:
            crosswalk[code] = row

    rows: list[dict[str, Any]] = []
    seen_codes: set[str] = set()
    direct_count = 0
    historical_count = 0
    total_occurrences = 0
    for row_number, row in read_dict_rows(MAPPING_FILE):
        code = clean_text(row.get("IATACode"))
        airport_id = parse_int(row.get("AirportID"), f"mapping AirportID row {row_number}")
        match_status = clean_text(row.get("MatchStatus"))
        resolved_status = clean_text(row.get("ResolvedMatchStatus"))
        method = clean_text(row.get("MappingMethod"))
        if len(code) != 3 or code in seen_codes:
            raise ValueError(f"Invalid/duplicate BTS IATA code in mapping file: {code!r}")
        if airport_id not in airport_ids:
            raise ValueError(f"Mapping {code} refers to unknown OurAirportsID {airport_id}")
        if code not in timezone_by_code:
            raise ValueError(f"Mapping code {code} missing from timezone candidate file")
        tz_airport_id = parse_int(timezone_by_code[code].get("OurAirportsID"), f"timezone {code} OurAirportsID")
        if airport_id != tz_airport_id:
            raise ValueError(f"Airport ID disagreement for {code}: resolved={airport_id}, timezone={tz_airport_id}")

        if method == "DIRECT_IATA_MATCH":
            if match_status != "MATCHED":
                raise ValueError(
                    f"Direct mapping {code} has unexpected MatchStatus: {match_status!r}"
                )
            if resolved_status != "MATCHED_DIRECT":
                raise ValueError(
                    f"Direct mapping {code} has unexpected ResolvedMatchStatus: {resolved_status!r}"
                )
            direct_count += 1
        elif method == "HISTORICAL_IATA_ALIAS":
            # Historical alias is a valid resolved mapping state, not an
            # unmatched or failed mapping. Preserve the source status exactly.
            if match_status != "MATCHED_HISTORICAL_ALIAS":
                raise ValueError(
                    f"Historical mapping {code} has unexpected MatchStatus: {match_status!r}"
                )
            if resolved_status != "MATCHED_HISTORICAL_ALIAS":
                raise ValueError(
                    f"Historical mapping {code} has unexpected ResolvedMatchStatus: {resolved_status!r}"
                )
            historical_count += 1
            cw = crosswalk.get(code)
            if not cw:
                raise ValueError(f"Historical alias {code} is missing from crosswalk")
            if parse_int(cw.get("AirportID"), f"crosswalk {code} AirportID") != airport_id:
                raise ValueError(f"Historical crosswalk AirportID mismatch for {code}")
            if clean_text(cw.get("CodeSystem")).upper() != "IATA":
                raise ValueError(f"Historical crosswalk code system is not IATA for {code}")
            if clean_text(cw.get("MappingMethod")) != "HISTORICAL_IATA_ALIAS":
                raise ValueError(f"Historical crosswalk method mismatch for {code}")
            crosswalk_valid_to = clean_text(cw.get("ValidTo"))
            if not crosswalk_valid_to:
                raise ValueError(f"Historical alias {code} has no ValidTo date")
            if crosswalk_valid_to != clean_text(row.get("HistoricalAliasValidTo")):
                raise ValueError(f"Historical alias valid-to mismatch for {code}")
            if code != "PBI" or airport_id != 3766:
                raise ValueError(
                    f"Unexpected historical alias target: {code} -> OurAirportsID {airport_id}"
                )
        else:
            raise ValueError(f"Unexpected MappingMethod for {code}: {method}")

        origin_rows = parse_int(row.get("OriginFlightRows"), f"mapping {code} OriginFlightRows")
        dest_rows = parse_int(row.get("DestFlightRows"), f"mapping {code} DestFlightRows")
        occurrences = parse_int(row.get("TotalOccurrences"), f"mapping {code} TotalOccurrences")
        if occurrences != origin_rows + dest_rows:
            raise ValueError(f"Occurrence total mismatch for {code}")
        total_occurrences += occurrences

        rows.append({
            "code": code,
            "airport_id": airport_id,
            "match_status": match_status,
            "original_status": blank_to_none(row.get("OriginalMatchStatus")),
            "resolved_status": resolved_status,
            "method": method,
            "origin_rows": origin_rows,
            "dest_rows": dest_rows,
            "occurrences": occurrences,
            "valid_to": clean_text(row.get("HistoricalAliasValidTo")) or None,
        })
        seen_codes.add(code)

    if len(rows) != EXPECTED_BTS_CODES:
        raise ValueError(f"Resolved mapping row count mismatch: {len(rows)} != {EXPECTED_BTS_CODES}")
    if direct_count != EXPECTED_DIRECT_CODES or historical_count != EXPECTED_HISTORICAL_CODES:
        raise ValueError(f"Direct/historical counts mismatch: {direct_count}/{historical_count}")
    if total_occurrences != EXPECTED_OCCURRENCES:
        raise ValueError(f"Mapping occurrence total mismatch: {total_occurrences:,} != {EXPECTED_OCCURRENCES:,}")
    if seen_codes != set(timezone_by_code):
        missing_tz = sorted(seen_codes - set(timezone_by_code))
        extra_tz = sorted(set(timezone_by_code) - seen_codes)
        raise ValueError(f"Mapping/timezone code set mismatch; missing timezone={missing_tz}, extra timezone={extra_tz}")

    historical = [r for r in rows if r["method"] == "HISTORICAL_IATA_ALIAS"]
    if len(historical) != 1 or historical[0]["code"] != "PBI" or historical[0]["airport_id"] != 3766:
        raise ValueError(f"Expected the single PBI historical alias to OurAirportsID 3766; got {historical}")

    return rows


def prepare_plan() -> dict[str, Any]:
    say("STEP 30C - DIMENSION ETL PREFLIGHT")
    say("=" * 78)
    airlines, bts_rows = load_airline_source()
    timezone_by_id, timezone_by_code, weather_inventory = load_timezone_lookup()
    airports, airport_ids = load_airport_source(timezone_by_id)
    mappings = load_mapping_source(airport_ids, timezone_by_code)

    timezone_airport_count = sum(1 for rec in airports if rec[-1] is not None)
    if timezone_airport_count != EXPECTED_BTS_CODES:
        raise ValueError(f"Expected {EXPECTED_BTS_CODES} airport timezone attributes; found {timezone_airport_count}")

    summary = {
        "Step": "30C",
        "Mode": "PREVIEW",
        "Result": "PASS",
        "BTSRowsScanned": bts_rows,
        "DistinctAirlines": len(airlines),
        "OurAirportsRows": len(airports),
        "OurAirportsUniqueIDs": len(airport_ids),
        "AirportRowsWithTimezone": timezone_airport_count,
        "ResolvedBTSAirportMappings": len(mappings),
        "DirectMappings": sum(1 for r in mappings if r["method"] == "DIRECT_IATA_MATCH"),
        "HistoricalMappings": sum(1 for r in mappings if r["method"] == "HISTORICAL_IATA_ALIAS"),
        "HistoricalAliasCodes": [r["code"] for r in mappings if r["method"] == "HISTORICAL_IATA_ALIAS"],
        "BTSAirportOccurrences": sum(r["occurrences"] for r in mappings),
        "OpenMeteoCLEANFilesFound": weather_inventory["OpenMeteoCleanFilesFound"],
        "RequiredOpenMeteoFilesPresent": weather_inventory["RequiredOpenMeteoFilesPresent"],
        "HistoricalCandidateFlagFalseCount": weather_inventory["HistoricalCandidateFlagFalseCount"],
        "HistoricalFlagFalseButCurrentFilePresentCount": weather_inventory["HistoricalFlagFalseButCurrentFilePresentCount"],
        "HistoricalFlagFalseButCurrentFilePresentCodes": weather_inventory["HistoricalFlagFalseButCurrentFilePresentCodes"],
        "TimezoneIssueRows": weather_inventory["TimezoneIssueRows"],
        "HistoricalAlias": {
            "Code": "PBI",
            "OurAirportsID": 3766,
            "ValidTo": next(r["valid_to"] for r in mappings if r["code"] == "PBI"),
        },
        "BatchSize": BATCH_SIZE,
        "SourceFilesReadOnly": True,
        "TargetDatabase": DB_NAME,
    }
    return {
        "airlines": airlines,
        "airports": airports,
        "mappings": mappings,
        "summary": summary,
    }


def write_reports(summary: dict[str, Any], mode: str) -> tuple[Path, Path]:
    PHYSICAL_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    payload = dict(summary)
    payload["Mode"] = mode
    payload["RunTimestamp"] = run_id
    json_path = PHYSICAL_DIR / f"30C_dimension_etl_{mode.lower()}_{run_id}.json"
    txt_path = PHYSICAL_DIR / f"30C_dimension_etl_{mode.lower()}_{run_id}.txt"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    lines = [
        "STEP 30C - DIMENSION ETL " + mode,
        "=" * 78,
        f"Result: {payload.get('Result')}",
        f"BTS rows scanned: {payload.get('BTSRowsScanned', 0):,}",
        f"Distinct airlines: {payload.get('DistinctAirlines', 0):,}",
        f"OurAirports rows: {payload.get('OurAirportsRows', 0):,}",
        f"Airport rows with timezone: {payload.get('AirportRowsWithTimezone', 0):,}",
        f"Resolved BTS airport mappings: {payload.get('ResolvedBTSAirportMappings', 0):,}",
        f"Direct mappings: {payload.get('DirectMappings', 0):,}",
        f"Historical mappings: {payload.get('HistoricalMappings', 0):,}",
        f"BTS airport occurrences: {payload.get('BTSAirportOccurrences', 0):,}",
        f"Open-Meteo CLEAN files found: {payload.get('OpenMeteoCLEANFilesFound', 0):,}",
        f"Required Open-Meteo files present: {payload.get('RequiredOpenMeteoFilesPresent', 0):,}",
        f"Historical candidate flags marked False: {payload.get('HistoricalCandidateFlagFalseCount', 0):,}",
        f"False historical flags with current files present: {payload.get('HistoricalFlagFalseButCurrentFilePresentCount', 0):,}",
        f"Timezone issue rows: {payload.get('TimezoneIssueRows', 0):,}",
        f"Target database: {DB_NAME}",
        "Source files: READ ONLY",
        f"JSON report: {json_path}",
    ]
    if payload.get("DatabaseValidation"):
        lines += ["", "DATABASE VALIDATION"]
        for key, value in payload["DatabaseValidation"].items():
            lines.append(f"{key}: {value}")
    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return txt_path, json_path


def check_existing_target_rows(cursor, plan: dict[str, Any]) -> None:
    say("[DB] Checking existing target keys for safe restart...")
    source_airlines = {dot: (rep, iata) for dot, rep, iata in plan["airlines"]}
    cursor.execute("SELECT DOT_ID_Reporting_Airline, ReportingAirlineCode, IATAReportingAirlineCode FROM Dim_Airline")
    for dot, rep, iata in cursor.fetchall():
        expected = source_airlines.get(int(dot))
        if expected is not None and expected != (rep.rstrip(), iata.rstrip()):
            raise ValueError(f"Existing Dim_Airline key {dot} conflicts with BTS CLEAN source")
        if expected is None:
            raise ValueError(f"Dim_Airline contains an unexpected DOT ID {dot}; refusing destructive reconciliation")

    source_airports = {rec[0]: rec[1] for rec in plan["airports"]}
    source_ident = {rec[1].casefold(): rec[0] for rec in plan["airports"]}
    cursor.execute("SELECT OurAirportsID, Ident FROM Dim_Airport")
    for airport_id, ident in cursor.fetchall():
        airport_id = int(airport_id)
        expected_ident = source_airports.get(airport_id)
        expected_id = source_ident.get(str(ident).casefold())
        if expected_ident is None or expected_ident != ident:
            raise ValueError(f"Existing Dim_Airport row conflicts with source: ID={airport_id}, Ident={ident}")
        if expected_id != airport_id:
            raise ValueError(f"Existing Dim_Airport Ident conflict: {ident}")

    source_map = {r["code"]: r["airport_id"] for r in plan["mappings"]}
    cursor.execute("SELECT BTSAirportCode, OurAirportsID FROM Map_BTS_Airport")
    for code, airport_id in cursor.fetchall():
        if source_map.get(code) != int(airport_id):
            raise ValueError(f"Existing Map_BTS_Airport row conflicts with resolved mapping: {code}")


def insert_in_batches(cursor, connection, sql: str, rows: list[tuple[Any, ...]], label: str) -> None:
    count = len(rows)
    for start in range(0, count, BATCH_SIZE):
        batch = rows[start:start + BATCH_SIZE]
        try:
            cursor.executemany(sql, batch)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        completed = min(start + len(batch), count)
        say(f"      {label}: {completed:,}/{count:,} rows submitted and committed")


def execute_load(plan: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    try:
        import mysql.connector  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "mysql-connector-python is not installed in this virtual environment. "
            "Install it with: python -m pip install mysql-connector-python"
        ) from exc

    host = args.host or os.getenv("FLIGHT_DW_DB_HOST", "127.0.0.1")
    port = args.port or int(os.getenv("FLIGHT_DW_DB_PORT", "3306"))
    user = args.user or os.getenv("FLIGHT_DW_DB_USER", "root")
    password = os.getenv("FLIGHT_DW_DB_PASSWORD")
    if password is None:
        password = getpass.getpass(f"MySQL password for {user}@{host}:{port}: ")

    say("\nSTEP 30C - EXECUTE DIMENSION LOAD")
    say("=" * 78)
    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=DB_NAME,
        charset="utf8mb4",
        collation="utf8mb4_0900_ai_ci",
        autocommit=False,
        connection_timeout=20,
    )
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DATABASE(), VERSION()")
        db_name, version = cursor.fetchone()
        say(f"Connected database: {db_name}; MySQL: {version}")
        if db_name != DB_NAME:
            raise RuntimeError(f"Connected to unexpected database: {db_name}")
        if not str(version).startswith("8.0."):
            raise RuntimeError(f"Expected MySQL 8.0.x, got {version}")

        # Ensure all expected target tables exist before any data writes.
        required_tables = {"dim_airline", "dim_airport", "map_bts_airport"}
        cursor.execute(
            "SELECT LOWER(TABLE_NAME) FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = %s AND LOWER(TABLE_NAME) IN (%s,%s,%s)",
            (DB_NAME, "dim_airline", "dim_airport", "map_bts_airport"),
        )
        found_tables = {r[0] for r in cursor.fetchall()}
        if found_tables != required_tables:
            raise RuntimeError(f"Missing target tables: {sorted(required_tables - found_tables)}")

        # Preserve the full source status value MATCHED_HISTORICAL_ALIAS (24 chars).
        # Validate target width before any insert batch is committed.
        cursor.execute(
            "SELECT CHARACTER_MAXIMUM_LENGTH "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'map_bts_airport' "
            "AND COLUMN_NAME = 'MatchStatus'",
            (DB_NAME,),
        )
        status_width_row = cursor.fetchone()
        if not status_width_row or status_width_row[0] is None or int(status_width_row[0]) < 24:
            actual_width = None if not status_width_row else status_width_row[0]
            raise RuntimeError(
                "Map_BTS_Airport.MatchStatus must support at least 24 characters "
                "to preserve MATCHED_HISTORICAL_ALIAS; current width="
                f"{actual_width!r}. Run the STEP 30C schema patch before --execute."
            )

        check_existing_target_rows(cursor, plan)

        airline_sql = """
            INSERT INTO Dim_Airline
                (DOT_ID_Reporting_Airline, ReportingAirlineCode, IATAReportingAirlineCode)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                ReportingAirlineCode = VALUES(ReportingAirlineCode),
                IATAReportingAirlineCode = VALUES(IATAReportingAirlineCode)
        """
        insert_in_batches(cursor, conn, airline_sql, plan["airlines"], "Dim_Airline")

        airport_sql = """
            INSERT INTO Dim_Airport (
                OurAirportsID, Ident, AirportType, AirportName,
                LatitudeRaw, LatitudeDeg, LongitudeDeg, ElevationFt,
                Continent, ISOCountry, ISORegion, Municipality,
                ScheduledService, ICAOCode, IATACode, GPSCode, LocalCode,
                HomeLink, WikipediaLink, Keywords, TimeZoneIANA
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                Ident = VALUES(Ident),
                AirportType = VALUES(AirportType),
                AirportName = VALUES(AirportName),
                LatitudeRaw = VALUES(LatitudeRaw),
                LatitudeDeg = VALUES(LatitudeDeg),
                LongitudeDeg = VALUES(LongitudeDeg),
                ElevationFt = VALUES(ElevationFt),
                Continent = VALUES(Continent),
                ISOCountry = VALUES(ISOCountry),
                ISORegion = VALUES(ISORegion),
                Municipality = VALUES(Municipality),
                ScheduledService = VALUES(ScheduledService),
                ICAOCode = VALUES(ICAOCode),
                IATACode = VALUES(IATACode),
                GPSCode = VALUES(GPSCode),
                LocalCode = VALUES(LocalCode),
                HomeLink = VALUES(HomeLink),
                WikipediaLink = VALUES(WikipediaLink),
                Keywords = VALUES(Keywords),
                TimeZoneIANA = VALUES(TimeZoneIANA)
        """
        insert_in_batches(cursor, conn, airport_sql, plan["airports"], "Dim_Airport")

        cursor.execute("SELECT OurAirportsID, AirportKey FROM Dim_Airport")
        airport_keys = {int(our_id): int(airport_key) for our_id, airport_key in cursor.fetchall()}
        missing_ids = sorted({r["airport_id"] for r in plan["mappings"]} - set(airport_keys))
        if missing_ids:
            raise RuntimeError(f"No Dim_Airport surrogate key for mapped IDs: {missing_ids[:10]}")

        mapping_rows = []
        for rec in plan["mappings"]:
            valid_to = date.fromisoformat(rec["valid_to"]) if rec["valid_to"] else None
            mapping_rows.append((
                rec["code"], airport_keys[rec["airport_id"]], rec["airport_id"],
                rec["match_status"], rec["original_status"], rec["resolved_status"],
                rec["method"], rec["origin_rows"], rec["dest_rows"], rec["occurrences"], valid_to,
            ))
        mapping_sql = """
            INSERT INTO Map_BTS_Airport (
                BTSAirportCode, AirportKey, OurAirportsID, MatchStatus,
                OriginalMatchStatus, ResolvedMatchStatus, MappingMethod,
                OriginFlightRows, DestFlightRows, TotalOccurrences,
                HistoricalAliasValidTo
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                AirportKey = VALUES(AirportKey),
                OurAirportsID = VALUES(OurAirportsID),
                MatchStatus = VALUES(MatchStatus),
                OriginalMatchStatus = VALUES(OriginalMatchStatus),
                ResolvedMatchStatus = VALUES(ResolvedMatchStatus),
                MappingMethod = VALUES(MappingMethod),
                OriginFlightRows = VALUES(OriginFlightRows),
                DestFlightRows = VALUES(DestFlightRows),
                TotalOccurrences = VALUES(TotalOccurrences),
                HistoricalAliasValidTo = VALUES(HistoricalAliasValidTo)
        """
        insert_in_batches(cursor, conn, mapping_sql, mapping_rows, "Map_BTS_Airport")

        # Read-back validation from MySQL; fail the run if target totals differ.
        validation: dict[str, Any] = {}
        cursor.execute("SELECT COUNT(*) FROM Dim_Airline")
        validation["Dim_Airline_Rows"] = int(cursor.fetchone()[0])
        cursor.execute("SELECT COUNT(*) FROM Dim_Airport")
        validation["Dim_Airport_Rows"] = int(cursor.fetchone()[0])
        cursor.execute("SELECT COUNT(*) FROM Dim_Airport WHERE TimeZoneIANA IS NOT NULL")
        validation["Dim_Airport_Timezone_Rows"] = int(cursor.fetchone()[0])
        cursor.execute("SELECT COUNT(*) FROM Map_BTS_Airport")
        validation["Map_BTS_Airport_Rows"] = int(cursor.fetchone()[0])
        cursor.execute("SELECT COUNT(*) FROM Map_BTS_Airport WHERE MappingMethod = 'DIRECT_IATA_MATCH'")
        validation["Direct_Mapping_Rows"] = int(cursor.fetchone()[0])
        cursor.execute("SELECT COUNT(*) FROM Map_BTS_Airport WHERE MappingMethod = 'HISTORICAL_IATA_ALIAS'")
        validation["Historical_Mapping_Rows"] = int(cursor.fetchone()[0])
        cursor.execute("SELECT COUNT(*) FROM Map_BTS_Airport WHERE BTSAirportCode = 'PBI' AND OurAirportsID = 3766")
        validation["PBI_Historical_Alias_Rows"] = int(cursor.fetchone()[0])
        cursor.execute("""
            SELECT COUNT(*)
            FROM Map_BTS_Airport m
            JOIN Dim_Airport a
              ON a.AirportKey = m.AirportKey
             AND a.OurAirportsID = m.OurAirportsID
        """)
        validation["Mapping_FK_Join_Rows"] = int(cursor.fetchone()[0])

        expected = {
            "Dim_Airline_Rows": len(plan["airlines"]),
            "Dim_Airport_Rows": EXPECTED_AIRPORT_ROWS,
            "Dim_Airport_Timezone_Rows": EXPECTED_BTS_CODES,
            "Map_BTS_Airport_Rows": EXPECTED_BTS_CODES,
            "Direct_Mapping_Rows": EXPECTED_DIRECT_CODES,
            "Historical_Mapping_Rows": EXPECTED_HISTORICAL_CODES,
            "PBI_Historical_Alias_Rows": 1,
            "Mapping_FK_Join_Rows": EXPECTED_BTS_CODES,
        }
        failures = {key: (validation.get(key), val) for key, val in expected.items() if validation.get(key) != val}
        if failures:
            conn.commit()  # retain idempotent loaded batches for safe retry/diagnosis
            raise RuntimeError(f"Database read-back validation failed: {failures}")
        conn.commit()
        summary = dict(plan["summary"])
        summary.update({"Mode": "EXECUTE", "Result": "PASS", "DatabaseVersion": version, "DatabaseValidation": validation})
        return summary
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="STEP 30C - load Flight DW dimensions")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--preview", action="store_true", help="Read and validate all source files; no MySQL writes (default)")
    mode.add_argument("--execute", action="store_true", help="Load dimensions into MySQL 8.0")
    parser.add_argument("--host", default=None, help="MySQL host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=None, help="MySQL port (default: 3306)")
    parser.add_argument("--user", default=None, help="MySQL user (default: root)")
    args = parser.parse_args()

    if not (ROOT / "04_CLEAN_DATA").is_dir() or not (ROOT / "06_DW_DESIGN").is_dir():
        say("ERROR: Run this script from the project root D:\\DATA_WAREHOUSE")
        return 2

    try:
        plan = prepare_plan()
        summary = plan["summary"]
        say("\nPREFLIGHT SUMMARY")
        say("-" * 78)
        for key, value in summary.items():
            say(f"{key}: {value}")
        say("\nSOURCE PREFLIGHT RESULT: PASS")

        if args.execute:
            summary = execute_load(plan, args)
            txt_path, json_path = write_reports(summary, "EXECUTE")
            say("\nSTEP 30C EXECUTE RESULT: PASS")
        else:
            txt_path, json_path = write_reports(summary, "PREVIEW")
            say("\nSTEP 30C PREVIEW RESULT: PASS")
            say("No MySQL rows were inserted or updated.")

        say(f"Text report: {txt_path}")
        say(f"JSON report: {json_path}")
        say("STEP 30C COMPLETE")
        return 0
    except Exception as exc:
        say("\nSTEP 30C RESULT: FAIL")
        say(f"ERROR: {type(exc).__name__}: {exc}")
        say("No RAW/STAGING/CLEAN/GOLD source files were modified.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
