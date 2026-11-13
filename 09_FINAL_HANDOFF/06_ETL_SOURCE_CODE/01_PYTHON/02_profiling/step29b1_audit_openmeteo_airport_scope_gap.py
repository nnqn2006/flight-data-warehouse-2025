"""STEP 29B1 - Audit Open-Meteo airport scope gaps.

Reads only existing mappings and RAW/CLEAN weather CSV files.
Does not call the API and does not modify RAW, STAGING, or CLEAN.
Uses Python standard library only.
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MAPPING_PATH = ROOT / "02_DATA_PROFILING" / "02_OurAirports" / "13_bts_airport_mapping_resolved.csv"
AIRPORTS_CLEAN_PATH = ROOT / "04_CLEAN_DATA" / "OurAirports" / "airports_clean.csv"
RAW_ROOT = ROOT / "01_RAW_DATA" / "OpenMeteo"
RAW_CSV_DIR = RAW_ROOT / "csv"
CLEAN_DIR = ROOT / "04_CLEAN_DATA" / "OpenMeteo"
OUT_DIR = ROOT / "06_DW_DESIGN" / "01_LOGICAL_MODEL"

EXPECTED_COLUMNS = [
    "AirportCode", "time", "temperature_2m", "relative_humidity_2m",
    "precipitation", "wind_speed_10m", "wind_direction_10m",
    "wind_gusts_10m", "cloud_cover", "weather_code", "Latitude",
    "Longitude", "TimezoneRequested", "WeatherModelRequested",
]
EXPECTED_ROWS = 8760
EXPECTED_RAW_FIRST = "2025-01-01T00:00"
EXPECTED_RAW_LAST = "2025-12-31T23:00"
EXPECTED_CLEAN_FIRST = "2025-01-01T00:00:00Z"
EXPECTED_CLEAN_LAST = "2025-12-31T23:00:00Z"
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
SUMMARY_PATH = OUT_DIR / f"29B1_openmeteo_airport_scope_gap_summary_{STAMP}.txt"
DETAIL_PATH = OUT_DIR / f"29B1_openmeteo_airport_scope_gap_detail_{STAMP}.csv"


def read_csv_rows(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        yield from csv.DictReader(f)


def read_airport_master(path: Path) -> dict[str, dict[str, str]]:
    required = {"id", "latitude_deg", "longitude_deg"}
    result: dict[str, dict[str, str]] = {}
    for row in read_csv_rows(path):
        if not required.issubset(row):
            raise ValueError(
                f"OurAirports CLEAN is missing required columns. "
                f"Found: {list(row.keys())}"
            )
        airport_id = (row.get("id") or "").strip()
        if airport_id:
            result[airport_id] = row
    return result


def list_json_by_code(raw_root: Path) -> dict[str, Path]:
    result: dict[str, Path] = {}
    if not raw_root.exists():
        return result
    for path in raw_root.rglob("weather_*_2025.json"):
        match = re.fullmatch(r"weather_([A-Z0-9]{3})_2025\.json", path.name, re.IGNORECASE)
        if match:
            result[match.group(1).upper()] = path
    return result


def inspect_weather_csv(path: Path | None, group: str) -> dict[str, str]:
    blank = {
        f"{group}Exists": "False",
        f"{group}Rows": "0",
        f"{group}Columns": "0",
        f"{group}FirstTime": "",
        f"{group}LastTime": "",
        f"{group}SchemaOK": "False",
        f"{group}YearSequenceOK": "False",
        f"{group}SHA256": "",
        f"{group}Error": "",
        f"{group}TimezoneRequested": "",
        f"{group}WeatherModelRequested": "",
    }
    if path is None or not path.exists():
        return blank

    out = dict(blank)
    out[f"{group}Exists"] = "True"
    try:
        digest = hashlib.sha256()
        row_count = 0
        first_time = ""
        last_time = ""
        columns: list[str] = []
        timezone_requested = ""
        model_requested = ""
        with path.open("rb") as fb:
            for chunk in iter(lambda: fb.read(1024 * 1024), b""):
                digest.update(chunk)
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            columns = list(reader.fieldnames or [])
            for row in reader:
                row_count += 1
                t = (row.get("time") or "").strip()
                if row_count == 1:
                    first_time = t
                    timezone_requested = (row.get("TimezoneRequested") or "").strip()
                    model_requested = (row.get("WeatherModelRequested") or "").strip()
                last_time = t
        expected_first = EXPECTED_RAW_FIRST if group == "RAW" else EXPECTED_CLEAN_FIRST
        expected_last = EXPECTED_RAW_LAST if group == "RAW" else EXPECTED_CLEAN_LAST
        schema_ok = columns == EXPECTED_COLUMNS
        sequence_ok = (
            row_count == EXPECTED_ROWS
            and first_time.startswith(expected_first)
            and last_time.startswith(expected_last)
        )
        out.update({
            f"{group}Rows": str(row_count),
            f"{group}Columns": str(len(columns)),
            f"{group}FirstTime": first_time,
            f"{group}LastTime": last_time,
            f"{group}SchemaOK": str(schema_ok),
            f"{group}YearSequenceOK": str(sequence_ok),
            f"{group}SHA256": digest.hexdigest(),
            f"{group}TimezoneRequested": timezone_requested,
            f"{group}WeatherModelRequested": model_requested,
        })
        if not schema_ok:
            out[f"{group}Error"] = f"Schema mismatch: {columns}"
        elif not sequence_ok:
            out[f"{group}Error"] = "Row count or time boundary mismatch"
    except Exception as exc:
        out[f"{group}Error"] = f"{type(exc).__name__}: {exc}"
    return out


def safe_int(value: str | None) -> int:
    try:
        return int(float(value or "0"))
    except (ValueError, TypeError):
        return 0


def main() -> int:
    for path in (MAPPING_PATH, AIRPORTS_CLEAN_PATH, RAW_CSV_DIR, CLEAN_DIR):
        if not path.exists():
            raise FileNotFoundError(f"Required path not found: {path}")

    mapping_rows = list(read_csv_rows(MAPPING_PATH))
    airport_master = read_airport_master(AIRPORTS_CLEAN_PATH)
    json_by_code = list_json_by_code(RAW_ROOT)

    if not mapping_rows:
        raise ValueError("Airport mapping resolved file is empty")

    mapped_codes = [((r.get("IATACode") or "").strip().upper()) for r in mapping_rows]
    mapped_codes = [c for c in mapped_codes if c]
    if len(mapped_codes) != len(set(mapped_codes)):
        raise ValueError("IATACode is not unique in resolved BTS airport mapping")

    raw_files = {p.name.removeprefix("weather_").removesuffix("_2025.csv").upper(): p
                 for p in RAW_CSV_DIR.glob("weather_*_2025.csv")}
    clean_files = {p.name.removeprefix("weather_").removesuffix("_2025.csv").upper(): p
                   for p in CLEAN_DIR.glob("weather_*_2025.csv")}

    rows_out: list[dict[str, str]] = []
    raw_valid_codes: set[str] = set()
    clean_valid_codes: set[str] = set()
    missing_raw_codes: list[str] = []
    missing_clean_codes: list[str] = []
    missing_any_codes: list[str] = []
    no_coord_codes: list[str] = []

    for m in sorted(mapping_rows, key=lambda x: (x.get("IATACode") or "")):
        code = (m.get("IATACode") or "").strip().upper()
        airport_id = (m.get("AirportID") or "").strip()
        master = airport_master.get(airport_id, {})
        raw = inspect_weather_csv(raw_files.get(code), "RAW")
        clean = inspect_weather_csv(clean_files.get(code), "CLEAN")
        raw_ok = (
            raw["RAWExists"] == "True" and raw["RAWSchemaOK"] == "True"
            and raw["RAWYearSequenceOK"] == "True"
        )
        clean_ok = (
            clean["CLEANExists"] == "True"
            and clean["CLEANRows"] == str(EXPECTED_ROWS)
            and clean["CLEANSchemaOK"] == "True"
            and clean["CLEANYearSequenceOK"] == "True"
        )
        if raw_ok:
            raw_valid_codes.add(code)
        if clean_ok:
            clean_valid_codes.add(code)
        if not raw_ok:
            missing_raw_codes.append(code)
        if not clean_ok:
            missing_clean_codes.append(code)
        if not (raw_ok and clean_ok):
            missing_any_codes.append(code)
        lat = (master.get("latitude_deg") or "").strip()
        lon = (master.get("longitude_deg") or "").strip()
        coord_ok = False
        try:
            lat_f, lon_f = float(lat), float(lon)
            coord_ok = -90 <= lat_f <= 90 and -180 <= lon_f <= 180
        except ValueError:
            pass
        if not coord_ok:
            no_coord_codes.append(code)

        status_parts = []
        if raw_ok and clean_ok:
            status = "RAW_AND_CLEAN_VALID"
        else:
            if not raw_ok:
                status_parts.append("RAW_MISSING_OR_INVALID")
            if not clean_ok:
                status_parts.append("CLEAN_MISSING_OR_INVALID")
            status = ";".join(status_parts)

        row = {
            "AirportCode": code,
            "BTS_AirportID": airport_id,
            "OurAirports_ID": airport_id,
            "AirportIdent": (m.get("AirportIdent") or master.get("ident") or "").strip(),
            "AirportName": (m.get("AirportName") or master.get("name") or "").strip(),
            "Municipality": (m.get("Municipality") or master.get("municipality") or "").strip(),
            "CountryCode": (m.get("CountryCode") or master.get("iso_country") or "").strip(),
            "RegionCode": (m.get("RegionCode") or master.get("iso_region") or "").strip(),
            "Latitude": lat,
            "Longitude": lon,
            "OriginFlightRows": str(safe_int(m.get("OriginFlightRows"))),
            "DestFlightRows": str(safe_int(m.get("DestFlightRows"))),
            "AirportOccurrences": str(safe_int(m.get("TotalOccurrences"))),
            "AirportType": (m.get("AirportType") or master.get("type") or "").strip(),
            "MappingMethod": (m.get("MappingMethod") or "").strip(),
            "HistoricalAliasValidTo": (m.get("HistoricalAliasValidTo") or "").strip(),
            "RAW_CSV_Path": str(raw_files.get(code, "")),
            "RAW_JSON_Path": str(json_by_code.get(code, "")),
            "CLEAN_CSV_Path": str(clean_files.get(code, "")),
            "HasRAWJSON": str(code in json_by_code),
            "RAW_Valid": str(raw_ok),
            "CLEAN_Valid": str(clean_ok),
            "CoordinatesValid": str(coord_ok),
            "Status": status,
        }
        row.update(raw)
        row.update(clean)
        rows_out.append(row)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fields = list(rows_out[0].keys())
    with DETAIL_PATH.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows_out)

    all_codes_sorted = sorted(mapped_codes)
    weather_valid_sorted = sorted(raw_valid_codes)
    prefix_match = weather_valid_sorted == all_codes_sorted[:len(weather_valid_sorted)]
    total_occurrences = sum(safe_int(m.get("TotalOccurrences")) for m in mapping_rows)
    missing_occurrences = sum(
        safe_int(m.get("TotalOccurrences")) for m in mapping_rows
        if (m.get("IATACode") or "").strip().upper() in set(missing_any_codes)
    )

    lines = [
        "STEP 29B1 - OPEN-METEO AIRPORT SCOPE GAP AUDIT",
        "=" * 96,
        f"Run timestamp: {STAMP}",
        f"BTS airport mapping: {MAPPING_PATH}",
        f"OurAirports CLEAN: {AIRPORTS_CLEAN_PATH}",
        f"RAW Open-Meteo CSV: {RAW_CSV_DIR}",
        f"CLEAN Open-Meteo CSV: {CLEAN_DIR}",
        "",
        "INVENTORY",
        "-" * 96,
        f"Distinct BTS IATA codes: {len(mapped_codes)}",
        f"RAW CSV files found: {len(raw_files)}",
        f"RAW JSON files found recursively: {len(json_by_code)}",
        f"CLEAN CSV files found: {len(clean_files)}",
        f"RAW CSV structurally valid (schema, 8,760 rows, year endpoints): {len(raw_valid_codes)}",
        f"CLEAN CSV structurally valid (schema, 8,760 rows, year endpoints): {len(clean_valid_codes)}",
        f"Codes missing/invalid RAW CSV: {len(missing_raw_codes)}",
        f"Codes missing/invalid CLEAN CSV: {len(missing_clean_codes)}",
        f"Codes missing/invalid in either RAW or CLEAN: {len(missing_any_codes)}",
        f"Codes with invalid/missing OurAirports coordinates: {len(no_coord_codes)}",
        "",
        "SCOPE GAP IMPACT",
        "-" * 96,
        f"Total BTS endpoint airport occurrences: {total_occurrences:,}",
        f"Endpoint occurrences for codes missing/invalid RAW or CLEAN: {missing_occurrences:,}",
        f"Share of endpoint occurrences affected: {missing_occurrences / total_occurrences * 100 if total_occurrences else 0:.4f}%",
        f"Valid RAW weather codes are exactly alphabetical prefix of BTS codes: {prefix_match}",
        f"Last valid RAW code alphabetically: {weather_valid_sorted[-1] if weather_valid_sorted else 'NONE'}",
        f"Next BTS code alphabetically: {all_codes_sorted[len(weather_valid_sorted)] if len(weather_valid_sorted) < len(all_codes_sorted) else 'NONE'}",
        "",
        "MISSING / INVALID AIRPORT CODES",
        "-" * 96,
        ", ".join(sorted(missing_any_codes)) if missing_any_codes else "None",
        "",
        "AIRPORTS WITHOUT VALID COORDINATES",
        "-" * 96,
        ", ".join(sorted(no_coord_codes)) if no_coord_codes else "None",
        "",
        "IMPORTANT INTERPRETATION",
        "-" * 96,
        "- This audit does not call Open-Meteo and does not modify any source data.",
        "- Missing scope means a BTS airport code lacks a fully valid RAW CSV and CLEAN CSV pair.",
        "- A structurally valid weather CSV is checked for schema, row count, and first/last UTC timestamps.",
        "- Historical full-year timestamp continuity was separately validated in STEP 29B for the existing 240 files.",
        "- Alphabetical-prefix pattern is evidence of a selection/truncation rule, not proof of which crawler line caused it.",
        "- Check historical alias codes (including PBI) through AirportID, not current IATA string alone.",
        "- Review coordinates and mapping rows before incremental download.",
        "",
        "OUTPUT",
        "-" * 96,
        str(DETAIL_PATH),
    ]
    SUMMARY_PATH.write_text("\n".join(lines), encoding="utf-8")

    print("STEP 29B1 FINISHED")
    print(f"BTS airport codes: {len(mapped_codes)}")
    print(f"RAW CSV valid: {len(raw_valid_codes)}")
    print(f"CLEAN CSV valid: {len(clean_valid_codes)}")
    print(f"Missing/invalid either layer: {len(missing_any_codes)}")
    print(f"Coordinates missing/invalid: {len(no_coord_codes)}")
    print(f"Summary: {SUMMARY_PATH}")
    print(f"Detail: {DETAIL_PATH}")
    print("RAW / STAGING / CLEAN were read only. No API request was made.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"STEP 29B1 FAILED: {type(exc).__name__}: {exc}")
        raise
