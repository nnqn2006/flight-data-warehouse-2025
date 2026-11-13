"""
STEP 29B3 - INCREMENTAL OPEN-METEO CLEAN + VALIDATION (CSV-COMPATIBLE)

Purpose
-------
Clean only the 112 newly crawled Open-Meteo airports from STEP 29B2 and append
those 112 CLEAN CSV files to the existing 240-file CLEAN dataset.

Read-only inputs:
    01_RAW_DATA/OpenMeteo/csv/weather_{IATA}_2025.csv
    01_RAW_DATA/OpenMeteo/json/weather_{IATA}_2025.json
    02_DATA_PROFILING/03_OpenMeteo/29b2_incremental_openmeteo_crawl_manifest_*.csv
    Existing 240 CLEAN CSV files (SHA256 baseline only)

New outputs:
    04_CLEAN_DATA/OpenMeteo/weather_{IATA}_2025.csv  (only the 112 missing codes)
    04_CLEAN_DATA/_CONTROL/29b3_incremental_openmeteo_clean_*.csv/.txt

Safety contract
---------------
- Does NOT import pyarrow or fastparquet.
- Uses pandas/numpy, matching STEP 28C canonicalization rules.
- RAW CSV, RAW JSON, existing 240 CLEAN files, and STAGING are read-only.
- Refuses to overwrite any existing CLEAN target or control report.
- Requires exactly 240 existing CLEAN files and 112 new manifest targets.
- Validates every RAW CSV against its JSON hourly arrays before cleaning.
- Preserves 14 columns and airport-hour grain; timestamps get trailing Z.
- No clipping, imputation, aggregation, local-time conversion, or code remapping.
- Uses a temporary folder, read-back checks, source hashes, and rollback of
  only newly promoted files if promotion fails.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


# =============================================================================
# 1. PROJECT PATHS AND DATA CONTRACT
# =============================================================================

ROOT = Path(r"D:\DATA_WAREHOUSE")
RAW_CSV_DIR = ROOT / "01_RAW_DATA" / "OpenMeteo" / "csv"
RAW_JSON_DIR = ROOT / "01_RAW_DATA" / "OpenMeteo" / "json"
PROFILE_DIR = ROOT / "02_DATA_PROFILING" / "03_OpenMeteo"
CLEAN_ROOT = ROOT / "04_CLEAN_DATA"
CLEAN_DIR = CLEAN_ROOT / "OpenMeteo"
CONTROL_DIR = CLEAN_ROOT / "_CONTROL"

EXPECTED_EXISTING_CLEAN_FILES = 240
EXPECTED_NEW_AIRPORTS = 112
EXPECTED_ROWS_PER_AIRPORT = 8760
EXPECTED_NEW_TOTAL_ROWS = EXPECTED_NEW_AIRPORTS * EXPECTED_ROWS_PER_AIRPORT
EXPECTED_FIRST_TIME = "2025-01-01T00:00"
EXPECTED_LAST_TIME = "2025-12-31T23:00"
EXPECTED_REQUESTED_MODEL = "ecmwf_ifs"
EXPECTED_REQUESTED_TIMEZONE = "UTC"

EXPECTED_COLUMNS = [
    "AirportCode",
    "time",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "cloud_cover",
    "weather_code",
    "Latitude",
    "Longitude",
    "TimezoneRequested",
    "WeatherModelRequested",
]

HOURLY_COLUMNS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "cloud_cover",
    "weather_code",
]

FLOAT_COLUMNS = [
    "temperature_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_gusts_10m",
    "Latitude",
    "Longitude",
]

INTEGER_COLUMNS = [
    "relative_humidity_2m",
    "wind_direction_10m",
    "cloud_cover",
    "weather_code",
]

STRING_COLUMNS = ["AirportCode", "TimezoneRequested", "WeatherModelRequested"]

# Keep this domain list identical to the completed STEP 28C validation contract.
WEATHER_CODES = {
    0, 1, 2, 3, 45,
    51, 53, 55,
    61, 63, 65,
    71, 73, 75,
    80, 81, 82,
    85, 86,
    95, 96,
}

MANIFEST_COLUMNS = [
    "AirportCode", "OurAirports_ID", "MappingMethod", "HistoricalAliasValidTo",
    "LatitudeRequested", "LongitudeRequested", "AirportOccurrences",
    "Result", "RowsReturned", "FirstTime", "LastTime", "TimezoneReturned",
    "UTCOffsetSeconds", "LatitudeReturned", "LongitudeReturned",
    "RAW_CSV_Path", "RAW_JSON_Path", "Error",
]

RUN_STAMP = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
TEMP_DIR = CLEAN_ROOT / f"_TEMP_STEP29B3_OpenMeteo_{RUN_STAMP}"
SUMMARY_PATH = CONTROL_DIR / f"29b3_incremental_openmeteo_clean_summary_{RUN_STAMP}.txt"
MANIFEST_OUT_PATH = CONTROL_DIR / f"29b3_incremental_openmeteo_clean_manifest_{RUN_STAMP}.csv"
MISSING_OUT_PATH = CONTROL_DIR / f"29b3_incremental_openmeteo_clean_missing_profile_{RUN_STAMP}.csv"
LOG_PATH = CONTROL_DIR / f"step29b3_incremental_openmeteo_clean_{RUN_STAMP}.log"


# =============================================================================
# 2. LOGGING AND GENERAL HELPERS
# =============================================================================

def make_logger() -> logging.Logger:
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("step29b3_incremental_openmeteo_clean")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    stream_handler = logging.StreamHandler(sys.stdout)
    for handler in (file_handler, stream_handler):
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def expected_utc_strings() -> list[str]:
    return pd.date_range(
        start="2025-01-01 00:00:00",
        periods=EXPECTED_ROWS_PER_AIRPORT,
        freq="h",
        tz="UTC",
    ).strftime("%Y-%m-%dT%H:%M").tolist()


def file_airport_code(path: Path) -> str:
    match = re.fullmatch(r"weather_([A-Z0-9]{3})_2025\.csv", path.name)
    if not match:
        raise ValueError(f"Unexpected airport CSV filename: {path.name}")
    return match.group(1)


def find_latest_manifest(explicit_path: Path | None) -> Path:
    if explicit_path is not None:
        path = explicit_path.resolve()
        if not path.is_file():
            raise FileNotFoundError(f"STEP 29B2 manifest not found: {path}")
        return path
    candidates = sorted(
        PROFILE_DIR.glob("29b2_incremental_openmeteo_crawl_manifest_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(
            f"No STEP 29B2 manifest found in {PROFILE_DIR}. "
            "Use --manifest to specify the exact file."
        )
    return candidates[0]


def read_manifest(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(
        path,
        dtype="string",
        encoding="utf-8-sig",
        keep_default_na=False,
        na_filter=False,
    )
    if list(frame.columns) != MANIFEST_COLUMNS:
        raise ValueError(
            "STEP 29B2 manifest schema/order mismatch.\n"
            f"Expected: {MANIFEST_COLUMNS}\nActual: {list(frame.columns)}"
        )
    if len(frame) != EXPECTED_NEW_AIRPORTS:
        raise ValueError(
            f"Expected {EXPECTED_NEW_AIRPORTS} manifest rows; found {len(frame)}"
        )
    if frame["AirportCode"].duplicated().any():
        duplicates = frame.loc[frame["AirportCode"].duplicated(), "AirportCode"].tolist()
        raise ValueError(f"Duplicate airport codes in manifest: {duplicates}")
    if not frame["AirportCode"].str.fullmatch(r"[A-Z0-9]{3}").all():
        raise ValueError("Manifest contains invalid AirportCode values")
    if not frame["Result"].eq("SUCCESS").all():
        raise ValueError("Manifest has one or more non-SUCCESS results")
    if not frame["Error"].eq("").all():
        raise ValueError("Manifest contains non-empty Error values")

    numeric_checks = {
        "RowsReturned": EXPECTED_ROWS_PER_AIRPORT,
        "UTCOffsetSeconds": 0,
    }
    for column, expected in numeric_checks.items():
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.isna().any() or not values.eq(expected).all():
            raise ValueError(f"Manifest {column} must equal {expected} for all rows")

    if not frame["FirstTime"].eq(EXPECTED_FIRST_TIME).all():
        raise ValueError("Manifest FirstTime mismatch")
    if not frame["LastTime"].eq(EXPECTED_LAST_TIME).all():
        raise ValueError("Manifest LastTime mismatch")
    if not frame["TimezoneReturned"].isin(["GMT", "UTC"]).all():
        raise ValueError("Manifest must report GMT/UTC for every airport")

    for column in ["LatitudeRequested", "LongitudeRequested", "LatitudeReturned", "LongitudeReturned"]:
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.isna().any() or not np.isfinite(values.to_numpy(dtype="float64")).all():
            raise ValueError(f"Manifest has invalid coordinates in {column}")
    if not pd.to_numeric(frame["AirportOccurrences"], errors="coerce").gt(0).all():
        raise ValueError("Manifest AirportOccurrences must be positive")

    # Preserve the historical IATA mapping used by the BTS source records.
    pbi = frame.loc[frame["AirportCode"].eq("PBI")]
    if len(pbi) != 1:
        raise ValueError("Expected exactly one PBI manifest row (historical alias)")
    pbi_row = pbi.iloc[0]
    if pbi_row["MappingMethod"] != "HISTORICAL_IATA_ALIAS":
        raise ValueError("PBI must retain HISTORICAL_IATA_ALIAS mapping")
    if pbi_row["OurAirports_ID"] != "3766":
        raise ValueError("PBI historical alias must map to OurAirports_ID 3766")
    if pbi_row["HistoricalAliasValidTo"] != "2026-08-17":
        raise ValueError("PBI historical alias validity date mismatch")

    direct_count = int(frame["MappingMethod"].eq("DIRECT_IATA_MATCH").sum())
    alias_count = int(frame["MappingMethod"].eq("HISTORICAL_IATA_ALIAS").sum())
    if direct_count != 111 or alias_count != 1:
        raise ValueError(
            "Unexpected mapping method distribution; expected 111 direct + 1 alias, "
            f"found {direct_count} + {alias_count}"
        )
    return frame


def resolve_and_check_manifest_paths(
    manifest: pd.DataFrame,
    manifest_path: Path,
) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    for _, row in manifest.iterrows():
        code = str(row["AirportCode"])
        expected_csv = RAW_CSV_DIR / f"weather_{code}_2025.csv"
        expected_json = RAW_JSON_DIR / f"weather_{code}_2025.json"
        csv_path = Path(str(row["RAW_CSV_Path"]))
        json_path = Path(str(row["RAW_JSON_Path"]))
        if not csv_path.is_file():
            raise FileNotFoundError(f"RAW CSV missing for {code}: {csv_path}")
        if not json_path.is_file():
            raise FileNotFoundError(f"RAW JSON missing for {code}: {json_path}")
        if csv_path.resolve() != expected_csv.resolve():
            raise ValueError(f"Manifest RAW CSV path mismatch for {code}: {csv_path}")
        if json_path.resolve() != expected_json.resolve():
            raise ValueError(f"Manifest RAW JSON path mismatch for {code}: {json_path}")
        clean_path = CLEAN_DIR / f"weather_{code}_2025.csv"
        if clean_path.exists():
            raise FileExistsError(
                f"Refusing to overwrite existing CLEAN target for {code}: {clean_path}"
            )
        targets.append({
            "AirportCode": code,
            "manifest_row": row.to_dict(),
            "raw_csv": csv_path,
            "raw_json": json_path,
            "clean_path": clean_path,
        })

    return targets


def inventory_existing_clean() -> tuple[list[Path], dict[str, str]]:
    if not CLEAN_DIR.is_dir():
        raise FileNotFoundError(f"Existing CLEAN directory not found: {CLEAN_DIR}")
    files = sorted(CLEAN_DIR.glob("weather_*_2025.csv"))
    if len(files) != EXPECTED_EXISTING_CLEAN_FILES:
        raise ValueError(
            "Expected exactly 240 pre-existing CLEAN files before incremental append; "
            f"found {len(files)}"
        )
    codes = [file_airport_code(path) for path in files]
    if len(set(codes)) != EXPECTED_EXISTING_CLEAN_FILES:
        raise ValueError("Duplicate airport codes in existing CLEAN inventory")
    hashes = {path.name: sha256_file(path) for path in files}
    return files, hashes


def check_control_outputs_available() -> None:
    for path in (SUMMARY_PATH, MANIFEST_OUT_PATH, MISSING_OUT_PATH):
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite control output: {path}")
    if TEMP_DIR.exists():
        raise FileExistsError(f"Temporary directory already exists: {TEMP_DIR}")


# =============================================================================
# 3. RAW CSV + JSON VALIDATION
# =============================================================================

def read_raw_csv(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(
        path,
        dtype="string",
        encoding="utf-8-sig",
        keep_default_na=False,
        na_filter=False,
        low_memory=False,
    )
    if list(frame.columns) != EXPECTED_COLUMNS:
        raise ValueError(
            f"{path.name}: RAW schema/order mismatch.\n"
            f"Expected: {EXPECTED_COLUMNS}\nActual: {list(frame.columns)}"
        )
    if len(frame) != EXPECTED_ROWS_PER_AIRPORT:
        raise ValueError(
            f"{path.name}: expected {EXPECTED_ROWS_PER_AIRPORT:,} rows; found {len(frame):,}"
        )
    blank_counts = frame.eq("").sum()
    blanks = blank_counts[blank_counts > 0]
    if not blanks.empty:
        raise ValueError(f"{path.name}: blank RAW cells detected: {blanks.to_dict()}")
    return frame


def validate_csv_json_pair(
    source: pd.DataFrame,
    json_path: Path,
    manifest_row: dict[str, Any],
    airport_code: str,
) -> None:
    try:
        payload = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"{json_path.name}: invalid JSON: {exc}") from exc

    if not isinstance(payload, dict) or not isinstance(payload.get("hourly"), dict):
        raise ValueError(f"{json_path.name}: unexpected JSON structure")
    if payload.get("timezone") not in ("GMT", "UTC"):
        raise ValueError(f"{json_path.name}: timezone must be GMT/UTC")
    if payload.get("utc_offset_seconds") != 0:
        raise ValueError(f"{json_path.name}: utc_offset_seconds must be 0")

    for key, manifest_key in (
        ("latitude", "LatitudeReturned"),
        ("longitude", "LongitudeReturned"),
    ):
        try:
            value = float(payload[key])
            manifest_value = float(manifest_row[manifest_key])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"{json_path.name}: invalid {key} metadata") from exc
        if not np.isfinite(value) or not np.isclose(value, manifest_value, rtol=0, atol=1e-6):
            raise ValueError(
                f"{json_path.name}: {key} does not agree with STEP 29B2 manifest"
            )

    hourly = payload["hourly"]
    expected_hourly_keys = ["time"] + HOURLY_COLUMNS
    if set(hourly.keys()) != set(expected_hourly_keys):
        raise ValueError(
            f"{json_path.name}: hourly keys mismatch; found {sorted(hourly.keys())}"
        )

    expected_times = expected_utc_strings()
    json_times = hourly.get("time")
    if not isinstance(json_times, list) or json_times != expected_times:
        raise ValueError(f"{json_path.name}: JSON UTC time sequence mismatch")

    csv_times = source["time"].astype(str).tolist()
    if csv_times != expected_times:
        raise ValueError(f"{airport_code}: RAW CSV UTC time sequence mismatch")

    if not source["AirportCode"].eq(airport_code).all():
        raise ValueError(f"{airport_code}: AirportCode does not match file/manifest")
    if not source["TimezoneRequested"].eq(EXPECTED_REQUESTED_TIMEZONE).all():
        raise ValueError(f"{airport_code}: TimezoneRequested must be UTC")
    if not source["WeatherModelRequested"].eq(EXPECTED_REQUESTED_MODEL).all():
        raise ValueError(
            f"{airport_code}: WeatherModelRequested must be {EXPECTED_REQUESTED_MODEL}"
        )

    # RAW CSV coordinates should be the requested airport coordinates, while
    # the JSON latitude/longitude are Open-Meteo's returned grid-cell coordinates.
    for csv_column, manifest_key in (
        ("Latitude", "LatitudeRequested"),
        ("Longitude", "LongitudeRequested"),
    ):
        values = pd.to_numeric(source[csv_column], errors="coerce").to_numpy(dtype="float64")
        if not np.isfinite(values).all() or not np.allclose(values, values[0], rtol=0, atol=1e-12):
            raise ValueError(f"{airport_code}: invalid or changing {csv_column} in RAW CSV")
        expected_value = float(manifest_row[manifest_key])
        if not np.isclose(values[0], expected_value, rtol=0, atol=1e-6):
            raise ValueError(
                f"{airport_code}: RAW CSV {csv_column} differs from requested coordinate"
            )

    # Compare all 8 hourly series in JSON against the corresponding RAW CSV.
    for column in HOURLY_COLUMNS:
        json_values = hourly.get(column)
        if not isinstance(json_values, list) or len(json_values) != EXPECTED_ROWS_PER_AIRPORT:
            raise ValueError(f"{json_path.name}: {column} must contain 8,760 values")
        if any(value is None for value in json_values):
            raise ValueError(f"{json_path.name}: null values found in {column}")
        try:
            json_numeric = np.asarray(json_values, dtype="float64")
            csv_numeric = pd.to_numeric(source[column], errors="coerce").to_numpy(dtype="float64")
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{airport_code}: non-numeric JSON/CSV values in {column}") from exc
        if (
            not np.isfinite(json_numeric).all()
            or not np.isfinite(csv_numeric).all()
            or not np.allclose(json_numeric, csv_numeric, rtol=0, atol=1e-12)
        ):
            raise ValueError(f"{airport_code}: RAW CSV/JSON value mismatch in {column}")


def parse_and_validate(
    source: pd.DataFrame,
    airport_code: str,
    source_name: str,
) -> pd.DataFrame:
    """Apply STEP 28C canonicalization rules without altering the data values."""
    if list(source.columns) != EXPECTED_COLUMNS:
        raise ValueError(f"{source_name}: unexpected column order")

    clean = source.copy()
    for column in STRING_COLUMNS:
        clean[column] = clean[column].astype("string").str.strip()

    if not clean["AirportCode"].eq(airport_code).all():
        raise ValueError(f"{source_name}: AirportCode does not match filename {airport_code}")
    if not clean["AirportCode"].str.fullmatch(r"[A-Z0-9]{3}").all():
        raise ValueError(f"{source_name}: invalid AirportCode format")
    if not clean["TimezoneRequested"].eq("UTC").all():
        raise ValueError(f"{source_name}: TimezoneRequested must be UTC")
    if clean["WeatherModelRequested"].eq("").any():
        raise ValueError(f"{source_name}: WeatherModelRequested contains blanks")
    if clean["WeatherModelRequested"].nunique(dropna=False) != 1:
        raise ValueError(f"{source_name}: WeatherModelRequested changes within file")

    parsed_time = pd.to_datetime(clean["time"], format="%Y-%m-%dT%H:%M", errors="coerce")
    if parsed_time.isna().any():
        raise ValueError(f"{source_name}: invalid timestamp values")
    parsed_time = parsed_time.dt.tz_localize("UTC")
    expected_time = pd.date_range(
        start="2025-01-01 00:00:00",
        periods=EXPECTED_ROWS_PER_AIRPORT,
        freq="h",
        tz="UTC",
    )
    if not np.array_equal(parsed_time.astype("int64").to_numpy(), expected_time.astype("int64")):
        raise ValueError(f"{source_name}: timestamps do not match full ordered 2025 UTC sequence")
    if parsed_time.duplicated().any():
        raise ValueError(f"{source_name}: duplicate UTC timestamps")
    clean["time"] = parsed_time.dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    for column in FLOAT_COLUMNS:
        values = pd.to_numeric(clean[column], errors="coerce")
        if values.isna().any() or not np.isfinite(values.to_numpy(dtype="float64")).all():
            raise ValueError(f"{source_name}: non-numeric or non-finite values in {column}")
        clean[column] = values.astype("float64")

    for column in INTEGER_COLUMNS:
        values = pd.to_numeric(clean[column], errors="coerce")
        if values.isna().any():
            raise ValueError(f"{source_name}: non-numeric values in {column}")
        numeric_values = values.to_numpy(dtype="float64")
        if not np.isfinite(numeric_values).all():
            raise ValueError(f"{source_name}: non-finite values in {column}")
        if not np.equal(numeric_values, np.floor(numeric_values)).all():
            raise ValueError(f"{source_name}: fractional values in {column}")
        clean[column] = values.astype("int64")

    checks = {
        "relative_humidity_2m": clean["relative_humidity_2m"].between(0, 100),
        "precipitation": clean["precipitation"].ge(0),
        "wind_speed_10m": clean["wind_speed_10m"].ge(0),
        "wind_direction_10m": clean["wind_direction_10m"].between(0, 360),
        "wind_gusts_10m": clean["wind_gusts_10m"].ge(0),
        "cloud_cover": clean["cloud_cover"].between(0, 100),
        "weather_code": clean["weather_code"].isin(WEATHER_CODES),
        "Latitude": clean["Latitude"].between(-90, 90),
        "Longitude": clean["Longitude"].between(-180, 180),
    }
    for column, valid_mask in checks.items():
        if not valid_mask.all():
            invalid_count = int((~valid_mask).sum())
            raise ValueError(f"{source_name}: {invalid_count} invalid values in {column}")

    if clean["Latitude"].nunique(dropna=False) != 1:
        raise ValueError(f"{source_name}: Latitude changes within airport file")
    if clean["Longitude"].nunique(dropna=False) != 1:
        raise ValueError(f"{source_name}: Longitude changes within airport file")

    return clean[EXPECTED_COLUMNS]


# =============================================================================
# 4. CLEAN WRITE-BACK VALIDATION
# =============================================================================

def write_clean_csv(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(
        path,
        index=False,
        encoding="utf-8-sig",
        lineterminator="\n",
        float_format="%.17g",
    )


def validate_clean_csv(
    clean_path: Path,
    source_frame: pd.DataFrame,
    airport_code: str,
) -> tuple[int, int, dict[str, int]]:
    read_back = pd.read_csv(
        clean_path,
        dtype="string",
        encoding="utf-8-sig",
        keep_default_na=False,
        na_filter=False,
        low_memory=False,
    )
    if list(read_back.columns) != EXPECTED_COLUMNS:
        raise ValueError(f"{clean_path.name}: CLEAN schema/order mismatch")
    if len(read_back) != EXPECTED_ROWS_PER_AIRPORT:
        raise ValueError(f"{clean_path.name}: CLEAN row count mismatch")
    blanks = read_back.eq("").sum()
    if blanks.any():
        raise ValueError(f"{clean_path.name}: blank CLEAN values: {blanks[blanks > 0].to_dict()}")
    if not read_back["AirportCode"].eq(airport_code).all():
        raise ValueError(f"{clean_path.name}: AirportCode mismatch")
    if not read_back["TimezoneRequested"].eq("UTC").all():
        raise ValueError(f"{clean_path.name}: timezone metadata changed")
    if not read_back["WeatherModelRequested"].eq(
        source_frame["WeatherModelRequested"].astype("string")
    ).all():
        raise ValueError(f"{clean_path.name}: request model metadata changed")

    parsed_time = pd.to_datetime(
        read_back["time"],
        format="%Y-%m-%dT%H:%M:%SZ",
        utc=True,
        errors="coerce",
    )
    if parsed_time.isna().any():
        raise ValueError(f"{clean_path.name}: invalid CLEAN UTC timestamp")
    expected_time = pd.date_range(
        start="2025-01-01 00:00:00",
        periods=EXPECTED_ROWS_PER_AIRPORT,
        freq="h",
        tz="UTC",
    )
    if not np.array_equal(parsed_time.astype("int64").to_numpy(), expected_time.astype("int64")):
        raise ValueError(f"{clean_path.name}: CLEAN UTC sequence mismatch")
    if parsed_time.duplicated().any():
        raise ValueError(f"{clean_path.name}: duplicate airport-hour keys")

    for column in FLOAT_COLUMNS + INTEGER_COLUMNS:
        source_numeric = pd.to_numeric(source_frame[column], errors="coerce").to_numpy(dtype="float64")
        clean_numeric = pd.to_numeric(read_back[column], errors="coerce").to_numpy(dtype="float64")
        if np.isnan(clean_numeric).any() or not np.isfinite(clean_numeric).all():
            raise ValueError(f"{clean_path.name}: invalid numeric read-back in {column}")
        if not np.allclose(source_numeric, clean_numeric, rtol=0.0, atol=1e-12):
            raise ValueError(f"{clean_path.name}: source-to-CLEAN mismatch in {column}")

    missing_counts = {column: int(read_back[column].eq("").sum()) for column in EXPECTED_COLUMNS}
    code96_count = int((pd.to_numeric(read_back["weather_code"], errors="coerce") == 96).sum())
    return len(read_back), code96_count, missing_counts


# =============================================================================
# 5. MAIN
# =============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="STEP 29B3: validate and clean only the 112 new Open-Meteo airports."
    )
    parser.add_argument(
        "--manifest", type=Path, default=None,
        help="Exact STEP 29B2 manifest. Defaults to the newest matching manifest.",
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Validate RAW CSV/JSON and create the 112 incremental CLEAN CSV files.",
    )
    args = parser.parse_args()

    logger = make_logger()
    logger.info("=" * 82)
    logger.info("STEP 29B3 - INCREMENTAL OPEN-METEO CLEAN + VALIDATION (CSV)")
    logger.info("Run timestamp: %s", RUN_STAMP)
    logger.info("Project root: %s", ROOT)
    logger.info("STAGING Parquet will not be opened or modified.")

    temp_created = False
    promoted: list[Path] = []
    try:
        manifest_path = find_latest_manifest(args.manifest)
        manifest = read_manifest(manifest_path)
        targets = resolve_and_check_manifest_paths(manifest, manifest_path)
        existing_clean_files, existing_clean_hashes_before = inventory_existing_clean()
        check_control_outputs_available()

        target_codes = {item["AirportCode"] for item in targets}
        existing_codes = {file_airport_code(path) for path in existing_clean_files}
        overlap = sorted(target_codes & existing_codes)
        if overlap:
            raise ValueError(f"New target codes already exist in CLEAN: {overlap}")

        print("=" * 88)
        print("STEP 29B3 - INCREMENTAL OPEN-METEO CLEAN")
        print("=" * 88)
        print(f"Project root       : {ROOT}")
        print(f"STEP 29B2 manifest : {manifest_path}")
        print(f"Existing CLEAN     : {len(existing_clean_files)} files (read-only)")
        print(f"New target airports: {len(targets)}")
        print(f"Expected new rows  : {EXPECTED_NEW_TOTAL_ROWS:,}")
        print(f"RAW CSV directory  : {RAW_CSV_DIR}")
        print(f"RAW JSON directory : {RAW_JSON_DIR}")
        print(f"CLEAN destination  : {CLEAN_DIR}")
        print("Mapping distribution: 111 DIRECT_IATA_MATCH + 1 HISTORICAL_IATA_ALIAS")
        print("Validation policy  : CSV/JSON reconciliation + STEP 28C rules + read-back")
        print("No PyArrow/Parquet operations will be performed.")

        if not args.execute:
            print("\nPREVIEW ONLY: 112 RAW CSV/JSON pairs exist, and all 112 CLEAN targets are absent.")
            print("No RAW, STAGING, or CLEAN data file was read for transformation or modified.")
            print("After reviewing this scope, run the same script with --execute.")
            logger.info("PREVIEW PASS | targets=%s | existing_clean=%s", len(targets), len(existing_clean_files))
            return 0

        TEMP_DIR.mkdir(parents=True, exist_ok=False)
        temp_created = True
        temp_data_dir = TEMP_DIR / "OpenMeteo"
        temp_data_dir.mkdir(parents=True, exist_ok=False)

        output_manifest_rows: list[dict[str, Any]] = []
        missing_profile_rows: list[dict[str, Any]] = []
        total_rows = 0
        total_code96 = 0
        total_json_csv_pairs_validated = 0

        for index, target in enumerate(targets, start=1):
            code = target["AirportCode"]
            raw_csv = target["raw_csv"]
            raw_json = target["raw_json"]
            clean_final = target["clean_path"]
            manifest_row = target["manifest_row"]
            temp_clean = temp_data_dir / clean_final.name

            logger.info("[%s/%s] Processing %s", index, len(targets), code)
            print(f"[{index:03d}/{len(targets):03d}] {code} ...", flush=True)

            raw_csv_sha_before = sha256_file(raw_csv)
            raw_json_sha_before = sha256_file(raw_json)

            source_frame = read_raw_csv(raw_csv)
            validate_csv_json_pair(source_frame, raw_json, manifest_row, code)
            total_json_csv_pairs_validated += 1

            clean_frame = parse_and_validate(source_frame, code, raw_csv.name)
            write_clean_csv(clean_frame, temp_clean)
            rows_read_back, code96_count, missing_counts = validate_clean_csv(
                temp_clean, source_frame, code
            )

            raw_csv_sha_after = sha256_file(raw_csv)
            raw_json_sha_after = sha256_file(raw_json)
            if raw_csv_sha_before != raw_csv_sha_after:
                raise RuntimeError(f"RAW CSV changed during processing: {raw_csv.name}")
            if raw_json_sha_before != raw_json_sha_after:
                raise RuntimeError(f"RAW JSON changed during processing: {raw_json.name}")

            for column in EXPECTED_COLUMNS:
                missing_profile_rows.append({
                    "AirportCode": code,
                    "ColumnName": column,
                    "MissingCount": missing_counts[column],
                    "MissingPct": missing_counts[column] / rows_read_back * 100,
                })

            output_manifest_rows.append({
                "AirportCode": code,
                "OurAirports_ID": manifest_row["OurAirports_ID"],
                "MappingMethod": manifest_row["MappingMethod"],
                "AirportOccurrences": int(manifest_row["AirportOccurrences"]),
                "SourceRAWCSV": raw_csv.name,
                "SourceRAWCSV_SHA256Before": raw_csv_sha_before,
                "SourceRAWCSV_SHA256After": raw_csv_sha_after,
                "SourceRAWCSV_Unchanged": raw_csv_sha_before == raw_csv_sha_after,
                "SourceRAWJSON": raw_json.name,
                "SourceRAWJSON_SHA256Before": raw_json_sha_before,
                "SourceRAWJSON_SHA256After": raw_json_sha_after,
                "SourceRAWJSON_Unchanged": raw_json_sha_before == raw_json_sha_after,
                "CleanFile": clean_final.name,
                "CleanSHA256": sha256_file(temp_clean),
                "RowsReadFromRAW": len(source_frame),
                "RowsReadBackFromCLEAN": rows_read_back,
                "Columns": len(clean_frame.columns),
                "WeatherCode96Rows": code96_count,
                "FirstTimestampUTC": clean_frame["time"].iloc[0],
                "LastTimestampUTC": clean_frame["time"].iloc[-1],
                "TimezoneRequested": EXPECTED_REQUESTED_TIMEZONE,
                "WeatherModelRequested": EXPECTED_REQUESTED_MODEL,
                "CSV_JSON_Reconciliation": "PASS",
                "Result": "PASS",
            })
            total_rows += rows_read_back
            total_code96 += code96_count
            logger.info(
                "%s PASS | rows=%s | code96=%s | CSV/JSON match | RAW unchanged",
                code, f"{rows_read_back:,}", code96_count,
            )

        # Global validation of the entire incremental batch.
        if len(output_manifest_rows) != EXPECTED_NEW_AIRPORTS:
            raise ValueError(f"Expected {EXPECTED_NEW_AIRPORTS} CLEAN outputs; got {len(output_manifest_rows)}")
        if total_rows != EXPECTED_NEW_TOTAL_ROWS:
            raise ValueError(f"Expected {EXPECTED_NEW_TOTAL_ROWS:,} rows; got {total_rows:,}")
        if total_json_csv_pairs_validated != EXPECTED_NEW_AIRPORTS:
            raise ValueError("Not all 112 RAW CSV/JSON pairs passed reconciliation")
        if not all(row["SourceRAWCSV_Unchanged"] and row["SourceRAWJSON_Unchanged"] for row in output_manifest_rows):
            raise RuntimeError("At least one RAW source hash changed")

        # Verify the original 240 CLEAN files did not change during this step.
        existing_clean_hashes_after = {
            path.name: sha256_file(path) for path in existing_clean_files
        }
        if existing_clean_hashes_before != existing_clean_hashes_after:
            changed = sorted(
                name for name in existing_clean_hashes_before
                if existing_clean_hashes_before[name] != existing_clean_hashes_after.get(name)
            )
            raise RuntimeError(f"Existing 240 CLEAN files changed during STEP 29B3: {changed}")

        # Prepare all control reports in the temporary directory before promotion.
        manifest_out = pd.DataFrame(output_manifest_rows)
        missing_out = pd.DataFrame(missing_profile_rows)
        temp_manifest = TEMP_DIR / MANIFEST_OUT_PATH.name
        temp_missing = TEMP_DIR / MISSING_OUT_PATH.name
        temp_summary = TEMP_DIR / SUMMARY_PATH.name

        manifest_out.to_csv(temp_manifest, index=False, encoding="utf-8-sig")
        missing_out.to_csv(temp_missing, index=False, encoding="utf-8-sig")

        summary_lines = [
            "STEP 29B3 - INCREMENTAL OPEN-METEO CLEAN SUMMARY",
            "=" * 82,
            f"Run timestamp: {RUN_STAMP}",
            f"STEP 29B2 manifest: {manifest_path}",
            f"Existing CLEAN files before/after: {len(existing_clean_files)} / {len(existing_clean_files)}",
            f"New airport files prepared: {len(output_manifest_rows)}",
            f"Expected rows per airport: {EXPECTED_ROWS_PER_AIRPORT:,}",
            f"Total new CLEAN rows read back: {total_rows:,}",
            f"RAW CSV/JSON pairs reconciled: {total_json_csv_pairs_validated}/{EXPECTED_NEW_AIRPORTS}",
            f"WeatherCode=96 rows in new batch: {total_code96}",
            f"Mapping: 111 direct + 1 historical alias (PBI -> OurAirports_ID 3766)",
            "",
            "VALIDATION CHECKS:",
            "manifest_schema_and_112_success_rows: PASS",
            "manifest_timestamps_rows_timezone_and_offsets: PASS",
            "all_112_raw_csv_and_json_files_exist: PASS",
            "raw_csv_schema_and_column_order_14: PASS",
            "raw_csv_rows_per_airport_8760: PASS",
            "raw_json_structure_and_utc_sequence: PASS",
            "raw_csv_json_hourly_values_match: PASS",
            "airport_code_matches_filename_and_manifest: PASS",
            "all_source_cells_nonblank: PASS",
            "all_numeric_fields_parse_and_are_finite: PASS",
            "integer_fields_have_no_fractional_values: PASS",
            "weather_code_domain_valid: PASS",
            "weather_numeric_domains_valid: PASS",
            "latitude_longitude_ranges_valid: PASS",
            "timezone_requested_is_UTC: PASS",
            "timestamps_are_complete_ordered_UTC_hours: PASS",
            "no_duplicate_airport_hour_keys: PASS",
            "clean_csv_read_back_schema_and_rows: PASS",
            "clean_numeric_values_match_RAW: PASS",
            "weather_code_96_preserved: PASS",
            "all_112_new_clean_files_prepared: PASS",
            "all_RAW_CSV_SHA256_unchanged: PASS",
            "all_RAW_JSON_SHA256_unchanged: PASS",
            "existing_240_CLEAN_SHA256_unchanged: PASS",
            "",
            "DATA POLICY:",
            "- RAW CSV and RAW JSON were read-only.",
            "- Existing 240 CLEAN CSV files were read-only and SHA256-verified.",
            "- Existing STAGING Parquet files were not opened or modified.",
            "- CLEAN preserves the 14 source columns and airport-hour grain.",
            "- One row represents one airport at one UTC hour.",
            "- Timestamp is serialized as ISO-8601 UTC with a trailing Z.",
            "- CSV numeric values use canonical numeric text.",
            "- No aggregation, clipping, imputation, or local-time conversion was applied.",
            "- Weather Code 96 is preserved as received; wind direction 360 is preserved.",
            "- WeatherModelRequested records request metadata, not independent served-model proof.",
            "",
            "OUTPUTS:",
            str(CLEAN_DIR),
            str(MANIFEST_OUT_PATH),
            str(MISSING_OUT_PATH),
            str(SUMMARY_PATH),
            str(LOG_PATH),
            "",
            "FINAL RESULT: PASS",
        ]
        temp_summary.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

        # Recheck every target and control output immediately before promotion.
        for target in targets:
            if target["clean_path"].exists():
                raise FileExistsError(
                    f"CLEAN target appeared during run; refusing overwrite: {target['clean_path']}"
                )
        for path in (SUMMARY_PATH, MANIFEST_OUT_PATH, MISSING_OUT_PATH):
            if path.exists():
                raise FileExistsError(f"Control output appeared during run: {path}")

        # Promote only the 112 newly prepared files. Track each new destination so
        # a failure can roll back only files created by this run.
        for target in targets:
            src = temp_data_dir / target["clean_path"].name
            dst = target["clean_path"]
            if dst.exists():
                raise FileExistsError(f"Refusing late overwrite: {dst}")
            src.replace(dst)
            promoted.append(dst)

        # Post-promotion inventory and old-file preservation check.
        final_clean_files = sorted(CLEAN_DIR.glob("weather_*_2025.csv"))
        if len(final_clean_files) != EXPECTED_EXISTING_CLEAN_FILES + EXPECTED_NEW_AIRPORTS:
            raise RuntimeError(
                "Final CLEAN inventory should be 352 files; found "
                f"{len(final_clean_files)}"
            )
        for name, expected_hash in existing_clean_hashes_before.items():
            actual_hash = sha256_file(CLEAN_DIR / name)
            if actual_hash != expected_hash:
                raise RuntimeError(f"Existing CLEAN file changed after promotion: {name}")

        for path in (SUMMARY_PATH, MANIFEST_OUT_PATH, MISSING_OUT_PATH):
            if path.exists():
                raise FileExistsError(f"Refusing report overwrite during final promotion: {path}")
        temp_summary.replace(SUMMARY_PATH)
        temp_manifest.replace(MANIFEST_OUT_PATH)
        temp_missing.replace(MISSING_OUT_PATH)

        shutil.rmtree(TEMP_DIR, ignore_errors=True)
        temp_created = False

        print("\n" + "=" * 88)
        print("STEP 29B3 FINISHED: PASS")
        print(f"New CLEAN files : {EXPECTED_NEW_AIRPORTS}")
        print(f"New CLEAN rows  : {total_rows:,}")
        print(f"Total CLEAN files now: {EXPECTED_EXISTING_CLEAN_FILES + EXPECTED_NEW_AIRPORTS}")
        print(f"Summary         : {SUMMARY_PATH}")
        print(f"Manifest        : {MANIFEST_OUT_PATH}")
        print(f"Missing profile : {MISSING_OUT_PATH}")
        print(f"Log             : {LOG_PATH}")
        logger.info("STEP 29B3 FINISHED: PASS | files=%s | rows=%s", EXPECTED_NEW_AIRPORTS, f"{total_rows:,}")
        return 0

    except Exception as exc:
        logger.exception("STEP 29B3 FAILED: %s", exc)
        # Roll back only CLEAN files this run successfully promoted; pre-existing
        # CLEAN files and all RAW/STAGING data are never touched.
        rollback_errors: list[str] = []
        for path in reversed(promoted):
            try:
                if path.exists():
                    path.unlink()
            except OSError as rollback_exc:
                rollback_errors.append(f"{path}: {rollback_exc}")
        if promoted:
            if rollback_errors:
                logger.error("Rollback incomplete: %s", " | ".join(rollback_errors))
            else:
                logger.warning("Rolled back %s newly promoted CLEAN files", len(promoted))
        print(f"\nSTEP 29B3 FAILED: {type(exc).__name__}: {exc}")
        if promoted:
            print(f"New CLEAN files promoted before failure: {len(promoted)}; rollback attempted.")
        if temp_created and TEMP_DIR.exists():
            print(f"Temporary evidence retained for inspection: {TEMP_DIR}")
        print(f"Log: {LOG_PATH}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
