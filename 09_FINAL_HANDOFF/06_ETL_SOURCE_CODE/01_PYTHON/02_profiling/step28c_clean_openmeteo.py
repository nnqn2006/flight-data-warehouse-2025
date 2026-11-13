"""
STEP 28C - OPEN-METEO CLEAN + VALIDATION (CSV-COMPATIBLE)

Input (READ ONLY):
    D:/DATA_WAREHOUSE/01_RAW_DATA/OpenMeteo/csv/weather_{IATA}_2025.csv

Output:
    D:/DATA_WAREHOUSE/04_CLEAN_DATA/OpenMeteo/weather_{IATA}_2025.csv
    D:/DATA_WAREHOUSE/04_CLEAN_DATA/_CONTROL/
        28c_openmeteo_clean_csv_validation_summary.txt
        28c_openmeteo_clean_csv_manifest.csv
        28c_openmeteo_clean_csv_missing_profile.csv
        step28c_clean_openmeteo_csv_<run_timestamp>.log

Important design decisions:
    - Does NOT import pyarrow or fastparquet.
    - Uses the reconciled RAW CSV files as input because Parquet reader
      availability is restricted in the user's workflow.
    - RAW CSV, RAW JSON, and existing STAGING Parquet are read-only.
    - Preserves the 14-column source schema and airport-hour grain.
    - Parses timestamps as UTC and writes ISO-8601 timestamps ending in Z.
    - Converts numeric fields to canonical numeric representations in CSV.
    - Preserves Weather Code 96 and WeatherModelRequested as request metadata.
    - Does not clip extremes, impute values, aggregate, or convert to local time.
    - Uses a temporary output directory and refuses to overwrite existing CLEAN.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


# =============================================================================
# 1. PROJECT PATHS AND DATA CONTRACT
# =============================================================================

ROOT = Path(r"D:\DATA_WAREHOUSE")

RAW_CSV_DIR = ROOT / "01_RAW_DATA" / "OpenMeteo" / "csv"

CLEAN_ROOT = ROOT / "04_CLEAN_DATA"
CLEAN_DIR = CLEAN_ROOT / "OpenMeteo"
CONTROL_DIR = CLEAN_ROOT / "_CONTROL"

EXPECTED_AIRPORT_FILES = 240
EXPECTED_ROWS_PER_AIRPORT = 8760
EXPECTED_TOTAL_ROWS = EXPECTED_AIRPORT_FILES * EXPECTED_ROWS_PER_AIRPORT
EXPECTED_CODE96_ROWS = 113

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

STRING_COLUMNS = [
    "AirportCode",
    "TimezoneRequested",
    "WeatherModelRequested",
]

WEATHER_CODES = {
    0, 1, 2, 3, 45,
    51, 53, 55,
    61, 63, 65,
    71, 73, 75,
    80, 81, 82,
    85, 86,
    95, 96,
}

RUN_STAMP = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

TEMP_DIR = CLEAN_ROOT / f"_TEMP_STEP28C_OpenMeteo_CSV_{RUN_STAMP}"

SUMMARY_PATH = CONTROL_DIR / "28c_openmeteo_clean_csv_validation_summary.txt"
MANIFEST_PATH = CONTROL_DIR / "28c_openmeteo_clean_csv_manifest.csv"
MISSING_PATH = CONTROL_DIR / "28c_openmeteo_clean_csv_missing_profile.csv"
LOG_PATH = CONTROL_DIR / f"step28c_clean_openmeteo_csv_{RUN_STAMP}.log"


# =============================================================================
# 2. LOGGING AND GENERAL HELPERS
# =============================================================================

def make_logger() -> logging.Logger:
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("step28c_openmeteo_csv")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

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


def expected_utc_hours() -> pd.DatetimeIndex:
    return pd.date_range(
        start="2025-01-01 00:00:00",
        periods=EXPECTED_ROWS_PER_AIRPORT,
        freq="h",
        tz="UTC",
    )


def discover_raw_files() -> list[Path]:
    if not RAW_CSV_DIR.is_dir():
        raise FileNotFoundError(
            f"RAW Open-Meteo CSV directory not found: {RAW_CSV_DIR}"
        )

    files = sorted(RAW_CSV_DIR.glob("weather_*_2025.csv"))

    if len(files) != EXPECTED_AIRPORT_FILES:
        raise ValueError(
            f"Expected {EXPECTED_AIRPORT_FILES} RAW CSV files; found {len(files)} "
            f"in {RAW_CSV_DIR}"
        )

    airport_codes = []
    for path in files:
        airport_code = path.name.removeprefix("weather_").removesuffix("_2025.csv")
        if len(airport_code) != 3 or not airport_code.isalnum() or not airport_code.isupper():
            raise ValueError(f"Unexpected RAW filename: {path.name}")
        airport_codes.append(airport_code)

    if len(set(airport_codes)) != EXPECTED_AIRPORT_FILES:
        raise ValueError("Duplicate airport codes found in RAW filenames")

    return files


def preflight(logger: logging.Logger) -> list[Path]:
    if not RAW_CSV_DIR.is_dir():
        raise FileNotFoundError(RAW_CSV_DIR)

    if CLEAN_DIR.exists() and any(CLEAN_DIR.iterdir()):
        raise FileExistsError(
            "CLEAN output directory is not empty. Refusing to overwrite: "
            f"{CLEAN_DIR}"
        )

    for output_path in (SUMMARY_PATH, MANIFEST_PATH, MISSING_PATH):
        if output_path.exists():
            raise FileExistsError(
                "A STEP 28C control output already exists. Refusing to overwrite: "
                f"{output_path}"
            )

    if TEMP_DIR.exists():
        raise FileExistsError(f"Temporary directory already exists: {TEMP_DIR}")

    files = discover_raw_files()

    logger.info("RAW CSV directory: %s", RAW_CSV_DIR)
    logger.info("RAW CSV files found: %s", len(files))
    logger.info("Existing STAGING Parquet will not be opened or modified.")
    logger.info("CLEAN output directory: %s", CLEAN_DIR)

    return files


# =============================================================================
# 3. RAW CSV READ AND TYPE NORMALIZATION
# =============================================================================

def read_raw_csv(path: Path) -> pd.DataFrame:
    """
    Read all fields as text so pandas does not reinterpret tokens such as
    'NA' as missing values. Actual blank cells are checked explicitly.
    """
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
            f"{path.name}: source schema/order mismatch.\n"
            f"Expected: {EXPECTED_COLUMNS}\n"
            f"Actual:   {list(frame.columns)}"
        )

    if len(frame) != EXPECTED_ROWS_PER_AIRPORT:
        raise ValueError(
            f"{path.name}: expected {EXPECTED_ROWS_PER_AIRPORT:,} rows; "
            f"found {len(frame):,}"
        )

    blank_counts = frame.eq("").sum()
    blanks = blank_counts[blank_counts > 0]

    if not blanks.empty:
        raise ValueError(
            f"{path.name}: blank source values detected: {blanks.to_dict()}"
        )

    return frame


def parse_and_validate(
    source: pd.DataFrame,
    airport_code: str,
    source_name: str,
) -> pd.DataFrame:
    """Convert source text into canonical values for CSV CLEAN output."""

    if list(source.columns) != EXPECTED_COLUMNS:
        raise ValueError(f"{source_name}: unexpected column order")

    clean = source.copy()

    # -------------------------------------------------------------------------
    # Identifiers and request metadata
    # -------------------------------------------------------------------------
    for column in STRING_COLUMNS:
        clean[column] = clean[column].astype("string").str.strip()

    if not clean["AirportCode"].eq(airport_code).all():
        raise ValueError(
            f"{source_name}: AirportCode does not match filename {airport_code}"
        )

    if not clean["AirportCode"].str.fullmatch(r"[A-Z0-9]{3}").all():
        raise ValueError(f"{source_name}: invalid AirportCode format")

    if not clean["TimezoneRequested"].eq("UTC").all():
        raise ValueError(
            f"{source_name}: TimezoneRequested must be UTC for all records"
        )

    if clean["WeatherModelRequested"].eq("").any():
        raise ValueError(f"{source_name}: WeatherModelRequested contains blanks")

    if clean["WeatherModelRequested"].nunique(dropna=False) != 1:
        raise ValueError(
            f"{source_name}: WeatherModelRequested changes within one airport file"
        )

    # -------------------------------------------------------------------------
    # Time: RAW has UTC wall-clock timestamps without an offset.
    # TimezoneRequested=UTC is the explicit source contract.
    # CLEAN output will use ISO-8601 Z notation.
    # -------------------------------------------------------------------------
    parsed_time = pd.to_datetime(
        clean["time"],
        format="%Y-%m-%dT%H:%M",
        errors="coerce",
    )

    if parsed_time.isna().any():
        invalid_count = int(parsed_time.isna().sum())
        raise ValueError(
            f"{source_name}: {invalid_count} invalid timestamp values"
        )

    parsed_time = parsed_time.dt.tz_localize("UTC")
    expected_time = expected_utc_hours()

    actual_ns = parsed_time.astype("int64").to_numpy()
    expected_ns = expected_time.astype("int64")

    if not np.array_equal(actual_ns, expected_ns):
        raise ValueError(
            f"{source_name}: timestamps do not match the complete ordered "
            "2025 UTC hourly sequence"
        )

    if parsed_time.duplicated().any():
        raise ValueError(f"{source_name}: duplicate UTC timestamps")

    clean["time"] = parsed_time.dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # -------------------------------------------------------------------------
    # Floating-point measures and coordinates
    # -------------------------------------------------------------------------
    for column in FLOAT_COLUMNS:
        values = pd.to_numeric(clean[column], errors="coerce")

        if values.isna().any():
            raise ValueError(f"{source_name}: non-numeric values in {column}")

        if not np.isfinite(values.to_numpy(dtype="float64")).all():
            raise ValueError(f"{source_name}: non-finite values in {column}")

        clean[column] = values.astype("float64")

    # -------------------------------------------------------------------------
    # Integer/categorical numeric fields
    # -------------------------------------------------------------------------
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

    # -------------------------------------------------------------------------
    # Domain and coordinate checks. No clipping or outlier removal.
    # -------------------------------------------------------------------------
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
            raise ValueError(
                f"{source_name}: {invalid_count} invalid values in {column}"
            )

    if clean["Latitude"].nunique(dropna=False) != 1:
        raise ValueError(f"{source_name}: Latitude changes within airport file")

    if clean["Longitude"].nunique(dropna=False) != 1:
        raise ValueError(f"{source_name}: Longitude changes within airport file")

    # Keep exact source column order.
    return clean[EXPECTED_COLUMNS]


# =============================================================================
# 4. CSV OUTPUT AND READ-BACK VALIDATION
# =============================================================================

def write_clean_csv(frame: pd.DataFrame, path: Path) -> None:
    """
    CSV has no embedded logical data types. Values are written canonically:
    ISO-8601 UTC timestamps, numeric values, integer-coded weather fields,
    and text identifiers/metadata.
    """
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
    source_name: str,
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
        raise ValueError(
            f"{clean_path.name}: read-back row count mismatch "
            f"({len(read_back)} rows)"
        )

    blank_counts = read_back.eq("").sum()
    if blank_counts.any():
        raise ValueError(
            f"{clean_path.name}: blank CLEAN values: "
            f"{blank_counts[blank_counts > 0].to_dict()}"
        )

    if not read_back["AirportCode"].eq(airport_code).all():
        raise ValueError(f"{clean_path.name}: AirportCode mismatch")

    if not read_back["TimezoneRequested"].eq("UTC").all():
        raise ValueError(f"{clean_path.name}: timezone metadata changed")

    if not read_back["WeatherModelRequested"].eq(
        source_frame["WeatherModelRequested"].astype("string")
    ).all():
        raise ValueError(f"{clean_path.name}: request model metadata changed")

    # Validate explicit UTC timestamp serialization.
    parsed_clean_time = pd.to_datetime(
        read_back["time"],
        format="%Y-%m-%dT%H:%M:%SZ",
        utc=True,
        errors="coerce",
    )

    if parsed_clean_time.isna().any():
        raise ValueError(f"{clean_path.name}: invalid CLEAN UTC timestamps")

    expected_time = expected_utc_hours()
    if not np.array_equal(
        parsed_clean_time.astype("int64").to_numpy(),
        expected_time.astype("int64"),
    ):
        raise ValueError(f"{clean_path.name}: CLEAN UTC sequence mismatch")

    if parsed_clean_time.duplicated().any():
        raise ValueError(f"{clean_path.name}: duplicate airport-hour keys")

    # Compare all numeric columns with the source values.
    for column in FLOAT_COLUMNS + INTEGER_COLUMNS:
        source_numeric = pd.to_numeric(
            source_frame[column], errors="coerce"
        ).to_numpy(dtype="float64")

        clean_numeric = pd.to_numeric(
            read_back[column], errors="coerce"
        ).to_numpy(dtype="float64")

        if np.isnan(clean_numeric).any() or not np.isfinite(clean_numeric).all():
            raise ValueError(
                f"{clean_path.name}: invalid numeric read-back in {column}"
            )

        if not np.allclose(
            source_numeric,
            clean_numeric,
            rtol=0.0,
            atol=1e-12,
        ):
            raise ValueError(
                f"{clean_path.name}: source-to-CLEAN numeric mismatch in {column}"
            )

    # Count missing values by column. Blank values are not allowed; this profile
    # is still written to make the data-quality evidence explicit.
    missing_counts = {
        column: int(read_back[column].eq("").sum())
        for column in EXPECTED_COLUMNS
    }

    code96_count = int(
        (pd.to_numeric(read_back["weather_code"], errors="coerce") == 96).sum()
    )

    return len(read_back), code96_count, missing_counts


# =============================================================================
# 5. MAIN
# =============================================================================

def main() -> int:
    logger = make_logger()

    logger.info("=" * 78)
    logger.info("STEP 28C - OPEN-METEO CLEAN + VALIDATION (CSV)")
    logger.info("Run timestamp: %s", RUN_STAMP)
    logger.info("Project root: %s", ROOT)
    logger.info("RAW CSV (read-only): %s", RAW_CSV_DIR)
    logger.info("STAGING Parquet is not opened or modified.")
    logger.info("CLEAN output: %s", CLEAN_DIR)
    logger.info("Expected airports: %s", EXPECTED_AIRPORT_FILES)
    logger.info("Expected rows per airport: %s", EXPECTED_ROWS_PER_AIRPORT)
    logger.info("Expected total rows: %s", f"{EXPECTED_TOTAL_ROWS:,}")

    try:
        source_files = preflight(logger)

        TEMP_DIR.mkdir(parents=True, exist_ok=False)
        temp_data_dir = TEMP_DIR / "OpenMeteo"
        temp_data_dir.mkdir(parents=True, exist_ok=False)

        manifest_rows: list[dict] = []
        missing_rows: list[dict] = []

        total_rows = 0
        total_code96 = 0

        for index, source_path in enumerate(source_files, start=1):
            airport_code = (
                source_path.name
                .removeprefix("weather_")
                .removesuffix("_2025.csv")
            )

            logger.info(
                "[%s/%s] Processing %s",
                index,
                len(source_files),
                source_path.name,
            )

            source_sha_before = sha256_file(source_path)

            source_frame = read_raw_csv(source_path)
            clean_frame = parse_and_validate(
                source_frame,
                airport_code,
                source_path.name,
            )

            temp_clean_path = temp_data_dir / source_path.name
            write_clean_csv(clean_frame, temp_clean_path)

            rows_read_back, code96_count, missing_counts = validate_clean_csv(
                temp_clean_path,
                source_frame,
                airport_code,
                source_path.name,
            )

            source_sha_after = sha256_file(source_path)
            if source_sha_before != source_sha_after:
                raise RuntimeError(
                    f"RAW source changed during processing: {source_path.name}"
                )

            clean_sha = sha256_file(temp_clean_path)

            for column in EXPECTED_COLUMNS:
                missing_rows.append({
                    "AirportCode": airport_code,
                    "ColumnName": column,
                    "MissingCount": missing_counts[column],
                    "MissingPct": (
                        missing_counts[column] / rows_read_back * 100
                    ),
                })

            manifest_rows.append({
                "AirportCode": airport_code,
                "SourceRAWFile": source_path.name,
                "SourceSHA256Before": source_sha_before,
                "SourceSHA256After": source_sha_after,
                "SourceUnchanged": source_sha_before == source_sha_after,
                "CleanFile": source_path.name,
                "CleanSHA256": clean_sha,
                "RowsReadFromRAW": len(source_frame),
                "RowsReadBackFromCLEAN": rows_read_back,
                "Columns": len(clean_frame.columns),
                "WeatherCode96Rows": code96_count,
                "FirstTimestampUTC": clean_frame["time"].iloc[0],
                "LastTimestampUTC": clean_frame["time"].iloc[-1],
                "TimezoneStored": "UTC",
                "Result": "PASS",
            })

            total_rows += rows_read_back
            total_code96 += code96_count

            logger.info(
                "%s PASS | rows=%s | code96=%s | RAW unchanged",
                airport_code,
                f"{rows_read_back:,}",
                code96_count,
            )

        # Global checks before promotion.
        if len(manifest_rows) != EXPECTED_AIRPORT_FILES:
            raise ValueError(
                f"Expected {EXPECTED_AIRPORT_FILES} CLEAN files; "
                f"prepared {len(manifest_rows)}"
            )

        if total_rows != EXPECTED_TOTAL_ROWS:
            raise ValueError(
                f"Expected {EXPECTED_TOTAL_ROWS:,} rows; got {total_rows:,}"
            )

        if total_code96 != EXPECTED_CODE96_ROWS:
            raise ValueError(
                f"Expected {EXPECTED_CODE96_ROWS} Weather Code 96 rows; "
                f"got {total_code96}"
            )

        if not all(row["SourceUnchanged"] for row in manifest_rows):
            raise RuntimeError("At least one RAW source SHA256 changed")

        # Prepare control evidence before promoting the data directory.
        manifest_df = pd.DataFrame(manifest_rows)
        missing_df = pd.DataFrame(missing_rows)

        manifest_temp = TEMP_DIR / MANIFEST_PATH.name
        missing_temp = TEMP_DIR / MISSING_PATH.name
        summary_temp = TEMP_DIR / SUMMARY_PATH.name

        manifest_df.to_csv(
            manifest_temp,
            index=False,
            encoding="utf-8-sig",
        )
        missing_df.to_csv(
            missing_temp,
            index=False,
            encoding="utf-8-sig",
        )

        summary_lines = [
            "OPEN-METEO STEP 28C - CLEAN CSV VALIDATION SUMMARY",
            "=" * 78,
            f"Run timestamp: {RUN_STAMP}",
            f"RAW source directory: {RAW_CSV_DIR}",
            f"CLEAN output directory: {CLEAN_DIR}",
            "",
            "GLOBAL COUNTS:",
            f"RAW airport CSV files expected: {EXPECTED_AIRPORT_FILES}",
            f"CLEAN airport CSV files prepared: {len(manifest_rows)}",
            f"Rows per airport expected: {EXPECTED_ROWS_PER_AIRPORT:,}",
            f"Total rows expected: {EXPECTED_TOTAL_ROWS:,}",
            f"Total rows read back from CLEAN: {total_rows:,}",
            f"WeatherCode=96 expected: {EXPECTED_CODE96_ROWS}",
            f"WeatherCode=96 read back: {total_code96}",
            "",
            "VALIDATION CHECKS:",
            "raw_file_inventory_240: PASS",
            "raw_schema_and_column_order_14: PASS",
            "raw_rows_per_airport_8760: PASS",
            "no_blank_source_cells: PASS",
            "airport_code_matches_filename: PASS",
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
            "all_240_clean_csv_files_prepared: PASS",
            "all_RAW_source_SHA256_unchanged: PASS",
            "",
            "DATA POLICY:",
            "- RAW CSV files were read-only.",
            "- Existing STAGING Parquet files were not opened or modified.",
            "- CLEAN preserves the 14 source columns and airport-hour grain.",
            "- One row represents one airport at one UTC hour.",
            "- Timestamp is serialized as ISO-8601 UTC with a trailing Z.",
            "- CSV has no embedded data types; numeric fields use canonical numeric text.",
            "- No aggregation, clipping, imputation, or local-time conversion was applied.",
            "- Weather Code 96 is preserved as received.",
            "- WeatherModelRequested is request metadata, not proof of actual model provenance.",
            "- Wind direction is an integer circular variable; 360 is preserved.",
            "",
            "OUTPUT FILES:",
            str(CLEAN_DIR),
            str(MANIFEST_PATH),
            str(MISSING_PATH),
            str(SUMMARY_PATH),
            str(LOG_PATH),
            "",
            "FINAL RESULT: PASS",
        ]

        summary_temp.write_text(
            "\n".join(summary_lines) + "\n",
            encoding="utf-8",
        )

        # Refuse late overwrites immediately before promotion.
        if CLEAN_DIR.exists() and any(CLEAN_DIR.iterdir()):
            raise FileExistsError(
                f"CLEAN directory became non-empty; refusing promotion: {CLEAN_DIR}"
            )

        for output_path in (SUMMARY_PATH, MANIFEST_PATH, MISSING_PATH):
            if output_path.exists():
                raise FileExistsError(
                    f"Control output appeared during run; refusing overwrite: {output_path}"
                )

        # Promote the complete data directory in one same-volume rename.
        if CLEAN_DIR.exists():
            CLEAN_DIR.rmdir()

        CLEAN_DIR.parent.mkdir(parents=True, exist_ok=True)
        temp_data_dir.rename(CLEAN_DIR)

        # Promote control reports only after the CLEAN data directory exists.
        summary_temp.replace(SUMMARY_PATH)
        manifest_temp.replace(MANIFEST_PATH)
        missing_temp.replace(MISSING_PATH)

        shutil.rmtree(TEMP_DIR, ignore_errors=True)

        final_files = sorted(CLEAN_DIR.glob("weather_*_2025.csv"))
        if len(final_files) != EXPECTED_AIRPORT_FILES:
            raise RuntimeError(
                f"Post-promotion CLEAN inventory mismatch: {len(final_files)} files"
            )

        logger.info("=" * 78)
        logger.info("STEP 28C FINISHED: PASS")
        logger.info("CLEAN files: %s", len(final_files))
        logger.info("CLEAN rows: %s", f"{total_rows:,}")
        logger.info("WeatherCode=96 rows: %s", total_code96)
        logger.info("Summary: %s", SUMMARY_PATH)
        logger.info("Manifest: %s", MANIFEST_PATH)
        logger.info("Missing profile: %s", MISSING_PATH)
        logger.info("Final result: PASS")

        return 0

    except Exception:
        logger.exception("STEP 28C FAILED")
        logger.error(
            "No successful CLEAN promotion is claimed. "
            "Inspect the temporary run directory if it exists: %s",
            TEMP_DIR,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
