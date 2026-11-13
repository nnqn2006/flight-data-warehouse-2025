
"""
STEP 25 - BUILD STAGING DATA

Project: D:\\DATA_WAREHOUSE

Purpose
-------
Create non-destructive, typed-as-text Parquet copies of the three source
families: BTS, OurAirports, and Open-Meteo.

Design principles
-----------------
1. Never modify files under 01_RAW_DATA.
2. Preserve the source grain; do not join sources or deduplicate records.
3. Read all source fields as strings and preserve literal codes such as "NA".
4. BTS is processed in chunks to control memory use.
5. The known empty BTS column "Unnamed: 109" is removed from the STAGING
   copy only, after verifying that it is blank in every chunk.
6. Add SourceFile (and BTS SourceMonth) as lineage columns.
7. Stop and report mismatches against the previously profiled source counts.
8. Do not overwrite existing STAGING outputs unless the configuration flag
   OVERWRITE_EXISTING_OUTPUTS is explicitly changed to True.

Dependencies: pandas, pyarrow
Run from any working directory:
    python .\\08_SCRIPTS\\02_profiling\\step25_build_staging_data.py
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# =============================================================================
# 1. CONFIGURATION
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "01_RAW_DATA"
STAGING_DIR = PROJECT_ROOT / "03_STAGING_DATA"

BTS_RAW_DIR = RAW_DIR / "BTS" / "csv"
OURAIRPORTS_RAW_DIR = RAW_DIR / "OurAirports"
OPENMETEO_RAW_DIR = RAW_DIR / "OpenMeteo" / "csv"

BTS_STAGING_DIR = STAGING_DIR / "BTS"
OURAIRPORTS_STAGING_DIR = STAGING_DIR / "OurAirports"
OPENMETEO_STAGING_DIR = STAGING_DIR / "OpenMeteo"
CONTROL_DIR = STAGING_DIR / "_CONTROL"

YEAR = 2025
BTS_CHUNK_SIZE = 100_000
PARQUET_COMPRESSION = "snappy"
CSV_ENCODING = "utf-8-sig"

EXPECTED_BTS_ROWS_BY_MONTH = {
    1: 539_747,
    2: 504_884,
    3: 600_872,
    4: 583_950,
    5: 605_648,
    6: 611_575,
    7: 631_428,
    8: 602_378,
    9: 562_439,
    10: 605_844,
    11: 570_550,
    12: 582_304,
}
EXPECTED_BTS_TOTAL_ROWS = sum(EXPECTED_BTS_ROWS_BY_MONTH.values())
EXPECTED_OURAIRPORTS_ROWS = 86_335
EXPECTED_OPENMETEO_FILES = 240
EXPECTED_OPENMETEO_ROWS_PER_AIRPORT = 8_760
EXPECTED_OPENMETEO_CODE96_ROWS = 113

BTS_EMPTY_COLUMN = "Unnamed: 109"
OVERWRITE_EXISTING_OUTPUTS = False

OPENMETEO_REQUIRED_COLUMNS = [
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


# =============================================================================
# 2. LOGGING AND GENERAL HELPERS
# =============================================================================


def build_logger() -> tuple[logging.Logger, Path]:
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    log_path = CONTROL_DIR / "step25_build_staging_data.log"

    logger = logging.getLogger("step25_staging")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger, log_path


def ensure_directories() -> None:
    for directory in [
        BTS_STAGING_DIR,
        OURAIRPORTS_STAGING_DIR,
        OPENMETEO_STAGING_DIR,
        CONTROL_DIR,
    ]:
        directory.mkdir(parents=True, exist_ok=True)


def assert_input_exists(path: Path) -> None:
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Required RAW source file not found: {path}")


def assert_output_available(path: Path) -> None:
    if path.exists() and not OVERWRITE_EXISTING_OUTPUTS:
        raise FileExistsError(
            f"STAGING output already exists: {path}\n"
            "To avoid accidental overwrite, this script stops. If you intentionally "
            "want to rebuild STAGING, first back up/remove the existing STAGING "
            "outputs or set OVERWRITE_EXISTING_OUTPUTS = True in the script."
        )


def write_parquet_atomic(df: pd.DataFrame, output_path: Path) -> None:
    """Write a DataFrame to Parquet via a temporary file, then rename atomically."""
    assert_output_available(output_path)
    temp_path = output_path.with_name(output_path.name + ".part")
    temp_path.unlink(missing_ok=True)
    try:
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(
            table,
            temp_path,
            compression=PARQUET_COMPRESSION,
            use_dictionary=True,
        )
        if not temp_path.exists() or temp_path.stat().st_size == 0:
            raise RuntimeError(f"Parquet output is empty: {temp_path}")
        temp_path.replace(output_path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def add_lineage_columns(
    df: pd.DataFrame,
    source_file: str,
    source_month: str | None = None,
) -> pd.DataFrame:
    df["SourceFile"] = pd.Series([source_file] * len(df), dtype="string")
    if source_month is not None:
        df["SourceMonth"] = pd.Series([source_month] * len(df), dtype="string")
    return df


def manifest_record(
    source_group: str,
    source_file: str,
    output_file: str,
    expected_rows: int | None,
    actual_rows: int | None,
    column_count: int | None,
    status: str,
    message: str = "",
    chunk_count: int | None = None,
    code96_count: int | None = None,
    duplicate_time_count: int | None = None,
) -> dict[str, Any]:
    return {
        "SourceGroup": source_group,
        "SourceFile": source_file,
        "StagingFile": output_file,
        "ExpectedRows": expected_rows,
        "StagedRows": actual_rows,
        "ColumnCount": column_count,
        "ChunkCount": chunk_count,
        "WeatherCode96Count": code96_count,
        "DuplicateTimestampCount": duplicate_time_count,
        "Status": status,
        "Message": message,
    }


# =============================================================================
# 3. BTS - CHUNKED STAGING
# =============================================================================


def stage_bts_file(
    month: int,
    logger: logging.Logger,
) -> dict[str, Any]:
    source_path = BTS_RAW_DIR / f"BTS_{YEAR}_{month:02d}.csv"
    output_path = BTS_STAGING_DIR / f"stg_bts_{YEAR}_{month:02d}.parquet"
    expected_rows = EXPECTED_BTS_ROWS_BY_MONTH[month]

    assert_input_exists(source_path)
    assert_output_available(output_path)

    logger.info("BTS %02d: staging %s", month, source_path.name)
    temp_path = output_path.with_name(output_path.name + ".part")
    temp_path.unlink(missing_ok=True)

    writer: pq.ParquetWriter | None = None
    schema: pa.Schema | None = None
    row_count = 0
    chunk_count = 0
    column_count: int | None = None
    source_columns: list[str] | None = None

    try:
        reader = pd.read_csv(
            source_path,
            dtype="string",
            keep_default_na=False,
            encoding=CSV_ENCODING,
            chunksize=BTS_CHUNK_SIZE,
            low_memory=False,
        )

        for chunk in reader:
            chunk_count += 1
            if source_columns is None:
                source_columns = list(chunk.columns)
                column_count = len(source_columns)
                if BTS_EMPTY_COLUMN not in source_columns:
                    raise ValueError(
                        f"Expected the profiled blank column '{BTS_EMPTY_COLUMN}' "
                        f"in {source_path.name}, but it was not found."
                    )
            elif list(chunk.columns) != source_columns:
                raise ValueError(f"Column schema changed between chunks in {source_path.name}.")

            # Drop only after verifying that the column is blank in this chunk.
            blank_values = chunk[BTS_EMPTY_COLUMN].astype("string").str.strip().eq("")
            if not bool(blank_values.all()):
                raise ValueError(
                    f"'{BTS_EMPTY_COLUMN}' contains nonblank values in {source_path.name}; "
                    "the column will not be dropped."
                )
            chunk = chunk.drop(columns=[BTS_EMPTY_COLUMN])

            chunk = add_lineage_columns(
                chunk,
                source_file=source_path.name,
                source_month=f"{YEAR}-{month:02d}",
            )

            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                schema = table.schema
                writer = pq.ParquetWriter(
                    temp_path,
                    schema,
                    compression=PARQUET_COMPRESSION,
                    use_dictionary=True,
                )
            else:
                if table.schema.names != schema.names:
                    raise ValueError(f"Parquet schema column names changed in {source_path.name}.")
                if not table.schema.types == schema.types:
                    raise ValueError(f"Parquet schema types changed in {source_path.name}.")
                table = table.cast(schema)

            writer.write_table(table)
            row_count += len(chunk)

            if chunk_count % 10 == 0:
                logger.info(
                    "  %s: processed %s chunks / %s rows",
                    source_path.name,
                    chunk_count,
                    f"{row_count:,}",
                )

        if writer is None:
            raise ValueError(f"No rows were read from {source_path.name}.")

        writer.close()
        writer = None

        if row_count != expected_rows:
            raise ValueError(
                f"Row-count mismatch for {source_path.name}: expected {expected_rows:,}, "
                f"read {row_count:,}. The staged file was not published."
            )

        if not temp_path.exists() or temp_path.stat().st_size == 0:
            raise RuntimeError(f"Parquet output is empty for {source_path.name}.")

        temp_path.replace(output_path)
        logger.info(
            "BTS %02d completed: %s rows, %s chunks, %s columns",
            month,
            f"{row_count:,}",
            chunk_count,
            len(schema.names) if schema else 0,
        )

        return manifest_record(
            "BTS",
            source_path.name,
            str(output_path),
            expected_rows,
            row_count,
            len(schema.names) if schema else column_count,
            "SUCCESS",
            "All fields staged as text; verified blank Unnamed: 109 removed from STAGING copy only.",
            chunk_count=chunk_count,
        )

    except Exception:
        if writer is not None:
            writer.close()
        temp_path.unlink(missing_ok=True)
        raise


# =============================================================================
# 4. OURAIRPORTS - STAGING
# =============================================================================


def stage_ourairports_file(
    file_name: str,
    output_name: str,
    logger: logging.Logger,
) -> dict[str, Any]:
    source_path = OURAIRPORTS_RAW_DIR / file_name
    output_path = OURAIRPORTS_STAGING_DIR / output_name
    assert_input_exists(source_path)
    assert_output_available(output_path)

    logger.info("OurAirports: staging %s", source_path.name)
    df = pd.read_csv(
        source_path,
        dtype="string",
        keep_default_na=False,
        encoding=CSV_ENCODING,
        low_memory=False,
    )
    actual_rows = len(df)
    column_count = len(df.columns)
    df = add_lineage_columns(df, source_file=source_path.name)

    if actual_rows != EXPECTED_OURAIRPORTS_ROWS:
        raise ValueError(
            f"Row-count mismatch for {source_path.name}: expected "
            f"{EXPECTED_OURAIRPORTS_ROWS:,}, read {actual_rows:,}."
        )

    write_parquet_atomic(df, output_path)
    logger.info("OurAirports completed: %s rows, %s columns", f"{actual_rows:,}", column_count)

    return manifest_record(
        "OurAirports",
        source_path.name,
        str(output_path),
        EXPECTED_OURAIRPORTS_ROWS,
        actual_rows,
        column_count + 1,
        "SUCCESS",
        "All source fields staged as text; no join or value imputation performed.",
    )


# =============================================================================
# 5. OPEN-METEO - AIRPORT-HOURLY STAGING
# =============================================================================


def stage_openmeteo_file(
    source_path: Path,
    logger: logging.Logger,
) -> dict[str, Any]:
    airport_code = source_path.name.replace(f"weather_", "").replace(f"_{YEAR}.csv", "")
    output_path = OPENMETEO_STAGING_DIR / f"weather_{airport_code}_{YEAR}.parquet"
    assert_output_available(output_path)

    logger.info("Open-Meteo: staging %s", source_path.name)
    df = pd.read_csv(
        source_path,
        dtype="string",
        keep_default_na=False,
        encoding=CSV_ENCODING,
        low_memory=False,
    )

    missing_columns = set(OPENMETEO_REQUIRED_COLUMNS) - set(df.columns)
    if missing_columns:
        raise ValueError(
            f"{source_path.name} is missing required columns: {sorted(missing_columns)}"
        )

    actual_rows = len(df)
    if actual_rows != EXPECTED_OPENMETEO_ROWS_PER_AIRPORT:
        raise ValueError(
            f"Row-count mismatch for {source_path.name}: expected "
            f"{EXPECTED_OPENMETEO_ROWS_PER_AIRPORT:,}, read {actual_rows:,}."
        )

    observed_airports = set(df["AirportCode"].astype("string").str.strip().str.upper().unique())
    if observed_airports != {airport_code.upper()}:
        raise ValueError(
            f"AirportCode content does not match file name in {source_path.name}: "
            f"{sorted(observed_airports)}"
        )

    duplicate_timestamps = int(df["time"].duplicated().sum())
    if duplicate_timestamps:
        raise ValueError(
            f"Found {duplicate_timestamps} duplicate timestamps in {source_path.name}."
        )

    code96_count = int(df["weather_code"].eq(str(96)).sum())
    column_count = len(df.columns)
    df = add_lineage_columns(df, source_file=source_path.name)
    write_parquet_atomic(df, output_path)

    logger.info(
        "Open-Meteo completed: %s rows, code96=%s, duplicate timestamps=%s",
        f"{actual_rows:,}",
        code96_count,
        duplicate_timestamps,
    )

    return manifest_record(
        "OpenMeteo",
        source_path.name,
        str(output_path),
        EXPECTED_OPENMETEO_ROWS_PER_AIRPORT,
        actual_rows,
        column_count + 1,
        "SUCCESS",
        "All source fields staged as text; timestamps retained in UTC string form.",
        code96_count=code96_count,
        duplicate_time_count=duplicate_timestamps,
    )


# =============================================================================
# 6. SUMMARY / MANIFEST
# =============================================================================


def write_control_outputs(
    records: list[dict[str, Any]],
    run_stamp: str,
    logger: logging.Logger,
) -> tuple[Path, Path]:
    manifest_path = CONTROL_DIR / "staging_manifest.csv"
    summary_path = CONTROL_DIR / "staging_summary.txt"

    manifest_df = pd.DataFrame(records)
    manifest_df.to_csv(manifest_path, index=False, encoding="utf-8-sig")

    success_count = int(manifest_df["Status"].eq("SUCCESS").sum()) if not manifest_df.empty else 0
    failed_count = int(len(manifest_df) - success_count)

    bts_df = manifest_df[manifest_df["SourceGroup"].eq("BTS")]
    our_df = manifest_df[manifest_df["SourceGroup"].eq("OurAirports")]
    weather_df = manifest_df[manifest_df["SourceGroup"].eq("OpenMeteo")]

    bts_rows = int(pd.to_numeric(bts_df["StagedRows"], errors="coerce").fillna(0).sum())
    our_rows = int(pd.to_numeric(our_df["StagedRows"], errors="coerce").fillna(0).sum())
    weather_rows = int(pd.to_numeric(weather_df["StagedRows"], errors="coerce").fillna(0).sum())
    weather_code96 = int(pd.to_numeric(weather_df["WeatherCode96Count"], errors="coerce").fillna(0).sum())

    lines = [
        "DATA WAREHOUSE - STEP 25 STAGING SUMMARY",
        "=" * 78,
        f"Run timestamp: {run_stamp}",
        f"Project root: {PROJECT_ROOT}",
        f"RAW directory (read-only): {RAW_DIR}",
        f"STAGING directory: {STAGING_DIR}",
        "",
        "PROCESSING RESULTS",
        "-" * 78,
        f"Total source files processed: {len(manifest_df)}",
        f"Successful files: {success_count}",
        f"Failed files: {failed_count}",
        "",
        "ROW RECONCILIATION",
        "-" * 78,
        f"BTS monthly files staged: {len(bts_df)}/12",
        f"BTS rows staged: {bts_rows:,} (expected {EXPECTED_BTS_TOTAL_ROWS:,})",
        f"OurAirports files staged: {len(our_df)}/2",
        f"OurAirports rows staged: {our_rows:,} (expected {EXPECTED_OURAIRPORTS_ROWS * 2:,})",
        f"Open-Meteo airport files staged: {len(weather_df)}/{EXPECTED_OPENMETEO_FILES}",
        f"Open-Meteo rows staged: {weather_rows:,} "
        f"(expected {EXPECTED_OPENMETEO_FILES * EXPECTED_OPENMETEO_ROWS_PER_AIRPORT:,})",
        f"WeatherCode=96 rows staged: {weather_code96:,} "
        f"(expected {EXPECTED_OPENMETEO_CODE96_ROWS:,})",
        "",
        "STAGING RULES APPLIED",
        "-" * 78,
        "1. All source columns were read as strings; literal codes such as 'NA' are preserved.",
        "2. BTS was processed in chunks; no source rows were filtered or deduplicated.",
        "3. BTS 'Unnamed: 109' was dropped only after verifying it was blank in every chunk.",
        "4. SourceFile was added as lineage metadata; BTS also received SourceMonth.",
        "5. Open-Meteo timestamps remain unchanged as UTC strings; no timezone conversion occurred.",
        "6. No NULL imputation, outlier removal, code remapping, or cross-source joins were performed.",
        "7. RAW files were read-only and were not modified.",
        "",
        "OUTPUT FOLDERS",
        f"BTS: {BTS_STAGING_DIR}",
        f"OurAirports: {OURAIRPORTS_STAGING_DIR}",
        f"Open-Meteo: {OPENMETEO_STAGING_DIR}",
        f"Manifest: {manifest_path}",
        f"Log: {CONTROL_DIR / 'step25_build_staging_data.log'}",
        "",
    ]

    if failed_count:
        lines.extend(["FAILED FILES", "-" * 78])
        for _, row in manifest_df[manifest_df["Status"].ne("SUCCESS")].iterrows():
            lines.append(f"{row['SourceGroup']} | {row['SourceFile']} | {row['Message']}")
        lines.append("")

    summary_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Manifest written: %s", manifest_path)
    logger.info("Summary written: %s", summary_path)
    return manifest_path, summary_path


# =============================================================================
# 7. MAIN PIPELINE
# =============================================================================


def main() -> int:
    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ensure_directories()
    logger, _ = build_logger()

    logger.info("STEP 25 - BUILD STAGING DATA")
    logger.info("Project root: %s", PROJECT_ROOT)
    logger.info("RAW source is read-only: %s", RAW_DIR)
    logger.info("STAGING output: %s", STAGING_DIR)
    logger.info("BTS chunk size: %s", BTS_CHUNK_SIZE)
    logger.info("Parquet compression: %s", PARQUET_COMPRESSION)
    logger.info("Overwrite existing outputs: %s", OVERWRITE_EXISTING_OUTPUTS)

    records: list[dict[str, Any]] = []

    # BTS: 12 monthly files.
    for month in range(1, 13):
        source_name = f"BTS_{YEAR}_{month:02d}.csv"
        output_name = f"stg_bts_{YEAR}_{month:02d}.parquet"
        try:
            records.append(stage_bts_file(month, logger))
        except Exception as exc:
            logger.exception("BTS staging failed for %s", source_name)
            records.append(manifest_record(
                "BTS", source_name, str(BTS_STAGING_DIR / output_name),
                EXPECTED_BTS_ROWS_BY_MONTH[month], None, None,
                "FAILED", repr(exc),
            ))

    # OurAirports: primary table and retained reference table.
    ourairports_jobs = [
        ("airports.csv", "stg_ourairports.parquet"),
        ("airports_reference.csv", "stg_ourairports_reference.parquet"),
    ]
    for source_name, output_name in ourairports_jobs:
        try:
            records.append(stage_ourairports_file(source_name, output_name, logger))
        except Exception as exc:
            logger.exception("OurAirports staging failed for %s", source_name)
            records.append(manifest_record(
                "OurAirports", source_name, str(OURAIRPORTS_STAGING_DIR / output_name),
                EXPECTED_OURAIRPORTS_ROWS, None, None, "FAILED", repr(exc),
            ))

    # Open-Meteo: one file per airport.
    weather_files = sorted(OPENMETEO_RAW_DIR.glob(f"weather_*_{YEAR}.csv"))
    if len(weather_files) != EXPECTED_OPENMETEO_FILES:
        logger.warning(
            "Expected %s Open-Meteo CSV files but found %s.",
            EXPECTED_OPENMETEO_FILES,
            len(weather_files),
        )

    for number, source_path in enumerate(weather_files, start=1):
        logger.info("Open-Meteo progress: [%s/%s]", number, len(weather_files))
        try:
            records.append(stage_openmeteo_file(source_path, logger))
        except Exception as exc:
            logger.exception("Open-Meteo staging failed for %s", source_path.name)
            airport_code = source_path.name.replace("weather_", "").replace(f"_{YEAR}.csv", "")
            records.append(manifest_record(
                "OpenMeteo",
                source_path.name,
                str(OPENMETEO_STAGING_DIR / f"weather_{airport_code}_{YEAR}.parquet"),
                EXPECTED_OPENMETEO_ROWS_PER_AIRPORT,
                None,
                None,
                "FAILED",
                repr(exc),
            ))

    manifest_path, summary_path = write_control_outputs(records, run_stamp, logger)

    # Global reconciliation against the profiling results.
    manifest_df = pd.DataFrame(records)
    bts_ok = manifest_df[
        manifest_df["SourceGroup"].eq("BTS") & manifest_df["Status"].eq("SUCCESS")
    ]
    our_ok = manifest_df[
        manifest_df["SourceGroup"].eq("OurAirports") & manifest_df["Status"].eq("SUCCESS")
    ]
    weather_ok = manifest_df[
        manifest_df["SourceGroup"].eq("OpenMeteo") & manifest_df["Status"].eq("SUCCESS")
    ]

    bts_rows = int(pd.to_numeric(bts_ok["StagedRows"], errors="coerce").fillna(0).sum())
    weather_rows = int(pd.to_numeric(weather_ok["StagedRows"], errors="coerce").fillna(0).sum())
    weather_code96 = int(pd.to_numeric(weather_ok["WeatherCode96Count"], errors="coerce").fillna(0).sum())
    failures = int(manifest_df["Status"].ne("SUCCESS").sum())

    global_checks = {
        "BTS files": len(bts_ok) == 12,
        "BTS rows": bts_rows == EXPECTED_BTS_TOTAL_ROWS,
        "OurAirports files": len(our_ok) == 2,
        "OurAirports rows": int(pd.to_numeric(our_ok["StagedRows"], errors="coerce").fillna(0).sum()) == EXPECTED_OURAIRPORTS_ROWS * 2,
        "Open-Meteo files": len(weather_ok) == EXPECTED_OPENMETEO_FILES,
        "Open-Meteo rows": weather_rows == EXPECTED_OPENMETEO_FILES * EXPECTED_OPENMETEO_ROWS_PER_AIRPORT,
        "WeatherCode 96 rows": weather_code96 == EXPECTED_OPENMETEO_CODE96_ROWS,
        "No file-level failures": failures == 0,
    }

    logger.info("GLOBAL RECONCILIATION")
    for name, passed in global_checks.items():
        logger.info("  %s: %s", name, "PASS" if passed else "FAIL")

    logger.info("Step 25 outputs are under: %s", STAGING_DIR)
    logger.info("Manifest: %s", manifest_path)
    logger.info("Summary: %s", summary_path)

    if all(global_checks.values()):
        logger.info("STEP 25 COMPLETED SUCCESSFULLY.")
        return 0

    logger.error("STEP 25 COMPLETED WITH VALIDATION FAILURES. Review manifest and log.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
