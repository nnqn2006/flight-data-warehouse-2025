
"""STEP 26 - Independent validation of Parquet files created by Step 25.

Checks: file inventory, rows, schema, Arrow string types, nulls, lineage,
first/middle/last row parity against RAW, OurAirports ID uniqueness, literal
"NA" preservation, and all 8,760 ordered UTC hours per Open-Meteo airport.

This script reads RAW and STAGING only; it writes reports under _CONTROL.
Dependencies: pandas, pyarrow
Run: python .\08_SCRIPTS\02_profiling\step26_validate_staging_data.py
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# =============================================================================
# 1. PATHS AND EXPECTED COUNTS
# =============================================================================

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "01_RAW_DATA"
STG = ROOT / "03_STAGING_DATA"
CONTROL = STG / "_CONTROL"

BTS_RAW = RAW / "BTS" / "csv"
BTS_STG = STG / "BTS"

OUR_RAW = RAW / "OurAirports"
OUR_STG = STG / "OurAirports"

WX_RAW = RAW / "OpenMeteo" / "csv"
WX_STG = STG / "OpenMeteo"

YEAR = 2025
CHUNK = 100_000
ENCODING = "utf-8-sig"

EMPTY_BTS_COL = "Unnamed: 109"

BTS_ROWS = {
    1: 539747,
    2: 504884,
    3: 600872,
    4: 583950,
    5: 605648,
    6: 611575,
    7: 631428,
    8: 602378,
    9: 562439,
    10: 605844,
    11: 570550,
    12: 582304,
}

OUR_ROWS = 86335
WX_FILES = 240
WX_ROWS = 8760
WX_CODE96 = 113


# =============================================================================
# 2. LOGGING AND GENERAL HELPERS
# =============================================================================

def make_logger():
    CONTROL.mkdir(parents=True, exist_ok=True)

    log = logging.getLogger("step26")
    log.setLevel(logging.INFO)
    log.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    handlers = [
        logging.FileHandler(
            CONTROL / "step26_validate_staging_data.log",
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]

    for handler in handlers:
        handler.setFormatter(fmt)
        log.addHandler(handler)

    return log


def read_header(path: Path) -> list[str]:
    return list(
        pd.read_csv(
            path,
            dtype="string",
            keep_default_na=False,
            encoding=ENCODING,
            nrows=0
        ).columns
    )


def is_text_type(dtype) -> bool:
    return (
        pa.types.is_string(dtype)
        or pa.types.is_large_string(dtype)
    )


def sample_positions(row_count: int) -> list[int]:
    if row_count <= 0:
        return []

    return sorted(set([
        0,
        row_count // 2,
        row_count - 1
    ]))


# =============================================================================
# 3. READ SAMPLE ROWS FROM RAW
# =============================================================================

def collect_raw_samples(
    path: Path,
    columns: list[str],
    row_count: int,
    group: str,
    month: str | None
):
    wanted = set(sample_positions(row_count))
    found = {}
    offset = 0

    reader = pd.read_csv(
        path,
        dtype="string",
        keep_default_na=False,
        encoding=ENCODING,
        chunksize=CHUNK,
        low_memory=False
    )

    for chunk in reader:

        positions = [
            p for p in wanted
            if offset <= p < offset + len(chunk)
        ]

        for pos in positions:

            row = {
                col: str(chunk.iloc[pos - offset][col])
                for col in columns
                if not (
                    group == "BTS"
                    and col == EMPTY_BTS_COL
                )
            }

            row["SourceFile"] = path.name

            if group == "BTS":
                row["SourceMonth"] = month

            found[pos] = row

        offset += len(chunk)

    return found


# =============================================================================
# 4. COMMON RAW VS PARQUET VALIDATION
# =============================================================================

def validate_pair(
    group: str,
    source_path: Path,
    parquet_path: Path,
    expected_rows: int,
    month: str | None = None
) -> dict:

    result = {
        "SourceGroup": group,
        "SourceFile": source_path.name,
        "StagingFile": str(parquet_path),
        "ExpectedRows": expected_rows,
        "ActualRows": None,
        "ExpectedColumns": None,
        "ActualColumns": None,
        "AllColumnsText": False,
        "ParquetNullCells": None,
        "SourceFileLineageOK": False,
        "SourceMonthLineageOK": None,
        "SampleRowsMatchRAW": False,
        "DuplicateTimestampCount": None,
        "WeatherCode96Count": None,
        "DuplicateAirportIDCount": None,
        "LiteralNAContinentCount": None,
        "LiteralNAIsoCountryCount": None,
        "Status": "FAILED",
        "Message": "",
    }

    if not source_path.is_file():
        raise FileNotFoundError(
            f"RAW source missing: {source_path}"
        )

    if not parquet_path.is_file():
        raise FileNotFoundError(
            f"STAGING Parquet missing: {parquet_path}"
        )

    raw_columns = read_header(source_path)

    source_columns = [
        col for col in raw_columns
        if not (
            group == "BTS"
            and col == EMPTY_BTS_COL
        )
    ]

    expected_columns = (
        source_columns
        + ["SourceFile"]
        + (["SourceMonth"] if group == "BTS" else [])
    )

    parquet_file = pq.ParquetFile(parquet_path)
    actual_columns = parquet_file.schema_arrow.names

    result["ExpectedColumns"] = len(expected_columns)
    result["ActualColumns"] = len(actual_columns)

    # Schema validation
    if actual_columns != expected_columns:
        raise ValueError(
            f"Schema mismatch.\n"
            f"Expected={expected_columns}\n"
            f"Actual={actual_columns}"
        )

    # Verify that every field remains text
    if not all(
        is_text_type(field.type)
        for field in parquet_file.schema_arrow
    ):
        bad_columns = [
            (field.name, str(field.type))
            for field in parquet_file.schema_arrow
            if not is_text_type(field.type)
        ]

        raise TypeError(
            f"Non-text Parquet columns: {bad_columns}"
        )

    result["AllColumnsText"] = True

    # Scan all Parquet batches
    null_cells = 0
    source_files = set()
    months = set()
    samples = {}

    wanted = set(sample_positions(expected_rows))
    offset = 0

    for batch in parquet_file.iter_batches(
        batch_size=CHUNK,
        columns=actual_columns
    ):

        null_cells += sum(
            batch.column(i).null_count
            for i in range(batch.num_columns)
        )

        if "SourceFile" in actual_columns:
            values = batch.column(
                actual_columns.index("SourceFile")
            ).to_pylist()

            source_files.update(
                str(v) for v in values
                if v is not None
            )

        if group == "BTS":
            values = batch.column(
                actual_columns.index("SourceMonth")
            ).to_pylist()

            months.update(
                str(v) for v in values
                if v is not None
            )

        batch_end = offset + batch.num_rows

        positions = [
            p for p in wanted
            if offset <= p < batch_end
        ]

        for pos in positions:
            local_pos = pos - offset

            samples[pos] = {
                col: batch.column(i)[local_pos].as_py()
                for i, col in enumerate(actual_columns)
            }

        offset = batch_end

    actual_rows = int(parquet_file.metadata.num_rows)

    result["ActualRows"] = actual_rows
    result["ParquetNullCells"] = null_cells
    result["SourceFileLineageOK"] = (
        source_files == {source_path.name}
    )

    if group == "BTS":
        result["SourceMonthLineageOK"] = (
            months == {month}
        )

    if actual_rows != expected_rows:
        raise ValueError(
            f"Row count mismatch: expected={expected_rows:,}; "
            f"actual={actual_rows:,}"
        )

    if null_cells:
        raise ValueError(
            f"Found {null_cells:,} Parquet NULL cells."
        )

    if not result["SourceFileLineageOK"]:
        raise ValueError(
            f"SourceFile lineage mismatch: {source_files}"
        )

    if (
        group == "BTS"
        and not result["SourceMonthLineageOK"]
    ):
        raise ValueError(
            f"SourceMonth lineage mismatch: {months}"
        )

    # Compare first, middle and last rows with RAW
    original_samples = collect_raw_samples(
        source_path,
        raw_columns,
        expected_rows,
        group,
        month
    )

    if set(original_samples) != set(samples):
        raise ValueError(
            "Could not collect identical sample positions."
        )

    sample_match = all(
        original_samples[pos] == samples[pos]
        for pos in original_samples
    )

    if not sample_match:
        raise ValueError(
            "First/middle/last sample rows do not match RAW exactly."
        )

    result["SampleRowsMatchRAW"] = True
    result["Status"] = "SUCCESS"
    result["Message"] = (
        "Rows, schema, text types, nulls, lineage "
        "and RAW sample checks passed."
    )

    return result


# =============================================================================
# 5. BTS VALIDATION
# =============================================================================

def validate_bts(log):

    results = []

    for month, expected_rows in BTS_ROWS.items():

        source_path = (
            BTS_RAW
            / f"BTS_{YEAR}_{month:02d}.csv"
        )

        parquet_path = (
            BTS_STG
            / f"stg_bts_{YEAR}_{month:02d}.parquet"
        )

        source_month = f"{YEAR}-{month:02d}"

        log.info("Validating BTS %02d", month)

        try:
            results.append(
                validate_pair(
                    "BTS",
                    source_path,
                    parquet_path,
                    expected_rows,
                    source_month
                )
            )

        except Exception as exc:
            log.exception(
                "BTS validation failed: %s",
                source_path.name
            )

            results.append({
                "SourceGroup": "BTS",
                "SourceFile": source_path.name,
                "StagingFile": str(parquet_path),
                "ExpectedRows": expected_rows,
                "Status": "FAILED",
                "Message": repr(exc)
            })

    return results


# =============================================================================
# 6. OURAIRPORTS VALIDATION
# =============================================================================

def validate_ourairports(log):

    results = []

    jobs = [
        ("airports.csv", "stg_ourairports.parquet"),
        (
            "airports_reference.csv",
            "stg_ourairports_reference.parquet"
        ),
    ]

    for source_name, parquet_name in jobs:

        source_path = OUR_RAW / source_name
        parquet_path = OUR_STG / parquet_name

        log.info(
            "Validating OurAirports %s",
            source_name
        )

        try:
            result = validate_pair(
                "OurAirports",
                source_path,
                parquet_path,
                OUR_ROWS
            )

            parquet_file = pq.ParquetFile(parquet_path)

            ids = set()
            duplicates = 0
            na_continent = 0
            na_country = 0

            columns = [
                col for col in [
                    "id",
                    "continent",
                    "iso_country"
                ]
                if col in parquet_file.schema_arrow.names
            ]

            if "id" not in columns:
                raise ValueError(
                    "OurAirports source has no id column."
                )

            for batch in parquet_file.iter_batches(
                batch_size=CHUNK,
                columns=columns
            ):

                for i, column in enumerate(columns):

                    values = batch.column(i).to_pylist()

                    if column == "id":

                        for value in values:

                            if value is None or value == "":
                                continue

                            if value in ids:
                                duplicates += 1

                            ids.add(value)

                    elif column == "continent":
                        na_continent += sum(
                            value == "NA"
                            for value in values
                        )

                    elif column == "iso_country":
                        na_country += sum(
                            value == "NA"
                            for value in values
                        )

            result["DuplicateAirportIDCount"] = duplicates

            result["LiteralNAContinentCount"] = (
                na_continent
                if "continent" in columns
                else None
            )

            result["LiteralNAIsoCountryCount"] = (
                na_country
                if "iso_country" in columns
                else None
            )

            if duplicates:
                raise ValueError(
                    f"Found {duplicates} repeated airport ID values."
                )

            result["Message"] += (
                f" Unique IDs; literal NA: "
                f"continent={result['LiteralNAContinentCount']}, "
                f"iso_country={result['LiteralNAIsoCountryCount']}."
            )

            results.append(result)

        except Exception as exc:
            log.exception(
                "OurAirports validation failed: %s",
                source_name
            )

            results.append({
                "SourceGroup": "OurAirports",
                "SourceFile": source_name,
                "StagingFile": str(parquet_path),
                "ExpectedRows": OUR_ROWS,
                "Status": "FAILED",
                "Message": repr(exc)
            })

    return results


# =============================================================================
# 7. OPEN-METEO VALIDATION
# =============================================================================

def validate_weather(log):

    results = []

    files = sorted(
        WX_RAW.glob(f"weather_*_{YEAR}.csv")
    )

    expected_times = pd.date_range(
        f"{YEAR}-01-01",
        periods=WX_ROWS,
        freq="h"
    ).strftime(
        "%Y-%m-%dT%H:%M"
    ).tolist()

    if len(files) != WX_FILES:
        log.error(
            "Expected %s Open-Meteo files; found %s",
            WX_FILES,
            len(files)
        )

    for index, source_path in enumerate(files, 1):

        airport = (
            source_path.name
            .replace("weather_", "")
            .replace(f"_{YEAR}.csv", "")
        )

        parquet_path = (
            WX_STG
            / f"weather_{airport}_{YEAR}.parquet"
        )

        log.info(
            "Validating Open-Meteo [%s/%s] %s",
            index,
            len(files),
            airport
        )

        try:
            result = validate_pair(
                "OpenMeteo",
                source_path,
                parquet_path,
                WX_ROWS
            )

            parquet_file = pq.ParquetFile(parquet_path)

            timestamps = []
            airport_codes = set()
            source_files = set()
            code96 = 0

            for batch in parquet_file.iter_batches(
                batch_size=CHUNK,
                columns=[
                    "time",
                    "weather_code",
                    "AirportCode",
                    "SourceFile"
                ]
            ):

                times = batch.column(0).to_pylist()
                weather_codes = batch.column(1).to_pylist()
                codes = batch.column(2).to_pylist()
                sources = batch.column(3).to_pylist()

                timestamps.extend(
                    str(value)
                    for value in times
                    if value is not None
                )

                code96 += sum(
                    value == "96"
                    for value in weather_codes
                )

                airport_codes.update(
                    str(value)
                    for value in codes
                    if value is not None
                )

                source_files.update(
                    str(value)
                    for value in sources
                    if value is not None
                )

            duplicate_count = (
                len(timestamps)
                - len(set(timestamps))
            )

            result["DuplicateTimestampCount"] = (
                duplicate_count
            )

            result["WeatherCode96Count"] = code96

            if duplicate_count:
                raise ValueError(
                    f"{duplicate_count} duplicate timestamps in Parquet."
                )

            if timestamps != expected_times:
                raise ValueError(
                    "Timestamp sequence is not all 8,760 "
                    "ordered UTC hours of 2025."
                )

            if airport_codes != {airport}:
                raise ValueError(
                    f"AirportCode mismatch: {airport_codes}"
                )

            if source_files != {source_path.name}:
                raise ValueError(
                    f"SourceFile lineage mismatch: {source_files}"
                )

            result["Message"] += (
                f" Complete ordered UTC timestamps; "
                f"WeatherCode96={code96}."
            )

            results.append(result)

        except Exception as exc:
            log.exception(
                "Open-Meteo validation failed: %s",
                source_path.name
            )

            results.append({
                "SourceGroup": "OpenMeteo",
                "SourceFile": source_path.name,
                "StagingFile": str(parquet_path),
                "ExpectedRows": WX_ROWS,
                "Status": "FAILED",
                "Message": repr(exc)
            })

    return results


# =============================================================================
# 8. MAIN AND SUMMARY
# =============================================================================

def main():

    log = make_logger()

    run_stamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    log.info(
        "STEP 26 - INDEPENDENT STAGING VALIDATION (READ-ONLY)"
    )

    records = (
        validate_bts(log)
        + validate_ourairports(log)
        + validate_weather(log)
    )

    df = pd.DataFrame(records)

    manifest_path = (
        CONTROL / "26_staging_validation_manifest.csv"
    )

    summary_path = (
        CONTROL / "26_staging_validation_summary.txt"
    )

    df.to_csv(
        manifest_path,
        index=False,
        encoding="utf-8-sig"
    )

    success_count = int(
        df["Status"].eq("SUCCESS").sum()
    )

    failed_count = len(df) - success_count

    bts = df[df["SourceGroup"] == "BTS"]
    our = df[df["SourceGroup"] == "OurAirports"]
    weather = df[df["SourceGroup"] == "OpenMeteo"]

    bts_rows = int(
        pd.to_numeric(
            bts.get("ActualRows"),
            errors="coerce"
        ).fillna(0).sum()
    )

    our_rows = int(
        pd.to_numeric(
            our.get("ActualRows"),
            errors="coerce"
        ).fillna(0).sum()
    )

    weather_rows = int(
        pd.to_numeric(
            weather.get("ActualRows"),
            errors="coerce"
        ).fillna(0).sum()
    )

    code96_rows = int(
        pd.to_numeric(
            weather.get("WeatherCode96Count"),
            errors="coerce"
        ).fillna(0).sum()
    )

    null_cells = int(
        pd.to_numeric(
            df.get("ParquetNullCells"),
            errors="coerce"
        ).fillna(0).sum()
    )

    sample_pass = int(
        df.get(
            "SampleRowsMatchRAW",
            pd.Series(dtype=bool)
        ).fillna(False).sum()
    )

    checks = {
        "BTS files 12/12": len(bts) == 12,
        "BTS rows": bts_rows == sum(BTS_ROWS.values()),
        "OurAirports files 2/2": len(our) == 2,
        "OurAirports rows": our_rows == OUR_ROWS * 2,
        "Open-Meteo files 240/240": len(weather) == WX_FILES,
        "Open-Meteo rows": weather_rows == WX_FILES * WX_ROWS,
        "WeatherCode 96 rows": code96_rows == WX_CODE96,
        "All file validations successful": failed_count == 0,
        "No Parquet null cells": null_cells == 0,
        "RAW sample checks pass for every file": sample_pass == len(df),
    }

    lines = [
        "DATA WAREHOUSE - STEP 26 INDEPENDENT STAGING VALIDATION",
        "=" * 78,
        f"Run timestamp: {run_stamp}",
        f"Files validated: {len(df)}",
        f"Successful: {success_count}",
        f"Failed: {failed_count}",
        "",
        f"BTS rows read back: {bts_rows:,} / {sum(BTS_ROWS.values()):,}",
        f"OurAirports rows read back: {our_rows:,} / {OUR_ROWS * 2:,}",
        f"Open-Meteo rows read back: {weather_rows:,} / {WX_FILES * WX_ROWS:,}",
        f"WeatherCode=96 rows read back: {code96_rows:,} / {WX_CODE96:,}",
        f"Parquet null cells: {null_cells:,}",
        f"RAW sample checks passed: {sample_pass}/{len(df)} files",
        "",
        "GLOBAL CHECKS",
        "-" * 78,
    ]

    lines += [
        f"{name}: {'PASS' if passed else 'FAIL'}"
        for name, passed in checks.items()
    ]

    lines += [
        "",
        "NOTES",
        "- This step independently reads Parquet outputs; it does not rely only on Step 25 counters.",
        "- First, middle and last rows of each file are compared exactly with RAW.",
        "- Parquet schema is checked against RAW headers; all staged columns must remain strings.",
        "- Open-Meteo timestamps must equal the complete ordered UTC hourly sequence for 2025.",
        "- No RAW or STAGING data file is modified.",
        "",
        "OUTPUT FILES",
        str(manifest_path),
        str(summary_path),
    ]

    if failed_count:
        lines += [
            "",
            "FAILED FILES",
            "-" * 78,
        ]

        for _, row in df[
            df["Status"] != "SUCCESS"
        ].iterrows():

            lines.append(
                f"{row['SourceGroup']} | "
                f"{row['SourceFile']} | "
                f"{row['Message']}"
            )

    summary_path.write_text(
        "\n".join(lines),
        encoding="utf-8"
    )

    log.info(
        "Validation summary: %s",
        summary_path
    )

    log.info(
        "Validation manifest: %s",
        manifest_path
    )

    log.info(
        "Step 26 finished: %s successes, %s failures",
        success_count,
        failed_count
    )

    return (
        0
        if failed_count == 0 and all(checks.values())
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
