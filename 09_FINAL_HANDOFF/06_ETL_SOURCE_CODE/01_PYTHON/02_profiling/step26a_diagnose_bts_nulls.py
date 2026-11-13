
"""STEP 26A - Diagnose BTS missing-value representation in RAW vs STAGING.

This diagnostic does NOT modify RAW or STAGING data.
It compares, column by column:
  - blank/missing cells in the original BTS CSV
  - NULL cells and empty strings in the corresponding Parquet file
  - literal 'NA' strings in RAW and Parquet
It also checks row counts, schema, lineage, and first/middle/last sample rows.

Dependencies: pandas, pyarrow
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "01_RAW_DATA" / "BTS" / "csv"
STG_DIR = ROOT / "03_STAGING_DATA" / "BTS"
CONTROL_DIR = ROOT / "03_STAGING_DATA" / "_CONTROL"

YEAR = 2025
CHUNK_SIZE = 100_000
ENCODING = "utf-8-sig"
DROPPED_EMPTY_COLUMN = "Unnamed: 109"

EXPECTED_ROWS = {
    1: 539747, 2: 504884, 3: 600872, 4: 583950,
    5: 605648, 6: 611575, 7: 631428, 8: 602378,
    9: 562439, 10: 605844, 11: 570550, 12: 582304,
}


def make_logger():
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("step26a_bts_null_diagnostic")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    for handler in [
        logging.FileHandler(
            CONTROL_DIR / "step26a_bts_null_diagnostic.log",
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]:
        handler.setFormatter(fmt)
        logger.addHandler(handler)

    return logger


def scalar_count(boolean_array) -> int:
    value = pc.sum(
        pc.cast(boolean_array, pa.int64()),
        skip_nulls=True
    ).as_py()

    return int(value or 0)


def norm(value):
    """Treat CSV blank and Parquet NULL as the same missing representation."""
    if value is None or value is pd.NA:
        return None

    if isinstance(value, str) and value == "":
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    return str(value)


def diagnose_month(month: int, logger: logging.Logger):

    source = RAW_DIR / f"BTS_{YEAR}_{month:02d}.csv"
    staged = STG_DIR / f"stg_bts_{YEAR}_{month:02d}.parquet"

    if not source.is_file():
        raise FileNotFoundError(f"RAW file not found: {source}")

    if not staged.is_file():
        raise FileNotFoundError(f"STAGING file not found: {staged}")

    raw_columns = list(pd.read_csv(
        source,
        dtype="string",
        keep_default_na=False,
        encoding=ENCODING,
        nrows=0
    ).columns)

    source_columns = [
        c for c in raw_columns
        if c != DROPPED_EMPTY_COLUMN
    ]

    expected_staging_columns = (
        source_columns + ["SourceFile", "SourceMonth"]
    )

    raw_blank = {c: 0 for c in source_columns}
    raw_literal_na = {c: 0 for c in source_columns}

    raw_samples = {}
    rows_read = 0

    expected_sample_positions = {
        0,
        EXPECTED_ROWS[month] // 2,
        EXPECTED_ROWS[month] - 1,
    }

    # ---------------------------------------------------------
    # A. SCAN RAW CSV
    # ---------------------------------------------------------

    logger.info("Scanning RAW %s", source.name)

    for chunk in pd.read_csv(
        source,
        dtype="string",
        keep_default_na=False,
        encoding=ENCODING,
        chunksize=CHUNK_SIZE,
        low_memory=False
    ):

        for col in source_columns:
            series = chunk[col]

            raw_blank[col] += int(
                (
                    series.isna()
                    | series.eq("").fillna(False)
                ).sum()
            )

            raw_literal_na[col] += int(
                series.eq("NA").fillna(False).sum()
            )

        chunk_end = rows_read + len(chunk)

        positions = [
            p for p in expected_sample_positions
            if rows_read <= p < chunk_end
        ]

        for pos in positions:

            raw_samples[pos] = {
                col: norm(chunk.iloc[pos - rows_read][col])
                for col in source_columns
            }

            raw_samples[pos]["SourceFile"] = source.name
            raw_samples[pos]["SourceMonth"] = (
                f"{YEAR}-{month:02d}"
            )

        rows_read += len(chunk)

    # ---------------------------------------------------------
    # B. SCAN PARQUET
    # ---------------------------------------------------------

    pf = pq.ParquetFile(staged)
    actual_columns = pf.schema_arrow.names

    if actual_columns != expected_staging_columns:
        raise ValueError(
            f"Schema mismatch in {staged.name}: "
            f"expected {len(expected_staging_columns)} columns, "
            f"found {len(actual_columns)}."
        )

    non_text = [
        (field.name, str(field.type))
        for field in pf.schema_arrow
        if not (
            pa.types.is_string(field.type)
            or pa.types.is_large_string(field.type)
        )
    ]

    if non_text:
        raise TypeError(
            f"Non-text Parquet columns in {staged.name}: {non_text}"
        )

    parquet_null = {c: 0 for c in source_columns}
    parquet_empty = {c: 0 for c in source_columns}
    parquet_literal_na = {c: 0 for c in source_columns}

    parquet_samples = {}
    source_files = set()
    source_months = set()
    rows_parquet = 0

    logger.info("Scanning Parquet %s", staged.name)

    for batch in pf.iter_batches(
        batch_size=CHUNK_SIZE,
        columns=actual_columns
    ):

        for col in source_columns:
            arr = batch.column(actual_columns.index(col))

            parquet_null[col] += arr.null_count

            parquet_empty[col] += scalar_count(
                pc.equal(arr, "")
            )

            parquet_literal_na[col] += scalar_count(
                pc.equal(arr, "NA")
            )

        sf = batch.column(
            actual_columns.index("SourceFile")
        ).to_pylist()

        sm = batch.column(
            actual_columns.index("SourceMonth")
        ).to_pylist()

        source_files.update(
            v for v in sf if v is not None
        )

        source_months.update(
            v for v in sm if v is not None
        )

        batch_end = rows_parquet + batch.num_rows

        positions = [
            p for p in expected_sample_positions
            if rows_parquet <= p < batch_end
        ]

        for pos in positions:
            local = pos - rows_parquet

            parquet_samples[pos] = {
                col: norm(
                    batch.column(
                        actual_columns.index(col)
                    )[local].as_py()
                )
                for col in source_columns
            }

            parquet_samples[pos]["SourceFile"] = norm(
                batch.column(
                    actual_columns.index("SourceFile")
                )[local].as_py()
            )

            parquet_samples[pos]["SourceMonth"] = norm(
                batch.column(
                    actual_columns.index("SourceMonth")
                )[local].as_py()
            )

        rows_parquet += batch.num_rows

    # ---------------------------------------------------------
    # C. COLUMN-LEVEL MISSINGNESS COMPARISON
    # ---------------------------------------------------------

    detail_rows = []

    for col in source_columns:

        parquet_blank_equivalent = (
            parquet_null[col] + parquet_empty[col]
        )

        detail_rows.append({
            "Month": f"{YEAR}-{month:02d}",
            "SourceFile": source.name,
            "Column": col,

            "RawBlankOrMissingCount": raw_blank[col],

            "ParquetNullCount": parquet_null[col],
            "ParquetEmptyStringCount": parquet_empty[col],

            "ParquetBlankEquivalentCount": (
                parquet_blank_equivalent
            ),

            "BlankCountMatch": (
                raw_blank[col] == parquet_blank_equivalent
            ),

            "RawLiteralNACount": raw_literal_na[col],
            "ParquetLiteralNACount": parquet_literal_na[col],

            "LiteralNAMatch": (
                raw_literal_na[col] == parquet_literal_na[col]
            ),
        })

    detail = pd.DataFrame(detail_rows)

    blank_mismatch = detail.loc[
        ~detail["BlankCountMatch"]
    ]

    na_mismatch = detail.loc[
        ~detail["LiteralNAMatch"]
    ]

    sample_match = raw_samples == parquet_samples

    row_count_match = (
        rows_read == EXPECTED_ROWS[month] == rows_parquet
    )

    lineage_match = (
        source_files == {source.name}
        and source_months == {f"{YEAR}-{month:02d}"}
    )

    status = "SUCCESS" if (
        row_count_match
        and blank_mismatch.empty
        and na_mismatch.empty
        and sample_match
        and lineage_match
    ) else "REVIEW"

    summary = {
        "Month": f"{YEAR}-{month:02d}",
        "SourceFile": source.name,
        "StagingFile": str(staged),

        "RawRows": rows_read,
        "ParquetRows": rows_parquet,
        "ExpectedRows": EXPECTED_ROWS[month],

        "ParquetNullCells": sum(parquet_null.values()),
        "ParquetEmptyStringCells": sum(parquet_empty.values()),
        "RawBlankOrMissingCells": sum(raw_blank.values()),

        "RawLiteralNACells": sum(raw_literal_na.values()),
        "ParquetLiteralNACells": sum(parquet_literal_na.values()),

        "BlankCountMismatchColumns": len(blank_mismatch),
        "LiteralNAMismatchColumns": len(na_mismatch),

        "SampleRowsMatchRAW": sample_match,
        "LineageOK": lineage_match,
        "RowCountOK": row_count_match,

        "Status": status,
    }

    if not blank_mismatch.empty:
        logger.warning(
            "%s: %s columns have blank-count mismatch",
            source.name,
            len(blank_mismatch)
        )

    if not na_mismatch.empty:
        logger.warning(
            "%s: %s columns have literal-NA mismatch",
            source.name,
            len(na_mismatch)
        )

    if not sample_match:
        logger.warning(
            "%s: first/middle/last sample mismatch",
            source.name
        )

    logger.info(
        "%s: RAW=%s rows, Parquet=%s rows, "
        "RAW blanks=%s, Parquet NULL+empty=%s, Status=%s",
        source.name,
        f"{rows_read:,}",
        f"{rows_parquet:,}",
        f"{sum(raw_blank.values()):,}",
        f"{sum(parquet_null.values()) + sum(parquet_empty.values()):,}",
        status
    )

    return summary, detail


def main():

    logger = make_logger()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger.info(
        "STEP 26A - BTS NULL / BLANK REPRESENTATION DIAGNOSTIC"
    )

    summaries = []
    details = []

    for month in range(1, 13):

        try:
            summary, detail = diagnose_month(month, logger)
            summaries.append(summary)
            details.append(detail)

        except Exception as exc:

            logger.exception(
                "Diagnostic failed for month %02d",
                month
            )

            summaries.append({
                "Month": f"{YEAR}-{month:02d}",
                "SourceFile": f"BTS_{YEAR}_{month:02d}.csv",
                "Status": "FAILED",
                "Error": repr(exc),
            })

    summary_df = pd.DataFrame(summaries)

    detail_df = (
        pd.concat(details, ignore_index=True)
        if details else pd.DataFrame()
    )

    summary_path = (
        CONTROL_DIR / "26a_bts_null_diagnostic_summary.txt"
    )

    monthly_path = (
        CONTROL_DIR / "26a_bts_null_diagnostic_by_month.csv"
    )

    detail_path = (
        CONTROL_DIR / "26a_bts_null_diagnostic_by_column.csv"
    )

    summary_df.to_csv(
        monthly_path,
        index=False,
        encoding="utf-8-sig"
    )

    detail_df.to_csv(
        detail_path,
        index=False,
        encoding="utf-8-sig"
    )

    successful = int(
        summary_df.get(
            "Status", pd.Series(dtype=str)
        ).eq("SUCCESS").sum()
    )

    review = int(
        summary_df.get(
            "Status", pd.Series(dtype=str)
        ).eq("REVIEW").sum()
    )

    failed = int(
        summary_df.get(
            "Status", pd.Series(dtype=str)
        ).eq("FAILED").sum()
    )

    lines = [
        "STEP 26A - BTS NULL / BLANK REPRESENTATION DIAGNOSTIC",
        "=" * 78,
        f"Run timestamp: {stamp}",
        f"Months successful: {successful}/12",
        f"Months requiring review: {review}/12",
        f"Months failed to process: {failed}/12",
        "",
        "MONTHLY RESULTS",
        "-" * 78,
    ]

    for _, row in summary_df.iterrows():

        parquet_blank = "NA"

        if pd.notna(row.get("ParquetNullCells")):
            parquet_blank = (
                int(row["ParquetNullCells"])
                + int(row["ParquetEmptyStringCells"])
            )

        lines.append(
            f"{row.get('Month')} | {row.get('Status')} | "
            f"RAW rows={row.get('RawRows')} | "
            f"Parquet rows={row.get('ParquetRows')} | "
            f"RAW blank={row.get('RawBlankOrMissingCells')} | "
            f"Parquet NULL+empty={parquet_blank} | "
            f"blank mismatch columns={row.get('BlankCountMismatchColumns')} | "
            f"literal NA mismatch columns={row.get('LiteralNAMismatchColumns')} | "
            f"sample match={row.get('SampleRowsMatchRAW')}"
        )

    lines += [
        "",
        "INTERPRETATION",
        "- A CSV blank is treated as missing; Parquet may represent it as NULL or an empty string.",
        "- Parquet NULL + empty-string counts must equal RAW blank/missing counts per column.",
        "- Literal 'NA' is checked separately and must remain a literal string.",
        "- No value is imputed, deleted, or rewritten. RAW and STAGING are read-only.",
        "",
        "OUTPUT FILES",
        str(summary_path),
        str(monthly_path),
        str(detail_path),
    ]

    summary_path.write_text(
        "\n".join(lines),
        encoding="utf-8"
    )

    logger.info("Step 26A completed. Summary: %s", summary_path)
    logger.info("Monthly report: %s", monthly_path)
    logger.info("Column report: %s", detail_path)

    return (
        0
        if successful == 12 and review == 0 and failed == 0
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
