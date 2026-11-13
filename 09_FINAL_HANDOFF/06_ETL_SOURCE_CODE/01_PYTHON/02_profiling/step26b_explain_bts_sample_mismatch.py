
"""STEP 26B - Explain BTS first/middle/last sample mismatches.

Read-only diagnostic. It does not modify RAW or STAGING data.
For each month, it compares the exact first, middle, and last RAW rows
against the same zero-based row positions in the corresponding Parquet file.
It reports every differing column and both values using repr() so that
empty strings, whitespace, and unusual text are visible.

Run from the project root or via:
python .\\08_SCRIPTS\\02_profiling\\step26b_explain_bts_sample_mismatch.py
Dependencies: pandas, pyarrow
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
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


def make_logger() -> logging.Logger:
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("step26b_bts_sample_diff")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler = logging.FileHandler(
        CONTROL_DIR / "step26b_bts_sample_diff.log",
        encoding="utf-8"
    )
    console_handler = logging.StreamHandler(sys.stdout)

    for handler in (file_handler, console_handler):
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def comparable(value):
    """Preserve exact text; represent a true missing scalar as None."""
    if value is None or value is pd.NA:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    return str(value)


def read_raw_samples(
    source: Path,
    source_columns: list[str],
    positions: set[int],
    month: int
):
    samples = {}
    offset = 0

    for chunk in pd.read_csv(
        source,
        dtype="string",
        keep_default_na=False,
        encoding=ENCODING,
        chunksize=CHUNK_SIZE,
        low_memory=False,
    ):
        selected = [
            p for p in positions
            if offset <= p < offset + len(chunk)
        ]

        for pos in selected:
            row = chunk.iloc[pos - offset]

            sample = {
                col: comparable(row[col])
                for col in source_columns
            }

            sample["SourceFile"] = source.name
            sample["SourceMonth"] = f"{YEAR}-{month:02d}"

            samples[pos] = sample

        offset += len(chunk)

    return samples, offset


def read_parquet_samples(
    staged: Path,
    source_columns: list[str],
    positions: set[int]
):
    pf = pq.ParquetFile(staged)
    actual_columns = pf.schema_arrow.names

    expected_columns = source_columns + [
        "SourceFile",
        "SourceMonth"
    ]

    if actual_columns != expected_columns:
        raise ValueError(
            f"Schema mismatch: expected {len(expected_columns)} columns, "
            f"found {len(actual_columns)}."
        )

    samples = {}
    offset = 0

    for batch in pf.iter_batches(
        batch_size=CHUNK_SIZE,
        columns=actual_columns
    ):
        selected = [
            p for p in positions
            if offset <= p < offset + batch.num_rows
        ]

        for pos in selected:
            local = pos - offset

            samples[pos] = {
                col: comparable(batch.column(i)[local].as_py())
                for i, col in enumerate(actual_columns)
            }

        offset += batch.num_rows

    return samples, offset


def diagnose_month(month: int, logger: logging.Logger):
    source = RAW_DIR / f"BTS_{YEAR}_{month:02d}.csv"

    staged = STG_DIR / (
        f"stg_bts_{YEAR}_{month:02d}.parquet"
    )

    if not source.is_file():
        raise FileNotFoundError(
            f"RAW file not found: {source}"
        )

    if not staged.is_file():
        raise FileNotFoundError(
            f"STAGING file not found: {staged}"
        )

    raw_columns = list(
        pd.read_csv(
            source,
            dtype="string",
            keep_default_na=False,
            encoding=ENCODING,
            nrows=0
        ).columns
    )

    source_columns = [
        c for c in raw_columns
        if c != DROPPED_EMPTY_COLUMN
    ]

    positions = {
        0,
        EXPECTED_ROWS[month] // 2,
        EXPECTED_ROWS[month] - 1
    }

    logger.info(
        "Comparing samples for %s",
        source.name
    )

    raw_samples, raw_rows = read_raw_samples(
        source,
        source_columns,
        positions,
        month
    )

    parquet_samples, parquet_rows = read_parquet_samples(
        staged,
        source_columns,
        positions
    )

    diffs = []

    for pos in sorted(positions):
        raw_row = raw_samples.get(pos)
        parquet_row = parquet_samples.get(pos)

        if raw_row is None or parquet_row is None:
            diffs.append({
                "Month": f"{YEAR}-{month:02d}",
                "RowPositionZeroBased": pos,
                "Column": "<ROW_NOT_FOUND>",
                "RawValueRepr": repr(raw_row),
                "ParquetValueRepr": repr(parquet_row),
                "RawValueLength": None,
                "ParquetValueLength": None,
            })
            continue

        for col in source_columns + [
            "SourceFile",
            "SourceMonth"
        ]:
            raw_value = raw_row.get(col)
            parquet_value = parquet_row.get(col)

            if raw_value != parquet_value:
                diffs.append({
                    "Month": f"{YEAR}-{month:02d}",
                    "RowPositionZeroBased": pos,
                    "Column": col,
                    "RawValueRepr": repr(raw_value),
                    "ParquetValueRepr": repr(parquet_value),
                    "RawValueLength": (
                        len(raw_value)
                        if isinstance(raw_value, str)
                        else None
                    ),
                    "ParquetValueLength": (
                        len(parquet_value)
                        if isinstance(parquet_value, str)
                        else None
                    ),
                })

    summary = {
        "Month": f"{YEAR}-{month:02d}",
        "SourceFile": source.name,
        "RawRowsRead": raw_rows,
        "ParquetRowsRead": parquet_rows,
        "ExpectedRows": EXPECTED_ROWS[month],
        "SamplePositionsChecked": len(positions),
        "MismatchCells": len(diffs),
        "MismatchColumns": len({
            d["Column"] for d in diffs
        }),
        "SampleParity": (
            "PASS"
            if (
                not diffs
                and raw_rows == parquet_rows == EXPECTED_ROWS[month]
            )
            else "REVIEW"
        ),
    }

    logger.info(
        "%s: sample parity=%s, mismatch cells=%s, mismatch columns=%s",
        source.name,
        summary["SampleParity"],
        summary["MismatchCells"],
        summary["MismatchColumns"]
    )

    for diff in diffs[:12]:
        logger.warning(
            "%s row=%s column=%s RAW=%s Parquet=%s",
            source.name,
            diff["RowPositionZeroBased"],
            diff["Column"],
            diff["RawValueRepr"],
            diff["ParquetValueRepr"]
        )

    if len(diffs) > 12:
        logger.warning(
            "%s: %s additional mismatch cells are in the CSV detail report",
            source.name,
            len(diffs) - 12
        )

    return summary, diffs


def main() -> int:
    logger = make_logger()

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger.info(
        "STEP 26B - EXPLAIN BTS SAMPLE ROW MISMATCHES (READ-ONLY)"
    )

    summaries = []
    all_diffs = []

    for month in range(1, 13):
        try:
            summary, diffs = diagnose_month(
                month,
                logger
            )

            summaries.append(summary)
            all_diffs.extend(diffs)

        except Exception as exc:
            logger.exception(
                "Step 26B failed for month %02d",
                month
            )

            summaries.append({
                "Month": f"{YEAR}-{month:02d}",
                "SourceFile": f"BTS_{YEAR}_{month:02d}.csv",
                "SampleParity": "FAILED",
                "Error": repr(exc),
            })

    summary_df = pd.DataFrame(summaries)
    diff_df = pd.DataFrame(all_diffs)

    summary_path = (
        CONTROL_DIR / "26b_bts_sample_diff_summary.csv"
    )

    diff_path = (
        CONTROL_DIR / "26b_bts_sample_diff_detail.csv"
    )

    summary_df.to_csv(
        summary_path,
        index=False,
        encoding="utf-8-sig"
    )

    diff_df.to_csv(
        diff_path,
        index=False,
        encoding="utf-8-sig"
    )

    passed = int(
        summary_df.get(
            "SampleParity",
            pd.Series(dtype=str)
        ).eq("PASS").sum()
    )

    failed = int(
        summary_df.get(
            "SampleParity",
            pd.Series(dtype=str)
        ).eq("FAILED").sum()
    )

    review = int(
        summary_df.get(
            "SampleParity",
            pd.Series(dtype=str)
        ).eq("REVIEW").sum()
    )

    summary_txt = (
        CONTROL_DIR / "26b_bts_sample_diff_summary.txt"
    )

    summary_txt.write_text(
        "STEP 26B - BTS SAMPLE ROW COMPARISON\n"
        + "=" * 70 + "\n"
        + f"Run timestamp: {stamp}\n"
        + f"Months PASS: {passed}/12\n"
        + f"Months REVIEW: {review}/12\n"
        + f"Months FAILED: {failed}/12\n\n"
        + summary_df.to_string(index=False)
        + "\n\nOnly sampled values are compared; "
        "RAW and STAGING are read-only.\n"
        + f"Mismatch detail: {diff_path}\n",
        encoding="utf-8",
    )

    logger.info(
        "Step 26B completed. Summary: %s",
        summary_txt
    )

    logger.info(
        "Mismatch detail: %s",
        diff_path
    )

    return (
        0
        if passed == 12 and review == 0 and failed == 0
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
