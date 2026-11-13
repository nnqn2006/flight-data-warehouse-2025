
"""STEP 26C - Full BTS lineage validation by Parquet row group.

Read-only diagnostic. Reads only SourceFile and SourceMonth from each
Parquet row group and validates EVERY row against expected lineage values.
It does not modify RAW or STAGING files.

Run from D:\\DATA_WAREHOUSE:
python .\\08_SCRIPTS\\02_profiling\\step26c_validate_bts_lineage.py

Dependencies: pyarrow, pandas
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
STG_DIR = ROOT / "03_STAGING_DATA" / "BTS"
CONTROL_DIR = ROOT / "03_STAGING_DATA" / "_CONTROL"

YEAR = 2025

EXPECTED_ROWS = {
    1: 539747, 2: 504884, 3: 600872, 4: 583950,
    5: 605648, 6: 611575, 7: 631428, 8: 602378,
    9: 562439, 10: 605844, 11: 570550, 12: 582304,
}

MAX_EXAMPLES_PER_MONTH = 30


def make_logger() -> logging.Logger:
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("step26c_bts_lineage")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    for handler in (
        logging.FileHandler(
            CONTROL_DIR / "step26c_bts_lineage.log",
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ):
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def validate_month(month: int, logger: logging.Logger):
    month_label = f"{YEAR}-{month:02d}"
    file_name = f"BTS_{YEAR}_{month:02d}.csv"

    path = STG_DIR / f"stg_bts_{YEAR}_{month:02d}.parquet"

    if not path.is_file():
        raise FileNotFoundError(
            f"Staging file not found: {path}"
        )

    expected_file = file_name
    expected_month = month_label

    pf = pq.ParquetFile(path)
    names = pf.schema_arrow.names

    for required in ("SourceFile", "SourceMonth"):
        if required not in names:
            raise ValueError(
                f"Missing lineage column {required} in {path.name}"
            )

    total_rows = 0
    total_file_null = 0
    total_month_null = 0
    total_file_wrong = 0
    total_month_wrong = 0

    row_group_rows = []
    examples = []

    logger.info(
        "%s: checking %s rows across %s row groups",
        path.name,
        pf.metadata.num_rows,
        pf.metadata.num_row_groups
    )

    # Read and validate each row group independently.
    for rg_index in range(pf.metadata.num_row_groups):
        table = pf.read_row_group(
            rg_index,
            columns=["SourceFile", "SourceMonth"]
        )

        file_values = table["SourceFile"].to_pylist()
        month_values = table["SourceMonth"].to_pylist()

        rg_rows = len(file_values)

        if len(month_values) != rg_rows:
            raise ValueError(
                f"Lineage column length mismatch in row group {rg_index}"
            )

        file_null = sum(v is None for v in file_values)
        month_null = sum(v is None for v in month_values)

        file_wrong = sum(
            v != expected_file for v in file_values
        )

        month_wrong = sum(
            v != expected_month for v in month_values
        )

        rg_start = total_rows
        rg_end = total_rows + rg_rows - 1

        row_group_rows.append({
            "Month": month_label,
            "StagingFile": path.name,
            "RowGroup": rg_index,
            "RowStartZeroBased": rg_start,
            "RowEndZeroBased": rg_end,
            "Rows": rg_rows,
            "SourceFileNullCount": file_null,
            "SourceMonthNullCount": month_null,
            "SourceFileUnexpectedCount": file_wrong,
            "SourceMonthUnexpectedCount": month_wrong,
            "SourceFileDistinctValues": " | ".join(
                sorted({repr(v) for v in file_values})
            )[:500],
            "SourceMonthDistinctValues": " | ".join(
                sorted({repr(v) for v in month_values})
            )[:500],
        })

        # Keep up to 30 examples of unexpected values per month.
        for local_index, (fv, mv) in enumerate(
            zip(file_values, month_values)
        ):
            if fv != expected_file or mv != expected_month:
                if len(examples) < MAX_EXAMPLES_PER_MONTH:
                    examples.append({
                        "Month": month_label,
                        "RowPositionZeroBased": (
                            rg_start + local_index
                        ),
                        "RowGroup": rg_index,
                        "RowPositionWithinGroup": local_index,
                        "SourceFileValueRepr": repr(fv),
                        "ExpectedSourceFile": expected_file,
                        "SourceMonthValueRepr": repr(mv),
                        "ExpectedSourceMonth": expected_month,
                    })

        total_file_null += file_null
        total_month_null += month_null
        total_file_wrong += file_wrong
        total_month_wrong += month_wrong
        total_rows += rg_rows

        logger.info(
            "%s row_group=%s rows=%s "
            "SourceFile null=%s unexpected=%s; "
            "SourceMonth null=%s unexpected=%s",
            month_label,
            rg_index,
            rg_rows,
            file_null,
            file_wrong,
            month_null,
            month_wrong
        )

    row_count_ok = total_rows == EXPECTED_ROWS[month]

    lineage_ok = (
        total_file_wrong == 0
        and total_month_wrong == 0
    )

    summary = {
        "Month": month_label,
        "StagingFile": path.name,
        "ParquetRows": total_rows,
        "ExpectedRows": EXPECTED_ROWS[month],
        "RowGroups": pf.metadata.num_row_groups,
        "SourceFileNullCount": total_file_null,
        "SourceMonthNullCount": total_month_null,
        "SourceFileUnexpectedCount": total_file_wrong,
        "SourceMonthUnexpectedCount": total_month_wrong,
        "RowCountOK": row_count_ok,
        "LineageOK_AllRows": lineage_ok,
        "Status": (
            "PASS"
            if row_count_ok and lineage_ok
            else "REVIEW"
        ),
    }

    return summary, row_group_rows, examples


def main() -> int:
    logger = make_logger()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger.info(
        "STEP 26C - FULL BTS LINEAGE VALIDATION (READ-ONLY)"
    )

    summaries = []
    row_groups = []
    examples = []

    for month in range(1, 13):
        try:
            summary, rg_rows, month_examples = validate_month(
                month,
                logger
            )

            summaries.append(summary)
            row_groups.extend(rg_rows)
            examples.extend(month_examples)

            logger.info(
                "%s final status=%s, "
                "SourceFile unexpected=%s, "
                "SourceMonth unexpected=%s",
                summary["Month"],
                summary["Status"],
                summary["SourceFileUnexpectedCount"],
                summary["SourceMonthUnexpectedCount"]
            )

        except Exception as exc:
            logger.exception(
                "Failed to validate month %02d",
                month
            )

            summaries.append({
                "Month": f"{YEAR}-{month:02d}",
                "StagingFile": (
                    f"stg_bts_{YEAR}_{month:02d}.parquet"
                ),
                "Status": "FAILED",
                "Error": repr(exc),
            })

    summary_df = pd.DataFrame(summaries)
    row_group_df = pd.DataFrame(row_groups)
    examples_df = pd.DataFrame(examples)

    summary_csv = (
        CONTROL_DIR / "26c_bts_lineage_summary.csv"
    )

    row_group_csv = (
        CONTROL_DIR / "26c_bts_lineage_by_row_group.csv"
    )

    examples_csv = (
        CONTROL_DIR / "26c_bts_lineage_bad_examples.csv"
    )

    summary_txt = (
        CONTROL_DIR / "26c_bts_lineage_summary.txt"
    )

    summary_df.to_csv(
        summary_csv,
        index=False,
        encoding="utf-8-sig"
    )

    row_group_df.to_csv(
        row_group_csv,
        index=False,
        encoding="utf-8-sig"
    )

    examples_df.to_csv(
        examples_csv,
        index=False,
        encoding="utf-8-sig"
    )

    passed = int(
        summary_df.get(
            "Status",
            pd.Series(dtype=str)
        ).eq("PASS").sum()
    )

    review = int(
        summary_df.get(
            "Status",
            pd.Series(dtype=str)
        ).eq("REVIEW").sum()
    )

    failed = int(
        summary_df.get(
            "Status",
            pd.Series(dtype=str)
        ).eq("FAILED").sum()
    )

    summary_txt.write_text(
        "STEP 26C - FULL BTS LINEAGE VALIDATION\n"
        + "=" * 72 + "\n"
        + f"Run timestamp: {stamp}\n"
        + f"Months PASS: {passed}/12\n"
        + f"Months REVIEW: {review}/12\n"
        + f"Months FAILED: {failed}/12\n\n"
        + summary_df.to_string(index=False)
        + "\n\n"
        + "This check reads every value in SourceFile "
        + "and SourceMonth.\n"
        + "RAW and STAGING files are not modified.\n"
        + f"Row-group report: {row_group_csv}\n"
        + f"Bad examples: {examples_csv}\n",
        encoding="utf-8"
    )

    logger.info(
        "Step 26C completed. Summary: %s",
        summary_txt
    )

    logger.info(
        "Row-group detail: %s",
        row_group_csv
    )

    logger.info(
        "Bad examples: %s",
        examples_csv
    )

    return (
        0
        if passed == 12 and review == 0 and failed == 0
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
