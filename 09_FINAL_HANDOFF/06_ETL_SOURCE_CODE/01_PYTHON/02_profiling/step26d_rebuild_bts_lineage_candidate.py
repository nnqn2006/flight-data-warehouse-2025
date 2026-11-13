
"""STEP 26D - Rebuild BTS Parquet candidates and verify every cell against RAW.

- RAW and current 03_STAGING_DATA/BTS are read-only.
- Candidate Parquet files go to 03_STAGING_DATA/_REBUILD_BTS_26D/BTS.
- Lineage is assigned positionally with pd.array (fixes the chunk-index bug).
- Every RAW and candidate Parquet value is compared in chunks.
- This script DOES NOT replace or overwrite current Staging files.

Run from D:\\DATA_WAREHOUSE:
python .\\08_SCRIPTS\\02_profiling\\step26d_rebuild_bts_lineage_candidate.py
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "01_RAW_DATA" / "BTS" / "csv"
CANDIDATE_DIR = ROOT / "03_STAGING_DATA" / "_REBUILD_BTS_26D" / "BTS"
CONTROL_DIR = ROOT / "03_STAGING_DATA" / "_CONTROL"

YEAR = 2025
CHUNK_SIZE = 100_000
CSV_ENCODING = "utf-8-sig"
EMPTY_COLUMN = "Unnamed: 109"

EXPECTED_ROWS = {
    1: 539747, 2: 504884, 3: 600872, 4: 583950,
    5: 605648, 6: 611575, 7: 631428, 8: 602378,
    9: 562439, 10: 605844, 11: 570550, 12: 582304,
}


def make_logger():
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("step26d")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    for handler in (
        logging.FileHandler(CONTROL_DIR / "step26d_bts_rebuild.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ):
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger


def prepare_chunk(chunk, source_file, source_month):
    if EMPTY_COLUMN not in chunk.columns:
        raise ValueError(f"Missing expected blank column: {EMPTY_COLUMN}")

    blank = (
        chunk[EMPTY_COLUMN].astype("string").str.strip().eq("").fillna(False)
    )
    if not bool(blank.all()):
        raise ValueError(f"{EMPTY_COLUMN} contains nonblank values; refusing to drop it")

    chunk = chunk.drop(columns=[EMPTY_COLUMN])

    # IMPORTANT: pd.array assigns by row position, not by DataFrame index.
    # read_csv(chunksize=...) preserves global indexes on later chunks.
    chunk["SourceFile"] = pd.array([source_file] * len(chunk), dtype="string")
    chunk["SourceMonth"] = pd.array([source_month] * len(chunk), dtype="string")
    return chunk


def build_candidate(month, logger):
    source_file = f"BTS_{YEAR}_{month:02d}.csv"
    source_path = RAW_DIR / source_file
    source_month = f"{YEAR}-{month:02d}"
    output_path = CANDIDATE_DIR / f"stg_bts_{YEAR}_{month:02d}.parquet"
    temp_path = output_path.with_suffix(output_path.suffix + ".part")

    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    if output_path.exists() or temp_path.exists():
        raise FileExistsError(f"Candidate already exists: {output_path}")

    writer = None
    schema = None
    rows = 0
    chunks = 0
    original_columns = None

    try:
        reader = pd.read_csv(
            source_path, dtype="string", keep_default_na=False,
            encoding=CSV_ENCODING, chunksize=CHUNK_SIZE, low_memory=False,
        )
        for chunk in reader:
            chunks += 1
            if original_columns is None:
                original_columns = list(chunk.columns)
            elif list(chunk.columns) != original_columns:
                raise ValueError(f"RAW schema changed in chunk {chunks}: {source_file}")

            prepared = prepare_chunk(chunk, source_file, source_month)
            table = pa.Table.from_pandas(prepared, preserve_index=False)

            if writer is None:
                schema = table.schema
                writer = pq.ParquetWriter(
                    temp_path, schema, compression="snappy", use_dictionary=True
                )
            else:
                if table.schema.names != schema.names or table.schema.types != schema.types:
                    raise ValueError(f"Arrow schema changed in chunk {chunks}: {source_file}")
                table = table.cast(schema)

            writer.write_table(table)
            rows += len(prepared)
            logger.info("BUILD %s chunk=%s rows=%s cumulative=%s", source_file, chunks, len(prepared), rows)

        if writer is None:
            raise ValueError(f"No data read from {source_file}")
        writer.close()
        writer = None

        if rows != EXPECTED_ROWS[month]:
            raise ValueError(f"Row count mismatch for {source_file}: {rows} != {EXPECTED_ROWS[month]}")
        if not temp_path.is_file() or temp_path.stat().st_size == 0:
            raise RuntimeError(f"Temporary Parquet missing/empty: {temp_path}")

        temp_path.replace(output_path)
        return {"BuildStatus": "BUILT", "RowsBuilt": rows, "ChunksBuilt": chunks, "Bytes": output_path.stat().st_size}

    except Exception:
        if writer is not None:
            writer.close()
        temp_path.unlink(missing_ok=True)
        raise


def validate_full_file(month, logger):
    source_file = f"BTS_{YEAR}_{month:02d}.csv"
    source_month = f"{YEAR}-{month:02d}"
    source_path = RAW_DIR / source_file
    candidate_path = CANDIDATE_DIR / f"stg_bts_{YEAR}_{month:02d}.parquet"

    pf = pq.ParquetFile(candidate_path)
    parquet_columns = pf.schema_arrow.names
    raw_header = list(pd.read_csv(
        source_path, dtype="string", keep_default_na=False,
        encoding=CSV_ENCODING, nrows=0,
    ).columns)
    expected_columns = [c for c in raw_header if c != EMPTY_COLUMN]
    expected_columns += ["SourceFile", "SourceMonth"]
    if parquet_columns != expected_columns:
        raise ValueError(f"Column schema mismatch in {candidate_path.name}")

    parquet_batches = iter(pf.iter_batches(batch_size=CHUNK_SIZE, columns=parquet_columns))
    raw_reader = pd.read_csv(
        source_path, dtype="string", keep_default_na=False,
        encoding=CSV_ENCODING, chunksize=CHUNK_SIZE, low_memory=False,
    )

    raw_rows = parquet_rows = chunks_checked = compared_cells = mismatch_cells = 0
    lineage_nulls = lineage_unexpected = 0

    for raw_chunk in raw_reader:
        chunks_checked += 1
        prepared = prepare_chunk(raw_chunk, source_file, source_month)
        raw_table = pa.Table.from_pandas(prepared, preserve_index=False)

        try:
            batch = next(parquet_batches)
        except StopIteration as exc:
            raise ValueError(f"Parquet ended before RAW chunk {chunks_checked}") from exc

        staged_table = pa.Table.from_batches([batch])
        if staged_table.num_rows != raw_table.num_rows:
            raise ValueError(
                f"Chunk row mismatch in {source_file}, chunk {chunks_checked}: "
                f"RAW={raw_table.num_rows}, Parquet={staged_table.num_rows}"
            )
        if staged_table.schema.names != raw_table.schema.names or staged_table.schema.types != raw_table.schema.types:
            raise ValueError(f"Chunk schema mismatch in {source_file}, chunk {chunks_checked}")

        # Exact comparison of EVERY value in EVERY column.
        for col in parquet_columns:
            left = raw_table[col]
            right = staged_table[col]
            compared_cells += raw_table.num_rows
            if not left.equals(right):
                lv, rv = left.to_pylist(), right.to_pylist()
                mismatch_cells += sum(a != b for a, b in zip(lv, rv))
                logger.error("DATA MISMATCH %s chunk=%s column=%s", source_file, chunks_checked, col)

        for col, expected in (("SourceFile", source_file), ("SourceMonth", source_month)):
            values = staged_table[col].to_pylist()
            lineage_nulls += sum(v is None for v in values)
            lineage_unexpected += sum(v != expected for v in values)

        raw_rows += raw_table.num_rows
        parquet_rows += staged_table.num_rows
        logger.info("VERIFY %s chunk=%s rows=%s", source_file, chunks_checked, raw_table.num_rows)

    try:
        extra = next(parquet_batches)
        extra_rows = extra.num_rows
    except StopIteration:
        extra_rows = 0

    row_count_ok = raw_rows == parquet_rows == EXPECTED_ROWS[month] and extra_rows == 0
    data_exact = mismatch_cells == 0
    lineage_ok = lineage_nulls == 0 and lineage_unexpected == 0
    status = "PASS" if row_count_ok and data_exact and lineage_ok else "REVIEW"

    return {
        "Month": source_month,
        "CandidateFile": candidate_path.name,
        "RawRowsCompared": raw_rows,
        "ParquetRowsCompared": parquet_rows,
        "ExpectedRows": EXPECTED_ROWS[month],
        "ChunksCompared": chunks_checked,
        "ColumnsCompared": len(parquet_columns),
        "CellsCompared": compared_cells,
        "DataMismatchCells": mismatch_cells,
        "LineageNullCells": lineage_nulls,
        "LineageUnexpectedCells": lineage_unexpected,
        "RowCountOK": row_count_ok,
        "DataExactMatch": data_exact,
        "LineageOK": lineage_ok,
        "Status": status,
    }


def main():
    logger = make_logger()
    CANDIDATE_DIR.mkdir(parents=True, exist_ok=True)
    if any(CANDIDATE_DIR.iterdir()):
        raise FileExistsError(
            f"Candidate directory is not empty: {CANDIDATE_DIR}. "
            "Rename/move it before rerunning to avoid overwriting outputs."
        )

    logger.info("STEP 26D - BUILD + FULL RAW/PARQUET VALIDATION")
    logger.info("Candidate output: %s", CANDIDATE_DIR)
    logger.info("Current Staging BTS directory remains untouched.")

    build_results = []
    validation_results = []

    for month in range(1, 13):
        try:
            result = build_candidate(month, logger)
            result.update({"Month": f"{YEAR}-{month:02d}"})
            build_results.append(result)
        except Exception as exc:
            logger.exception("BUILD FAILED: month %02d", month)
            build_results.append({"Month": f"{YEAR}-{month:02d}", "BuildStatus": "FAILED", "Error": repr(exc)})

    for month in range(1, 13):
        candidate = CANDIDATE_DIR / f"stg_bts_{YEAR}_{month:02d}.parquet"
        if not candidate.is_file():
            validation_results.append({"Month": f"{YEAR}-{month:02d}", "Status": "NOT_BUILT"})
            continue
        try:
            result = validate_full_file(month, logger)
            validation_results.append(result)
            logger.info("RESULT %s status=%s data_mismatch=%s lineage_null=%s", result["Month"], result["Status"], result["DataMismatchCells"], result["LineageNullCells"])
        except Exception as exc:
            logger.exception("VALIDATION FAILED: month %02d", month)
            validation_results.append({"Month": f"{YEAR}-{month:02d}", "Status": "FAILED", "Error": repr(exc)})

    build_df = pd.DataFrame(build_results)
    validation_df = pd.DataFrame(validation_results)
    build_csv = CONTROL_DIR / "26d_bts_candidate_build_summary.csv"
    validation_csv = CONTROL_DIR / "26d_bts_candidate_validation_summary.csv"
    summary_txt = CONTROL_DIR / "26d_bts_candidate_summary.txt"

    build_df.to_csv(build_csv, index=False, encoding="utf-8-sig")
    validation_df.to_csv(validation_csv, index=False, encoding="utf-8-sig")

    passed = int(validation_df.get("Status", pd.Series(dtype=str)).eq("PASS").sum())
    review = int(validation_df.get("Status", pd.Series(dtype=str)).eq("REVIEW").sum())
    failed = int(validation_df.get("Status", pd.Series(dtype=str)).eq("FAILED").sum())
    not_built = int(validation_df.get("Status", pd.Series(dtype=str)).eq("NOT_BUILT").sum())

    summary_txt.write_text(
        "STEP 26D - BTS CANDIDATE REBUILD AND FULL VALIDATION\n"
        + "=" * 72 + "\n"
        + f"Run timestamp: {datetime.now():%Y%m%d_%H%M%S}\n"
        + f"Months PASS: {passed}/12\nMonths REVIEW: {review}/12\n"
        + f"Months FAILED: {failed}/12\nMonths NOT_BUILT: {not_built}/12\n\n"
        + "BUILD RESULTS\n" + build_df.to_string(index=False)
        + "\n\nFULL VALIDATION RESULTS\n" + validation_df.to_string(index=False)
        + "\n\nOriginal 03_STAGING_DATA/BTS files were NOT modified.\n"
        + "Candidate files have NOT been promoted/replaced.\n"
        + f"Candidate directory: {CANDIDATE_DIR}\n"
        + f"Build CSV: {build_csv}\nValidation CSV: {validation_csv}\n",
        encoding="utf-8",
    )

    logger.info("Step 26D complete. Summary: %s", summary_txt)
    return 0 if passed == 12 and review == 0 and failed == 0 and not_built == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
