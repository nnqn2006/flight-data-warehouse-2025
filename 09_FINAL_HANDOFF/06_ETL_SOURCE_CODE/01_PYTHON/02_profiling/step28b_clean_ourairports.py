from __future__ import annotations

import csv
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

# ============================================================
# STEP 28B - OUR AIRPORTS CLEAN + VALIDATION
# Source of truth: 01_RAW_DATA/OurAirports/airports.csv
# Comparison/reference source: airports_reference.csv (not modified)
#
# Policies:
#   - RAW is read-only.
#   - Preserve all 86,335 source rows and the exact 19-column schema.
#   - Keep literal "NA" tokens; do not interpret them as missing values.
#   - Normalize whitespace and code casing; cast numeric columns safely.
#   - Do not deduplicate local_code (it is not a unique key).
#   - Do not modify BTS mapping or historical crosswalk outputs.
#   - Never overwrite an existing CLEAN output or temporary output.
#   - Promote TEMP -> CLEAN only if every validation check passes.
# ============================================================

ROOT = Path(r"D:\DATA_WAREHOUSE")
SOURCE_FILE = ROOT / "01_RAW_DATA" / "OurAirports" / "airports.csv"

CLEAN_DIR = ROOT / "04_CLEAN_DATA" / "OurAirports"
CONTROL_DIR = ROOT / "04_CLEAN_DATA" / "_CONTROL"

OUTPUT_FILE = CLEAN_DIR / "airports_clean.csv"
TEMP_FILE = CLEAN_DIR / "airports_clean.tmp.csv"
SUMMARY_FILE = CONTROL_DIR / "28B_OurAirports_clean_validation_summary.txt"
MISSING_FILE = CONTROL_DIR / "28B_OurAirports_clean_missing_profile.csv"
LOG_FILE = CONTROL_DIR / "28B_OurAirports_clean.log"

CHUNK_SIZE = 100_000
EXPECTED_ROWS = 86_335
EXPECTED_COLUMNS = 19

# Exact schema confirmed by the existing 01_ourairports_schema.csv profiling output.
EXPECTED_SCHEMA = [
    "id",
    "ident",
    "type",
    "name",
    "latitude_deg",
    "longitude_deg",
    "elevation_ft",
    "continent",
    "iso_country",
    "iso_region",
    "municipality",
    "scheduled_service",
    "icao_code",
    "iata_code",
    "gps_code",
    "local_code",
    "home_link",
    "wikipedia_link",
    "keywords",
]

CODE_COLUMNS = [
    "ident",
    "iso_country",
    "iso_region",
    "icao_code",
    "iata_code",
    "gps_code",
    "local_code",
]
LOWER_COLUMNS = ["type", "scheduled_service"]
NUMERIC_COLUMNS = ["id", "latitude_deg", "longitude_deg", "elevation_ft"]
REQUIRED_NONBLANK = ["id", "ident", "name", "latitude_deg", "longitude_deg"]

CLEAN_DIR.mkdir(parents=True, exist_ok=True)
CONTROL_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8", mode="w"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("STEP28B_OURAIRPORTS_CLEAN")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def detect_encoding(path: Path) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin1"):
        try:
            with path.open("r", encoding=encoding, newline="") as stream:
                stream.read(100_000)
            return encoding
        except UnicodeDecodeError:
            continue
    raise UnicodeError(f"Cannot decode source file: {path}")


def detect_delimiter(path: Path, encoding: str) -> str:
    with path.open("r", encoding=encoding, newline="") as stream:
        sample = stream.read(50_000)
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
    except csv.Error:
        return ","


def normalize_chunk(chunk: pd.DataFrame, counters: dict[str, Any]) -> pd.DataFrame:
    # Trim whitespace in all text fields while preserving empty values as empty strings.
    for col in chunk.columns:
        chunk[col] = chunk[col].astype("string").str.strip().fillna("")

    # Normalize airport/code fields only. Do not alter proper-name fields.
    for col in CODE_COLUMNS:
        chunk[col] = chunk[col].str.upper()

    for col in LOWER_COLUMNS:
        chunk[col] = chunk[col].str.lower()

    # Convert numeric fields; count nonblank values that cannot be parsed.
    for col in NUMERIC_COLUMNS:
        raw = chunk[col].astype("string").str.strip()
        nonblank = raw.ne("")
        parsed = pd.to_numeric(raw.mask(~nonblank), errors="coerce")
        invalid = nonblank & parsed.isna()
        counters["invalid_numeric"][col] += int(invalid.sum())

        if col == "id":
            fractional = parsed.notna() & ((parsed % 1) != 0)
            counters["fractional_id"] += int(fractional.sum())
            chunk[col] = parsed.astype("Int64")
        else:
            chunk[col] = parsed.astype("Float64")

    return chunk


def inspect_temp_output(path: Path) -> dict[str, Any]:
    """Read TEMP output back to verify on-disk schema, row count and literal NA tokens."""
    header = pd.read_csv(
        path,
        dtype="string",
        encoding="utf-8-sig",
        keep_default_na=False,
        na_filter=False,
        nrows=0,
    )
    output_columns = list(header.columns)

    rows = 0
    na_continent = 0
    na_iso_country = 0

    for chunk in pd.read_csv(
        path,
        dtype="string",
        encoding="utf-8-sig",
        keep_default_na=False,
        na_filter=False,
        chunksize=CHUNK_SIZE,
        low_memory=False,
    ):
        rows += len(chunk)
        na_continent += int(chunk["continent"].eq("NA").sum())
        na_iso_country += int(chunk["iso_country"].eq("NA").sum())

    return {
        "columns": output_columns,
        "rows": rows,
        "na_continent": na_continent,
        "na_iso_country": na_iso_country,
    }


def main() -> int:
    logger.info("=" * 72)
    logger.info("STEP 28B - OUR AIRPORTS CLEAN + VALIDATION")
    logger.info("Started: %s", datetime.now().isoformat(timespec="seconds"))
    logger.info("Source: %s", SOURCE_FILE)
    logger.info("Chunk size: %s", CHUNK_SIZE)

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"RAW source not found: {SOURCE_FILE}")

    # Protect existing results. Inspect them before any manual rerun/recovery.
    if OUTPUT_FILE.exists():
        raise FileExistsError(
            f"CLEAN output already exists: {OUTPUT_FILE}\n"
            "No overwrite was performed. Validate or archive the existing file first."
        )
    if TEMP_FILE.exists():
        raise FileExistsError(
            f"Temporary output already exists: {TEMP_FILE}\n"
            "No overwrite was performed. Inspect the TEMP file before rerunning."
        )

    encoding = detect_encoding(SOURCE_FILE)
    delimiter = detect_delimiter(SOURCE_FILE, encoding)
    source_hash_before = sha256_file(SOURCE_FILE)

    logger.info("Encoding: %s", encoding)
    logger.info("Delimiter: %r", delimiter)
    logger.info("Source size: %s bytes", SOURCE_FILE.stat().st_size)
    logger.info("Source SHA256 before: %s", source_hash_before)

    header = pd.read_csv(
        SOURCE_FILE,
        sep=delimiter,
        encoding=encoding,
        dtype="string",
        keep_default_na=False,
        na_filter=False,
        nrows=0,
    )
    source_columns = list(header.columns)

    if source_columns != EXPECTED_SCHEMA:
        raise ValueError(
            "Source schema/order differs from the confirmed profiling schema.\n"
            f"Expected: {EXPECTED_SCHEMA}\n"
            f"Actual:   {source_columns}\n"
            "No CLEAN file was promoted."
        )

    if len(source_columns) != EXPECTED_COLUMNS:
        raise ValueError(
            f"Unexpected source column count: {len(source_columns)}; "
            f"expected {EXPECTED_COLUMNS}."
        )

    counters: dict[str, Any] = {
        "rows_read": 0,
        "blank_counts": {col: 0 for col in source_columns},
        "invalid_numeric": {col: 0 for col in NUMERIC_COLUMNS},
        "fractional_id": 0,
        "required_blank": {col: 0 for col in REQUIRED_NONBLANK},
        "duplicate_ids": 0,
        "duplicate_idents": 0,
        "invalid_latitude": 0,
        "invalid_longitude": 0,
        "na_continent_values": 0,
        "na_iso_country_values": 0,
    }
    seen_ids: set[str] = set()
    seen_idents: set[str] = set()
    first_chunk = True

    reader = pd.read_csv(
        SOURCE_FILE,
        sep=delimiter,
        encoding=encoding,
        dtype="string",
        keep_default_na=False,
        na_filter=False,
        chunksize=CHUNK_SIZE,
        low_memory=False,
    )

    try:
        for chunk_number, chunk in enumerate(reader, start=1):
            if list(chunk.columns) != source_columns:
                raise ValueError("Column order/schema changed while reading source.")

            chunk = normalize_chunk(chunk, counters)
            counters["rows_read"] += len(chunk)

            for col in source_columns:
                if col in NUMERIC_COLUMNS:
                    counters["blank_counts"][col] += int(chunk[col].isna().sum())
                else:
                    counters["blank_counts"][col] += int(chunk[col].astype("string").eq("").sum())

            for col in REQUIRED_NONBLANK:
                if col in NUMERIC_COLUMNS:
                    counters["required_blank"][col] += int(chunk[col].isna().sum())
                else:
                    counters["required_blank"][col] += int(chunk[col].astype("string").eq("").sum())

            ids = chunk["id"].dropna().astype("string")
            for value in ids:
                if value in seen_ids:
                    counters["duplicate_ids"] += 1
                else:
                    seen_ids.add(value)

            idents = chunk["ident"].astype("string")
            for value in idents[idents.ne("")]:
                if value in seen_idents:
                    counters["duplicate_idents"] += 1
                else:
                    seen_idents.add(value)

            lat = chunk["latitude_deg"]
            lon = chunk["longitude_deg"]
            counters["invalid_latitude"] += int(
                (lat.notna() & ~lat.between(-90, 90)).sum()
            )
            counters["invalid_longitude"] += int(
                (lon.notna() & ~lon.between(-180, 180)).sum()
            )

            counters["na_continent_values"] += int(chunk["continent"].eq("NA").sum())
            counters["na_iso_country_values"] += int(chunk["iso_country"].eq("NA").sum())

            chunk.to_csv(
                TEMP_FILE,
                mode="w" if first_chunk else "a",
                header=first_chunk,
                index=False,
                encoding="utf-8-sig" if first_chunk else "utf-8",
            )
            first_chunk = False

            logger.info(
                "Chunk %s complete | rows=%s | total=%s",
                chunk_number,
                f"{len(chunk):,}",
                f"{counters['rows_read']:,}",
            )

    except Exception:
        logger.exception("CLEAN failed. Temporary output was not promoted.")
        raise

    if not TEMP_FILE.exists():
        raise RuntimeError("TEMP output was not created; no CLEAN file was promoted.")

    # Verify the actual bytes read from RAW have not changed during this run.
    source_hash_after = sha256_file(SOURCE_FILE)

    # Verify the generated TEMP CSV after writing it to disk.
    temp_check = inspect_temp_output(TEMP_FILE)

    total = counters["rows_read"]
    missing_rows = []
    for col in source_columns:
        missing = counters["blank_counts"][col]
        missing_rows.append({
            "Column": col,
            "TotalRows": total,
            "BlankCount": missing,
            "NonBlankCount": total - missing,
            "BlankPercent": round(missing / total * 100, 4) if total else 0,
        })
    pd.DataFrame(missing_rows).to_csv(
        MISSING_FILE, index=False, encoding="utf-8-sig"
    )

    validation_checks = {
        "row_count_matches_profile": total == EXPECTED_ROWS,
        "column_count_matches_profile": len(source_columns) == EXPECTED_COLUMNS,
        "exact_schema_and_order_matches_profile": source_columns == EXPECTED_SCHEMA,
        "id_unique": counters["duplicate_ids"] == 0 and len(seen_ids) == total,
        "ident_unique": counters["duplicate_idents"] == 0 and len(seen_idents) == total,
        "required_fields_nonblank": sum(counters["required_blank"].values()) == 0,
        "numeric_values_valid": sum(counters["invalid_numeric"].values()) == 0,
        "id_values_integer": counters["fractional_id"] == 0,
        "latitude_in_range": counters["invalid_latitude"] == 0,
        "longitude_in_range": counters["invalid_longitude"] == 0,
        "raw_sha256_unchanged": source_hash_before == source_hash_after,
        "temp_row_count_matches_source": temp_check["rows"] == total,
        "temp_schema_matches_source": temp_check["columns"] == source_columns,
        "literal_na_tokens_preserved": (
            temp_check["na_continent"] == counters["na_continent_values"]
            and temp_check["na_iso_country"] == counters["na_iso_country_values"]
        ),
    }
    passed = all(validation_checks.values())

    lines = [
        "STEP 28B - OUR AIRPORTS CLEAN VALIDATION SUMMARY",
        "=" * 72,
        f"Run timestamp: {datetime.now().isoformat(timespec='seconds')}",
        f"Source file: {SOURCE_FILE}",
        f"Source SHA256 before: {source_hash_before}",
        f"Source SHA256 after:  {source_hash_after}",
        f"Rows read: {total:,}",
        f"Expected rows (from profiling): {EXPECTED_ROWS:,}",
        f"Columns read: {len(source_columns)}",
        f"Expected columns: {EXPECTED_COLUMNS}",
        f"TEMP rows read back: {temp_check['rows']:,}",
        "",
        "VALIDATION CHECKS:",
    ]
    for name, result in validation_checks.items():
        lines.append(f"{name}: {'PASS' if result else 'FAIL'}")

    lines += [
        "",
        "KEY COUNTS:",
        f"Unique IDs observed: {len(seen_ids):,}",
        f"Duplicate ID rows: {counters['duplicate_ids']:,}",
        f"Unique idents observed: {len(seen_idents):,}",
        f"Duplicate ident rows: {counters['duplicate_idents']:,}",
        "",
        "INVALID VALUE COUNTS:",
    ]
    for col, count in counters["invalid_numeric"].items():
        lines.append(f"Invalid numeric {col}: {count:,}")
    lines += [
        f"Fractional IDs: {counters['fractional_id']:,}",
        f"Invalid latitude rows: {counters['invalid_latitude']:,}",
        f"Invalid longitude rows: {counters['invalid_longitude']:,}",
        "",
        "REQUIRED FIELD BLANK COUNTS:",
    ]
    for col, count in counters["required_blank"].items():
        lines.append(f"{col}: {count:,}")
    lines += [
        "",
        "LITERAL 'NA' TOKEN PRESERVATION:",
        f"continent == NA in source pass: {counters['na_continent_values']:,}",
        f"continent == NA in TEMP: {temp_check['na_continent']:,}",
        f"iso_country == NA in source pass: {counters['na_iso_country_values']:,}",
        f"iso_country == NA in TEMP: {temp_check['na_iso_country']:,}",
        "",
        "POLICY:",
        "- RAW source was read only; before/after SHA256 hashes were compared.",
        "- All source rows and the exact 19-column order were retained.",
        "- Literal NA was preserved; keep_default_na=False and na_filter=False.",
        "- local_code was not treated as a unique key and was not deduplicated.",
        "- Historical IATA alias PBI remains in the separate crosswalk, not in current iata_code.",
        "- Existing CLEAN output was not overwritten.",
        "",
        f"FINAL RESULT: {'PASS' if passed else 'FAIL'}",
    ]
    SUMMARY_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")

    if passed:
        TEMP_FILE.replace(OUTPUT_FILE)
        logger.info("VALIDATION PASS. CLEAN file promoted: %s", OUTPUT_FILE)
    else:
        logger.error("VALIDATION FAIL. TEMP file retained for inspection: %s", TEMP_FILE)

    logger.info("Summary: %s", SUMMARY_FILE)
    logger.info("Missing profile: %s", MISSING_FILE)
    logger.info("Final result: %s", "PASS" if passed else "FAIL")
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
