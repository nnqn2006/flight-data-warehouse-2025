from __future__ import annotations

import logging
import shutil
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

# =============================================================================
# STEP 28A - BTS BASE CLEANING (CSV-ONLY)
# =============================================================================
# Purpose:
#   1) Read the 12 RAW BTS 2025 monthly CSV files in chunks.
#   2) Preserve all 109 official BTS source columns and all 7,001,619 rows.
#   3) Verify the trailing Unnamed: 109 column is truly blank before omitting it.
#   4) Create explicit clean companion fields for FlightDate and key HHMM fields.
#   5) Add airport IANA timezone attributes from the approved candidate mapping.
#   6) Add quality flags; do not impute, delete, or silently repair source values.
#   7) Write monthly CLEAN CSVs and control reports.
#
# Important:
#   - This step DOES NOT create UTC event timestamps. C5 has shown that several
#     timestamp candidate methods disagree on a small number of records. UTC
#     timestamp construction will be a separate, governed step after review.
#   - Numeric source columns remain source text in CSV to avoid changing source
#     values. The SQL physical model will apply explicit SQL types later.
#   - Literal text "NA" is preserved. Empty strings remain empty.
#   - RAW, STAGING, and prior audit outputs are read-only.
# =============================================================================

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "01_RAW_DATA" / "BTS" / "csv"
MODEL_DIR = ROOT / "06_DW_DESIGN" / "01_LOGICAL_MODEL"
TIMEZONE_MAP = MODEL_DIR / "27B3B_airport_timezone_mapping_candidates.csv"
CLEAN_DIR = ROOT / "04_CLEAN_DATA" / "BTS"
CONTROL_DIR = ROOT / "04_CLEAN_DATA" / "_CONTROL"
BUILD_DIR = CLEAN_DIR / "_STEP28A_BUILD"

EXPECTED_TOTAL = 7_001_619
EXPECTED_MONTH_ROWS = {
    "01": 539_747,
    "02": 504_884,
    "03": 600_872,
    "04": 583_950,
    "05": 605_648,
    "06": 611_575,
    "07": 631_428,
    "08": 602_378,
    "09": 562_439,
    "10": 605_844,
    "11": 570_550,
    "12": 582_304,
}
CHUNK_SIZE = 100_000
TRAILING_COLUMN = "Unnamed: 109"

HHMM_FIELDS = [
    "CRSDepTime", "DepTime", "CRSArrTime", "ArrTime",
    "WheelsOff", "WheelsOn", "FirstDepTime",
    "Div1WheelsOn", "Div1WheelsOff",
    "Div2WheelsOn", "Div2WheelsOff",
    "Div3WheelsOn", "Div3WheelsOff",
    "Div4WheelsOn", "Div4WheelsOff",
    "Div5WheelsOn", "Div5WheelsOff",
]

OUTPUT_REPORTS = [
    "28A_BTS_cleaning_summary.txt",
    "28A_BTS_cleaning_by_month.csv",
    "28A_BTS_field_quality_profile.csv",
    "28A_BTS_cleaning.log",
]


def setup_logger() -> logging.Logger:
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("step28a_bts_clean")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(CONTROL_DIR / OUTPUT_REPORTS[3], encoding="utf-8")
    sh = logging.StreamHandler(sys.stdout)
    fh.setFormatter(fmt)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def normalize_id(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip().str.replace(r"\.0$", "", regex=True)


def is_blank_only(s: pd.Series) -> pd.Series:
    # Deliberately do NOT treat literal "NA" as blank.
    return s.isna() | s.astype("string").str.strip().eq("")


def normalize_date(s: pd.Series) -> tuple[pd.Series, pd.Series]:
    raw = s.astype("string").str.strip()
    parsed = pd.to_datetime(raw, format="%Y-%m-%d", errors="coerce")
    out = parsed.dt.strftime("%Y-%m-%d").astype("string")
    out = out.fillna("")
    status = pd.Series("VALID", index=s.index, dtype="string")
    status.loc[raw.eq("") | raw.isna()] = "MISSING"
    status.loc[parsed.isna() & ~(raw.eq("") | raw.isna())] = "INVALID"
    return out, status


def normalize_hhmm(s: pd.Series) -> tuple[pd.Series, pd.Series]:
    raw = s.astype("string").str.strip()
    missing = raw.isna() | raw.eq("")
    n_float = pd.to_numeric(raw.mask(missing), errors="coerce")
    integer = n_float.notna() & np.isclose(n_float.fillna(0) % 1, 0)
    n = n_float.fillna(-999999).astype("int64")
    is_2400 = integer & n.eq(2400)
    regular = (
        integer & n.ge(0) & n.le(2359)
        & n.mod(100).lt(60) & n.floordiv(100).le(23)
    )
    valid = regular | is_2400

    clean = raw.copy()
    clean.loc[regular] = n.loc[regular].map(lambda x: f"{int(x):04d}")
    clean.loc[is_2400] = "2400"
    clean.loc[missing] = ""

    status = pd.Series("INVALID_HHMM", index=s.index, dtype="string")
    status.loc[missing] = "MISSING"
    status.loc[regular] = "VALID"
    status.loc[is_2400] = "VALID_2400"
    return clean.astype("string"), status


def validate_binary_flag(s: pd.Series) -> pd.Series:
    raw = s.astype("string").str.strip()
    n = pd.to_numeric(raw.mask(raw.eq("")), errors="coerce")
    status = pd.Series("INVALID", index=s.index, dtype="string")
    status.loc[raw.eq("")] = "MISSING"
    status.loc[n.eq(0)] = "VALID_0"
    status.loc[n.eq(1)] = "VALID_1"
    return status


def main() -> int:
    logger = setup_logger()

    if not RAW_DIR.exists():
        logger.error("RAW BTS directory not found: %s", RAW_DIR)
        return 2
    if not TIMEZONE_MAP.exists():
        logger.error("Timezone mapping not found: %s", TIMEZONE_MAP)
        return 2

    files = sorted(RAW_DIR.glob("BTS_2025_*.csv"))
    expected_names = [f"BTS_2025_{m:02d}.csv" for m in range(1, 13)]
    if [p.name for p in files] != expected_names:
        logger.error("Expected 12 monthly files. Found: %s", [p.name for p in files])
        return 2

    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    if BUILD_DIR.exists():
        logger.error(
            "Build folder already exists; inspect/archive it before rerunning: %s",
            BUILD_DIR,
        )
        return 2

    final_files = [CLEAN_DIR / f"BTS_CLEAN_2025_{m:02d}.csv" for m in range(1, 13)]
    existing_final = [str(p) for p in final_files if p.exists()]
    if existing_final:
        logger.error("CLEAN outputs already exist; no overwrite permitted:\n%s", "\n".join(existing_final))
        return 2

    mapping = pd.read_csv(TIMEZONE_MAP, dtype="string", keep_default_na=False)
    required = {"BTSAirportID", "TimeZoneIANA"}
    if not required.issubset(mapping.columns):
        logger.error("Timezone mapping missing columns: %s", sorted(required - set(mapping.columns)))
        return 2

    mapping["BTSAirportID"] = normalize_id(mapping["BTSAirportID"])
    mapping["TimeZoneIANA"] = mapping["TimeZoneIANA"].astype("string").str.strip()
    if mapping["BTSAirportID"].duplicated().any() or mapping["TimeZoneIANA"].eq("").any():
        logger.error("Timezone mapping has duplicate IDs or blank timezone values.")
        return 2

    tz_map = dict(zip(mapping["BTSAirportID"].astype(str), mapping["TimeZoneIANA"].astype(str)))

    total_raw_bytes = sum(p.stat().st_size for p in files)
    free_bytes = shutil.disk_usage(ROOT).free
    estimated_need = int(total_raw_bytes * 1.15)
    logger.info("RAW files=12; mapping rows=%d; chunk=%d", len(mapping), CHUNK_SIZE)
    logger.info("RAW BTS size=%.2f GB; free disk=%.2f GB", total_raw_bytes / 1e9, free_bytes / 1e9)
    if free_bytes < estimated_need:
        logger.warning(
            "Free disk is below 115%% of RAW BTS size. CLEAN CSV output may run out of space."
        )

    BUILD_DIR.mkdir(parents=True, exist_ok=False)

    monthly_rows = Counter()
    quality_counts = Counter()
    summary_months = []
    source_schema = None
    official_columns = None
    total_rows = 0
    total_trailing_nonblank = 0

    logger.info("STEP 28A - BTS base cleaning started.")
    logger.info("RAW is read-only. All 109 official source columns will be retained.")

    try:
        for file_idx, path in enumerate(files, start=1):
            month = path.stem[-2:]
            logger.info("Processing %s (%d/12)", path.name, file_idx)
            temp_output = BUILD_DIR / f"BTS_CLEAN_2025_{month}.csv"
            file_rows = 0
            source_row_start = 1
            wrote_header = False

            reader = pd.read_csv(
                path,
                dtype="string",
                keep_default_na=False,
                chunksize=CHUNK_SIZE,
                low_memory=False,
            )

            for chunk in reader:
                chunk = chunk.reset_index(drop=True)
                if chunk.empty:
                    continue

                if source_schema is None:
                    source_schema = list(chunk.columns)
                    if len(source_schema) != 110:
                        raise RuntimeError(
                            f"Expected 110 source positions in first file; got {len(source_schema)}"
                        )
                    if TRAILING_COLUMN not in source_schema:
                        raise RuntimeError(
                            f"Expected trailing {TRAILING_COLUMN!r} column was not found."
                        )
                    official_columns = [c for c in source_schema if c != TRAILING_COLUMN]
                    if len(official_columns) != 109:
                        raise RuntimeError("Expected exactly 109 official BTS columns.")
                elif list(chunk.columns) != source_schema:
                    raise RuntimeError(f"Source schema changed in {path.name}.")

                # Verify the trailing unnamed field is empty in EVERY source row.
                trailing_nonblank = ~is_blank_only(chunk[TRAILING_COLUMN])
                n_trailing = int(trailing_nonblank.sum())
                total_trailing_nonblank += n_trailing
                if n_trailing:
                    examples = chunk.loc[trailing_nonblank, TRAILING_COLUMN].head(5).tolist()
                    raise RuntimeError(
                        f"{TRAILING_COLUMN} contains nonblank values in {path.name}; examples={examples}"
                    )

                n = len(chunk)
                row_numbers = np.arange(source_row_start, source_row_start + n, dtype=np.int64)
                source_row_start += n
                file_rows += n
                total_rows += n
                monthly_rows[month] += n

                # Source values are retained as text. Clean fields are additive.
                clean = chunk[official_columns].copy()

                flight_date_clean, flight_date_status = normalize_date(chunk["FlightDate"])
                clean["FlightDate_Clean"] = flight_date_clean
                clean["DQ_FlightDate_Status"] = flight_date_status
                quality_counts[(month, "FlightDate", "VALID")] += int(flight_date_status.eq("VALID").sum())
                quality_counts[(month, "FlightDate", "MISSING")]+= int(flight_date_status.eq("MISSING").sum())
                quality_counts[(month, "FlightDate", "INVALID")] += int(flight_date_status.eq("INVALID").sum())

                for field in HHMM_FIELDS:
                    if field not in chunk.columns:
                        continue
                    clean_value, field_status = normalize_hhmm(chunk[field])
                    clean[f"{field}_Clean"] = clean_value
                    clean[f"DQ_{field}_Status"] = field_status
                    for st, count in field_status.value_counts(dropna=False).items():
                        quality_counts[(month, field, str(st))] += int(count)

                origin_id = normalize_id(chunk["OriginAirportID"])
                dest_id = normalize_id(chunk["DestAirportID"])
                origin_tz = origin_id.map(tz_map).astype("string").fillna("")
                dest_tz = dest_id.map(tz_map).astype("string").fillna("")

                clean["OriginTimeZoneIANA"] = origin_tz
                clean["DestinationTimeZoneIANA"] = dest_tz
                clean["DQ_OriginAirportTimezone_Status"] = np.where(
                    origin_tz.eq(""), "UNMAPPED", "MAPPED"
                )
                clean["DQ_DestinationAirportTimezone_Status"] = np.where(
                    dest_tz.eq(""), "UNMAPPED", "MAPPED"
                )
                quality_counts[(month, "OriginAirportID", "MAPPED")] += int(origin_tz.ne("").sum())
                quality_counts[(month, "OriginAirportID", "UNMAPPED")] += int(origin_tz.eq("").sum())
                quality_counts[(month, "DestAirportID", "MAPPED")] += int(dest_tz.ne("").sum())
                quality_counts[(month, "DestAirportID", "UNMAPPED")] += int(dest_tz.eq("").sum())

                clean["DQ_Cancelled_Status"] = validate_binary_flag(chunk["Cancelled"])
                clean["DQ_Diverted_Status"] = validate_binary_flag(chunk["Diverted"])
                for field in ["Cancelled", "Diverted"]:
                    for st, count in clean[f"DQ_{field}_Status"].value_counts(dropna=False).items():
                        quality_counts[(month, field, str(st))] += int(count)

                clean.insert(0, "SourceFile", path.name)
                clean.insert(1, "SourceMonth", month)
                clean.insert(2, "SourceRowNumber", row_numbers)

                clean.to_csv(
                    temp_output,
                    mode="a",
                    index=False,
                    header=not wrote_header,
                    encoding="utf-8-sig" if not wrote_header else "utf-8",
                )
                wrote_header = True

            expected = EXPECTED_MONTH_ROWS[month]
            if file_rows != expected:
                raise RuntimeError(
                    f"Row count mismatch in {path.name}: expected={expected:,}, actual={file_rows:,}"
                )

            summary_months.append({
                "SourceMonth": month,
                "SourceFile": path.name,
                "RowsRead": file_rows,
                "ExpectedRows": expected,
                "RowsMatch": file_rows == expected,
                "RawSizeMB": round(path.stat().st_size / 1e6, 2),
                "CleanCandidateSizeMB": round(temp_output.stat().st_size / 1e6, 2),
            })

            logger.info("Completed %s; rows=%s", path.name, f"{file_rows:,}")

        if total_rows != EXPECTED_TOTAL:
            raise RuntimeError(
                f"Total row count mismatch: expected={EXPECTED_TOTAL:,}, actual={total_rows:,}"
            )

        if total_trailing_nonblank != 0:
            raise RuntimeError("Trailing unnamed column validation failed.")

        # Promote all 12 monthly files only after every source month passes.
        for month in EXPECTED_MONTH_ROWS:
            src = BUILD_DIR / f"BTS_CLEAN_2025_{month}.csv"
            dst = CLEAN_DIR / src.name
            if dst.exists():
                raise RuntimeError(f"Refusing to overwrite existing CLEAN output: {dst}")
            src.replace(dst)

        BUILD_DIR.rmdir()

        month_df = pd.DataFrame(summary_months).sort_values("SourceMonth")
        month_df.to_csv(CONTROL_DIR / OUTPUT_REPORTS[1], index=False, encoding="utf-8-sig")

        quality_df = pd.DataFrame([
            {
                "SourceMonth": m,
                "FieldName": field,
                "QualityStatus": status,
                "Rows": int(count),
            }
            for (m, field, status), count in quality_counts.items()
        ]).sort_values(["SourceMonth", "FieldName", "QualityStatus"])
        quality_df.to_csv(CONTROL_DIR / OUTPUT_REPORTS[2], index=False, encoding="utf-8-sig")

        clean_size = sum(p.stat().st_size for p in final_files)
        lines = [
            "STEP 28A - BTS BASE CLEANING SUMMARY",
            "=" * 78,
            f"RAW monthly files read: {len(files)}",
            f"Rows read: {total_rows:,}",
            f"Expected rows: {EXPECTED_TOTAL:,}",
            f"Official source columns retained: {len(official_columns or [])}",
            f"Trailing unnamed column nonblank count: {total_trailing_nonblank:,}",
            f"Timezone mapping rows: {len(mapping):,}",
            f"CLEAN monthly CSV files written: {len(final_files)}",
            f"CLEAN total size: {clean_size / 1e9:.3f} GB",
            "",
            "TRANSFORMATIONS:",
            "- Original 109 named BTS source columns are retained.",
            "- FlightDate_Clean added in ISO YYYY-MM-DD format.",
            "- Companion *_Clean HHMM fields added; valid times padded to HHMM; 2400 retained as 2400.",
            "- OriginTimeZoneIANA and DestinationTimeZoneIANA added using BTS AirportID mapping.",
            "- DQ status fields added for date, key HHMM fields, timezone mapping, Cancelled and Diverted.",
            "- SourceFile, SourceMonth and SourceRowNumber added for lineage.",
            "- The trailing Unnamed: 109 column was omitted only after verifying it was blank in every row.",
            "- Literal NA text was preserved; no NULL imputation, no outlier removal, no flight-row deletion.",
            "- No UTC event timestamp was created in Step 28A; that is a separate controlled step.",
            "- RAW and STAGING were not modified.",
            "",
            "MONTHLY OUTPUTS:",
        ]
        for p in final_files:
            lines.append(f"{p} ({p.stat().st_size / 1e6:.2f} MB)")
        lines += [
            "",
            "CONTROL REPORTS:",
            str(CONTROL_DIR / OUTPUT_REPORTS[1]),
            str(CONTROL_DIR / OUTPUT_REPORTS[2]),
            str(CONTROL_DIR / OUTPUT_REPORTS[3]),
        ]
        (CONTROL_DIR / OUTPUT_REPORTS[0]).write_text("\n".join(lines) + "\n", encoding="utf-8")

        logger.info("STEP 28A completed. rows=%s; CLEAN size=%.3f GB", f"{total_rows:,}", clean_size / 1e9)
        logger.info("Summary: %s", CONTROL_DIR / OUTPUT_REPORTS[0])
        return 0

    except Exception as exc:
        logger.exception("STEP 28A FAILED: %s", exc)
        logger.error(
            "Any partial build files are preserved at %s. Inspect them before rerunning.",
            BUILD_DIR,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
