
from __future__ import annotations

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# =============================================================================
# STEP 28A2 - BTS RAW-TO-CLEAN VALIDATION
# CSV-only | Read-only validation | No PyArrow
# =============================================================================

ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = ROOT / "01_RAW_DATA" / "BTS" / "csv"
CLEAN_DIR = ROOT / "04_CLEAN_DATA" / "BTS"
CONTROL_DIR = ROOT / "04_CLEAN_DATA" / "_CONTROL"

MAP_FILE = (
    ROOT / "06_DW_DESIGN" / "01_LOGICAL_MODEL"
    / "27B3B_airport_timezone_mapping_candidates.csv"
)

EXPECTED_ROWS = {
    "01": 539747, "02": 504884, "03": 600872,
    "04": 583950, "05": 605648, "06": 611575,
    "07": 631428, "08": 602378, "09": 562439,
    "10": 605844, "11": 570550, "12": 582304,
}

EXPECTED_TOTAL = sum(EXPECTED_ROWS.values())
CHUNK_SIZE = 100000
SAMPLE_LIMIT = 30

TIME_FIELDS = [
    "CRSDepTime", "DepTime", "WheelsOff", "WheelsOn",
    "CRSArrTime", "ArrTime", "FirstDepTime",
    "Div1WheelsOff", "Div1WheelsOn",
    "Div2WheelsOff", "Div2WheelsOn",
    "Div3WheelsOff", "Div3WheelsOn",
    "Div4WheelsOff", "Div4WheelsOn",
    "Div5WheelsOff", "Div5WheelsOn",
]

OUT_MONTH = CONTROL_DIR / "28A2_BTS_source_to_clean_monthly.csv"
OUT_SAMPLE = CONTROL_DIR / "28A2_BTS_source_to_clean_mismatch_samples.csv"
OUT_SUMMARY = CONTROL_DIR / "28A2_BTS_source_to_clean_validation_summary.txt"
OUT_LOG = CONTROL_DIR / "28A2_BTS_source_to_clean_validation.log"

OUTPUTS = [OUT_MONTH, OUT_SAMPLE, OUT_SUMMARY, OUT_LOG]


def norm_id(s):
    return (
        s.astype("string")
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )


def blank(s):
    x = s.astype("string").str.strip()
    return x.isna() | x.eq("")


def expected_hhmm(raw):
    """Expected zero-padded HHMM, preserving 2400."""
    s = raw.astype("string").str.strip()
    missing = blank(s)

    numeric = pd.to_numeric(s.mask(missing), errors="coerce")
    integer = (
        numeric.notna()
        & np.isclose(numeric.fillna(0) % 1, 0)
    )

    n = numeric.fillna(-999999).astype("int64")

    is2400 = integer & n.eq(2400)

    regular = (
        integer
        & n.ge(0)
        & n.le(2359)
        & n.mod(100).lt(60)
        & n.floordiv(100).le(23)
    )

    valid = regular | is2400

    result = pd.Series("", index=raw.index, dtype="string")

    result.loc[valid] = (
        n.loc[valid].map(lambda x: f"{x:04d}").astype("string")
    )

    return result, valid


def setup_logger():
    logger = logging.getLogger("step28a2")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    for handler in [
        logging.FileHandler(OUT_LOG, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]:
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def main():

    CONTROL_DIR.mkdir(parents=True, exist_ok=True)

    existing = [str(p) for p in OUTPUTS if p.exists()]

    if existing:
        print(
            "STOP - Step 28A2 outputs already exist. "
            "Archive/rename before rerun:\n"
            + "\n".join(existing)
        )
        return 2

    for path in [RAW_DIR, CLEAN_DIR, MAP_FILE]:
        if not path.exists():
            print(f"STOP - Missing path: {path}")
            return 2

    logger = setup_logger()

    logger.info("STEP 28A2 - BTS RAW-to-CLEAN validation")
    logger.info("Chunk size=%s", CHUNK_SIZE)

    # Load airport timezone mapping.
    mapping = pd.read_csv(
        MAP_FILE,
        dtype="string",
        keep_default_na=False,
    )

    required_mapping = {"BTSAirportID", "TimeZoneIANA"}

    if not required_mapping.issubset(mapping.columns):
        logger.error("Timezone mapping schema is invalid.")
        return 2

    mapping["BTSAirportID"] = norm_id(mapping["BTSAirportID"])
    mapping["TimeZoneIANA"] = (
        mapping["TimeZoneIANA"].astype("string").str.strip()
    )

    if mapping["BTSAirportID"].duplicated().any():
        logger.error("Duplicate BTSAirportID in mapping.")
        return 2

    timezone_map = dict(
        zip(mapping["BTSAirportID"], mapping["TimeZoneIANA"])
    )

    month_results = []
    mismatch_samples = []

    totals = {
        "rows": 0,
        "source": 0,
        "trailing": 0,
        "lineage": 0,
        "date": 0,
        "timezone": 0,
        "hhmm": 0,
        "extra_clean_rows": 0,
    }

    for month, expected_count in EXPECTED_ROWS.items():

        raw_path = RAW_DIR / f"BTS_2025_{month}.csv"
        clean_path = CLEAN_DIR / f"BTS_CLEAN_2025_{month}.csv"

        logger.info("Checking month %s", month)

        if not raw_path.exists() or not clean_path.exists():
            logger.error("Missing RAW/CLEAN file for month %s", month)
            month_results.append({
                "SourceMonth": month,
                "Status": "MISSING_FILE",
            })
            continue

        raw_header = pd.read_csv(
            raw_path, nrows=0, dtype="string",
            keep_default_na=False
        ).columns.tolist()

        clean_header = pd.read_csv(
            clean_path, nrows=0, dtype="string",
            keep_default_na=False
        ).columns.tolist()

        if len(raw_header) != 110 or raw_header[-1] != "Unnamed: 109":
            logger.error("Unexpected RAW schema for month %s", month)
            month_results.append({
                "SourceMonth": month,
                "Status": "RAW_SCHEMA_UNEXPECTED",
            })
            continue

        official_cols = raw_header[:-1]

        required_extra = [
            "FlightDate_Clean",
            "OriginTimeZoneIANA",
            "DestinationTimeZoneIANA",
            "SourceFile",
            "SourceMonth",
            "SourceRowNumber",
        ]

        missing_source = [
            c for c in official_cols if c not in clean_header
        ]

        missing_extra = [
            c for c in required_extra if c not in clean_header
        ]

        if missing_source or missing_extra:
            logger.error(
                "Missing columns for %s. Source=%s Derived=%s",
                month, missing_source, missing_extra
            )
            month_results.append({
                "SourceMonth": month,
                "Status": "CLEAN_SCHEMA_MISSING_COLUMNS",
                "MissingSourceColumns": ";".join(missing_source),
                "MissingDerivedColumns": ";".join(missing_extra),
            })
            continue

        raw_reader = pd.read_csv(
            raw_path,
            usecols=official_cols + ["Unnamed: 109"],
            dtype="string",
            keep_default_na=False,
            chunksize=CHUNK_SIZE,
            low_memory=False,
        )

        companion_fields = [
            f"{field}_Clean"
            for field in TIME_FIELDS
            if f"{field}_Clean" in clean_header
        ]

        clean_usecols = (
            official_cols
            + required_extra
            + companion_fields
        )

        clean_reader = pd.read_csv(
            clean_path,
            usecols=clean_usecols,
            dtype="string",
            keep_default_na=False,
            chunksize=CHUNK_SIZE,
            low_memory=False,
        )

        month_count = {
            "rows": 0,
            "source": 0,
            "trailing": 0,
            "lineage": 0,
            "date": 0,
            "timezone": 0,
            "hhmm": 0,
            "extra_clean_rows": 0,
        }

        while True:

            try:
                raw_chunk = next(raw_reader)
            except StopIteration:
                # Drain any extra CLEAN rows rather than silently ignoring them.
                for extra in clean_reader:
                    month_count["extra_clean_rows"] += len(extra)
                break

            try:
                clean_chunk = next(clean_reader)
            except StopIteration:
                month_count["lineage"] += len(raw_chunk)
                logger.error("CLEAN ended before RAW for month %s", month)
                break

            n = len(raw_chunk)
            month_count["rows"] += n
            start_row = month_count["rows"] - n + 1

            # 1. Exact source-to-clean comparison for 109 official columns.
            raw_values = raw_chunk[official_cols].astype("string")
            clean_values = clean_chunk[official_cols].astype("string")

            equal = raw_values.eq(clean_values).fillna(False)
            mismatch = ~equal

            mismatch_count = int(mismatch.to_numpy().sum())
            month_count["source"] += mismatch_count

            if mismatch_count and len(mismatch_samples) < SAMPLE_LIMIT:
                positions = np.argwhere(mismatch.to_numpy())

                for r, c in positions[
                    :SAMPLE_LIMIT - len(mismatch_samples)
                ]:
                    field = official_cols[int(c)]

                    mismatch_samples.append({
                        "SourceMonth": month,
                        "SourceRowNumber": start_row + int(r),
                        "FieldName": field,
                        "RAW_Value": raw_values.iloc[int(r)][field],
                        "CLEAN_Value": clean_values.iloc[int(r)][field],
                    })

            # 2. Verify trailing unnamed source column is empty.
            month_count["trailing"] += int(
                (~blank(raw_chunk["Unnamed: 109"])).sum()
            )

            # 3. Verify source lineage.
            expected_file = f"BTS_2025_{month}.csv"

            source_bad = clean_chunk["SourceFile"].ne(expected_file)

            month_bad = (
                clean_chunk["SourceMonth"]
                .astype("string")
                .str.zfill(2)
                .ne(month)
            )

            expected_rows = pd.Series(
                range(start_row, start_row + n),
                index=clean_chunk.index,
                dtype="int64",
            ).astype("string")

            row_bad = norm_id(
                clean_chunk["SourceRowNumber"]
            ).ne(expected_rows)

            month_count["lineage"] += int(
                source_bad.sum() + month_bad.sum() + row_bad.sum()
            )

            # 4. Verify FlightDate_Clean.
            raw_dates = pd.to_datetime(
                raw_chunk["FlightDate"].astype("string").str.strip(),
                errors="coerce",
            )

            expected_dates = (
                raw_dates.dt.strftime("%Y-%m-%d")
                .fillna("")
                .astype("string")
            )

            date_bad = (
                clean_chunk["FlightDate_Clean"]
                .astype("string")
                .str.strip()
                .ne(expected_dates)
            )

            month_count["date"] += int(date_bad.sum())

            # 5. Verify origin/destination timezone mapping.
            expected_origin = (
                norm_id(clean_chunk["OriginAirportID"])
                .map(timezone_map)
                .fillna("")
                .astype("string")
            )

            expected_dest = (
                norm_id(clean_chunk["DestAirportID"])
                .map(timezone_map)
                .fillna("")
                .astype("string")
            )

            origin_bad = (
                clean_chunk["OriginTimeZoneIANA"]
                .astype("string")
                .ne(expected_origin)
            )

            dest_bad = (
                clean_chunk["DestinationTimeZoneIANA"]
                .astype("string")
                .ne(expected_dest)
            )

            month_count["timezone"] += int(
                origin_bad.sum() + dest_bad.sum()
            )

            # 6. Verify normalized valid HHMM companion fields.
            for field in TIME_FIELDS:

                companion = f"{field}_Clean"

                if companion not in clean_chunk.columns:
                    continue

                expected_clock, valid = expected_hhmm(
                    raw_chunk[field]
                )

                actual_clock = (
                    clean_chunk[companion]
                    .astype("string")
                    .str.strip()
                )

                bad = valid & actual_clock.ne(expected_clock)

                month_count["hhmm"] += int(bad.sum())

        row_match = month_count["rows"] == expected_count

        month_pass = (
            row_match
            and month_count["source"] == 0
            and month_count["trailing"] == 0
            and month_count["lineage"] == 0
            and month_count["date"] == 0
            and month_count["timezone"] == 0
            and month_count["hhmm"] == 0
            and month_count["extra_clean_rows"] == 0
        )

        for key in totals:
            totals[key] += month_count[key]

        result = {
            "SourceMonth": month,
            "RowsRead": month_count["rows"],
            "ExpectedRows": expected_count,
            "RowsMatch": row_match,
            "SourceCellMismatches": month_count["source"],
            "TrailingUnnamedNonblank": month_count["trailing"],
            "LineageErrors": month_count["lineage"],
            "FlightDateCleanErrors": month_count["date"],
            "TimezoneMappingErrors": month_count["timezone"],
            "HHMMCompanionErrors": month_count["hhmm"],
            "ExtraCLEANRows": month_count["extra_clean_rows"],
            "Status": "PASS" if month_pass else "REVIEW",
        }

        month_results.append(result)

        logger.info(
            "Month %s: %s; rows=%s; source mismatches=%s; "
            "lineage=%s; date=%s; timezone=%s; HHMM=%s",
            month,
            result["Status"],
            f"{month_count['rows']:,}",
            month_count["source"],
            month_count["lineage"],
            month_count["date"],
            month_count["timezone"],
            month_count["hhmm"],
        )

    monthly_df = pd.DataFrame(month_results)

    monthly_df.to_csv(
        OUT_MONTH,
        index=False,
        encoding="utf-8-sig",
    )

    pd.DataFrame(mismatch_samples).to_csv(
        OUT_SAMPLE,
        index=False,
        encoding="utf-8-sig",
    )

    passed = (
        len(monthly_df) == 12
        and monthly_df["Status"].eq("PASS").all()
        and totals["rows"] == EXPECTED_TOTAL
        and totals["source"] == 0
        and totals["trailing"] == 0
        and totals["lineage"] == 0
        and totals["date"] == 0
        and totals["timezone"] == 0
        and totals["hhmm"] == 0
        and totals["extra_clean_rows"] == 0
    )

    lines = [
        "STEP 28A2 - BTS RAW-TO-CLEAN VALIDATION SUMMARY",
        "=" * 76,
        f"Months passed: {int(monthly_df['Status'].eq('PASS').sum())}/12",
        f"Rows read: {totals['rows']:,}",
        f"Expected rows: {EXPECTED_TOTAL:,}",
        f"Official source-cell mismatches: {totals['source']:,}",
        f"Trailing Unnamed:109 nonblank values: {totals['trailing']:,}",
        f"Source lineage errors: {totals['lineage']:,}",
        f"FlightDate_Clean errors: {totals['date']:,}",
        f"Timezone mapping errors: {totals['timezone']:,}",
        f"HHMM companion errors: {totals['hhmm']:,}",
        f"Extra CLEAN rows: {totals['extra_clean_rows']:,}",
        f"FINAL RESULT: {'PASS' if passed else 'REVIEW'}",
        "",
        "PASS means:",
        "- 109 official BTS source columns match RAW cell-by-cell.",
        "- All expected monthly row counts match.",
        "- Trailing Unnamed:109 is blank.",
        "- Source lineage fields match their monthly source files.",
        "- FlightDate_Clean and timezone mappings match their source rules.",
        "- Valid HHMM companions are correctly zero-padded; 2400 is preserved.",
        "- No UTC timestamps are approved or created by this validator.",
        "- RAW and CLEAN files were not modified.",
        "",
        "MONTHLY RESULTS:",
        monthly_df.to_string(index=False),
        "",
        "OUTPUT FILES:",
        str(OUT_MONTH),
        str(OUT_SAMPLE),
        str(OUT_SUMMARY),
        str(OUT_LOG),
    ]

    OUT_SUMMARY.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )

    logger.info(
        "STEP 28A2 finished: %s",
        "PASS" if passed else "REVIEW",
    )

    logger.info("Summary: %s", OUT_SUMMARY)

    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
