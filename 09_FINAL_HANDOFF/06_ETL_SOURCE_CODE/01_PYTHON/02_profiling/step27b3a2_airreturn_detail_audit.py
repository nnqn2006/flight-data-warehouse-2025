
from __future__ import annotations

import logging
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


# ============================================================
# STEP 27B-3A2 - TARGETED AIR-RETURN DETAIL AUDIT
# Read-only audit. Does not modify RAW or STAGING.
# ============================================================

ROOT = Path(r"D:\DATA_WAREHOUSE")
BTS_DIR = ROOT / "03_STAGING_DATA" / "BTS"
OUT_DIR = ROOT / "06_DW_DESIGN" / "01_LOGICAL_MODEL"

BATCH_SIZE = 100_000
EXPECTED_TOTAL_ROWS = 7_001_619
EXPECTED_DIVERTED_ZERO_WITH_DIV1 = 710

SUMMARY_FILE = OUT_DIR / "27B3A2_airreturn_detail_summary.txt"
PROFILE_FILE = OUT_DIR / "27B3A2_airreturn_flag_profile.csv"
RECORDS_FILE = OUT_DIR / "27B3A2_diverted0_with_div1_records.csv"
LOG_FILE = OUT_DIR / "27B3A2_airreturn_detail_audit.log"

# These are original BTS fields plus staging lineage fields.
COLUMNS = [
    "FlightDate",
    "Reporting_Airline",
    "DOT_ID_Reporting_Airline",
    "Tail_Number",
    "Flight_Number_Reporting_Airline",
    "OriginAirportID",
    "Origin",
    "DestAirportID",
    "Dest",
    "CRSDepTime",
    "DepTime",
    "WheelsOff",
    "WheelsOn",
    "ArrTime",
    "Cancelled",
    "CancellationCode",
    "Diverted",
    "DivAirportLandings",
    "DivReachedDest",
    "DivActualElapsedTime",
    "DivArrDelay",
    "DivDistance",
    "Div1Airport",
    "Div1AirportID",
    "Div1AirportSeqID",
    "Div1WheelsOn",
    "Div1TotalGTime",
    "Div1LongestGTime",
    "Div1WheelsOff",
    "Div1TailNum",
    "Div2Airport",
    "Div2AirportID",
    "SourceFile",
    "SourceMonth",
]


def setup_logger() -> logging.Logger:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("step27b3a2")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler = logging.FileHandler(
        LOG_FILE,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def norm(series: pd.Series) -> pd.Series:
    """Normalize only for comparisons; original values remain in output."""
    return series.fillna("").astype("string").str.strip()


def category(series: pd.Series) -> pd.Series:
    """Retain literal values while making blank/NA groups explicit."""
    s = norm(series)

    s = s.mask(s.eq(""), "<BLANK>")
    s = s.mask(s.str.upper().eq("NA"), "<LITERAL_NA>")

    return s


def truth(series: pd.Series, expected: str = "1.00") -> pd.Series:
    return norm(series).eq(expected)


def main() -> int:

    # --------------------------------------------------------
    # 1. Check output files to prevent accidental overwrite
    # --------------------------------------------------------

    outputs = [
        SUMMARY_FILE,
        PROFILE_FILE,
        RECORDS_FILE,
        LOG_FILE
    ]

    existing = [p for p in outputs if p.exists()]

    if existing:
        raise FileExistsError(
            "Output already exists; archive or rename these files before rerunning:\n"
            + "\n".join(str(p) for p in existing)
        )

    logger = setup_logger()

    # --------------------------------------------------------
    # 2. Discover BTS Parquet files
    # --------------------------------------------------------

    files = sorted(
        BTS_DIR.glob("stg_bts_2025_*.parquet")
    )

    if len(files) != 12:
        raise FileNotFoundError(
            f"Expected 12 BTS Parquet files, "
            f"found {len(files)} in {BTS_DIR}"
        )

    total_rows = 0
    target_rows = 0

    profile_counts: Counter[tuple[str, ...]] = Counter()

    record_parts: list[pd.DataFrame] = []

    logger.info(
        "STEP 27B-3A2 - TARGETED AIR-RETURN DETAIL AUDIT"
    )

    logger.info(
        "BTS files found: %d",
        len(files)
    )

    # --------------------------------------------------------
    # 3. Scan each monthly BTS file
    # --------------------------------------------------------

    for file_idx, path in enumerate(files, start=1):

        pf = pq.ParquetFile(path)

        missing = [
            c for c in COLUMNS
            if c not in pf.schema.names
        ]

        if missing:
            raise ValueError(
                f"Missing columns in {path.name}: {missing}"
            )

        file_rows = 0

        logger.info(
            "Scanning %s (%d/%d)",
            path.name,
            file_idx,
            len(files)
        )

        # Read in batches to control memory consumption.
        for batch in pf.iter_batches(
            batch_size=BATCH_SIZE,
            columns=COLUMNS
        ):

            df = batch.to_pandas()

            n = len(df)

            if n == 0:
                continue

            file_rows += n
            total_rows += n

            # Starting row position of this batch in the file.
            row_start = file_rows - n

            # ------------------------------------------------
            # 3.1. Normalize key status fields for comparison
            # ------------------------------------------------

            cancelled = norm(df["Cancelled"])
            diverted = norm(df["Diverted"])

            div1_airport = norm(df["Div1Airport"])

            has_div1 = (
                div1_airport.ne("")
                & div1_airport.str.upper().ne("NA")
            )

            # Population under review:
            # Diverted = 0 AND Div1Airport populated.
            mask = diverted.eq("0.00") & has_div1

            count = int(mask.sum())

            if count:

                selected = df.loc[mask].copy()

                selected["StagingFile"] = path.name

                selected["ParquetRowNumber1Based"] = [
                    row_start + int(i) + 1
                    for i in selected.index
                ]

                selected["CSVLineNumberIncludingHeader"] = [
                    row_start + int(i) + 2
                    for i in selected.index
                ]

                # --------------------------------------------
                # 3.2. Diagnostic comparisons
                # --------------------------------------------

                selected["Div1AirportEqualsOrigin"] = (
                    norm(selected["Div1Airport"]).str.upper()
                    == norm(selected["Origin"]).str.upper()
                )

                selected["Div1AirportEqualsDestination"] = (
                    norm(selected["Div1Airport"]).str.upper()
                    == norm(selected["Dest"]).str.upper()
                )

                selected["DivAirportLandingsCategory"] = category(
                    selected["DivAirportLandings"]
                )

                selected["DivReachedDestCategory"] = category(
                    selected["DivReachedDest"]
                )

                selected["CancelledCategory"] = category(
                    selected["Cancelled"]
                )

                selected["CancellationCodeCategory"] = category(
                    selected["CancellationCode"]
                )

                record_parts.append(selected)

                target_rows += count

            # ------------------------------------------------
            # 3.3. Build detailed flag profile
            # ------------------------------------------------

            for idx in df.index:

                if not bool(mask.loc[idx]):
                    continue

                row = df.loc[idx]

                key = (

                    str(
                        category(
                            pd.Series([row["Cancelled"]])
                        ).iloc[0]
                    ),

                    str(
                        category(
                            pd.Series([row["Diverted"]])
                        ).iloc[0]
                    ),

                    str(
                        category(
                            pd.Series([row["DivAirportLandings"]])
                        ).iloc[0]
                    ),

                    str(
                        category(
                            pd.Series([row["DivReachedDest"]])
                        ).iloc[0]
                    ),

                    "Y" if str(row["Div1Airport"]).strip() else "N",

                    "Y" if (
                        str(row["Div1Airport"]).strip().upper()
                        == str(row["Origin"]).strip().upper()
                    ) else "N",

                    "Y" if (
                        str(row["Div1WheelsOff"]).strip()
                        not in ("", "NA")
                    ) else "N",

                    "Y" if (
                        str(row["Div1TailNum"]).strip()
                        not in ("", "NA")
                    ) else "N",
                )

                profile_counts[key] += 1

        logger.info(
            "Completed %s; rows=%s",
            path.name,
            file_rows
        )

    # --------------------------------------------------------
    # 4. Validate total records
    # --------------------------------------------------------

    if total_rows != EXPECTED_TOTAL_ROWS:

        raise ValueError(
            f"BTS row total mismatch: "
            f"expected {EXPECTED_TOTAL_ROWS:,}, "
            f"got {total_rows:,}"
        )

    records = (
        pd.concat(record_parts, ignore_index=True)
        if record_parts
        else pd.DataFrame(columns=COLUMNS)
    )

    if target_rows != EXPECTED_DIVERTED_ZERO_WITH_DIV1:

        logger.warning(
            "Target population differs from Step 27B-3A baseline: "
            "expected %s, got %s",
            EXPECTED_DIVERTED_ZERO_WITH_DIV1,
            target_rows
        )

    # --------------------------------------------------------
    # 5. Export profile CSV
    # --------------------------------------------------------

    profile_columns = [
        "CancelledRaw",
        "DivertedRaw",
        "DivAirportLandingsRaw",
        "DivReachedDestRaw",
        "HasDiv1Airport",
        "Div1AirportEqualsOrigin",
        "HasDiv1WheelsOff",
        "HasDiv1TailNum",
        "Rows",
    ]

    profile_rows = [
        list(key) + [count]
        for key, count in sorted(profile_counts.items())
    ]

    pd.DataFrame(
        profile_rows,
        columns=profile_columns
    ).to_csv(
        PROFILE_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    # --------------------------------------------------------
    # 6. Export all 710 target records
    # --------------------------------------------------------

    records.to_csv(
        RECORDS_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    # --------------------------------------------------------
    # 7. Summarize group counts
    # --------------------------------------------------------

    if not records.empty:

        by_flag = (
            records.groupby(
                [
                    "CancelledCategory",
                    "Diverted",
                    "DivAirportLandingsCategory",
                    "DivReachedDestCategory",
                ],
                dropna=False
            )
            .size()
            .reset_index(name="Rows")
        )

    else:
        by_flag = pd.DataFrame()

    # --------------------------------------------------------
    # 8. Build summary report
    # --------------------------------------------------------

    summary_lines = [

        "STEP 27B-3A2 - TARGETED AIR-RETURN DETAIL AUDIT",

        "=" * 76,

        f"Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",

        f"BTS files scanned: {len(files)}",

        f"Total BTS rows scanned: {total_rows:,}",

        f"Rows with Diverted=0 and Div1Airport populated: {target_rows:,}",

        "",

        "Purpose: inspect DivAirportLandings and DivReachedDest "
        "alongside flight-level fields.",

        "The airport-equals-origin flag is diagnostic only; "
        "it is not an automatic classification.",

        "No source data are modified and no missing values are imputed.",

        "",

        "FLAG PROFILE",

        profile_file_label(PROFILE_FILE),

        "",

        "RECORD DETAIL EXPORT",

        str(RECORDS_FILE),

        "",

        "GROUP COUNTS",
    ]

    if not by_flag.empty:

        summary_lines.extend(
            by_flag.to_string(index=False).splitlines()
        )

    else:

        summary_lines.append(
            "No records matched the target filter."
        )

    summary_lines.extend([
        "",
        "OUTPUT FILES",
        str(PROFILE_FILE),
        str(RECORDS_FILE),
        str(LOG_FILE)
    ])

    SUMMARY_FILE.write_text(
        "\n".join(summary_lines) + "\n",
        encoding="utf-8"
    )

    logger.info(
        "Completed. Total rows=%s; target rows=%s",
        f"{total_rows:,}",
        f"{target_rows:,}"
    )

    logger.info(
        "Summary: %s",
        SUMMARY_FILE
    )

    return 0


def profile_file_label(path: Path) -> str:
    return str(path)


# ============================================================
# 9. ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:
        raise SystemExit(main())

    except Exception as exc:

        print(
            f"ERROR: {exc}",
            file=sys.stderr
        )

        raise
