
from __future__ import annotations

import logging
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


# ============================================================
# STEP 27B-3B2 - FLIGHT-LEVEL WEATHER AIRPORT COVERAGE AUDIT
# Read-only: does not modify RAW or STAGING.
# Checks airport-file availability, NOT exact hourly matches.
# ============================================================

ROOT = Path(r"D:\DATA_WAREHOUSE")
BTS_DIR = ROOT / "03_STAGING_DATA" / "BTS"

MAPPING_FILE = (
    ROOT / "06_DW_DESIGN" / "01_LOGICAL_MODEL"
    / "27B3B_airport_timezone_mapping_candidates.csv"
)

OUT_DIR = ROOT / "06_DW_DESIGN" / "01_LOGICAL_MODEL"

BATCH_SIZE = 100_000
EXPECTED_BTS_ROWS = 7_001_619
EXPECTED_AIRPORT_CODES = 352
EXPECTED_WEATHER_AIRPORTS = 240

MAX_SAMPLES_PER_CATEGORY = 20

SUMMARY_FILE = (
    OUT_DIR / "27B3B2_flight_weather_coverage_summary.txt"
)

MONTH_FILE = (
    OUT_DIR / "27B3B2_flight_weather_coverage_by_month.csv"
)

STATUS_FILE = (
    OUT_DIR / "27B3B2_flight_weather_coverage_by_status.csv"
)

SAMPLE_FILE = (
    OUT_DIR / "27B3B2_flight_weather_coverage_samples.csv"
)

AIRPORT_FILE = (
    OUT_DIR / "27B3B2_airport_weather_scope_profile.csv"
)

LOG_FILE = (
    OUT_DIR / "27B3B2_flight_weather_coverage.log"
)


BTS_COLUMNS = [
    "FlightDate",
    "Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "Dest",
    "Cancelled",
    "Diverted",
    "SourceFile",
    "SourceMonth",
]


CATEGORIES = [
    "BOTH_ORIGIN_AND_DEST_HAVE_WEATHER_FILE",
    "ORIGIN_ONLY_HAS_WEATHER_FILE",
    "DEST_ONLY_HAS_WEATHER_FILE",
    "NEITHER_ENDPOINT_HAS_WEATHER_FILE",
    "UNMAPPED_OR_MISSING_AIRPORT_CODE",
]


# ============================================================
# 1. LOGGER
# ============================================================

def setup_logger() -> logging.Logger:

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("step27b3b2")
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


# ============================================================
# 2. PREVENT OVERWRITING OLD OUTPUTS
# ============================================================

def check_outputs_do_not_exist() -> None:

    outputs = [
        SUMMARY_FILE,
        MONTH_FILE,
        STATUS_FILE,
        SAMPLE_FILE,
        AIRPORT_FILE,
        LOG_FILE,
    ]

    existing = [
        path for path in outputs if path.exists()
    ]

    if existing:

        raise FileExistsError(
            "Output Step 27B-3B2 đã tồn tại. "
            "Hãy archive hoặc đổi tên trước khi chạy lại:\n"
            + "\n".join(str(p) for p in existing)
        )


# ============================================================
# 3. NORMALIZE AIRPORT CODE FOR COMPARISON
# ============================================================

def normalized_code(series: pd.Series) -> pd.Series:

    values = (
        series.astype("string")
        .str.strip()
        .str.upper()
    )

    return values.mask(
        values.isna()
        | values.eq("")
        | values.isin(["NA", "NAN", "NONE", "NULL"])
    )


def safe_rate(numerator: int, denominator: int) -> float:

    return (
        numerator / denominator
        if denominator
        else 0.0
    )


# ============================================================
# 4. MAIN
# ============================================================

def main() -> int:

    check_outputs_do_not_exist()

    logger = setup_logger()

    # --------------------------------------------------------
    # 4.1. Load approved-candidate airport mapping
    # --------------------------------------------------------

    if not MAPPING_FILE.exists():

        raise FileNotFoundError(MAPPING_FILE)

    mapping = pd.read_csv(
        MAPPING_FILE,
        dtype="string",
        keep_default_na=False
    )

    required_mapping_columns = [
        "BTSAirportCode",
        "HasOpenMeteo2025File",
        "TimeZoneIANA",
        "BTSAirportID",
        "OurAirportsID",
        "BTSOriginFlightOccurrences",
        "BTSDestinationFlightOccurrences",
    ]

    missing_mapping = [
        col
        for col in required_mapping_columns
        if col not in mapping.columns
    ]

    if missing_mapping:

        raise ValueError(
            f"Mapping file thiếu cột: {missing_mapping}"
        )

    mapping["BTSAirportCode"] = normalized_code(
        mapping["BTSAirportCode"]
    )

    if mapping["BTSAirportCode"].isna().any():

        raise ValueError(
            "Mapping chứa BTSAirportCode trống/NA."
        )

    if mapping["BTSAirportCode"].duplicated().any():

        duplicates = mapping.loc[
            mapping["BTSAirportCode"].duplicated(keep=False),
            "BTSAirportCode"
        ].tolist()

        raise ValueError(
            f"BTSAirportCode không unique: {duplicates}"
        )

    mapping_codes = set(
        mapping["BTSAirportCode"].astype(str)
    )

    weather_flag = (
        mapping["HasOpenMeteo2025File"]
        .astype("string")
        .str.strip()
        .str.lower()
    )

    invalid_flags = ~weather_flag.isin(
        ["true", "false"]
    )

    if invalid_flags.any():

        raise ValueError(
            "HasOpenMeteo2025File phải chỉ gồm True/False."
        )

    weather_codes = set(
        mapping.loc[
            weather_flag.eq("true"),
            "BTSAirportCode"
        ].astype(str)
    )

    # --------------------------------------------------------
    # 4.2. Weighted airport-endpoint occurrence coverage
    # --------------------------------------------------------

    occurrence_columns = [
        "BTSOriginFlightOccurrences",
        "BTSDestinationFlightOccurrences",
    ]

    for col in occurrence_columns:

        mapping[col] = pd.to_numeric(
            mapping[col],
            errors="coerce"
        ).fillna(0)

    total_origin_occurrences = int(
        mapping["BTSOriginFlightOccurrences"].sum()
    )

    total_dest_occurrences = int(
        mapping["BTSDestinationFlightOccurrences"].sum()
    )

    covered_origin_occurrences = int(
        mapping.loc[
            weather_flag.eq("true"),
            "BTSOriginFlightOccurrences"
        ].sum()
    )

    covered_dest_occurrences = int(
        mapping.loc[
            weather_flag.eq("true"),
            "BTSDestinationFlightOccurrences"
        ].sum()
    )

    if len(mapping_codes) != EXPECTED_AIRPORT_CODES:

        logger.warning(
            "Airport code count differs from baseline %d: %d",
            EXPECTED_AIRPORT_CODES,
            len(mapping_codes)
        )

    if len(weather_codes) != EXPECTED_WEATHER_AIRPORTS:

        logger.warning(
            "Weather airport count differs from baseline %d: %d",
            EXPECTED_WEATHER_AIRPORTS,
            len(weather_codes)
        )

    # --------------------------------------------------------
    # 4.3. Find BTS files
    # --------------------------------------------------------

    files = sorted(
        BTS_DIR.glob("stg_bts_2025_*.parquet")
    )

    if len(files) != 12:

        raise FileNotFoundError(
            f"Expected 12 BTS Parquet files; "
            f"found {len(files)} in {BTS_DIR}"
        )

    total_rows = 0

    category_counts = Counter()
    month_counts = Counter()
    status_counts = Counter()

    sample_rows = defaultdict(list)
    unknown_code_counts = Counter()

    logger.info(
        "STEP 27B-3B2 - FLIGHT-LEVEL WEATHER AIRPORT COVERAGE AUDIT"
    )

    logger.info(
        "BTS airport codes=%d; weather airport codes=%d",
        len(mapping_codes),
        len(weather_codes)
    )

    # ========================================================
    # 5. SCAN BTS FILES IN BATCHES
    # ========================================================

    for file_no, path in enumerate(files, start=1):

        parquet_file = pq.ParquetFile(path)

        missing_columns = [
            col
            for col in BTS_COLUMNS
            if col not in parquet_file.schema.names
        ]

        if missing_columns:

            raise ValueError(
                f"Missing columns in {path.name}: "
                f"{missing_columns}"
            )

        file_rows = 0

        logger.info(
            "Scanning %s (%d/12)",
            path.name,
            file_no
        )

        for batch in parquet_file.iter_batches(
            batch_size=BATCH_SIZE,
            columns=BTS_COLUMNS
        ):

            frame = batch.to_pandas()

            n = len(frame)

            if not n:
                continue

            file_rows += n
            total_rows += n

            origin = normalized_code(frame["Origin"])
            dest = normalized_code(frame["Dest"])

            # Does the airport exist in the 352-airport mapping?
            origin_known = origin.isin(mapping_codes)
            dest_known = dest.isin(mapping_codes)

            unknown = (
                (~origin_known)
                | (~dest_known)
            )

            # Does the airport have an Open-Meteo 2025 file?
            origin_weather = origin.isin(weather_codes)
            dest_weather = dest.isin(weather_codes)

            # ------------------------------------------------
            # 5.1. Classify weather file coverage
            # ------------------------------------------------

            category = pd.Series(
                "NEITHER_ENDPOINT_HAS_WEATHER_FILE",
                index=frame.index,
                dtype="string"
            )

            category.loc[
                origin_weather & ~dest_weather
            ] = "ORIGIN_ONLY_HAS_WEATHER_FILE"

            category.loc[
                ~origin_weather & dest_weather
            ] = "DEST_ONLY_HAS_WEATHER_FILE"

            category.loc[
                origin_weather & dest_weather
            ] = "BOTH_ORIGIN_AND_DEST_HAVE_WEATHER_FILE"

            category.loc[
                unknown
            ] = "UNMAPPED_OR_MISSING_AIRPORT_CODE"

            for cat, count in category.value_counts(
                dropna=False
            ).items():

                category_counts[str(cat)] += int(count)

            # ------------------------------------------------
            # 5.2. Coverage by month
            # ------------------------------------------------

            month_values = (
                frame["SourceMonth"]
                .astype("string")
                .fillna("<NULL>")
            )

            month_frame = pd.DataFrame({
                "SourceMonth": month_values,
                "CoverageCategory": category,
            })

            for key, count in month_frame.value_counts(
                dropna=False
            ).items():

                month_counts[
                    (str(key[0]), str(key[1]))
                ] += int(count)

            # ------------------------------------------------
            # 5.3. Coverage by Cancelled / Diverted status
            # ------------------------------------------------

            status_frame = pd.DataFrame({

                "CancelledRaw":
                    frame["Cancelled"].astype("string").fillna("<NULL>"),

                "DivertedRaw":
                    frame["Diverted"].astype("string").fillna("<NULL>"),

                "CoverageCategory":
                    category,
            })

            for key, count in status_frame.value_counts(
                dropna=False
            ).items():

                status_counts[
                    (str(key[0]), str(key[1]), str(key[2]))
                ] += int(count)

            # ------------------------------------------------
            # 5.4. Capture sample rows for each category
            # ------------------------------------------------

            for cat in CATEGORIES:

                remaining = (
                    MAX_SAMPLES_PER_CATEGORY
                    - len(sample_rows[cat])
                )

                if remaining <= 0:
                    continue

                selected_indices = (
                    category.index[category.eq(cat)]
                    [:remaining]
                )

                for idx in selected_indices:

                    sample_rows[cat].append({

                        "CoverageCategory": cat,

                        "FlightDate":
                            frame.at[idx, "FlightDate"],

                        "Reporting_Airline":
                            frame.at[idx, "Reporting_Airline"],

                        "Flight_Number_Reporting_Airline":
                            frame.at[
                                idx,
                                "Flight_Number_Reporting_Airline"
                            ],

                        "Origin": frame.at[idx, "Origin"],

                        "Dest": frame.at[idx, "Dest"],

                        "Cancelled": frame.at[idx, "Cancelled"],

                        "Diverted": frame.at[idx, "Diverted"],

                        "SourceFile": frame.at[idx, "SourceFile"],

                        "SourceMonth": frame.at[idx, "SourceMonth"],

                        "StagingFile": path.name,
                    })

            # ------------------------------------------------
            # 5.5. Count airport codes not in mapping
            # ------------------------------------------------

            if unknown.any():

                for code, count in (
                    origin[~origin_known].value_counts().items()
                ):

                    unknown_code_counts[
                        ("Origin", str(code))
                    ] += int(count)

                for code, count in (
                    dest[~dest_known].value_counts().items()
                ):

                    unknown_code_counts[
                        ("Dest", str(code))
                    ] += int(count)

        logger.info(
            "Completed %s; rows=%s",
            path.name,
            f"{file_rows:,}"
        )

    # ========================================================
    # 6. VALIDATE TOTAL ROWS
    # ========================================================

    if total_rows != EXPECTED_BTS_ROWS:

        raise ValueError(
            f"BTS row count mismatch: "
            f"expected {EXPECTED_BTS_ROWS:,}, "
            f"got {total_rows:,}"
        )

    # ========================================================
    # 7. EXPORT MONTHLY COVERAGE
    # ========================================================

    month_rows = [

        {
            "SourceMonth": month,
            "CoverageCategory": cat,
            "Rows": count,
        }

        for (month, cat), count
        in sorted(month_counts.items())
    ]

    pd.DataFrame(month_rows).to_csv(
        MONTH_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    # ========================================================
    # 8. EXPORT COVERAGE BY FLIGHT STATUS
    # ========================================================

    status_rows = [

        {
            "CancelledRaw": cancelled,
            "DivertedRaw": diverted,
            "CoverageCategory": cat,
            "Rows": count,
        }

        for (cancelled, diverted, cat), count
        in sorted(status_counts.items())
    ]

    pd.DataFrame(status_rows).to_csv(
        STATUS_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    # ========================================================
    # 9. EXPORT SAMPLE RECORDS
    # ========================================================

    sample_export = [

        row

        for cat in CATEGORIES

        for row in sample_rows[cat]
    ]

    pd.DataFrame(sample_export).to_csv(
        SAMPLE_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    # ========================================================
    # 10. EXPORT AIRPORT WEATHER SCOPE PROFILE
    # ========================================================

    airport_profile = mapping.copy()

    airport_profile["WeatherFileCoverage"] = (
        weather_flag.eq("true").map({
            True: "COVERED",
            False: "NOT_COVERED"
        })
    )

    airport_profile.to_csv(
        AIRPORT_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    # ========================================================
    # 11. BUILD SUMMARY
    # ========================================================

    both = category_counts[
        "BOTH_ORIGIN_AND_DEST_HAVE_WEATHER_FILE"
    ]

    origin_only = category_counts[
        "ORIGIN_ONLY_HAS_WEATHER_FILE"
    ]

    dest_only = category_counts[
        "DEST_ONLY_HAS_WEATHER_FILE"
    ]

    neither = category_counts[
        "NEITHER_ENDPOINT_HAS_WEATHER_FILE"
    ]

    unmapped = category_counts[
        "UNMAPPED_OR_MISSING_AIRPORT_CODE"
    ]

    summary = [

        "STEP 27B-3B2 - FLIGHT-LEVEL WEATHER AIRPORT COVERAGE SUMMARY",

        "=" * 78,

        f"Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",

        "",

        "SOURCE COUNTS",

        f"BTS files scanned: {len(files)}",

        f"BTS flight records scanned: {total_rows:,}",

        f"BTS airport codes in timezone mapping: {len(mapping_codes):,}",

        f"Airport codes with Open-Meteo 2025 files: {len(weather_codes):,}",

        f"Distinct-airport coverage: "
        f"{safe_rate(len(weather_codes), len(mapping_codes))*100:.4f}%",

        "",

        "OCCURRENCE-WEIGHTED AIRPORT ENDPOINT COVERAGE",

        f"Origin endpoint occurrences with weather file: "
        f"{covered_origin_occurrences:,} / {total_origin_occurrences:,} "
        f"({safe_rate(covered_origin_occurrences, total_origin_occurrences)*100:.4f}%)",

        f"Destination endpoint occurrences with weather file: "
        f"{covered_dest_occurrences:,} / {total_dest_occurrences:,} "
        f"({safe_rate(covered_dest_occurrences, total_dest_occurrences)*100:.4f}%)",

        f"Combined endpoint occurrences with weather file: "
        f"{covered_origin_occurrences + covered_dest_occurrences:,} / "
        f"{total_origin_occurrences + total_dest_occurrences:,} "
        f"({safe_rate(covered_origin_occurrences + covered_dest_occurrences, total_origin_occurrences + total_dest_occurrences)*100:.4f}%)",

        "",

        "FLIGHT-LEVEL AIRPORT WEATHER-FILE COVERAGE",

        f"Both origin and destination have weather files: "
        f"{both:,} ({safe_rate(both, total_rows)*100:.4f}%)",

        f"Origin only has a weather file: "
        f"{origin_only:,} ({safe_rate(origin_only, total_rows)*100:.4f}%)",

        f"Destination only has a weather file: "
        f"{dest_only:,} ({safe_rate(dest_only, total_rows)*100:.4f}%)",

        f"Neither endpoint has a weather file: "
        f"{neither:,} ({safe_rate(neither, total_rows)*100:.4f}%)",

        f"Unmapped/missing airport code at either endpoint: "
        f"{unmapped:,} ({safe_rate(unmapped, total_rows)*100:.4f}%)",

        "",

        "INTERPRETATION",

        "- This audit measures the presence of an Open-Meteo airport file at origin/destination.",

        "- It does NOT confirm a valid event timestamp or an exact airport-hour weather match.",

        "- A row with both endpoint files can still lack actual departure/arrival timestamps.",

        "- Cancelled and diverted records are retained and profiled separately by status.",

        "- No source values were imputed, deleted, or modified.",

        "- RAW and STAGING were read-only.",

        "",

        "OUTPUT FILES",

        str(MONTH_FILE),

        str(STATUS_FILE),

        str(SAMPLE_FILE),

        str(AIRPORT_FILE),

        str(LOG_FILE),
    ]

    SUMMARY_FILE.write_text(
        "\n".join(summary) + "\n",
        encoding="utf-8"
    )

    # ========================================================
    # 12. LOG COMPLETION
    # ========================================================

    logger.info(
        "Completed. rows=%s; both endpoints=%s; origin-only=%s; "
        "dest-only=%s; neither=%s; unmapped=%s",

        f"{total_rows:,}",
        f"{both:,}",
        f"{origin_only:,}",
        f"{dest_only:,}",
        f"{neither:,}",
        f"{unmapped:,}"
    )

    logger.info(
        "Summary: %s",
        SUMMARY_FILE
    )

    return 0


# ============================================================
# 13. ENTRY POINT
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
