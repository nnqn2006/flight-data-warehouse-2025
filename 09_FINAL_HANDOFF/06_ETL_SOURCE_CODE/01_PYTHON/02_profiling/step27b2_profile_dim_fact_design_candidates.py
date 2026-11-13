
"""STEP 27B-2 - Profile dimensional keys and design candidates.

Read-only profiling of BTS, OurAirports and Open-Meteo Staging Parquet.
Creates design/decision evidence only; does not modify RAW or STAGING.

Run from D:\\DATA_WAREHOUSE:
python .\\08_SCRIPTS\\02_profiling\\step27b2_profile_dim_fact_design_candidates.py

Dependencies: pandas, pyarrow
"""
from __future__ import annotations

import logging
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
STAGING = ROOT / "03_STAGING_DATA"
BTS_DIR = STAGING / "BTS"
AIRPORT_DIR = STAGING / "OurAirports"
WEATHER_DIR = STAGING / "OpenMeteo"
DESIGN_DIR = ROOT / "06_DW_DESIGN" / "01_LOGICAL_MODEL"

YEAR = 2025

BTS_COLUMNS = [
    "FlightDate", "Reporting_Airline", "DOT_ID_Reporting_Airline",
    "IATA_CODE_Reporting_Airline", "Flight_Number_Reporting_Airline",
    "OriginAirportID", "OriginAirportSeqID", "OriginCityMarketID",
    "Origin", "OriginCityName", "OriginState", "OriginStateFips",
    "OriginStateName", "OriginWac", "DestAirportID", "DestAirportSeqID",
    "DestCityMarketID", "Dest", "DestCityName", "DestState",
    "DestStateFips", "DestStateName", "DestWac", "CRSDepTime",
    "Tail_Number", "Cancelled", "Diverted", "Flights",
    "DivAirportLandings",
]

ORIGIN_ATTRIBUTES = [
    "Origin", "OriginAirportSeqID", "OriginCityMarketID", "OriginCityName",
    "OriginState", "OriginStateFips", "OriginStateName", "OriginWac",
]

DEST_ATTRIBUTES = [
    "Dest", "DestAirportSeqID", "DestCityMarketID", "DestCityName",
    "DestState", "DestStateFips", "DestStateName", "DestWac",
]

AIRLINE_COLUMNS = [
    "DOT_ID_Reporting_Airline", "Reporting_Airline",
    "IATA_CODE_Reporting_Airline",
]

# This is only a candidate natural key for duplicate assessment.
# It is NOT assumed to be the final Flight business key.
FLIGHT_KEY_COLUMNS = [
    "FlightDate", "Reporting_Airline",
    "Flight_Number_Reporting_Airline", "Origin", "Dest", "CRSDepTime",
]

ROUTE_COLUMNS = ["Origin", "Dest"]

DIVERSION_COLUMNS = [
    f"Div{n}{suffix}"
    for n in range(1, 6)
    for suffix in (
        "Airport", "AirportID", "AirportSeqID", "WheelsOn",
        "TotalGTime", "LongestGTime", "WheelsOff", "TailNum",
    )
]

WEATHER_META_COLUMNS = [
    "AirportCode", "time", "Latitude", "Longitude",
    "TimezoneRequested", "WeatherModelRequested",
]

OUTPUT_NAMES = [
    "27B2_airport_attribute_consistency.csv",
    "27B2_airline_identifier_combinations.csv",
    "27B2_candidate_flight_key_duplicates.csv",
    "27B2_route_profile.csv",
    "27B2_diversion_field_coverage.csv",
    "27B2_weather_file_metadata_profile.csv",
    "27B2_design_decision_summary.txt",
]


def make_logger() -> logging.Logger:
    DESIGN_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("step27b2_design_profile")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    for handler in (
        logging.FileHandler(
            DESIGN_DIR / "27B2_design_profile.log",
            encoding="utf-8",
        ),
        logging.StreamHandler(sys.stdout),
    ):
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def is_blank(value) -> bool:
    if value is None or pd.isna(value):
        return True
    return str(value).strip() == ""


def clean_value(value):
    return None if is_blank(value) else str(value).strip()


def require_columns(path: Path, columns: list[str]) -> None:
    names = pq.ParquetFile(path).schema_arrow.names
    missing = sorted(set(columns) - set(names))

    if missing:
        raise ValueError(
            f"{path.name} missing columns: {missing}"
        )


def airport_profile_update(
    store, role, df, id_col, attributes
):
    grouped = df.groupby(
        id_col,
        dropna=True,
        sort=False,
    )

    for airport_id, group in grouped:
        airport_id = clean_value(airport_id)

        if airport_id is None:
            continue

        key = (role, airport_id)

        record = store.setdefault(key, {
            "role": role,
            "airport_id": airport_id,
            "rows": 0,
            "attributes": {
                c: set() for c in attributes
            },
            "blank_counts": {
                c: 0 for c in attributes
            },
        })

        record["rows"] += len(group)

        for col in attributes:
            series = group[col]

            blank_mask = (
                series.isna()
                | series.astype("string")
                    .str.strip().eq("")
                    .fillna(False)
            )

            record["blank_counts"][col] += int(
                blank_mask.sum()
            )

            distinct_values = (
                series[~blank_mask]
                .astype("string")
                .unique()
                .tolist()
            )

            record["attributes"][col].update(
                str(v).strip() for v in distinct_values
            )


def main() -> int:
    logger = make_logger()

    logger.info(
        "STEP 27B-2 - PROFILE DIMENSION / FACT DESIGN CANDIDATES"
    )

    outputs = [
        DESIGN_DIR / name
        for name in OUTPUT_NAMES
    ]

    existing = [
        str(p) for p in outputs if p.exists()
    ]

    if existing:
        raise FileExistsError(
            "Output already exists; archive or rename before rerun: "
            + ", ".join(existing)
        )

    bts_files = sorted(
        BTS_DIR.glob(f"stg_bts_{YEAR}_*.parquet")
    )

    weather_files = sorted(
        WEATHER_DIR.glob(f"weather_*_{YEAR}.parquet")
    )

    airport_file = (
        AIRPORT_DIR / "stg_ourairports.parquet"
    )

    if len(bts_files) != 12:
        raise FileNotFoundError(
            f"Expected 12 BTS files, found {len(bts_files)}"
        )

    if len(weather_files) != 240:
        raise FileNotFoundError(
            f"Expected 240 Open-Meteo files, found {len(weather_files)}"
        )

    if not airport_file.is_file():
        raise FileNotFoundError(airport_file)

    for f in bts_files:
        require_columns(
            f,
            BTS_COLUMNS
            + FLIGHT_KEY_COLUMNS
            + ROUTE_COLUMNS
            + DIVERSION_COLUMNS,
        )

    # Distinct identifiers and attribute sets only.
    # The full BTS dataset is not retained in memory.
    airport_store = {}
    airline_combo_counts = Counter()
    flight_key_duplicate_rows = []
    route_counts = Counter()
    tail_numbers = set()

    total_bts_rows = 0
    total_nonblank_tail = 0

    cancelled_counts = Counter()
    diverted_counts = Counter()
    flights_values = Counter()
    div_field_nonblank = Counter()

    # --------------------------------------------------------
    # 1. BTS profiling
    # --------------------------------------------------------

    for month_index, path in enumerate(bts_files, start=1):
        logger.info(
            "Profiling BTS %s (%s/12)",
            path.name,
            month_index,
        )

        pf = pq.ParquetFile(path)
        month_rows = 0

        needed = sorted(set(
            BTS_COLUMNS
            + FLIGHT_KEY_COLUMNS
            + ROUTE_COLUMNS
            + DIVERSION_COLUMNS
        ))

        for batch in pf.iter_batches(
            batch_size=100_000,
            columns=needed,
        ):
            df = batch.to_pandas()

            n = len(df)
            month_rows += n
            total_bts_rows += n

            # Airport attributes by BTS airport ID.
            airport_profile_update(
                airport_store,
                "Origin",
                df,
                "OriginAirportID",
                ORIGIN_ATTRIBUTES,
            )

            airport_profile_update(
                airport_store,
                "Destination",
                df,
                "DestAirportID",
                DEST_ATTRIBUTES,
            )

            # Airline identifier combinations.
            combo_counts = (
                df.groupby(
                    AIRLINE_COLUMNS,
                    dropna=False,
                ).size()
            )

            for combo, count in combo_counts.items():
                if not isinstance(combo, tuple):
                    combo = (combo,)

                normalized = tuple(
                    clean_value(x) for x in combo
                )

                airline_combo_counts[normalized] += int(count)

            # Route frequency.
            route_group = (
                df.groupby(
                    ROUTE_COLUMNS,
                    dropna=False,
                ).size()
            )

            for route, count in route_group.items():
                route_counts[
                    tuple(clean_value(x) for x in route)
                ] += int(count)

            # Aircraft tail number coverage.
            tail_series = df["Tail_Number"]

            tail_blank = (
                tail_series.isna()
                | tail_series.astype("string")
                    .str.strip().eq("")
                    .fillna(False)
            )

            nonblank_tail = (
                tail_series[~tail_blank]
                .astype("string")
            )

            total_nonblank_tail += int(
                (~tail_blank).sum()
            )

            tail_numbers.update(
                str(v).strip()
                for v in nonblank_tail.unique().tolist()
            )

            # Flags and BTS Flights source field distributions.
            for value, count in (
                df["Cancelled"].value_counts(dropna=False).items()
            ):
                cancelled_counts[clean_value(value)] += int(count)

            for value, count in (
                df["Diverted"].value_counts(dropna=False).items()
            ):
                diverted_counts[clean_value(value)] += int(count)

            for value, count in (
                df["Flights"].value_counts(dropna=False).items()
            ):
                flights_values[clean_value(value)] += int(count)

            # Coverage of the 40 diversion segment fields.
            for col in DIVERSION_COLUMNS:
                div_field_nonblank[col] += int(
                    (~df[col].map(is_blank)).sum()
                )

        # Evaluate the candidate key across the full monthly file.
        # This avoids missing duplicate keys split across 100k batches.
        key_df = pd.read_parquet(
            path,
            columns=FLIGHT_KEY_COLUMNS,
            engine="pyarrow",
        )

        key_counts = (
            key_df.groupby(
                FLIGHT_KEY_COLUMNS,
                dropna=False,
            ).size()
        )

        dupes = key_counts[key_counts > 1]

        if len(dupes):
            for key, count in dupes.items():
                if not isinstance(key, tuple):
                    key = (key,)

                flight_key_duplicate_rows.append({
                    "Month": path.stem.replace(
                        "stg_bts_", ""
                    ).replace("_", "-"),
                    **{
                        f"Key_{c}": clean_value(v)
                        for c, v in zip(
                            FLIGHT_KEY_COLUMNS,
                            key,
                        )
                    },
                    "RowsWithSameCandidateKey": int(count),
                })

        logger.info(
            "BTS %s rows scanned=%s; candidate key duplicate groups=%s",
            path.name,
            month_rows,
            len(dupes),
        )

    # --------------------------------------------------------
    # 2. Airport attribute consistency report
    # --------------------------------------------------------

    airport_rows = []

    for (role, airport_id), record in sorted(
        airport_store.items()
    ):
        attributes = (
            ORIGIN_ATTRIBUTES
            if role == "Origin"
            else DEST_ATTRIBUTES
        )

        for col in attributes:
            vals = sorted(
                record["attributes"][col]
            )

            airport_rows.append({
                "AirportRole": role,
                "BTSAirportID": airport_id,
                "RowsObserved": record["rows"],
                "Attribute": col,
                "DistinctNonblankValues": len(vals),
                "HasConflict": len(vals) > 1,
                "BlankCells": record["blank_counts"][col],
                "ObservedValues": " | ".join(vals[:10]),
            })

    airport_df = pd.DataFrame(airport_rows)

    # --------------------------------------------------------
    # 3. Airline combinations report
    # --------------------------------------------------------

    airline_rows = []

    for combo, count in sorted(
        airline_combo_counts.items(),
        key=lambda x: (-x[1], str(x[0])),
    ):
        airline_rows.append({
            "DOT_ID_Reporting_Airline": combo[0],
            "Reporting_Airline": combo[1],
            "IATA_CODE_Reporting_Airline": combo[2],
            "FlightRows": count,
        })

    airline_df = pd.DataFrame(airline_rows)

    # --------------------------------------------------------
    # 4. Candidate natural flight-key duplicates
    # --------------------------------------------------------

    duplicate_df = pd.DataFrame(
        flight_key_duplicate_rows
    )

    if duplicate_df.empty:
        duplicate_df = pd.DataFrame(columns=[
            "Month",
            *[f"Key_{c}" for c in FLIGHT_KEY_COLUMNS],
            "RowsWithSameCandidateKey",
        ])

    # --------------------------------------------------------
    # 5. Route frequency
    # --------------------------------------------------------

    route_rows = [
        {
            "OriginIATA": k[0],
            "DestinationIATA": k[1],
            "FlightRows": v,
        }
        for k, v in sorted(
            route_counts.items(),
            key=lambda x: (-x[1], str(x[0])),
        )
    ]

    route_df = pd.DataFrame(route_rows)

    # --------------------------------------------------------
    # 6. Diversion field coverage
    # --------------------------------------------------------

    diversion_df = pd.DataFrame([
        {
            "SourceColumn": col,
            "NonblankRows": div_field_nonblank[col],
            "BlankRows": (
                total_bts_rows - div_field_nonblank[col]
            ),
            "NonblankRate": (
                div_field_nonblank[col] / total_bts_rows
                if total_bts_rows else 0
            ),
        }
        for col in DIVERSION_COLUMNS
    ])

    # --------------------------------------------------------
    # 7. Open-Meteo metadata and grain diagnostics
    # --------------------------------------------------------

    weather_rows = []

    for i, path in enumerate(weather_files, start=1):
        logger.info(
            "Profiling Open-Meteo %s (%s/240)",
            path.name,
            i,
        )

        require_columns(
            path,
            WEATHER_META_COLUMNS,
        )

        df = pd.read_parquet(
            path,
            columns=WEATHER_META_COLUMNS,
            engine="pyarrow",
        )

        time_values = pd.to_datetime(
            df["time"],
            errors="coerce",
            utc=True,
        )

        airport_codes = sorted({
            x for x in df["AirportCode"]
                .map(clean_value).tolist()
            if x is not None
        })

        duplicates = int(
            df.duplicated(
                subset=["AirportCode", "time"]
            ).sum()
        )

        meta = {}

        for col in (
            "Latitude",
            "Longitude",
            "TimezoneRequested",
            "WeatherModelRequested",
        ):
            vals = sorted({
                x for x in df[col].map(clean_value).tolist()
                if x is not None
            })

            meta[col + "DistinctCount"] = len(vals)
            meta[col + "Values"] = " | ".join(vals[:5])

        expected_code = path.stem.split("_")[1]

        weather_rows.append({
            "File": path.name,
            "Rows": len(df),
            "ExpectedAirportCodeFromFilename": expected_code,
            "AirportCodeValues": " | ".join(airport_codes),
            "AirportCodeCount": len(airport_codes),
            "AirportCodeMatchesFilename": (
                airport_codes == [expected_code]
            ),
            "TimestampParseFailures": int(
                time_values.isna().sum()
            ),
            "TimestampMinUTC": (
                time_values.min().isoformat()
                if time_values.notna().any() else ""
            ),
            "TimestampMaxUTC": (
                time_values.max().isoformat()
                if time_values.notna().any() else ""
            ),
            "DuplicateAirportHourRows": duplicates,
            **meta,
        })

    weather_df = pd.DataFrame(weather_rows)

    # --------------------------------------------------------
    # 8. OurAirports key profile
    # --------------------------------------------------------

    airport_master = pd.read_parquet(
        AIRPORT_DIR / "stg_ourairports.parquet",
        columns=[
            "id", "ident", "icao_code",
            "iata_code", "gps_code", "local_code",
        ],
        engine="pyarrow",
    )

    def populated_mask(series):
        return (
            series.notna()
            & series.astype("string").str.strip().ne("").fillna(False)
        )

    iata_mask = populated_mask(airport_master["iata_code"])
    icao_mask = populated_mask(airport_master["icao_code"])
    ident_mask = populated_mask(airport_master["ident"])
    local_mask = populated_mask(airport_master["local_code"])

    airport_key_profile = {
        "OurAirportsRows": len(airport_master),
        "OurAirportsIDDistinct": int(
            airport_master["id"].nunique(dropna=True)
        ),
        "OurAirportsIDNull": int(
            airport_master["id"].isna().sum()
        ),
        "IATACodePopulated": int(iata_mask.sum()),
        "IATACodeDistinctPopulated": int(
            airport_master.loc[iata_mask, "iata_code"].nunique()
        ),
        "ICAOCodePopulated": int(icao_mask.sum()),
        "ICAOCodeDistinctPopulated": int(
            airport_master.loc[icao_mask, "icao_code"].nunique()
        ),
        "IdentPopulated": int(ident_mask.sum()),
        "IdentDistinctPopulated": int(
            airport_master.loc[ident_mask, "ident"].nunique()
        ),
        "LocalCodePopulated": int(local_mask.sum()),
        "LocalCodeDistinctPopulated": int(
            airport_master.loc[local_mask, "local_code"].nunique()
        ),
    }

    # --------------------------------------------------------
    # 9. Write outputs
    # --------------------------------------------------------

    airport_df.to_csv(
        DESIGN_DIR / OUTPUT_NAMES[0],
        index=False,
        encoding="utf-8-sig",
    )

    airline_df.to_csv(
        DESIGN_DIR / OUTPUT_NAMES[1],
        index=False,
        encoding="utf-8-sig",
    )

    duplicate_df.to_csv(
        DESIGN_DIR / OUTPUT_NAMES[2],
        index=False,
        encoding="utf-8-sig",
    )

    route_df.to_csv(
        DESIGN_DIR / OUTPUT_NAMES[3],
        index=False,
        encoding="utf-8-sig",
    )

    diversion_df.to_csv(
        DESIGN_DIR / OUTPUT_NAMES[4],
        index=False,
        encoding="utf-8-sig",
    )

    weather_df.to_csv(
        DESIGN_DIR / OUTPUT_NAMES[5],
        index=False,
        encoding="utf-8-sig",
    )

    airport_conflicts = (
        int(airport_df["HasConflict"].sum())
        if not airport_df.empty else 0
    )

    airline_dot_conflicts = 0

    if not airline_df.empty:
        for dot_id, grp in airline_df.groupby(
            "DOT_ID_Reporting_Airline",
            dropna=False,
        ):
            reporting_n = grp["Reporting_Airline"].nunique(
                dropna=False
            )
            iata_n = grp["IATA_CODE_Reporting_Airline"].nunique(
                dropna=False
            )

            if reporting_n > 1 or iata_n > 1:
                airline_dot_conflicts += 1

    weather_bad = int((
        ~weather_df["AirportCodeMatchesFilename"]
        | weather_df["TimestampParseFailures"].ne(0)
        | weather_df["DuplicateAirportHourRows"].ne(0)
    ).sum()) if not weather_df.empty else 0

    summary_lines = [
        "STEP 27B-2 - DIMENSION / FACT DESIGN CANDIDATE PROFILE",
        "=" * 76,
        f"Run timestamp: {datetime.now():%Y%m%d_%H%M%S}",
        "",
        "BTS SUMMARY",
        f"Total BTS rows scanned: {total_bts_rows:,}",
        f"Distinct origin BTS airport IDs: "
        f"{sum(1 for k in airport_store if k[0] == 'Origin'):,}",
        f"Distinct destination BTS airport IDs: "
        f"{sum(1 for k in airport_store if k[0] == 'Destination'):,}",
        f"Airport attribute conflict cells (ID x attribute): "
        f"{airport_conflicts:,}",
        f"Airline identifier combinations: {len(airline_df):,}",
        f"DOT IDs with multiple Reporting/IATA code combinations: "
        f"{airline_dot_conflicts:,}",
        f"Distinct origin-destination routes: {len(route_df):,}",
        f"Candidate flight-key duplicate groups: {len(duplicate_df):,}",
        f"Tail_Number populated rows: "
        f"{total_nonblank_tail:,} / {total_bts_rows:,}",
        f"Distinct populated Tail_Number values: {len(tail_numbers):,}",
        f"Cancelled value distribution: {dict(cancelled_counts)}",
        f"Diverted value distribution: {dict(diverted_counts)}",
        f"Flights source-field value distribution (top 20): "
        f"{flights_values.most_common(20)}",
        "",
        "OURAIRPORTS KEY PROFILE",
        *[
            f"{k}: {v}"
            for k, v in airport_key_profile.items()
        ],
        "",
        "DIVERSION FIELD COVERAGE",
        diversion_df.to_string(index=False),
        "",
        "OPEN-METEO SUMMARY",
        f"Weather files profiled: {len(weather_df)}",
        f"Files with airport-code/timestamp/duplicate issues: {weather_bad}",
        f"Total weather rows: "
        f"{int(weather_df['Rows'].sum()) if not weather_df.empty else 0:,}",
        "",
        "DESIGN INTERPRETATION GUARDRAILS",
        "- Airport attributes should be loaded to Dim_Airport only after checking ID-level consistency.",
        "- Airline codes/IDs should be treated as a governed identifier set, not assumed interchangeable.",
        "- Candidate flight key is diagnostic only; a surrogate FlightKey and stable source-row lineage are still required.",
        "- Dim_Route may be redundant if it only stores Origin and Destination; inspect route counts and analytical need.",
        "- Tail_Number is currently a degenerate attribute candidate; BTS does not supply aircraft model attributes here.",
        "- Diversion segment fields are sparse and should be evaluated as a child segment fact/bridge, not flattened blindly.",
        "- Open-Meteo Latitude/Longitude and requested model/timezone are provenance until their exact semantics are confirmed.",
        "- This script profiles Staging only and does not change RAW, STAGING, or approved mappings.",
        "",
        "OUTPUT FILES",
        *[
            str(DESIGN_DIR / name)
            for name in OUTPUT_NAMES[:-1]
        ],
    ]

    summary_path = DESIGN_DIR / OUTPUT_NAMES[-1]
    summary_path.write_text(
        "\n".join(summary_lines),
        encoding="utf-8",
    )

    logger.info("Step 27B-2 completed.")
    logger.info("Summary: %s", summary_path)
    logger.info("Airport profile rows: %s", len(airport_df))
    logger.info("Airline combinations: %s", len(airline_df))
    logger.info("Candidate flight-key duplicate groups: %s", len(duplicate_df))
    logger.info("Route pairs: %s", len(route_df))
    logger.info("Weather files with issues: %s", weather_bad)

    return 0 if weather_bad == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
