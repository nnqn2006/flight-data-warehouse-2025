
"""STEP 27B-1 - Extract verified source schemas and create a mapping template.

Read-only: reads existing Parquet Staging files and writes design artifacts only.
It does not modify RAW or STAGING data.

Run from D:\\DATA_WAREHOUSE:
python .\\08_SCRIPTS\\02_profiling\\step27b_extract_source_schema_inventory.py

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
STAGING = ROOT / "03_STAGING_DATA"
DESIGN_DIR = ROOT / "06_DW_DESIGN" / "01_LOGICAL_MODEL"

BTS_DIR = STAGING / "BTS"
AIRPORT_DIR = STAGING / "OurAirports"
WEATHER_DIR = STAGING / "OpenMeteo"

LINEAGE_COLUMNS = {"SourceFile", "SourceMonth"}

# Candidate mappings only. These are not yet approved physical table definitions.
BTS_RULES = {
    "Year": ("Dim_Date", "CalendarYear", "DATE_ATTRIBUTE",
             "Cross-check against FlightDate; do not treat as independent date key."),
    "Quarter": ("Dim_Date", "CalendarQuarter", "DATE_ATTRIBUTE",
                "Cross-check/derive from FlightDate."),
    "Month": ("Dim_Date", "CalendarMonth", "DATE_ATTRIBUTE",
              "Cross-check/derive from FlightDate."),
    "DayofMonth": ("Dim_Date", "DayOfMonth", "DATE_ATTRIBUTE",
                   "Cross-check/derive from FlightDate."),
    "DayOfWeek": ("Dim_Date", "DayOfWeek", "DATE_ATTRIBUTE",
                  "Cross-check/derive from FlightDate."),
    "FlightDate": ("Dim_Date", "FullDate", "DATE_NATURAL_ATTRIBUTE",
                   "Candidate date role; retain raw value for audit."),
    "Reporting_Airline": ("Dim_Airline", "ReportingAirlineCode",
                          "DIMENSION_ATTRIBUTE",
                          "Resolve meaning/code system before selecting business key."),
    "DOT_ID_Reporting_Airline": ("Dim_Airline", "DOTAirlineID",
                                 "DIMENSION_BUSINESS_KEY",
                                 "Preserve as source identifier."),
    "IATA_CODE_Reporting_Airline": ("Dim_Airline", "IATAAirlineCode",
                                    "DIMENSION_BUSINESS_KEY",
                                    "Preserve as source identifier; confirm null/duplicate behavior."),
    "Origin": ("Dim_Airport", "OriginIATACode",
               "ROLE_PLAYING_DIMENSION_KEY",
               "Map to airport dimension as Origin role using date-aware airport crosswalk."),
    "Dest": ("Dim_Airport", "DestinationIATACode",
             "ROLE_PLAYING_DIMENSION_KEY",
             "Map to airport dimension as Destination role using date-aware airport crosswalk."),
    "OriginAirportID": ("Fact_Flight", "SourceOriginAirportID",
                        "SOURCE_IDENTIFIER",
                        "BTS source identifier; do not equate directly to OurAirports id."),
    "DestAirportID": ("Fact_Flight", "SourceDestAirportID",
                      "SOURCE_IDENTIFIER",
                      "BTS source identifier; do not equate directly to OurAirports id."),
    "OriginAirportSeqID": ("Fact_Flight", "SourceOriginAirportSeqID",
                           "SOURCE_IDENTIFIER",
                           "Preserve source airport sequence identifier."),
    "DestAirportSeqID": ("Fact_Flight", "SourceDestAirportSeqID",
                         "SOURCE_IDENTIFIER",
                         "Preserve source airport sequence identifier."),
    "Tail_Number": ("Fact_Flight", "TailNumber",
                    "DEGENERATE_ATTRIBUTE_CANDIDATE",
                    "Keep in Fact initially; decide whether a useful Dim_Aircraft exists."),
    "Flight_Number_Reporting_Airline": ("Fact_Flight", "FlightNumber",
                                        "DEGENERATE_ATTRIBUTE",
                                        "Not unique by itself; do not use as a flight primary key."),
    "CRSDepTime": ("Dim_Time", "ScheduledDepartureTime",
                   "ROLE_PLAYING_TIME",
                   "BTS local HHMM; parse with explicit 2400/invalid-value rules."),
    "DepTime": ("Dim_Time", "ActualDepartureTime",
                "ROLE_PLAYING_TIME",
                "BTS local HHMM; timestamp needs Origin airport IANA timezone."),
    "WheelsOff": ("Dim_Time", "WheelsOffTime",
                  "ROLE_PLAYING_TIME",
                  "BTS local HHMM; timestamp needs Origin airport IANA timezone."),
    "WheelsOn": ("Dim_Time", "WheelsOnTime",
                 "ROLE_PLAYING_TIME",
                 "BTS local HHMM; timestamp needs Destination airport IANA timezone."),
    "CRSArrTime": ("Dim_Time", "ScheduledArrivalTime",
                   "ROLE_PLAYING_TIME",
                   "BTS local HHMM; parse with explicit 2400/invalid-value rules."),
    "ArrTime": ("Dim_Time", "ActualArrivalTime",
                "ROLE_PLAYING_TIME",
                "BTS local HHMM; timestamp needs Destination airport IANA timezone."),
    "DepTimeBlk": ("Fact_Flight", "ScheduledDepartureTimeBlock",
                   "FACT_ATTRIBUTE",
                   "Categorical scheduled departure time block."),
    "ArrTimeBlk": ("Fact_Flight", "ScheduledArrivalTimeBlock",
                   "FACT_ATTRIBUTE",
                   "Categorical scheduled arrival time block."),
    "Cancelled": ("Fact_Flight", "CancelledFlag",
                  "FACT_FLAG",
                  "Preserve 0/1 semantics."),
    "CancellationCode": ("Fact_Flight", "CancellationCode",
                         "FACT_ATTRIBUTE",
                         "NULL is context-dependent; do not replace with zero/string code."),
    "Diverted": ("Fact_Flight", "DivertedFlag",
                 "FACT_FLAG",
                 "Preserve 0/1 semantics."),
}

BTS_FACT_MEASURES = {
    "DepDelay": "DepartureDelayMinutesSigned",
    "DepDelayMinutes": "DepartureDelayMinutesNonNegative",
    "DepDel15": "DepartureDelay15MinFlag",
    "DepartureDelayGroups": "DepartureDelayGroup",
    "TaxiOut": "TaxiOutMinutes",
    "TaxiIn": "TaxiInMinutes",
    "ArrDelay": "ArrivalDelayMinutesSigned",
    "ArrDelayMinutes": "ArrivalDelayMinutesNonNegative",
    "ArrDel15": "ArrivalDelay15MinFlag",
    "ArrivalDelayGroups": "ArrivalDelayGroup",
    "CRSElapsedTime": "ScheduledElapsedTimeMinutes",
    "ActualElapsedTime": "ActualElapsedTimeMinutes",
    "AirTime": "AirTimeMinutes",
    "Flights": "SourceFlightCountMeasure",
    "Distance": "DistanceMiles",
    "DistanceGroup": "DistanceGroup",
    "CarrierDelay": "CarrierDelayMinutes",
    "WeatherDelay": "WeatherDelayMinutes",
    "NASDelay": "NASDelayMinutes",
    "SecurityDelay": "SecurityDelayMinutes",
    "LateAircraftDelay": "LateAircraftDelayMinutes",
    "FirstDepTime": "FirstDepartureTime",
    "TotalAddGTime": "TotalAdditionalGroundTimeMinutes",
    "LongestAddGTime": "LongestAdditionalGroundTimeMinutes",
    "DivAirportLandings": "DiversionAirportLandingsCount",
    "DivReachedDest": "DivertedReachedDestinationFlag",
    "DivActualElapsedTime": "DivertedActualElapsedTimeMinutes",
    "DivArrDelay": "DivertedArrivalDelayMinutes",
    "DivDistance": "DivertedDistanceMiles",
}

AIRPORT_RULES = {
    "id": ("Dim_Airport", "OurAirportsID", "DIMENSION_BUSINESS_KEY",
           "OurAirports identifier; not interchangeable with BTS airport ID."),
    "ident": ("Dim_Airport", "AirportIdent", "DIMENSION_ATTRIBUTE",
              "Preserve source ident."),
    "type": ("Dim_Airport", "AirportType", "DIMENSION_ATTRIBUTE",
             "Airport classification."),
    "name": ("Dim_Airport", "AirportName", "DIMENSION_ATTRIBUTE",
             "Descriptive airport name."),
    "latitude_deg": ("Dim_Airport", "LatitudeDegrees", "GEOGRAPHIC_ATTRIBUTE",
                     "Coordinate of airport record."),
    "longitude_deg": ("Dim_Airport", "LongitudeDegrees", "GEOGRAPHIC_ATTRIBUTE",
                      "Coordinate of airport record."),
    "elevation_ft": ("Dim_Airport", "ElevationFeet", "GEOGRAPHIC_ATTRIBUTE",
                     "Elevation in feet."),
    "continent": ("Dim_Airport", "ContinentCode", "GEOGRAPHIC_ATTRIBUTE",
                  "Literal NA is a source token; profile/interpret separately, do not silently convert."),
    "iso_country": ("Dim_Airport", "CountryCode", "GEOGRAPHIC_ATTRIBUTE",
                    "Literal NA is a source token; profile/interpret separately, do not silently convert."),
    "iso_region": ("Dim_Airport", "RegionCode", "GEOGRAPHIC_ATTRIBUTE",
                   "ISO region code."),
    "municipality": ("Dim_Airport", "Municipality", "GEOGRAPHIC_ATTRIBUTE",
                     "Municipality/locality."),
    "scheduled_service": ("Dim_Airport", "ScheduledServiceFlag",
                          "DIMENSION_ATTRIBUTE",
                          "Boolean-like source field; standardize in Clean layer."),
    "gps_code": ("Dim_Airport", "GPSCode", "DIMENSION_ATTRIBUTE",
                 "Preserve GPS code."),
    "iata_code": ("Dim_Airport", "IATACode",
                  "DIMENSION_BUSINESS_KEY_CANDIDATE",
                  "Use date-aware crosswalk for historical code PBI; preserve original BTS code."),
    "local_code": ("Dim_Airport", "LocalCode", "DIMENSION_ATTRIBUTE",
                   "Not guaranteed unique."),
    "home_link": ("Dim_Airport", "HomePageURL", "DESCRIPTIVE_ATTRIBUTE",
                  "Optional URL."),
    "wikipedia_link": ("Dim_Airport", "WikipediaURL", "DESCRIPTIVE_ATTRIBUTE",
                       "Optional URL."),
    "keywords": ("Dim_Airport", "Keywords", "DESCRIPTIVE_ATTRIBUTE",
                 "Text attribute; assess usefulness."),
}

WEATHER_RULES = {
    "time": ("Fact_Airport_Weather_Hourly", "WeatherTimestampUTC",
             "NATURAL_GRAIN_ATTRIBUTE",
             "Hourly timestamp in UTC; keep UTC semantics."),
    "temperature_2m": ("Fact_Airport_Weather_Hourly", "Temperature2m",
                       "FACT_MEASURE",
                       "Temperature; confirm unit from saved request/response metadata."),
    "relative_humidity_2m": ("Fact_Airport_Weather_Hourly", "RelativeHumidity2m",
                             "FACT_MEASURE",
                             "Relative humidity percentage."),
    "precipitation": ("Fact_Airport_Weather_Hourly", "Precipitation",
                      "FACT_MEASURE",
                      "Hourly precipitation; preserve numeric zero separately from missing."),
    "wind_speed_10m": ("Fact_Airport_Weather_Hourly", "WindSpeed10m",
                       "FACT_MEASURE",
                       "Wind speed; confirm unit from saved request/response metadata."),
    "wind_direction_10m": ("Fact_Airport_Weather_Hourly", "WindDirection10m",
                           "FACT_MEASURE_CIRCULAR",
                           "Circular variable; do not average as an ordinary linear measure."),
    "wind_gusts_10m": ("Fact_Airport_Weather_Hourly", "WindGusts10m",
                       "FACT_MEASURE",
                       "Wind gust speed; confirm unit from saved request/response metadata."),
    "cloud_cover": ("Fact_Airport_Weather_Hourly", "CloudCover",
                    "FACT_MEASURE",
                    "Cloud cover percentage."),
    "weather_code": ("Fact_Airport_Weather_Hourly", "WeatherCode",
                     "FACT_ATTRIBUTE_CODE",
                     "Retain all codes, including 96; decode via a weather-code lookup if needed."),
}


def make_logger():
    DESIGN_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("step27b_schema_inventory")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    for handler in (
        logging.FileHandler(
            DESIGN_DIR / "27B_schema_inventory.log",
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ):
        handler.setFormatter(fmt)
        logger.addHandler(handler)

    return logger


def inspect_group(group, paths, logical_entity, logger):
    if not paths:
        raise FileNotFoundError(
            f"No Parquet files found for {group}"
        )

    paths = sorted(paths)
    base_schema = pq.ParquetFile(paths[0]).schema_arrow

    base_names = base_schema.names
    base_types = [str(t) for t in base_schema.types]

    consistency = []

    for path in paths:
        pf = pq.ParquetFile(path)
        schema = pf.schema_arrow

        same = (
            schema.names == base_names
            and [str(t) for t in schema.types] == base_types
        )

        consistency.append({
            "SourceGroup": group,
            "LogicalEntity": logical_entity,
            "File": path.name,
            "Rows": pf.metadata.num_rows,
            "Columns": len(schema.names),
            "SchemaMatchesFirstFile": same,
            "FirstFile": paths[0].name,
        })

        if not same:
            logger.warning("Schema mismatch: %s", path)

    inventory = []

    for ordinal, (name, typ) in enumerate(
        zip(base_names, base_schema.types),
        start=1
    ):
        inventory.append({
            "SourceGroup": group,
            "LogicalEntity": logical_entity,
            "ReferenceFile": paths[0].name,
            "ColumnOrdinal": ordinal,
            "SourceColumn": name,
            "ParquetType": str(typ),
            "IsLineageColumn": name in LINEAGE_COLUMNS,
        })

    logger.info(
        "%s: files=%s, rows(first)=%s, columns=%s, schema_consistent=%s",
        group,
        len(paths),
        pq.ParquetFile(paths[0]).metadata.num_rows,
        len(base_names),
        all(x["SchemaMatchesFirstFile"] for x in consistency),
    )

    return inventory, consistency, base_names


def candidate_mapping(inventory):
    rows = []

    for item in inventory:
        group = item["SourceGroup"]
        col = item["SourceColumn"]

        target_table = ""
        target_col = ""
        category = "UNMAPPED_REVIEW"

        rule = (
            "Inspect field meaning and decide whether it belongs "
            "in a dimension, fact, bridge, or audit metadata."
        )

        null_rule = (
            "Preserve source missingness; do not replace blank/NULL/"
            "literal NA without a documented rule."
        )

        status = "NEEDS_REVIEW"
        notes = "Candidate only; approve during logical modeling."

        if col in LINEAGE_COLUMNS:
            target_table = "ETL_AUDIT_METADATA"
            target_col = col
            category = "LINEAGE"
            rule = (
                "Preserve source lineage for traceability; "
                "final storage location to be decided."
            )
            status = "PROPOSED"
            notes = "Not a business dimension or measure."

        elif group == "BTS":
            if col in BTS_RULES:
                target_table, target_col, category, notes = BTS_RULES[col]
                rule = notes
                status = "PROPOSED"

            elif col in BTS_FACT_MEASURES:
                target_table = "Fact_Flight"
                target_col = BTS_FACT_MEASURES[col]
                category = "FACT_MEASURE_OR_ATTRIBUTE"

                rule = (
                    "Preserve source value and original sign/meaning; "
                    "define aggregation and NULL semantics before ETL."
                )

                status = "PROPOSED"
                notes = (
                    "Measure/attribute candidate; "
                    "confirm analytical use and unit."
                )

            elif col.startswith(("Origin", "Dest")):
                target_table = "Dim_Airport / Fact_Flight"
                target_col = col
                category = "AIRPORT_ROLE_OR_SOURCE_ATTRIBUTE"

                rule = (
                    "Determine whether descriptive attributes belong in "
                    "Dim_Airport or source identifiers remain in Fact_Flight."
                )

                notes = (
                    "Origin and Destination are role-playing references; "
                    "do not duplicate airport dimensions by default."
                )

            elif col.startswith("Div"):
                target_table = "Fact_Flight"
                target_col = col
                category = "DIVERSION_ATTRIBUTE_OR_MEASURE"

                rule = (
                    "Preserve as source field; assess sparse/conditional "
                    "availability and role before modeling."
                )

                notes = (
                    "Diversion fields are condition-dependent; "
                    "NULL is not zero."
                )

            else:
                target_table = (
                    "Fact_Flight / Dim_Airline / Dim_Date / Dim_Time"
                )
                target_col = col
                category = "UNMAPPED_REVIEW"

                rule = (
                    "Assign after reviewing BTS data dictionary "
                    "and analytical requirements."
                )

        elif group == "OurAirports_primary":
            if col in AIRPORT_RULES:
                target_table, target_col, category, notes = AIRPORT_RULES[col]
                rule = notes
                status = "PROPOSED"
            else:
                target_table = "Dim_Airport"
                target_col = col
                category = "UNMAPPED_REVIEW"

                rule = (
                    "Review source definition and decide whether "
                    "the attribute is useful for the airport dimension."
                )

        elif group == "OurAirports_reference":
            target_table = "REFERENCE_ONLY"
            target_col = col
            category = "REFERENCE_VALIDATION"

            rule = (
                "Retain as supporting reference; do not load as a "
                "second set of airport dimension members by default."
            )

            status = "PROPOSED"
            notes = (
                "Use for cross-checking source attributes and provenance."
            )

        elif group == "OpenMeteo":
            if col in WEATHER_RULES:
                target_table, target_col, category, notes = WEATHER_RULES[col]
                rule = notes
                status = "PROPOSED"

            elif col.lower() in {
                "airport_code", "iata_code", "airport_iata", "airport"
            }:
                target_table = "Dim_Airport"
                target_col = "IATACode"
                category = "WEATHER_AIRPORT_REFERENCE"

                rule = (
                    "Resolve to Dim_Airport through "
                    "the validated airport crosswalk."
                )

                status = "PROPOSED"

            else:
                target_table = (
                    "Fact_Airport_Weather_Hourly / ETL_AUDIT_METADATA"
                )
                target_col = col
                category = "WEATHER_METADATA_REVIEW"

                rule = (
                    "Inspect metadata meaning; some fields may be "
                    "provenance rather than fact attributes."
                )

        rows.append({
            "SourceGroup": group,
            "LogicalEntity": item["LogicalEntity"],
            "ReferenceFile": item["ReferenceFile"],
            "SourceColumn": col,
            "ColumnOrdinal": item["ColumnOrdinal"],
            "SourceType": item["ParquetType"],
            "CandidateTargetTable": target_table,
            "CandidateTargetColumn": target_col,
            "MappingCategory": category,
            "TransformationOrRule": rule,
            "NullAndLiteralNAHandling": null_rule,
            "DecisionStatus": status,
            "DesignNotes": notes,
        })

    return rows


def main():
    logger = make_logger()

    logger.info(
        "STEP 27B-1 - EXTRACT SOURCE SCHEMAS (READ-ONLY)"
    )

    bts_files = list(
        BTS_DIR.glob("stg_bts_2025_*.parquet")
    )

    airport_primary = [
        AIRPORT_DIR / "stg_ourairports.parquet"
    ]

    airport_reference = [
        AIRPORT_DIR / "stg_ourairports_reference.parquet"
    ]

    weather_files = list(
        WEATHER_DIR.glob("weather_*.parquet")
    )

    groups = [
        ("BTS", bts_files, "BTS_Flight"),
        (
            "OurAirports_primary",
            airport_primary,
            "OurAirports_Airport_Master",
        ),
        (
            "OurAirports_reference",
            airport_reference,
            "OurAirports_Reference",
        ),
        (
            "OpenMeteo",
            weather_files,
            "OpenMeteo_Airport_Hourly_Weather",
        ),
    ]

    all_inventory = []
    all_consistency = []
    group_names = {}

    for group, paths, entity in groups:
        inv, con, names = inspect_group(
            group, paths, entity, logger
        )

        all_inventory.extend(inv)
        all_consistency.extend(con)
        group_names[group] = names

    inventory_df = pd.DataFrame(all_inventory)
    consistency_df = pd.DataFrame(all_consistency)
    mapping_df = pd.DataFrame(
        candidate_mapping(all_inventory)
    )

    inventory_path = (
        DESIGN_DIR / "27B_source_schema_inventory.csv"
    )

    consistency_path = (
        DESIGN_DIR / "27B_schema_consistency.csv"
    )

    mapping_path = (
        DESIGN_DIR / "27B_source_to_target_mapping_template.csv"
    )

    summary_path = (
        DESIGN_DIR / "27B_schema_inventory_summary.txt"
    )

    existing_outputs = [
        p for p in (
            inventory_path,
            consistency_path,
            mapping_path,
            summary_path,
        )
        if p.exists()
    ]

    if existing_outputs:
        raise FileExistsError(
            "Step 27B output already exists. Rename/archive previous "
            "outputs before rerunning: "
            + ", ".join(str(p) for p in existing_outputs)
        )

    inventory_df.to_csv(
        inventory_path,
        index=False,
        encoding="utf-8-sig",
    )

    consistency_df.to_csv(
        consistency_path,
        index=False,
        encoding="utf-8-sig",
    )

    mapping_df.to_csv(
        mapping_path,
        index=False,
        encoding="utf-8-sig",
    )

    consistent_groups = (
        consistency_df.groupby("SourceGroup")[
            "SchemaMatchesFirstFile"
        ].all().to_dict()
    )

    lines = [
        "STEP 27B-1 - SOURCE SCHEMA INVENTORY",
        "=" * 72,
        f"Run timestamp: {datetime.now():%Y%m%d_%H%M%S}",
        "",
        "SOURCE GROUP SUMMARY",
    ]

    for group, paths, entity in groups:
        lines.append(
            f"{group}: files={len(paths)}, "
            f"columns={len(group_names[group])}, "
            f"schema_consistent={consistent_groups.get(group, False)}"
        )

    lines += [
        "",
        f"Inventory rows: {len(inventory_df)}",
        f"Mapping-template rows: {len(mapping_df)}",
        "",
        "FILES CREATED",
        str(inventory_path),
        str(consistency_path),
        str(mapping_path),
        "",
        "IMPORTANT",
        "- Candidate mappings are proposals, not approved target definitions.",
        "- Review all NEEDS_REVIEW rows before finalizing the logical model.",
        "- Open-Meteo metadata fields are deliberately not guessed if unknown.",
        "- OurAirports reference file is marked reference-only by default.",
        "- No RAW or STAGING data was modified.",
    ]

    summary_path.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )

    logger.info(
        "Step 27B-1 complete. Summary: %s",
        summary_path,
    )

    logger.info(
        "Mapping template: %s",
        mapping_path,
    )

    return (
        0 if all(consistent_groups.values()) else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
