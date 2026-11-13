
from __future__ import annotations

import logging
import sys
from collections import Counter, defaultdict
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd
import pyarrow.parquet as pq
from timezonefinder import TimezoneFinder


# ============================================================
# STEP 27B-3B - AIRPORT TO IANA TIMEZONE MAPPING CANDIDATES
# Read-only: does not modify RAW or STAGING.
# ============================================================

ROOT = Path(r"D:\DATA_WAREHOUSE")
BTS_DIR = ROOT / "03_STAGING_DATA" / "BTS"
OURAIRPORTS_FILE = ROOT / "03_STAGING_DATA" / "OurAirports" / "stg_ourairports.parquet"
WEATHER_DIR = ROOT / "03_STAGING_DATA" / "OpenMeteo"
OUT_DIR = ROOT / "06_DW_DESIGN" / "01_LOGICAL_MODEL"

BATCH_SIZE = 100_000
EXPECTED_BTS_ROWS = 7_001_619
EXPECTED_AIRPORT_IDS = 352

# Historical 2025 exception verified in the earlier airport crosswalk:
# BTS code PBI -> OurAirports ID 3766, ident KPBI.
HISTORICAL_IATA_ALIAS = {"PBI": ("3766", "KPBI")}

MAP_FILE = OUT_DIR / "27B3B_airport_timezone_mapping_candidates.csv"
ISSUES_FILE = OUT_DIR / "27B3B_airport_timezone_mapping_issues.csv"
SUMMARY_FILE = OUT_DIR / "27B3B_timezone_validation_summary.txt"
LOG_FILE = OUT_DIR / "27B3B_timezone_mapping.log"

BTS_COLUMNS = ["OriginAirportID", "Origin", "DestAirportID", "Dest"]
OA_COLUMNS = [
    "id", "ident", "type", "name", "latitude_deg", "longitude_deg",
    "iso_country", "iso_region", "municipality", "iata_code", "icao_code",
]


def pkg_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "NOT_INSTALLED"


def clean_text(series: pd.Series, upper: bool = False) -> pd.Series:
    s = series.astype("string").str.strip()
    s = s.mask(
        s.isna()
        | s.eq("")
        | s.str.upper().isin(["NA", "NAN", "NONE", "NULL"])
    )
    return s.str.upper() if upper else s


def clean_id(series: pd.Series) -> pd.Series:
    s = clean_text(series)
    numeric = pd.to_numeric(s, errors="coerce")
    valid = numeric.notna()
    s.loc[valid] = (
        numeric.loc[valid].round().astype("Int64").astype("string")
    )
    return s


def make_logger() -> logging.Logger:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("step27b3b")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)

    return logger


def check_outputs_do_not_exist() -> None:
    existing = [
        p for p in [MAP_FILE, ISSUES_FILE, SUMMARY_FILE, LOG_FILE]
        if p.exists()
    ]

    if existing:
        raise FileExistsError(
            "Output Step 27B-3B đã tồn tại; hãy archive/đổi tên trước "
            "khi chạy lại:\n"
            + "\n".join(str(p) for p in existing)
        )


def add_pairs(
    frame: pd.DataFrame,
    id_column: str,
    code_column: str,
    counter: Counter,
) -> tuple[int, int]:

    ids = clean_id(frame[id_column])
    codes = clean_text(frame[code_column], upper=True)

    missing_ids = int(ids.isna().sum())
    missing_codes = int(codes.isna().sum())

    pairs = pd.DataFrame({
        "AirportID": ids,
        "IATACode": codes,
    }).dropna()

    if not pairs.empty:
        for (airport_id, code), count in pairs.value_counts(
            sort=False
        ).items():
            counter[(str(airport_id), str(code))] += int(count)

    return missing_ids, missing_codes


def scan_bts_airports(logger: logging.Logger) -> dict:

    files = sorted(BTS_DIR.glob("stg_bts_2025_*.parquet"))

    if len(files) != 12:
        raise FileNotFoundError(
            f"Cần 12 BTS Parquet files; tìm thấy {len(files)} "
            f"tại {BTS_DIR}"
        )

    origin_pairs = Counter()
    dest_pairs = Counter()
    missing = Counter()
    total_rows = 0

    for file_no, path in enumerate(files, start=1):

        pf = pq.ParquetFile(path)

        absent = [
            c for c in BTS_COLUMNS
            if c not in pf.schema.names
        ]

        if absent:
            raise ValueError(
                f"Thiếu cột trong {path.name}: {absent}"
            )

        file_rows = 0

        logger.info(
            "Scanning BTS %s (%d/12)",
            path.name,
            file_no
        )

        for batch in pf.iter_batches(
            batch_size=BATCH_SIZE,
            columns=BTS_COLUMNS
        ):

            df = batch.to_pandas()

            file_rows += len(df)
            total_rows += len(df)

            a, b = add_pairs(
                df,
                "OriginAirportID",
                "Origin",
                origin_pairs
            )

            missing["OriginAirportID"] += a
            missing["Origin"] += b

            a, b = add_pairs(
                df,
                "DestAirportID",
                "Dest",
                dest_pairs
            )

            missing["DestAirportID"] += a
            missing["Dest"] += b

        logger.info(
            "Completed %s; rows=%s",
            path.name,
            f"{file_rows:,}"
        )

    if total_rows != EXPECTED_BTS_ROWS:
        raise ValueError(
            f"BTS row count mismatch: expected "
            f"{EXPECTED_BTS_ROWS:,}, got {total_rows:,}"
        )

    id_to_codes = defaultdict(Counter)
    code_to_ids = defaultdict(Counter)

    origin_counts = Counter()
    dest_counts = Counter()

    for (airport_id, code), count in origin_pairs.items():
        id_to_codes[airport_id][code] += count
        code_to_ids[code][airport_id] += count
        origin_counts[airport_id] += count

    for (airport_id, code), count in dest_pairs.items():
        id_to_codes[airport_id][code] += count
        code_to_ids[code][airport_id] += count
        dest_counts[airport_id] += count

    logger.info(
        "BTS scan complete: rows=%s, AirportIDs=%d, IATA codes=%d",
        f"{total_rows:,}",
        len(id_to_codes),
        len(code_to_ids)
    )

    if len(id_to_codes) != EXPECTED_AIRPORT_IDS:
        logger.warning(
            "BTS AirportID count differs from baseline %d: %d",
            EXPECTED_AIRPORT_IDS,
            len(id_to_codes)
        )

    return {
        "files": files,
        "rows": total_rows,
        "id_to_codes": id_to_codes,
        "code_to_ids": code_to_ids,
        "origin_counts": origin_counts,
        "dest_counts": dest_counts,
        "missing": missing,
    }


def load_reference_data(
    logger: logging.Logger
) -> tuple[pd.DataFrame, set[str]]:

    if not OURAIRPORTS_FILE.exists():
        raise FileNotFoundError(OURAIRPORTS_FILE)

    pf = pq.ParquetFile(OURAIRPORTS_FILE)

    absent = [
        c for c in OA_COLUMNS
        if c not in pf.schema.names
    ]

    if absent:
        raise ValueError(
            f"Thiếu cột trong OurAirports: {absent}"
        )

    oa = pf.read(columns=OA_COLUMNS).to_pandas()

    oa["id"] = clean_id(oa["id"])
    oa["iata_code"] = clean_text(oa["iata_code"], upper=True)
    oa["ident"] = clean_text(oa["ident"], upper=True)

    for col in [
        "type", "name", "iso_country", "iso_region",
        "municipality", "icao_code"
    ]:
        oa[col] = clean_text(oa[col])

    oa["latitude_deg"] = pd.to_numeric(
        oa["latitude_deg"], errors="coerce"
    )

    oa["longitude_deg"] = pd.to_numeric(
        oa["longitude_deg"], errors="coerce"
    )

    weather_codes = set()

    for path in WEATHER_DIR.glob("weather_*_2025.parquet"):
        code = path.stem[
            len("weather_"):-len("_2025")
        ].upper()

        weather_codes.add(code)

    logger.info(
        "OurAirports rows=%s; Open-Meteo airport files=%d",
        f"{len(oa):,}",
        len(weather_codes)
    )

    return oa, weather_codes


def build_candidates(
    bts: dict,
    oa: pd.DataFrame,
    weather_codes: set[str],
    logger: logging.Logger
) -> pd.DataFrame:

    by_iata = {
        str(code): grp
        for code, grp in oa.dropna(
            subset=["iata_code"]
        ).groupby("iata_code", sort=False)
    }

    by_id = {
        str(airport_id): grp
        for airport_id, grp in oa.dropna(
            subset=["id"]
        ).groupby("id", sort=False)
    }

    finder = TimezoneFinder()

    output = []

    for airport_id in sorted(
        bts["id_to_codes"],
        key=lambda x: int(x)
    ):

        code_counts = bts["id_to_codes"][airport_id]
        codes = sorted(code_counts)

        code = codes[0] if len(codes) == 1 else None

        row = {
            "BTSAirportID": airport_id,
            "BTSAirportCode": code,
            "BTSObservedCodesForID": ";".join(codes),
            "BTSCodeVariantCountForID": len(codes),

            "BTSOriginFlightOccurrences": int(
                bts["origin_counts"].get(airport_id, 0)
            ),

            "BTSDestinationFlightOccurrences": int(
                bts["dest_counts"].get(airport_id, 0)
            ),

            "BTSAirportOccurrences": int(
                bts["origin_counts"].get(airport_id, 0)
                + bts["dest_counts"].get(airport_id, 0)
            ),

            "OurAirportsID": None,
            "OurAirportsIdent": None,
            "AirportName": None,
            "AirportType": None,
            "Municipality": None,
            "ISOCountry": None,
            "ISORegion": None,
            "Latitude": None,
            "Longitude": None,

            "AirportCrosswalkMethod": None,
            "AirportCrosswalkStatus": None,

            "TimezoneMatches": None,
            "TimezoneFinderPrimary": None,
            "TimeZoneIANA": None,
            "TimezoneLookupStatus": None,

            "UTCOffset_2025_01_15_Minutes": None,
            "UTCOffset_2025_07_15_Minutes": None,
            "TimezoneOffsetDifference_JulMinusJan_Minutes": None,

            "HasOpenMeteo2025File": bool(
                code in weather_codes
            ) if code else False,
        }

        # A BTS AirportID must correspond to exactly one IATA code.
        if len(codes) != 1:

            row["AirportCrosswalkStatus"] = (
                "REVIEW_BTS_ID_HAS_MULTIPLE_CODES"
            )

            row["AirportCrosswalkMethod"] = "NOT_MAPPED"
            row["TimezoneLookupStatus"] = "NOT_ATTEMPTED"

            output.append(row)
            continue

        # One BTS code should not refer to multiple IDs.
        if len(bts["code_to_ids"].get(code, {})) > 1:

            row["AirportCrosswalkStatus"] = (
                "REVIEW_BTS_CODE_HAS_MULTIPLE_IDS"
            )

            row["AirportCrosswalkMethod"] = "NOT_MAPPED"
            row["TimezoneLookupStatus"] = "NOT_ATTEMPTED"

            output.append(row)
            continue

        # Historical alias: PBI in 2025 -> OurAirports ID 3766 / KPBI.
        if code in HISTORICAL_IATA_ALIAS:

            target_id, expected_ident = HISTORICAL_IATA_ALIAS[code]
            target = by_id.get(target_id)

            if target is None or len(target) != 1:

                row["AirportCrosswalkStatus"] = (
                    "REVIEW_HISTORICAL_TARGET_NOT_FOUND_OR_NOT_UNIQUE"
                )

                row["AirportCrosswalkMethod"] = "HISTORICAL_IATA_ALIAS"
                row["TimezoneLookupStatus"] = "NOT_ATTEMPTED"

                output.append(row)
                continue

            airport = target.iloc[0]

            if airport["ident"] != expected_ident:

                row["AirportCrosswalkStatus"] = (
                    "REVIEW_HISTORICAL_IDENT_MISMATCH"
                )

                row["AirportCrosswalkMethod"] = "HISTORICAL_IATA_ALIAS"
                row["TimezoneLookupStatus"] = "NOT_ATTEMPTED"

                output.append(row)
                continue

            row["AirportCrosswalkMethod"] = (
                "HISTORICAL_IATA_ALIAS_PBI_TO_ID_3766"
            )

            row["AirportCrosswalkStatus"] = (
                "MAPPED_HISTORICAL_ALIAS"
            )

        else:

            matches = by_iata.get(code)

            if matches is None or len(matches) == 0:

                row["AirportCrosswalkStatus"] = (
                    "REVIEW_OURAIRPORTS_IATA_NOT_FOUND"
                )

                row["AirportCrosswalkMethod"] = "DIRECT_IATA_MATCH"
                row["TimezoneLookupStatus"] = "NOT_ATTEMPTED"

                output.append(row)
                continue

            if len(matches) != 1:

                row["AirportCrosswalkStatus"] = (
                    "REVIEW_OURAIRPORTS_IATA_NOT_UNIQUE"
                )

                row["AirportCrosswalkMethod"] = "DIRECT_IATA_MATCH"
                row["TimezoneLookupStatus"] = "NOT_ATTEMPTED"

                output.append(row)
                continue

            airport = matches.iloc[0]

            row["AirportCrosswalkMethod"] = "DIRECT_IATA_MATCH"
            row["AirportCrosswalkStatus"] = "MAPPED_DIRECT"

        # Attach attributes from OurAirports primary.
        row.update({
            "OurAirportsID": str(airport["id"]),
            "OurAirportsIdent": airport["ident"],
            "AirportName": airport["name"],
            "AirportType": airport["type"],
            "Municipality": airport["municipality"],
            "ISOCountry": airport["iso_country"],
            "ISORegion": airport["iso_region"],
            "Latitude": airport["latitude_deg"],
            "Longitude": airport["longitude_deg"],
        })

        lat = airport["latitude_deg"]
        lon = airport["longitude_deg"]

        if pd.isna(lat) or pd.isna(lon):

            row["TimezoneLookupStatus"] = (
                "REVIEW_COORDINATES_MISSING"
            )

            output.append(row)
            continue

        lat, lon = float(lat), float(lon)

        if not (-90 <= lat <= 90 and -180 <= lon <= 180):

            row["TimezoneLookupStatus"] = (
                "REVIEW_COORDINATES_OUT_OF_RANGE"
            )

            output.append(row)
            continue

        try:
            # Returns all timezone polygons containing this point.
            zones = sorted(set(
                finder.timezones_at(lng=lon, lat=lat) or []
            ))

            row["TimezoneMatches"] = ";".join(zones)

            row["TimezoneFinderPrimary"] = finder.timezone_at(
                lng=lon,
                lat=lat
            )

            if len(zones) == 0:

                row["TimezoneLookupStatus"] = (
                    "REVIEW_NO_TIMEZONE_POLYGON_MATCH"
                )

            elif len(zones) > 1:

                row["TimezoneLookupStatus"] = (
                    "REVIEW_MULTIPLE_TIMEZONE_POLYGON_MATCHES"
                )

            else:

                zone_name = zones[0]

                try:
                    zone = ZoneInfo(zone_name)

                except ZoneInfoNotFoundError:

                    row["TimezoneLookupStatus"] = (
                        "REVIEW_ZONEINFO_KEY_NOT_FOUND"
                    )

                else:

                    row["TimeZoneIANA"] = zone_name

                    row["TimezoneLookupStatus"] = (
                        "LOOKUP_OK_UNAMBIGUOUS"
                    )

                    # Diagnostic offsets only; not fixed UTC offsets.
                    jan = datetime(
                        2025, 1, 15, 12, 0, tzinfo=zone
                    )

                    jul = datetime(
                        2025, 7, 15, 12, 0, tzinfo=zone
                    )

                    jan_offset = int(
                        jan.utcoffset().total_seconds() // 60
                    )

                    jul_offset = int(
                        jul.utcoffset().total_seconds() // 60
                    )

                    row["UTCOffset_2025_01_15_Minutes"] = jan_offset
                    row["UTCOffset_2025_07_15_Minutes"] = jul_offset

                    row[
                        "TimezoneOffsetDifference_JulMinusJan_Minutes"
                    ] = jul_offset - jan_offset

        except Exception as exc:

            row["TimezoneLookupStatus"] = (
                "REVIEW_TIMEZONEFINDER_ERROR_"
                + type(exc).__name__
            )

        output.append(row)

    mapping = pd.DataFrame(output)

    # Check if multiple BTS IDs were mapped to the same OurAirports ID.
    duplicated_oa = (
        mapping["OurAirportsID"].notna()
        & mapping["OurAirportsID"].duplicated(keep=False)
    )

    if duplicated_oa.any():

        mapping.loc[
            duplicated_oa,
            "AirportCrosswalkStatus"
        ] = "REVIEW_MULTIPLE_BTS_IDS_TO_ONE_OURAIRPORTS_ID"

    mapping["CandidateStatus"] = "CANDIDATE_READY_FOR_REVIEW"

    needs_review = (
        mapping["AirportCrosswalkStatus"]
        .astype("string")
        .str.startswith("REVIEW", na=False)
        |
        mapping["TimezoneLookupStatus"]
        .astype("string")
        .str.startswith("REVIEW", na=False)
    )

    mapping.loc[
        needs_review,
        "CandidateStatus"
    ] = "REVIEW"

    logger.info(
        "Built %d airport mapping candidates",
        len(mapping)
    )

    return mapping


def main() -> int:

    check_outputs_do_not_exist()
    logger = make_logger()

    logger.info("STEP 27B-3B - AIRPORT TIMEZONE MAPPING")

    logger.info(
        "timezonefinder=%s",
        pkg_version("timezonefinder")
    )

    logger.info(
        "timezonefinder-data=%s",
        pkg_version("timezonefinder-data")
    )

    logger.info(
        "tzdata=%s",
        pkg_version("tzdata")
    )

    bts = scan_bts_airports(logger)

    oa, weather_codes = load_reference_data(logger)

    mapping = build_candidates(
        bts,
        oa,
        weather_codes,
        logger
    )

    mapping = mapping.sort_values(
        "BTSAirportID",
        key=lambda s: pd.to_numeric(s, errors="coerce"),
        kind="stable"
    ).reset_index(drop=True)

    issues = mapping[
        mapping["CandidateStatus"].eq("REVIEW")
    ].copy()

    mapping.to_csv(
        MAP_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    issues.to_csv(
        ISSUES_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    crosswalk_counts = (
        mapping["AirportCrosswalkStatus"]
        .value_counts(dropna=False)
        .to_dict()
    )

    timezone_counts = (
        mapping["TimezoneLookupStatus"]
        .value_counts(dropna=False)
        .to_dict()
    )

    mapped_count = int(
        mapping["OurAirportsID"].notna().sum()
    )

    timezone_count = int(
        mapping["TimeZoneIANA"].notna().sum()
    )

    ready_count = int(
        mapping["CandidateStatus"]
        .eq("CANDIDATE_READY_FOR_REVIEW")
        .sum()
    )

    review_count = int(
        mapping["CandidateStatus"].eq("REVIEW").sum()
    )

    summary = [
        "STEP 27B-3B - AIRPORT TIMEZONE MAPPING CANDIDATE SUMMARY",
        "=" * 78,

        f"Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",

        "",

        "PACKAGE VERSIONS",

        f"timezonefinder: {pkg_version('timezonefinder')}",
        f"timezonefinder-data: {pkg_version('timezonefinder-data')}",
        f"tzdata: {pkg_version('tzdata')}",

        "",

        "SOURCE COUNTS",

        f"BTS files scanned: {len(bts['files'])}",
        f"BTS rows scanned: {bts['rows']:,}",
        f"Distinct BTS Airport IDs: {len(bts['id_to_codes']):,}",
        f"Distinct BTS IATA codes: {len(bts['code_to_ids']):,}",
        f"OurAirports rows: {len(oa):,}",
        f"Open-Meteo airport files: {len(weather_codes):,}",

        "",

        "MAPPING RESULTS",

        f"Airport mapping rows: {len(mapping):,}",
        f"Matched to OurAirports ID: {mapped_count:,}",
        f"Unambiguous IANA timezone candidates: {timezone_count:,}",
        f"Candidate-ready rows (not final approval): {ready_count:,}",
        f"Rows requiring review: {review_count:,}",

        f"Distinct assigned IANA timezones: "
        f"{mapping['TimeZoneIANA'].nunique(dropna=True):,}",

        f"Airports with Open-Meteo 2025 file: "
        f"{int(mapping['HasOpenMeteo2025File'].sum()):,}",

        "",

        "AIRPORT CROSSWALK STATUS COUNTS",
    ]

    summary.extend(
        f"{k}: {v}"
        for k, v in sorted(crosswalk_counts.items())
    )

    summary.extend([
        "",
        "TIMEZONE LOOKUP STATUS COUNTS"
    ])

    summary.extend(
        f"{k}: {v}"
        for k, v in sorted(timezone_counts.items())
    )

    summary.extend([
        "",
        "BTS MISSING KEY COUNTS",

        f"OriginAirportID missing: "
        f"{bts['missing']['OriginAirportID']:,}",

        f"Origin code missing: "
        f"{bts['missing']['Origin']:,}",

        f"DestAirportID missing: "
        f"{bts['missing']['DestAirportID']:,}",

        f"Dest code missing: "
        f"{bts['missing']['Dest']:,}",

        "",

        "INTERPRETATION GUARDRAILS",

        "- This is a candidate mapping, not a final approved production crosswalk.",
        "- BTS AirportID/IATA pairs are re-derived from the 2025 BTS source records.",
        "- Historical PBI is explicitly mapped to OurAirports ID 3766 / ident KPBI.",
        "- Coordinates come from OurAirports primary data, not Open-Meteo request metadata.",
        "- Multiple timezone polygon matches are flagged; no arbitrary timezone is selected.",
        "- January/July UTC offsets are diagnostic samples, not fixed UTC offsets.",
        "- This step does not convert BTS local flight timestamps to UTC.",
        "- RAW and STAGING are read-only and were not modified.",

        "",

        "OUTPUT FILES",

        str(MAP_FILE),
        str(ISSUES_FILE),
        str(SUMMARY_FILE),
        str(LOG_FILE),
    ])

    SUMMARY_FILE.write_text(
        "\n".join(summary) + "\n",
        encoding="utf-8"
    )

    logger.info(
        "Completed: airport rows=%d, mapped=%d, timezone candidates=%d, review=%d",
        len(mapping),
        mapped_count,
        timezone_count,
        review_count
    )

    logger.info("Mapping: %s", MAP_FILE)
    logger.info("Issues: %s", ISSUES_FILE)
    logger.info("Summary: %s", SUMMARY_FILE)

    return 0


if __name__ == "__main__":

    try:
        raise SystemExit(main())

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
