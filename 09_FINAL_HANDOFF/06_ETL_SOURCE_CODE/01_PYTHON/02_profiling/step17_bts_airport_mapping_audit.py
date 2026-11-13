
from pathlib import Path
from collections import Counter

import pandas as pd


# =====================================================
# 1. CONFIGURATION
# =====================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BTS_DIR = PROJECT_ROOT / "01_RAW_DATA" / "BTS"

AIRPORT_FILE = (
    PROJECT_ROOT
    / "01_RAW_DATA"
    / "OurAirports"
    / "airports.csv"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "02_DATA_PROFILING"
    / "02_OurAirports"
)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE = 200_000


# =====================================================
# 2. LOAD AIRPORT MASTER
# =====================================================

print("=" * 70)
print("STEP17 - BTS / OURAIRPORTS MAPPING AUDIT")
print("=" * 70)

airports = pd.read_csv(
    AIRPORT_FILE,
    dtype="string",
    keep_default_na=False,
    encoding="utf-8-sig",
    low_memory=False
)

required_columns = [
    "id",
    "ident",
    "type",
    "name",
    "municipality",
    "iso_country",
    "iso_region",
    "iata_code"
]

missing_columns = [
    col for col in required_columns
    if col not in airports.columns
]

if missing_columns:
    raise ValueError(
        f"Missing airport columns: {missing_columns}"
    )

airports["iata_key"] = (
    airports["iata_code"]
    .str.strip()
    .str.upper()
)

airport_iata = airports[
    airports["iata_key"].ne("")
].copy()

duplicate_iata = airport_iata[
    airport_iata["iata_key"].duplicated(keep=False)
]

if not duplicate_iata.empty:

    print("\nWARNING: Duplicate IATA codes detected.")

    duplicate_iata.to_csv(
        OUTPUT_DIR / "09_duplicate_iata_codes.csv",
        index=False,
        encoding="utf-8-sig"
    )

    raise ValueError(
        "IATA codes are not unique. "
        "Resolve mapping ambiguity before continuing."
    )

airport_lookup = (
    airport_iata
    .set_index("iata_key")
    .to_dict("index")
)

print(f"\nOurAirports total rows: {len(airports):,}")
print(f"Airports with IATA code: {len(airport_iata):,}")
print(
    "Unique IATA codes: "
    f"{airport_iata['iata_key'].nunique():,}"
)


# =====================================================
# 3. FIND BTS FILES
# =====================================================

bts_files = sorted(
    BTS_DIR.rglob("BTS_2025_*.csv")
)

if not bts_files:
    raise FileNotFoundError(
        f"No BTS monthly CSV files found in {BTS_DIR}"
    )

print(f"\nFound {len(bts_files)} BTS files.")


# =====================================================
# 4. COUNT BTS AIRPORT CODE OCCURRENCES
# =====================================================

origin_counter = Counter()
dest_counter = Counter()

blank_origin = 0
blank_dest = 0

for file_path in bts_files:

    print(f"\nProcessing {file_path.name}...")

    for chunk in pd.read_csv(
        file_path,
        usecols=["Origin", "Dest"],
        dtype="string",
        keep_default_na=False,
        encoding="utf-8-sig",
        chunksize=CHUNK_SIZE,
        low_memory=False
    ):

        origins = (
            chunk["Origin"]
            .str.strip()
            .str.upper()
        )

        destinations = (
            chunk["Dest"]
            .str.strip()
            .str.upper()
        )

        blank_origin += int(origins.eq("").sum())
        blank_dest += int(destinations.eq("").sum())

        origin_counter.update(
            origins[origins.ne("")].tolist()
        )

        dest_counter.update(
            destinations[destinations.ne("")].tolist()
        )

    print(f"Completed: {file_path.name}")


# =====================================================
# 5. BUILD DISTINCT AIRPORT CODE LIST
# =====================================================

all_codes = sorted(
    set(origin_counter.keys())
    | set(dest_counter.keys())
)

print("\nBTS distinct airport codes:", len(all_codes))


# =====================================================
# 6. MATCH BTS WITH OURAIRPORTS
# =====================================================

mapping_rows = []

for code in all_codes:

    airport = airport_lookup.get(code)

    origin_count = origin_counter.get(code, 0)
    dest_count = dest_counter.get(code, 0)

    row = {
        "IATACode": code,
        "CodeLength": len(code),

        "OriginFlightRows": origin_count,
        "DestFlightRows": dest_count,

        "TotalOccurrences": (
            origin_count + dest_count
        ),

        "MatchStatus": (
            "MATCHED"
            if airport is not None
            else "UNMATCHED"
        )
    }

    if airport is not None:

        row.update({
            "AirportID": airport["id"],
            "AirportIdent": airport["ident"],
            "AirportType": airport["type"],
            "AirportName": airport["name"],
            "Municipality": airport["municipality"],
            "CountryCode": airport["iso_country"],
            "RegionCode": airport["iso_region"]
        })

    else:

        row.update({
            "AirportID": "",
            "AirportIdent": "",
            "AirportType": "",
            "AirportName": "",
            "Municipality": "",
            "CountryCode": "",
            "RegionCode": ""
        })

    mapping_rows.append(row)


mapping_df = pd.DataFrame(mapping_rows)


# =====================================================
# 7. EXPORT MAPPING DETAILS
# =====================================================

mapping_path = (
    OUTPUT_DIR
    / "08_bts_ourairports_iata_mapping.csv"
)

mapping_df.to_csv(
    mapping_path,
    index=False,
    encoding="utf-8-sig"
)


# =====================================================
# 8. EXPORT UNMATCHED CODES
# =====================================================

unmatched_df = mapping_df[
    mapping_df["MatchStatus"] == "UNMATCHED"
].copy()

unmatched_path = (
    OUTPUT_DIR
    / "09_bts_unmatched_iata_codes.csv"
)

unmatched_df.to_csv(
    unmatched_path,
    index=False,
    encoding="utf-8-sig"
)


# =====================================================
# 9. SUMMARY
# =====================================================

matched_df = mapping_df[
    mapping_df["MatchStatus"] == "MATCHED"
].copy()

distinct_total = len(mapping_df)
distinct_matched = len(matched_df)

distinct_unmatched = len(unmatched_df)

total_occurrences = int(
    mapping_df["TotalOccurrences"].sum()
)

matched_occurrences = int(
    matched_df["TotalOccurrences"].sum()
)

unmatched_occurrences = int(
    unmatched_df["TotalOccurrences"].sum()
)

distinct_match_rate = (
    distinct_matched / distinct_total * 100
    if distinct_total else 0
)

occurrence_match_rate = (
    matched_occurrences / total_occurrences * 100
    if total_occurrences else 0
)

summary = pd.DataFrame([{

    "BTS_DistinctAirportCodes": distinct_total,

    "MatchedDistinctCodes": distinct_matched,

    "UnmatchedDistinctCodes": distinct_unmatched,

    "DistinctCodeMatchPercent": round(
        distinct_match_rate, 4
    ),

    "BTS_TotalAirportCodeOccurrences": total_occurrences,

    "MatchedOccurrences": matched_occurrences,

    "UnmatchedOccurrences": unmatched_occurrences,

    "OccurrenceMatchPercent": round(
        occurrence_match_rate, 4
    ),

    "BlankOriginRows": blank_origin,

    "BlankDestRows": blank_dest

}])

summary_path = (
    OUTPUT_DIR
    / "10_bts_ourairports_mapping_summary.csv"
)

summary.to_csv(
    summary_path,
    index=False,
    encoding="utf-8-sig"
)


# =====================================================
# 10. PRINT RESULTS
# =====================================================

print("\n" + "=" * 70)
print("MAPPING AUDIT SUMMARY")
print("=" * 70)

print(summary.to_string(index=False))

print("\nMatched distinct codes:", distinct_matched)
print("Unmatched distinct codes:", distinct_unmatched)

print("\nTop 20 unmatched codes:")

if not unmatched_df.empty:

    print(
        unmatched_df[
            [
                "IATACode",
                "OriginFlightRows",
                "DestFlightRows",
                "TotalOccurrences"
            ]
        ]
        .sort_values(
            "TotalOccurrences",
            ascending=False
        )
        .head(20)
        .to_string(index=False)
    )

else:

    print("No unmatched codes.")

print("\nReports saved:")
print(mapping_path)
print(unmatched_path)
print(summary_path)

print("\nNo source files were modified.")
