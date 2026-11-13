
from pathlib import Path
import pandas as pd

# =====================================================
# 1. CONFIGURATION
# =====================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

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


# =====================================================
# 2. LOAD DATA
# =====================================================

columns = [
    "id",
    "ident",
    "type",
    "name",
    "latitude_deg",
    "longitude_deg",
    "iso_country",
    "iso_region",
    "icao_code",
    "iata_code",
    "gps_code",
    "local_code",
    "keywords"
]

df = pd.read_csv(
    AIRPORT_FILE,
    usecols=columns,
    dtype="string",
    keep_default_na=False,
    encoding="utf-8-sig",
    low_memory=False
)


# =====================================================
# 3. STANDARDIZE FOR SEARCH ONLY
# =====================================================

search_fields = [
    "ident",
    "icao_code",
    "iata_code",
    "gps_code",
    "local_code"
]

for col in search_fields:
    df[f"_{col}_search"] = (
        df[col].str.strip().str.upper()
    )

df["_keywords_search"] = (
    df["keywords"].str.upper()
)


# =====================================================
# 4. SEARCH HISTORICAL AND CURRENT IDENTIFIERS
# =====================================================

keyword_tokens = (
    df["keywords"]
    .str.upper()
    .str.split(",")
    .apply(
        lambda values: [
            value.strip() for value in values
        ]
    )
)

has_pbi_keyword = keyword_tokens.apply(
    lambda values: "PBI" in values
)

has_kpbi_keyword = keyword_tokens.apply(
    lambda values: "KPBI" in values
)

has_palm_beach_name = df["name"].str.contains(
    "PALM BEACH|PRESIDENT DONALD",
    case=False,
    regex=True,
    na=False
)

has_related_identifier = (
    df["_iata_code_search"].isin(["PBI", "DJT"])
    | df["_ident_search"].isin(["KPBI", "KDJT"])
    | df["_icao_code_search"].isin(["KPBI", "KDJT"])
    | df["_gps_code_search"].isin(["KPBI", "KDJT"])
    | df["_local_code_search"].isin(["PBI", "DJT"])
)

candidate_mask = (
    has_pbi_keyword
    | has_kpbi_keyword
    | has_palm_beach_name
    | has_related_identifier
)

candidates = df.loc[candidate_mask].copy()


# =====================================================
# 5. ADD MATCH REASONS
# =====================================================

def get_reasons(row):

    reasons = []

    if row["_iata_code_search"] in ["PBI", "DJT"]:
        reasons.append("IATA_CODE_MATCH")

    if row["_ident_search"] in ["KPBI", "KDJT"]:
        reasons.append("IDENT_MATCH")

    if row["_icao_code_search"] in ["KPBI", "KDJT"]:
        reasons.append("ICAO_MATCH")

    if row["_gps_code_search"] in ["KPBI", "KDJT"]:
        reasons.append("GPS_MATCH")

    if row["_local_code_search"] in ["PBI", "DJT"]:
        reasons.append("LOCAL_CODE_MATCH")

    tokens = [
        token.strip()
        for token in row["keywords"].upper().split(",")
    ]

    if "PBI" in tokens:
        reasons.append("OLD_IATA_IN_KEYWORDS")

    if "KPBI" in tokens:
        reasons.append("OLD_ICAO_IN_KEYWORDS")

    if (
        "PALM BEACH" in row["name"].upper()
        or "PRESIDENT DONALD" in row["name"].upper()
    ):
        reasons.append("AIRPORT_NAME_MATCH")

    return " | ".join(reasons)


candidates["MatchReason"] = candidates.apply(
    get_reasons,
    axis=1
)


# =====================================================
# 6. EXPORT
# =====================================================

output_columns = columns + ["MatchReason"]

output_df = candidates[output_columns].copy()

output_path = (
    OUTPUT_DIR
    / "11_ourairports_pbi_historical_code_check.csv"
)

output_df.to_csv(
    output_path,
    index=False,
    encoding="utf-8-sig"
)


# =====================================================
# 7. SUMMARY
# =====================================================

print("\n" + "=" * 70)
print("PBI HISTORICAL CODE CHECK")
print("=" * 70)

print(f"Total OurAirports rows: {len(df):,}")
print(f"Related candidate rows: {len(candidates):,}")

if candidates.empty:

    print("\nNo related airport record found.")

else:

    print("\nCandidate records:")

    print(
        output_df.to_string(index=False)
    )

print(f"\nReport saved: {output_path}")
print("\nNo source files were modified.")
