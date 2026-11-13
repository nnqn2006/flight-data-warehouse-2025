
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

MAPPING_FILE = (
    PROJECT_ROOT
    / "02_DATA_PROFILING"
    / "02_OurAirports"
    / "08_bts_ourairports_iata_mapping.csv"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "02_DATA_PROFILING"
    / "02_OurAirports"
)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Explicitly validated historical IATA alias.
# Do not infer aliases automatically from arbitrary keywords.

HISTORICAL_ALIAS = {
    "PBI": {
        "airport_id": "3766",
        "valid_from": "",
        "valid_to": "2026-08-17",
        "mapping_method": "HISTORICAL_IATA_ALIAS"
    }
}

CURRENT_CODE = {
    "DJT": {
        "airport_id": "3766",
        "valid_from": "2026-08-18",
        "valid_to": "",
        "mapping_method": "CURRENT_IATA_CODE"
    }
}


# =====================================================
# 2. LOAD INPUT DATA
# =====================================================

mapping = pd.read_csv(
    MAPPING_FILE,
    dtype="string",
    keep_default_na=False,
    encoding="utf-8-sig"
)

airports = pd.read_csv(
    AIRPORT_FILE,
    usecols=[
        "id",
        "ident",
        "type",
        "name",
        "iso_country",
        "iso_region",
        "icao_code",
        "iata_code",
        "keywords"
    ],
    dtype="string",
    keep_default_na=False,
    encoding="utf-8-sig"
)

required_mapping_columns = [
    "IATACode",
    "TotalOccurrences",
    "MatchStatus"
]

for col in required_mapping_columns:
    if col not in mapping.columns:
        raise ValueError(
            f"Missing mapping column: {col}"
        )


# =====================================================
# 3. VALIDATE AIRPORT RECORD
# =====================================================

airport_id = "3766"

airport_record = airports[
    airports["id"].str.strip().eq(airport_id)
]

if len(airport_record) != 1:
    raise ValueError(
        "Expected exactly one OurAirports record "
        f"for airport ID {airport_id}."
    )

airport = airport_record.iloc[0]

if airport["iata_code"].strip().upper() != "DJT":
    raise ValueError(
        "Current IATA code validation failed."
    )

keywords = [
    value.strip().upper()
    for value in airport["keywords"].split(",")
]

if "PBI" not in keywords:
    raise ValueError(
        "Historical PBI alias not found in keywords."
    )

if "KPBI" not in keywords:
    raise ValueError(
        "Historical KPBI alias not found in keywords."
    )


# =====================================================
# 4. VALIDATE ORIGINAL UNMATCHED CODE
# =====================================================

pbi_rows = mapping[
    mapping["IATACode"].str.upper().eq("PBI")
]

if len(pbi_rows) != 1:
    raise ValueError(
        "Expected exactly one PBI row in Step17 report."
    )

pbi_row = pbi_rows.iloc[0]

if pbi_row["MatchStatus"] != "UNMATCHED":
    raise ValueError(
        "PBI was not originally marked as unmatched."
    )

pbi_occurrences = int(
    pd.to_numeric(
        pd.Series([pbi_row["TotalOccurrences"]]),
        errors="raise"
    ).iloc[0]
)

if pbi_occurrences != 61573:
    raise ValueError(
        f"Unexpected PBI occurrence count: "
        f"{pbi_occurrences}"
    )


# =====================================================
# 5. CREATE HISTORICAL CODE CROSSWALK
# =====================================================

crosswalk_rows = []

for code, info in HISTORICAL_ALIAS.items():

    crosswalk_rows.append({
        "AirportID": info["airport_id"],
        "AirportCode": code,
        "CodeSystem": "IATA",
        "ValidFrom": info["valid_from"],
        "ValidTo": info["valid_to"],
        "IsCurrent": False,
        "MappingMethod": info["mapping_method"],
        "Evidence": (
            "OurAirports keywords and official "
            "airport IATA code change notice"
        )
    })

for code, info in CURRENT_CODE.items():

    crosswalk_rows.append({
        "AirportID": info["airport_id"],
        "AirportCode": code,
        "CodeSystem": "IATA",
        "ValidFrom": info["valid_from"],
        "ValidTo": info["valid_to"],
        "IsCurrent": True,
        "MappingMethod": info["mapping_method"],
        "Evidence": (
            "Current OurAirports iata_code"
        )
    })

crosswalk = pd.DataFrame(crosswalk_rows)


# =====================================================
# 6. APPLY EXPLICIT HISTORICAL MAPPING
# =====================================================

resolved = mapping.copy()

resolved["OriginalMatchStatus"] = (
    resolved["MatchStatus"]
)

resolved["ResolvedMatchStatus"] = (
    resolved["MatchStatus"].map({
        "MATCHED": "MATCHED_DIRECT",
        "UNMATCHED": "UNMATCHED"
    })
)

resolved["MappingMethod"] = resolved[
    "MatchStatus"
].map({
    "MATCHED": "DIRECT_IATA_MATCH",
    "UNMATCHED": "NOT_MAPPED"
})

resolved["HistoricalAliasValidTo"] = ""

for code, info in HISTORICAL_ALIAS.items():

    mask = (
        resolved["IATACode"]
        .str.upper()
        .eq(code)
    )

    resolved.loc[mask, "AirportID"] = (
        airport["id"]
    )

    resolved.loc[mask, "AirportIdent"] = (
        airport["ident"]
    )

    resolved.loc[mask, "AirportType"] = (
        airport["type"]
    )

    resolved.loc[mask, "AirportName"] = (
        airport["name"]
    )

    resolved.loc[mask, "CountryCode"] = (
        airport["iso_country"]
    )

    resolved.loc[mask, "RegionCode"] = (
        airport["iso_region"]
    )

    resolved.loc[mask, "MatchStatus"] = (
        "MATCHED_HISTORICAL_ALIAS"
    )

    resolved.loc[mask, "ResolvedMatchStatus"] = (
        "MATCHED_HISTORICAL_ALIAS"
    )

    resolved.loc[mask, "MappingMethod"] = (
        info["mapping_method"]
    )

    resolved.loc[mask, "HistoricalAliasValidTo"] = (
        info["valid_to"]
    )


# =====================================================
# 7. BUILD SUMMARY
# =====================================================

resolved["TotalOccurrences"] = pd.to_numeric(
    resolved["TotalOccurrences"],
    errors="raise"
)

total_codes = len(resolved)

matched = resolved[
    resolved["ResolvedMatchStatus"].isin([
        "MATCHED_DIRECT",
        "MATCHED_HISTORICAL_ALIAS"
    ])
]

unmatched = resolved[
    resolved["ResolvedMatchStatus"] == "UNMATCHED"
]

total_occurrences = int(
    resolved["TotalOccurrences"].sum()
)

matched_occurrences = int(
    matched["TotalOccurrences"].sum()
)

unmatched_occurrences = int(
    unmatched["TotalOccurrences"].sum()
)

summary = pd.DataFrame([{

    "TotalDistinctCodes": total_codes,

    "DirectlyMatchedCodes": int(
        (resolved["ResolvedMatchStatus"] == "MATCHED_DIRECT").sum()
    ),

    "HistoricallyMatchedCodes": int(
        (
            resolved["ResolvedMatchStatus"]
            == "MATCHED_HISTORICAL_ALIAS"
        ).sum()
    ),

    "UnmatchedCodes": len(unmatched),

    "DistinctMatchPercent": round(
        len(matched) / total_codes * 100,
        4
    ) if total_codes else 0,

    "TotalOccurrences": total_occurrences,

    "MatchedOccurrences": matched_occurrences,

    "UnmatchedOccurrences": unmatched_occurrences,

    "OccurrenceMatchPercent": round(
        matched_occurrences / total_occurrences * 100,
        4
    ) if total_occurrences else 0

}])


# =====================================================
# 8. EXPORT REPORTS
# =====================================================

crosswalk.to_csv(
    OUTPUT_DIR / "12_historical_airport_code_crosswalk.csv",
    index=False,
    encoding="utf-8-sig"
)

resolved.to_csv(
    OUTPUT_DIR / "13_bts_airport_mapping_resolved.csv",
    index=False,
    encoding="utf-8-sig"
)

summary.to_csv(
    OUTPUT_DIR / "14_bts_airport_mapping_resolved_summary.csv",
    index=False,
    encoding="utf-8-sig"
)


# =====================================================
# 9. PRINT RESULTS
# =====================================================

print("\n" + "=" * 70)
print("HISTORICAL AIRPORT CODE MAPPING")
print("=" * 70)

print("\nValidated airport:")
print(f"AirportID: {airport['id']}")
print(f"Current IATA: {airport['iata_code']}")
print(f"Historical alias: PBI")
print(f"Original PBI occurrences: {pbi_occurrences:,}")

print("\nHistorical crosswalk:")
print(crosswalk.to_string(index=False))

print("\nResolved mapping summary:")
print(summary.to_string(index=False))

print("\nResolved PBI record:")

print(
    resolved[
        resolved["IATACode"].eq("PBI")
    ][
        [
            "IATACode",
            "AirportID",
            "AirportName",
            "OriginalMatchStatus",
            "ResolvedMatchStatus",
            "MappingMethod",
            "HistoricalAliasValidTo",
            "TotalOccurrences"
        ]
    ].to_string(index=False)
)

print("\nReports saved in:")
print(OUTPUT_DIR)

print("\nNo source files were modified.")
