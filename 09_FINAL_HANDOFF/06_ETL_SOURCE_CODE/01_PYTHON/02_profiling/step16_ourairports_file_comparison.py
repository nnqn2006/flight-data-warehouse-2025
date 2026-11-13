
from pathlib import Path
import pandas as pd
import numpy as np

# =====================================================
# 1. CONFIGURATION
# =====================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = PROJECT_ROOT / "01_RAW_DATA" / "OurAirports"
OUTPUT_DIR = PROJECT_ROOT / "02_DATA_PROFILING" / "02_OurAirports"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FILE_MAIN = RAW_DIR / "airports.csv"
FILE_REFERENCE = RAW_DIR / "airports_reference.csv"

NUMERIC_COLUMNS = {
    "latitude_deg",
    "longitude_deg",
    "elevation_ft"
}


# =====================================================
# 2. READ DATA
# =====================================================

def read_source(file_path):

    return pd.read_csv(
        file_path,
        dtype="string",
        keep_default_na=False,
        encoding="utf-8-sig",
        low_memory=False
    )


print("=" * 70)
print("OURAIRPORTS FILE COMPARISON")
print("=" * 70)

main_df = read_source(FILE_MAIN)
ref_df = read_source(FILE_REFERENCE)

print(f"airports.csv rows: {len(main_df):,}")
print(f"airports_reference.csv rows: {len(ref_df):,}")


# =====================================================
# 3. CHECK IDENTIFIER UNIQUENESS
# =====================================================

for name, df in [
    ("airports.csv", main_df),
    ("airports_reference.csv", ref_df)
]:

    if "id" not in df.columns:
        raise ValueError(f"{name} does not contain id")

    df["_id_key"] = df["id"].str.strip()

    duplicate_ids = df["_id_key"].duplicated().sum()

    blank_ids = df["_id_key"].eq("").sum()

    print(
        f"\n{name}: "
        f"duplicate IDs = {duplicate_ids:,}, "
        f"blank IDs = {blank_ids:,}"
    )

    if duplicate_ids > 0 or blank_ids > 0:
        raise ValueError(
            f"Cannot safely compare {name} using id."
        )


# =====================================================
# 4. COMPARE COLUMN STRUCTURE
# =====================================================

main_columns = set(main_df.columns) - {"_id_key"}
ref_columns = set(ref_df.columns) - {"_id_key"}

main_only = sorted(main_columns - ref_columns)
ref_only = sorted(ref_columns - main_columns)
common_columns = [
    col for col in main_df.columns
    if col in ref_columns and col != "_id_key"
]

column_structure = pd.DataFrame([
    {
        "Column": col,
        "AvailableIn": "airports.csv only"
    }
    for col in main_only
] + [
    {
        "Column": col,
        "AvailableIn": "airports_reference.csv only"
    }
    for col in ref_only
])

column_structure.to_csv(
    OUTPUT_DIR / "07_ourairports_column_differences.csv",
    index=False,
    encoding="utf-8-sig"
)


# =====================================================
# 5. MERGE BY ID
# =====================================================

merged = main_df.merge(
    ref_df,
    on="_id_key",
    how="outer",
    suffixes=("_airports", "_reference"),
    indicator=True
)

matched = merged[merged["_merge"] == "both"].copy()

unmatched = merged[merged["_merge"] != "both"].copy()

print("\n" + "=" * 70)
print("ID MATCHING RESULTS")
print("=" * 70)

print(f"Matched IDs: {len(matched):,}")
print(f"Unmatched IDs: {len(unmatched):,}")

print("\nMerge status:")
print(merged["_merge"].value_counts())


# =====================================================
# 6. EXPORT UNMATCHED RECORDS
# =====================================================

unmatched_columns = [
    "_id_key",
    "_merge"
]

for col in ["id", "ident"]:
    for suffix in ["_airports", "_reference"]:
        name = col + suffix

        if name in unmatched.columns:
            unmatched_columns.append(name)

unmatched[unmatched_columns].to_csv(
    OUTPUT_DIR / "06_ourairports_unmatched_ids.csv",
    index=False,
    encoding="utf-8-sig"
)


# =====================================================
# 7. COMPARE SHARED COLUMN VALUES
# =====================================================

comparison_results = []
mismatch_records = []

for col in common_columns:

    if col == "id":
        continue

    left_col = f"{col}_airports"
    right_col = f"{col}_reference"

    if left_col not in matched.columns:
        continue

    if right_col not in matched.columns:
        continue

    left_raw = (
        matched[left_col]
        .fillna("")
        .astype("string")
        .str.strip()
    )

    right_raw = (
        matched[right_col]
        .fillna("")
        .astype("string")
        .str.strip()
    )

    left_blank = left_raw.eq("")
    right_blank = right_raw.eq("")

    both_blank = left_blank & right_blank

    if col in NUMERIC_COLUMNS:

        left_num = pd.to_numeric(
            left_raw.mask(left_blank),
            errors="coerce"
        )

        right_num = pd.to_numeric(
            right_raw.mask(right_blank),
            errors="coerce"
        )

        invalid_left = (~left_blank) & left_num.isna()
        invalid_right = (~right_blank) & right_num.isna()

        numeric_equal = pd.Series(
            np.isclose(
                left_num.fillna(0).to_numpy(dtype=float),
                right_num.fillna(0).to_numpy(dtype=float),
                atol=1e-8,
                rtol=0
            ),
            index=matched.index
        )

        same = (
            both_blank
            | (
                left_num.notna()
                & right_num.notna()
                & numeric_equal
            )
        )

    else:

        invalid_left = pd.Series(
            False,
            index=matched.index
        )

        invalid_right = pd.Series(
            False,
            index=matched.index
        )

        same_value = left_raw.eq(right_raw).fillna(False)

        same = both_blank | (
            ~left_blank
            & ~right_blank
            & same_value
        )

    mismatch_mask = ~same

    mismatch_count = int(mismatch_mask.sum())

    comparison_results.append({

        "Column": col,

        "ComparedRows": len(matched),

        "MainBlank": int(left_blank.sum()),

        "ReferenceBlank": int(right_blank.sum()),

        "BothBlank": int(both_blank.sum()),

        "MainPopulated_ReferenceBlank": int(
            ((~left_blank) & right_blank).sum()
        ),

        "ReferencePopulated_MainBlank": int(
            (left_blank & (~right_blank)).sum()
        ),

        "DifferentNonBlank": int(
            (
                (~left_blank)
                & (~right_blank)
                & mismatch_mask
            ).sum()
        ),

        "InvalidNumeric_Main": int(invalid_left.sum()),

        "InvalidNumeric_Reference": int(invalid_right.sum()),

        "MismatchRows": mismatch_count

    })

    # Export detailed mismatch records.
    if mismatch_count > 0:

        selected = matched.loc[
            mismatch_mask,
            [
                "id_airports",
                "ident_airports",
                left_col,
                right_col
            ]
        ].copy()

        selected.columns = [
            "AirportID",
            "AirportIdent",
            "airports_csv_Value",
            "airports_reference_Value"
        ]

        selected.insert(2, "Column", col)

        mismatch_records.append(selected)

    print(
        f"{col}: {mismatch_count:,} mismatched rows"
    )


# =====================================================
# 8. EXPORT COMPARISON RESULTS
# =====================================================

comparison_df = pd.DataFrame(comparison_results)

comparison_df.to_csv(
    OUTPUT_DIR / "04_ourairports_file_comparison.csv",
    index=False,
    encoding="utf-8-sig"
)

if mismatch_records:

    mismatch_df = pd.concat(
        mismatch_records,
        ignore_index=True
    )

else:

    mismatch_df = pd.DataFrame(
        columns=[
            "AirportID",
            "AirportIdent",
            "Column",
            "airports_csv_Value",
            "airports_reference_Value"
        ]
    )

mismatch_df.to_csv(
    OUTPUT_DIR / "05_ourairports_value_mismatches.csv",
    index=False,
    encoding="utf-8-sig"
)


# =====================================================
# 9. FINAL SUMMARY
# =====================================================

print("\n" + "=" * 70)
print("COMPARISON SUMMARY")
print("=" * 70)

print(f"Main file rows: {len(main_df):,}")
print(f"Reference file rows: {len(ref_df):,}")
print(f"Matched IDs: {len(matched):,}")
print(f"Unmatched IDs: {len(unmatched):,}")
print(f"Shared columns compared: {len(comparison_results):,}")
print(f"Total detailed mismatches: {len(mismatch_df):,}")

print("\nColumn comparison:")

if not comparison_df.empty:

    print(
        comparison_df[
            [
                "Column",
                "MismatchRows",
                "MainPopulated_ReferenceBlank",
                "ReferencePopulated_MainBlank",
                "DifferentNonBlank"
            ]
        ].to_string(index=False)
    )

print("\nReports saved in:")
print(OUTPUT_DIR)

print("\nNo source files were modified.")
