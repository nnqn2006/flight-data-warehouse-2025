
from pathlib import Path
import pandas as pd

# =====================================================
# 1. CONFIGURATION
# =====================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = PROJECT_ROOT / "01_RAW_DATA" / "OurAirports"
OUTPUT_DIR = PROJECT_ROOT / "02_DATA_PROFILING" / "02_OurAirports"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FILES = [
    RAW_DIR / "airports.csv",
    RAW_DIR / "airports_reference.csv"
]

KEY_CANDIDATES = [
    "id",
    "ident",
    "iata_code",
    "gps_code",
    "local_code"
]


# =====================================================
# 2. PROFILE EACH FILE
# =====================================================

all_schema = []
all_missing = []
all_duplicates = []

for file_path in FILES:

    if not file_path.exists():
        print(f"\nFile not found: {file_path}")
        continue

    print("\n" + "=" * 70)
    print(f"PROCESSING: {file_path.name}")
    print("=" * 70)

    # Read everything as strings to preserve original values.
    # Empty cells are retained as empty strings for profiling.
    df = pd.read_csv(
        file_path,
        dtype="string",
        keep_default_na=False,
        encoding="utf-8-sig",
        low_memory=False
    )

    row_count, column_count = df.shape

    print(f"Rows: {row_count:,}")
    print(f"Columns: {column_count:,}")
    print(f"Full duplicate rows: {df.duplicated().sum():,}")

    print("\nCOLUMN NAMES:")
    for i, col in enumerate(df.columns, start=1):
        print(f"{i:02d}. {col}")

    # =================================================
    # 3. MISSING VALUE PROFILE
    # =================================================

    for col in df.columns:

        blank_mask = df[col].str.strip().eq("")
        blank_count = int(blank_mask.sum())

        non_blank = df.loc[~blank_mask, col]

        all_missing.append({
            "SourceFile": file_path.name,
            "Column": col,
            "TotalRows": row_count,
            "BlankCount": blank_count,
            "NonBlankCount": row_count - blank_count,
            "BlankPercent": round(
                blank_count / row_count * 100, 4
            ) if row_count else 0
        })

        # =================================================
        # 4. SCHEMA PROFILE
        # =================================================

        samples = non_blank.drop_duplicates().head(3).tolist()

        all_schema.append({
            "SourceFile": file_path.name,
            "ColumnPosition": len(all_schema) + 1,
            "Column": col,
            "PandasDtype": str(df[col].dtype),
            "NonBlankCount": len(non_blank),
            "UniqueNonBlankCount": non_blank.nunique(),
            "BlankCount": blank_count,
            "BlankPercent": round(
                blank_count / row_count * 100, 4
            ) if row_count else 0,
            "SampleValues": " | ".join(
                str(x)[:80] for x in samples
            )
        })

    # =================================================
    # 5. DUPLICATE PROFILE
    # =================================================

    all_duplicates.append({
        "SourceFile": file_path.name,
        "TotalRows": row_count,
        "TotalColumns": column_count,
        "FullDuplicateRows": int(df.duplicated().sum()),
        "DuplicateColumnNames": int(
            df.columns.duplicated().sum()
        )
    })

    # Check potential identifier columns.
    for key in KEY_CANDIDATES:

        if key not in df.columns:
            continue

        values = df[key].str.strip()
        valid_values = values[values.ne("")]

        duplicate_count = int(
            valid_values.duplicated().sum()
        )

        duplicate_value_count = int(
            valid_values[
                valid_values.duplicated(keep=False)
            ].nunique()
        )

        all_duplicates.append({
            "SourceFile": file_path.name,
            "TotalRows": row_count,
            "TotalColumns": column_count,
            "FullDuplicateRows": None,
            "DuplicateColumnNames": None,
            "CheckedKey": key,
            "NonBlankKeyCount": len(valid_values),
            "DuplicateKeyRows": duplicate_count,
            "DistinctDuplicatedKeys": duplicate_value_count
        })

        print(
            f"{key}: "
            f"{duplicate_count:,} duplicated nonblank values"
        )

    # Save a small sample for manual inspection.
    sample_path = OUTPUT_DIR / (
        file_path.stem + "_sample.csv"
    )

    df.head(10).to_csv(
        sample_path,
        index=False,
        encoding="utf-8-sig"
    )

    print(f"\nSample saved: {sample_path}")


# =====================================================
# 6. EXPORT REPORTS
# =====================================================

schema_df = pd.DataFrame(all_schema)
missing_df = pd.DataFrame(all_missing)
duplicate_df = pd.DataFrame(all_duplicates)

schema_path = OUTPUT_DIR / "01_ourairports_schema.csv"
missing_path = OUTPUT_DIR / "02_ourairports_missing_profile.csv"
duplicate_path = OUTPUT_DIR / "03_ourairports_duplicate_profile.csv"

schema_df.to_csv(
    schema_path,
    index=False,
    encoding="utf-8-sig"
)

missing_df.to_csv(
    missing_path,
    index=False,
    encoding="utf-8-sig"
)

duplicate_df.to_csv(
    duplicate_path,
    index=False,
    encoding="utf-8-sig"
)

print("\n" + "=" * 70)
print("OURAIRPORTS SCHEMA PROFILING COMPLETED")
print("=" * 70)

print(f"Schema report: {schema_path}")
print(f"Missing report: {missing_path}")
print(f"Duplicate report: {duplicate_path}")

print("\nNo source files were modified.")
