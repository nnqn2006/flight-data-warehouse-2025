
import pandas as pd
from pathlib import Path
import re

# =========================================
# 1. CONFIGURATION
# =========================================

ROOT_DIR = Path.cwd()

BTS_DIR = ROOT_DIR / "01_RAW_DATA" / "BTS"

OUTPUT_DIR = (
    ROOT_DIR
    / "02_DATA_PROFILING"
    / "01_BTS"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)

CHUNK_SIZE = 100_000

# Chỉ nhận file BTS năm 2025
pattern = re.compile(
    r"BTS_2025_(\d{1,2})\.csv$",
    re.IGNORECASE
)

files = []

for file in BTS_DIR.rglob("*.csv"):

    match = pattern.match(file.name)

    if match:
        month = int(match.group(1))
        files.append((month, file))

print("BTS Directory:", BTS_DIR.resolve())
print("Directory exists:", BTS_DIR.exists())

print("\nCSV files found:")

for file in BTS_DIR.rglob("*.csv"):
    print(file.name)
    
files.sort(key=lambda x: x[0])

if len(files) != 12:
    raise ValueError(
        f"Cần đúng 12 file BTS, hiện tìm thấy {len(files)}"
    )

# =========================================
# 2. INITIALIZE VARIABLES
# =========================================

global_missing = {}
monthly_records = []
empty_column_records = []

grand_total_rows = 0
reference_columns = None

# =========================================
# 3. PROFILE EACH MONTH
# =========================================

for month, file in files:

    print(f"\nProcessing {file.name}...")

    monthly_missing = {}
    monthly_total = 0

    for chunk in pd.read_csv(
        file,
        dtype="string",
        chunksize=CHUNK_SIZE,
        low_memory=False,
        keep_default_na=False,
        encoding="utf-8-sig"
    ):

        if reference_columns is None:
            reference_columns = chunk.columns.tolist()

            global_missing = {
                col: 0
                for col in reference_columns
            }

        # Kiểm tra schema không đổi
        if chunk.columns.tolist() != reference_columns:
            raise ValueError(
                f"Schema không đồng nhất: {file.name}"
            )

        monthly_total += len(chunk)

        for col in reference_columns:

            # Ô trống hoặc chỉ có khoảng trắng
            blank_mask = (
                chunk[col]
                .str.strip()
                .eq("")
            )

            missing_count = int(
                blank_mask.sum()
            )

            global_missing[col] += missing_count

            monthly_missing[col] = (
                monthly_missing.get(col, 0)
                + missing_count
            )

    grand_total_rows += monthly_total

    # -------------------------------------
    # MONTHLY REPORT
    # -------------------------------------

    for col in reference_columns:

        count = monthly_missing[col]

        monthly_records.append({
            "Month": month,
            "Column": col,
            "Total_Rows": monthly_total,
            "Missing_Count": count,
            "Missing_Rate": round(
                count / monthly_total * 100,
                4
            )
        })

    print(
        f"Rows: {monthly_total:,} | "
        f"Columns: {len(reference_columns)}"
    )

# =========================================
# 4. FIELD-LEVEL MISSING REPORT
# =========================================

field_records = []

for col in reference_columns:

    missing_count = global_missing[col]

    field_records.append({
        "Column": col,
        "Total_Rows": grand_total_rows,
        "Missing_Count": missing_count,
        "Non_Missing_Count": (
            grand_total_rows - missing_count
        ),
        "Missing_Rate": round(
            missing_count / grand_total_rows * 100,
            4
        )
    })

field_report = pd.DataFrame(field_records)

field_report = field_report.sort_values(
    "Missing_Rate",
    ascending=False
)

# =========================================
# 5. CHECK EMPTY HEADER COLUMN
# =========================================

for i, col in enumerate(reference_columns):

    if (
        col.startswith("Unnamed:")
        or col.strip() == ""
    ):

        empty_column_records.append({
            "Column_Position": i + 1,
            "Column_Name": col,
            "Total_Rows": grand_total_rows,
            "Blank_Count": global_missing[col],
            "Non_Blank_Count": (
                grand_total_rows - global_missing[col]
            )
        })

empty_report = pd.DataFrame(
    empty_column_records
)

# =========================================
# 6. SAVE REPORTS
# =========================================

monthly_report = pd.DataFrame(
    monthly_records
)

field_report.to_csv(
    OUTPUT_DIR / "03_bts_missing_by_field.csv",
    index=False,
    encoding="utf-8-sig"
)

monthly_report.to_csv(
    OUTPUT_DIR / "04_bts_missing_by_month.csv",
    index=False,
    encoding="utf-8-sig"
)

empty_report.to_csv(
    OUTPUT_DIR / "05_bts_empty_column_check.csv",
    index=False,
    encoding="utf-8-sig"
)

# =========================================
# 7. DISPLAY RESULTS
# =========================================

print("\n========== MISSING VALUE REPORT ==========")

print("Total Rows:", f"{grand_total_rows:,}")
print("Total Columns:", len(reference_columns))

print("\nTop 20 Missing Columns:")

print(
    field_report.head(20).to_string(
        index=False
    )
)

print("\nEmpty Header Column Check:")

print(
    empty_report.to_string(
        index=False
    )
)

print("\nReports saved at:")
print(OUTPUT_DIR)
