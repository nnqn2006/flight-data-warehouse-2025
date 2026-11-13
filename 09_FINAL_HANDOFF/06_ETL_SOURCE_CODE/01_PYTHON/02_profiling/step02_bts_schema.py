
import pandas as pd
import csv
import re

from pathlib import Path

# ============================================
# 1. KHAI BÁO ĐƯỜNG DẪN
# ============================================

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

if not BTS_DIR.exists():
    raise FileNotFoundError(
        f"Không tìm thấy: {BTS_DIR}"
    )

# ============================================
# 2. TÌM CÁC FILE CSV BTS NĂM 2025
# ============================================

pattern = re.compile(
    r"2025[_-](0?[1-9]|1[0-2])(?:$|[_-])"
)

monthly_files = {}

for file in BTS_DIR.rglob("*.csv"):

    match = pattern.search(file.stem)

    if match:

        month = int(match.group(1))

        monthly_files.setdefault(
            month, []
        ).append(file)

print("========== BTS FILE CHECK ==========")

for month in range(1, 13):

    matches = monthly_files.get(
        month, []
    )

    print(
        f"Month {month:02d}: "
        f"{len(matches)} file(s)"
    )

    for f in matches:
        print("   ", f.name)

# Phải có đúng một file dữ liệu cho mỗi tháng
invalid_months = {
    month: monthly_files.get(month, [])
    for month in range(1, 13)
    if len(monthly_files.get(month, [])) != 1
}

if invalid_months:

    raise RuntimeError(
        "Có tháng thiếu hoặc có nhiều file CSV. "
        "Hãy kiểm tra danh sách file trước khi tiếp tục."
    )

# ============================================
# 3. KIỂM TRA HEADER VÀ SCHEMA
# ============================================

schema_results = []
reference_header = None
schema_consistent = True

for month in range(1, 13):

    file = monthly_files[month][0]

    with open(
        file,
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        raw_header = next(
            csv.reader(f)
        )

    pandas_header = pd.read_csv(
        file,
        nrows=0
    ).columns.tolist()

    blank_positions = [
        i + 1
        for i, col in enumerate(raw_header)
        if col.strip() == ""
    ]

    if reference_header is None:

        reference_header = raw_header

    is_same = (
        raw_header == reference_header
    )

    if not is_same:
        schema_consistent = False

    schema_results.append({
        "Month": month,
        "File_Name": file.name,
        "Column_Count": len(raw_header),
        "Blank_Header_Count": len(blank_positions),
        "Blank_Header_Positions": str(
            blank_positions
        ),
        "Schema_Consistent": is_same
    })

# ============================================
# 4. KIỂM TRA SỐ DÒNG VÀ NGÀY
# ============================================

monthly_results = []

for month in range(1, 13):

    file = monthly_files[month][0]

    print(
        f"\nProcessing: {file.name}"
    )

    total_rows = 0
    invalid_dates = 0
    out_of_month = 0

    min_date = None
    max_date = None

    for chunk in pd.read_csv(
        file,
        usecols=["FlightDate"],
        dtype={"FlightDate": "string"},
        chunksize=200000
    ):

        dates = pd.to_datetime(
            chunk["FlightDate"],
            errors="coerce"
        )

        total_rows += len(chunk)

        invalid_dates += dates.isna().sum()

        valid_dates = dates.dropna()

        if not valid_dates.empty:

            chunk_min = valid_dates.min()
            chunk_max = valid_dates.max()

            if (
                min_date is None
                or chunk_min < min_date
            ):
                min_date = chunk_min

            if (
                max_date is None
                or chunk_max > max_date
            ):
                max_date = chunk_max

            wrong_month = (
                (valid_dates.dt.year != 2025)
                | (valid_dates.dt.month != month)
            )

            out_of_month += wrong_month.sum()

    monthly_results.append({
        "Month": month,
        "File_Name": file.name,
        "Total_Rows": total_rows,
        "Invalid_Dates": int(invalid_dates),
        "Out_Of_Month": int(out_of_month),
        "Min_Date": (
            str(min_date.date())
            if min_date is not None
            else None
        ),
        "Max_Date": (
            str(max_date.date())
            if max_date is not None
            else None
        )
    })

    print(
        "Rows:", total_rows,
        "| Invalid:", invalid_dates,
        "| Out of month:", out_of_month
    )

# ============================================
# 5. XUẤT BÁO CÁO
# ============================================

monthly_report = pd.DataFrame(
    monthly_results
)

schema_report = pd.DataFrame(
    schema_results
)

monthly_report.to_csv(
    OUTPUT_DIR / "01_monthly_summary.csv",
    index=False,
    encoding="utf-8-sig"
)

schema_report.to_csv(
    OUTPUT_DIR / "02_schema_comparison.csv",
    index=False,
    encoding="utf-8-sig"
)

# ============================================
# 6. TỔNG KẾT
# ============================================

print("\n========== FINAL SUMMARY ==========")

print(
    "Total BTS rows:",
    monthly_report["Total_Rows"].sum()
)

print(
    "Total columns:",
    len(reference_header)
)

print(
    "Schema consistent:",
    schema_consistent
)

print(
    "Total invalid dates:",
    monthly_report["Invalid_Dates"].sum()
)

print(
    "Total out-of-month:",
    monthly_report["Out_Of_Month"].sum()
)

print("\n--- Monthly Summary ---")

print(
    monthly_report.to_string(
        index=False
    )
)

print("\nReports saved at:")
print(OUTPUT_DIR)
