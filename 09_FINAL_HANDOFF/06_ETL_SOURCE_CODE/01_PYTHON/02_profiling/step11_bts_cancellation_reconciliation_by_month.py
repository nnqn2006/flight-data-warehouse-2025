
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

BTS_DIR = ROOT / "01_RAW_DATA" / "BTS"
OUTPUT_DIR = ROOT / "02_DATA_PROFILING" / "01_BTS"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

files = sorted(BTS_DIR.rglob("BTS_2025_*.csv"))

if len(files) != 12:
    raise FileNotFoundError(
        f"Expected 12 monthly files, found {len(files)}"
    )

monthly_results = []

for file in files:

    print(f"Processing {file.name}...")

    total_cancelled = 0
    total_wheelsoff_blank = 0
    total_landing9 = 0
    total_overlap = 0
    total_landing9_cancelled0 = 0

    for chunk in pd.read_csv(
        file,
        dtype=str,
        keep_default_na=False,
        usecols=[
            "Cancelled",
            "WheelsOff",
            "DivAirportLandings"
        ],
        chunksize=100_000,
        low_memory=False
    ):

        cancelled = pd.to_numeric(
            chunk["Cancelled"],
            errors="coerce"
        )

        landings = pd.to_numeric(
            chunk["DivAirportLandings"].str.strip(),
            errors="coerce"
        )

        wheelsoff_blank = (
            chunk["WheelsOff"].str.strip().eq("")
        )

        landing9 = landings.eq(9)

        # Các chỉ số theo từng tháng
        total_cancelled += int(cancelled.eq(1).sum())

        total_wheelsoff_blank += int(
            wheelsoff_blank.sum()
        )

        total_landing9 += int(landing9.sum())

        # Giao giữa hai nhóm:
        # WheelsOff trống VÀ DivAirportLandings = 9
        total_overlap += int(
            (wheelsoff_blank & landing9).sum()
        )

        # Mã 9 nhưng Cancelled = 0
        total_landing9_cancelled0 += int(
            (landing9 & cancelled.eq(0)).sum()
        )

    raw_formula = (
        total_wheelsoff_blank + total_landing9
    )

    raw_difference = raw_formula - total_cancelled

    # Phép đối chiếu chẩn đoán:
    # Loại ảnh hưởng đếm trùng và các dòng mã 9
    # nhưng Cancelled = 0.
    adjusted_formula = (
        raw_formula
        - total_overlap
        - total_landing9_cancelled0
    )

    adjusted_difference = (
        adjusted_formula - total_cancelled
    )

    monthly_results.append({
        "Month": file.stem[-2:],
        "Cancelled_Flag_1": total_cancelled,
        "WheelsOff_Blank": total_wheelsoff_blank,
        "DivAirportLandings_9": total_landing9,
        "Overlap_Between_Two_Groups": total_overlap,
        "Landing9_Cancelled0": total_landing9_cancelled0,
        "Raw_BTS_Formula": raw_formula,
        "Raw_Difference": raw_difference,
        "Diagnostic_Adjusted_Formula": adjusted_formula,
        "Diagnostic_Adjusted_Difference": adjusted_difference
    })

    print(
        f"Completed {file.name} | "
        f"Raw difference: {raw_difference} | "
        f"Adjusted difference: {adjusted_difference}"
    )

# Xuất báo cáo
result = pd.DataFrame(monthly_results)

output_file = (
    OUTPUT_DIR
    / "16_bts_cancellation_reconciliation_by_month.csv"
)

result.to_csv(
    output_file,
    index=False,
    encoding="utf-8-sig"
)

print("\n========== MONTHLY CANCELLATION RECONCILIATION ==========")
print(result.to_string(index=False))

print("\n========== ANNUAL TOTAL ==========")

numeric_columns = [
    col for col in result.columns
    if col != "Month"
]

annual = result[numeric_columns].sum()

for key, value in annual.items():
    print(f"{key}: {value}")

print(f"\nReport saved to: {output_file}")
