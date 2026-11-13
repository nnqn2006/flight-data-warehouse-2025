
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

BTS_DIR = ROOT / "01_RAW_DATA" / "BTS"
OUTPUT_DIR = ROOT / "02_DATA_PROFILING" / "01_BTS"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Tìm file tháng 9
files = list(BTS_DIR.rglob("BTS_2025_09.csv"))

if len(files) != 1:
    raise FileNotFoundError(
        f"Expected 1 September file, found {len(files)}"
    )

source_file = files[0]

# Xác định bản ghi cần kiểm tra
matches = []

for chunk in pd.read_csv(
    source_file,
    dtype=str,
    keep_default_na=False,
    chunksize=100_000,
    low_memory=False
):
    mask = (
        chunk["FlightDate"].eq("2025-09-06")
        & chunk["Reporting_Airline"].eq("YX")
        & chunk["Flight_Number_Reporting_Airline"].eq("5859")
        & chunk["Origin"].eq("JFK")
        & chunk["Dest"].eq("MVY")
        & chunk["CRSDepTime"].eq("1209")
    )

    if mask.any():
        matches.append(chunk.loc[mask])

if not matches:
    print("No matching record found.")
else:
    result = pd.concat(matches, ignore_index=True)

    output_file = (
        OUTPUT_DIR
        / "09_bts_full_record_anomaly.csv"
    )

    # Lưu toàn bộ cột, bao gồm cả cột rỗng nếu có
    result.to_csv(
        output_file,
        index=False,
        encoding="utf-8-sig"
    )

    print("\n========== FULL RECORD INSPECTION ==========")
    print(f"Number of matching records: {len(result)}")

    for idx, row in result.iterrows():
        print(f"\n--- Record {idx + 1} ---")

        # Chỉ in các trường có giá trị
        for col, value in row.items():
            if str(value).strip() != "":
                print(f"{col}: {value}")

    print(f"\nSaved to: {output_file}")
