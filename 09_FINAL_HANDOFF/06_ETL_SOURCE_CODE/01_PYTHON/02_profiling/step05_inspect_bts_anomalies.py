from pathlib import Path
import pandas as pd

# Project root
ROOT = Path(__file__).resolve().parents[2]

BTS_DIR = ROOT / "01_RAW_DATA" / "BTS"
OUTPUT_DIR = ROOT / "02_DATA_PROFILING" / "01_BTS"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Các trường cần kiểm tra
columns_to_check = [
    "FlightDate",
    "Reporting_Airline",
    "Tail_Number",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "Dest",
    "CRSDepTime",
    "DepTime",
    "DepDelay",
    "DepDelayMinutes",
    "CRSArrTime",
    "ArrTime",
    "ArrDelay",
    "ArrDelayMinutes",
    "ArrDel15",
    "Cancelled",
    "CancellationCode",
    "Diverted",
    "DivReachedDest",
    "DivAirportLandings",
    "Div1Airport",
    "Div1AirportID",
    "Div1AirportSeqID"
]

files = sorted(BTS_DIR.rglob("BTS_2025_*.csv"))

if not files:
    raise FileNotFoundError(f"No BTS monthly CSV files found in {BTS_DIR}")

results = []

for file in files:
    print(f"Processing {file.name}...")

    for chunk in pd.read_csv(
        file,
        dtype=str,
        keep_default_na=False,
        usecols=lambda col: col in columns_to_check,
        chunksize=100_000,
        low_memory=False
    ):
        # Chỉ lấy bản ghi có ArrDel15 rỗng
        mask = chunk["ArrDel15"].str.strip().eq("")

        if mask.any():
            results.append(chunk.loc[mask])

if results:
    anomalies = pd.concat(results, ignore_index=True)

    output_file = OUTPUT_DIR / "08_bts_missing_arrdel15_records.csv"
    anomalies.to_csv(output_file, index=False, encoding="utf-8-sig")

    print("\n========== ANOMALY RECORDS ==========")
    print(anomalies.to_string(index=False))

    print(f"\nTotal records: {len(anomalies)}")
    print(f"Saved to: {output_file}")

else:
    print("No records with missing ArrDel15 found.")