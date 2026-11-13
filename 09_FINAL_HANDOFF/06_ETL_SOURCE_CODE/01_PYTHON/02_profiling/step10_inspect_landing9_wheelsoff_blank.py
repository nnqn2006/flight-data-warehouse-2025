
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

BTS_DIR = ROOT / "01_RAW_DATA" / "BTS"
OUTPUT_DIR = ROOT / "02_DATA_PROFILING" / "01_BTS"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

files = sorted(BTS_DIR.rglob("BTS_2025_*.csv"))

results = []

for file in files:
    print(f"Processing {file.name}...")

    for chunk in pd.read_csv(
        file,
        dtype=str,
        keep_default_na=False,
        chunksize=100_000,
        low_memory=False
    ):

        cancelled = pd.to_numeric(
            chunk["Cancelled"],
            errors="coerce"
        )

        mask = (
            chunk["DivAirportLandings"].str.strip().eq("9")
            & chunk["WheelsOff"].str.strip().eq("")
            & cancelled.eq(1)
        )

        if mask.any():
            matched = chunk.loc[mask].copy()
            results.append(matched)

if results:
    full_records = pd.concat(
        results,
        ignore_index=True
    )

    output_file = (
        OUTPUT_DIR
        / "15_bts_landing9_missing_wheelsoff.csv"
    )

    full_records.to_csv(
        output_file,
        index=False,
        encoding="utf-8-sig"
    )

    print("\n========== FULL RECORD INSPECTION ==========")
    print(f"Number of matching records: {len(full_records)}")

    for idx, row in full_records.iterrows():

        print(f"\n--- Record {idx + 1} ---")

        for col, value in row.items():
            if str(value).strip() != "":
                print(f"{col}: {value}")

    print(f"\nSaved to: {output_file}")

else:
    print("No matching record found.")
