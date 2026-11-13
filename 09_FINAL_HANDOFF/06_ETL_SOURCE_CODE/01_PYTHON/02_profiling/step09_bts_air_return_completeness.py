
from pathlib import Path
from collections import Counter

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

BTS_DIR = ROOT / "01_RAW_DATA" / "BTS"
OUTPUT_DIR = ROOT / "02_DATA_PROFILING" / "01_BTS"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

columns = [
    "FlightDate",
    "Reporting_Airline",
    "Tail_Number",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "Dest",
    "DepTime",
    "WheelsOff",
    "WheelsOn",
    "ArrTime",
    "ArrDelay",
    "ArrDelayMinutes",
    "ArrDel15",
    "ActualElapsedTime",
    "AirTime",
    "Cancelled",
    "CancellationCode",
    "Diverted",
    "DivAirportLandings",
    "DivReachedDest",
    "Div1Airport",
    "Div1WheelsOn",
    "Div1TotalGTime",
    "Div1LongestGTime",
    "Div1WheelsOff",
    "Div1TailNum",
]

files = sorted(BTS_DIR.rglob("BTS_2025_*.csv"))

if len(files) != 12:
    raise FileNotFoundError(
        f"Expected 12 monthly files, found {len(files)}"
    )

total = Counter()
exception_parts = []

def blank_count(df, column, mask):
    return int(
        df.loc[mask, column]
        .str.strip()
        .eq("")
        .sum()
    )

for file in files:

    print(f"Processing {file.name}...")

    month_stats = Counter()

    for chunk in pd.read_csv(
        file,
        dtype=str,
        keep_default_na=False,
        usecols=lambda col: col in columns,
        chunksize=100_000,
        low_memory=False
    ):

        landings_raw = (
            chunk["DivAirportLandings"].str.strip()
        )

        landings = pd.to_numeric(
            landings_raw,
            errors="coerce"
        )

        cancelled = pd.to_numeric(
            chunk["Cancelled"],
            errors="coerce"
        )

        # Nhóm Air Return có mã 9
        landing_9 = landings.eq(9)

        # Nhóm đã bị hủy
        cancelled_1 = cancelled.eq(1)

        # Nhóm chưa bị hủy
        cancelled_0 = cancelled.eq(0)

        group_9_cancelled = landing_9 & cancelled_1
        group_9_not_cancelled = landing_9 & cancelled_0

        # Nhóm có Div1Airport nhưng thiếu DivAirportLandings
        div1_present = (
            chunk["Div1Airport"].str.strip().ne("")
        )

        landing_blank = landings_raw.eq("")

        blank_landing_with_div1 = (
            landing_blank & div1_present
        )

        # Kiểm tra WheelsOff trống
        wheelsoff_blank = (
            chunk["WheelsOff"].str.strip().eq("")
        )

        # Tổng hợp quy tắc
        metrics = {
            "Total_Rows": len(chunk),

            "Cancelled_Flag_1": int(cancelled_1.sum()),

            "WheelsOff_Blank": int(
                wheelsoff_blank.sum()
            ),

            "Landing_9_Total": int(landing_9.sum()),

            "Landing_9_Cancelled_1": int(
                group_9_cancelled.sum()
            ),

            "Landing_9_Cancelled_0": int(
                group_9_not_cancelled.sum()
            ),

            "Landing_9_Cancelled1_ArrTime_Present":
                int(
                    (
                        group_9_cancelled
                        & chunk["ArrTime"].str.strip().ne("")
                    ).sum()
                ),

            "Landing_9_Cancelled1_WheelsOn_Present":
                int(
                    (
                        group_9_cancelled
                        & chunk["WheelsOn"].str.strip().ne("")
                    ).sum()
                ),

            "Landing_9_Cancelled1_Code_Blank":
                blank_count(
                    chunk, "CancellationCode",
                    group_9_cancelled
                ),

            "Landing_9_Cancelled1_Div1Airport_Blank":
                blank_count(
                    chunk, "Div1Airport",
                    group_9_cancelled
                ),

            "Landing_9_Cancelled1_Div1WheelsOn_Blank":
                blank_count(
                    chunk, "Div1WheelsOn",
                    group_9_cancelled
                ),

            "Landing_9_Cancelled1_GroundTime_Blank":
                blank_count(
                    chunk, "Div1TotalGTime",
                    group_9_cancelled
                ),

            "Landing_9_Cancelled1_LongestTime_Blank":
                blank_count(
                    chunk, "Div1LongestGTime",
                    group_9_cancelled
                ),

            "Landing_9_Cancelled0_ArrTime_Present":
                int(
                    (
                        group_9_not_cancelled
                        & chunk["ArrTime"].str.strip().ne("")
                    ).sum()
                ),

            "Landing_Blank_With_Div1Airport":
                int(blank_landing_with_div1.sum()),
        }

        month_stats.update(metrics)
        total.update(metrics)

        # Chỉ lưu 15 trường hợp chưa bị hủy nhưng có mã 9,
        # và các trường hợp thiếu mã landing nhưng có Div1Airport.
        exception_mask = (
            group_9_not_cancelled
            | blank_landing_with_div1
        )

        if exception_mask.any():

            selected = chunk.loc[
                exception_mask
            ].copy()

            selected["SourceFile"] = file.name

            exception_parts.append(selected)

        # Kiểm tra các nhóm cần thiết để đối chiếu công thức
        total["Landing_9_WheelsOff_Blank"] += int(
            (landing_9 & wheelsoff_blank).sum()
        )

    print(f"Completed: {file.name}")

# Đối chiếu tổng số chuyến bay bị hủy
bts_cancelled_formula = (
    total["WheelsOff_Blank"]
    + total["Landing_9_Total"]
)

cancelled_difference = (
    bts_cancelled_formula
    - total["Cancelled_Flag_1"]
)

print("\n========== AIR RETURN COMPLETENESS ==========")

for key, value in total.items():
    print(f"{key}: {value}")

print("\n========== CANCELLATION RECONCILIATION ==========")

print(
    f"Cancelled flag count: "
    f"{total['Cancelled_Flag_1']}"
)

print(
    f"WheelsOff blank + Landing 9: "
    f"{bts_cancelled_formula}"
)

print(
    f"Difference: {cancelled_difference}"
)

if exception_parts:

    exceptions = pd.concat(
        exception_parts,
        ignore_index=True
    )

    output_file = (
        OUTPUT_DIR
        / "14_bts_air_return_exceptions.csv"
    )

    exceptions.to_csv(
        output_file,
        index=False,
        encoding="utf-8-sig"
    )

    print(f"\nException file saved: {output_file}")

else:
    print("\nNo exceptional records found.")
