
from pathlib import Path
from collections import Counter

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

BTS_DIR = ROOT / "01_RAW_DATA" / "BTS"
OUTPUT_DIR = ROOT / "02_DATA_PROFILING" / "01_BTS"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Các trường cần dùng
columns = [
    "FlightDate",
    "Reporting_Airline",
    "Tail_Number",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "Dest",
    "DepTime",
    "WheelsOff",
    "ArrTime",
    "ArrDelay",
    "ArrDelayMinutes",
    "ArrDel15",
    "Cancelled",
    "CancellationCode",
    "Diverted",
    "DivAirportLandings",
    "DivReachedDest",
    "DivActualElapsedTime",
    "DivArrDelay",
    "DivDistance",
    "Div1Airport",
    "Div1WheelsOn",
    "Div1TotalGTime",
    "Div1LongestGTime",
    "Div1WheelsOff",
    "Div1TailNum",
    "Div2Airport",
    "Div3Airport",
    "Div4Airport",
    "Div5Airport",
]

files = sorted(BTS_DIR.rglob("BTS_2025_*.csv"))

if len(files) != 12:
    raise FileNotFoundError(
        f"Expected 12 files, found {len(files)}"
    )

total = Counter()
monthly_results = []
candidate_parts = []

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

        diverted = pd.to_numeric(
            chunk["Diverted"],
            errors="coerce"
        )

        origin = chunk["Origin"].str.strip()
        div1_airport = chunk["Div1Airport"].str.strip()
        div1_wheels_on = chunk["Div1WheelsOn"].str.strip()

        # Các điều kiện về số lần hạ cánh chuyển hướng
        landing_blank = landings_raw.eq("")
        landing_zero = landings.eq(0)
        landing_1_to_5 = landings.between(1, 5)
        landing_9 = landings.eq(9)

        landing_other = (
            ~landing_blank
            & landings.isna()
        ) | (
            landings.notna()
            & ~(
                landing_zero
                | landing_1_to_5
                | landing_9
            )
        )

        # Kiểm tra sự tồn tại của sân bay chuyển hướng
        div1_present = div1_airport.ne("")

        all_div_airports = [
            "Div1Airport",
            "Div2Airport",
            "Div3Airport",
            "Div4Airport",
            "Div5Airport",
        ]

        any_div_airport = (
            chunk[all_div_airports]
            .apply(lambda s: s.str.strip().ne(""))
            .any(axis=1)
        )

        # Kiểm tra trường hợp air return:
        # Cancelled = 0, Diverted = 0,
        # Div1Airport trùng Origin và có WheelsOn
        air_return_candidate = (
            cancelled.eq(0)
            & diverted.eq(0)
            & origin.ne("")
            & origin.eq(div1_airport)
            & div1_wheels_on.ne("")
        )

        # Theo BTS, giá trị 9 cần tương ứng với chuyến bay cuối cùng bị hủy
        landing_9_cancelled_0 = (
            landing_9 & cancelled.eq(0)
        )

        landing_9_cancelled_1 = (
            landing_9 & cancelled.eq(1)
        )

        # Có Div1Airport nhưng thiếu số lần hạ cánh
        div1_without_landing_count = (
            div1_present & landing_blank
        )

        # DivAirportLandings từ 1-5 nhưng Diverted = 0
        positive_landing_diverted_0 = (
            landing_1_to_5 & diverted.eq(0)
        )

        # Diverted = 1 nhưng thiếu cả số lần hạ cánh
        # lẫn thông tin sân bay chuyển hướng
        diverted_without_details = (
            diverted.eq(1)
            & (landing_blank | landing_zero)
            & ~any_div_airport
        )

        metrics = {
            "Total_Rows": len(chunk),
            "DivAirportLandings_Blank": int(landing_blank.sum()),
            "DivAirportLandings_0": int(landing_zero.sum()),
            "DivAirportLandings_1_to_5": int(landing_1_to_5.sum()),
            "DivAirportLandings_9": int(landing_9.sum()),
            "DivAirportLandings_Other": int(landing_other.sum()),

            "Landing_9_Cancelled_0": int(
                landing_9_cancelled_0.sum()
            ),

            "Landing_9_Cancelled_1": int(
                landing_9_cancelled_1.sum()
            ),

            "Landing_1_to_5_Diverted_0": int(
                positive_landing_diverted_0.sum()
            ),

            "Div1Airport_Present_Landing_Blank": int(
                div1_without_landing_count.sum()
            ),

            "Potential_Air_Return_Flags_0": int(
                air_return_candidate.sum()
            ),

            "Diverted_1_Without_Details": int(
                diverted_without_details.sum()
            ),
        }

        month_stats.update(metrics)
        total.update(metrics)

        # Chỉ xuất bản ghi cần xem xét
        candidate_mask = (
            air_return_candidate
            | landing_9_cancelled_0
            | div1_without_landing_count
            | positive_landing_diverted_0
        )

        if candidate_mask.any():

            selected = chunk.loc[
                candidate_mask
            ].copy()

            selected["SourceFile"] = file.name

            selected["Audit_AirReturn_Flags_0"] = (
                air_return_candidate.loc[candidate_mask]
            )

            selected["Audit_Landing9_Cancelled0"] = (
                landing_9_cancelled_0.loc[candidate_mask]
            )

            selected[
                "Audit_Div1Airport_LandingBlank"
            ] = div1_without_landing_count.loc[
                candidate_mask
            ]

            selected[
                "Audit_Landing1to5_Diverted0"
            ] = positive_landing_diverted_0.loc[
                candidate_mask
            ]

            candidate_parts.append(selected)

    monthly_results.append({
        "Month": file.stem[-2:],
        **dict(month_stats)
    })

    print(f"Completed: {file.name}")

# Xuất báo cáo theo tháng
monthly_df = pd.DataFrame(monthly_results)

monthly_file = (
    OUTPUT_DIR
    / "12_bts_air_return_rule_summary.csv"
)

monthly_df.to_csv(
    monthly_file,
    index=False,
    encoding="utf-8-sig"
)

# Xuất các bản ghi cần xác minh
candidate_file = (
    OUTPUT_DIR
    / "13_bts_air_return_rule_candidates.csv"
)

if candidate_parts:
    candidate_df = pd.concat(
        candidate_parts,
        ignore_index=True
    ).drop_duplicates()

    candidate_df.to_csv(
        candidate_file,
        index=False,
        encoding="utf-8-sig"
    )

else:
    candidate_df = pd.DataFrame()

    candidate_df.to_csv(
        candidate_file,
        index=False,
        encoding="utf-8-sig"
    )

print("\n========== AIR RETURN RULE AUDIT ==========")

for key, value in total.items():
    print(f"{key}: {value}")

print("\n========== CANDIDATE RECORDS ==========")
print(f"Total candidates: {len(candidate_df)}")

print(f"\nMonthly report: {monthly_file}")
print(f"Candidate report: {candidate_file}")
