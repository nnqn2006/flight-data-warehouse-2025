
from pathlib import Path
from collections import Counter

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

BTS_DIR = ROOT / "01_RAW_DATA" / "BTS"
OUTPUT_DIR = ROOT / "02_DATA_PROFILING" / "01_BTS"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

files = sorted(BTS_DIR.rglob("BTS_2025_*.csv"))

if len(files) != 12:
    raise FileNotFoundError(
        f"Expected 12 BTS monthly files, found {len(files)}"
    )

# Các trường cần xuất để kiểm tra
export_columns = [
    "FlightDate",
    "Reporting_Airline",
    "Tail_Number",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "Dest",
    "DepTime",
    "WheelsOff",
    "ArrTime",
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

# Tổng hợp toàn bộ 12 tháng
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
        chunksize=100_000,
        low_memory=False
    ):

        # Chuyển các cờ sang dạng số để xử lý
        cancelled = pd.to_numeric(
            chunk["Cancelled"], errors="coerce"
        )

        diverted = pd.to_numeric(
            chunk["Diverted"], errors="coerce"
        )

        # Tìm tất cả trường thông tin liên quan đến diversion
        # Không bao gồm chính cột Diverted
        div_columns = [
            col for col in chunk.columns
            if col.startswith("Div") and col != "Diverted"
        ]

        # True nếu ít nhất một trường chi tiết diversion có giá trị
        div_details_present = (
            chunk[div_columns]
            .apply(lambda s: s.str.strip().ne(""))
            .any(axis=1)
        )

        origin = chunk["Origin"].str.strip()
        div1_airport = chunk["Div1Airport"].str.strip()

        # Các điều kiện kiểm tra
        diverted_0 = diverted.eq(0)
        diverted_1 = diverted.eq(1)

        cancelled_0 = cancelled.eq(0)

        arrtime_blank = chunk["ArrTime"].str.strip().eq("")
        div1_airport_present = div1_airport.ne("")

        origin_equals_div1 = (
            div1_airport_present
            & origin.eq(div1_airport)
        )

        # 1. Flag Diverted = 0 nhưng có thông tin diversion
        flag0_with_details = (
            diverted_0 & div_details_present
        )

        # 2. Flag Diverted = 1 nhưng không có thông tin diversion
        flag1_without_details = (
            diverted_1 & ~div_details_present
        )

        # 3. Chưa bị hủy, không bị đánh dấu chuyển hướng,
        # nhưng thiếu giờ đến
        missing_arrival_in_normal_flags = (
            cancelled_0
            & diverted_0
            & arrtime_blank
        )

        # 4. Có thông tin Div1Airport nhưng thiếu số lần hạ cánh
        landing_count_blank = (
            div1_airport_present
            & chunk["DivAirportLandings"].str.strip().eq("")
        )

        # 5. Dấu hiệu air return:
        # Diverted = 0, Cancelled = 0,
        # sân bay Div1 trùng Origin
        air_return_signature = (
            cancelled_0
            & diverted_0
            & origin_equals_div1
            & chunk["Div1WheelsOn"].str.strip().ne("")
        )

        # Cộng dồn số lượng
        metrics = {
            "Total_Rows": len(chunk),
            "Rows_With_Div_Details": int(
                div_details_present.sum()
            ),
            "Diverted_0_With_Div_Details": int(
                flag0_with_details.sum()
            ),
            "Diverted_1_Without_Div_Details": int(
                flag1_without_details.sum()
            ),
            "NotCancelled_NotDiverted_ArrTime_Blank": int(
                missing_arrival_in_normal_flags.sum()
            ),
            "Div1Airport_Present_LandingCount_Blank": int(
                landing_count_blank.sum()
            ),
            "Potential_Air_Return_With_Flags_0": int(
                air_return_signature.sum()
            ),
        }

        month_stats.update(metrics)
        total.update(metrics)

        # Lưu các bản ghi cần kiểm tra
        candidate_mask = (
            flag0_with_details
            | missing_arrival_in_normal_flags
        )

        if candidate_mask.any():
            available_columns = [
                col for col in export_columns
                if col in chunk.columns
            ]

            candidates = chunk.loc[
                candidate_mask, available_columns
            ].copy()

            candidates["SourceFile"] = file.name

            candidates["Audit_Diverted0_With_DivDetails"] = (
                flag0_with_details.loc[candidate_mask]
            )

            candidates[
                "Audit_ArrTimeBlank_NotCancelled_NotDiverted"
            ] = missing_arrival_in_normal_flags.loc[
                candidate_mask
            ]

            candidates["Audit_Origin_Equals_Div1Airport"] = (
                origin_equals_div1.loc[candidate_mask]
            )

            candidate_parts.append(candidates)

    monthly_results.append({
        "Month": file.stem[-2:],
        **dict(month_stats)
    })

    print(f"Completed: {file.name}")

# Xuất báo cáo tổng hợp theo tháng
monthly_df = pd.DataFrame(monthly_results)

monthly_file = (
    OUTPUT_DIR / "10_bts_diversion_consistency_by_month.csv"
)

monthly_df.to_csv(
    monthly_file,
    index=False,
    encoding="utf-8-sig"
)

# Xuất danh sách bản ghi cần điều tra
candidate_file = (
    OUTPUT_DIR / "11_bts_diversion_consistency_candidates.csv"
)

if candidate_parts:
    candidate_df = pd.concat(
        candidate_parts,
        ignore_index=True
    )

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

# In kết quả
print("\n========== DIVERSION CONSISTENCY AUDIT ==========")

for key, value in total.items():
    print(f"{key}: {value}")

print("\n========== CANDIDATE RECORDS ==========")
print(f"Total candidates: {len(candidate_df)}")

print(f"\nMonthly report: {monthly_file}")
print(f"Candidate report: {candidate_file}")
