
import pandas as pd
from pathlib import Path
import re

# =========================================
# 1. CONFIG
# =========================================

ROOT_DIR = Path.cwd()

BTS_DIR = ROOT_DIR / "01_RAW_DATA" / "BTS"

OUTPUT_DIR = (
    ROOT_DIR
    / "02_DATA_PROFILING"
    / "01_BTS"
)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE = 100_000

pattern = re.compile(
    r"BTS_2025_(\d{1,2})\.csv$",
    re.IGNORECASE
)

files = []

for file in BTS_DIR.rglob("*.csv"):

    match = pattern.match(file.name)

    if match:
        files.append(
            (int(match.group(1)), file)
        )

files.sort(key=lambda x: x[0])

if len(files) != 12:
    raise ValueError(
        f"Expected 12 files, found {len(files)}"
    )

# =========================================
# 2. COLUMNS TO ANALYZE
# =========================================

columns = [
    "Cancelled",
    "Diverted",
    "ArrDel15",
    "DepTime",
    "DepDelayMinutes",
    "ArrTime",
    "ArrDelayMinutes",
    "CancellationCode",
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay",
    "DivReachedDest"
]

cause_columns = [
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay"
]

WATCH_COLUMNS = [
    "DepTime",
    "DepDelayMinutes",
    "ArrTime",
    "ArrDelayMinutes",
    "CancellationCode",
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay",
    "DivReachedDest"
]

# =========================================
# 3. INITIALIZE COUNTERS
# =========================================

group_results = []

checks = {
    "Total_Rows": 0,
    "Cancelled_Rows": 0,
    "Diverted_Rows": 0,
    "Cancelled_And_Diverted": 0,

    "Cancelled_Missing_CancellationCode": 0,
    "NotCancelled_Has_CancellationCode": 0,

    "ArrDel15_1_Rows": 0,
    "ArrDel15_1_All_Causes_Missing": 0,
    "ArrDel15_1_Any_Cause_Present": 0,

    "ArrDel15_0_Rows": 0,
    "ArrDel15_0_Any_Cause_Present": 0,

    "Diverted_Missing_DivReachedDest": 0,
    "NotDiverted_Has_DivReachedDest": 0
}

# =========================================
# 4. PROCESS EACH BTS FILE
# =========================================

for month, file in files:

    print(f"\nProcessing {file.name}")

    for chunk in pd.read_csv(
        file,
        usecols=columns,
        dtype="string",
        chunksize=CHUNK_SIZE,
        keep_default_na=False,
        low_memory=False
    ):

        # Convert flags to numeric
        cancelled = pd.to_numeric(
            chunk["Cancelled"],
            errors="coerce"
        )

        diverted = pd.to_numeric(
            chunk["Diverted"],
            errors="coerce"
        )

        arr15 = pd.to_numeric(
            chunk["ArrDel15"],
            errors="coerce"
        )

        # ---------------------------------
        # DEFINE FLIGHT STATUS
        # ---------------------------------

        status = pd.Series(
            "Normal",
            index=chunk.index
        )

        status.loc[cancelled.eq(1)] = "Cancelled"

        status.loc[diverted.eq(1)] = "Diverted"

        status.loc[
            cancelled.eq(1) & diverted.eq(1)
        ] = "Both"

        # ---------------------------------
        # DEFINE ARRIVAL DELAY STATUS
        # ---------------------------------

        arr_status = pd.Series(
            "Other",
            index=chunk.index
        )

        arr_status.loc[
            chunk["ArrDel15"].str.strip().eq("")
        ] = "Blank"

        arr_status.loc[arr15.eq(0)] = "0"

        arr_status.loc[arr15.eq(1)] = "1"

        # ---------------------------------
        # CALCULATE MISSING FLAGS
        # ---------------------------------

        temp = pd.DataFrame({
            "Flight_Status": status,
            "ArrDel15_Status": arr_status
        })

        for col in WATCH_COLUMNS:

            temp[f"Missing_{col}"] = (
                chunk[col]
                .str.strip()
                .eq("")
                .astype("int8")
            )

        # ---------------------------------
        # GROUP STATISTICS
        # ---------------------------------

        grouped = (
            temp
            .groupby(
                ["Flight_Status", "ArrDel15_Status"]
            )
            .sum(numeric_only=True)
        )

        counts = (
            temp
            .groupby(
                ["Flight_Status", "ArrDel15_Status"]
            )
            .size()
            .rename("Row_Count")
            .to_frame()
        )

        grouped = counts.join(grouped)

        for index, row in grouped.iterrows():

            flight_status, arr_state = index

            record = {
                "Flight_Status": flight_status,
                "ArrDel15_Status": arr_state,
                "Row_Count": int(row["Row_Count"])
            }

            for col in WATCH_COLUMNS:
                record[f"Missing_{col}"] = int(
                    row[f"Missing_{col}"]
                )

            group_results.append(record)

        # ---------------------------------
        # RULE CHECKS
        # ---------------------------------

        blank = {
            col: chunk[col].str.strip().eq("")
            for col in WATCH_COLUMNS
        }

        cause_any_present = (
            ~chunk[cause_columns]
            .apply(lambda s: s.str.strip().eq(""))
            .all(axis=1)
        )

        cause_all_missing = (
            chunk[cause_columns]
            .apply(lambda s: s.str.strip().eq(""))
            .all(axis=1)
        )

        checks["Total_Rows"] += len(chunk)

        checks["Cancelled_Rows"] += int(
            cancelled.eq(1).sum()
        )

        checks["Diverted_Rows"] += int(
            diverted.eq(1).sum()
        )

        checks["Cancelled_And_Diverted"] += int(
            (cancelled.eq(1) & diverted.eq(1)).sum()
        )

        checks["Cancelled_Missing_CancellationCode"] += int(
            (cancelled.eq(1) & blank["CancellationCode"]).sum()
        )

        checks["NotCancelled_Has_CancellationCode"] += int(
            (cancelled.eq(0) & ~blank["CancellationCode"]).sum()
        )

        checks["ArrDel15_1_Rows"] += int(
            arr15.eq(1).sum()
        )

        checks["ArrDel15_1_All_Causes_Missing"] += int(
            (arr15.eq(1) & cause_all_missing).sum()
        )

        checks["ArrDel15_1_Any_Cause_Present"] += int(
            (arr15.eq(1) & cause_any_present).sum()
        )

        checks["ArrDel15_0_Rows"] += int(
            arr15.eq(0).sum()
        )

        checks["ArrDel15_0_Any_Cause_Present"] += int(
            (arr15.eq(0) & cause_any_present).sum()
        )
        checks["Diverted_Missing_DivReachedDest"] += int(
            (diverted.eq(1) & blank["DivReachedDest"]).sum()
        )

        checks["NotDiverted_Has_DivReachedDest"] += int(
            (diverted.eq(0) & ~blank["DivReachedDest"]).sum()
        )

    print("Completed month:", month)

# =========================================
# 5. CONSOLIDATE REPORTS
# =========================================

group_report = pd.DataFrame(group_results)

group_report = (
    group_report
    .groupby(
        ["Flight_Status", "ArrDel15_Status"],
        as_index=False
    )
    .sum(numeric_only=True)
)

group_report.to_csv(
    OUTPUT_DIR / "06_missing_by_flight_status.csv",
    index=False,
    encoding="utf-8-sig"
)

check_report = pd.DataFrame(
    [
        {
            "Check": key,
            "Count": value
        }
        for key, value in checks.items()
    ]
)

check_report.to_csv(
    OUTPUT_DIR / "07_business_rule_checks.csv",
    index=False,
    encoding="utf-8-sig"
)

# =========================================
# 6. DISPLAY RESULTS
# =========================================

print("\n========== FLIGHT STATUS PROFILE ==========")

print(
    group_report.to_string(index=False)
)

print("\n========== BUSINESS RULE CHECKS ==========")

print(
    check_report.to_string(index=False)
)

print("\nReports saved at:")
print(OUTPUT_DIR)
