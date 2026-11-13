
from pathlib import Path
import pandas as pd


# ============================================================
# 1. PROJECT CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parents[2]

BTS_DIR = ROOT / "01_RAW_DATA" / "BTS"

OUTPUT_DIR = ROOT / "02_DATA_PROFILING" / "01_BTS"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TOP_N = 10


# ============================================================
# 2. NUMERIC FIELDS TO INSPECT
# ============================================================

numeric_fields = [
    "DepDelay",
    "DepDelayMinutes",
    "ArrDelay",
    "ArrDelayMinutes",
    "TaxiOut",
    "TaxiIn",
    "CRSElapsedTime",
    "ActualElapsedTime",
    "AirTime",
    "Distance",
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay",
]


# ============================================================
# 3. CONTEXT FIELDS
# Keep flight information to investigate extreme records
# ============================================================

context_fields = [
    # Flight identification
    "Year",
    "Quarter",
    "Month",
    "DayofMonth",
    "DayOfWeek",
    "FlightDate",

    # Airline and aircraft
    "Reporting_Airline",
    "DOT_ID_Reporting_Airline",
    "Tail_Number",
    "Flight_Number_Reporting_Airline",

    # Route
    "Origin",
    "Dest",

    # Scheduled and actual departure
    "CRSDepTime",
    "DepTime",
    "DepDelay",
    "DepDelayMinutes",
    "DepDel15",
    "TaxiOut",
    "WheelsOff",

    # Actual arrival
    "WheelsOn",
    "TaxiIn",
    "CRSArrTime",
    "ArrTime",
    "ArrDelay",
    "ArrDelayMinutes",
    "ArrDel15",

    # Flight status
    "Cancelled",
    "CancellationCode",
    "Diverted",

    # Duration and distance
    "CRSElapsedTime",
    "ActualElapsedTime",
    "AirTime",
    "Distance",

    # Delay causes
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay",

    # Diversion information
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
]


# Important:
# Read BOTH context_fields and numeric_fields.
# This prevents KeyError when a numeric field is not
# included in context_fields.

read_columns = set(context_fields) | set(numeric_fields)

# Remove duplicated column names while preserving order
output_columns = list(
    dict.fromkeys(context_fields + numeric_fields)
)


# ============================================================
# 4. FIND BTS FILES
# ============================================================

files = sorted(
    BTS_DIR.rglob("BTS_2025_*.csv")
)

if len(files) != 12:
    raise FileNotFoundError(
        f"Expected 12 BTS monthly CSV files, "
        f"but found {len(files)} in {BTS_DIR}"
    )

print(f"Found {len(files)} BTS monthly files.")


# ============================================================
# 5. INITIALIZE RESULT STORAGE
# ============================================================

# Top N maximum observations for each numeric field
top_records = {
    field: pd.DataFrame()
    for field in numeric_fields
}

# Five lowest negative values for signed delay fields
bottom_fields = [
    "DepDelay",
    "ArrDelay",
]

bottom_records = {
    field: pd.DataFrame()
    for field in bottom_fields
}

# Store all records with negative CRSElapsedTime
negative_crs_parts = []


# ============================================================
# 6. PASS 1: FIND EXTREME VALUES
# ============================================================

print("\n========== PASS 1: FIND EXTREME VALUES ==========")

for file in files:

    print(f"\nProcessing {file.name}...")

    # Validate the source schema before reading chunks
    source_columns = set(
        pd.read_csv(file, nrows=0).columns
    )

    missing_columns = read_columns - source_columns

    if missing_columns:
        raise ValueError(
            f"Missing required columns in {file.name}: "
            f"{sorted(missing_columns)}"
        )

    for chunk in pd.read_csv(
        file,
        dtype=str,
        keep_default_na=False,
        usecols=lambda col: col in read_columns,
        chunksize=100_000,
        low_memory=False
    ):

        # ================================================
        # Validate required numeric fields
        # ================================================

        missing_numeric = (
            set(numeric_fields) - set(chunk.columns)
        )

        if missing_numeric:
            raise ValueError(
                f"Missing numeric columns in {file.name}: "
                f"{sorted(missing_numeric)}"
            )

        # ================================================
        # 6.1. TOP N MAXIMUM VALUES
        # ================================================

        for field in numeric_fields:

            values = pd.to_numeric(
                chunk[field].str.strip(),
                errors="coerce"
            )

            valid_mask = values.notna()

            if not valid_mask.any():
                continue

            candidates = chunk.loc[
                valid_mask,
                output_columns
            ].copy()

            candidates["SourceFile"] = file.name
            candidates["Extreme_Field"] = field
            candidates["Extreme_Direction"] = "HIGH"

            candidates["Extreme_Value"] = (
                values.loc[valid_mask].to_numpy(
                    dtype="float64"
                )
            )

            combined = pd.concat(
                [
                    top_records[field],
                    candidates
                ],
                ignore_index=True
            )

            top_records[field] = (
                combined
                .nlargest(TOP_N, "Extreme_Value")
                .reset_index(drop=True)
            )

        # ================================================
        # 6.2. LOWEST NEGATIVE DELAY VALUES
        # ================================================

        for field in bottom_fields:

            values = pd.to_numeric(
                chunk[field].str.strip(),
                errors="coerce"
            )

            negative_mask = (
                values.notna() & values.lt(0)
            )

            if not negative_mask.any():
                continue

            candidates = chunk.loc[
                negative_mask,
                output_columns
            ].copy()

            candidates["SourceFile"] = file.name
            candidates["Extreme_Field"] = field
            candidates["Extreme_Direction"] = "LOW"

            candidates["Extreme_Value"] = (
                values.loc[negative_mask].to_numpy(
                    dtype="float64"
                )
            )

            combined = pd.concat(
                [
                    bottom_records[field],
                    candidates
                ],
                ignore_index=True
            )

            bottom_records[field] = (
                combined
                .nsmallest(5, "Extreme_Value")
                .reset_index(drop=True)
            )

        # ================================================
        # 6.3. ALL NEGATIVE CRS ELAPSED TIME RECORDS
        # ================================================

        crs_elapsed = pd.to_numeric(
            chunk["CRSElapsedTime"].str.strip(),
            errors="coerce"
        )

        negative_crs_mask = (
            crs_elapsed.notna()
            & crs_elapsed.lt(0)
        )

        if negative_crs_mask.any():

            candidates = chunk.loc[
                negative_crs_mask,
                output_columns
            ].copy()

            candidates["SourceFile"] = file.name

            negative_crs_parts.append(candidates)

    print(f"Completed: {file.name}")


# ============================================================
# 7. EXPORT EXTREME VALUE CANDIDATES
# ============================================================

print("\n========== EXPORT EXTREME VALUE CANDIDATES ==========")

all_candidates = []

# Collect top maximum records
for field, df in top_records.items():

    if not df.empty:
        all_candidates.append(df)

# Collect lowest negative delay records
for field, df in bottom_records.items():

    if not df.empty:
        all_candidates.append(df)


if all_candidates:

    extreme_df = pd.concat(
        all_candidates,
        ignore_index=True
    )

    # Arrange columns for easier inspection
    priority_columns = [
        "FlightDate",
        "Reporting_Airline",
        "Tail_Number",
        "Flight_Number_Reporting_Airline",
        "Origin",
        "Dest",
        "Extreme_Field",
        "Extreme_Direction",
        "Extreme_Value",
        "Cancelled",
        "CancellationCode",
        "Diverted",
        "SourceFile",
    ]

    remaining_columns = [
        col for col in extreme_df.columns
        if col not in priority_columns
    ]

    extreme_df = extreme_df[
        priority_columns + remaining_columns
    ]

    extreme_file = (
        OUTPUT_DIR
        / "18_bts_extreme_value_candidates.csv"
    )

    extreme_df.to_csv(
        extreme_file,
        index=False,
        encoding="utf-8-sig"
    )

    print("\n========== EXTREME VALUE CANDIDATES ==========")

    print(
        extreme_df[
            priority_columns
        ].to_string(index=False)
    )

    print(
        f"\nTotal extreme candidate rows: "
        f"{len(extreme_df)}"
    )

    print(f"Saved to: {extreme_file}")

else:
    print("No extreme value candidates found.")


# ============================================================
# 8. EXPORT ALL NEGATIVE CRS ELAPSED TIME RECORDS
# ============================================================

print(
    "\n========== NEGATIVE CRS ELAPSED TIME INSPECTION =========="
)

negative_file = (
    OUTPUT_DIR
    / "19_bts_negative_crselapsed_records.csv"
)

if negative_crs_parts:

    negative_df = pd.concat(
        negative_crs_parts,
        ignore_index=True
    )

    # Avoid duplicated records if any
    negative_df = negative_df.drop_duplicates()

    negative_df.to_csv(
        negative_file,
        index=False,
        encoding="utf-8-sig"
    )

    print(
        f"Number of negative CRSElapsedTime records: "
        f"{len(negative_df)}"
    )

    print(
        negative_df[
            [
                "FlightDate",
                "Reporting_Airline",
                "Tail_Number",
                "Flight_Number_Reporting_Airline",
                "Origin",
                "Dest",
                "CRSDepTime",
                "CRSArrTime",
                "CRSElapsedTime",
                "Cancelled",
                "CancellationCode",
                "Diverted",
                "SourceFile",
            ]
        ].to_string(index=False)
    )

    print(f"\nSaved to: {negative_file}")

else:

    print("No negative CRSElapsedTime records found.")


# ============================================================
# 9. FINAL STATUS
# ============================================================

print("\n========== STEP 13 COMPLETED ==========")

print("Reports generated:")

print(f"1. {OUTPUT_DIR / '18_bts_extreme_value_candidates.csv'}")

print(f"2. {negative_file}")

print("\nAll source BTS files were read-only.")
print("No source data was modified or deleted.")
