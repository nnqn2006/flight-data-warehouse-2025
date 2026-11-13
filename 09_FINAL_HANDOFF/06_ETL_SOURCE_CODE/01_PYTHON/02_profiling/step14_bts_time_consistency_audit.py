
from pathlib import Path
from collections import Counter

import pandas as pd
import numpy as np


# ============================================================
# 1. PROJECT CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parents[2]

BTS_DIR = ROOT / "01_RAW_DATA" / "BTS"
OUTPUT_DIR = ROOT / "02_DATA_PROFILING" / "01_BTS"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# 2. REQUIRED COLUMNS
# ============================================================

columns = [
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

    "WheelsOff",
    "WheelsOn",

    "TaxiOut",
    "TaxiIn",

    "CRSArrTime",
    "ArrTime",
    "ArrDelay",
    "ArrDelayMinutes",

    "CRSElapsedTime",
    "ActualElapsedTime",
    "AirTime",
    "Distance",

    "Cancelled",
    "CancellationCode",
    "Diverted",

    "DivAirportLandings",
    "DivReachedDest",

    "DivActualElapsedTime",
    "DivArrDelay",

    "Div1Airport",
    "Div1WheelsOn",
    "Div1TotalGTime",
    "Div1LongestGTime",
    "Div1WheelsOff",
    "Div1TailNum",
]


# ============================================================
# 3. HELPER FUNCTIONS
# ============================================================

def hhmm_to_minutes(series):

    """
    Convert HHMM values into minutes after midnight.

    Example:
        0712 -> 432 minutes
        2359 -> 1439 minutes
        2400 -> 0 minutes

    Invalid HHMM values become NaN.
    """

    raw = series.astype(str).str.strip()

    values = pd.to_numeric(
        raw.replace("", np.nan),
        errors="coerce"
    )

    hours = np.floor(values / 100)
    minutes = values % 100

    valid = (
        (
            hours.between(0, 23)
            & minutes.between(0, 59)
        )
        | values.eq(2400)
    )

    result = hours * 60 + minutes

    result.loc[values.eq(2400)] = 0

    return result.where(valid)


def modulo_residual(reported_value, clock_difference):

    """
    Check whether reported duration/delay agrees with
    the difference between HHMM clock values.

    Differences of 1440 minutes (24 hours) are allowed.

    The returned residual is between -720 and +720.
    A residual close to zero indicates consistency.
    """

    difference = reported_value - clock_difference

    return ((difference + 720) % 1440) - 720


def to_numeric(series):

    return pd.to_numeric(
        series.astype(str).str.strip(),
        errors="coerce"
    )


# ============================================================
# 4. FIND SOURCE FILES
# ============================================================

files = sorted(
    BTS_DIR.rglob("BTS_2025_*.csv")
)

if len(files) != 12:
    raise FileNotFoundError(
        f"Expected 12 BTS files, found {len(files)}"
    )

print(f"Found {len(files)} BTS files.")


# ============================================================
# 5. INITIALIZE AUDIT RESULTS
# ============================================================

metric_names = [
    "DepDelay_ClockMismatch",
    "ArrDelay_ClockMismatch",
    "TaxiOut_ClockMismatch",
    "TaxiIn_ClockMismatch",

    "Negative_CRSElapsedTime",
    "CRSElapsedTime_Above_1000",

    "TaxiOut_AtLeast_180",
    "TaxiIn_AtLeast_180",

    "LongActualElapsed_ShortDistance",
    "LongAirTime_ShortDistance",

    "Landing9_Cancelled0",
]

monthly_results = []
candidate_parts = []


# ============================================================
# 6. PROCESS ALL 12 MONTHS
# ============================================================

for file in files:

    print(f"\nProcessing {file.name}...")

    month_counter = Counter()

    for chunk in pd.read_csv(
        file,
        dtype=str,
        keep_default_na=False,
        usecols=lambda col: col in columns,
        chunksize=100_000,
        low_memory=False
    ):

        # ----------------------------------------------------
        # Validate required fields
        # ----------------------------------------------------

        missing_columns = (
            set(columns) - set(chunk.columns)
        )

        if missing_columns:
            raise ValueError(
                f"Missing columns in {file.name}: "
                f"{sorted(missing_columns)}"
            )

        # ----------------------------------------------------
        # Convert clock fields
        # ----------------------------------------------------

        crs_dep = hhmm_to_minutes(
            chunk["CRSDepTime"]
        )

        dep = hhmm_to_minutes(
            chunk["DepTime"]
        )

        wheels_off = hhmm_to_minutes(
            chunk["WheelsOff"]
        )

        wheels_on = hhmm_to_minutes(
            chunk["WheelsOn"]
        )

        crs_arr = hhmm_to_minutes(
            chunk["CRSArrTime"]
        )

        arr = hhmm_to_minutes(
            chunk["ArrTime"]
        )

        # ----------------------------------------------------
        # Convert numeric fields
        # ----------------------------------------------------

        dep_delay = to_numeric(
            chunk["DepDelay"]
        )

        arr_delay = to_numeric(
            chunk["ArrDelay"]
        )

        taxi_out = to_numeric(
            chunk["TaxiOut"]
        )

        taxi_in = to_numeric(
            chunk["TaxiIn"]
        )

        crs_elapsed = to_numeric(
            chunk["CRSElapsedTime"]
        )

        actual_elapsed = to_numeric(
            chunk["ActualElapsedTime"]
        )

        air_time = to_numeric(
            chunk["AirTime"]
        )

        distance = to_numeric(
            chunk["Distance"]
        )

        cancelled = to_numeric(
            chunk["Cancelled"]
        )

        landings = to_numeric(
            chunk["DivAirportLandings"]
        )

        # ----------------------------------------------------
        # A. DEPARTURE DELAY CONSISTENCY
        # ----------------------------------------------------

        dep_clock_difference = dep - crs_dep

        dep_residual = modulo_residual(
            dep_delay,
            dep_clock_difference
        )

        dep_valid = (
            dep_delay.notna()
            & dep_clock_difference.notna()
        )

        dep_mismatch = (
            dep_valid
            & dep_residual.abs().gt(1)
        )

        # ----------------------------------------------------
        # B. ARRIVAL DELAY CONSISTENCY
        # ----------------------------------------------------

        arr_clock_difference = arr - crs_arr

        arr_residual = modulo_residual(
            arr_delay,
            arr_clock_difference
        )

        arr_valid = (
            arr_delay.notna()
            & arr_clock_difference.notna()
        )

        arr_mismatch = (
            arr_valid
            & arr_residual.abs().gt(1)
        )

        # ----------------------------------------------------
        # C. TAXI OUT CONSISTENCY
        # ----------------------------------------------------

        taxi_out_clock_difference = (
            wheels_off - dep
        )

        taxi_out_residual = modulo_residual(
            taxi_out,
            taxi_out_clock_difference
        )

        taxi_out_valid = (
            taxi_out.notna()
            & taxi_out_clock_difference.notna()
        )

        taxi_out_mismatch = (
            taxi_out_valid
            & taxi_out_residual.abs().gt(1)
        )

        # ----------------------------------------------------
        # D. TAXI IN CONSISTENCY
        # ----------------------------------------------------

        taxi_in_clock_difference = (
            arr - wheels_on
        )

        taxi_in_residual = modulo_residual(
            taxi_in,
            taxi_in_clock_difference
        )

        taxi_in_valid = (
            taxi_in.notna()
            & taxi_in_clock_difference.notna()
        )

        taxi_in_mismatch = (
            taxi_in_valid
            & taxi_in_residual.abs().gt(1)
        )

        # ----------------------------------------------------
        # E. ELAPSED TIME DOMAIN CHECKS
        # ----------------------------------------------------

        negative_crs_elapsed = (
            crs_elapsed.notna()
            & crs_elapsed.lt(0)
        )

        crs_elapsed_above_1000 = (
            crs_elapsed.notna()
            & crs_elapsed.gt(1000)
        )

        # ----------------------------------------------------
        # F. EXTREME TAXI TIME
        # ----------------------------------------------------

        taxi_out_extreme = (
            taxi_out.notna()
            & taxi_out.ge(180)
        )

        taxi_in_extreme = (
            taxi_in.notna()
            & taxi_in.ge(180)
        )

        # ----------------------------------------------------
        # G. EXTREME DURATION ON SHORT ROUTES
        # ----------------------------------------------------

        long_actual_short_route = (
            actual_elapsed.ge(600)
            & distance.lt(1000)
            & actual_elapsed.notna()
            & distance.notna()
        )

        long_air_short_route = (
            air_time.ge(600)
            & distance.lt(1000)
            & air_time.notna()
            & distance.notna()
        )

        # ----------------------------------------------------
        # H. AIR RETURN STATUS CHECK
        # ----------------------------------------------------

        landing9_cancelled0 = (
            landings.eq(9)
            & cancelled.eq(0)
        )

        # ----------------------------------------------------
        # I. COLLECT METRICS
        # ----------------------------------------------------

        flags = {
            "DepDelay_ClockMismatch": dep_mismatch,
            "ArrDelay_ClockMismatch": arr_mismatch,
            "TaxiOut_ClockMismatch": taxi_out_mismatch,
            "TaxiIn_ClockMismatch": taxi_in_mismatch,

            "Negative_CRSElapsedTime": negative_crs_elapsed,
            "CRSElapsedTime_Above_1000": crs_elapsed_above_1000,

            "TaxiOut_AtLeast_180": taxi_out_extreme,
            "TaxiIn_AtLeast_180": taxi_in_extreme,

            "LongActualElapsed_ShortDistance": long_actual_short_route,
            "LongAirTime_ShortDistance": long_air_short_route,

            "Landing9_Cancelled0": landing9_cancelled0,
        }

        for name, mask in flags.items():

            month_counter[name] += int(
                mask.sum()
            )

        # ----------------------------------------------------
        # J. BUILD CANDIDATE MASK
        # ----------------------------------------------------

        candidate_mask = pd.Series(
            False,
            index=chunk.index
        )

        for mask in flags.values():
            candidate_mask |= mask

        # ----------------------------------------------------
        # K. STORE CANDIDATES
        # ----------------------------------------------------

        if candidate_mask.any():

            selected = chunk.loc[
                candidate_mask
            ].copy()

            selected["SourceFile"] = file.name

            selected["DepDelay_ClockResidual"] = (
                dep_residual.loc[candidate_mask]
            )

            selected["ArrDelay_ClockResidual"] = (
                arr_residual.loc[candidate_mask]
            )

            selected["TaxiOut_ClockResidual"] = (
                taxi_out_residual.loc[candidate_mask]
            )

            selected["TaxiIn_ClockResidual"] = (
                taxi_in_residual.loc[candidate_mask]
            )

            for name, mask in flags.items():

                selected[name] = (
                    mask.loc[candidate_mask].to_numpy()
                )

            candidate_parts.append(selected)

    # --------------------------------------------------------
    # MONTHLY SUMMARY
    # --------------------------------------------------------

    monthly_results.append({
        "Month": file.stem[-2:],
        **dict(month_counter)
    })

    print(f"Completed: {file.name}")


# ============================================================
# 7. EXPORT MONTHLY AUDIT SUMMARY
# ============================================================

monthly_df = pd.DataFrame(
    monthly_results
)

summary_file = (
    OUTPUT_DIR
    / "20_bts_time_consistency_by_month.csv"
)

monthly_df.to_csv(
    summary_file,
    index=False,
    encoding="utf-8-sig"
)


# ============================================================
# 8. EXPORT CANDIDATE RECORDS
# ============================================================

candidate_file = (
    OUTPUT_DIR
    / "21_bts_time_consistency_candidates.csv"
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


# ============================================================
# 9. FINAL REPORT
# ============================================================

print("\n========== TIME CONSISTENCY AUDIT ==========")

print(monthly_df.to_string(index=False))

print("\n========== ANNUAL TOTAL ==========")

for metric in metric_names:

    count = int(
        monthly_df[metric].sum()
    )

    print(f"{metric}: {count}")

print("\n========== CANDIDATE RECORDS ==========")

print(
    f"Total candidate rows: {len(candidate_df)}"
)

print(f"\nMonthly report: {summary_file}")
print(f"Candidate report: {candidate_file}")

print("\nNo source BTS files were modified.")
