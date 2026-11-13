
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

BTS_DIR = ROOT / "01_RAW_DATA" / "BTS"
OUTPUT_DIR = ROOT / "02_DATA_PROFILING" / "01_BTS"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Các biến định lượng cần profiling
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

descriptions = {
    "DepDelay": "Departure delay, signed minutes",
    "DepDelayMinutes": "Departure delay minutes, early = 0",
    "ArrDelay": "Arrival delay, signed minutes",
    "ArrDelayMinutes": "Arrival delay minutes, early = 0",
    "TaxiOut": "Taxi out time, minutes",
    "TaxiIn": "Taxi in time, minutes",
    "CRSElapsedTime": "Scheduled elapsed time, minutes",
    "ActualElapsedTime": "Actual elapsed time, minutes",
    "AirTime": "Airborne time, minutes",
    "Distance": "Distance between airports, miles",
    "CarrierDelay": "Carrier-caused delay, minutes",
    "WeatherDelay": "Weather-caused delay, minutes",
    "NASDelay": "NAS-caused delay, minutes",
    "SecurityDelay": "Security-caused delay, minutes",
    "LateAircraftDelay": "Late aircraft delay, minutes",
}

files = sorted(BTS_DIR.rglob("BTS_2025_*.csv"))

if len(files) != 12:
    raise FileNotFoundError(
        f"Expected 12 BTS monthly files, found {len(files)}"
    )

# ==================================================
# PASS 1: EXACT BASIC STATS + SAMPLE FOR QUANTILES
# ==================================================

stats = {
    field: {
        "Valid_Count": 0,
        "Missing_Count": 0,
        "Invalid_Numeric_Count": 0,
        "Negative_Count": 0,
        "Zero_Count": 0,
        "Exact_Min": None,
        "Exact_Max": None,
        "Above_1440_Count": 0,
    }
    for field in numeric_fields
}

samples = {field: [] for field in numeric_fields}

sample_fraction = 0.05
random_seed = 2025

chunk_number = 0

for file in files:

    print(f"Pass 1 - Processing {file.name}...")

    for chunk in pd.read_csv(
        file,
        dtype=str,
        keep_default_na=False,
        usecols=numeric_fields,
        chunksize=100_000,
        low_memory=False
    ):

        # Lấy mẫu để ước lượng phân vị
        sampled = chunk.sample(
            frac=sample_fraction,
            random_state=random_seed + chunk_number
        )

        chunk_number += 1

        for field in numeric_fields:

            raw = chunk[field].str.strip()

            values = pd.to_numeric(
                raw,
                errors="coerce"
            )

            nonblank = raw.ne("")
            valid = values.notna()

            valid_values = values.loc[valid]

            s = stats[field]

            s["Valid_Count"] += int(valid.sum())

            s["Missing_Count"] += int(
                (~nonblank).sum()
            )

            s["Invalid_Numeric_Count"] += int(
                (nonblank & ~valid).sum()
            )

            s["Negative_Count"] += int(
                (valid_values < 0).sum()
            )

            s["Zero_Count"] += int(
                (valid_values == 0).sum()
            )

            if len(valid_values) > 0:

                chunk_min = float(valid_values.min())
                chunk_max = float(valid_values.max())

                if s["Exact_Min"] is None:
                    s["Exact_Min"] = chunk_min
                else:
                    s["Exact_Min"] = min(
                        s["Exact_Min"],
                        chunk_min
                    )

                if s["Exact_Max"] is None:
                    s["Exact_Max"] = chunk_max
                else:
                    s["Exact_Max"] = max(
                        s["Exact_Max"],
                        chunk_max
                    )

                if field in [
                    "DepDelayMinutes",
                    "ArrDelayMinutes"
                ]:
                    s["Above_1440_Count"] += int(
                        (valid_values > 1440).sum()
                    )

            sample_values = pd.to_numeric(
                sampled[field].str.strip(),
                errors="coerce"
            ).dropna()

            if not sample_values.empty:
                samples[field].append(
                    sample_values.to_numpy(dtype="float64")
                )

# ==================================================
# CALCULATE SAMPLE QUANTILES AND IQR FENCES
# ==================================================

fences = {}
quantile_results = {}

for field in numeric_fields:

    if not samples[field]:
        continue

    sample_array = np.concatenate(samples[field])

    percentiles = np.quantile(
        sample_array,
        [0.01, 0.25, 0.50, 0.75, 0.95, 0.99]
    )

    p01, q1, median, q3, p95, p99 = percentiles

    iqr = q3 - q1

    lower_fence = q1 - 1.5 * iqr
    upper_fence = q3 + 1.5 * iqr

    fences[field] = {
        "Lower_IQR_Fence": float(lower_fence),
        "Upper_IQR_Fence": float(upper_fence),
    }

    quantile_results[field] = {
        "Sample_N": len(sample_array),
        "P01": float(p01),
        "Q1": float(q1),
        "Median": float(median),
        "Q3": float(q3),
        "P95": float(p95),
        "P99": float(p99),
        "IQR": float(iqr),
        "Lower_IQR_Fence": float(lower_fence),
        "Upper_IQR_Fence": float(upper_fence),
    }

# ==================================================
# PASS 2: COUNT ALL RECORDS OUTSIDE IQR FENCES
# ==================================================

outlier_counts = {
    field: {
        "Below_IQR_Fence_Count": 0,
        "Above_IQR_Fence_Count": 0,
    }
    for field in numeric_fields
    if field in fences
}

for file in files:

    print(f"Pass 2 - Processing {file.name}...")

    for chunk in pd.read_csv(
        file,
        dtype=str,
        keep_default_na=False,
        usecols=numeric_fields,
        chunksize=100_000,
        low_memory=False
    ):

        for field in fences:

            values = pd.to_numeric(
                chunk[field].str.strip(),
                errors="coerce"
            )

            valid_values = values.dropna()

            lower = fences[field]["Lower_IQR_Fence"]
            upper = fences[field]["Upper_IQR_Fence"]

            outlier_counts[field][
                "Below_IQR_Fence_Count"
            ] += int((valid_values < lower).sum())

            outlier_counts[field][
                "Above_IQR_Fence_Count"
            ] += int((valid_values > upper).sum())

# ==================================================
# BUILD FINAL REPORT
# ==================================================

records = []

for field in numeric_fields:

    s = stats[field]

    q = quantile_results.get(field, {})
    o = outlier_counts.get(field, {})

    records.append({
        "Field": field,
        "Description": descriptions[field],

        "Valid_Count": s["Valid_Count"],
        "Missing_Count": s["Missing_Count"],
        "Invalid_Numeric_Count": s["Invalid_Numeric_Count"],

        "Negative_Count": s["Negative_Count"],
        "Zero_Count": s["Zero_Count"],

        "Exact_Min": s["Exact_Min"],
        "Exact_Max": s["Exact_Max"],

        "Sample_N": q.get("Sample_N"),
        "P01": q.get("P01"),
        "Q1": q.get("Q1"),
        "Median": q.get("Median"),
        "Q3": q.get("Q3"),
        "P95": q.get("P95"),
        "P99": q.get("P99"),
        "IQR": q.get("IQR"),

        "Lower_IQR_Fence": q.get("Lower_IQR_Fence"),
        "Upper_IQR_Fence": q.get("Upper_IQR_Fence"),

        "Below_IQR_Fence_Count":
            o.get("Below_IQR_Fence_Count"),

        "Above_IQR_Fence_Count":
            o.get("Above_IQR_Fence_Count"),

        "Above_1440_Count": s["Above_1440_Count"],
    })

report = pd.DataFrame(records)

output_file = (
    OUTPUT_DIR / "17_bts_outlier_profile_summary.csv"
)

report.to_csv(
    output_file,
    index=False,
    encoding="utf-8-sig"
)

print("\n========== BTS OUTLIER PROFILE ==========")

print(
    report[
        [
            "Field",
            "Valid_Count",
            "Missing_Count",
            "Exact_Min",
            "P01",
            "Q1",
            "Median",
            "Q3",
            "P99",
            "Exact_Max",
            "Below_IQR_Fence_Count",
            "Above_IQR_Fence_Count",
            "Above_1440_Count",
        ]
    ].to_string(index=False)
)

print(f"\nReport saved to: {output_file}")
print("\nNote: IQR thresholds are estimated from a 5% sample.")
print("Outlier counts are scanned across the full dataset.")
