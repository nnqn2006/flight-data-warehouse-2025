
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime, timedelta
import csv
import heapq
import itertools
import math


# =========================================================
# 1. CONFIGURATION
# =========================================================

BASE_DIR = Path(__file__).resolve().parents[2]

CSV_DIR = BASE_DIR / "01_RAW_DATA" / "OpenMeteo" / "csv"

OUTPUT_DIR = BASE_DIR / "02_DATA_PROFILING" / "03_OpenMeteo"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

EXPECTED_AIRPORTS = 240
EXPECTED_HOURS_PER_AIRPORT = 8760
EXPECTED_TOTAL_ROWS = EXPECTED_AIRPORTS * EXPECTED_HOURS_PER_AIRPORT

WEATHER_CODE_TO_AUDIT = 96

# Keep 10 lowest and 10 highest values globally
TOP_K = 10

# Weather variables for global extreme inspection.
# Circular direction and categorical weather code are excluded.
EXTREME_VARIABLES = [
    "temperature_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_gusts_10m",
]

# Context: two hours before and two hours after
CONTEXT_HOURS = 2

# These are the original CSV fields
SOURCE_FIELDS = [
    "AirportCode",
    "time",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "cloud_cover",
    "weather_code",
    "Latitude",
    "Longitude",
    "TimezoneRequested",
    "WeatherModelRequested",
]


# =========================================================
# 2. HELPER FUNCTIONS
# =========================================================

def extract_airport_code(file_path):

    name = file_path.stem

    prefix = "weather_"
    suffix = "_2025"

    if name.startswith(prefix) and name.endswith(suffix):
        return name[len(prefix):-len(suffix)].upper()

    return name.upper()


def parse_numeric(value):

    if value is None or str(value).strip() == "":
        return None

    try:
        number = float(str(value).strip())

        if not math.isfinite(number):
            return None

        return number

    except (ValueError, TypeError):
        return None


def parse_timestamp(value):

    try:
        return datetime.strptime(
            str(value).strip(),
            "%Y-%m-%dT%H:%M"
        )

    except (ValueError, TypeError):
        return None


def write_csv(path, rows, fieldnames):

    with open(
        path,
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore"
        )

        writer.writeheader()
        writer.writerows(rows)


def get_source_row(row):

    return {
        field: row.get(field, "")
        for field in SOURCE_FIELDS
    }


# =========================================================
# 3. INITIALIZE GLOBAL EXTREME HEAPS
# =========================================================

# For maximum values:
# Keep the K largest values using a min-heap.

# For minimum values:
# Keep the K smallest values using a heap with negative values.

max_heaps = {
    variable: []
    for variable in EXTREME_VARIABLES
}

min_heaps = {
    variable: []
    for variable in EXTREME_VARIABLES
}

counter = itertools.count()

total_rows = 0

total_files = 0

weather_code_frequency = Counter()

weather_code_96_rows = []

weather_code_96_by_airport = Counter()

weather_code_96_models = Counter()

weather_code_96_timezones = Counter()

missing_timestamp_rows = 0

missing_numeric_values = Counter()


# =========================================================
# 4. FIRST PASS: SCAN ALL 240 CSV FILES
# =========================================================

csv_files = sorted(
    CSV_DIR.glob("weather_*_2025.csv")
)

if not csv_files:
    raise FileNotFoundError(
        f"No Open-Meteo CSV files found in: {CSV_DIR}"
    )


for file_index, file_path in enumerate(
    csv_files,
    start=1
):

    airport_code = extract_airport_code(file_path)

    total_files += 1

    print(
        f"[{file_index}/{len(csv_files)}] "
        f"Scanning airport: {airport_code}"
    )

    with open(
        file_path,
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        reader = csv.DictReader(f)

        for row in reader:

            total_rows += 1

            # -------------------------------------------------
            # 4.1 Read timestamp
            # -------------------------------------------------

            timestamp = str(
                row.get("time", "")
            ).strip()

            if not timestamp:
                missing_timestamp_rows += 1

            # -------------------------------------------------
            # 4.2 Count weather code frequency
            # -------------------------------------------------

            weather_code = parse_numeric(
                row.get("weather_code")
            )

            if weather_code is not None:

                if weather_code.is_integer():

                    code = int(weather_code)

                    weather_code_frequency[code] += 1

                    # -----------------------------------------
                    # Collect all code 96 records
                    # -----------------------------------------

                    if code == WEATHER_CODE_TO_AUDIT:

                        source_row = get_source_row(row)

                        weather_code_96_rows.append(
                            source_row
                        )

                        weather_code_96_by_airport[
                            airport_code
                        ] += 1

                        weather_code_96_models[
                            row.get(
                                "WeatherModelRequested",
                                ""
                            )
                        ] += 1

                        weather_code_96_timezones[
                            row.get(
                                "TimezoneRequested",
                                ""
                            )
                        ] += 1

            # -------------------------------------------------
            # 4.3 Profile global extremes
            # -------------------------------------------------

            for variable in EXTREME_VARIABLES:

                value = parse_numeric(
                    row.get(variable)
                )

                if value is None:

                    missing_numeric_values[
                        variable
                    ] += 1

                    continue

                # ---------------------------------------------
                # Keep K largest values
                # ---------------------------------------------

                max_heap = max_heaps[variable]

                if len(max_heap) < TOP_K:

                    heapq.heappush(
                        max_heap,
                        (
                            value,
                            next(counter),
                            get_source_row(row)
                        )
                    )

                elif value > max_heap[0][0]:

                    heapq.heapreplace(
                        max_heap,
                        (
                            value,
                            next(counter),
                            get_source_row(row)
                        )
                    )

                # ---------------------------------------------
                # Keep K smallest values
                # ---------------------------------------------

                min_heap = min_heaps[variable]

                negative_value = -value

                if len(min_heap) < TOP_K:

                    heapq.heappush(
                        min_heap,
                        (
                            negative_value,
                            next(counter),
                            get_source_row(row)
                        )
                    )

                elif negative_value > min_heap[0][0]:

                    heapq.heapreplace(
                        min_heap,
                        (
                            negative_value,
                            next(counter),
                            get_source_row(row)
                        )
                    )


# =========================================================
# 5. EXPORT GLOBAL EXTREME RECORDS
# =========================================================

extreme_rows = []

# Store selected event keys for contextual inspection.
# Key: (AirportCode, timestamp)
# Value: set of event descriptions.

event_reasons = defaultdict(set)


for variable in EXTREME_VARIABLES:

    # -----------------------------------------------------
    # 5.1 Largest values
    # -----------------------------------------------------

    max_records = sorted(
        max_heaps[variable],
        key=lambda item: item[0],
        reverse=True
    )

    for rank, item in enumerate(
        max_records,
        start=1
    ):

        value, _, source_row = item

        output_row = {
            "Variable": variable,
            "ExtremeType": "GLOBAL_MAX",
            "Rank": rank,
            "Value": value,
            **source_row
        }

        extreme_rows.append(output_row)

        key = (
            source_row["AirportCode"],
            source_row["time"]
        )

        event_reasons[key].add(
            f"GLOBAL_MAX_{variable}"
        )

    # -----------------------------------------------------
    # 5.2 Smallest values
    # -----------------------------------------------------

    min_records = sorted(
        min_heaps[variable],
        key=lambda item: -item[0]
    )

    for rank, item in enumerate(
        min_records,
        start=1
    ):

        negative_value, _, source_row = item

        value = -negative_value

        output_row = {
            "Variable": variable,
            "ExtremeType": "GLOBAL_MIN",
            "Rank": rank,
            "Value": value,
            **source_row
        }

        extreme_rows.append(output_row)

        key = (
            source_row["AirportCode"],
            source_row["time"]
        )

        event_reasons[key].add(
            f"GLOBAL_MIN_{variable}"
        )


extreme_path = (
    OUTPUT_DIR
    / "31_openmeteo_global_extreme_records.csv"
)

extreme_fields = [
    "Variable",
    "ExtremeType",
    "Rank",
    "Value",
    *SOURCE_FIELDS
]

write_csv(
    extreme_path,
    extreme_rows,
    extreme_fields
)


# =========================================================
# 6. EXPORT ALL WEATHER CODE 96 RECORDS
# =========================================================

code96_path = (
    OUTPUT_DIR
    / "32_openmeteo_weather_code_96_records.csv"
)

write_csv(
    code96_path,
    weather_code_96_rows,
    SOURCE_FIELDS
)

# Add each code 96 event to the contextual audit.
for row in weather_code_96_rows:

    key = (
        row["AirportCode"],
        row["time"]
    )

    event_reasons[key].add(
        "WEATHER_CODE_96"
    )


# =========================================================
# 7. BUILD CONTEXT TIMESTAMPS
# =========================================================

# Key:
# (AirportCode, timestamp)
#
# Value:
# List of event labels and relative time offsets.

context_map = defaultdict(set)

invalid_event_timestamps = []


for event_key, reasons in event_reasons.items():

    airport_code, event_time = event_key

    parsed_time = parse_timestamp(event_time)

    if parsed_time is None:

        invalid_event_timestamps.append(
            event_key
        )

        continue

    for offset in range(
        -CONTEXT_HOURS,
        CONTEXT_HOURS + 1
    ):

        context_time = (
            parsed_time
            + timedelta(hours=offset)
        )

        context_time_str = context_time.strftime(
            "%Y-%m-%dT%H:%M"
        )

        context_key = (
            airport_code,
            context_time_str
        )

        for reason in reasons:

            tag = (
                f"{reason}@{offset:+d}h"
            )

            context_map[context_key].add(tag)


# =========================================================
# 8. SECOND PASS: EXTRACT CONTEXTUAL RECORDS
# =========================================================

context_rows = []

csv_file_lookup = {
    extract_airport_code(file_path): file_path
    for file_path in csv_files
}

context_airports = sorted({
    airport_code
    for airport_code, _ in context_map
})

for file_index, airport_code in enumerate(
    context_airports,
    start=1
):

    file_path = csv_file_lookup.get(
        airport_code
    )

    if file_path is None:
        continue

    print(
        f"[Context {file_index}/{len(context_airports)}] "
        f"Extracting: {airport_code}"
    )

    with open(
        file_path,
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        reader = csv.DictReader(f)

        for row in reader:

            timestamp = str(
                row.get("time", "")
            ).strip()

            key = (
                airport_code,
                timestamp
            )

            if key not in context_map:
                continue

            output_row = get_source_row(row)

            output_row["ContextTags"] = " | ".join(
                sorted(context_map[key])
            )

            output_row["ContextEventCount"] = len(
                context_map[key]
            )

            context_rows.append(output_row)


# =========================================================
# 9. EXPORT CONTEXT RECORDS
# =========================================================

context_path = (
    OUTPUT_DIR
    / "33_openmeteo_extreme_event_context.csv"
)

context_fields = [
    *SOURCE_FIELDS,
    "ContextTags",
    "ContextEventCount"
]

write_csv(
    context_path,
    context_rows,
    context_fields
)


# =========================================================
# 10. BUILD WEATHER CODE FREQUENCY SUMMARY
# =========================================================

code_frequency_rows = []

for code, count in sorted(
    weather_code_frequency.items()
):

    percentage = (
        count / total_rows * 100
        if total_rows else 0
    )

    code_frequency_rows.append({
        "WeatherCode": code,
        "Count": count,
        "Percentage": round(
            percentage,
            5
        )
    })


code_frequency_path = (
    OUTPUT_DIR
    / "34_openmeteo_weather_code_frequency_audit.csv"
)

write_csv(
    code_frequency_path,
    code_frequency_rows,
    [
        "WeatherCode",
        "Count",
        "Percentage"
    ]
)


# =========================================================
# 11. EXPORT SUMMARY REPORT
# =========================================================

summary_path = (
    OUTPUT_DIR
    / "35_openmeteo_extreme_weathercode_audit_summary.txt"
)

with open(
    summary_path,
    "w",
    encoding="utf-8"
) as f:

    f.write(
        "OPEN-METEO EXTREME AND WEATHER CODE AUDIT\n"
    )

    f.write("=" * 75 + "\n\n")

    f.write(
        f"CSV files scanned: {total_files}\n"
    )

    f.write(
        f"Expected CSV files: {EXPECTED_AIRPORTS}\n"
    )

    f.write(
        f"Total rows scanned: {total_rows:,}\n"
    )

    f.write(
        f"Expected total rows: {EXPECTED_TOTAL_ROWS:,}\n"
    )

    f.write(
        f"Missing timestamp rows: "
        f"{missing_timestamp_rows:,}\n\n"
    )

    # -----------------------------------------------------
    # Weather code 96 summary
    # -----------------------------------------------------

    f.write(
        f"WEATHER CODE {WEATHER_CODE_TO_AUDIT} SUMMARY\n"
    )

    f.write("-" * 75 + "\n")

    f.write(
        f"Total code {WEATHER_CODE_TO_AUDIT} records: "
        f"{len(weather_code_96_rows):,}\n"
    )

    f.write(
        f"Airports containing code {WEATHER_CODE_TO_AUDIT}: "
        f"{len(weather_code_96_by_airport):,}\n"
    )

    f.write(
        f"Requested model values: "
        f"{dict(weather_code_96_models)}\n"
    )

    f.write(
        f"Requested timezone values: "
        f"{dict(weather_code_96_timezones)}\n\n"
    )

    f.write("CODE 96 COUNTS BY AIRPORT\n")
    f.write("-" * 75 + "\n")

    for airport_code, count in sorted(
        weather_code_96_by_airport.items()
    ):

        f.write(
            f"{airport_code}: {count}\n"
        )

    # -----------------------------------------------------
    # Global weather code frequency
    # -----------------------------------------------------

    f.write("\nWEATHER CODE FREQUENCY\n")
    f.write("-" * 75 + "\n")

    for code, count in sorted(
        weather_code_frequency.items()
    ):

        f.write(
            f"{code}: {count:,} "
            f"({count / total_rows * 100:.5f}%)\n"
        )

    # -----------------------------------------------------
    # Global extremes
    # -----------------------------------------------------

    f.write("\nGLOBAL EXTREME RECORDS\n")
    f.write("-" * 75 + "\n")

    for variable in EXTREME_VARIABLES:

        f.write(f"\n{variable}\n")

        f.write("Largest values:\n")

        max_records = sorted(
            max_heaps[variable],
            key=lambda item: item[0],
            reverse=True
        )

        for rank, item in enumerate(
            max_records,
            start=1
        ):

            value, _, row = item

            f.write(
                f"MAX #{rank}: {value} | "
                f"{row['AirportCode']} | "
                f"{row['time']} | "
                f"WeatherCode={row['weather_code']}\n"
            )

        f.write("\nSmallest values:\n")

        min_records = sorted(
            min_heaps[variable],
            key=lambda item: -item[0]
        )

        for rank, item in enumerate(
            min_records,
            start=1
        ):

            negative_value, _, row = item

            value = -negative_value

            f.write(
                f"MIN #{rank}: {value} | "
                f"{row['AirportCode']} | "
                f"{row['time']} | "
                f"WeatherCode={row['weather_code']}\n"
            )

    # -----------------------------------------------------
    # Context audit
    # -----------------------------------------------------

    f.write("\nCONTEXT EXTRACTION\n")
    f.write("-" * 75 + "\n")

    f.write(
        f"Unique event timestamps: "
        f"{len(event_reasons):,}\n"
    )

    f.write(
        f"Context rows extracted: "
        f"{len(context_rows):,}\n"
    )

    f.write(
        f"Invalid event timestamps: "
        f"{len(invalid_event_timestamps):,}\n"
    )

    # -----------------------------------------------------
    # Missing numeric values
    # -----------------------------------------------------

    f.write("\nMISSING NUMERIC VALUES DURING EXTREME SCAN\n")
    f.write("-" * 75 + "\n")

    if missing_numeric_values:

        for variable, count in sorted(
            missing_numeric_values.items()
        ):

            f.write(
                f"{variable}: {count:,}\n"
            )

    else:

        f.write(
            "No missing or invalid numeric values "
            "found in selected extreme variables.\n"
        )

    # -----------------------------------------------------
    # Interpretation notes
    # -----------------------------------------------------

    f.write("\nINTERPRETATION NOTES\n")
    f.write("-" * 75 + "\n")

    f.write(
        "Extreme records are review candidates, "
        "not automatically erroneous records.\n"
    )

    f.write(
        "Weather code 96 is retained as reported "
        "and is not changed by this script.\n"
    )

    f.write(
        "Context includes two hourly records before "
        "and after each selected event.\n"
    )

    f.write(
        "This script does not modify Bronze source data.\n"
    )


# =========================================================
# 12. CONSOLE SUMMARY
# =========================================================

print("\n========== STEP 23 AUDIT SUMMARY ==========")

print(f"CSV files scanned: {total_files}")

print(f"Total rows scanned: {total_rows:,}")

print(
    f"Expected total rows: {EXPECTED_TOTAL_ROWS:,}"
)

print(
    f"\nWeather code {WEATHER_CODE_TO_AUDIT} records: "
    f"{len(weather_code_96_rows):,}"
)

print(
    f"Airports containing code 96: "
    f"{len(weather_code_96_by_airport):,}"
)

print(
    f"Code 96 model values: "
    f"{dict(weather_code_96_models)}"
)

print(
    f"Code 96 timezone values: "
    f"{dict(weather_code_96_timezones)}"
)

print("\nCODE 96 BY AIRPORT:")

for airport_code, count in sorted(
    weather_code_96_by_airport.items()
):

    print(
        f"{airport_code}: {count}"
    )

print(
    f"\nUnique event timestamps: "
    f"{len(event_reasons):,}"
)

print(
    f"Context records extracted: "
    f"{len(context_rows):,}"
)

print(
    f"Invalid event timestamps: "
    f"{len(invalid_event_timestamps):,}"
)

print("\nGLOBAL EXTREME RECORDS:")

for variable in EXTREME_VARIABLES:

    highest = max(
        max_heaps[variable],
        key=lambda item: item[0]
    )

    lowest = max(
        min_heaps[variable],
        key=lambda item: item[0]
        )

    max_value, _, max_row = highest

    negative_min_value, _, min_row = lowest

    min_value = -negative_min_value

    print(
        f"{variable}: "
        f"MIN={min_value} ({min_row['AirportCode']} "
        f"{min_row['time']}) | "
        f"MAX={max_value} ({max_row['AirportCode']} "
        f"{max_row['time']})"
    )

print("\nOUTPUT FILES:")

print(extreme_path)

print(code96_path)

print(context_path)

print(code_frequency_path)

print(summary_path)

print("\n========== STEP 23 COMPLETED ==========")
