
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter
import csv
import json
import math


# =========================================================
# 1. PATHS AND CONFIGURATION
# =========================================================

BASE_DIR = Path(__file__).resolve().parents[2]

CSV_DIR = BASE_DIR / "01_RAW_DATA" / "OpenMeteo" / "csv"
JSON_DIR = BASE_DIR / "01_RAW_DATA" / "OpenMeteo" / "json"

OUTPUT_DIR = BASE_DIR / "02_DATA_PROFILING" / "03_OpenMeteo"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

EXPECTED_YEAR = 2025
EXPECTED_HOURS = 8760

WEATHER_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "cloud_cover",
    "weather_code",
]


# Expected hourly timeline for 2025
start_time = datetime(EXPECTED_YEAR, 1, 1, 0, 0)

expected_times = [
    (start_time + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M")
    for i in range(EXPECTED_HOURS)
]

expected_time_set = set(expected_times)


# =========================================================
# 2. HELPER FUNCTIONS
# =========================================================

def extract_airport_code(file_path):
    name = file_path.stem

    prefix = "weather_"
    suffix = f"_{EXPECTED_YEAR}"

    if name.startswith(prefix) and name.endswith(suffix):
        return name[len(prefix):-len(suffix)].upper()

    return name.upper()


def normalize_csv_number(value):
    if value is None or str(value).strip() == "":
        return None

    try:
        number = float(value)

        if not math.isfinite(number):
            return None

        return number

    except (ValueError, TypeError):
        return "INVALID"


def values_match(csv_value, json_value, tolerance=1e-9):
    csv_number = normalize_csv_number(csv_value)

    if csv_number is None:
        return json_value is None

    if csv_number == "INVALID":
        return False

    if json_value is None:
        return False

    try:
        json_number = float(json_value)

        if not math.isfinite(json_number):
            return False

        return abs(csv_number - json_number) <= tolerance

    except (ValueError, TypeError):
        return False


def haversine_km(lat1, lon1, lat2, lon2):
    """
    Calculate distance between two coordinates.
    This is an informational comparison between
    the requested coordinate and returned grid coordinate.
    """

    if None in (lat1, lon1, lat2, lon2):
        return None

    earth_radius_km = 6371.0

    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1)
        * math.cos(lat2)
        * math.sin(dlon / 2) ** 2
    )

    c = 2 * math.asin(math.sqrt(a))

    return earth_radius_km * c


# =========================================================
# 3. RECONCILIATION
# =========================================================

csv_files = sorted(CSV_DIR.glob("weather_*_2025.csv"))

airport_results = []

total_csv_rows = 0
total_sequence_mismatches = 0
total_duplicate_rows = 0
total_missing_hours = 0
total_extra_timestamps = 0
total_json_time_mismatches = 0
total_json_value_mismatches = 0
total_json_array_length_mismatches = 0

missing_json_files = []
invalid_json_files = []


for csv_path in csv_files:

    code = extract_airport_code(csv_path)
    json_path = JSON_DIR / f"weather_{code}_2025.json"

    print(f"Processing airport: {code}")

    # -----------------------------------------------------
    # 3.1 Check JSON existence and validity
    # -----------------------------------------------------

    if not json_path.exists():
        missing_json_files.append(code)
        continue

    try:
        with open(json_path, "r", encoding="utf-8-sig") as f:
            json_data = json.load(f)

    except Exception as e:
        invalid_json_files.append(f"{code}: {e}")
        continue

    hourly = json_data.get("hourly", {})

    if not isinstance(hourly, dict):
        invalid_json_files.append(
            f"{code}: JSON hourly is not an object"
        )
        continue

    json_times = hourly.get("time", [])

    if not isinstance(json_times, list):
        json_times = []

    # -----------------------------------------------------
    # 3.2 Validate JSON array lengths
    # -----------------------------------------------------

    array_length_mismatches = []

    for variable in WEATHER_VARIABLES:

        values = hourly.get(variable, [])

        if not isinstance(values, list):
            array_length_mismatches.append(variable)

        elif len(values) != EXPECTED_HOURS:
            array_length_mismatches.append(variable)

    if len(json_times) != EXPECTED_HOURS:
        array_length_mismatches.append("time")

    # -----------------------------------------------------
    # 3.3 Read CSV and compare with JSON
    # -----------------------------------------------------

    csv_row_count = 0

    sequence_mismatches = 0
    duplicate_timestamps = 0
    json_time_mismatches = 0
    json_value_mismatches = 0

    seen_timestamps = set()

    csv_airport_codes = set()
    csv_latitudes = set()
    csv_longitudes = set()
    csv_timezones = set()
    csv_models = set()

    first_csv_time = ""
    last_csv_time = ""

    requested_latitude = None
    requested_longitude = None

    with open(
        csv_path,
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        reader = csv.DictReader(f)

        for row_index, row in enumerate(reader):

            csv_row_count += 1

            # Airport and metadata
            csv_airport_codes.add(
                str(row.get("AirportCode", "")).strip()
            )

            lat_raw = str(row.get("Latitude", "")).strip()
            lon_raw = str(row.get("Longitude", "")).strip()

            if lat_raw:
                csv_latitudes.add(lat_raw)

                if requested_latitude is None:
                    try:
                        requested_latitude = float(lat_raw)
                    except ValueError:
                        pass

            if lon_raw:
                csv_longitudes.add(lon_raw)

                if requested_longitude is None:
                    try:
                        requested_longitude = float(lon_raw)
                    except ValueError:
                        pass

            csv_timezones.add(
                str(row.get("TimezoneRequested", "")).strip()
            )

            csv_models.add(
                str(row.get("WeatherModelRequested", "")).strip()
            )

            # Timestamp
            csv_time = str(row.get("time", "")).strip()

            if row_index == 0:
                first_csv_time = csv_time

            last_csv_time = csv_time

            if csv_time in seen_timestamps:
                duplicate_timestamps += 1

            seen_timestamps.add(csv_time)

            if (
                row_index >= EXPECTED_HOURS
                or csv_time != expected_times[row_index]
            ):
                sequence_mismatches += 1

            # Compare CSV timestamp with JSON timestamp
            if (
                row_index >= len(json_times)
                or csv_time != json_times[row_index]
            ):
                json_time_mismatches += 1

            # Compare weather values between CSV and JSON
            for variable in WEATHER_VARIABLES:

                json_values = hourly.get(variable, [])

                if (
                    not isinstance(json_values, list)
                    or row_index >= len(json_values)
                ):
                    json_value_mismatches += 1
                    continue

                csv_value = row.get(variable)
                json_value = json_values[row_index]

                if not values_match(csv_value, json_value):
                    json_value_mismatches += 1

    # -----------------------------------------------------
    # 3.4 Validate expected time coverage
    # -----------------------------------------------------

    missing_expected_hours = len(
        expected_time_set - seen_timestamps
    )

    extra_distinct_timestamps = len(
        seen_timestamps - expected_time_set
    )

    # -----------------------------------------------------
    # 3.5 Compare requested coordinates and grid coordinates
    # -----------------------------------------------------

    json_latitude = json_data.get("latitude")
    json_longitude = json_data.get("longitude")

    try:
        json_latitude = float(json_latitude)
        json_longitude = float(json_longitude)
    except (ValueError, TypeError):
        json_latitude = None
        json_longitude = None

    coordinate_distance = haversine_km(
        requested_latitude,
        requested_longitude,
        json_latitude,
        json_longitude,
    )

    # -----------------------------------------------------
    # 3.6 Save airport result
    # -----------------------------------------------------

    json_timezone = json_data.get("timezone", "")
    json_utc_offset = json_data.get("utc_offset_seconds", "")

    result = {
        "AirportCode": code,

        "CSVRows": csv_row_count,
        "CSV_AirportCodeValues": " | ".join(
            sorted(csv_airport_codes)
        ),

        "CSV_FirstTime": first_csv_time,
        "CSV_LastTime": last_csv_time,

        "TimestampSequenceMismatchRows": sequence_mismatches,
        "DuplicateTimestampRows": duplicate_timestamps,
        "MissingExpectedHours": missing_expected_hours,
        "ExtraDistinctTimestamps": extra_distinct_timestamps,

        "JSONTimeArrayLength": len(json_times),
        "JSONTimeMismatchRows": json_time_mismatches,

        "JSONHourlyArrayLengthMismatchCount": len(
            array_length_mismatches
        ),
        "JSONHourlyArrayLengthMismatchFields": " | ".join(
            array_length_mismatches
        ),

        "CSV_JSON_ValueMismatchCount": json_value_mismatches,

        "CSV_LatitudeUniqueCount": len(csv_latitudes),
        "CSV_LongitudeUniqueCount": len(csv_longitudes),
        "CSV_TimezoneRequested": " | ".join(
            sorted(csv_timezones)
        ),
        "CSV_WeatherModelRequested": " | ".join(
            sorted(csv_models)
        ),

        "JSON_Timezone": json_timezone,
        "JSON_UTCOffsetSeconds": json_utc_offset,

        "CSV_RequestedLatitude": requested_latitude,
        "CSV_RequestedLongitude": requested_longitude,

        "JSON_GridLatitude": json_latitude,
        "JSON_GridLongitude": json_longitude,

        "RequestedToGridDistanceKm": (
            round(coordinate_distance, 3)
            if coordinate_distance is not None
            else ""
        ),
    }

    airport_results.append(result)

    # -----------------------------------------------------
    # 3.7 Update global summary
    # -----------------------------------------------------

    total_csv_rows += csv_row_count
    total_sequence_mismatches += sequence_mismatches
    total_duplicate_rows += duplicate_timestamps
    total_missing_hours += missing_expected_hours
    total_extra_timestamps += extra_distinct_timestamps
    total_json_time_mismatches += json_time_mismatches
    total_json_value_mismatches += json_value_mismatches
    total_json_array_length_mismatches += len(
        array_length_mismatches
    )


# =========================================================
# 4. EXPORT AIRPORT-LEVEL RESULTS
# =========================================================

output_csv = (
    OUTPUT_DIR / "24_openmeteo_csv_json_reconciliation.csv"
)

if airport_results:

    with open(
        output_csv,
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=airport_results[0].keys()
        )

        writer.writeheader()
        writer.writerows(airport_results)


# =========================================================
# 5. EXPORT SUMMARY REPORT
# =========================================================

summary_path = (
    OUTPUT_DIR / "25_openmeteo_reconciliation_summary.txt"
)

with open(summary_path, "w", encoding="utf-8") as f:

    f.write("OPEN-METEO CSV-JSON RECONCILIATION\n")
    f.write("=" * 65 + "\n\n")

    f.write(f"CSV files found: {len(csv_files)}\n")
    f.write(f"Successfully reconciled files: {len(airport_results)}\n")
    f.write(f"Missing JSON files: {len(missing_json_files)}\n")
    f.write(f"Invalid JSON files: {len(invalid_json_files)}\n\n")

    f.write(f"Total CSV rows: {total_csv_rows:,}\n")
    f.write(
        f"Timestamp sequence mismatches: "
        f"{total_sequence_mismatches:,}\n"
    )
    f.write(
        f"Duplicate timestamp rows: "
        f"{total_duplicate_rows:,}\n"
    )
    f.write(
        f"Missing expected hours: "
        f"{total_missing_hours:,}\n"
    )
    f.write(
        f"Extra distinct timestamps: "
        f"{total_extra_timestamps:,}\n"
    )

    f.write(
        f"CSV-JSON time mismatches: "
        f"{total_json_time_mismatches:,}\n"
    )

    f.write(
        f"CSV-JSON weather value mismatches: "
        f"{total_json_value_mismatches:,}\n"
    )

    f.write(
        f"JSON array length mismatches: "
        f"{total_json_array_length_mismatches:,}\n"
    )

    f.write("\nMissing JSON files:\n")
    for code in missing_json_files:
        f.write(f"- {code}\n")

    f.write("\nInvalid JSON files:\n")
    for item in invalid_json_files:
        f.write(f"- {item}\n")

    f.write("\nAirports with at least one reconciliation issue:\n")

    for result in airport_results:

        has_issue = any([
            result["CSVRows"] != EXPECTED_HOURS,
            result["TimestampSequenceMismatchRows"] > 0,
            result["DuplicateTimestampRows"] > 0,
            result["MissingExpectedHours"] > 0,
            result["ExtraDistinctTimestamps"] > 0,
            result["JSONTimeArrayLength"] != EXPECTED_HOURS,
            result["JSONTimeMismatchRows"] > 0,
            result["JSONHourlyArrayLengthMismatchCount"] > 0,
            result["CSV_JSON_ValueMismatchCount"] > 0,
        ])

        if has_issue:
            f.write(
                f"- {result['AirportCode']}: "
                f"{result}\n"
            )


# =========================================================
# 6. CONSOLE OUTPUT
# =========================================================

print("\n========== OPEN-METEO CSV-JSON RECONCILIATION ==========")

print(f"CSV files found: {len(csv_files)}")
print(f"Successfully reconciled: {len(airport_results)}")
print(f"Missing JSON files: {len(missing_json_files)}")
print(f"Invalid JSON files: {len(invalid_json_files)}")

print(f"\nTotal CSV rows: {total_csv_rows:,}")

print(
    f"Timestamp sequence mismatches: "
    f"{total_sequence_mismatches:,}"
)

print(
    f"Duplicate timestamp rows: "
    f"{total_duplicate_rows:,}"
)

print(
    f"Missing expected hours: "
    f"{total_missing_hours:,}"
)

print(
    f"Extra distinct timestamps: "
    f"{total_extra_timestamps:,}"
)

print(
    f"CSV-JSON time mismatches: "
    f"{total_json_time_mismatches:,}"
)

print(
    f"CSV-JSON weather value mismatches: "
    f"{total_json_value_mismatches:,}"
)

print(
    f"JSON array length mismatches: "
    f"{total_json_array_length_mismatches:,}"
)

print("\nOutput files:")
print(output_csv)
print(summary_path)

print("\n========== COMPLETED ==========")
