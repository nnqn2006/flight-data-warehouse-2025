
from pathlib import Path
from collections import Counter
import csv
import math
import random


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

# Random sampling rate: 5%
SAMPLE_RATE = 0.05

# Fixed base seed to make sampling reproducible
BASE_RANDOM_SEED = 2025

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

VARIABLE_TYPES = {
    "temperature_2m": "Continuous",
    "relative_humidity_2m": "Continuous",
    "precipitation": "Continuous",
    "wind_speed_10m": "Continuous",
    "wind_direction_10m": "Circular",
    "wind_gusts_10m": "Continuous",
    "cloud_cover": "Continuous",
    "weather_code": "Categorical_Code",
}


# =========================================================
# 2. WMO WEATHER CODE REFERENCE
# =========================================================

WMO_CODES = {
    0, 1, 2, 3,
    45, 48,
    51, 53, 55, 56, 57,
    61, 63, 65, 66, 67,
    71, 73, 75, 77,
    80, 81, 82,
    85, 86,
    95, 96, 97, 99,
}

WMO_DESCRIPTIONS = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    56: "Light freezing drizzle",
    57: "Dense freezing drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    66: "Light freezing rain",
    67: "Heavy freezing rain",
    71: "Slight snowfall",
    73: "Moderate snowfall",
    75: "Heavy snowfall",
    77: "Snow grains",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    85: "Slight snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with slight hail",
    97: "Heavy thunderstorm",
    99: "Thunderstorm with heavy hail",
}


# =========================================================
# 3. VALIDATE CONFIGURATION
# =========================================================

if not (0 < SAMPLE_RATE <= 1):
    raise ValueError("SAMPLE_RATE must be greater than 0 and <= 1.")


# =========================================================
# 4. PROFILE CLASS
# =========================================================

class Profile:

    def __init__(self):

        self.total = 0
        self.valid = 0
        self.missing = 0
        self.non_numeric = 0
        self.non_finite = 0

        self.zero = 0
        self.negative = 0

        self.minimum = None
        self.maximum = None

        # Welford's online algorithm
        # Used to calculate mean and population variance
        self.mean = 0.0
        self.m2 = 0.0

        # Random sample for quantile estimation
        self.sample = []

    def add(self, raw_value, take_sample=False):

        self.total += 1

        # -------------------------------------------------
        # Missing values
        # -------------------------------------------------

        if raw_value is None or str(raw_value).strip() == "":
            self.missing += 1
            return "MISSING", None

        # -------------------------------------------------
        # Numeric conversion
        # -------------------------------------------------

        try:
            value = float(str(raw_value).strip())

        except (ValueError, TypeError):
            self.non_numeric += 1
            return "NON_NUMERIC", None

        # -------------------------------------------------
        # Infinite / NaN values
        # -------------------------------------------------

        if not math.isfinite(value):
            self.non_finite += 1
            return "NON_FINITE", None

        # -------------------------------------------------
        # Valid numeric value
        # -------------------------------------------------

        self.valid += 1

        if value == 0:
            self.zero += 1

        if value < 0:
            self.negative += 1

        # Min
        if self.minimum is None or value < self.minimum:
            self.minimum = value

        # Max
        if self.maximum is None or value > self.maximum:
            self.maximum = value

        # -------------------------------------------------
        # Welford's online algorithm
        # -------------------------------------------------

        delta = value - self.mean

        self.mean += delta / self.valid

        delta2 = value - self.mean

        self.m2 += delta * delta2

        # -------------------------------------------------
        # Random sample
        # -------------------------------------------------

        if take_sample:
            self.sample.append(value)

        return "VALID", value

    # -----------------------------------------------------
    # Quantile calculation
    # -----------------------------------------------------

    def get_quantile(self, probability):

        if not self.sample:
            return ""

        values = sorted(self.sample)

        position = (len(values) - 1) * probability

        lower_index = int(math.floor(position))
        upper_index = int(math.ceil(position))

        if lower_index == upper_index:
            return values[lower_index]

        weight = position - lower_index

        return (
            values[lower_index] * (1 - weight)
            + values[upper_index] * weight
        )

    # -----------------------------------------------------
    # Export profile as dictionary
    # -----------------------------------------------------

    def to_dict(self, variable, domain_count=0):

        if self.valid > 0:
            std = math.sqrt(self.m2 / self.valid)
        else:
            std = ""

        is_code = variable == "weather_code"
        is_circular = variable == "wind_direction_10m"

        # -------------------------------------------------
        # Mean and standard deviation
        # Do not calculate for categorical or circular data
        # -------------------------------------------------

        if self.valid and not is_code and not is_circular:
            mean_value = round(self.mean, 5)
            std_value = round(std, 5)
        else:
            mean_value = ""
            std_value = ""

        # -------------------------------------------------
        # Quantiles
        # Do not calculate for categorical or circular data
        # -------------------------------------------------

        if self.sample and not is_code and not is_circular:

            p01 = round(self.get_quantile(0.01), 5)
            q1 = round(self.get_quantile(0.25), 5)
            median = round(self.get_quantile(0.50), 5)
            q3 = round(self.get_quantile(0.75), 5)
            p99 = round(self.get_quantile(0.99), 5)

        else:

            p01 = ""
            q1 = ""
            median = ""
            q3 = ""
            p99 = ""

        return {

            "Variable": variable,

            "DataType": VARIABLE_TYPES[variable],

            "TotalRows": self.total,

            "ValidCount": self.valid,

            "MissingCount": self.missing,

            "NonNumericCount": self.non_numeric,

            "NonFiniteCount": self.non_finite,

            "MissingPct": (
                round(self.missing / self.total * 100, 5)
                if self.total else 0
            ),

            "InvalidPct": (
                round(
                    (
                        self.non_numeric
                        + self.non_finite
                    ) / self.total * 100,
                    5
                )
                if self.total else 0
            ),

            "Min": self.minimum if self.valid else "",

            "Max": self.maximum if self.valid else "",

            "Mean": mean_value,

            "StdPopulation": std_value,

            "P01_Sample": p01,

            "Q1_Sample": q1,

            "Median_Sample": median,

            "Q3_Sample": q3,

            "P99_Sample": p99,

            "SampleCount": len(self.sample),

            "ZeroCount": self.zero,

            "NegativeCount": self.negative,

            "DomainReviewCount": domain_count,
        }


# =========================================================
# 5. DOMAIN VALIDATION RULES
# =========================================================

def check_domain(variable, value):

    issues = []

    # Relative humidity: 0–100%
    if variable == "relative_humidity_2m":

        if value < 0 or value > 100:
            issues.append("HUMIDITY_OUTSIDE_0_100")

    # Precipitation: non-negative
    elif variable == "precipitation":

        if value < 0:
            issues.append("NEGATIVE_PRECIPITATION")

    # Wind speed: non-negative
    elif variable == "wind_speed_10m":

        if value < 0:
            issues.append("NEGATIVE_WIND_SPEED")

    # Wind direction: 0–360 degrees
    elif variable == "wind_direction_10m":

        if value < 0 or value > 360:
            issues.append("WIND_DIRECTION_OUTSIDE_0_360")

    # Wind gusts: non-negative
    elif variable == "wind_gusts_10m":

        if value < 0:
            issues.append("NEGATIVE_WIND_GUSTS")

    # Cloud cover: 0–100%
    elif variable == "cloud_cover":

        if value < 0 or value > 100:
            issues.append("CLOUD_COVER_OUTSIDE_0_100")

    # WMO weather code
    elif variable == "weather_code":

        if not value.is_integer():

            issues.append("NON_INTEGER_WEATHER_CODE")

        elif int(value) not in WMO_CODES:

            issues.append("UNRECOGNIZED_WMO_WEATHER_CODE")

    # No universal hard temperature boundary is applied.
    # Temperature extremes require contextual review.

    return issues


# =========================================================
# 6. HELPER FUNCTIONS
# =========================================================

def extract_airport_code(file_path):

    name = file_path.stem

    prefix = "weather_"
    suffix = "_2025"

    if name.startswith(prefix) and name.endswith(suffix):

        return name[len(prefix):-len(suffix)].upper()

    return name.upper()


def format_code(value):

    if value.is_integer():
        return str(int(value))

    return f"{value:g}"


def write_csv(path, rows):

    if not rows:

        print(f"No rows to write: {path}")

        return

    with open(
        path,
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=list(rows[0].keys())
        )

        writer.writeheader()

        writer.writerows(rows)


# =========================================================
# 7. INITIALIZE GLOBAL PROFILES
# =========================================================

csv_files = sorted(
    CSV_DIR.glob("weather_*_2025.csv")
)

overall_profiles = {

    variable: Profile()

    for variable in WEATHER_VARIABLES

}

airport_profile_rows = []

overall_domain_counts = Counter()

airport_domain_counts = Counter()

weather_code_frequency = Counter()

total_rows = 0

candidate_count = 0


# =========================================================
# 8. OUTPUT CANDIDATE FILE
# =========================================================

candidate_path = (

    OUTPUT_DIR
    / "29_openmeteo_domain_review_candidates.csv"

)

candidate_fields = [

    "AirportCode",

    "Time",

    "Variable",

    "RawValue",

    "Issue",

]


# =========================================================
# 9. PROFILE ALL AIRPORT FILES
# =========================================================

with open(

    candidate_path,

    "w",

    encoding="utf-8-sig",

    newline=""

) as candidate_file:

    candidate_writer = csv.DictWriter(

        candidate_file,

        fieldnames=candidate_fields

    )

    candidate_writer.writeheader()

    # -----------------------------------------------------
    # Process each airport
    # -----------------------------------------------------

    for file_index, file_path in enumerate(

        csv_files,

        start=1

    ):

        airport_code = extract_airport_code(
            file_path
        )

        print(

            f"[{file_index}/{len(csv_files)}] "
            f"Profiling airport: {airport_code}"

        )

        # -------------------------------------------------
        # Independent deterministic random generator
        # for each airport
        # -------------------------------------------------

        sampling_rng = random.Random(

            BASE_RANDOM_SEED
            + sum(ord(char) for char in airport_code)

        )

        # -------------------------------------------------
        # Airport-level profiles
        # -------------------------------------------------

        airport_profiles = {

            variable: Profile()

            for variable in WEATHER_VARIABLES

        }

        airport_rows = 0

        # -------------------------------------------------
        # Read CSV
        # -------------------------------------------------

        with open(

            file_path,

            "r",

            encoding="utf-8-sig",

            newline=""

        ) as f:

            reader = csv.DictReader(f)

            for row_index, row in enumerate(reader):

                airport_rows += 1

                total_rows += 1

                time_value = row.get("time", "")

                # -----------------------------------------
                # Random sample: 5% per row
                # -----------------------------------------

                take_sample = (

                    sampling_rng.random() < SAMPLE_RATE

                )

                # -----------------------------------------
                # Profile each weather variable
                # -----------------------------------------

                for variable in WEATHER_VARIABLES:

                    raw_value = row.get(variable)

                    airport_profile = (
                        airport_profiles[variable]
                    )

                    overall_profile = (
                        overall_profiles[variable]
                    )

                    # -------------------------------------
                    # Airport-level profiling
                    # -------------------------------------

                    airport_status, airport_value = (
                        airport_profile.add(

                            raw_value,

                            take_sample=take_sample

                        )
                    )

                    # -------------------------------------
                    # Overall profiling
                    # -------------------------------------

                    overall_status, overall_value = (
                        overall_profile.add(

                            raw_value,

                            take_sample=take_sample

                        )
                    )

                    # -------------------------------------
                    # Non-numeric candidate
                    # -------------------------------------

                    if airport_status == "NON_NUMERIC":

                        candidate_writer.writerow({

                            "AirportCode": airport_code,

                            "Time": time_value,

                            "Variable": variable,

                            "RawValue": raw_value,

                            "Issue": "NON_NUMERIC_VALUE",

                        })

                        candidate_count += 1

                    # -------------------------------------
                    # Non-finite candidate
                    # -------------------------------------

                    elif airport_status == "NON_FINITE":

                        candidate_writer.writerow({

                            "AirportCode": airport_code,

                            "Time": time_value,

                            "Variable": variable,

                            "RawValue": raw_value,

                            "Issue": "NON_FINITE_VALUE",

                        })

                        candidate_count += 1

                    # -------------------------------------
                    # Skip domain checks for invalid values
                    # -------------------------------------

                    if airport_status != "VALID":

                        continue

                    # -------------------------------------
                    # Weather code frequency
                    # -------------------------------------

                    if variable == "weather_code":

                        code_key = format_code(
                            airport_value
                        )

                        weather_code_frequency[
                            code_key
                        ] += 1

                    # -------------------------------------
                    # Domain validation
                    # -------------------------------------

                    issues = check_domain(

                        variable,

                        airport_value

                    )

                    for issue in issues:

                        overall_domain_counts[
                            variable
                        ] += 1

                        airport_domain_counts[
                            (airport_code, variable)
                        ] += 1

                        candidate_writer.writerow({

                            "AirportCode": airport_code,

                            "Time": time_value,

                            "Variable": variable,

                            "RawValue": raw_value,

                            "Issue": issue,

                        })

                        candidate_count += 1

        # =================================================
        # 10. AIRPORT-LEVEL PROFILE OUTPUT
        # =================================================

        for variable in WEATHER_VARIABLES:

            profile = airport_profiles[variable]

            row = profile.to_dict(

                variable,

                domain_count=airport_domain_counts[

                    (airport_code, variable)

                ]

            )

            row["AirportCode"] = airport_code

            row["AirportRows"] = airport_rows

            airport_profile_rows.append(row)


# =========================================================
# 11. OVERALL VARIABLE PROFILE
# =========================================================

overall_rows = []

for variable in WEATHER_VARIABLES:

    profile = overall_profiles[variable]

    row = profile.to_dict(

        variable,

        domain_count=overall_domain_counts[variable]

    )

    overall_rows.append(row)


# =========================================================
# 12. WEATHER CODE FREQUENCY
# =========================================================

weather_code_rows = []

weather_code_total = sum(
    weather_code_frequency.values()
)

for code, count in sorted(

    weather_code_frequency.items(),

    key=lambda item: float(item[0])

):

    try:

        numeric_code = float(code)

        if numeric_code.is_integer():

            description = WMO_DESCRIPTIONS.get(

                int(numeric_code),

                "UNRECOGNIZED CODE"

            )

        else:

            description = "NON-INTEGER CODE"

    except ValueError:

        description = "UNRECOGNIZED CODE"

    weather_code_rows.append({

        "WeatherCode": code,

        "Description": description,

        "Count": count,

        "Percentage": (

            round(
                count / weather_code_total * 100,
                5
            )

            if weather_code_total

            else 0

        ),

    })


# =========================================================
# 13. EXPORT CSV FILES
# =========================================================

overall_path = (

    OUTPUT_DIR
    / "26_openmeteo_variable_profile_overall.csv"

)

airport_path = (

    OUTPUT_DIR
    / "27_openmeteo_variable_profile_by_airport.csv"

)

weather_code_path = (

    OUTPUT_DIR
    / "28_openmeteo_weather_code_frequency.csv"

)

write_csv(

    overall_path,

    overall_rows

)

write_csv(

    airport_path,

    airport_profile_rows

)

write_csv(

    weather_code_path,

    weather_code_rows

)


# =========================================================
# 14. EXPORT TEXT SUMMARY
# =========================================================

summary_path = (

    OUTPUT_DIR
    / "30_openmeteo_profiling_summary.txt"

)

with open(

    summary_path,

    "w",

    encoding="utf-8"

) as f:

    f.write(

        "OPEN-METEO WEATHER VARIABLE PROFILING\n"

    )

    f.write("=" * 75 + "\n\n")

    f.write(

        f"Airport files: {len(csv_files)}\n"

    )

    f.write(

        f"Expected airports: {EXPECTED_AIRPORTS}\n"

    )

    f.write(

        f"Total rows: {total_rows:,}\n"

    )

    f.write(

        f"Expected total rows: {EXPECTED_TOTAL_ROWS:,}\n"

    )

    f.write(

        f"Variables profiled: {len(WEATHER_VARIABLES)}\n"

    )

    f.write(

        f"Random sample rate: {SAMPLE_RATE * 100:.1f}%\n"

    )

    f.write(

        f"Base random seed: {BASE_RANDOM_SEED}\n"

    )

    f.write(

        f"Domain / invalid value candidates: "
        f"{candidate_count:,}\n\n"

    )

    # -----------------------------------------------------
    # Overall profile
    # -----------------------------------------------------

    f.write("OVERALL VARIABLE PROFILE\n")

    f.write("-" * 75 + "\n")

    for row in overall_rows:

        f.write(

            f"\n{row['Variable']} "
            f"({row['DataType']})\n"

        )

        f.write(

            f"Valid={row['ValidCount']:,}; "
            f"Missing={row['MissingCount']:,}; "
            f"NonNumeric={row['NonNumericCount']:,}; "
            f"NonFinite={row['NonFiniteCount']:,}; "
            f"MissingPct={row['MissingPct']}%\n"

        )

        f.write(

            f"Min={row['Min']}; "
            f"Max={row['Max']}; "
            f"Mean={row['Mean']}; "
            f"Std={row['StdPopulation']}\n"

        )

        f.write(

            f"P01={row['P01_Sample']}; "
            f"Q1={row['Q1_Sample']}; "
            f"Median={row['Median_Sample']}; "
            f"Q3={row['Q3_Sample']}; "
            f"P99={row['P99_Sample']}\n"

        )

        f.write(

            f"SampleCount={row['SampleCount']:,}; "
            f"ZeroCount={row['ZeroCount']:,}; "
            f"NegativeCount={row['NegativeCount']:,}; "
            f"DomainReviewCount={row['DomainReviewCount']:,}\n"

        )

    # -----------------------------------------------------
    # Domain review
    # -----------------------------------------------------

    f.write("\nDOMAIN REVIEW COUNTS\n")

    f.write("-" * 75 + "\n")

    if overall_domain_counts:

        for variable, count in sorted(
            overall_domain_counts.items()
        ):

            f.write(

                f"{variable}: {count:,}\n"

            )

    else:

        f.write("No domain review candidates found.\n")

    # -----------------------------------------------------
    # Weather code frequency
    # -----------------------------------------------------

    f.write("\nWEATHER CODE FREQUENCY\n")

    f.write("-" * 75 + "\n")

    for row in weather_code_rows:

        f.write(

            f"{row['WeatherCode']}: "
            f"{row['Description']} | "
            f"{row['Count']:,} | "
            f"{row['Percentage']}%\n"

        )

    # -----------------------------------------------------
    # Sampling methodology
    # -----------------------------------------------------

    f.write("\nQUANTILE METHOD\n")

    f.write("-" * 75 + "\n")

    f.write(

        "Quantiles are estimated from deterministic "
        "random sampling at 5% probability per row, "
        "with a separate fixed seed for each airport.\n"

    )

    f.write(

        "Min, max, mean, standard deviation, missingness "
        "and domain counts use all rows.\n"

    )

    f.write(

        "Quantiles are not reported for categorical "
        "or circular variables.\n"

    )

    # -----------------------------------------------------
    # Temperature note
    # -----------------------------------------------------

    f.write("\nTEMPERATURE NOTE\n")

    f.write("-" * 75 + "\n")

    f.write(

        "No universal hard temperature boundary was applied. "
        "Temperature extremes require contextual review.\n"

    )

    # -----------------------------------------------------
    # Wind direction note
    # -----------------------------------------------------

    f.write("\nWIND DIRECTION NOTE\n")

    f.write("-" * 75 + "\n")

    f.write(

        "Wind direction is a circular variable. "
        "Ordinary arithmetic mean, standard deviation "
        "and linear quantiles are not reported.\n"

    )

    # -----------------------------------------------------
    # Final note
    # -----------------------------------------------------

    f.write("\nIMPORTANT\n")

    f.write("-" * 75 + "\n")

    f.write(

        "This script only profiles and exports review "
        "candidates. It does not modify source data "
        "or perform cleaning.\n"

    )


# =========================================================
# 15. CONSOLE SUMMARY
# =========================================================

print("\n========== OPEN-METEO VARIABLE PROFILING ==========")

print(f"Airport files: {len(csv_files)}")

print(f"Expected airports: {EXPECTED_AIRPORTS}")

print(f"Total rows: {total_rows:,}")

print(f"Expected total rows: {EXPECTED_TOTAL_ROWS:,}")

print(f"Random sample rate: {SAMPLE_RATE * 100:.1f}%")

print("\nOVERALL PROFILE:")

for row in overall_rows:

    print(

        f"{row['Variable']}: "
        f"Valid={row['ValidCount']:,}, "
        f"Missing={row['MissingCount']:,}, "
        f"Invalid={row['NonNumericCount'] + row['NonFiniteCount']:,}, "
        f"Min={row['Min']}, "
        f"Max={row['Max']}, "
        f"Mean={row['Mean']}, "
        f"DomainCandidates={row['DomainReviewCount']:,}"

    )

print(

    f"\nTotal domain / invalid candidates: "
    f"{candidate_count:,}"

)

print("\nOUTPUT FILES:")

print(overall_path)

print(airport_path)

print(weather_code_path)

print(candidate_path)

print(summary_path)

print("\n========== COMPLETED ==========")
