
from pathlib import Path
from collections import Counter
import csv
import json
import re

# =========================================================
# 1. PATHS
# =========================================================

BASE_DIR = Path(__file__).resolve().parents[2]

CSV_DIR = BASE_DIR / "01_RAW_DATA" / "OpenMeteo" / "csv"
JSON_DIR = BASE_DIR / "01_RAW_DATA" / "OpenMeteo" / "json"

OUTPUT_DIR = BASE_DIR / "02_DATA_PROFILING" / "03_OpenMeteo"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def extract_airport_code(file_path):
    match = re.match(r"weather_(.+)_2025$", file_path.stem, re.IGNORECASE)
    return match.group(1).upper() if match else file_path.stem.upper()


def detect_delimiter(file_path):
    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(8192)

    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
    except csv.Error:
        return ","


# =========================================================
# 2. PROFILE CSV FILES
# =========================================================

csv_files = sorted(CSV_DIR.glob("*.csv"))
csv_profiles = {}
header_counter = Counter()

for file_path in csv_files:
    code = extract_airport_code(file_path)
    delimiter = detect_delimiter(file_path)

    row_count = 0
    empty_lines = 0
    bad_width_rows = 0
    min_time = None
    max_time = None
    time_column = None

    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)

        header = next(reader, [])
        header = [str(col).strip() for col in header]
        header_counter[tuple(header)] += 1

        time_candidates = {
            "time", "datetime", "date", "timestamp", "valid_time"
        }

        for col in header:
            if col.lower() in time_candidates:
                time_column = col
                break

        time_index = header.index(time_column) if time_column else None

        for row in reader:
            if not row:
                empty_lines += 1
                continue

            row_count += 1

            if len(row) != len(header):
                bad_width_rows += 1

            if time_index is not None and time_index < len(row):
                value = row[time_index].strip()

                if value:
                    if min_time is None or value < min_time:
                        min_time = value

                    if max_time is None or value > max_time:
                        max_time = value

    csv_profiles[code] = {
        "exists": True,
        "size": file_path.stat().st_size,
        "rows": row_count,
        "columns": len(header),
        "header": header,
        "delimiter": delimiter,
        "time_column": time_column or "",
        "min_time": min_time or "",
        "max_time": max_time or "",
        "bad_width_rows": bad_width_rows,
        "empty_lines": empty_lines,
    }


# =========================================================
# 3. PROFILE JSON FILES
# =========================================================

def describe_json_structure(data):
    top_type = type(data).__name__
    top_keys = []
    details = []

    if isinstance(data, dict):
        top_keys = list(data.keys())

        for key, value in data.items():
            value_type = type(value).__name__

            if isinstance(value, dict):
                details.append(
                    f"{key}: object; keys={list(value.keys())[:15]}"
                )

            elif isinstance(value, list):
                sample = value[0] if value else None
                sample_type = (
                    type(sample).__name__ if sample is not None else "empty"
                )
                sample_keys = (
                    list(sample.keys())[:15]
                    if isinstance(sample, dict)
                    else []
                )

                details.append(
                    f"{key}: array; length={len(value)}; "
                    f"first_item_type={sample_type}; "
                    f"first_item_keys={sample_keys}"
                )

            else:
                details.append(f"{key}: {value_type}")

    elif isinstance(data, list):
        details.append(f"Top-level array length={len(data)}")

    return top_type, top_keys, details


json_files = sorted(JSON_DIR.glob("*.json"))
json_profiles = {}

for file_path in json_files:
    code = extract_airport_code(file_path)

    try:
        with open(file_path, "r", encoding="utf-8-sig") as f:
            data = json.load(f)

        top_type, top_keys, details = describe_json_structure(data)

        json_profiles[code] = {
            "exists": True,
            "size": file_path.stat().st_size,
            "valid": True,
            "top_type": top_type,
            "top_keys": top_keys,
            "details": details,
            "error": "",
        }

    except Exception as e:
        json_profiles[code] = {
            "exists": True,
            "size": file_path.stat().st_size,
            "valid": False,
            "top_type": "",
            "top_keys": [],
            "details": [],
            "error": str(e),
        }


# =========================================================
# 4. MATCH CSV AND JSON FILES BY AIRPORT CODE
# =========================================================

all_codes = sorted(set(csv_profiles) | set(json_profiles))

inventory_rows = []

for code in all_codes:
    csv_info = csv_profiles.get(code, {})
    json_info = json_profiles.get(code, {})

    inventory_rows.append({
        "AirportCode": code,
        "CSV_Exists": bool(csv_info),
        "CSV_SizeBytes": csv_info.get("size", ""),
        "CSV_DataRows": csv_info.get("rows", ""),
        "CSV_ColumnCount": csv_info.get("columns", ""),
        "CSV_Delimiter": csv_info.get("delimiter", ""),
        "CSV_TimeColumn": csv_info.get("time_column", ""),
        "CSV_MinTime": csv_info.get("min_time", ""),
        "CSV_MaxTime": csv_info.get("max_time", ""),
        "CSV_BadWidthRows": csv_info.get("bad_width_rows", ""),
        "CSV_EmptyLines": csv_info.get("empty_lines", ""),
        "JSON_Exists": bool(json_info),
        "JSON_SizeBytes": json_info.get("size", ""),
        "JSON_Valid": json_info.get("valid", ""),
        "JSON_TopLevelType": json_info.get("top_type", ""),
        "JSON_TopLevelKeys": " | ".join(
            map(str, json_info.get("top_keys", []))
        ),
        "JSON_Error": json_info.get("error", ""),
    })


inventory_path = OUTPUT_DIR / "20_openmeteo_inventory.csv"

with open(inventory_path, "w", encoding="utf-8-sig", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=inventory_rows[0].keys())
    writer.writeheader()
    writer.writerows(inventory_rows)


# =========================================================
# 5. CSV SCHEMA FREQUENCY
# =========================================================

schema_rows = []

for header, count in header_counter.items():
    for position, column_name in enumerate(header, start=1):
        schema_rows.append({
            "SchemaFileCount": count,
            "ColumnPosition": position,
            "ColumnName": column_name,
        })

schema_path = OUTPUT_DIR / "21_openmeteo_csv_schema_frequency.csv"

with open(schema_path, "w", encoding="utf-8-sig", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "SchemaFileCount",
            "ColumnPosition",
            "ColumnName",
        ],
    )
    writer.writeheader()
    writer.writerows(schema_rows)


# =========================================================
# 6. JSON STRUCTURE SUMMARY
# =========================================================

json_rows = []

for code, info in sorted(json_profiles.items()):
    json_rows.append({
        "AirportCode": code,
        "ValidJSON": info["valid"],
        "TopLevelType": info["top_type"],
        "TopLevelKeys": " | ".join(map(str, info["top_keys"])),
        "NestedStructure": " || ".join(info["details"]),
        "Error": info["error"],
    })

json_summary_path = OUTPUT_DIR / "22_openmeteo_json_structure.csv"

with open(json_summary_path, "w", encoding="utf-8-sig", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=json_rows[0].keys())
    writer.writeheader()
    writer.writerows(json_rows)


# =========================================================
# 7. DETAILED SAMPLE INSPECTION
# =========================================================

sample_codes = []

if all_codes:
    sample_codes = sorted(set(all_codes))
    indexes = sorted(set([
        0,
        len(sample_codes) // 2,
        len(sample_codes) - 1
    ]))
    sample_codes = [sample_codes[i] for i in indexes]

sample_path = OUTPUT_DIR / "23_openmeteo_sample_inspection.txt"

with open(sample_path, "w", encoding="utf-8") as f:
    for code in sample_codes:
        f.write("=" * 80 + "\n")
        f.write(f"AIRPORT CODE: {code}\n")
        f.write("=" * 80 + "\n\n")

        csv_path = CSV_DIR / f"weather_{code}_2025.csv"

        if csv_path.exists():
            f.write("[CSV]\n")

            with open(
                csv_path, "r", encoding="utf-8-sig", newline=""
            ) as csv_file:
                delimiter = detect_delimiter(csv_path)
                reader = csv.reader(csv_file, delimiter=delimiter)

                for i, row in enumerate(reader):
                    if i >= 6:
                        break
                    f.write(str(row) + "\n")

            f.write("\n")

        json_path = JSON_DIR / f"weather_{code}_2025.json"

        if json_path.exists():
            f.write("[JSON]\n")

            with open(json_path, "r", encoding="utf-8-sig") as jf:
                data = json.load(jf)

            top_type, top_keys, details = describe_json_structure(data)

            f.write(f"Top-level type: {top_type}\n")
            f.write(f"Top-level keys: {top_keys}\n")

            for detail in details:
                f.write(f"- {detail}\n")

            if isinstance(data, dict):
                f.write("\nSample values:\n")

                for key, value in data.items():
                    if isinstance(value, list):
                        f.write(
                            f"{key}: {value[:3]}\n"
                        )
                    elif isinstance(value, dict):
                        f.write(
                            f"{key}: {dict(list(value.items())[:5])}\n"
                        )
                    else:
                        f.write(f"{key}: {value}\n")

            f.write("\n")


# =========================================================
# 8. CONSOLE SUMMARY
# =========================================================

matched_codes = set(csv_profiles) & set(json_profiles)
csv_only = set(csv_profiles) - set(json_profiles)
json_only = set(json_profiles) - set(csv_profiles)

valid_json_count = sum(
    1 for info in json_profiles.values() if info["valid"]
)

print("\n========== OPEN-METEO STRUCTURE PROFILE ==========")
print(f"CSV files: {len(csv_files)}")
print(f"JSON files: {len(json_files)}")
print(f"Unique airport codes: {len(all_codes)}")
print(f"Matched CSV-JSON pairs: {len(matched_codes)}")
print(f"CSV only: {len(csv_only)}")
print(f"JSON only: {len(json_only)}")
print(f"Valid JSON files: {valid_json_count}")
print(f"Different CSV header patterns: {len(header_counter)}")

print("\nCSV schema patterns:")
for header, count in header_counter.items():
    print(f"- {count} files | {len(header)} columns | {list(header)}")

print("\nCSV row-count range:")
if csv_profiles:
    row_counts = [x["rows"] for x in csv_profiles.values()]
    print(f"Min rows: {min(row_counts):,}")
    print(f"Max rows: {max(row_counts):,}")

print("\nOutput files:")
print(inventory_path)
print(schema_path)
print(json_summary_path)
print(sample_path)
