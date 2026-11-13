#!/usr/bin/env python3
"""
STEP 30E1 - Load Open-Meteo hourly CLEAN data into MySQL 8.0.46.

Target:
  flight_dw_2025.Fact_Airport_Weather_Hourly

Grain:
  One canonical OurAirportsID x one UTC hour.
  Expected: 352 airports x 8,760 hours = 3,083,520 rows.

Safety:
  - Reads only 04_CLEAN_DATA/OpenMeteo and approved mapping/design CSVs.
  - Does not modify RAW, STAGING, CLEAN, or Bridge CSVs.
  - Does not DROP, TRUNCATE, or DELETE database rows.
  - Uses idempotent INSERT ... ON DUPLICATE KEY UPDATE.
  - Commits in batches and writes one checkpoint only after an airport file
    is loaded and verified in MySQL.
  - A rerun can safely resume: complete files with matching SHA256 and complete
    database coverage are skipped; incomplete files are safely re-upserted.
  - DATETIME values are stored as naive UTC clock values by convention.

Usage:
  python step30e1_load_weather_fact.py --preview
  python step30e1_load_weather_fact.py --execute
"""
from __future__ import annotations

import argparse
import csv
import getpass
import hashlib
import json
import sys
import time
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

ROOT = Path.cwd()
DB_NAME = "flight_dw_2025"
DB_HOST = "127.0.0.1"
DB_PORT = 3306
DB_USER = "root"

OPENMETEO_DIR = ROOT / "04_CLEAN_DATA" / "OpenMeteo"
TIMEZONE_FILE = ROOT / "06_DW_DESIGN" / "01_LOGICAL_MODEL" / "27B3B_airport_timezone_mapping_candidates.csv"
PHYSICAL_DIR = ROOT / "06_DW_DESIGN" / "02_PHYSICAL_MODEL"
STATE_DIR = PHYSICAL_DIR / "30E1_weather_load_state"

EXPECTED_AIRPORTS = 352
HOURS_PER_AIRPORT = 8760
EXPECTED_ROWS = EXPECTED_AIRPORTS * HOURS_PER_AIRPORT
BATCH_SIZE_DEFAULT = 1000
START_UTC = datetime(2025, 1, 1, 0, 0, 0)
END_UTC = datetime(2026, 1, 1, 0, 0, 0)

EXPECTED_COLUMNS = [
    "AirportCode", "time", "temperature_2m", "relative_humidity_2m",
    "precipitation", "wind_speed_10m", "wind_direction_10m",
    "wind_gusts_10m", "cloud_cover", "weather_code", "Latitude",
    "Longitude", "TimezoneRequested", "WeatherModelRequested",
]

INSERT_SQL = """
INSERT INTO Fact_Airport_Weather_Hourly (
    AirportKey, OurAirportsID, WeatherDateKey, WeatherTimeKey, WeatherHourUTC,
    Temperature2m, RelativeHumidity2m, Precipitation, WindSpeed10m,
    WindDirection10m, WindGusts10m, CloudCover, WeatherCode,
    Latitude, Longitude, TimezoneRequested, WeatherModelRequested
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s
)
ON DUPLICATE KEY UPDATE
    AirportKey = VALUES(AirportKey),
    WeatherDateKey = VALUES(WeatherDateKey),
    WeatherTimeKey = VALUES(WeatherTimeKey),
    Temperature2m = VALUES(Temperature2m),
    RelativeHumidity2m = VALUES(RelativeHumidity2m),
    Precipitation = VALUES(Precipitation),
    WindSpeed10m = VALUES(WindSpeed10m),
    WindDirection10m = VALUES(WindDirection10m),
    WindGusts10m = VALUES(WindGusts10m),
    CloudCover = VALUES(CloudCover),
    WeatherCode = VALUES(WeatherCode),
    Latitude = VALUES(Latitude),
    Longitude = VALUES(Longitude),
    TimezoneRequested = VALUES(TimezoneRequested),
    WeatherModelRequested = VALUES(WeatherModelRequested)
"""


def say(message: str) -> None:
    print(message, flush=True)


def clean_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def require_file(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"Required file not found: {path}")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_decimal(value: Any, field: str, row_number: int) -> Decimal:
    raw = clean_text(value)
    if raw == "":
        raise ValueError(f"Blank required numeric field {field} at row {row_number}")
    try:
        result = Decimal(raw)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid decimal {field}={raw!r} at row {row_number}") from exc
    if not result.is_finite():
        raise ValueError(f"Non-finite decimal {field}={raw!r} at row {row_number}")
    return result


def quantize(value: Decimal, places: str) -> Decimal:
    return value.quantize(Decimal(places), rounding=ROUND_HALF_UP)


def parse_uint(value: Any, field: str, row_number: int, minimum: int, maximum: int) -> int:
    raw = clean_text(value)
    try:
        parsed = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid integer {field}={raw!r} at row {row_number}") from exc
    if str(parsed) != raw and raw not in (f"+{parsed}",):
        # Reject decimal strings such as "360.0" rather than silently coercing.
        raise ValueError(f"Non-canonical integer {field}={raw!r} at row {row_number}")
    if not minimum <= parsed <= maximum:
        raise ValueError(
            f"{field} outside target range [{minimum}, {maximum}] at row {row_number}: {parsed}"
        )
    return parsed


def parse_utc_hour(value: Any, row_number: int) -> datetime:
    raw = clean_text(value)
    if not raw.endswith("Z"):
        raise ValueError(f"Timestamp is not explicit UTC (missing trailing Z) at row {row_number}: {raw!r}")
    try:
        parsed = datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise ValueError(f"Invalid hourly UTC timestamp at row {row_number}: {raw!r}") from exc
    if parsed.minute != 0 or parsed.second != 0:
        raise ValueError(f"Weather timestamp is not on an exact UTC hour at row {row_number}: {raw!r}")
    return parsed


def read_timezone_candidates() -> dict[str, int]:
    require_file(TIMEZONE_FILE)
    result: dict[str, int] = {}
    with TIMEZONE_FILE.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"BTSAirportCode", "OurAirportsID", "TimeZoneIANA",
                    "TimezoneLookupStatus", "CandidateStatus"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Timezone candidate file missing columns: {sorted(missing)}")
        for row_number, row in enumerate(reader, start=2):
            code = clean_text(row.get("BTSAirportCode")).upper()
            airport_id = parse_uint(row.get("OurAirportsID"), "OurAirportsID", row_number, 1, 2**32 - 1)
            if len(code) != 3 or code in result:
                raise ValueError(f"Invalid/duplicate airport code in timezone candidates at row {row_number}: {code!r}")
            if not clean_text(row.get("TimeZoneIANA")):
                raise ValueError(f"Blank TimeZoneIANA for {code}")
            if clean_text(row.get("TimezoneLookupStatus")) != "LOOKUP_OK_UNAMBIGUOUS":
                raise ValueError(f"Timezone not unambiguous for {code}")
            if clean_text(row.get("CandidateStatus")) != "CANDIDATE_READY_FOR_REVIEW":
                raise ValueError(f"Unexpected candidate status for {code}")
            result[code] = airport_id
    if len(result) != EXPECTED_AIRPORTS:
        raise ValueError(f"Expected {EXPECTED_AIRPORTS} timezone candidates; found {len(result)}")
    return result


def locate_weather_files(expected_codes: set[str]) -> dict[str, Path]:
    if not OPENMETEO_DIR.is_dir():
        raise FileNotFoundError(f"Open-Meteo CLEAN directory not found: {OPENMETEO_DIR}")
    found: dict[str, Path] = {}
    for path in OPENMETEO_DIR.glob("weather_*_2025.csv"):
        parts = path.stem.split("_")
        if len(parts) != 3 or parts[0].lower() != "weather" or len(parts[1]) != 3:
            continue
        code = parts[1].upper()
        if code in found:
            raise ValueError(f"Duplicate Open-Meteo file for airport code {code}")
        found[code] = path
    missing = sorted(expected_codes - set(found))
    extra = sorted(set(found) - expected_codes)
    if missing or extra:
        raise ValueError(f"Open-Meteo file set mismatch; missing={missing[:20]}, extra={extra[:20]}")
    return found


def validate_weather_file(code: str, path: Path, expected_airport_id: int) -> dict[str, Any]:
    """Full streaming validation. Returns row count, hashes, range and stable coordinates."""
    file_hash = sha256_file(path)
    rows = 0
    first_ts: datetime | None = None
    last_ts: datetime | None = None
    latitude: Decimal | None = None
    longitude: Decimal | None = None
    timezone_requested: str | None = None
    model_requested: str | None = None
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != EXPECTED_COLUMNS:
            raise ValueError(
                f"Unexpected Open-Meteo schema in {path.name}: {reader.fieldnames}"
            )
        expected_ts = START_UTC
        for row_number, row in enumerate(reader, start=2):
            row_code = clean_text(row.get("AirportCode")).upper()
            if row_code != code:
                raise ValueError(f"AirportCode mismatch in {path.name}:{row_number}: {row_code!r}")
            ts = parse_utc_hour(row.get("time"), row_number)
            if ts != expected_ts:
                raise ValueError(
                    f"UTC sequence gap/order error in {path.name}:{row_number}; "
                    f"expected {expected_ts.isoformat()}Z, found {ts.isoformat()}Z"
                )
            if ts < START_UTC or ts >= END_UTC:
                raise ValueError(f"Timestamp outside 2025 in {path.name}:{row_number}: {ts}")
            temperature = parse_decimal(row.get("temperature_2m"), "temperature_2m", row_number)
            humidity = parse_uint(row.get("relative_humidity_2m"), "relative_humidity_2m", row_number, 0, 100)
            precipitation = parse_decimal(row.get("precipitation"), "precipitation", row_number)
            wind_speed = parse_decimal(row.get("wind_speed_10m"), "wind_speed_10m", row_number)
            wind_direction = parse_uint(row.get("wind_direction_10m"), "wind_direction_10m", row_number, 0, 360)
            wind_gusts = parse_decimal(row.get("wind_gusts_10m"), "wind_gusts_10m", row_number)
            cloud_cover = parse_uint(row.get("cloud_cover"), "cloud_cover", row_number, 0, 100)
            weather_code = parse_uint(row.get("weather_code"), "weather_code", row_number, 0, 65535)
            lat = parse_decimal(row.get("Latitude"), "Latitude", row_number)
            lon = parse_decimal(row.get("Longitude"), "Longitude", row_number)
            if not Decimal("-90") <= lat <= Decimal("90"):
                raise ValueError(f"Latitude out of range in {path.name}:{row_number}")
            if not Decimal("-180") <= lon <= Decimal("180"):
                raise ValueError(f"Longitude out of range in {path.name}:{row_number}")
            if latitude is None:
                latitude, longitude = lat, lon
            elif lat != latitude or lon != longitude:
                raise ValueError(f"Open-Meteo coordinates change within {path.name}")
            tz = clean_text(row.get("TimezoneRequested"))
            model = clean_text(row.get("WeatherModelRequested"))
            if not tz or len(tz) > 16 or not model or len(model) > 32:
                raise ValueError(f"Invalid timezone/model metadata in {path.name}:{row_number}")
            if timezone_requested is None:
                timezone_requested, model_requested = tz, model
            elif tz != timezone_requested or model != model_requested:
                raise ValueError(f"Timezone/model request metadata changes within {path.name}")
            if len(quantize(temperature, "0.01").as_tuple().digits) > 6:
                raise ValueError(f"temperature_2m cannot fit DECIMAL(6,2) at {path.name}:{row_number}")
            if precipitation < 0 or precipitation > Decimal("999999.99"):
                raise ValueError(f"precipitation outside DECIMAL(8,2) at {path.name}:{row_number}")
            if wind_speed < 0 or wind_speed > Decimal("99999.99"):
                raise ValueError(f"wind_speed_10m outside DECIMAL(7,2) at {path.name}:{row_number}")
            if wind_gusts < 0 or wind_gusts > Decimal("99999.99"):
                raise ValueError(f"wind_gusts_10m outside DECIMAL(7,2) at {path.name}:{row_number}")
            if abs(lat) > Decimal("999.999999999") or abs(lon) > Decimal("999.999999999"):
                raise ValueError(f"Coordinate cannot fit DECIMAL(12,9) at {path.name}:{row_number}")
            rows += 1
            if first_ts is None:
                first_ts = ts
            last_ts = ts
            expected_ts += timedelta(hours=1)
        if expected_ts != END_UTC:
            raise ValueError(
                f"{path.name}: expected {HOURS_PER_AIRPORT:,} hourly rows; "
                f"found {rows:,}; next expected timestamp={expected_ts}"
            )
    if rows != HOURS_PER_AIRPORT:
        raise ValueError(f"{path.name}: row count {rows} != {HOURS_PER_AIRPORT}")
    return {
        "AirportCode": code,
        "OurAirportsID": expected_airport_id,
        "Rows": rows,
        "SHA256": file_hash,
        "FirstWeatherHourUTC": first_ts.strftime("%Y-%m-%d %H:%M:%S") if first_ts else None,
        "LastWeatherHourUTC": last_ts.strftime("%Y-%m-%d %H:%M:%S") if last_ts else None,
        "Latitude": str(latitude),
        "Longitude": str(longitude),
        "TimezoneRequested": timezone_requested,
        "WeatherModelRequested": model_requested,
    }


def run_source_preflight() -> tuple[dict[str, int], dict[str, Path], list[dict[str, Any]]]:
    say("STEP 30E1 - OPEN-METEO WEATHER FACT PREFLIGHT")
    say("=" * 78)
    by_code = read_timezone_candidates()
    expected_codes = set(by_code)
    files = locate_weather_files(expected_codes)
    say(f"[1/2] Inventory: {len(files)} Open-Meteo CLEAN files match the {len(expected_codes)} required BTS airport codes.")
    summaries: list[dict[str, Any]] = []
    total = 0
    for index, code in enumerate(sorted(files), start=1):
        summary = validate_weather_file(code, files[code], by_code[code])
        summaries.append(summary)
        total += summary["Rows"]
        if index == 1 or index % 25 == 0 or index == len(files):
            say(f"      Validated {index}/{len(files)} files; {total:,}/{EXPECTED_ROWS:,} hourly rows.")
    if total != EXPECTED_ROWS:
        raise ValueError(f"Total weather row mismatch: {total:,} != {EXPECTED_ROWS:,}")
    say(f"[2/2] Full source validation PASS: {total:,} rows; 0 schema/sequence/width errors.")
    return by_code, files, summaries


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.flush()
    temp.replace(path)


def write_reports(run_id: str, mode: str, result: str, summaries: list[dict[str, Any]],
                  extra: dict[str, Any] | None = None) -> tuple[Path, Path]:
    PHYSICAL_DIR.mkdir(parents=True, exist_ok=True)
    txt_path = PHYSICAL_DIR / f"30E1_weather_fact_{mode.lower()}_{run_id}.txt"
    json_path = PHYSICAL_DIR / f"30E1_weather_fact_{mode.lower()}_{run_id}.json"
    total = sum(item.get("Rows", 0) for item in summaries)
    payload = {
        "Step": "30E1",
        "Mode": mode,
        "Result": result,
        "RunID": run_id,
        "TargetDatabase": DB_NAME,
        "TargetTable": "Fact_Airport_Weather_Hourly",
        "ExpectedAirports": EXPECTED_AIRPORTS,
        "ExpectedRows": EXPECTED_ROWS,
        "FilesValidated": len(summaries),
        "RowsValidated": total,
        "FileSummaries": summaries,
    }
    if extra:
        payload.update(extra)
    atomic_json(json_path, payload)
    lines = [
        "STEP 30E1 - OPEN-METEO WEATHER FACT",
        "=" * 78,
        f"RunID: {run_id}",
        f"Mode: {mode}",
        f"Result: {result}",
        f"Target: {DB_NAME}.Fact_Airport_Weather_Hourly",
        f"FilesValidated: {len(summaries)} / {EXPECTED_AIRPORTS}",
        f"RowsValidated: {total:,} / {EXPECTED_ROWS:,}",
    ]
    if extra:
        for key, value in extra.items():
            lines.append(f"{key}: {value}")
    lines.extend(["", "FILE VALIDATION", "-" * 78])
    for item in summaries:
        lines.append(
            f"{item['AirportCode']}: rows={item['Rows']}; "
            f"OurAirportsID={item['OurAirportsID']}; SHA256={item['SHA256']}"
        )
    lines.append(f"\nFINAL RESULT: {result}")
    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return txt_path, json_path


def load_checkpoints() -> dict[str, dict[str, Any]]:
    if not STATE_DIR.exists():
        return {}
    result: dict[str, dict[str, Any]] = {}
    for path in STATE_DIR.glob("checkpoint_*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            code = clean_text(payload.get("AirportCode")).upper()
            if code:
                result[code] = payload
        except (OSError, json.JSONDecodeError):
            continue
    return result


def checkpoint_path(code: str) -> Path:
    return STATE_DIR / f"checkpoint_{code}.json"


def connect_mysql(password: str, host: str, port: int, user: str):
    try:
        import mysql.connector
    except ImportError as exc:
        raise RuntimeError(
            "mysql-connector-python is not installed in this virtual environment. "
            "Run: python -m pip install mysql-connector-python"
        ) from exc
    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=DB_NAME,
        charset="utf8mb4",
        collation="utf8mb4_0900_ai_ci",
        autocommit=False,
        connection_timeout=30,
    )


def fetch_db_mappings(cursor) -> dict[str, tuple[int, int]]:
    cursor.execute("""
        SELECT BTSAirportCode, AirportKey, OurAirportsID
        FROM Map_BTS_Airport
        ORDER BY BTSAirportCode
    """)
    mapping = {str(code): (int(airport_key), int(airport_id))
               for code, airport_key, airport_id in cursor.fetchall()}
    if len(mapping) != EXPECTED_AIRPORTS:
        raise ValueError(f"Map_BTS_Airport expected {EXPECTED_AIRPORTS} rows; found {len(mapping)}")
    return mapping


def validate_database_prerequisites(cursor, timezone_by_code: dict[str, int]) -> dict[str, Any]:
    cursor.execute("SELECT COUNT(*) FROM Dim_Airport")
    airport_rows = int(cursor.fetchone()[0])
    cursor.execute("SELECT COUNT(*) FROM Map_BTS_Airport")
    mapping_rows = int(cursor.fetchone()[0])
    cursor.execute("SELECT COUNT(*) FROM Dim_Date WHERE CalendarDate BETWEEN '2025-01-01' AND '2025-12-31'")
    date_rows = int(cursor.fetchone()[0])
    cursor.execute("SELECT COUNT(*) FROM Dim_Time WHERE MinuteOfDay IN (0,60,120,180,240,300,360,420,480,540,600,660,720,780,840,900,960,1020,1080,1140,1200,1260,1320,1380)")
    hour_time_rows = int(cursor.fetchone()[0])
    if airport_rows != 86335:
        raise ValueError(f"Dim_Airport expected 86,335 rows; found {airport_rows}")
    if mapping_rows != EXPECTED_AIRPORTS:
        raise ValueError(f"Map_BTS_Airport expected 352 rows; found {mapping_rows}")
    if date_rows != 365:
        raise ValueError(f"Dim_Date must contain all 365 2025 dates; found {date_rows}")
    if hour_time_rows != 24:
        raise ValueError(f"Dim_Time must contain all 24 top-of-hour keys; found {hour_time_rows}")
    mapping = fetch_db_mappings(cursor)
    for code, our_id in timezone_by_code.items():
        if code not in mapping:
            raise ValueError(f"Missing BTS airport mapping in MySQL: {code}")
        if mapping[code][1] != our_id:
            raise ValueError(
                f"OurAirportsID mismatch for {code}: database={mapping[code][1]}, "
                f"timezone map={our_id}"
            )
    cursor.execute("SELECT COUNT(*) FROM Fact_Airport_Weather_Hourly")
    existing_rows = int(cursor.fetchone()[0])
    if existing_rows > EXPECTED_ROWS:
        raise ValueError(f"Target table has too many rows: {existing_rows:,} > {EXPECTED_ROWS:,}")
    return {
        "DimAirportRows": airport_rows,
        "MapBTSAirportRows": mapping_rows,
        "DimDate2025Rows": date_rows,
        "DimTimeTopOfHourRows": hour_time_rows,
        "ExistingWeatherFactRows": existing_rows,
        "AirportMapVerified": len(mapping),
    }


def make_insert_tuple(row: dict[str, str], airport_key: int, ourairports_id: int,
                      row_number: int) -> tuple[Any, ...]:
    ts = parse_utc_hour(row.get("time"), row_number)
    date_key = int(ts.strftime("%Y%m%d"))
    minute_of_day = ts.hour * 60
    time_key = minute_of_day + 1  # Dim_Time.TimeKey = MinuteOfDay + 1; 0 is Unknown.
    temp = quantize(parse_decimal(row.get("temperature_2m"), "temperature_2m", row_number), "0.01")
    humidity = parse_uint(row.get("relative_humidity_2m"), "relative_humidity_2m", row_number, 0, 100)
    precipitation = quantize(parse_decimal(row.get("precipitation"), "precipitation", row_number), "0.01")
    wind_speed = quantize(parse_decimal(row.get("wind_speed_10m"), "wind_speed_10m", row_number), "0.01")
    wind_direction = parse_uint(row.get("wind_direction_10m"), "wind_direction_10m", row_number, 0, 360)
    wind_gusts = quantize(parse_decimal(row.get("wind_gusts_10m"), "wind_gusts_10m", row_number), "0.01")
    cloud_cover = parse_uint(row.get("cloud_cover"), "cloud_cover", row_number, 0, 100)
    weather_code = parse_uint(row.get("weather_code"), "weather_code", row_number, 0, 65535)
    latitude = quantize(parse_decimal(row.get("Latitude"), "Latitude", row_number), "0.000000001")
    longitude = quantize(parse_decimal(row.get("Longitude"), "Longitude", row_number), "0.000000001")
    tz = clean_text(row.get("TimezoneRequested"))
    model = clean_text(row.get("WeatherModelRequested"))
    if not tz or len(tz) > 16 or not model or len(model) > 32:
        raise ValueError(f"Invalid request metadata at source row {row_number}")
    return (
        airport_key, ourairports_id, date_key, time_key, ts,
        temp, humidity, precipitation, wind_speed, wind_direction,
        wind_gusts, cloud_cover, weather_code, latitude, longitude, tz, model,
    )


def db_file_coverage(cursor, ourairports_id: int) -> tuple[int, int, datetime | None, datetime | None]:
    cursor.execute("""
        SELECT COUNT(*), COUNT(DISTINCT WeatherHourUTC),
               MIN(WeatherHourUTC), MAX(WeatherHourUTC)
        FROM Fact_Airport_Weather_Hourly
        WHERE OurAirportsID = %s
    """, (ourairports_id,))
    row_count, distinct_count, minimum, maximum = cursor.fetchone()
    return (
        int(row_count), int(distinct_count),
        minimum if isinstance(minimum, datetime) else minimum,
        maximum if isinstance(maximum, datetime) else maximum,
    )


def load_one_file(cursor, connection, code: str, path: Path,
                  airport_key: int, ourairports_id: int,
                  batch_size: int) -> dict[str, Any]:
    source_hash = sha256_file(path)
    count = 0
    batch: list[tuple[Any, ...]] = []
    first_ts: datetime | None = None
    last_ts: datetime | None = None
    expected_ts = START_UTC
    started = time.time()

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != EXPECTED_COLUMNS:
            raise ValueError(f"Unexpected schema during load: {path.name}")
        for row_number, row in enumerate(reader, start=2):
            if clean_text(row.get("AirportCode")).upper() != code:
                raise ValueError(f"AirportCode mismatch during load: {path.name}:{row_number}")
            ts = parse_utc_hour(row.get("time"), row_number)
            if ts != expected_ts:
                raise ValueError(
                    f"UTC sequence changed since preflight in {path.name}:{row_number}; "
                    f"expected {expected_ts}, found {ts}"
                )
            batch.append(make_insert_tuple(row, airport_key, ourairports_id, row_number))
            expected_ts += timedelta(hours=1)
            count += 1
            if first_ts is None:
                first_ts = ts
            last_ts = ts
            if len(batch) >= batch_size:
                cursor.executemany(INSERT_SQL, batch)
                connection.commit()
                batch.clear()
    if batch:
        cursor.executemany(INSERT_SQL, batch)
        connection.commit()
        batch.clear()

    if count != HOURS_PER_AIRPORT or expected_ts != END_UTC:
        raise ValueError(f"{path.name}: loaded source rows {count} != {HOURS_PER_AIRPORT}")

    db_count, db_distinct, db_min, db_max = db_file_coverage(cursor, ourairports_id)
    expected_min = START_UTC
    expected_max = END_UTC - timedelta(hours=1)
    if (db_count != HOURS_PER_AIRPORT or db_distinct != HOURS_PER_AIRPORT
            or db_min != expected_min or db_max != expected_max):
        raise ValueError(
            f"Database coverage check failed for {code}/OurAirportsID={ourairports_id}: "
            f"rows={db_count}, distinct={db_distinct}, min={db_min}, max={db_max}"
        )

    payload = {
        "AirportCode": code,
        "AirportKey": airport_key,
        "OurAirportsID": ourairports_id,
        "SourceFile": path.name,
        "SourceSHA256": source_hash,
        "SourceRowsProcessed": count,
        "DatabaseRowsForAirport": db_count,
        "DatabaseDistinctHours": db_distinct,
        "FirstWeatherHourUTC": first_ts.strftime("%Y-%m-%d %H:%M:%S") if first_ts else None,
        "LastWeatherHourUTC": last_ts.strftime("%Y-%m-%d %H:%M:%S") if last_ts else None,
        "Status": "PASS",
    }
    payload["ElapsedSeconds"] = round(time.time() - started, 3)
    atomic_json(checkpoint_path(code), payload)
    return payload


def can_skip_checkpoint(cursor, checkpoint: dict[str, Any], code: str,
                        path: Path, airport_key: int, ourairports_id: int) -> bool:
    if not checkpoint or checkpoint.get("Status") != "PASS":
        return False
    if checkpoint.get("SourceSHA256") != sha256_file(path):
        return False
    if int(checkpoint.get("AirportKey", -1)) != airport_key:
        return False
    if int(checkpoint.get("OurAirportsID", -1)) != ourairports_id:
        return False
    db_count, db_distinct, db_min, db_max = db_file_coverage(cursor, ourairports_id)
    return (
        db_count == HOURS_PER_AIRPORT
        and db_distinct == HOURS_PER_AIRPORT
        and db_min == START_UTC
        and db_max == END_UTC - timedelta(hours=1)
    )


def run(args: argparse.Namespace) -> int:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    summaries: list[dict[str, Any]] = []
    connection = None
    try:
        timezone_by_code, files, source_summaries = run_source_preflight()
        if args.preview:
            text_report, json_report = write_reports(
                run_id, "PREVIEW", "PASS", source_summaries,
                {"MySQLRowsInsertedOrUpdated": 0,
                 "Message": "Source preflight only; no MySQL connection or writes performed."}
            )
            say("")
            say("SOURCE PREFLIGHT RESULT: PASS")
            say("STEP 30E1 PREVIEW RESULT: PASS")
            say("No MySQL rows were inserted or updated.")
            say(f"Text report: {text_report}")
            say(f"JSON report: {json_report}")
            say("STEP 30E1 COMPLETE")
            return 0

        say("")
        password = getpass.getpass(f"MySQL password for {args.user}@{args.host}:{args.port}: ")
        connection = connect_mysql(password, args.host, args.port, args.user)
        cursor = connection.cursor()
        cursor.execute("SELECT DATABASE(), VERSION()")
        database, version = cursor.fetchone()
        if database != DB_NAME:
            raise ValueError(f"Connected to unexpected database: {database!r}")
        say(f"Connected database: {database}; MySQL: {version}")

        db_prereq = validate_database_prerequisites(cursor, timezone_by_code)
        say("[DB] Prerequisites PASS: "
            f"Dim_Airport={db_prereq['DimAirportRows']:,}; "
            f"Map_BTS_Airport={db_prereq['MapBTSAirportRows']}; "
            f"Dim_Date_2025={db_prereq['DimDate2025Rows']}; "
            f"Dim_Time_hour_keys={db_prereq['DimTimeTopOfHourRows']}.")
        cursor.execute("""
            SELECT BTSAirportCode, AirportKey, OurAirportsID
            FROM Map_BTS_Airport
        """)
        airport_map = {
            str(code): (int(airport_key), int(airport_id))
            for code, airport_key, airport_id in cursor.fetchall()
        }

        checkpoints = load_checkpoints()
        skipped = 0
        processed = 0
        say("[ETL] Loading hourly weather by airport; batch size="
            f"{args.batch_size:,}; existing target rows={db_prereq['ExistingWeatherFactRows']:,}.")
        for index, code in enumerate(sorted(files), start=1):
            airport_key, airport_id = airport_map[code]
            prior = checkpoints.get(code, {})
            if can_skip_checkpoint(cursor, prior, code, files[code], airport_key, airport_id):
                skipped += 1
                if skipped == 1 or skipped % 25 == 0 or index == len(files):
                    say(f"      {code}: checkpoint verified; skipped ({skipped} files skipped).")
                summaries.append({
                    "AirportCode": code, "OurAirportsID": airport_id,
                    "Rows": HOURS_PER_AIRPORT, "SHA256": prior.get("SourceSHA256"),
                    "Status": "SKIPPED_VERIFIED_CHECKPOINT",
                })
                continue

            item = load_one_file(
                cursor, connection, code, files[code],
                airport_key, airport_id, args.batch_size
            )
            summaries.append({
                "AirportCode": code,
                "OurAirportsID": airport_id,
                "Rows": item["SourceRowsProcessed"],
                "SHA256": item["SourceSHA256"],
                "Status": "PASS",
            })
            processed += 1
            if processed == 1 or processed % 10 == 0 or index == len(files):
                say(
                    f"      {code}: {item['SourceRowsProcessed']:,} rows committed and verified; "
                    f"files processed this run={processed}; checkpoint skips={skipped}."
                )

        cursor.execute("SELECT COUNT(*) FROM Fact_Airport_Weather_Hourly")
        final_count = int(cursor.fetchone()[0])
        if final_count != EXPECTED_ROWS:
            raise ValueError(
                f"Final Fact_Airport_Weather_Hourly row count mismatch: "
                f"{final_count:,} != {EXPECTED_ROWS:,}"
            )
        cursor.execute("""
            SELECT COUNT(*), COUNT(DISTINCT CONCAT(OurAirportsID, '|', WeatherHourUTC))
            FROM Fact_Airport_Weather_Hourly
        """)
        final_rows, final_natural_keys = (int(x) for x in cursor.fetchone())
        if final_rows != final_natural_keys:
            raise ValueError(
                f"Duplicate airport-hour natural keys detected: rows={final_rows}, distinct={final_natural_keys}"
            )

        extra = {
            "MySQLVersion": version,
            "TargetRowsBeforeLoad": db_prereq["ExistingWeatherFactRows"],
            "FilesProcessedThisRun": processed,
            "FilesSkippedByVerifiedCheckpoint": skipped,
            "FinalDatabaseRows": final_count,
            "FinalDistinctAirportHourKeys": final_natural_keys,
            "DimensionAirportRows": db_prereq["DimAirportRows"],
            "DimensionMappingRows": db_prereq["MapBTSAirportRows"],
            "NoSourceFilesModified": True,
        }
        text_report, json_report = write_reports(run_id, "EXECUTE", "PASS", summaries, extra)
        say("")
        say("STEP 30E1 EXECUTE RESULT: PASS")
        say(f"Final Fact_Airport_Weather_Hourly rows: {final_count:,}")
        say(f"Files processed this run: {processed}; verified checkpoint skips: {skipped}")
        say(f"Text report: {text_report}")
        say(f"JSON report: {json_report}")
        say("STEP 30E1 COMPLETE")
        return 0

    except Exception as exc:
        if connection is not None:
            try:
                connection.rollback()
            except Exception:
                pass
        say("")
        say("STEP 30E1 RESULT: FAIL")
        say(f"ERROR: {type(exc).__name__}: {exc}")
        say("No RAW/STAGING/CLEAN source files were modified.")
        try:
            if summaries:
                write_reports(run_id, "EXECUTE" if args.execute else "PREVIEW",
                              "FAIL", summaries,
                              {"Error": f"{type(exc).__name__}: {exc}"})
        except Exception:
            pass
        return 1
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="STEP 30E1 Open-Meteo weather fact ETL")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--preview", action="store_true", help="Full read-only source preflight")
    group.add_argument("--execute", action="store_true", help="Load weather fact into MySQL")
    parser.add_argument("--host", default=DB_HOST)
    parser.add_argument("--port", type=int, default=DB_PORT)
    parser.add_argument("--user", default=DB_USER)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE_DEFAULT)
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 10000:
        parser.error("--batch-size must be between 1 and 10000")
    return args


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
