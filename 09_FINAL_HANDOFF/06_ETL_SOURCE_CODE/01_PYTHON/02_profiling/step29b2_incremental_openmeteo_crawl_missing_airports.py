from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


# STEP 29B2 - Incremental Open-Meteo crawl for the airport codes identified by
# STEP 29B1. This script only creates missing RAW JSON/CSV files. It never edits
# existing RAW, STAGING, or CLEAN files.

API_URL = "https://archive-api.open-meteo.com/v1/archive"
START_DATE = "2025-01-01"
END_DATE = "2025-12-31"
REQUESTED_TIMEZONE = "UTC"
REQUESTED_MODEL = "ecmwf_ifs"

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "cloud_cover",
    "weather_code",
]

OUTPUT_COLUMNS = [
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

EXPECTED_HOURS = 8760
EXPECTED_FIRST = "2025-01-01T00:00"
EXPECTED_LAST = "2025-12-31T23:00"


def parse_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def project_root() -> Path:
    # Script location: <root>/08_SCRIPTS/02_profiling/<this_file>.py
    return Path(__file__).resolve().parents[2]


def find_latest_audit(root: Path) -> Path:
    folder = root / "06_DW_DESIGN" / "01_LOGICAL_MODEL"
    matches = sorted(folder.glob("29B1_openmeteo_airport_scope_gap_detail_*.csv"))
    if not matches:
        raise FileNotFoundError(
            "Không tìm thấy báo cáo STEP 29B1 trong "
            f"{folder}. Hãy đặt file 29B1_openmeteo_airport_scope_gap_detail_*.csv vào đây."
        )
    return matches[-1]


def read_targets(audit_path: Path) -> list[dict[str, str]]:
    with audit_path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    required = {
        "AirportCode", "Latitude", "Longitude", "RAW_Valid", "CLEAN_Valid",
        "CoordinatesValid", "MappingMethod", "OurAirports_ID", "AirportOccurrences",
    }
    if not rows:
        raise ValueError(f"File audit rỗng: {audit_path}")
    missing_columns = required - set(rows[0].keys())
    if missing_columns:
        raise ValueError(f"Audit thiếu cột bắt buộc: {sorted(missing_columns)}")

    targets: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        code = row["AirportCode"].strip().upper()
        raw_valid = parse_bool(row["RAW_Valid"])
        clean_valid = parse_bool(row["CLEAN_Valid"])
        if raw_valid and clean_valid:
            continue
        if code in seen:
            raise ValueError(f"AirportCode bị trùng trong audit: {code}")
        seen.add(code)
        if not parse_bool(row["CoordinatesValid"]):
            raise ValueError(f"{code}: tọa độ không hợp lệ; dừng để tránh gọi API sai vị trí.")
        try:
            lat = float(row["Latitude"])
            lon = float(row["Longitude"])
        except (TypeError, ValueError):
            raise ValueError(f"{code}: Latitude/Longitude không chuyển được sang số.")
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            raise ValueError(f"{code}: tọa độ ngoài miền hợp lệ ({lat}, {lon}).")
        if not row["OurAirports_ID"].strip():
            raise ValueError(f"{code}: thiếu OurAirports_ID; dừng để kiểm tra mapping.")
        row = dict(row)
        row["AirportCode"] = code
        row["_latitude"] = str(lat)
        row["_longitude"] = str(lon)
        row["_occurrences"] = str(int(float(row["AirportOccurrences"] or 0)))
        targets.append(row)

    # Process airports with the largest BTS endpoint footprint first. This makes
    # a one-airport pilot meaningful and prioritizes analytical coverage impact.
    targets.sort(key=lambda r: (-int(r["_occurrences"]), r["AirportCode"]))
    return targets


def expected_utc_times() -> list[str]:
    start = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
    return [
        (start + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M")
        for i in range(EXPECTED_HOURS)
    ]


def request_open_meteo(lat: float, lon: float, timeout: int = 90) -> tuple[bytes, dict[str, Any]]:
    params = {
        "latitude": f"{lat:.10f}",
        "longitude": f"{lon:.10f}",
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": ",".join(HOURLY_VARIABLES),
        "models": REQUESTED_MODEL,
        "timezone": REQUESTED_TIMEZONE,
        "timeformat": "iso8601",
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
    }
    url = API_URL + "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "FlightDataWarehouse-STEP29B2/1.0 (educational research)",
            "Accept": "application/json",
        },
        method="GET",
    )

    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read()
            payload = json.loads(body.decode("utf-8"))
            if payload.get("error"):
                raise RuntimeError(f"Open-Meteo API error: {payload.get('reason', payload)}")
            return body, payload
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:1000]
            last_error = RuntimeError(f"HTTP {exc.code}: {detail}")
            if exc.code not in {429, 500, 502, 503, 504}:
                break
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, RuntimeError) as exc:
            last_error = exc
            # JSON/API validation errors are not transient; do not retry those.
            if isinstance(exc, RuntimeError) and "Open-Meteo API error" in str(exc):
                break
        if attempt < 5:
            wait_seconds = min(60, 2 ** attempt)
            print(f"    Retry {attempt}/5 sau lỗi: {last_error}; đợi {wait_seconds}s")
            time.sleep(wait_seconds)

    raise RuntimeError(f"Không tải được dữ liệu sau 5 lần thử: {last_error}")


def validate_payload(payload: dict[str, Any], airport_code: str) -> tuple[list[str], dict[str, list[Any]]]:
    if not isinstance(payload, dict) or "hourly" not in payload:
        raise ValueError(f"{airport_code}: API response không có object 'hourly'.")
    hourly = payload["hourly"]
    if not isinstance(hourly, dict):
        raise ValueError(f"{airport_code}: 'hourly' không phải object.")

    times = hourly.get("time")
    if not isinstance(times, list):
        raise ValueError(f"{airport_code}: hourly.time không phải list.")
    if len(times) != EXPECTED_HOURS:
        raise ValueError(f"{airport_code}: số giờ {len(times)}, kỳ vọng {EXPECTED_HOURS}.")
    if times[0] != EXPECTED_FIRST or times[-1] != EXPECTED_LAST:
        raise ValueError(
            f"{airport_code}: mốc thời gian không đúng: {times[0]} -> {times[-1]}"
        )
    if times != expected_utc_times():
        raise ValueError(f"{airport_code}: chuỗi thời gian không liên tục theo giờ UTC.")

    if payload.get("timezone") not in {"UTC", "GMT"}:
        raise ValueError(f"{airport_code}: API timezone={payload.get('timezone')!r}, không phải UTC.")
    if int(payload.get("utc_offset_seconds", 999999)) != 0:
        raise ValueError(f"{airport_code}: utc_offset_seconds không bằng 0.")

    values: dict[str, list[Any]] = {}
    for variable in HOURLY_VARIABLES:
        series = hourly.get(variable)
        if not isinstance(series, list):
            raise ValueError(f"{airport_code}: thiếu hourly variable {variable}.")
        if len(series) != EXPECTED_HOURS:
            raise ValueError(
                f"{airport_code}: {variable} có {len(series)} giá trị, kỳ vọng {EXPECTED_HOURS}."
            )
        values[variable] = series
    return times, values


def write_raw_pair(
    airport: dict[str, str],
    body: bytes,
    times: list[str],
    values: dict[str, list[Any]],
    raw_csv_dir: Path,
    raw_json_dir: Path,
) -> tuple[Path, Path]:
    code = airport["AirportCode"]
    csv_path = raw_csv_dir / f"weather_{code}_2025.csv"
    json_path = raw_json_dir / f"weather_{code}_2025.json"

    # Fail closed: never overwrite or partially replace an existing airport file.
    conflicts = [p for p in (csv_path, json_path) if p.exists()]
    if conflicts:
        raise FileExistsError("Từ chối ghi đè file đã tồn tại: " + ", ".join(map(str, conflicts)))

    raw_csv_dir.mkdir(parents=True, exist_ok=True)
    raw_json_dir.mkdir(parents=True, exist_ok=True)
    temp_csv = csv_path.with_suffix(csv_path.suffix + ".step29b2_tmp")
    temp_json = json_path.with_suffix(json_path.suffix + ".step29b2_tmp")
    if temp_csv.exists() or temp_json.exists():
        raise FileExistsError(f"Tồn tại file tạm từ lần chạy trước cho {code}; hãy kiểm tra thủ công.")

    try:
        with temp_csv.open("x", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for i, timestamp in enumerate(times):
                row: dict[str, Any] = {
                    "AirportCode": code,
                    "time": timestamp,
                    "Latitude": airport["_latitude"],
                    "Longitude": airport["_longitude"],
                    "TimezoneRequested": REQUESTED_TIMEZONE,
                    "WeatherModelRequested": REQUESTED_MODEL,
                }
                for variable in HOURLY_VARIABLES:
                    value = values[variable][i]
                    row[variable] = "" if value is None else value
                writer.writerow(row)

        with temp_json.open("xb") as f:
            f.write(body)

        # Recheck final destinations before atomic rename.
        if csv_path.exists() or json_path.exists():
            raise FileExistsError(f"File đích vừa xuất hiện trong lúc chạy: {code}")
        temp_json.replace(json_path)
        temp_csv.replace(csv_path)
    except Exception:
        for temp in (temp_csv, temp_json):
            try:
                if temp.exists():
                    temp.unlink()
            except OSError:
                pass
        # If only one final file was moved, do not auto-delete it. Preserve evidence
        # and let the operator inspect the partial pair before retrying.
        raise

    return csv_path, json_path


def write_manifest(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "AirportCode", "OurAirports_ID", "MappingMethod", "HistoricalAliasValidTo",
        "LatitudeRequested", "LongitudeRequested", "AirportOccurrences",
        "Result", "RowsReturned", "FirstTime", "LastTime", "TimezoneReturned",
        "UTCOffsetSeconds", "LatitudeReturned", "LongitudeReturned",
        "RAW_CSV_Path", "RAW_JSON_Path", "Error",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Incremental STEP 29B2 crawl: only missing Open-Meteo airport files."
    )
    parser.add_argument("--audit-csv", type=Path, default=None,
                        help="Đường dẫn CSV detail STEP 29B1; mặc định tự tìm bản mới nhất.")
    parser.add_argument("--execute", action="store_true",
                        help="Thực sự gọi API và ghi RAW. Không có flag này thì chỉ preview.")
    parser.add_argument("--limit", type=int, default=0,
                        help="Giới hạn số sân bay để chạy thử; 0 = toàn bộ.")
    parser.add_argument("--pause-seconds", type=float, default=0.75,
                        help="Khoảng nghỉ giữa các request (mặc định 0.75 giây).")
    args = parser.parse_args()

    root = project_root()
    audit_path = args.audit_csv.resolve() if args.audit_csv else find_latest_audit(root)
    targets = read_targets(audit_path)
    if not targets:
        print("Không có airport scope gap trong audit. Không làm gì.")
        return 0
    if args.limit < 0:
        raise ValueError("--limit phải >= 0")
    if args.pause_seconds < 0:
        raise ValueError("--pause-seconds phải >= 0")
    if args.limit:
        targets = targets[:args.limit]

    raw_csv_dir = root / "01_RAW_DATA" / "OpenMeteo" / "csv"
    raw_json_dir = root / "01_RAW_DATA" / "OpenMeteo" / "json"
    clean_dir = root / "04_CLEAN_DATA" / "OpenMeteo"
    profile_dir = root / "02_DATA_PROFILING" / "03_OpenMeteo"
    profile_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    manifest_path = profile_dir / f"29b2_incremental_openmeteo_crawl_manifest_{stamp}.csv"
    summary_path = profile_dir / f"29b2_incremental_openmeteo_crawl_summary_{stamp}.txt"

    print("=" * 88)
    print("STEP 29B2 - INCREMENTAL OPEN-METEO CRAWL")
    print("=" * 88)
    print(f"Project root : {root}")
    print(f"Audit input  : {audit_path}")
    print(f"Target count : {len(targets)}")
    print(f"API endpoint : {API_URL}")
    print(f"Date range   : {START_DATE} -> {END_DATE}")
    print(f"Model        : requested {REQUESTED_MODEL}")
    print(f"Timezone     : {REQUESTED_TIMEZONE}")
    print(f"Variables    : {', '.join(HOURLY_VARIABLES)}")
    print("Order        : highest BTS airport occurrence first")

    conflicts = []
    for row in targets:
        code = row["AirportCode"]
        candidates = [
            raw_csv_dir / f"weather_{code}_2025.csv",
            raw_json_dir / f"weather_{code}_2025.json",
            clean_dir / f"weather_{code}_2025.csv",
        ]
        existing = [p for p in candidates if p.exists()]
        if existing:
            conflicts.append((code, existing))

    if conflicts:
        print("\nSAFETY STOP: phát hiện file đích đã tồn tại trong scope gap.")
        for code, paths in conflicts:
            print(f"  {code}: " + "; ".join(str(p) for p in paths))
        print("Không tự ghi đè. Hãy chạy lại STEP 29B1 hoặc kiểm tra các file này trước.")
        return 2

    print("\nDanh sách scope:")
    for row in targets:
        print(
            f"  {row['AirportCode']:>3} | occ={int(row['_occurrences']):>8,} | "
            f"lat={float(row['_latitude']): .6f}, lon={float(row['_longitude']): .6f} | "
            f"{row['MappingMethod']}"
        )

    if not args.execute:
        print("\nPREVIEW ONLY: chưa gọi API, chưa tạo RAW file.")
        print("Sau khi kiểm tra danh sách, chạy lại với --execute.")
        return 0

    results: list[dict[str, Any]] = []
    success = 0
    failed = 0
    expected = expected_utc_times()

    for index, airport in enumerate(targets, start=1):
        code = airport["AirportCode"]
        print(f"\n[{index}/{len(targets)}] {code} - {airport.get('AirportName', '')}")
        base_result: dict[str, Any] = {
            "AirportCode": code,
            "OurAirports_ID": airport.get("OurAirports_ID", ""),
            "MappingMethod": airport.get("MappingMethod", ""),
            "HistoricalAliasValidTo": airport.get("HistoricalAliasValidTo", ""),
            "LatitudeRequested": airport["_latitude"],
            "LongitudeRequested": airport["_longitude"],
            "AirportOccurrences": airport["_occurrences"],
            "Result": "FAILED",
            "RowsReturned": "",
            "FirstTime": "",
            "LastTime": "",
            "TimezoneReturned": "",
            "UTCOffsetSeconds": "",
            "LatitudeReturned": "",
            "LongitudeReturned": "",
            "RAW_CSV_Path": "",
            "RAW_JSON_Path": "",
            "Error": "",
        }
        try:
            body, payload = request_open_meteo(float(airport["_latitude"]), float(airport["_longitude"]))
            times, values = validate_payload(payload, code)
            csv_path, json_path = write_raw_pair(
                airport, body, times, values, raw_csv_dir, raw_json_dir
            )
            base_result.update({
                "Result": "SUCCESS",
                "RowsReturned": len(times),
                "FirstTime": times[0],
                "LastTime": times[-1],
                "TimezoneReturned": payload.get("timezone", ""),
                "UTCOffsetSeconds": payload.get("utc_offset_seconds", ""),
                "LatitudeReturned": payload.get("latitude", ""),
                "LongitudeReturned": payload.get("longitude", ""),
                "RAW_CSV_Path": str(csv_path),
                "RAW_JSON_Path": str(json_path),
            })
            success += 1
            print(f"    SUCCESS: {len(times):,} hourly rows; {times[0]} -> {times[-1]}")
        except Exception as exc:
            base_result["Error"] = f"{type(exc).__name__}: {exc}"
            failed += 1
            print(f"    FAILED: {base_result['Error']}")
        results.append(base_result)
        write_manifest(manifest_path, results)
        if index < len(targets) and args.pause_seconds:
            time.sleep(args.pause_seconds)

    # Final summary and manifest are generated even when some airports fail.
    summary_lines = [
        "STEP 29B2 - INCREMENTAL OPEN-METEO CRAWL SUMMARY",
        "=" * 88,
        f"Run timestamp: {stamp}",
        f"Audit input: {audit_path}",
        f"Target airports: {len(targets)}",
        f"Successful airports: {success}",
        f"Failed airports: {failed}",
        f"Expected rows per successful airport: {EXPECTED_HOURS}",
        f"Expected model requested: {REQUESTED_MODEL}",
        f"Expected timezone: {REQUESTED_TIMEZONE}",
        f"RAW CSV folder: {raw_csv_dir}",
        f"RAW JSON folder: {raw_json_dir}",
        f"Manifest: {manifest_path}",
        "",
        "IMPORTANT:",
        "- WeatherModelRequested records the requested model, not independent proof of served model.",
        "- This step creates only missing RAW files; it does not modify existing RAW/STAGING/CLEAN.",
        "- Do not run STEP 29B3 (incremental CLEAN) until the manifest and RAW files are validated.",
    ]
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n" + "=" * 88)
    print(f"STEP 29B2 finished: success={success}, failed={failed}")
    print(f"Manifest: {manifest_path}")
    print(f"Summary : {summary_path}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"FATAL: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(2)
