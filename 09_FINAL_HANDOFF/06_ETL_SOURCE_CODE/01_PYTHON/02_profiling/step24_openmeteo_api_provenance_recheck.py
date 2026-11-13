
"""
STEP 24 - OPEN-METEO WEATHER CODE 96 API PROVENANCE RECHECK

Purpose
-------
Re-query only the airport/date windows that contain original weather_code=96
records in the Bronze Open-Meteo CSV files. The request repeats the original
Historical Weather API endpoint and requested model (ecmwf_ifs), then compares
current API values with the original Bronze values at each flagged timestamp.

IMPORTANT
---------
- This script NEVER modifies files under 01_RAW_DATA.
- WeatherModelRequested in the original CSV is local metadata written by the
  crawler; it is not an API-returned model identifier.
- The Historical Weather API response schema does not expose a dedicated
  actual-model identifier. This script records that limitation explicitly.
- A changed re-query result does not prove the original row was wrong; archive
  data may have been updated since the original download.

Dependencies: pandas, requests
Run from the DATA_WAREHOUSE project (or execute this file from its project location):
    python 08_SCRIPTS/02_profiling/step24_openmeteo_api_provenance_recheck.py
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -----------------------------------------------------------------------------
# PROJECT PATHS AND REQUEST SETTINGS
# -----------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_CSV_DIR = PROJECT_ROOT / "01_RAW_DATA" / "OpenMeteo" / "csv"
OUTPUT_BASE_DIR = PROJECT_ROOT / "02_DATA_PROFILING" / "03_OpenMeteo" / "step24_api_provenance_recheck"

API_URL = "https://archive-api.open-meteo.com/v1/archive"
YEAR = 2025
REQUESTED_MODEL = "ecmwf_ifs"
REQUESTED_TIMEZONE = "UTC"
REQUEST_PAUSE_SECONDS = 1.0
REQUEST_TIMEOUT_SECONDS = 180
NUMERIC_TOLERANCE = 1e-6
EXPECTED_AIRPORT_FILES = 240
TARGET_WEATHER_CODE = 96

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

# Original CSV names use weather_code; the API uses the same variable name.
COMPARE_VARIABLES = WEATHER_VARIABLES.copy()

MODEL_METADATA_KEYS = {
    "model",
    "models",
    "model_name",
    "model_id",
    "model_identifier",
    "weather_model",
}


# -----------------------------------------------------------------------------
# LOGGING / HTTP SESSION
# -----------------------------------------------------------------------------
def build_logger(run_dir: Path) -> logging.Logger:
    run_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("step24_openmeteo_provenance")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    file_handler = logging.FileHandler(run_dir / "step24_execution.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "FlightDWStudentResearch/1.0"})
    return session


# -----------------------------------------------------------------------------
# READ ORIGINAL BRONZE RECORDS (READ-ONLY)
# -----------------------------------------------------------------------------
def load_code96_events(logger: logging.Logger) -> tuple[pd.DataFrame, int]:
    if not RAW_CSV_DIR.exists():
        raise FileNotFoundError(f"Open-Meteo raw CSV directory not found: {RAW_CSV_DIR}")

    files = sorted(RAW_CSV_DIR.glob(f"weather_*_{YEAR}.csv"))
    if not files:
        raise FileNotFoundError(f"No weather CSV files found in {RAW_CSV_DIR}")

    events: list[pd.DataFrame] = []
    required_columns = {
        "AirportCode", "time", "weather_code", "Latitude", "Longitude",
        "TimezoneRequested", "WeatherModelRequested",
    }
    total_files = len(files)

    for index, path in enumerate(files, start=1):
        logger.info("[%s/%s] Reading Bronze file: %s", index, total_files, path.name)
        df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
        missing = required_columns - set(df.columns)
        if missing:
            raise ValueError(f"{path.name} is missing required columns: {sorted(missing)}")

        codes = pd.to_numeric(df["weather_code"], errors="coerce")
        selected = df.loc[codes.eq(TARGET_WEATHER_CODE)].copy()
        if not selected.empty:
            selected["SourceFile"] = path.name
            selected["weather_code"] = pd.to_numeric(selected["weather_code"], errors="coerce")
            for col in [
                "Latitude", "Longitude", "temperature_2m", "relative_humidity_2m",
                "precipitation", "wind_speed_10m", "wind_direction_10m",
                "wind_gusts_10m", "cloud_cover",
            ]:
                if col in selected.columns:
                    selected[col] = pd.to_numeric(selected[col], errors="coerce")
            events.append(selected)

    if not events:
        raise ValueError(f"No weather_code={TARGET_WEATHER_CODE} records found in Bronze CSV files.")

    result = pd.concat(events, ignore_index=True)
    result["time"] = result["time"].astype(str)
    result["EventDate"] = result["time"].str.slice(0, 10)
    result = result.sort_values(["AirportCode", "time"]).reset_index(drop=True)

    duplicate_keys = result.duplicated(subset=["AirportCode", "time"]).sum()
    if duplicate_keys:
        raise ValueError(
            f"Found {duplicate_keys} duplicate AirportCode/time code-96 events. "
            "Resolve event-key duplication before re-querying."
        )

    logger.info("Bronze CSV files found: %s (expected %s)", total_files, EXPECTED_AIRPORT_FILES)
    logger.info("Original code-96 event rows found: %s", len(result))
    logger.info("Airports containing code-96 events: %s", result["AirportCode"].nunique())
    if total_files != EXPECTED_AIRPORT_FILES:
        logger.warning(
            "Expected %s files but found %s. The script will continue using the files found.",
            EXPECTED_AIRPORT_FILES,
            total_files,
        )
    return result, total_files


# -----------------------------------------------------------------------------
# API RESPONSE UTILITIES
# -----------------------------------------------------------------------------
def json_safe(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    if pd.isna(value):
        return None
    return value


def get_response_model_keys(payload: dict[str, Any]) -> list[str]:
    return sorted(key for key in payload.keys() if str(key).lower() in MODEL_METADATA_KEYS)


def compare_value(original: Any, current: Any, variable: str) -> tuple[bool | None, float | None]:
    if original is None or pd.isna(original) or current is None or pd.isna(current):
        return None, None

    try:
        original_num = float(original)
        current_num = float(current)
    except (TypeError, ValueError):
        return str(original) == str(current), None

    difference = abs(original_num - current_num)
    if variable == "weather_code":
        return int(original_num) == int(current_num), difference
    return bool(np.isclose(original_num, current_num, rtol=0.0, atol=NUMERIC_TOLERANCE)), difference


def request_airport_window(
    session: requests.Session,
    airport_code: str,
    airport_events: pd.DataFrame,
    raw_json_dir: Path,
    logger: logging.Logger,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    # Use the original crawler's requested airport coordinates.
    latitudes = airport_events["Latitude"].dropna().unique()
    longitudes = airport_events["Longitude"].dropna().unique()
    if len(latitudes) != 1 or len(longitudes) != 1:
        raise ValueError(
            f"{airport_code}: expected exactly one pair of requested coordinates, "
            f"got latitudes={latitudes.tolist()}, longitudes={longitudes.tolist()}"
        )

    start_date = str(airport_events["EventDate"].min())
    end_date = str(airport_events["EventDate"].max())
    params = {
        "latitude": float(latitudes[0]),
        "longitude": float(longitudes[0]),
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(WEATHER_VARIABLES),
        "timezone": REQUESTED_TIMEZONE,
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
        "models": REQUESTED_MODEL,
    }

    request_record: dict[str, Any] = {
        "AirportCode": airport_code,
        "StartDate": start_date,
        "EndDate": end_date,
        "RequestedModel": REQUESTED_MODEL,
        "RequestedLatitude": params["latitude"],
        "RequestedLongitude": params["longitude"],
        "RequestParamsJSON": json.dumps(params, ensure_ascii=False, sort_keys=True),
        "RequestURL": None,
        "HTTPStatus": None,
        "RequestStatus": "NOT_STARTED",
        "Error": None,
        "ResponseTopLevelKeys": None,
        "ResponseModelMetadataKeys": None,
        "ResponseModelIdentifierExposed": False,
        "ResponseLatitude": None,
        "ResponseLongitude": None,
        "ResponseElevation": None,
        "ResponseTimezone": None,
        "ResponseUTCOffsetSeconds": None,
        "GenerationTimeMs": None,
        "ResponseHourlyRows": 0,
        "RawJSONFile": None,
        "ResponseDateHeader": None,
        "ResponseContentType": None,
        "ResponseServerHeader": None,
    }

    try:
        response = session.get(API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        request_record["RequestURL"] = response.url
        request_record["HTTPStatus"] = response.status_code
        response.raise_for_status()
        payload = response.json()

        if not isinstance(payload, dict) or "hourly" not in payload or "time" not in payload["hourly"]:
            raise ValueError(f"Unexpected API response structure: {str(payload)[:500]}")

        raw_json_path = raw_json_dir / f"{REQUESTED_MODEL}_{airport_code}_{start_date}_{end_date}.json"
        raw_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        hourly = payload["hourly"]
        times = hourly.get("time", [])
        request_record.update({
            "RequestStatus": "SUCCESS",
            "ResponseTopLevelKeys": "|".join(sorted(payload.keys())),
            "ResponseModelMetadataKeys": "|".join(get_response_model_keys(payload)),
            "ResponseModelIdentifierExposed": bool(get_response_model_keys(payload)),
            "ResponseLatitude": payload.get("latitude"),
            "ResponseLongitude": payload.get("longitude"),
            "ResponseElevation": payload.get("elevation"),
            "ResponseTimezone": payload.get("timezone"),
            "ResponseUTCOffsetSeconds": payload.get("utc_offset_seconds"),
            "GenerationTimeMs": payload.get("generationtime_ms"),
            "ResponseHourlyRows": len(times),
            "RawJSONFile": str(raw_json_path),
            "ResponseDateHeader": response.headers.get("Date"),
            "ResponseContentType": response.headers.get("Content-Type"),
            "ResponseServerHeader": response.headers.get("Server"),
        })
        return payload, request_record

    except Exception as exc:
        request_record["RequestStatus"] = "FAILED"
        request_record["Error"] = repr(exc)
        logger.exception("API recheck failed for %s (%s to %s)", airport_code, start_date, end_date)
        return None, request_record


# -----------------------------------------------------------------------------
# EVENT-LEVEL COMPARISON
# -----------------------------------------------------------------------------
def build_event_comparison(
    airport_events: pd.DataFrame,
    payload: dict[str, Any] | None,
    request_record: dict[str, Any],
) -> list[dict[str, Any]]:
    comparisons: list[dict[str, Any]] = []

    hourly = payload.get("hourly", {}) if payload else {}
    response_times = hourly.get("time", []) if payload else []
    time_to_index = {str(timestamp): i for i, timestamp in enumerate(response_times)}

    for _, event in airport_events.iterrows():
        event_time = str(event["time"])
        base: dict[str, Any] = {
            "AirportCode": event["AirportCode"],
            "EventTimeUTC": event_time,
            "EventDate": event["EventDate"],
            "SourceFile": event["SourceFile"],
            "OriginalWeatherCode": json_safe(event.get("weather_code")),
            "RequestedModel": REQUESTED_MODEL,
            "WeatherModelRequestedInBronze": event.get("WeatherModelRequested"),
            "APIRequestStatus": request_record["RequestStatus"],
            "APITimestampFound": False,
            "RecheckWeatherCode": None,
            "WeatherCodeMatch": None,
            "ComparedVariables": 0,
            "MatchingVariables": 0,
            "AllComparedVariablesMatch": None,
            "ResponseModelIdentifierExposed": request_record["ResponseModelIdentifierExposed"],
            "ResponseLatitude": request_record["ResponseLatitude"],
            "ResponseLongitude": request_record["ResponseLongitude"],
            "ResponseElevation": request_record["ResponseElevation"],
            "ResponseTimezone": request_record["ResponseTimezone"],
            "ResponseModelMetadataKeys": request_record["ResponseModelMetadataKeys"],
            "ProvenanceAssessment": None,
        }

        if payload is None:
            base["ProvenanceAssessment"] = "API_REQUEST_FAILED"
            comparisons.append(base)
            continue

        if event_time not in time_to_index:
            base["ProvenanceAssessment"] = "API_TIMESTAMP_NOT_RETURNED"
            comparisons.append(base)
            continue

        idx = time_to_index[event_time]
        base["APITimestampFound"] = True
        current_weather_code = hourly.get("weather_code", [None] * len(response_times))[idx]
        base["RecheckWeatherCode"] = json_safe(current_weather_code)

        match_results: list[bool] = []
        for variable in COMPARE_VARIABLES:
            original_value = event.get(variable)
            current_values = hourly.get(variable)
            current_value = current_values[idx] if current_values is not None and idx < len(current_values) else None
            is_match, difference = compare_value(original_value, current_value, variable)

            base[f"Bronze_{variable}"] = json_safe(original_value)
            base[f"APIRecheck_{variable}"] = json_safe(current_value)
            base[f"{variable}_Match"] = is_match
            if difference is not None:
                base[f"{variable}_AbsDiff"] = difference
            if is_match is not None:
                base["ComparedVariables"] += 1
                match_results.append(bool(is_match))
                if is_match:
                    base["MatchingVariables"] += 1

        base["WeatherCodeMatch"] = compare_value(
            event.get("weather_code"), current_weather_code, "weather_code"
        )[0]
        base["AllComparedVariablesMatch"] = (
            all(match_results) if match_results else None
        )

        current_code_num = pd.to_numeric(pd.Series([current_weather_code]), errors="coerce").iloc[0]
        if pd.notna(current_code_num) and int(current_code_num) == TARGET_WEATHER_CODE:
            if request_record["ResponseModelIdentifierExposed"]:
                base["ProvenanceAssessment"] = "CODE96_REPRODUCED_RESPONSE_HAS_MODEL_METADATA_REVIEW_FIELD"
            else:
                base["ProvenanceAssessment"] = "CODE96_REPRODUCED_BUT_RESPONSE_DOES_NOT_IDENTIFY_ACTUAL_MODEL"
        else:
            base["ProvenanceAssessment"] = "CODE96_NOT_REPRODUCED_IN_CURRENT_API_RECHECK"

        comparisons.append(base)

    return comparisons


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main() -> None:
    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = OUTPUT_BASE_DIR / f"run_{run_stamp}"
    raw_json_dir = run_dir / "raw_json"
    raw_json_dir.mkdir(parents=True, exist_ok=True)
    logger = build_logger(run_dir)

    logger.info("STEP 24 - OPEN-METEO WEATHER CODE 96 API PROVENANCE RECHECK")
    logger.info("Project root: %s", PROJECT_ROOT)
    logger.info("Bronze source directory (read-only): %s", RAW_CSV_DIR)
    logger.info("Output directory: %s", run_dir)
    logger.info("API endpoint: %s", API_URL)
    logger.info("Requested model: %s", REQUESTED_MODEL)

    events, files_found = load_code96_events(logger)
    session = build_session()

    request_records: list[dict[str, Any]] = []
    event_records: list[dict[str, Any]] = []
    metadata_records: list[dict[str, Any]] = []

    airport_groups = list(events.groupby("AirportCode", sort=True))
    total_airports = len(airport_groups)

    for number, (airport_code, airport_events) in enumerate(airport_groups, start=1):
        logger.info(
            "[%s/%s] Rechecking %s: %s code-96 events (%s to %s)",
            number,
            total_airports,
            airport_code,
            len(airport_events),
            airport_events["EventDate"].min(),
            airport_events["EventDate"].max(),
        )

        try:
            payload, request_record = request_airport_window(
                session=session,
                airport_code=str(airport_code),
                airport_events=airport_events,
                raw_json_dir=raw_json_dir,
                logger=logger,
            )
        except Exception as exc:
            logger.exception("Could not prepare API request for %s", airport_code)
            request_record = {
                "AirportCode": airport_code,
                "RequestStatus": "PREPARATION_FAILED",
                "Error": repr(exc),
                "RequestedModel": REQUESTED_MODEL,
            }
            payload = None

        request_records.append(request_record)
        event_records.extend(build_event_comparison(airport_events, payload, request_record))

        metadata_records.append({
            "AirportCode": airport_code,
            "RequestStatus": request_record.get("RequestStatus"),
            "RequestedModel": REQUESTED_MODEL,
            "RequestedLatitude": request_record.get("RequestedLatitude"),
            "RequestedLongitude": request_record.get("RequestedLongitude"),
            "ResponseLatitude": request_record.get("ResponseLatitude"),
            "ResponseLongitude": request_record.get("ResponseLongitude"),
            "ResponseElevation": request_record.get("ResponseElevation"),
            "ResponseTimezone": request_record.get("ResponseTimezone"),
            "ResponseUTCOffsetSeconds": request_record.get("ResponseUTCOffsetSeconds"),
            "GenerationTimeMs": request_record.get("GenerationTimeMs"),
            "ResponseTopLevelKeys": request_record.get("ResponseTopLevelKeys"),
            "ResponseModelMetadataKeys": request_record.get("ResponseModelMetadataKeys"),
            "ResponseModelIdentifierExposed": request_record.get("ResponseModelIdentifierExposed"),
            "ResponseHourlyRows": request_record.get("ResponseHourlyRows"),
            "RawJSONFile": request_record.get("RawJSONFile"),
            "ResponseDateHeader": request_record.get("ResponseDateHeader"),
            "ResponseContentType": request_record.get("ResponseContentType"),
            "ResponseServerHeader": request_record.get("ResponseServerHeader"),
        })

        time.sleep(REQUEST_PAUSE_SECONDS)

    event_df = pd.DataFrame(event_records)
    request_df = pd.DataFrame(request_records)
    metadata_df = pd.DataFrame(metadata_records)

    event_output = run_dir / "36_openmeteo_code96_recheck_by_event.csv"
    request_output = run_dir / "37_openmeteo_code96_api_request_log.csv"
    metadata_output = run_dir / "38_openmeteo_code96_api_metadata.csv"
    summary_output = run_dir / "39_openmeteo_code96_recheck_summary.txt"

    event_df.to_csv(event_output, index=False, encoding="utf-8-sig")
    request_df.to_csv(request_output, index=False, encoding="utf-8-sig")
    metadata_df.to_csv(metadata_output, index=False, encoding="utf-8-sig")

    request_success = int(request_df["RequestStatus"].eq("SUCCESS").sum()) if not request_df.empty else 0
    request_failed = int(len(request_df) - request_success)
    timestamp_found = int(event_df["APITimestampFound"].fillna(False).sum()) if not event_df.empty else 0
    code96_reproduced = int(
        pd.to_numeric(event_df.get("RecheckWeatherCode"), errors="coerce").eq(TARGET_WEATHER_CODE).sum()
    ) if not event_df.empty else 0
    code96_not_reproduced = int(
        event_df["ProvenanceAssessment"].eq("CODE96_NOT_REPRODUCED_IN_CURRENT_API_RECHECK").sum()
    ) if not event_df.empty else 0
    model_metadata_exposed = int(
        request_df.get("ResponseModelIdentifierExposed", pd.Series(dtype=bool)).fillna(False).sum()
    ) if not request_df.empty else 0
    all_values_match = int(
        event_df["AllComparedVariablesMatch"].fillna(False).sum()
    ) if not event_df.empty else 0

    assessment_counts = (
        event_df["ProvenanceAssessment"].value_counts(dropna=False).to_dict()
        if not event_df.empty else {}
    )

    lines = [
        "OPEN-METEO STEP 24 - WEATHER CODE 96 API PROVENANCE RECHECK",
        "=" * 78,
        f"Run timestamp: {run_stamp}",
        f"API endpoint: {API_URL}",
        f"Requested model: {REQUESTED_MODEL}",
        f"Bronze CSV files found: {files_found} (expected {EXPECTED_AIRPORT_FILES})",
        f"Original WeatherCode=96 events: {len(events)}",
        f"Airports with WeatherCode=96: {events['AirportCode'].nunique()}",
        f"Airport-level API requests: {len(request_df)}",
        f"Successful API requests: {request_success}",
        f"Failed API requests: {request_failed}",
        f"Event timestamps found in API response: {timestamp_found}/{len(events)}",
        f"WeatherCode=96 reproduced by current API: {code96_reproduced}",
        f"WeatherCode=96 not reproduced by current API: {code96_not_reproduced}",
        f"Requests whose response exposes a model identifier: {model_metadata_exposed}/{len(request_df)}",
        f"Events whose compared variables all match Bronze: {all_values_match}/{len(events)}",
        "",
        "EVENT-LEVEL ASSESSMENTS",
        "-" * 78,
    ]
    if assessment_counts:
        for label, count in assessment_counts.items():
            lines.append(f"{label}: {count}")
    lines.extend([
        "",
        "INTERPRETATION",
        "- The API request repeats the original endpoint and explicitly requests models=ecmwf_ifs.",
        "- WeatherModelRequested in Bronze was written locally by the crawler from its request constant.",
        "- The API response metadata is inspected for an actual model identifier; absence means the",
        "  response itself does not independently identify which model produced the returned values.",
        "- If code 96 is reproduced, that establishes repeatability in the current API response, not",
        "  independent proof of actual model provenance or physical hail occurrence.",
        "- If code 96 is not reproduced, that establishes a difference from the current API response,",
        "  not that the original Bronze value was necessarily erroneous at download time.",
        "- Original Bronze CSV and JSON files are read-only and are not modified by this script.",
        "",
        "OUTPUT FILES",
        f"{event_output}",
        f"{request_output}",
        f"{metadata_output}",
        f"{raw_json_dir}",
        f"{summary_output}",
    ])
    summary_output.write_text("\n".join(lines), encoding="utf-8")

    logger.info("Step 24 completed.")
    logger.info("Event comparison: %s", event_output)
    logger.info("Request log: %s", request_output)
    logger.info("API metadata: %s", metadata_output)
    logger.info("Summary: %s", summary_output)
    logger.info("Raw response JSON folder: %s", raw_json_dir)


if __name__ == "__main__":
    main()
