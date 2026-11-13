
from __future__ import annotations

import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

# =============================================================================
# STEP 27B-3C2 - UTC EVENT TIMESTAMP CROSS-VALIDATION (RAW CSV / READ-ONLY)
# =============================================================================
# This version intentionally does NOT import/use PyArrow or read Parquet.
# It reads the 12 monthly BTS RAW CSV files in chunks and does not modify RAW,
# STAGING, CLEAN, or any source file.
#
# Timestamp logic:
# - CRSDepTime is localized from FlightDate in the ORIGIN timezone.
# - Scheduled arrival UTC = selected scheduled departure UTC + CRSElapsedTime.
# - DepDelay is applied on the ORIGIN'S LOCAL WALL CLOCK to infer the expected
#   local actual-departure datetime. That local datetime is then localized to
#   UTC candidate(s), preserving both DST folds when ambiguous.
# - ActualElapsedTime is added to each actual-departure UTC candidate. The
#   resulting UTC instant is converted to DESTINATION local time and checked
#   against ArrTime. This can resolve an ambiguous actual-departure fold.
# - ArrDelay is checked as a LOCAL WALL-CLOCK residual between the actual
#   arrival wall datetime derived from UTC elapsed time and the scheduled
#   arrival wall datetime. It is NOT compared directly to a UTC difference.
# - Nonexistent local times are retained as audit exceptions; no time is
#   shifted/fabricated. Unresolved DST folds remain unresolved.
#
# All audit outputs are diagnostic; they do not create CLEAN timestamps.
# =============================================================================

EXPECTED_ROWS = 7_001_619
BATCH_SIZE = 100_000
TOLERANCE_MINUTES = 1.0
SAMPLE_PER_ISSUE_MONTH = 10

INPUT_COLUMNS = [
    "FlightDate", "Reporting_Airline", "Flight_Number_Reporting_Airline",
    "Origin", "Dest", "OriginAirportID", "DestAirportID",
    "CRSDepTime", "DepTime", "DepDelay", "CRSArrTime", "ArrTime",
    "ArrDelay", "CRSElapsedTime", "ActualElapsedTime", "Cancelled",
    "Diverted",
]

OUTPUTS = [
    "27B3C2_event_status_by_month.csv",
    "27B3C2_timezone_dst_profile.csv",
    "27B3C2_time_field_profile.csv",
    "27B3C2_event_timestamp_samples.csv",
    "27B3C2_utc_event_timestamp_summary.txt",
    "27B3C2_utc_event_timestamp_audit.log",
]


def is_missing(s: pd.Series) -> pd.Series:
    x = s.astype("string").str.strip()
    # Do not globally interpret the literal token "NA" as missing. RAW uses
    # keep_default_na=False; numeric parsing below naturally rejects nonnumbers.
    return x.isna() | x.eq("") | x.str.upper().isin(["N/A", "NULL", "NONE"])


def to_num(s: pd.Series) -> pd.Series:
    x = s.astype("string").str.strip().mask(is_missing(s))
    return pd.to_numeric(x, errors="coerce")


def normalize_id(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip().str.replace(r"\.0$", "", regex=True)


def parse_hhmm(s: pd.Series):
    """Return minute-of-day, day-add for 2400, status, and 2400 flag."""
    raw = s.astype("string").str.strip()
    missing = is_missing(raw)
    num = pd.to_numeric(raw.mask(missing), errors="coerce")
    integer = num.notna() & np.isclose(num.fillna(0) % 1, 0)
    n = num.fillna(-999999).astype("int64")
    is2400 = integer & n.eq(2400)
    regular = (
        integer & n.ge(0) & n.le(2359)
        & n.mod(100).lt(60) & n.floordiv(100).le(23)
    )
    valid = is2400 | regular
    minute = pd.Series(np.nan, index=s.index, dtype="float64")
    minute.loc[regular] = (
        (n.loc[regular] // 100) * 60 + (n.loc[regular] % 100)
    ).astype(float)
    minute.loc[is2400] = 0.0
    day_add = pd.Series(0.0, index=s.index, dtype="float64")
    day_add.loc[is2400] = 1.0
    status = pd.Series("INVALID_HHMM", index=s.index, dtype="string")
    status.loc[missing] = "MISSING_HHMM"
    status.loc[valid] = "VALID_HHMM"
    return minute, day_add, status, is2400


def build_local_naive(date_s: pd.Series, minute_s: pd.Series,
                      day_add: pd.Series | None = None) -> pd.Series:
    if day_add is None:
        day_add = pd.Series(0.0, index=date_s.index)
    total_minutes = minute_s + day_add.fillna(0) * 1440
    out = date_s + pd.to_timedelta(total_minutes, unit="m")
    return out.where(date_s.notna() & minute_s.notna())


def localize_folds(naive: pd.Series, tz: pd.Series):
    """Return UTC candidates for both folds; nonexistent local times become NaT."""
    u0 = pd.Series(pd.NaT, index=naive.index, dtype="datetime64[ns, UTC]")
    u1 = pd.Series(pd.NaT, index=naive.index, dtype="datetime64[ns, UTC]")
    zones = tz.dropna().astype("string").unique().tolist()
    for zone in zones:
        mask = tz.eq(zone).fillna(False) & naive.notna()
        if not mask.any():
            continue
        try:
            loc0 = naive.loc[mask].dt.tz_localize(
                str(zone), ambiguous=True, nonexistent="NaT"
            )
            loc1 = naive.loc[mask].dt.tz_localize(
                str(zone), ambiguous=False, nonexistent="NaT"
            )
            u0.loc[mask] = loc0.dt.tz_convert("UTC")
            u1.loc[mask] = loc1.dt.tz_convert("UTC")
        except Exception as exc:
            raise RuntimeError(f"Timezone localization failed for {zone}: {exc}") from exc
    return u0, u1


def utc_to_local(utc_s: pd.Series, tz: pd.Series):
    """Return local naive wall datetime and minute-of-day."""
    wall = pd.Series(pd.NaT, index=utc_s.index, dtype="datetime64[ns]")
    minute = pd.Series(np.nan, index=utc_s.index, dtype="float64")
    zones = tz.dropna().astype("string").unique().tolist()
    for zone in zones:
        mask = tz.eq(zone).fillna(False) & utc_s.notna()
        if not mask.any():
            continue
        local = utc_s.loc[mask].dt.tz_convert(str(zone))
        wall.loc[mask] = local.dt.tz_localize(None)
        minute.loc[mask] = (
            local.dt.hour.astype(float) * 60 + local.dt.minute.astype(float)
            + local.dt.second.astype(float) / 60
        )
    return wall, minute


def classify_local(naive: pd.Series, tz: pd.Series,
                   u0: pd.Series, u1: pd.Series) -> pd.Series:
    status = pd.Series("UNCLASSIFIED", index=naive.index, dtype="string")
    status.loc[naive.isna()] = "MISSING_LOCAL_DATETIME"
    status.loc[naive.notna() & tz.isna()] = "MISSING_TIMEZONE"
    status.loc[
        naive.notna() & tz.notna() & u0.isna() & u1.isna()
    ] = "NONEXISTENT_LOCAL_TIME"
    both = u0.notna() & u1.notna()
    status.loc[both & u0.eq(u1)] = "UNIQUE_LOCAL_TIME"
    status.loc[both & u0.ne(u1)] = "AMBIGUOUS_LOCAL_TIME"
    status.loc[u0.notna() ^ u1.notna()] = "ONE_FOLD_ONLY"
    return status


def circular_diff(a: pd.Series, b: pd.Series) -> pd.Series:
    return ((a - b + 720) % 1440) - 720


def reported_clock_local_status(event_wall: pd.Series, reported_minute: pd.Series,
                                tz: pd.Series) -> pd.Series:
    """Classify the reported HHMM on the event's inferred local calendar date."""
    candidate = pd.Series(pd.NaT, index=event_wall.index, dtype="datetime64[ns]")
    ok = event_wall.notna() & reported_minute.notna()
    candidate.loc[ok] = (
        event_wall.loc[ok].dt.normalize()
        + pd.to_timedelta(reported_minute.loc[ok], unit="m")
    )
    u0, u1 = localize_folds(candidate, tz)
    return classify_local(candidate, tz, u0, u1)


def add_group_counts(counter: Counter, month: pd.Series,
                     dimension: str, status: pd.Series):
    temp = pd.DataFrame({"month": month, "status": status})
    for (m, st), value in temp.groupby(["month", "status"], dropna=False).size().items():
        counter[(str(m), dimension, str(st))] += int(value)


def add_tz_counts(counter: Counter, timezone: pd.Series,
                  dimension: str, status: pd.Series):
    temp = pd.DataFrame({"tz": timezone, "status": status})
    for (zone, st), value in temp.groupby(["tz", "status"], dropna=False).size().items():
        counter[(str(zone), dimension, str(st))] += int(value)


def main() -> int:
    # Expected location:
    # D:\DATA_WAREHOUSE\08_SCRIPTS\02_profiling\this_script.py
    root = Path(__file__).resolve().parents[2]
    raw_dir = root / "01_RAW_DATA" / "BTS" / "csv"
    model_dir = root / "06_DW_DESIGN" / "01_LOGICAL_MODEL"
    mapping_path = model_dir / "27B3B_airport_timezone_mapping_candidates.csv"
    model_dir.mkdir(parents=True, exist_ok=True)

    existing = [
        str(model_dir / name)
        for name in OUTPUTS
        if (model_dir / name).exists()
    ]
    if existing:
        print(
            "STOP - C2 outputs already exist. Archive/rename them before running:\n"
            + "\n".join(existing)
        )
        return 2

    if not raw_dir.exists() or not mapping_path.exists():
        print(
            "STOP - Missing RAW BTS directory or timezone mapping:\n"
            f"{raw_dir}\n{mapping_path}"
        )
        return 2

    files = sorted(raw_dir.glob("BTS_2025_*.csv"))
    if len(files) != 12:
        print(f"STOP - Expected 12 monthly BTS CSV files; found {len(files)}")
        return 2

    expected_names = [f"BTS_2025_{m:02d}.csv" for m in range(1, 13)]
    actual_names = [p.name for p in files]
    if actual_names != expected_names:
        print("STOP - Monthly filenames do not match the expected Jan-Dec sequence:")
        print("Found:", actual_names)
        return 2

    logger = logging.getLogger("step27b3c2_csv")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(model_dir / OUTPUTS[5], encoding="utf-8")
    fh.setFormatter(formatter)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(sh)

    mapping = pd.read_csv(mapping_path, dtype="string", keep_default_na=False)
    required_map = {"BTSAirportID", "TimeZoneIANA"}
    if not required_map.issubset(mapping.columns):
        logger.error(
            "Timezone mapping missing columns: %s",
            sorted(required_map - set(mapping.columns))
        )
        return 2

    mapping["BTSAirportID"] = normalize_id(mapping["BTSAirportID"])
    mapping["TimeZoneIANA"] = mapping["TimeZoneIANA"].astype("string").str.strip()

    if mapping["BTSAirportID"].duplicated().any():
        logger.error("BTSAirportID is duplicated in timezone mapping.")
        return 2

    if mapping["TimeZoneIANA"].eq("").any():
        logger.error("Timezone mapping contains blank IANA timezone values.")
        return 2

    tz_map: Dict[str, str] = dict(
        zip(
            mapping["BTSAirportID"].astype(str),
            mapping["TimeZoneIANA"].astype(str)
        )
    )

    logger.info("STEP 27B-3C2 - UTC EVENT TIMESTAMP CROSS-VALIDATION (RAW CSV)")
    logger.info(
        "BTS files=%d; timezone rows=%d; batch=%d",
        len(files), len(mapping), BATCH_SIZE
    )
    logger.info("No PyArrow; RAW read-only; no STAGING/CLEAN changes.")

    month_counts = Counter()
    tz_counts = Counter()
    profile_counts = Counter()
    sample_counts = Counter()
    sample_frames: list[pd.DataFrame] = []
    total_rows = 0

    def capture(sample_df: pd.DataFrame, issue: str, mask: pd.Series):
        if not mask.any():
            return
        for month_value, group in sample_df.loc[mask].groupby(
            "SourceMonth", dropna=False
        ):
            key = (issue, str(month_value))
            remaining = SAMPLE_PER_ISSUE_MONTH - sample_counts[key]
            if remaining > 0:
                part = group.head(remaining).copy()
                part.insert(0, "AuditIssue", issue)
                sample_frames.append(part)
                sample_counts[key] += len(part)

    for file_idx, path in enumerate(files, start=1):
        source_month = path.stem[-2:]
        logger.info("Reading %s (%d/12)", path.name, file_idx)
        file_rows = 0

        reader = pd.read_csv(
            path,
            usecols=INPUT_COLUMNS,
            dtype="string",
            keep_default_na=False,
            chunksize=BATCH_SIZE,
            low_memory=False,
        )

        for df in reader:
            if df.empty:
                continue

            df = df.reset_index(drop=True)
            n = len(df)
            total_rows += n
            file_rows += n

            month = pd.Series(source_month, index=df.index, dtype="string")
            flight_date = pd.to_datetime(
                df["FlightDate"].astype("string").str.strip(),
                errors="coerce"
            )

            parsed = {
                field: parse_hhmm(df[field])
                for field in ["CRSDepTime", "DepTime", "CRSArrTime", "ArrTime"]
            }

            for field, (_, _, status, is2400) in parsed.items():
                profile_counts[(field, "Rows")] += n
                profile_counts[(field, "ValidHHMM")] += int(
                    status.eq("VALID_HHMM").sum()
                )
                profile_counts[(field, "MissingHHMM")] += int(
                    status.eq("MISSING_HHMM").sum()
                )
                profile_counts[(field, "InvalidHHMM")] += int(
                    status.eq("INVALID_HHMM").sum()
                )
                profile_counts[(field, "Special2400")] += int(is2400.sum())

            cdep_min, cdep_add, cdep_status, _ = parsed["CRSDepTime"]
            dep_min, dep_add, dep_status, _ = parsed["DepTime"]
            carr_min, carr_add, carr_status, _ = parsed["CRSArrTime"]
            arr_min, arr_add, arr_status, _ = parsed["ArrTime"]

            crs_elapsed = to_num(df["CRSElapsedTime"])
            actual_elapsed = to_num(df["ActualElapsedTime"])
            dep_delay = to_num(df["DepDelay"])
            arr_delay = to_num(df["ArrDelay"])
            cancelled = to_num(df["Cancelled"]).fillna(-1)
            diverted = to_num(df["Diverted"]).fillna(-1)
            normal_flight = cancelled.eq(0) & diverted.eq(0)

            origin_tz = normalize_id(df["OriginAirportID"]).map(tz_map).astype("string")
            dest_tz = normalize_id(df["DestAirportID"]).map(tz_map).astype("string")

            # 1) Scheduled departure local datetime and UTC fold candidates.
            sched_dep_local = build_local_naive(
                flight_date, cdep_min, cdep_add
            )
            sched0, sched1 = localize_folds(sched_dep_local, origin_tz)
            sched_dst = classify_local(
                sched_dep_local, origin_tz, sched0, sched1
            )

            # Use CRSElapsedTime (elapsed duration) to identify the scheduled
            # departure fold against destination-local CRSArrTime.
            crs_elapsed_valid = crs_elapsed.where(crs_elapsed.ge(0))
            sched_arr0_utc = sched0 + pd.to_timedelta(
                crs_elapsed_valid, unit="m"
            )
            sched_arr1_utc = sched1 + pd.to_timedelta(
                crs_elapsed_valid, unit="m"
            )
            sched_arr0_wall, sched_arr0_min = utc_to_local(
                sched_arr0_utc, dest_tz
            )
            sched_arr1_wall, sched_arr1_min = utc_to_local(
                sched_arr1_utc, dest_tz
            )

            match_sched0 = circular_diff(
                sched_arr0_min, carr_min
            ).abs().le(TOLERANCE_MINUTES)
            match_sched1 = circular_diff(
                sched_arr1_min, carr_min
            ).abs().le(TOLERANCE_MINUTES)

            selected_sched_dep_utc = sched0.copy()
            scheduled_dep_status = sched_dst.copy()
            sched_amb = sched_dst.eq("AMBIGUOUS_LOCAL_TIME")

            resolve_sched0 = sched_amb & match_sched0 & ~match_sched1
            resolve_sched1 = sched_amb & match_sched1 & ~match_sched0
            unresolved_sched_amb = sched_amb & ~(
                resolve_sched0 | resolve_sched1
            )

            selected_sched_dep_utc.loc[resolve_sched1] = sched1.loc[resolve_sched1]
            selected_sched_dep_utc.loc[unresolved_sched_amb] = pd.NaT

            scheduled_dep_status.loc[
                resolve_sched0 | resolve_sched1
            ] = "AMBIGUOUS_RESOLVED_BY_CRS_ARRIVAL"
            scheduled_dep_status.loc[
                unresolved_sched_amb
            ] = "AMBIGUOUS_UNRESOLVED"

            scheduled_arr_utc = selected_sched_dep_utc + pd.to_timedelta(
                crs_elapsed_valid, unit="m"
            )
            scheduled_arr_wall, scheduled_arr_min_calc = utc_to_local(
                scheduled_arr_utc, dest_tz
            )
            sched_arr_resid = circular_diff(
                scheduled_arr_min_calc, carr_min
            )

            scheduled_arr_status = pd.Series(
                "MATCH_WITHIN_1_MINUTE", index=df.index, dtype="string"
            )
            scheduled_arr_status.loc[
                selected_sched_dep_utc.isna()
            ] = "SCHEDULED_DEP_UTC_UNAVAILABLE"
            scheduled_arr_status.loc[
                crs_elapsed.isna()
            ] = "CRSElapsedTime_MISSING"
            scheduled_arr_status.loc[
                crs_elapsed.notna() & crs_elapsed.lt(0)
            ] = "CRSElapsedTime_INVALID_NEGATIVE"
            scheduled_arr_status.loc[
                carr_status.eq("MISSING_HHMM")
            ] = "CRSARRTIME_MISSING"
            scheduled_arr_status.loc[
                carr_status.eq("INVALID_HHMM")
            ] = "CRSARRTIME_INVALID"
            scheduled_arr_status.loc[
                dest_tz.isna()
            ] = "MISSING_DESTINATION_TIMEZONE"

            sched_arr_comparable = (
                selected_sched_dep_utc.notna()
                & crs_elapsed_valid.notna()
                & scheduled_arr_wall.notna()
                & carr_min.notna()
                & dest_tz.notna()
            )
            scheduled_arr_status.loc[
                sched_arr_comparable
                & sched_arr_resid.abs().gt(TOLERANCE_MINUTES)
            ] = "CRS_ELAPSED_VS_CRSARR_CLOCK_MISMATCH"

            sched_arr_reported_dst = reported_clock_local_status(
                scheduled_arr_wall, carr_min, dest_tz
            )
            sched_arr_reported_dst.loc[
                sched_arr_reported_dst.eq("AMBIGUOUS_LOCAL_TIME")
                & sched_arr_comparable
                & sched_arr_resid.abs().le(TOLERANCE_MINUTES)
            ] = "AMBIGUOUS_CLOCK_OCCURRENCE_IDENTIFIED_BY_CRS_ELAPSED"

            sched_arr_reported_dst.loc[
                sched_arr_comparable
                & sched_arr_resid.abs().gt(TOLERANCE_MINUTES)
            ] = "REPORTED_CRSARR_CLOCK_DOES_NOT_MATCH_ELAPSED_CANDIDATE"

            # 2) Actual departure: apply DepDelay to LOCAL wall time, not UTC.
            # The resulting local datetime can itself be ambiguous/nonexistent.
            actual_dep_local_expected = (
                sched_dep_local + pd.to_timedelta(dep_delay, unit="m")
            ).where(sched_dep_local.notna() & dep_delay.notna())

            actual_dep0, actual_dep1 = localize_folds(
                actual_dep_local_expected, origin_tz
            )
            actual_dep_local_dst = classify_local(
                actual_dep_local_expected, origin_tz,
                actual_dep0, actual_dep1
            )

            dep_clock_resid = circular_diff(
                actual_dep_local_expected.dt.hour.astype("float64") * 60
                + actual_dep_local_expected.dt.minute.astype("float64"),
                dep_min
            )

            actual_dep_status = pd.Series(
                "LOCAL_DELAY_CLOCK_MATCH", index=df.index, dtype="string"
            )
            actual_dep_status.loc[
                dep_delay.isna()
            ] = "DEPDELAY_MISSING_NO_LOCAL_DATE_CANDIDATE"
            actual_dep_status.loc[
                sched_dep_local.isna()
            ] = "SCHEDULED_DEP_LOCAL_DATETIME_UNAVAILABLE"
            actual_dep_status.loc[
                dep_status.eq("MISSING_HHMM")
            ] = "DEPTIME_MISSING_CANNOT_VALIDATE"
            actual_dep_status.loc[
                dep_status.eq("INVALID_HHMM")
            ] = "DEPTIME_INVALID"
            actual_dep_status.loc[
                origin_tz.isna()
            ] = "MISSING_ORIGIN_TIMEZONE"

            dep_check = (
                actual_dep_local_expected.notna()
                & dep_delay.notna()
                & dep_min.notna()
                & origin_tz.notna()
            )
            dep_local_clock_match = (
                dep_check
                & dep_status.eq("VALID_HHMM")
                & dep_clock_resid.abs().le(TOLERANCE_MINUTES)
            )

            actual_dep_status.loc[
                dep_check & dep_clock_resid.abs().gt(TOLERANCE_MINUTES)
            ] = "DEPTIME_CLOCK_MISMATCH_VS_LOCAL_DEPDELAY"

            actual_dep_status.loc[
                dep_check
                & dep_clock_resid.abs().le(TOLERANCE_MINUTES)
                & actual_dep_local_dst.eq("NONEXISTENT_LOCAL_TIME")
            ] = "REPORTED_DEPTIME_IN_DST_GAP"

            actual_dep_status.loc[
                dep_check
                & dep_clock_resid.abs().le(TOLERANCE_MINUTES)
                & actual_dep_local_dst.eq("AMBIGUOUS_LOCAL_TIME")
            ] = "AMBIGUOUS_DEPTIME_REQUIRES_ARRIVAL_EVIDENCE"

            # 3) Resolve actual-departure fold with ActualElapsedTime + ArrTime.
            actual_elapsed_valid = actual_elapsed.where(actual_elapsed.ge(0))
            actual_arr0_utc = actual_dep0 + pd.to_timedelta(
                actual_elapsed_valid, unit="m"
            )
            actual_arr1_utc = actual_dep1 + pd.to_timedelta(
                actual_elapsed_valid, unit="m"
            )

            actual_arr0_wall, actual_arr0_min = utc_to_local(
                actual_arr0_utc, dest_tz
            )
            actual_arr1_wall, actual_arr1_min = utc_to_local(
                actual_arr1_utc, dest_tz
            )

            match_actual_arr0 = circular_diff(
                actual_arr0_min, arr_min
            ).abs().le(TOLERANCE_MINUTES)
            match_actual_arr1 = circular_diff(
                actual_arr1_min, arr_min
            ).abs().le(TOLERANCE_MINUTES)

            dep_amb = (
                actual_dep_local_dst.eq("AMBIGUOUS_LOCAL_TIME")
                & dep_local_clock_match
            )
            resolve_actual0 = (
                dep_amb & match_actual_arr0 & ~match_actual_arr1
            )
            resolve_actual1 = (
                dep_amb & match_actual_arr1 & ~match_actual_arr0
            )
            unresolved_actual_amb = dep_amb & ~(
                resolve_actual0 | resolve_actual1
            )

            # Do not promote a UTC candidate unless DepTime independently agrees
            # with the local DepDelay-derived wall datetime. Ambiguous times are
            # promoted only after arrival evidence selects exactly one fold.
            selected_actual_dep_utc = pd.Series(
                pd.NaT, index=df.index, dtype="datetime64[ns, UTC]"
            )
            unique_actual_dep = (
                actual_dep_local_dst.eq("UNIQUE_LOCAL_TIME")
                & dep_local_clock_match
            )
            selected_actual_dep_utc.loc[
                unique_actual_dep
            ] = actual_dep0.loc[unique_actual_dep]
            selected_actual_dep_utc.loc[
                resolve_actual0
            ] = actual_dep0.loc[resolve_actual0]
            selected_actual_dep_utc.loc[
                resolve_actual1
            ] = actual_dep1.loc[resolve_actual1]

            actual_dep_status.loc[
                resolve_actual0 | resolve_actual1
            ] = "AMBIGUOUS_RESOLVED_BY_ACTUAL_ELAPSED_AND_ARRTIME"
            actual_dep_status.loc[
                unresolved_actual_amb
            ] = "AMBIGUOUS_UNRESOLVED_BY_ARRIVAL_EVIDENCE"
            actual_dep_status.loc[
                actual_dep_local_dst.eq("UNIQUE_LOCAL_TIME")
                & dep_check
                & dep_clock_resid.abs().le(TOLERANCE_MINUTES)
            ] = "UNIQUE_LOCAL_TIME_LOCAL_DELAY_MATCH"

            # If reported DepTime is in a DST gap, retain that status even though
            # the expected wall datetime is the same clock time.
            actual_dep_reported_dst = reported_clock_local_status(
                actual_dep_local_expected, dep_min, origin_tz
            )
            actual_dep_reported_dst.loc[
                actual_dep_reported_dst.eq("AMBIGUOUS_LOCAL_TIME")
                & resolve_actual0
            ] = "AMBIGUOUS_RESOLVED_BY_ARRIVAL_EVIDENCE"
            actual_dep_reported_dst.loc[
                actual_dep_reported_dst.eq("AMBIGUOUS_LOCAL_TIME")
                & resolve_actual1
            ] = "AMBIGUOUS_RESOLVED_BY_ARRIVAL_EVIDENCE"

            # Recompute actual arrival from the selected actual-departure UTC.
            actual_arr_utc = selected_actual_dep_utc + pd.to_timedelta(
                actual_elapsed_valid, unit="m"
            )
            actual_arr_wall, actual_arr_min_calc = utc_to_local(
                actual_arr_utc, dest_tz
            )
            arr_clock_resid = circular_diff(
                actual_arr_min_calc, arr_min
            )

            actual_arr_status = pd.Series(
                "NOT_EVALUATED", index=df.index, dtype="string"
            )
            actual_arr_status.loc[
                cancelled.eq(1)
            ] = "CANCELLED_SEPARATE_HANDLING"
            actual_arr_status.loc[
                cancelled.ne(1) & diverted.eq(1)
            ] = "DIVERTED_SEPARATE_HANDLING"
            actual_arr_status.loc[
                normal_flight & selected_actual_dep_utc.isna()
            ] = "ACTUAL_DEP_UTC_UNAVAILABLE_OR_UNRESOLVED"
            actual_arr_status.loc[
                normal_flight & actual_elapsed.isna()
            ] = "ACTUALELAPSEDTIME_MISSING"
            actual_arr_status.loc[
                normal_flight & actual_elapsed.notna() & actual_elapsed.lt(0)
            ] = "ACTUALELAPSEDTIME_INVALID_NEGATIVE"
            actual_arr_status.loc[
                normal_flight & arr_status.eq("MISSING_HHMM")
            ] = "ARRTIME_MISSING"
            actual_arr_status.loc[
                normal_flight & arr_status.eq("INVALID_HHMM")
            ] = "ARRTIME_INVALID"
            actual_arr_status.loc[
                normal_flight & dest_tz.isna()
            ] = "MISSING_DESTINATION_TIMEZONE"

            arr_check = (
                normal_flight
                & selected_actual_dep_utc.notna()
                & actual_elapsed_valid.notna()
                & actual_arr_wall.notna()
                & arr_min.notna()
                & dest_tz.notna()
            )

            actual_arr_status.loc[
                arr_check & arr_clock_resid.abs().gt(TOLERANCE_MINUTES)
            ] = "ARRTIME_CLOCK_MISMATCH_VS_UTC_ELAPSED"
            actual_arr_status.loc[
                arr_check & arr_clock_resid.abs().le(TOLERANCE_MINUTES)
            ] = "ARRIVAL_CLOCK_MATCH_VS_UTC_ELAPSED"

            # ArrDelay is checked in destination LOCAL wall-clock minutes,
            # not by subtracting UTC instants.
            local_arr_delay_residual = (
                (actual_arr_wall - scheduled_arr_wall).dt.total_seconds() / 60.0
                - arr_delay
            )

            arr_delay_check = (
                arr_check
                & scheduled_arr_wall.notna()
                & arr_delay.notna()
                & scheduled_arr_status.eq("MATCH_WITHIN_1_MINUTE")
                & arr_clock_resid.abs().le(TOLERANCE_MINUTES)
            )

            actual_arr_status.loc[
                arr_check
                & arr_clock_resid.abs().le(TOLERANCE_MINUTES)
                & arr_delay.isna()
            ] = "ARRIVAL_CLOCK_MATCH_ARRDELAY_MISSING"

            actual_arr_status.loc[
                arr_check
                & arr_clock_resid.abs().le(TOLERANCE_MINUTES)
                & arr_delay.notna()
                & ~scheduled_arr_status.eq("MATCH_WITHIN_1_MINUTE")
            ] = "ARRIVAL_CLOCK_MATCH_SCHEDULED_BASELINE_UNVERIFIED"

            actual_arr_status.loc[
                arr_delay_check
                & local_arr_delay_residual.abs().gt(TOLERANCE_MINUTES)
            ] = "ARRDELAY_LOCAL_WALLCLOCK_RESIDUAL_MISMATCH"

            actual_arr_status.loc[
                arr_delay_check
                & local_arr_delay_residual.abs().le(TOLERANCE_MINUTES)
            ] = "ARRIVAL_CLOCK_AND_LOCAL_ARRDELAY_MATCH"

            actual_arr_reported_dst = reported_clock_local_status(
                actual_arr_wall, arr_min, dest_tz
            )
            actual_arr_reported_dst.loc[
                actual_arr_reported_dst.eq("AMBIGUOUS_LOCAL_TIME")
                & arr_check
                & arr_clock_resid.abs().le(TOLERANCE_MINUTES)
            ] = "AMBIGUOUS_ARRTIME_OCCURRENCE_IDENTIFIED_BY_UTC_ELAPSED"

            actual_arr_reported_dst.loc[
                actual_arr_reported_dst.eq("NONEXISTENT_LOCAL_TIME")
                & arr_check
            ] = "REPORTED_ARRTIME_IN_DST_GAP"

            actual_arr_reported_dst.loc[
                arr_check & arr_clock_resid.abs().gt(TOLERANCE_MINUTES)
            ] = "REPORTED_ARRTIME_DOES_NOT_MATCH_UTC_ELAPSED"

            dimensions = [
                ("SCHEDULED_DEPARTURE_ORIGIN", scheduled_dep_status),
                ("SCHEDULED_ARRIVAL_DESTINATION", scheduled_arr_status),
                ("SCHEDULED_ARRIVAL_DST_CLOCK", sched_arr_reported_dst),
                ("ACTUAL_DEPARTURE_ORIGIN", actual_dep_status),
                ("ACTUAL_DEPARTURE_DST_CLOCK", actual_dep_reported_dst),
                ("ACTUAL_ARRIVAL_DESTINATION", actual_arr_status),
                ("ACTUAL_ARRIVAL_DST_CLOCK", actual_arr_reported_dst),
            ]

            for dimension, status in dimensions:
                add_group_counts(month_counts, month, dimension, status)

            for dimension, timezone, status in [
                ("SCHEDULED_DEPARTURE_ORIGIN", origin_tz, scheduled_dep_status),
                ("SCHEDULED_ARRIVAL_DESTINATION", dest_tz, sched_arr_reported_dst),
                ("ACTUAL_DEPARTURE_ORIGIN", origin_tz, actual_dep_reported_dst),
                ("ACTUAL_ARRIVAL_DESTINATION", dest_tz, actual_arr_reported_dst),
            ]:
                add_tz_counts(tz_counts, timezone, dimension, status)

            sample_df = pd.DataFrame({
                "SourceFile": path.name,
                "SourceMonth": month,
                "FlightDate": df["FlightDate"],
                "Reporting_Airline": df["Reporting_Airline"],
                "Flight_Number_Reporting_Airline": df["Flight_Number_Reporting_Airline"],
                "Origin": df["Origin"],
                "Dest": df["Dest"],
                "OriginAirportID": df["OriginAirportID"],
                "DestAirportID": df["DestAirportID"],
                "OriginTZ": origin_tz,
                "DestTZ": dest_tz,
                "Cancelled": cancelled,
                "Diverted": diverted,
                "CRSDepTime": df["CRSDepTime"],
                "DepTime": df["DepTime"],
                "DepDelay": df["DepDelay"],
                "CRSArrTime": df["CRSArrTime"],
                "ArrTime": df["ArrTime"],
                "ArrDelay": df["ArrDelay"],
                "CRSElapsedTime": df["CRSElapsedTime"],
                "ActualElapsedTime": df["ActualElapsedTime"],
                "ScheduledDepStatus": scheduled_dep_status,
                "ScheduledArrStatus": scheduled_arr_status,
                "ScheduledArrClockResidualMin": sched_arr_resid,
                "ActualDepLocalStatus": actual_dep_status,
                "ActualDepLocalClockResidualMin": dep_clock_resid,
                "ActualDepDSTStatus": actual_dep_reported_dst,
                "ActualDepUTCCandidate": selected_actual_dep_utc.astype("string"),
                "ActualArrStatus": actual_arr_status,
                "ActualArrClockResidualMin": arr_clock_resid,
                "LocalArrDelayResidualMin": local_arr_delay_residual,
                "ScheduledDepUTCCandidate": selected_sched_dep_utc.astype("string"),
                "ScheduledArrUTCCandidate": scheduled_arr_utc.astype("string"),
                "ActualArrUTCCandidateFromElapsed": actual_arr_utc.astype("string"),
                "ActualArrDSTStatus": actual_arr_reported_dst,
            })

            capture(
                sample_df,
                "SCHEDULED_ARRIVAL_CLOCK_MISMATCH",
                scheduled_arr_status.eq("CRS_ELAPSED_VS_CRSARR_CLOCK_MISMATCH")
            )
            capture(
                sample_df,
                "ACTUAL_DEPARTURE_LOCAL_DELAY_MISMATCH",
                actual_dep_status.eq("DEPTIME_CLOCK_MISMATCH_VS_LOCAL_DEPDELAY")
            )
            capture(
                sample_df,
                "ACTUAL_DEPARTURE_DST_GAP",
                actual_dep_local_dst.eq("NONEXISTENT_LOCAL_TIME")
            )
            capture(
                sample_df,
                "ACTUAL_DEPARTURE_DST_AMBIGUOUS_UNRESOLVED",
                actual_dep_status.eq("AMBIGUOUS_UNRESOLVED_BY_ARRIVAL_EVIDENCE")
            )
            capture(
                sample_df,
                "ACTUAL_ARRIVAL_UTC_CLOCK_MISMATCH",
                actual_arr_status.eq("ARRTIME_CLOCK_MISMATCH_VS_UTC_ELAPSED")
            )
            capture(
                sample_df,
                "ACTUAL_ARRIVAL_LOCAL_ARRDELAY_MISMATCH",
                actual_arr_status.eq("ARRDELAY_LOCAL_WALLCLOCK_RESIDUAL_MISMATCH")
            )
            capture(
                sample_df,
                "ACTUAL_ARRIVAL_DST_GAP",
                actual_arr_reported_dst.eq("REPORTED_ARRTIME_IN_DST_GAP")
            )
            capture(
                sample_df,
                "ACTUAL_ARRIVAL_DST_AMBIGUOUS",
                actual_arr_reported_dst.eq(
                    "AMBIGUOUS_ARRTIME_OCCURRENCE_IDENTIFIED_BY_UTC_ELAPSED"
                )
            )

        logger.info("Completed %s; rows=%s", path.name, f"{file_rows:,}")

    month_df = pd.DataFrame([
        {
            "SourceMonth": m,
            "AuditDimension": dim,
            "AuditStatus": st,
            "Rows": v
        }
        for (m, dim, st), v in month_counts.items()
    ]).sort_values(
        ["SourceMonth", "AuditDimension", "AuditStatus"]
    )

    tz_df = pd.DataFrame([
        {
            "IANA_Timezone": zone,
            "AuditDimension": dim,
            "AuditStatus": st,
            "Rows": v
        }
        for (zone, dim, st), v in tz_counts.items()
    ]).sort_values(
        ["IANA_Timezone", "AuditDimension", "AuditStatus"]
    )

    profile_df = pd.DataFrame([
        {
            "FieldName": field,
            "Metric": metric,
            "Rows": int(value)
        }
        for (field, metric), value in profile_counts.items()
    ]).sort_values(["FieldName", "Metric"])

    samples_df = (
        pd.concat(sample_frames, ignore_index=True)
        if sample_frames else pd.DataFrame()
    )

    month_df.to_csv(
        model_dir / OUTPUTS[0], index=False, encoding="utf-8-sig"
    )
    tz_df.to_csv(
        model_dir / OUTPUTS[1], index=False, encoding="utf-8-sig"
    )
    profile_df.to_csv(
        model_dir / OUTPUTS[2], index=False, encoding="utf-8-sig"
    )
    samples_df.to_csv(
        model_dir / OUTPUTS[3], index=False, encoding="utf-8-sig"
    )

    lines = [
        "STEP 27B-3C2 - UTC EVENT TIMESTAMP CROSS-VALIDATION SUMMARY (RAW CSV)",
        "=" * 82,
        f"BTS files scanned: {len(files)}",
        f"BTS records scanned: {total_rows:,}",
        f"Expected records: {EXPECTED_ROWS:,}",
        f"Timezone mapping rows: {len(mapping):,}",
        "",
        "TIMESTAMP INTERPRETATION",
        "- Scheduled departure is localized from FlightDate + CRSDepTime at origin.",
        "- Scheduled arrival UTC is scheduled departure UTC + CRSElapsedTime.",
        "- DepDelay is applied to the origin-local wall-clock schedule to infer actual departure local datetime.",
        "- Actual departure local datetime is localized to UTC candidate(s); both DST folds are retained until evidence resolves them.",
        "- ActualElapsedTime is added to actual departure UTC; the result is converted to destination local time and checked against ArrTime.",
        "- ArrDelay is checked against destination-local wall-clock actual-vs-scheduled arrival difference, not a UTC difference.",
        "- DST gaps are not shifted or fabricated; unresolved folds remain unresolved.",
        "- Cancelled/diverted flights are retained; actual-arrival handling is separate.",
        "- Results are audit candidates, not CLEAN timestamps or proof of source ground truth.",
        "- No RAW/STAGING mutation; no Parquet/PyArrow dependency; no CLEAN output.",
        "",
        "TOTALS BY DIMENSION AND STATUS",
    ]

    totals = month_df.groupby(
        ["AuditDimension", "AuditStatus"], as_index=False
    )["Rows"].sum()

    for dimension in totals["AuditDimension"].drop_duplicates():
        lines.append(f"\n[{dimension}]")
        for row in totals[totals.AuditDimension == dimension].itertuples(index=False):
            lines.append(f"{row.AuditStatus}: {int(row.Rows):,}")

    lines += ["", "OUTPUT FILES"]
    lines.extend(
        str(model_dir / name)
        for name in OUTPUTS
        if not name.endswith(".log")
    )

    (model_dir / OUTPUTS[4]).write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8"
    )

    logger.info("Completed. records=%s", f"{total_rows:,}")

    if total_rows != EXPECTED_ROWS:
        logger.warning(
            "Record count differs from expected %s",
            f"{EXPECTED_ROWS:,}"
        )

    logger.info("Summary: %s", model_dir / OUTPUTS[4])
    logger.info("Log: %s", model_dir / OUTPUTS[5])

    print(
        "\nSTEP 27B-3C2 CSV-only completed. "
        "Review summary and samples before normalization."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
