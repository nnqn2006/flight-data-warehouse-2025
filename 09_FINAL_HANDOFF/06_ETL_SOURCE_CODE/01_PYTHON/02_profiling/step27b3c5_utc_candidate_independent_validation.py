from __future__ import annotations

import logging
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

# STEP 27B-3C5
# Independent validation of UTC event candidates using BTS 2025 reporting logic.
# Reads RAW monthly CSVs in chunks. No PyArrow. No RAW/STAGING/CLEAN mutation.
#
# Candidate construction:
#   ScheduledDepUTC = localize(FlightDate + CRSDepTime, origin timezone)
#   ScheduledArrUTC = ScheduledDepUTC + CRSElapsedTime
#   ActualDepUTC candidate = ScheduledDepUTC + DepDelay
#   ActualArrUTC candidate A = ScheduledArrUTC + ArrDelay
#   ActualArrUTC candidate B = ActualDepUTC + ActualElapsedTime
#
# Important: BTS defines event clock fields as local time and defines delays
# as differences between actual and CRS scheduled times. This script tests UTC
# minute-offset candidates as an explicit audit hypothesis; it does NOT claim
# the directive prescribes UTC arithmetic for delay fields. Every candidate is
# converted back to airport-local time and compared with DepTime / ArrTime.
# Arrival candidate A vs B is checked independently of ArrTime; this avoids the
# C4 defect where a clock mismatch could prevent an elapsed-time pair test.
# This is an audit, not an automatic repair or approval for CLEAN.

EXPECTED_ROWS = 7_001_619
CHUNK_SIZE = 100_000
TOLERANCE_MIN = 1.0
SAMPLES_PER_ISSUE_MONTH = 12

INPUT_COLUMNS = [
    "FlightDate", "Reporting_Airline", "Flight_Number_Reporting_Airline",
    "Origin", "Dest", "OriginAirportID", "DestAirportID",
    "CRSDepTime", "DepTime", "DepDelay", "CRSArrTime", "ArrTime",
    "ArrDelay", "CRSElapsedTime", "ActualElapsedTime", "Cancelled",
    "Diverted",
]

OUTPUTS = [
    "27B3C5_event_status_by_month.csv",
    "27B3C5_timezone_dst_profile.csv",
    "27B3C5_time_field_profile.csv",
    "27B3C5_event_timestamp_samples.csv",
    "27B3C5_utc_event_timestamp_summary.txt",
    "27B3C5_utc_event_timestamp_audit.log",
]


def is_missing(s: pd.Series) -> pd.Series:
    x = s.astype("string").str.strip()
    return x.isna() | x.eq("") | x.str.upper().isin(["N/A", "NULL", "NONE"])


def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(
        s.astype("string").str.strip().mask(is_missing(s)),
        errors="coerce",
    )


def norm_id(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip().str.replace(r"\.0$", "", regex=True)


def parse_hhmm(s: pd.Series):
    raw = s.astype("string").str.strip()
    miss = is_missing(raw)
    v = pd.to_numeric(raw.mask(miss), errors="coerce")
    integer = v.notna() & np.isclose(v.fillna(0) % 1, 0)
    n = v.fillna(-999999).astype("int64")
    is2400 = integer & n.eq(2400)
    regular = (
        integer & n.ge(0) & n.le(2359)
        & n.mod(100).lt(60) & n.floordiv(100).le(23)
    )
    valid = regular | is2400
    minute = pd.Series(np.nan, index=s.index, dtype="float64")
    minute.loc[regular] = (
        (n.loc[regular] // 100) * 60 + n.loc[regular] % 100
    ).astype(float)
    minute.loc[is2400] = 0.0
    day_add = pd.Series(0.0, index=s.index, dtype="float64")
    day_add.loc[is2400] = 1.0
    status = pd.Series("INVALID_HHMM", index=s.index, dtype="string")
    status.loc[miss] = "MISSING_HHMM"
    status.loc[valid] = "VALID_HHMM"
    return minute, day_add, status, is2400


def localize_two_folds(local_naive: pd.Series, zones: pd.Series):
    u0 = pd.Series(pd.NaT, index=local_naive.index, dtype="datetime64[ns, UTC]")
    u1 = pd.Series(pd.NaT, index=local_naive.index, dtype="datetime64[ns, UTC]")
    for zone in zones.dropna().astype("string").unique():
        m = zones.eq(zone).fillna(False) & local_naive.notna()
        if not m.any():
            continue
        try:
            a = local_naive.loc[m].dt.tz_localize(
                str(zone), ambiguous=True, nonexistent="NaT"
            ).dt.tz_convert("UTC")
            b = local_naive.loc[m].dt.tz_localize(
                str(zone), ambiguous=False, nonexistent="NaT"
            ).dt.tz_convert("UTC")
            u0.loc[m] = a
            u1.loc[m] = b
        except Exception as exc:
            raise RuntimeError(f"Timezone localization failed for {zone}: {exc}") from exc
    return u0, u1


def utc_to_local(utc_s: pd.Series, zones: pd.Series):
    wall = pd.Series(pd.NaT, index=utc_s.index, dtype="datetime64[ns]")
    minute = pd.Series(np.nan, index=utc_s.index, dtype="float64")
    for zone in zones.dropna().astype("string").unique():
        m = zones.eq(zone).fillna(False) & utc_s.notna()
        if not m.any():
            continue
        loc = utc_s.loc[m].dt.tz_convert(str(zone))
        wall.loc[m] = loc.dt.tz_localize(None)
        minute.loc[m] = (
            loc.dt.hour.astype(float) * 60
            + loc.dt.minute.astype(float)
            + loc.dt.second.astype(float) / 60
        )
    return wall, minute


def classify_local(local: pd.Series, zones: pd.Series, u0: pd.Series, u1: pd.Series):
    out = pd.Series("UNCLASSIFIED", index=local.index, dtype="string")
    out.loc[local.isna()] = "MISSING_LOCAL_DATETIME"
    out.loc[local.notna() & zones.isna()] = "MISSING_TIMEZONE"
    gap = local.notna() & zones.notna() & u0.isna() & u1.isna()
    out.loc[gap] = "NONEXISTENT_LOCAL_TIME"
    both = u0.notna() & u1.notna()
    out.loc[both & u0.eq(u1)] = "UNIQUE_LOCAL_TIME"
    out.loc[both & u0.ne(u1)] = "AMBIGUOUS_LOCAL_TIME"
    out.loc[u0.notna() ^ u1.notna()] = "ONE_FOLD_ONLY"
    return out


def circular_minute_diff(a: pd.Series, b: pd.Series):
    """Signed clock-minute difference in the interval [-720, 720)."""
    return ((a - b + 720) % 1440) - 720


def clock_status_from_utc(utc_s: pd.Series, zones: pd.Series, reported_min: pd.Series):
    wall, local_min = utc_to_local(utc_s, zones)
    residual = ((local_min - reported_min + 720) % 1440) - 720
    u0, u1 = localize_two_folds(wall, zones)
    dst = classify_local(wall, zones, u0, u1)
    status = pd.Series("UTC_CANDIDATE_CLOCK_MATCH", index=utc_s.index, dtype="string")
    status.loc[utc_s.isna()] = "UTC_CANDIDATE_UNAVAILABLE"
    status.loc[utc_s.notna() & reported_min.isna()] = "REPORTED_CLOCK_MISSING"
    status.loc[utc_s.notna() & reported_min.notna() & residual.abs().gt(TOLERANCE_MIN)] = "UTC_CANDIDATE_CLOCK_MISMATCH"
    status.loc[utc_s.notna() & dst.eq("MISSING_TIMEZONE")] = "TIMEZONE_MISSING"
    status.loc[utc_s.notna() & dst.eq("AMBIGUOUS_LOCAL_TIME") & residual.abs().le(TOLERANCE_MIN)] = "AMBIGUOUS_FOLD_IDENTIFIED_BY_UTC_CANDIDATE"
    status.loc[utc_s.notna() & dst.eq("NONEXISTENT_LOCAL_TIME")] = "UNEXPECTED_NONEXISTENT_CONVERTED_LOCAL_TIME"
    return wall, residual, dst, status


def add_counts(counter: Counter, month: pd.Series, dim: str, status: pd.Series):
    tmp = pd.DataFrame({"month": month, "status": status})
    for (m, st), value in tmp.groupby(["month", "status"], dropna=False).size().items():
        counter[(str(m), dim, str(st))] += int(value)


def add_tz_counts(counter: Counter, zones: pd.Series, dim: str, status: pd.Series):
    tmp = pd.DataFrame({"zone": zones, "status": status})
    for (z, st), value in tmp.groupby(["zone", "status"], dropna=False).size().items():
        counter[(str(z), dim, str(st))] += int(value)


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    raw_dir = root / "01_RAW_DATA" / "BTS" / "csv"
    out_dir = root / "06_DW_DESIGN" / "01_LOGICAL_MODEL"
    mapping_path = out_dir / "27B3B_airport_timezone_mapping_candidates.csv"
    out_dir.mkdir(parents=True, exist_ok=True)

    existing = [str(out_dir / x) for x in OUTPUTS if (out_dir / x).exists()]
    if existing:
        print("STOP - C5 outputs already exist; archive/rename before rerunning:\n" + "\n".join(existing))
        return 2
    if not raw_dir.exists() or not mapping_path.exists():
        print(f"STOP - Missing RAW folder or timezone mapping:\n{raw_dir}\n{mapping_path}")
        return 2

    files = sorted(raw_dir.glob("BTS_2025_*.csv"))
    expected = [f"BTS_2025_{m:02d}.csv" for m in range(1, 13)]
    if [p.name for p in files] != expected:
        print(f"STOP - Expected 12 monthly files; found {[p.name for p in files]}")
        return 2

    logger = logging.getLogger("step27b3c5")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    for handler in [
        logging.FileHandler(out_dir / OUTPUTS[5], encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]:
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    mp = pd.read_csv(mapping_path, dtype="string", keep_default_na=False)
    if not {"BTSAirportID", "TimeZoneIANA"}.issubset(mp.columns):
        logger.error("Timezone mapping lacks BTSAirportID or TimeZoneIANA")
        return 2
    mp["BTSAirportID"] = norm_id(mp["BTSAirportID"])
    mp["TimeZoneIANA"] = mp["TimeZoneIANA"].astype("string").str.strip()
    if mp["BTSAirportID"].duplicated().any() or mp["TimeZoneIANA"].eq("").any():
        logger.error("Duplicate AirportID or blank timezone in mapping")
        return 2
    tz_map = dict(zip(mp.BTSAirportID.astype(str), mp.TimeZoneIANA.astype(str)))

    logger.info("STEP 27B-3C5 - INDEPENDENT UTC CANDIDATE VALIDATION")
    logger.info("12 RAW CSV files; timezone rows=%d; chunks=%d", len(mp), CHUNK_SIZE)
    logger.info("No PyArrow; RAW read-only; no STAGING/CLEAN mutation")

    month_counts, tz_counts, field_counts = Counter(), Counter(), Counter()
    sample_frames, sample_counts = [], Counter()
    total_rows = 0

    def capture(base: pd.DataFrame, issue: str, mask: pd.Series):
        if not mask.any():
            return
        for mon, grp in base.loc[mask].groupby("SourceMonth", dropna=False):
            key = (issue, str(mon))
            remain = SAMPLES_PER_ISSUE_MONTH - sample_counts[key]
            if remain > 0:
                part = grp.head(remain).copy()
                part.insert(0, "AuditIssue", issue)
                sample_frames.append(part)
                sample_counts[key] += len(part)

    for file_no, path in enumerate(files, 1):
        month_num = path.stem[-2:]
        logger.info("Reading %s (%d/12)", path.name, file_no)
        file_rows = 0
        reader = pd.read_csv(
            path, usecols=INPUT_COLUMNS, dtype="string",
            keep_default_na=False, chunksize=CHUNK_SIZE, low_memory=False,
        )

        for df in reader:
            if df.empty:
                continue
            df = df.reset_index(drop=True)
            n = len(df)
            total_rows += n
            file_rows += n
            month = pd.Series(month_num, index=df.index, dtype="string")
            flight_date = pd.to_datetime(df.FlightDate.str.strip(), errors="coerce")

            parsed = {f: parse_hhmm(df[f]) for f in ["CRSDepTime", "DepTime", "CRSArrTime", "ArrTime"]}
            for field, (_, _, st, is2400) in parsed.items():
                field_counts[(field, "Rows")] += n
                field_counts[(field, "ValidHHMM")] += int(st.eq("VALID_HHMM").sum())
                field_counts[(field, "MissingHHMM")] += int(st.eq("MISSING_HHMM").sum())
                field_counts[(field, "InvalidHHMM")] += int(st.eq("INVALID_HHMM").sum())
                field_counts[(field, "Special2400")] += int(is2400.sum())

            cdep, cdep_day, cdep_st, _ = parsed["CRSDepTime"]
            dep_clock, _, dep_st, _ = parsed["DepTime"]
            carr, carr_day, carr_st, _ = parsed["CRSArrTime"]
            arr_clock, _, arr_st, _ = parsed["ArrTime"]

            crs_elapsed = to_num(df.CRSElapsedTime)
            actual_elapsed = to_num(df.ActualElapsedTime)
            dep_delay = to_num(df.DepDelay)
            arr_delay = to_num(df.ArrDelay)
            cancelled = to_num(df.Cancelled).fillna(-1)
            diverted = to_num(df.Diverted).fillna(-1)

            origin_tz = norm_id(df.OriginAirportID).map(tz_map).astype("string")
            dest_tz = norm_id(df.DestAirportID).map(tz_map).astype("string")

            # Scheduled departure local datetime; 2400 represents next midnight.
            sched_dep_local = (
                flight_date
                + pd.to_timedelta(cdep + cdep_day.fillna(0) * 1440, unit="m")
            ).where(flight_date.notna() & cdep.notna())
            s0, s1 = localize_two_folds(sched_dep_local, origin_tz)
            s0_arr = s0 + pd.to_timedelta(crs_elapsed.where(crs_elapsed.ge(0)), unit="m")
            s1_arr = s1 + pd.to_timedelta(crs_elapsed.where(crs_elapsed.ge(0)), unit="m")
            _, s0_arr_min = utc_to_local(s0_arr, dest_tz)
            _, s1_arr_min = utc_to_local(s1_arr, dest_tz)
            sched_m0 = circular_minute_diff(s0_arr_min, carr).abs().le(TOLERANCE_MIN)
            sched_m1 = circular_minute_diff(s1_arr_min, carr).abs().le(TOLERANCE_MIN)
            sched_amb = s0.notna() & s1.notna() & s0.ne(s1)
            choose_s0 = sched_amb & sched_m0 & ~sched_m1
            choose_s1 = sched_amb & sched_m1 & ~sched_m0
            sched_unresolved = sched_amb & ~(choose_s0 | choose_s1)
            sched_dep_utc = s0.copy()
            sched_dep_utc.loc[choose_s1] = s1.loc[choose_s1]
            sched_dep_utc.loc[sched_unresolved] = pd.NaT
            sched_dep_status = pd.Series("UNIQUE_LOCAL_TIME", index=df.index, dtype="string")
            sched_dep_status.loc[sched_dep_local.isna()] = "MISSING_LOCAL_DATETIME"
            sched_dep_status.loc[origin_tz.isna() & sched_dep_local.notna()] = "MISSING_ORIGIN_TIMEZONE"
            sched_dep_status.loc[s0.isna() & s1.isna() & sched_dep_local.notna() & origin_tz.notna()] = "NONEXISTENT_LOCAL_TIME"
            sched_dep_status.loc[choose_s0 | choose_s1] = "AMBIGUOUS_RESOLVED_BY_CRS_ARRIVAL"
            sched_dep_status.loc[sched_unresolved] = "AMBIGUOUS_UNRESOLVED"
            sched_dep_status.loc[s0.notna() ^ s1.notna()] = "ONE_FOLD_ONLY"

            crs_valid = crs_elapsed.where(crs_elapsed.ge(0))
            sched_arr_utc = sched_dep_utc + pd.to_timedelta(crs_valid, unit="m")
            sched_arr_wall, sched_arr_min = utc_to_local(sched_arr_utc, dest_tz)
            sched_arr_resid = circular_minute_diff(sched_arr_min, carr)
            sched_arr_status = pd.Series("MATCH_WITHIN_1_MINUTE", index=df.index, dtype="string")
            sched_arr_status.loc[sched_dep_utc.isna()] = "SCHEDULED_DEP_UTC_UNAVAILABLE"
            sched_arr_status.loc[crs_elapsed.isna()] = "CRSElapsedTime_MISSING"
            sched_arr_status.loc[crs_elapsed.notna() & crs_elapsed.lt(0)] = "CRSElapsedTime_INVALID_NEGATIVE"
            sched_arr_status.loc[carr_st.eq("MISSING_HHMM")] = "CRSARRTIME_MISSING"
            sched_arr_status.loc[carr_st.eq("INVALID_HHMM")] = "CRSARRTIME_INVALID"
            sched_arr_status.loc[dest_tz.isna()] = "MISSING_DESTINATION_TIMEZONE"
            sched_arr_check = sched_arr_utc.notna() & sched_arr_min.notna() & carr.notna() & dest_tz.notna()
            sched_arr_status.loc[sched_arr_check & sched_arr_resid.abs().gt(TOLERANCE_MIN)] = "CRS_ELAPSED_VS_CRSARR_CLOCK_MISMATCH"

            # Actual departure candidate: UTC duration from scheduled departure.
            actual_dep_utc = sched_dep_utc + pd.to_timedelta(dep_delay, unit="m")
            dep_wall, dep_resid, dep_dst, dep_clock_status = clock_status_from_utc(
                actual_dep_utc, origin_tz, dep_clock
            )
            dep_clock_status.loc[dep_st.eq("INVALID_HHMM")] = "DEPTIME_INVALID"
            dep_clock_status.loc[dep_delay.isna()] = "DEPDELAY_MISSING"
            dep_clock_status.loc[sched_dep_utc.isna()] = "SCHEDULED_DEP_UTC_UNAVAILABLE"
            dep_clock_status.loc[dep_st.eq("MISSING_HHMM") & dep_delay.notna()] = "DEPTIME_MISSING"

            # Actual arrival candidate A: scheduled arrival UTC + ArrDelay.
            actual_arr_a_utc = sched_arr_utc + pd.to_timedelta(arr_delay, unit="m")
            # Candidate B: actual departure UTC + ActualElapsedTime.
            actual_arr_b_utc = actual_dep_utc + pd.to_timedelta(
                actual_elapsed.where(actual_elapsed.ge(0)), unit="m"
            )

            arr_fields_present = (
                ~is_missing(df.ArrTime)
                & ~is_missing(df.ArrDelay)
                & ~is_missing(df.ActualElapsedTime)
            )
            cancelled_rows = cancelled.eq(1)
            diverted_rows = diverted.eq(1) & ~cancelled_rows
            operated = cancelled.eq(0)
            diverted_destination_evaluable = diverted_rows & arr_fields_present
            ordinary_arrival_evaluable = operated & ~diverted_rows
            arr_evaluable = ordinary_arrival_evaluable | diverted_destination_evaluable

            arr_wall_a, arr_resid_a, arr_dst_a, arr_status_a = clock_status_from_utc(
                actual_arr_a_utc.where(arr_evaluable), dest_tz, arr_clock
            )
            arr_wall_b, arr_resid_b, arr_dst_b, arr_status_b = clock_status_from_utc(
                actual_arr_b_utc.where(arr_evaluable), dest_tz, arr_clock
            )

            # Candidate A depends on ArrDelay; candidate B depends on
            # ActualElapsedTime. Keep their missing-data statuses separate.
            arr_status_a.loc[cancelled_rows] = "CANCELLED_SEPARATE_HANDLING"
            arr_status_a.loc[diverted_rows & ~arr_fields_present] = "DIVERTED_SCHEDULED_DESTINATION_NOT_CONFIRMED"
            arr_status_a.loc[arr_evaluable & arr_delay.isna()] = "ARRDELAY_MISSING"
            arr_status_a.loc[arr_evaluable & arr_clock.isna()] = "ARRTIME_MISSING"

            arr_status_b.loc[cancelled_rows] = "CANCELLED_SEPARATE_HANDLING"
            arr_status_b.loc[diverted_rows & ~arr_fields_present] = "DIVERTED_SCHEDULED_DESTINATION_NOT_CONFIRMED"
            arr_status_b.loc[arr_evaluable & actual_elapsed.isna()] = "ACTUALELAPSEDTIME_MISSING"
            arr_status_b.loc[arr_evaluable & arr_clock.isna()] = "ARRTIME_MISSING"

            # Independent candidate comparison: do NOT gate this on ArrTime.
            pair_delta = (actual_arr_a_utc - actual_arr_b_utc).dt.total_seconds() / 60.0
            pair_ready = (
                arr_evaluable
                & actual_arr_a_utc.notna()
                & actual_arr_b_utc.notna()
                & actual_dep_utc.notna()
            )
            pair_status = pd.Series("UTC_PAIR_CANDIDATE_UNAVAILABLE", index=df.index, dtype="string")
            pair_status.loc[cancelled_rows] = "CANCELLED_SEPARATE_HANDLING"
            pair_status.loc[diverted_rows & ~arr_fields_present] = "DIVERTED_SCHEDULED_DESTINATION_NOT_CONFIRMED"
            pair_status.loc[pair_ready & pair_delta.abs().le(TOLERANCE_MIN)] = "UTC_CANDIDATE_PAIR_MATCHES_ACTUALELAPSEDTIME"
            pair_status.loc[pair_ready & pair_delta.abs().gt(TOLERANCE_MIN)] = "UTC_CANDIDATE_PAIR_MISMATCHES_ACTUALELAPSEDTIME"

            # Re-label the two arrival candidate clock checks as separate evidence.
            arr_status_a.loc[
                arr_evaluable & arr_delay.notna() & arr_clock.notna()
                & arr_resid_a.abs().le(TOLERANCE_MIN)
            ] = "ARRDELAY_UTC_CANDIDATE_MATCHES_ARRTIME"
            arr_status_a.loc[
                arr_evaluable & arr_delay.notna() & arr_clock.notna()
                & arr_resid_a.abs().gt(TOLERANCE_MIN)
            ] = "ARRDELAY_UTC_CANDIDATE_MISMATCHES_ARRTIME"
            arr_status_b.loc[
                arr_evaluable & actual_elapsed.notna() & arr_clock.notna()
                & arr_resid_b.abs().le(TOLERANCE_MIN)
            ] = "ELAPSED_UTC_CANDIDATE_MATCHES_ARRTIME"
            arr_status_b.loc[
                arr_evaluable & actual_elapsed.notna() & arr_clock.notna()
                & arr_resid_b.abs().gt(TOLERANCE_MIN)
            ] = "ELAPSED_UTC_CANDIDATE_MISMATCHES_ARRTIME"

            dims = [
                ("SCHEDULED_DEPARTURE_ORIGIN", sched_dep_status),
                ("SCHEDULED_ARRIVAL_DESTINATION", sched_arr_status),
                ("ACTUAL_DEPARTURE_UTC_CANDIDATE", dep_clock_status),
                ("ACTUAL_DEPARTURE_DST_LOCAL_CLOCK", dep_dst),
                ("ACTUAL_ARRIVAL_FROM_ARRDELAY_UTC", arr_status_a),
                ("ACTUAL_ARRIVAL_FROM_ELAPSED_UTC", arr_status_b),
                ("ACTUAL_ARRIVAL_ARRDELAY_CANDIDATE_DST", arr_dst_a),
                ("ACTUAL_ARRIVAL_ELAPSED_CANDIDATE_DST", arr_dst_b),
                ("ACTUAL_ARRIVAL_CANDIDATE_PAIR", pair_status),
            ]
            for dim, st in dims:
                add_counts(month_counts, month, dim, st)

            for dim, zone, st in [
                ("SCHEDULED_DEPARTURE_ORIGIN", origin_tz, sched_dep_status),
                ("ACTUAL_DEPARTURE_UTC_CANDIDATE", origin_tz, dep_clock_status),
                ("ACTUAL_DEPARTURE_DST_LOCAL_CLOCK", origin_tz, dep_dst),
                ("SCHEDULED_ARRIVAL_DESTINATION", dest_tz, sched_arr_status),
                ("ACTUAL_ARRIVAL_FROM_ARRDELAY_UTC", dest_tz, arr_status_a),
                ("ACTUAL_ARRIVAL_FROM_ELAPSED_UTC", dest_tz, arr_status_b),
                ("ACTUAL_ARRIVAL_ARRDELAY_CANDIDATE_DST", dest_tz, arr_dst_a),
                ("ACTUAL_ARRIVAL_ELAPSED_CANDIDATE_DST", dest_tz, arr_dst_b),
            ]:
                add_tz_counts(tz_counts, zone, dim, st)

            base = pd.DataFrame({
                "SourceFile": path.name,
                "SourceMonth": month,
                "FlightDate": df.FlightDate,
                "Reporting_Airline": df.Reporting_Airline,
                "Flight_Number_Reporting_Airline": df.Flight_Number_Reporting_Airline,
                "Origin": df.Origin,
                "Dest": df.Dest,
                "OriginAirportID": df.OriginAirportID,
                "DestAirportID": df.DestAirportID,
                "OriginTZ": origin_tz,
                "DestTZ": dest_tz,
                "Cancelled": cancelled,
                "Diverted": diverted,
                "CRSDepTime": df.CRSDepTime,
                "DepTime": df.DepTime,
                "DepDelay": df.DepDelay,
                "CRSArrTime": df.CRSArrTime,
                "ArrTime": df.ArrTime,
                "ArrDelay": df.ArrDelay,
                "CRSElapsedTime": df.CRSElapsedTime,
                "ActualElapsedTime": df.ActualElapsedTime,
                "ScheduledDepUTC": sched_dep_utc.astype("string"),
                "ScheduledDepStatus": sched_dep_status,
                "ScheduledArrUTC": sched_arr_utc.astype("string"),
                "ScheduledArrStatus": sched_arr_status,
                "ScheduledArrClockResidualMin": sched_arr_resid,
                "ActualDepUTC_Candidate": actual_dep_utc.astype("string"),
                "ActualDepClockResidualMin": dep_resid,
                "ActualDepClockStatus": dep_clock_status,
                "ActualDepLocalFromUTC": dep_wall,
                "ActualArrUTC_FromArrDelay": actual_arr_a_utc.astype("string"),
                "ActualArrClockResidualFromArrDelayMin": arr_resid_a,
                "ActualArrClockStatusFromArrDelay": arr_status_a,
                "ActualArrUTC_FromElapsed": actual_arr_b_utc.astype("string"),
                "ActualArrClockResidualFromElapsedMin": arr_resid_b,
                "ActualArrClockStatusFromElapsed": arr_status_b,
                "ActualArrCandidateDeltaMin": pair_delta,
                "ActualArrCandidatePairStatus": pair_status,
            })

            capture(base, "SCHEDULED_ARRIVAL_MISMATCH", sched_arr_status.eq("CRS_ELAPSED_VS_CRSARR_CLOCK_MISMATCH"))
            capture(base, "ACTUAL_DEPARTURE_CLOCK_MISMATCH", dep_clock_status.eq("UTC_CANDIDATE_CLOCK_MISMATCH"))
            capture(base, "ACTUAL_ARRIVAL_ARRDELAY_CLOCK_MISMATCH", arr_status_a.eq("ARRDELAY_UTC_CANDIDATE_MISMATCHES_ARRTIME"))
            capture(base, "ACTUAL_ARRIVAL_ELAPSED_CLOCK_MISMATCH", arr_status_b.eq("ELAPSED_UTC_CANDIDATE_MISMATCHES_ARRTIME"))
            capture(base, "ACTUAL_ARRIVAL_METHOD_PAIR_MISMATCH", pair_status.eq("UTC_CANDIDATE_PAIR_MISMATCHES_ACTUALELAPSEDTIME"))
            capture(base, "ACTUAL_ARRIVAL_DST_AMBIGUOUS", arr_dst_a.eq("AMBIGUOUS_LOCAL_TIME") & arr_evaluable)
            capture(base, "ACTUAL_ARRIVAL_DST_GAP_FROM_C4_RECHECK", arr_status_a.eq("UNEXPECTED_NONEXISTENT_CONVERTED_LOCAL_TIME"))

        logger.info("Completed %s; rows=%s", path.name, f"{file_rows:,}")

    month_df = pd.DataFrame([
        {"SourceMonth": m, "AuditDimension": d, "AuditStatus": s, "Rows": v}
        for (m, d, s), v in month_counts.items()
    ]).sort_values(["SourceMonth", "AuditDimension", "AuditStatus"])

    tz_df = pd.DataFrame([
        {"IANA_Timezone": z, "AuditDimension": d, "AuditStatus": s, "Rows": v}
        for (z, d, s), v in tz_counts.items()
    ]).sort_values(["IANA_Timezone", "AuditDimension", "AuditStatus"])

    field_df = pd.DataFrame([
        {"FieldName": f, "Metric": m, "Rows": int(v)}
        for (f, m), v in field_counts.items()
    ]).sort_values(["FieldName", "Metric"])

    samples_df = pd.concat(sample_frames, ignore_index=True) if sample_frames else pd.DataFrame()

    month_df.to_csv(out_dir / OUTPUTS[0], index=False, encoding="utf-8-sig")
    tz_df.to_csv(out_dir / OUTPUTS[1], index=False, encoding="utf-8-sig")
    field_df.to_csv(out_dir / OUTPUTS[2], index=False, encoding="utf-8-sig")
    samples_df.to_csv(out_dir / OUTPUTS[3], index=False, encoding="utf-8-sig")

    summary = [
        "STEP 27B-3C5 - INDEPENDENT UTC CANDIDATE VALIDATION SUMMARY",
        "=" * 78,
        f"BTS files scanned: {len(files)}",
        f"BTS records scanned: {total_rows:,}",
        f"Expected records: {EXPECTED_ROWS:,}",
        f"Timezone mapping rows: {len(mp):,}",
        "",
        "METHOD:",
        "- ScheduledDepUTC = localized FlightDate + CRSDepTime at origin.",
        "- ScheduledArrUTC = ScheduledDepUTC + CRSElapsedTime.",
        "- ActualDepUTC candidate = ScheduledDepUTC + DepDelay (UTC-offset audit hypothesis).",
        "- ActualArrUTC candidate A = ScheduledArrUTC + ArrDelay (UTC-offset audit hypothesis).",
        "- BTS specifies local event-time reporting and delay differences; UTC-offset arithmetic here is a tested audit hypothesis, not an explicit BTS-mandated conversion rule.",
        "- ActualArrUTC candidate B = ActualDepUTC + ActualElapsedTime.",
        "- UTC candidates are converted back to local time and checked independently against DepTime/ArrTime.",
        "- Candidate A vs B elapsed-time comparison is independent of ArrTime clock matching.",
        "- Ambiguous local display clocks are identified from the UTC instant; no DST gap is shifted.",
        "- Cancelled/diverted rows are retained; diverted destination arrival is evaluated only when arrival fields are present.",
        "- Audit only: no automatic correction, no CLEAN timestamp approval, no source mutation.",
        "- No PyArrow dependency.",
        "",
        "TOTALS BY DIMENSION AND STATUS:",
    ]
    totals = month_df.groupby(["AuditDimension", "AuditStatus"], as_index=False)["Rows"].sum()
    for dim in totals.AuditDimension.drop_duplicates():
        summary.append(f"\n[{dim}]")
        for row in totals[totals.AuditDimension.eq(dim)].itertuples(index=False):
            summary.append(f"{row.AuditStatus}: {int(row.Rows):,}")
    summary += ["", "OUTPUT FILES:"] + [str(out_dir / x) for x in OUTPUTS if not x.endswith(".log")]

    (out_dir / OUTPUTS[4]).write_text("\n".join(summary) + "\n", encoding="utf-8")

    logger.info("Completed rows=%s; expected=%s", f"{total_rows:,}", f"{EXPECTED_ROWS:,}")
    logger.info("Summary: %s", out_dir / OUTPUTS[4])
    logger.info("Samples: %s", out_dir / OUTPUTS[3])

    return 0 if total_rows == EXPECTED_ROWS else 1


if __name__ == "__main__":
    raise SystemExit(main())
