from __future__ import annotations

import logging
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

# =============================================================================
# STEP 27B-3C4
# Local-wall-clock delay candidates + UTC/DST cross-validation.
# RAW CSV is read-only. No PyArrow. No STAGING/CLEAN mutation.
#
# Key distinction:
#   DepDelay / ArrDelay are differences between local CRS and local actual
#   clock times (BTS Technical Directive #39, 2025). Therefore C4 first builds
#   local wall-clock candidates:
#       ActualDepLocalCandidate = ScheduledDepLocal + DepDelay
#       ActualArrLocalCandidate = ScheduledArrLocal + ArrDelay
#   Then localizes each candidate using the airport IANA timezone. Ambiguous
#   DST folds are retained as two UTC candidates; nonexistent times are not
#   shifted. ActualElapsedTime is used to cross-check UTC departure/arrival
#   candidate pairs, not to silently overwrite local source values.
#
# Candidate timestamps are audit outputs only, not approved CLEAN values.
# =============================================================================

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
    "27B3C4_event_status_by_month.csv",
    "27B3C4_timezone_dst_profile.csv",
    "27B3C4_time_field_profile.csv",
    "27B3C4_event_timestamp_samples.csv",
    "27B3C4_utc_event_timestamp_summary.txt",
    "27B3C4_utc_event_timestamp_audit.log",
]


def is_missing(s: pd.Series) -> pd.Series:
    x = s.astype("string").str.strip()
    return x.isna() | x.eq("") | x.str.upper().isin(["N/A", "NULL", "NONE"])


def to_num(s: pd.Series) -> pd.Series:
    x = s.astype("string").str.strip().mask(is_missing(s))
    return pd.to_numeric(x, errors="coerce")


def norm_id(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip().str.replace(r"\.0$", "", regex=True)


def parse_hhmm(s: pd.Series):
    raw = s.astype("string").str.strip()
    miss = is_missing(raw)
    nfloat = pd.to_numeric(raw.mask(miss), errors="coerce")
    integer = nfloat.notna() & np.isclose(nfloat.fillna(0) % 1, 0)
    n = nfloat.fillna(-999999).astype("int64")
    special_2400 = integer & n.eq(2400)
    regular = (
        integer & n.ge(0) & n.le(2359)
        & n.mod(100).lt(60) & n.floordiv(100).le(23)
    )
    valid = regular | special_2400
    minute = pd.Series(np.nan, index=s.index, dtype="float64")
    minute.loc[regular] = (
        (n.loc[regular] // 100) * 60 + n.loc[regular] % 100
    ).astype(float)
    minute.loc[special_2400] = 0.0
    day_add = pd.Series(0.0, index=s.index, dtype="float64")
    day_add.loc[special_2400] = 1.0
    status = pd.Series("INVALID_HHMM", index=s.index, dtype="string")
    status.loc[miss] = "MISSING_HHMM"
    status.loc[valid] = "VALID_HHMM"
    return minute, day_add, status, special_2400


def make_local_datetime(date_s: pd.Series, minute_s: pd.Series,
                        day_add: pd.Series | None = None) -> pd.Series:
    if day_add is None:
        day_add = pd.Series(0.0, index=date_s.index)
    out = date_s + pd.to_timedelta(
        minute_s + day_add.fillna(0) * 1440, unit="m"
    )
    return out.where(date_s.notna() & minute_s.notna())


def localize_two_folds(local_naive: pd.Series, zones: pd.Series):
    u0 = pd.Series(pd.NaT, index=local_naive.index, dtype="datetime64[ns, UTC]")
    u1 = pd.Series(pd.NaT, index=local_naive.index, dtype="datetime64[ns, UTC]")
    for zone in zones.dropna().astype("string").unique():
        mask = zones.eq(zone).fillna(False) & local_naive.notna()
        if not mask.any():
            continue
        try:
            x0 = local_naive.loc[mask].dt.tz_localize(
                str(zone), ambiguous=True, nonexistent="NaT"
            ).dt.tz_convert("UTC")
            x1 = local_naive.loc[mask].dt.tz_localize(
                str(zone), ambiguous=False, nonexistent="NaT"
            ).dt.tz_convert("UTC")
            u0.loc[mask] = x0
            u1.loc[mask] = x1
        except Exception as exc:
            raise RuntimeError(f"Timezone localization failed for {zone}: {exc}") from exc
    return u0, u1


def utc_to_local(utc_s: pd.Series, zones: pd.Series):
    wall = pd.Series(pd.NaT, index=utc_s.index, dtype="datetime64[ns]")
    minute = pd.Series(np.nan, index=utc_s.index, dtype="float64")
    for zone in zones.dropna().astype("string").unique():
        mask = zones.eq(zone).fillna(False) & utc_s.notna()
        if not mask.any():
            continue
        local = utc_s.loc[mask].dt.tz_convert(str(zone))
        wall.loc[mask] = local.dt.tz_localize(None)
        minute.loc[mask] = (
            local.dt.hour.astype(float) * 60
            + local.dt.minute.astype(float)
            + local.dt.second.astype(float) / 60
        )
    return wall, minute


def classify_local(local: pd.Series, zones: pd.Series,
                   u0: pd.Series, u1: pd.Series):
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
    return ((a - b + 720) % 1440) - 720


def add_counts(counter: Counter, month: pd.Series,
               dimension: str, status: pd.Series):
    temp = pd.DataFrame({"month": month, "status": status})
    for (m, st), count in temp.groupby(
        ["month", "status"], dropna=False
    ).size().items():
        counter[(str(m), dimension, str(st))] += int(count)


def add_tz_counts(counter: Counter, zones: pd.Series,
                  dimension: str, status: pd.Series):
    temp = pd.DataFrame({"zone": zones, "status": status})
    for (z, st), count in temp.groupby(
        ["zone", "status"], dropna=False
    ).size().items():
        counter[(str(z), dimension, str(st))] += int(count)


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    raw_dir = root / "01_RAW_DATA" / "BTS" / "csv"
    out_dir = root / "06_DW_DESIGN" / "01_LOGICAL_MODEL"
    mapping_path = out_dir / "27B3B_airport_timezone_mapping_candidates.csv"
    out_dir.mkdir(parents=True, exist_ok=True)

    existing = [str(out_dir / name) for name in OUTPUTS if (out_dir / name).exists()]
    if existing:
        print("STOP - C4 outputs already exist; archive/rename before rerunning:\n" + "\n".join(existing))
        return 2

    if not raw_dir.exists() or not mapping_path.exists():
        print(f"STOP - Missing RAW directory or timezone mapping:\n{raw_dir}\n{mapping_path}")
        return 2

    files = sorted(raw_dir.glob("BTS_2025_*.csv"))
    expected_names = [f"BTS_2025_{m:02d}.csv" for m in range(1, 13)]
    if [p.name for p in files] != expected_names:
        print(f"STOP - Expected these 12 monthly files; found {[p.name for p in files]}")
        return 2

    logger = logging.getLogger("step27b3c4")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    for handler in [
        logging.FileHandler(out_dir / OUTPUTS[5], encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]:
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    mapping = pd.read_csv(mapping_path, dtype="string", keep_default_na=False)
    required = {"BTSAirportID", "TimeZoneIANA"}
    if not required.issubset(mapping.columns):
        logger.error("Timezone map lacks columns: %s", sorted(required - set(mapping.columns)))
        return 2
    mapping["BTSAirportID"] = norm_id(mapping["BTSAirportID"])
    mapping["TimeZoneIANA"] = mapping["TimeZoneIANA"].astype("string").str.strip()
    if mapping["BTSAirportID"].duplicated().any() or mapping["TimeZoneIANA"].eq("").any():
        logger.error("Duplicate airport IDs or blank IANA timezone in mapping")
        return 2
    tz_map = dict(zip(mapping["BTSAirportID"].astype(str), mapping["TimeZoneIANA"].astype(str)))

    logger.info("STEP 27B-3C4 - LOCAL DELAY / UTC CANDIDATE AUDIT")
    logger.info("Files=%d; timezone rows=%d; chunk=%d", len(files), len(mapping), CHUNK_SIZE)
    logger.info("RAW CSV read-only; no PyArrow; no STAGING/CLEAN mutation")

    month_counts = Counter()
    tz_counts = Counter()
    field_counts = Counter()
    samples: list[pd.DataFrame] = []
    sample_counts = Counter()
    total_rows = 0

    def collect(base: pd.DataFrame, issue: str, mask: pd.Series):
        if not mask.any():
            return
        for mon, group in base.loc[mask].groupby("SourceMonth", dropna=False):
            key = (issue, str(mon))
            remain = SAMPLES_PER_ISSUE_MONTH - sample_counts[key]
            if remain > 0:
                part = group.head(remain).copy()
                part.insert(0, "AuditIssue", issue)
                samples.append(part)
                sample_counts[key] += len(part)

    for file_no, path in enumerate(files, 1):
        source_month = path.stem[-2:]
        logger.info("Reading %s (%d/12)", path.name, file_no)
        file_rows = 0
        reader = pd.read_csv(
            path,
            usecols=INPUT_COLUMNS,
            dtype="string",
            keep_default_na=False,
            chunksize=CHUNK_SIZE,
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
            flight_date = pd.to_datetime(df["FlightDate"].str.strip(), errors="coerce")

            parsed = {name: parse_hhmm(df[name]) for name in [
                "CRSDepTime", "DepTime", "CRSArrTime", "ArrTime"
            ]}
            for field, (_, _, st, special) in parsed.items():
                field_counts[(field, "Rows")] += n
                field_counts[(field, "ValidHHMM")] += int(st.eq("VALID_HHMM").sum())
                field_counts[(field, "MissingHHMM")] += int(st.eq("MISSING_HHMM").sum())
                field_counts[(field, "InvalidHHMM")] += int(st.eq("INVALID_HHMM").sum())
                field_counts[(field, "Special2400")] += int(special.sum())

            cdep, cdep_add, cdep_st, _ = parsed["CRSDepTime"]
            dep_clock, _, dep_st, _ = parsed["DepTime"]
            carr, carr_add, carr_st, _ = parsed["CRSArrTime"]
            arr_clock, _, arr_st, _ = parsed["ArrTime"]

            crs_elapsed = to_num(df["CRSElapsedTime"])
            actual_elapsed = to_num(df["ActualElapsedTime"])
            dep_delay = to_num(df["DepDelay"])
            arr_delay = to_num(df["ArrDelay"])
            cancelled = to_num(df["Cancelled"]).fillna(-1)
            diverted = to_num(df["Diverted"]).fillna(-1)

            origin_tz = norm_id(df["OriginAirportID"]).map(tz_map).astype("string")
            dest_tz = norm_id(df["DestAirportID"]).map(tz_map).astype("string")

            # 1. Scheduled departure: FlightDate + CRSDepTime, origin local.
            sched_dep_local = make_local_datetime(flight_date, cdep, cdep_add)
            sd0, sd1 = localize_two_folds(sched_dep_local, origin_tz)
            sched_dep_dst = classify_local(sched_dep_local, origin_tz, sd0, sd1)

            # 2. Scheduled arrival UTC is based on the elapsed duration.
            crs_elapsed_valid = crs_elapsed.where(crs_elapsed.ge(0))
            sa0_utc = sd0 + pd.to_timedelta(crs_elapsed_valid, unit="m")
            sa1_utc = sd1 + pd.to_timedelta(crs_elapsed_valid, unit="m")
            sa0_wall, sa0_min = utc_to_local(sa0_utc, dest_tz)
            sa1_wall, sa1_min = utc_to_local(sa1_utc, dest_tz)
            m_sched0 = circular_minute_diff(sa0_min, carr).abs().le(TOLERANCE_MIN)
            m_sched1 = circular_minute_diff(sa1_min, carr).abs().le(TOLERANCE_MIN)

            sched_dep_utc = sd0.copy()
            sched_dep_status = sched_dep_dst.copy()
            sched_amb = sched_dep_dst.eq("AMBIGUOUS_LOCAL_TIME")
            sched_choose0 = sched_amb & m_sched0 & ~m_sched1
            sched_choose1 = sched_amb & m_sched1 & ~m_sched0
            sched_unresolved = sched_amb & ~(sched_choose0 | sched_choose1)
            sched_dep_utc.loc[sched_choose1] = sd1.loc[sched_choose1]
            sched_dep_utc.loc[sched_unresolved] = pd.NaT
            sched_dep_status.loc[sched_choose0 | sched_choose1] = "AMBIGUOUS_RESOLVED_BY_CRS_ARRIVAL"
            sched_dep_status.loc[sched_unresolved] = "AMBIGUOUS_UNRESOLVED"

            sched_arr_utc = sched_dep_utc + pd.to_timedelta(crs_elapsed_valid, unit="m")
            sched_arr_local, sched_arr_min = utc_to_local(sched_arr_utc, dest_tz)
            sched_arr_resid = circular_minute_diff(sched_arr_min, carr)
            sched_arr_status = pd.Series("MATCH_WITHIN_1_MINUTE", index=df.index, dtype="string")
            sched_arr_status.loc[sched_dep_utc.isna()] = "SCHEDULED_DEP_UTC_UNAVAILABLE"
            sched_arr_status.loc[crs_elapsed.isna()] = "CRSElapsedTime_MISSING"
            sched_arr_status.loc[crs_elapsed.notna() & crs_elapsed.lt(0)] = "CRSElapsedTime_INVALID_NEGATIVE"
            sched_arr_status.loc[carr_st.eq("MISSING_HHMM")] = "CRSARRTIME_MISSING"
            sched_arr_status.loc[carr_st.eq("INVALID_HHMM")] = "CRSARRTIME_INVALID"
            sched_arr_status.loc[dest_tz.isna()] = "MISSING_DESTINATION_TIMEZONE"
            sched_arr_check = (
                sched_dep_utc.notna() & crs_elapsed_valid.notna()
                & sched_arr_min.notna() & carr.notna() & dest_tz.notna()
            )
            sched_arr_status.loc[
                sched_arr_check & sched_arr_resid.abs().gt(TOLERANCE_MIN)
            ] = "CRS_ELAPSED_VS_CRSARR_CLOCK_MISMATCH"

            # 3. Actual departure local candidate from the local BTS delay definition.
            # Do not add DepDelay directly to UTC here.
            actual_dep_local = (
                sched_dep_local + pd.to_timedelta(dep_delay, unit="m")
            ).where(sched_dep_local.notna() & dep_delay.notna())
            ad0, ad1 = localize_two_folds(actual_dep_local, origin_tz)
            actual_dep_dst = classify_local(actual_dep_local, origin_tz, ad0, ad1)

            dep_local_min = (
                actual_dep_local.dt.hour.astype("float64") * 60
                + actual_dep_local.dt.minute.astype("float64")
                + actual_dep_local.dt.second.astype("float64") / 60
            )
            dep_resid = circular_minute_diff(dep_local_min, dep_clock)
            dep_clock_match = (
                actual_dep_local.notna() & dep_clock.notna()
                & dep_resid.abs().le(TOLERANCE_MIN)
            )

            actual_dep_status = pd.Series("LOCAL_DEPDELAY_CLOCK_MATCH", index=df.index, dtype="string")
            actual_dep_status.loc[cancelled.eq(1)] = "CANCELLED_SEPARATE_HANDLING"
            actual_dep_status.loc[dep_delay.isna() & cancelled.ne(1)] = "DEPDELAY_MISSING"
            actual_dep_status.loc[dep_st.eq("MISSING_HHMM") & cancelled.ne(1)] = "DEPTIME_MISSING"
            actual_dep_status.loc[dep_st.eq("INVALID_HHMM") & cancelled.ne(1)] = "DEPTIME_INVALID"
            actual_dep_status.loc[origin_tz.isna() & cancelled.ne(1)] = "MISSING_ORIGIN_TIMEZONE"
            actual_dep_status.loc[
                dep_clock_match & actual_dep_dst.eq("NONEXISTENT_LOCAL_TIME") & cancelled.ne(1)
            ] = "ACTUAL_DEP_LOCAL_CANDIDATE_IN_DST_GAP"
            actual_dep_status.loc[
                actual_dep_local.notna() & dep_clock.notna()
                & dep_resid.abs().gt(TOLERANCE_MIN) & cancelled.ne(1)
            ] = "DEPTIME_VS_LOCAL_DEPDELAY_MISMATCH"
            actual_dep_status.loc[
                dep_clock_match & actual_dep_dst.eq("AMBIGUOUS_LOCAL_TIME") & cancelled.ne(1)
            ] = "AMBIGUOUS_ACTUAL_DEP_FOLD_REQUIRES_ELAPSED_EVIDENCE"

            # 4. Actual arrival local candidate from scheduled local arrival + ArrDelay.
            # For diverted flights, evaluate the scheduled destination only when
            # the BTS arrival fields are populated; otherwise do not assume that
            # the aircraft reached its scheduled destination.
            arr_fields_present = (
                ~is_missing(df["ArrTime"])
                & ~is_missing(df["ArrDelay"])
                & ~is_missing(df["ActualElapsedTime"])
            )
            diverted_missing_scheduled_dest_arrival = (
                cancelled.eq(0) & diverted.eq(1) & ~arr_fields_present
            )
            arrival_evaluable = (
                cancelled.eq(0)
                & (diverted.eq(0) | (diverted.eq(1) & arr_fields_present))
            )

            scheduled_arr_baseline_ok = sched_arr_status.eq("MATCH_WITHIN_1_MINUTE")
            actual_arr_local = (
                sched_arr_local + pd.to_timedelta(arr_delay, unit="m")
            ).where(
                sched_arr_local.notna()
                & arr_delay.notna()
                & scheduled_arr_baseline_ok
            )
            aa0, aa1 = localize_two_folds(actual_arr_local, dest_tz)
            actual_arr_dst = classify_local(actual_arr_local, dest_tz, aa0, aa1)

            arr_local_min = (
                actual_arr_local.dt.hour.astype("float64") * 60
                + actual_arr_local.dt.minute.astype("float64")
                + actual_arr_local.dt.second.astype("float64") / 60
            )
            arr_resid = circular_minute_diff(arr_local_min, arr_clock)
            arr_clock_match = (
                actual_arr_local.notna() & arr_clock.notna()
                & arr_resid.abs().le(TOLERANCE_MIN)
            )

            actual_arr_local_status = pd.Series("NOT_EVALUATED", index=df.index, dtype="string")
            actual_arr_local_status.loc[
                arrival_evaluable & ~scheduled_arr_baseline_ok
            ] = "SCHEDULED_ARRIVAL_BASELINE_UNVERIFIED"
            actual_arr_local_status.loc[
                arrival_evaluable & scheduled_arr_baseline_ok
            ] = "LOCAL_ARRDELAY_CLOCK_MATCH"
            actual_arr_local_status.loc[cancelled.eq(1)] = "CANCELLED_SEPARATE_HANDLING"
            actual_arr_local_status.loc[diverted_missing_scheduled_dest_arrival] = (
                "DIVERTED_SCHEDULED_DESTINATION_ARRIVAL_NOT_REPORTED"
            )
            actual_arr_local_status.loc[
                arrival_evaluable & scheduled_arr_baseline_ok & arr_delay.isna()
            ] = "ARRDELAY_MISSING"
            actual_arr_local_status.loc[
                arrival_evaluable & scheduled_arr_baseline_ok & arr_st.eq("MISSING_HHMM")
            ] = "ARRTIME_MISSING"
            actual_arr_local_status.loc[
                arrival_evaluable & scheduled_arr_baseline_ok & arr_st.eq("INVALID_HHMM")
            ] = "ARRTIME_INVALID"
            actual_arr_local_status.loc[
                arrival_evaluable & scheduled_arr_baseline_ok
                & actual_arr_local.notna() & arr_clock.notna()
                & arr_resid.abs().gt(TOLERANCE_MIN)
            ] = "ARRTIME_VS_LOCAL_ARRDELAY_MISMATCH"
            actual_arr_local_status.loc[
                arrival_evaluable & scheduled_arr_baseline_ok & arr_clock_match
                & actual_arr_dst.eq("NONEXISTENT_LOCAL_TIME")
            ] = "ACTUAL_ARR_LOCAL_CANDIDATE_IN_DST_GAP"
            actual_arr_local_status.loc[
                arrival_evaluable & scheduled_arr_baseline_ok & arr_clock_match
                & actual_arr_dst.eq("AMBIGUOUS_LOCAL_TIME")
            ] = "AMBIGUOUS_ACTUAL_ARR_FOLD_REQUIRES_ELAPSED_EVIDENCE"

            # 5. Cross-check UTC candidate pairs with ActualElapsedTime.
            # For unique local times, fold 0 and fold 1 are identical. For a
            # true DST fold, count distinct UTC pairs only once.
            dep0_valid = ad0.notna()
            dep1_distinct = ad1.notna() & ad0.notna() & ad1.ne(ad0)
            arr0_valid = aa0.notna()
            arr1_distinct = aa1.notna() & aa0.notna() & aa1.ne(aa0)
            elapsed_valid = actual_elapsed.where(actual_elapsed.ge(0))

            pair_scope = (
                arrival_evaluable & elapsed_valid.notna()
                & dep_clock_match & arr_clock_match
                & scheduled_arr_baseline_ok
            )

            def pair_match(dep_utc: pd.Series, arr_utc: pd.Series, enabled: pd.Series):
                delta = (arr_utc - dep_utc).dt.total_seconds() / 60.0
                return (
                    pair_scope & enabled & delta.notna()
                    & (delta - elapsed_valid).abs().le(TOLERANCE_MIN)
                ), delta

            p00, d00 = pair_match(ad0, aa0, dep0_valid & arr0_valid)
            p01, d01 = pair_match(ad0, aa1, dep0_valid & arr1_distinct)
            p10, d10 = pair_match(ad1, aa0, dep1_distinct & arr0_valid)
            p11, d11 = pair_match(ad1, aa1, dep1_distinct & arr1_distinct)

            pair_count = (
                p00.astype("int8") + p01.astype("int8")
                + p10.astype("int8") + p11.astype("int8")
            )

            # Default assignment is allowed only for a unique local timestamp.
            selected_dep_utc = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")
            selected_arr_utc = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")
            dep_unique = (
                cancelled.eq(0)
                & actual_dep_dst.eq("UNIQUE_LOCAL_TIME")
                & dep_clock_match
            )
            arr_unique = (
                arrival_evaluable & scheduled_arr_baseline_ok
                & actual_arr_dst.eq("UNIQUE_LOCAL_TIME")
                & arr_clock_match
            )
            selected_dep_utc.loc[dep_unique] = ad0.loc[dep_unique]
            selected_arr_utc.loc[arr_unique] = aa0.loc[arr_unique]

            # A unique elapsed-time-consistent pair can resolve one or both folds.
            select00 = p00 & pair_count.eq(1)
            select01 = p01 & pair_count.eq(1)
            select10 = p10 & pair_count.eq(1)
            select11 = p11 & pair_count.eq(1)
            selected_dep_utc.loc[select00 | select01] = ad0.loc[select00 | select01]
            selected_arr_utc.loc[select00 | select10] = aa0.loc[select00 | select10]
            selected_dep_utc.loc[select10 | select11] = ad1.loc[select10 | select11]
            selected_arr_utc.loc[select01 | select11] = aa1.loc[select01 | select11]

            # If arrival delay candidate is unavailable, a unique actual departure
            # still supports an elapsed-based arrival UTC candidate; it does not
            # resolve an ambiguous departure fold.
            arr_utc_from_elapsed = (
                selected_dep_utc + pd.to_timedelta(elapsed_valid, unit="m")
            )
            arr_elapsed_wall, arr_elapsed_min = utc_to_local(
                arr_utc_from_elapsed, dest_tz
            )
            arr_elapsed_clock_resid = circular_minute_diff(arr_elapsed_min, arr_clock)

            dep_utc_status = actual_dep_status.copy()
            dep_utc_status.loc[
                actual_dep_dst.eq("AMBIGUOUS_LOCAL_TIME") & dep_clock_match
                & selected_dep_utc.notna()
            ] = "AMBIGUOUS_FOLD_RESOLVED_BY_ACTUAL_ELAPSED_TIME"
            dep_utc_status.loc[
                actual_dep_dst.eq("AMBIGUOUS_LOCAL_TIME") & dep_clock_match
                & selected_dep_utc.isna()
            ] = "AMBIGUOUS_FOLD_UNRESOLVED"
            dep_utc_status.loc[
                dep_unique & selected_dep_utc.notna()
            ] = "UNIQUE_LOCAL_TIME_UTC_CANDIDATE"

            arr_utc_status = actual_arr_local_status.copy()
            arr_utc_status.loc[
                actual_arr_dst.eq("AMBIGUOUS_LOCAL_TIME") & arr_clock_match
                & selected_arr_utc.notna()
            ] = "AMBIGUOUS_FOLD_RESOLVED_BY_ACTUAL_ELAPSED_TIME"
            arr_utc_status.loc[
                actual_arr_dst.eq("AMBIGUOUS_LOCAL_TIME") & arr_clock_match
                & selected_arr_utc.isna()
            ] = "AMBIGUOUS_FOLD_UNRESOLVED"
            arr_utc_status.loc[
                arr_unique & selected_arr_utc.notna()
            ] = "UNIQUE_LOCAL_TIME_UTC_CANDIDATE"

            elapsed_status = pd.Series("NOT_EVALUATED", index=df.index, dtype="string")
            cancelled_mask = cancelled.eq(1)
            diverted_no_arrival = (
                cancelled.ne(1) & diverted.eq(1)
                & (arr_delay.isna() | arr_clock.isna() | actual_elapsed.isna())
            )
            evaluable = arrival_evaluable
            elapsed_status.loc[cancelled_mask] = "CANCELLED_SEPARATE_HANDLING"
            elapsed_status.loc[diverted_missing_scheduled_dest_arrival] = (
                "DIVERTED_ORIGINAL_DESTINATION_NOT_CONFIRMED"
            )
            elapsed_status.loc[evaluable & actual_elapsed.isna()] = "ACTUALELAPSEDTIME_MISSING"
            elapsed_status.loc[evaluable & actual_elapsed.notna() & actual_elapsed.lt(0)] = "ACTUALELAPSEDTIME_INVALID_NEGATIVE"
            elapsed_status.loc[evaluable & pair_count.eq(1)] = "UNIQUE_UTC_PAIR_MATCHES_ACTUALELAPSEDTIME"
            elapsed_status.loc[evaluable & pair_count.gt(1)] = "MULTIPLE_UTC_PAIRS_MATCH_ACTUALELAPSEDTIME"
            elapsed_status.loc[
                evaluable & elapsed_valid.notna() & pair_count.eq(0)
                & ad0.notna() & aa0.notna()
            ] = "NO_UTC_CANDIDATE_PAIR_MATCHES_ACTUALELAPSEDTIME"
            elapsed_status.loc[
                evaluable & elapsed_valid.notna() & pair_count.eq(0)
                & (ad0.isna() | aa0.isna())
            ] = "UTC_PAIR_CANDIDATE_UNAVAILABLE_OR_UNRESOLVED"

            # Actual arrival computed from the selected actual-departure UTC + elapsed.
            elapsed_arr_status = pd.Series("NOT_EVALUATED", index=df.index, dtype="string")
            elapsed_arr_status.loc[cancelled_mask] = "CANCELLED_SEPARATE_HANDLING"
            elapsed_arr_status.loc[diverted_missing_scheduled_dest_arrival] = (
                "DIVERTED_ORIGINAL_DESTINATION_NOT_CONFIRMED"
            )
            elapsed_arr_status.loc[evaluable & selected_dep_utc.isna()] = "ACTUAL_DEP_UTC_UNAVAILABLE_OR_UNRESOLVED"
            elapsed_arr_status.loc[evaluable & elapsed_valid.isna()] = "ACTUALELAPSEDTIME_MISSING_OR_INVALID"
            elapsed_arr_check = (
                evaluable & arr_utc_from_elapsed.notna()
                & arr_clock.notna() & arr_elapsed_min.notna()
            )
            elapsed_arr_status.loc[
                elapsed_arr_check
                & arr_elapsed_clock_resid.abs().le(TOLERANCE_MIN)
            ] = "ARRIVAL_CLOCK_MATCHES_DEPARTURE_UTC_PLUS_ELAPSED"
            elapsed_arr_status.loc[
                elapsed_arr_check
                & arr_elapsed_clock_resid.abs().gt(TOLERANCE_MIN)
            ] = "ARRIVAL_CLOCK_MISMATCH_VS_DEPARTURE_UTC_PLUS_ELAPSED"

            dimensions = [
                ("SCHEDULED_DEPARTURE_ORIGIN", sched_dep_status),
                ("SCHEDULED_ARRIVAL_DESTINATION", sched_arr_status),
                ("ACTUAL_DEPARTURE_LOCAL_DELAY", actual_dep_status),
                ("ACTUAL_DEPARTURE_UTC_CANDIDATE", dep_utc_status),
                ("ACTUAL_ARRIVAL_LOCAL_DELAY", actual_arr_local_status),
                ("ACTUAL_ARRIVAL_UTC_CANDIDATE", arr_utc_status),
                ("ACTUAL_ELAPSED_TIME_CROSSCHECK", elapsed_status),
                ("ACTUAL_ARRIVAL_FROM_ELAPSED", elapsed_arr_status),
            ]
            for dim, status in dimensions:
                add_counts(month_counts, month, dim, status)

            for dim, zone, status in [
                ("SCHEDULED_DEPARTURE_ORIGIN", origin_tz, sched_dep_status),
                ("ACTUAL_DEPARTURE_UTC_CANDIDATE", origin_tz, dep_utc_status),
                ("SCHEDULED_ARRIVAL_DESTINATION", dest_tz, sched_arr_status),
                ("ACTUAL_ARRIVAL_UTC_CANDIDATE", dest_tz, arr_utc_status),
            ]:
                add_tz_counts(tz_counts, zone, dim, status)

            base = pd.DataFrame({
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
                "ScheduledDepLocalCandidate": sched_dep_local.astype("string"),
                "ScheduledDepUTC": sched_dep_utc.astype("string"),
                "ScheduledDepStatus": sched_dep_status,
                "ScheduledArrLocalFromUTC": sched_arr_local.astype("string"),
                "ScheduledArrUTC": sched_arr_utc.astype("string"),
                "ScheduledArrStatus": sched_arr_status,
                "ActualDepLocalFromDepDelay": actual_dep_local.astype("string"),
                "ActualDepUTC_Fold0": ad0.astype("string"),
                "ActualDepUTC_Fold1": ad1.astype("string"),
                "ActualDepUTC_Selected": selected_dep_utc.astype("string"),
                "ActualDepLocalClockResidualMin": dep_resid,
                "ActualDepUTCStatus": dep_utc_status,
                "ActualArrLocalFromArrDelay": actual_arr_local.astype("string"),
                "ActualArrUTC_Fold0": aa0.astype("string"),
                "ActualArrUTC_Fold1": aa1.astype("string"),
                "ActualArrUTC_Selected": selected_arr_utc.astype("string"),
                "ActualArrLocalClockResidualMin": arr_resid,
                "ActualArrUTCStatus": arr_utc_status,
                "ActualElapsedPairCount": pair_count,
                "ActualElapsedCrosscheckStatus": elapsed_status,
                "ActualArrUTC_FromDepUTCPlusElapsed": arr_utc_from_elapsed.astype("string"),
                "ActualArrClockResidualFromElapsedMin": arr_elapsed_clock_resid,
                "ActualArrElapsedClockStatus": elapsed_arr_status,
            })

            collect(base, "SCHEDULED_ARRIVAL_MISMATCH", sched_arr_status.eq("CRS_ELAPSED_VS_CRSARR_CLOCK_MISMATCH"))
            collect(base, "ACTUAL_DEPARTURE_LOCAL_DELAY_MISMATCH", actual_dep_status.eq("DEPTIME_VS_LOCAL_DEPDELAY_MISMATCH"))
            collect(base, "ACTUAL_DEPARTURE_DST_GAP", actual_dep_dst.eq("NONEXISTENT_LOCAL_TIME"))
            collect(base, "ACTUAL_DEPARTURE_FOLD_UNRESOLVED", dep_utc_status.eq("AMBIGUOUS_FOLD_UNRESOLVED"))
            collect(base, "ACTUAL_ARRIVAL_LOCAL_DELAY_MISMATCH", actual_arr_local_status.eq("ARRTIME_VS_LOCAL_ARRDELAY_MISMATCH"))
            collect(base, "ACTUAL_ARRIVAL_DST_GAP", actual_arr_dst.eq("NONEXISTENT_LOCAL_TIME"))
            collect(base, "ACTUAL_ARRIVAL_FOLD_UNRESOLVED", arr_utc_status.eq("AMBIGUOUS_FOLD_UNRESOLVED"))
            collect(base, "ACTUAL_ELAPSED_MISMATCH", elapsed_status.eq("NO_UTC_CANDIDATE_PAIR_MATCHES_ACTUALELAPSEDTIME"))
            collect(base, "ARRIVAL_CLOCK_MISMATCH_FROM_ELAPSED", elapsed_arr_status.eq("ARRIVAL_CLOCK_MISMATCH_VS_DEPARTURE_UTC_PLUS_ELAPSED"))

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

    samples_df = pd.concat(samples, ignore_index=True) if samples else pd.DataFrame()

    month_df.to_csv(out_dir / OUTPUTS[0], index=False, encoding="utf-8-sig")
    tz_df.to_csv(out_dir / OUTPUTS[1], index=False, encoding="utf-8-sig")
    field_df.to_csv(out_dir / OUTPUTS[2], index=False, encoding="utf-8-sig")
    samples_df.to_csv(out_dir / OUTPUTS[3], index=False, encoding="utf-8-sig")

    summary = [
        "STEP 27B-3C4 - LOCAL DELAY / UTC CANDIDATE AUDIT SUMMARY",
        "=" * 80,
        f"BTS files scanned: {len(files)}",
        f"BTS records scanned: {total_rows:,}",
        f"Expected records: {EXPECTED_ROWS:,}",
        f"Timezone mapping rows: {len(mapping):,}",
        "",
        "METHOD AND LIMITATIONS:",
        "- DepDelay is applied to the origin-local scheduled wall datetime to form an actual departure local candidate.",
        "- ArrDelay is applied to the destination-local scheduled arrival wall datetime to form an actual arrival local candidate.",
        "- Local candidates are localized to UTC with both DST folds retained; nonexistent wall times are not shifted.",
        "- ActualElapsedTime is used to test UTC departure/arrival candidate pairs.",
        "- Scheduled arrival UTC is scheduled departure UTC + CRSElapsedTime.",
        "- This is an audit candidate process, not automatic correction or approval for CLEAN.",
        "- Cancelled and diverted records are retained; diverted destination semantics require separate review.",
        "- RAW read-only; no STAGING/CLEAN mutation; no PyArrow dependency.",
        "",
        "TOTALS BY DIMENSION AND STATUS:",
    ]

    totals = month_df.groupby(["AuditDimension", "AuditStatus"], as_index=False)["Rows"].sum()
    for dimension in totals["AuditDimension"].drop_duplicates():
        summary.append(f"\n[{dimension}]")
        for row in totals[totals.AuditDimension.eq(dimension)].itertuples(index=False):
            summary.append(f"{row.AuditStatus}: {int(row.Rows):,}")

    summary += ["", "OUTPUT FILES:"]
    summary.extend(str(out_dir / name) for name in OUTPUTS if not name.endswith(".log"))
    (out_dir / OUTPUTS[4]).write_text("\n".join(summary) + "\n", encoding="utf-8")

    logger.info("Completed all rows=%s (expected=%s)", f"{total_rows:,}", f"{EXPECTED_ROWS:,}")
    logger.info("Summary: %s", out_dir / OUTPUTS[4])

    if total_rows != EXPECTED_ROWS:
        logger.error("Unexpected row count; do not proceed to CLEAN decisions.")
        return 1

    logger.info("Review C4 outputs before approving event timestamps for CLEAN.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
