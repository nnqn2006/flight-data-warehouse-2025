
from __future__ import annotations

import logging
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

# STEP 27B-3C3
# UTC-based cross-validation of BTS scheduled/actual flight events.
# Reads RAW monthly CSVs in chunks. No PyArrow. No RAW/STAGING/CLEAN mutation.
# C3 tests UTC event candidates derived from BTS delay and elapsed fields, then
# validates those candidates by converting them to airport-local time and
# comparing with reported HHMM. This is an audit hypothesis, not an automatic
# repair rule. It avoids subtracting naive local datetimes across DST transitions.

EXPECTED_ROWS = 7_001_619
CHUNK_SIZE = 100_000
TOLERANCE_MIN = 1.0
SAMPLES_PER_ISSUE_MONTH = 10

INPUT_COLUMNS = [
    "FlightDate", "Reporting_Airline", "Flight_Number_Reporting_Airline",
    "Origin", "Dest", "OriginAirportID", "DestAirportID",
    "CRSDepTime", "DepTime", "DepDelay", "CRSArrTime", "ArrTime",
    "ArrDelay", "CRSElapsedTime", "ActualElapsedTime", "Cancelled", "Diverted",
]

OUTPUTS = [
    "27B3C3_event_status_by_month.csv",
    "27B3C3_timezone_dst_profile.csv",
    "27B3C3_time_field_profile.csv",
    "27B3C3_event_timestamp_samples.csv",
    "27B3C3_utc_event_timestamp_summary.txt",
    "27B3C3_utc_event_timestamp_audit.log",
]


def is_missing(s: pd.Series) -> pd.Series:
    x = s.astype("string").str.strip()
    return x.isna() | x.eq("") | x.str.upper().isin(["N/A", "NULL", "NONE"])


def num(s: pd.Series) -> pd.Series:
    x = s.astype("string").str.strip().mask(is_missing(s))
    return pd.to_numeric(x, errors="coerce")


def norm_id(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip().str.replace(r"\.0$", "", regex=True)


def parse_hhmm(s: pd.Series):
    raw = s.astype("string").str.strip()
    miss = is_missing(raw)
    n_float = pd.to_numeric(raw.mask(miss), errors="coerce")
    integer = n_float.notna() & np.isclose(n_float.fillna(0) % 1, 0)
    n = n_float.fillna(-999999).astype("int64")
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
    add_day = pd.Series(0.0, index=s.index, dtype="float64")
    add_day.loc[is2400] = 1.0
    status = pd.Series("INVALID_HHMM", index=s.index, dtype="string")
    status.loc[miss] = "MISSING_HHMM"
    status.loc[valid] = "VALID_HHMM"
    return minute, add_day, status, is2400


def localize_candidates(local_naive: pd.Series, zones: pd.Series):
    a = pd.Series(pd.NaT, index=local_naive.index, dtype="datetime64[ns, UTC]")
    b = pd.Series(pd.NaT, index=local_naive.index, dtype="datetime64[ns, UTC]")

    for zone in zones.dropna().astype("string").unique():
        m = zones.eq(zone).fillna(False) & local_naive.notna()

        if not m.any():
            continue

        try:
            x = local_naive.loc[m].dt.tz_localize(
                str(zone), ambiguous=True, nonexistent="NaT"
            ).dt.tz_convert("UTC")

            y = local_naive.loc[m].dt.tz_localize(
                str(zone), ambiguous=False, nonexistent="NaT"
            ).dt.tz_convert("UTC")

            a.loc[m] = x
            b.loc[m] = y

        except Exception as exc:
            raise RuntimeError(
                f"Cannot localize timezone {zone}: {exc}"
            ) from exc

    return a, b


def utc_to_local(utc: pd.Series, zones: pd.Series):
    wall = pd.Series(pd.NaT, index=utc.index, dtype="datetime64[ns]")
    minute = pd.Series(np.nan, index=utc.index, dtype="float64")

    for zone in zones.dropna().astype("string").unique():
        m = zones.eq(zone).fillna(False) & utc.notna()

        if not m.any():
            continue

        loc = utc.loc[m].dt.tz_convert(str(zone))
        wall.loc[m] = loc.dt.tz_localize(None)

        minute.loc[m] = (
            loc.dt.hour * 60
            + loc.dt.minute
            + loc.dt.second / 60
        )

    return wall, minute


def local_status(
    local: pd.Series,
    zones: pd.Series,
    u0: pd.Series,
    u1: pd.Series
):
    out = pd.Series("UNCLASSIFIED", index=local.index, dtype="string")

    out.loc[local.isna()] = "MISSING_LOCAL_DATETIME"
    out.loc[local.notna() & zones.isna()] = "MISSING_TIMEZONE"

    both_missing = (
        u0.isna() & u1.isna()
        & local.notna() & zones.notna()
    )
    out.loc[both_missing] = "NONEXISTENT_LOCAL_TIME"

    both = u0.notna() & u1.notna()

    out.loc[both & u0.eq(u1)] = "UNIQUE_LOCAL_TIME"
    out.loc[both & u0.ne(u1)] = "AMBIGUOUS_LOCAL_TIME"
    out.loc[u0.notna() ^ u1.notna()] = "ONE_FOLD_ONLY"

    return out


def clock_on_date_status(
    event_wall: pd.Series,
    clock_min: pd.Series,
    zones: pd.Series
):
    candidate = pd.Series(
        pd.NaT, index=event_wall.index, dtype="datetime64[ns]"
    )

    ok = event_wall.notna() & clock_min.notna()

    candidate.loc[ok] = (
        event_wall.loc[ok].dt.normalize()
        + pd.to_timedelta(clock_min.loc[ok], unit="m")
    )

    u0, u1 = localize_candidates(candidate, zones)

    return local_status(candidate, zones, u0, u1)


def diff_clock(a: pd.Series, b: pd.Series):
    return ((a - b + 720) % 1440) - 720


def add_counts(
    counter: Counter,
    month: pd.Series,
    dim: str,
    status: pd.Series
):
    tmp = pd.DataFrame({"month": month, "status": status})

    for (m, st), n in tmp.groupby(
        ["month", "status"], dropna=False
    ).size().items():
        counter[(str(m), dim, str(st))] += int(n)


def add_tz_counts(
    counter: Counter,
    zones: pd.Series,
    dim: str,
    status: pd.Series
):
    tmp = pd.DataFrame({"tz": zones, "status": status})

    for (z, st), n in tmp.groupby(
        ["tz", "status"], dropna=False
    ).size().items():
        counter[(str(z), dim, str(st))] += int(n)


def main() -> int:

    root = Path(__file__).resolve().parents[2]

    raw_dir = root / "01_RAW_DATA" / "BTS" / "csv"
    out_dir = root / "06_DW_DESIGN" / "01_LOGICAL_MODEL"
    map_path = out_dir / "27B3B_airport_timezone_mapping_candidates.csv"

    out_dir.mkdir(parents=True, exist_ok=True)

    exists = [
        str(out_dir / x)
        for x in OUTPUTS
        if (out_dir / x).exists()
    ]

    if exists:
        print(
            "STOP: C3 output exists; archive/rename before rerunning:\n"
            + "\n".join(exists)
        )
        return 2

    if not raw_dir.exists() or not map_path.exists():
        print(
            f"STOP: Missing input folder or timezone mapping:\n"
            f"{raw_dir}\n{map_path}"
        )
        return 2

    files = sorted(raw_dir.glob("BTS_2025_*.csv"))

    expected_names = [
        f"BTS_2025_{m:02d}.csv"
        for m in range(1, 13)
    ]

    if [p.name for p in files] != expected_names:
        print(
            "STOP: Expected exactly these 12 monthly files; "
            f"found {[p.name for p in files]}"
        )
        return 2

    logger = logging.getLogger("step27b3c3")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    handlers = [
        logging.FileHandler(out_dir / OUTPUTS[5], encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]

    for handler in handlers:
        handler.setFormatter(fmt)
        logger.addHandler(handler)

    mp = pd.read_csv(
        map_path,
        dtype="string",
        keep_default_na=False
    )

    required = {"BTSAirportID", "TimeZoneIANA"}

    if not required.issubset(mp.columns):
        logger.error(
            "Timezone mapping lacks columns: %s",
            required - set(mp.columns)
        )
        return 2

    mp["BTSAirportID"] = norm_id(mp["BTSAirportID"])
    mp["TimeZoneIANA"] = (
        mp["TimeZoneIANA"].astype("string").str.strip()
    )

    if (
        mp["BTSAirportID"].duplicated().any()
        or mp["TimeZoneIANA"].eq("").any()
    ):
        logger.error(
            "Duplicate airport IDs or blank timezone in mapping"
        )
        return 2

    tz_map = dict(
        zip(
            mp["BTSAirportID"].astype(str),
            mp["TimeZoneIANA"].astype(str)
        )
    )

    logger.info(
        "STEP 27B-3C3 UTC-BASED AUDIT; files=%d; timezones=%d; chunk=%d",
        len(files), len(mp), CHUNK_SIZE
    )

    logger.info(
        "Reads RAW CSV only; no PyArrow; no RAW/STAGING/CLEAN changes."
    )

    month_counts = Counter()
    tz_counts = Counter()
    field_counts = Counter()

    samples = []
    sample_counts = Counter()

    total = 0

    def collect(
        base: pd.DataFrame,
        issue: str,
        mask: pd.Series
    ):
        if not mask.any():
            return

        for mon, group in base.loc[mask].groupby(
            "SourceMonth", dropna=False
        ):
            key = (issue, str(mon))
            remain = SAMPLES_PER_ISSUE_MONTH - sample_counts[key]

            if remain > 0:
                part = group.head(remain).copy()
                part.insert(0, "AuditIssue", issue)
                samples.append(part)
                sample_counts[key] += len(part)

    for fi, path in enumerate(files, 1):

        month_num = path.stem[-2:]

        logger.info(
            "Reading %s (%d/12)",
            path.name, fi
        )

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

            df = df.reset_index(drop=True)

            if df.empty:
                continue

            n = len(df)
            total += n
            file_rows += n

            month = pd.Series(
                month_num,
                index=df.index,
                dtype="string"
            )

            flight_date = pd.to_datetime(
                df["FlightDate"].str.strip(),
                errors="coerce"
            )

            parsed = {
                c: parse_hhmm(df[c])
                for c in [
                    "CRSDepTime", "DepTime",
                    "CRSArrTime", "ArrTime"
                ]
            }

            for field, (_, _, st, is2400) in parsed.items():

                field_counts[(field, "Rows")] += n

                field_counts[(field, "ValidHHMM")] += int(
                    st.eq("VALID_HHMM").sum()
                )

                field_counts[(field, "MissingHHMM")] += int(
                    st.eq("MISSING_HHMM").sum()
                )

                field_counts[(field, "InvalidHHMM")] += int(
                    st.eq("INVALID_HHMM").sum()
                )

                field_counts[(field, "Special2400")] += int(
                    is2400.sum()
                )

            cdep, cdep_add, cdep_st, _ = parsed["CRSDepTime"]
            dep_clock, _, dep_st, _ = parsed["DepTime"]
            carr, carr_add, carr_st, _ = parsed["CRSArrTime"]
            arr_clock, _, arr_st, _ = parsed["ArrTime"]

            origin_tz = (
                norm_id(df["OriginAirportID"])
                .map(tz_map)
                .astype("string")
            )

            dest_tz = (
                norm_id(df["DestAirportID"])
                .map(tz_map)
                .astype("string")
            )

            crs_elapsed = num(df["CRSElapsedTime"])
            actual_elapsed = num(df["ActualElapsedTime"])
            dep_delay = num(df["DepDelay"])
            arr_delay = num(df["ArrDelay"])

            cancelled = num(df["Cancelled"]).fillna(-1)
            diverted = num(df["Diverted"]).fillna(-1)

            normal = cancelled.eq(0) & diverted.eq(0)

            # BTS reporting instructions require destination-arrival
            # fields for diverted flights only when they reach the
            # originally scheduled destination.
            arr_fields_present = (
                ~is_missing(df["ArrTime"])
                & ~is_missing(df["ArrDelay"])
                & ~is_missing(df["ActualElapsedTime"])
            )

            arr_evaluable = (
                normal
                | (
                    cancelled.eq(0)
                    & diverted.eq(1)
                    & arr_fields_present
                )
            )

            diverted_not_to_scheduled_dest = (
                cancelled.ne(1)
                & diverted.eq(1)
                & ~arr_fields_present
            )

            # ---------------------------------------------------------
            # 1. SCHEDULED DEPARTURE
            # ---------------------------------------------------------

            sched_dep_local = (
                flight_date
                + pd.to_timedelta(
                    cdep + cdep_add.fillna(0) * 1440,
                    unit="m"
                )
            )

            s0, s1 = localize_candidates(
                sched_dep_local,
                origin_tz
            )

            s_local_status = local_status(
                sched_dep_local,
                origin_tz,
                s0,
                s1
            )

            crs_valid = crs_elapsed.where(crs_elapsed.ge(0))

            sa0 = s0 + pd.to_timedelta(crs_valid, unit="m")
            sa1 = s1 + pd.to_timedelta(crs_valid, unit="m")

            _, sa0_min = utc_to_local(sa0, dest_tz)
            _, sa1_min = utc_to_local(sa1, dest_tz)

            m0 = diff_clock(sa0_min, carr).abs().le(TOLERANCE_MIN)
            m1 = diff_clock(sa1_min, carr).abs().le(TOLERANCE_MIN)

            sched_amb = s_local_status.eq("AMBIGUOUS_LOCAL_TIME")

            choose0 = sched_amb & m0 & ~m1
            choose1 = sched_amb & m1 & ~m0

            unresolved_s = sched_amb & ~(choose0 | choose1)

            sched_utc = s0.copy()
            sched_utc.loc[choose1] = s1.loc[choose1]
            sched_utc.loc[unresolved_s] = pd.NaT

            sched_dep_status = s_local_status.copy()

            sched_dep_status.loc[
                choose0 | choose1
            ] = "AMBIGUOUS_RESOLVED_BY_CRS_ARRIVAL_UTC"

            sched_dep_status.loc[
                unresolved_s
            ] = "AMBIGUOUS_UNRESOLVED"

            # Scheduled arrival UTC = scheduled departure UTC
            # plus CRSElapsedTime.
            sched_arr_utc = (
                sched_utc
                + pd.to_timedelta(crs_valid, unit="m")
            )

            sched_arr_wall, sched_arr_min = utc_to_local(
                sched_arr_utc,
                dest_tz
            )

            sched_arr_resid = diff_clock(
                sched_arr_min,
                carr
            )

            sched_arr_status = pd.Series(
                "MATCH_WITHIN_1_MINUTE",
                index=df.index,
                dtype="string"
            )

            sched_arr_status.loc[
                sched_utc.isna()
            ] = "SCHEDULED_DEP_UTC_UNAVAILABLE"

            sched_arr_status.loc[
                crs_elapsed.isna()
            ] = "CRSElapsedTime_MISSING"

            sched_arr_status.loc[
                crs_elapsed.notna() & crs_elapsed.lt(0)
            ] = "CRSElapsedTime_INVALID_NEGATIVE"

            sched_arr_status.loc[
                carr_st.eq("MISSING_HHMM")
            ] = "CRSARRTIME_MISSING"

            sched_arr_status.loc[
                carr_st.eq("INVALID_HHMM")
            ] = "CRSARRTIME_INVALID"

            sched_arr_status.loc[
                dest_tz.isna()
            ] = "MISSING_DESTINATION_TIMEZONE"

            sc = (
                sched_utc.notna()
                & crs_valid.notna()
                & sched_arr_min.notna()
                & carr.notna()
                & dest_tz.notna()
            )

            sched_arr_status.loc[
                sc & sched_arr_resid.abs().gt(TOLERANCE_MIN)
            ] = "CRS_ELAPSED_VS_CRSARR_CLOCK_MISMATCH"

            sched_arr_clock_dst = clock_on_date_status(
                sched_arr_wall,
                carr,
                dest_tz
            )

            sched_arr_clock_dst.loc[
                sc & sched_arr_resid.abs().gt(TOLERANCE_MIN)
            ] = "CRSARR_CLOCK_MISMATCH"

            sched_arr_clock_dst.loc[
                sc
                & sched_arr_resid.abs().le(TOLERANCE_MIN)
                & sched_arr_clock_dst.eq("AMBIGUOUS_LOCAL_TIME")
            ] = "AMBIGUOUS_OCCURRENCE_IDENTIFIED_BY_CRSElapsedTime"

            # ---------------------------------------------------------
            # 2. ACTUAL DEPARTURE
            # ---------------------------------------------------------

            # Actual departure UTC candidate =
            # scheduled departure UTC + DepDelay.
            #
            # Convert this instant back to origin-local time and
            # validate against DepTime. Do not add DepDelay to a
            # naive local wall clock across DST transitions.

            actual_dep_utc = (
                sched_utc
                + pd.to_timedelta(dep_delay, unit="m")
            )

            actual_dep_wall, actual_dep_min = utc_to_local(
                actual_dep_utc,
                origin_tz
            )

            dep_clock_resid = diff_clock(
                actual_dep_min,
                dep_clock
            )

            operated = cancelled.eq(0)

            actual_dep_status = pd.Series(
                "UTC_DELAY_AND_LOCAL_CLOCK_MATCH",
                index=df.index,
                dtype="string"
            )

            actual_dep_status.loc[
                cancelled.eq(1)
            ] = "CANCELLED_SEPARATE_HANDLING"

            actual_dep_status.loc[
                operated & sched_utc.isna()
            ] = "SCHEDULED_DEP_UTC_UNAVAILABLE"

            actual_dep_status.loc[
                operated & dep_delay.isna()
            ] = "DEPDELAY_MISSING"

            actual_dep_status.loc[
                operated & dep_st.eq("MISSING_HHMM")
            ] = "DEPTIME_MISSING_CLOCK_VALIDATION_UNAVAILABLE"

            actual_dep_status.loc[
                operated & dep_st.eq("INVALID_HHMM")
            ] = "DEPTIME_INVALID"

            actual_dep_status.loc[
                operated & origin_tz.isna()
            ] = "MISSING_ORIGIN_TIMEZONE"

            dep_check = (
                operated
                & actual_dep_utc.notna()
                & dep_clock.notna()
                & actual_dep_min.notna()
            )

            actual_dep_status.loc[
                dep_check
                & dep_clock_resid.abs().gt(TOLERANCE_MIN)
            ] = "UTC_DEPDELAY_VS_DEPTIME_MISMATCH"

            actual_dep_reported_dst = clock_on_date_status(
                actual_dep_wall,
                dep_clock,
                origin_tz
            )

            actual_dep_reported_dst.loc[
                dep_check
                & dep_clock_resid.abs().le(TOLERANCE_MIN)
                & actual_dep_reported_dst.eq("AMBIGUOUS_LOCAL_TIME")
            ] = "AMBIGUOUS_DEPTIME_OCCURRENCE_IDENTIFIED_BY_UTC_DELAY"

            actual_dep_reported_dst.loc[
                dep_check
                & dep_clock_resid.abs().gt(TOLERANCE_MIN)
                & actual_dep_reported_dst.eq("NONEXISTENT_LOCAL_TIME")
            ] = "REPORTED_DEPTIME_IN_DST_GAP"

            # ---------------------------------------------------------
            # 3. ACTUAL ARRIVAL
            # ---------------------------------------------------------

            # Candidate A: actual departure UTC + ActualElapsedTime.
            actual_elapsed_valid = actual_elapsed.where(
                actual_elapsed.ge(0)
            )

            arr_utc_elapsed = (
                actual_dep_utc
                + pd.to_timedelta(actual_elapsed_valid, unit="m")
            )

            arr_wall_elapsed, arr_min_elapsed = utc_to_local(
                arr_utc_elapsed,
                dest_tz
            )

            arr_clock_resid = diff_clock(
                arr_min_elapsed,
                arr_clock
            )

            # Candidate B: scheduled arrival UTC + ArrDelay.
            arr_utc_delay = (
                sched_arr_utc
                + pd.to_timedelta(arr_delay, unit="m")
            )

            # Do not trust ArrDelay as an independent UTC anchor when
            # the scheduled-arrival baseline failed its CRS clock check.
            arr_utc_delay = arr_utc_delay.where(
                sched_arr_status.eq("MATCH_WITHIN_1_MINUTE")
            )

            both_arr = (
                arr_evaluable
                & arr_utc_elapsed.notna()
                & arr_utc_delay.notna()
            )

            arr_candidate_delta = (
                arr_utc_elapsed - arr_utc_delay
            ).dt.total_seconds() / 60.0

            actual_arr_status = pd.Series(
                "NOT_EVALUATED",
                index=df.index,
                dtype="string"
            )

            actual_arr_status.loc[
                cancelled.eq(1)
            ] = "CANCELLED_SEPARATE_HANDLING"

            actual_arr_status.loc[
                diverted_not_to_scheduled_dest
            ] = "DIVERTED_NOT_REPORTED_AS_REACHING_SCHEDULED_DESTINATION"

            actual_arr_status.loc[
                arr_evaluable & actual_dep_utc.isna()
            ] = "ACTUAL_DEP_UTC_UNAVAILABLE"

            actual_arr_status.loc[
                arr_evaluable & actual_elapsed.isna()
            ] = "ACTUALELAPSEDTIME_MISSING"

            actual_arr_status.loc[
                arr_evaluable
                & actual_elapsed.notna()
                & actual_elapsed.lt(0)
            ] = "ACTUALELAPSEDTIME_INVALID_NEGATIVE"

            actual_arr_status.loc[
                arr_evaluable & arr_st.eq("MISSING_HHMM")
            ] = "ARRTIME_MISSING"

            actual_arr_status.loc[
                arr_evaluable & arr_st.eq("INVALID_HHMM")
            ] = "ARRTIME_INVALID"

            actual_arr_status.loc[
                arr_evaluable & dest_tz.isna()
            ] = "MISSING_DESTINATION_TIMEZONE"

            arr_check = (
                arr_evaluable
                & arr_utc_elapsed.notna()
                & arr_clock.notna()
                & arr_min_elapsed.notna()
                & dest_tz.notna()
            )

            actual_arr_status.loc[
                arr_check
                & arr_clock_resid.abs().gt(TOLERANCE_MIN)
            ] = "UTC_ELAPSED_VS_ARRTIME_CLOCK_MISMATCH"

            actual_arr_status.loc[
                arr_check
                & arr_clock_resid.abs().le(TOLERANCE_MIN)
            ] = "ARRIVAL_CLOCK_MATCH_UTC_ELAPSED"

            actual_arr_status.loc[
                both_arr
                & arr_candidate_delta.abs().gt(TOLERANCE_MIN)
            ] = "ARRDELAY_VS_ACTUALELAPSED_UTC_MISMATCH"

            actual_arr_status.loc[
                both_arr
                & arr_candidate_delta.abs().le(TOLERANCE_MIN)
                & arr_check
                & arr_clock_resid.abs().le(TOLERANCE_MIN)
            ] = "ARRIVAL_CLOCK_AND_BOTH_UTC_METHODS_MATCH"

            actual_arr_status.loc[
                arr_evaluable
                & arr_utc_elapsed.isna()
                & arr_utc_delay.notna()
            ] = "ARRIVAL_UTC_FROM_ARRDELAY_ONLY"

            actual_arr_status.loc[
                arr_evaluable
                & arr_utc_elapsed.notna()
                & arr_utc_delay.isna()
                & arr_check
                & arr_clock_resid.abs().le(TOLERANCE_MIN)
            ] = "ARRIVAL_UTC_FROM_ACTUALELAPSED_ONLY"

            actual_arr_clock_dst = clock_on_date_status(
                arr_wall_elapsed,
                arr_clock,
                dest_tz
            )

            actual_arr_clock_dst.loc[
                arr_check
                & arr_clock_resid.abs().gt(TOLERANCE_MIN)
            ] = "REPORTED_ARRTIME_CLOCK_MISMATCH"

            actual_arr_clock_dst.loc[
                arr_check
                & arr_clock_resid.abs().le(TOLERANCE_MIN)
                & actual_arr_clock_dst.eq("AMBIGUOUS_LOCAL_TIME")
            ] = "AMBIGUOUS_ARRTIME_OCCURRENCE_IDENTIFIED_BY_UTC"

            # For diverted flights, BTS Dest is the scheduled destination.
            # Do not assume arrival at Dest unless arrival fields are present.
            actual_arr_clock_dst.loc[
                cancelled.eq(1)
            ] = "CANCELLED_SEPARATE_HANDLING"

            actual_arr_clock_dst.loc[
                diverted_not_to_scheduled_dest
            ] = "DIVERTED_ACTUAL_ARRIVAL_AIRPORT_NOT_ASSUMED_DEST"

            # ---------------------------------------------------------
            # 4. AGGREGATION AND SAMPLES
            # ---------------------------------------------------------

            dimensions = [
                ("SCHEDULED_DEPARTURE_ORIGIN", sched_dep_status),
                ("SCHEDULED_ARRIVAL_DESTINATION", sched_arr_status),
                ("SCHEDULED_ARRIVAL_DST_CLOCK", sched_arr_clock_dst),
                ("ACTUAL_DEPARTURE_ORIGIN", actual_dep_status),
                ("ACTUAL_DEPARTURE_DST_CLOCK", actual_dep_reported_dst),
                ("ACTUAL_ARRIVAL_DESTINATION", actual_arr_status),
                ("ACTUAL_ARRIVAL_DST_CLOCK", actual_arr_clock_dst),
            ]

            for dim, status in dimensions:
                add_counts(month_counts, month, dim, status)

            for dim, zone, status in [
                ("SCHEDULED_DEPARTURE_ORIGIN", origin_tz, sched_dep_status),
                ("SCHEDULED_ARRIVAL_DESTINATION", dest_tz, sched_arr_clock_dst),
                ("ACTUAL_DEPARTURE_ORIGIN", origin_tz, actual_dep_reported_dst),
                ("ACTUAL_ARRIVAL_DESTINATION", dest_tz, actual_arr_clock_dst),
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
                "ScheduledDepUTC": sched_utc.astype("string"),
                "ScheduledDepStatus": sched_dep_status,
                "ScheduledArrUTC": sched_arr_utc.astype("string"),
                "ScheduledArrStatus": sched_arr_status,
                "ScheduledArrClockResidualMin": sched_arr_resid,
                "ActualDepUTC_From_ScheduledUTC_Plus_DepDelay": actual_dep_utc.astype("string"),
                "ActualDepStatus": actual_dep_status,
                "ActualDepClockResidualMin": dep_clock_resid,
                "ActualDepReportedDSTStatus": actual_dep_reported_dst,
                "ActualArrUTC_From_Elapsed": arr_utc_elapsed.astype("string"),
                "ActualArrUTC_From_ArrDelay": arr_utc_delay.astype("string"),
                "ActualArrCandidateDeltaMin": arr_candidate_delta,
                "ActualArrStatus": actual_arr_status,
                "ActualArrClockResidualMin": arr_clock_resid,
                "ActualArrReportedDSTStatus": actual_arr_clock_dst,
            })

            collect(
                base,
                "SCHEDULED_ARRIVAL_CLOCK_MISMATCH",
                sched_arr_status.eq("CRS_ELAPSED_VS_CRSARR_CLOCK_MISMATCH")
            )

            collect(
                base,
                "ACTUAL_DEPARTURE_UTC_CLOCK_MISMATCH",
                actual_dep_status.eq("UTC_DEPDELAY_VS_DEPTIME_MISMATCH")
            )

            collect(
                base,
                "ACTUAL_DEPARTURE_DST_GAP",
                actual_dep_reported_dst.eq("REPORTED_DEPTIME_IN_DST_GAP")
            )

            collect(
                base,
                "ACTUAL_ARRIVAL_UTC_CLOCK_MISMATCH",
                actual_arr_status.eq("UTC_ELAPSED_VS_ARRTIME_CLOCK_MISMATCH")
            )

            collect(
                base,
                "ACTUAL_ARRIVAL_UTC_METHOD_MISMATCH",
                actual_arr_status.eq("ARRDELAY_VS_ACTUALELAPSED_UTC_MISMATCH")
            )

            collect(
                base,
                "ACTUAL_ARRIVAL_DST_AMBIGUOUS",
                actual_arr_clock_dst.eq(
                    "AMBIGUOUS_ARRTIME_OCCURRENCE_IDENTIFIED_BY_UTC"
                )
            )

        logger.info(
            "Completed %s; rows=%s",
            path.name,
            f"{file_rows:,}"
        )

    # -------------------------------------------------------------
    # 5. WRITE REPORTS
    # -------------------------------------------------------------

    month_df = pd.DataFrame([
        {
            "SourceMonth": m,
            "AuditDimension": d,
            "AuditStatus": s,
            "Rows": v
        }
        for (m, d, s), v in month_counts.items()
    ]).sort_values(
        ["SourceMonth", "AuditDimension", "AuditStatus"]
    )

    tz_df = pd.DataFrame([
        {
            "IANA_Timezone": z,
            "AuditDimension": d,
            "AuditStatus": s,
            "Rows": v
        }
        for (z, d, s), v in tz_counts.items()
    ]).sort_values(
        ["IANA_Timezone", "AuditDimension", "AuditStatus"]
    )

    field_df = pd.DataFrame([
        {
            "FieldName": f,
            "Metric": m,
            "Rows": int(v)
        }
        for (f, m), v in field_counts.items()
    ]).sort_values(
        ["FieldName", "Metric"]
    )

    sample_df = (
        pd.concat(samples, ignore_index=True)
        if samples else pd.DataFrame()
    )

    month_df.to_csv(
        out_dir / OUTPUTS[0],
        index=False,
        encoding="utf-8-sig"
    )

    tz_df.to_csv(
        out_dir / OUTPUTS[1],
        index=False,
        encoding="utf-8-sig"
    )

    field_df.to_csv(
        out_dir / OUTPUTS[2],
        index=False,
        encoding="utf-8-sig"
    )

    sample_df.to_csv(
        out_dir / OUTPUTS[3],
        index=False,
        encoding="utf-8-sig"
    )

    lines = [
        "STEP 27B-3C3 - UTC-BASED EVENT TIMESTAMP AUDIT SUMMARY",
        "=" * 76,
        f"BTS files scanned: {len(files)}",
        f"BTS records scanned: {total:,}",
        f"Expected records: {EXPECTED_ROWS:,}",
        f"Timezone mapping rows: {len(mp):,}",
        "",
        "METHOD:",
        "- Scheduled departure UTC is localized from FlightDate + CRSDepTime at origin.",
        "- Scheduled arrival UTC = scheduled departure UTC + CRSElapsedTime.",
        "- Actual departure UTC candidate = scheduled departure UTC + DepDelay; convert it to origin-local time and validate against DepTime (candidate, not automatic repair).",
        "- Actual arrival UTC candidate A = actual departure UTC + ActualElapsedTime.",
        "- Actual arrival UTC candidate B = scheduled arrival UTC + ArrDelay.",
        "- UTC event candidates are converted to airport-local time and checked against reported HHMM.",
        "- Actual arrival is cross-checked using actual departure UTC + ActualElapsedTime versus scheduled arrival UTC + ArrDelay.",
        "- Cancelled and diverted flights are retained and separately classified for actual arrival.",
        "- This is an audit, not permission to overwrite source values or automatically repair timestamps.",
        "- RAW is read-only; no STAGING/CLEAN mutation; no PyArrow dependency.",
        "",
        "TOTALS BY DIMENSION AND STATUS:",
    ]

    totals_df = month_df.groupby(
        ["AuditDimension", "AuditStatus"],
        as_index=False
    )["Rows"].sum()

    for dimension in totals_df["AuditDimension"].drop_duplicates():
        lines.append(f"\n[{dimension}]")

        for row in totals_df[
            totals_df.AuditDimension == dimension
        ].itertuples(index=False):
            lines.append(
                f"{row.AuditStatus}: {int(row.Rows):,}"
            )

    lines += ["", "OUTPUT FILES:"] + [
        str(out_dir / x)
        for x in OUTPUTS
        if not x.endswith(".log")
    ]

    (out_dir / OUTPUTS[4]).write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8"
    )

    logger.info(
        "Completed all rows=%s (expected=%s)",
        f"{total:,}",
        f"{EXPECTED_ROWS:,}"
    )

    logger.info(
        "Summary: %s",
        out_dir / OUTPUTS[4]
    )

    logger.info(
        "Review C3 mismatches before any CLEAN timestamp decision."
    )

    return 0 if total == EXPECTED_ROWS else 1


if __name__ == "__main__":
    raise SystemExit(main())
