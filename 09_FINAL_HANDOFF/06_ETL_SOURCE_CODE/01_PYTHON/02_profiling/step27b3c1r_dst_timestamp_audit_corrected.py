from __future__ import annotations

import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# =============================================================================
# STEP 27B-3C1R - CORRECTED LOCAL-TIME / DST AUDIT (READ-ONLY)
# =============================================================================
# BTS HHMM fields are LOCAL wall-clock time.
# DepDelay is checked against local departure-clock difference (same origin TZ).
# ArrDelay is checked against local arrival-clock difference (same destination TZ).
# CRSElapsedTime and ActualElapsedTime are elapsed minutes adjusted for
# origin/destination time-zone differences, and are therefore used as elapsed
# UTC durations when validating scheduled/actual arrival.
#
# The script does NOT write normalized timestamps to CLEAN or modify RAW/STAGING.
# It never chooses an occurrence of an ambiguous local time from DepDelay alone.
# An ambiguous actual departure may be resolved only if exactly one UTC fold
# makes ActualElapsedTime agree with both ArrTime and (when usable) ArrDelay.
# =============================================================================

EXPECTED_ROWS = 7_001_619
BATCH_SIZE = 100_000
TOLERANCE_MINUTES = 1.0
SAMPLE_PER_ISSUE_MONTH = 8

INPUT_COLUMNS = [
    "FlightDate", "Reporting_Airline", "Flight_Number_Reporting_Airline",
    "Origin", "Dest", "OriginAirportID", "DestAirportID",
    "CRSDepTime", "DepTime", "DepDelay", "CRSArrTime", "ArrTime",
    "ArrDelay", "CRSElapsedTime", "ActualElapsedTime", "Cancelled",
    "Diverted", "SourceFile", "SourceMonth",
]

OUTPUTS = [
    "27B3C1R_time_field_profile.csv",
    "27B3C1R_status_by_month.csv",
    "27B3C1R_timezone_dst_profile.csv",
    "27B3C1R_timestamp_audit_samples.csv",
    "27B3C1R_timestamp_audit_summary.txt",
    "27B3C1R_timestamp_audit.log",
]


def null_mask(s: pd.Series) -> pd.Series:
    x = s.astype("string").str.strip()
    return x.isna() | x.str.upper().isin(["", "NA", "N/A", "NULL", "NONE"])


def numeric(s: pd.Series) -> pd.Series:
    x = s.astype("string").str.strip().mask(null_mask(s))
    return pd.to_numeric(x, errors="coerce")


def normalize_id(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip().str.replace(r"\.0$", "", regex=True)


def parse_hhmm(s: pd.Series):
    raw = s.astype("string").str.strip()
    missing = null_mask(raw)
    num = pd.to_numeric(raw.mask(missing), errors="coerce")
    integer = num.notna() & np.isclose(num.fillna(0) % 1, 0)
    n = num.fillna(-999999).astype("int64")

    is2400 = integer & n.eq(2400)

    valid_regular = (
        integer & n.ge(0) & n.le(2359)
        & n.mod(100).lt(60) & n.floordiv(100).le(23)
    )

    valid = is2400 | valid_regular

    mins = pd.Series(np.nan, index=s.index, dtype="float64")

    mins.loc[valid_regular] = (
        (n.loc[valid_regular] // 100) * 60
        + n.loc[valid_regular] % 100
    ).astype(float)

    mins.loc[is2400] = 0.0

    day_add = pd.Series(0.0, index=s.index, dtype="float64")
    day_add.loc[is2400] = 1.0

    status = pd.Series("INVALID_HHMM", index=s.index, dtype="string")
    status.loc[missing] = "MISSING_HHMM"
    status.loc[valid] = "VALID_HHMM"

    return mins, day_add, status, is2400


def build_local_naive(date_s, minute_s, day_add=None):
    if day_add is None:
        day_add = pd.Series(0.0, index=date_s.index)

    total = minute_s + day_add.fillna(0) * 1440
    out = date_s + pd.to_timedelta(total, unit="m")

    return out.where(date_s.notna() & minute_s.notna())


def localize_folds(naive: pd.Series, tz: pd.Series):
    u0 = pd.Series(
        pd.NaT,
        index=naive.index,
        dtype="datetime64[ns, UTC]",
    )

    u1 = pd.Series(
        pd.NaT,
        index=naive.index,
        dtype="datetime64[ns, UTC]",
    )

    zones = tz.dropna().astype("string").unique().tolist()

    for zone in zones:
        mask = tz.eq(zone).fillna(False) & naive.notna()

        if not mask.any():
            continue

        try:
            a = naive.loc[mask].dt.tz_localize(
                str(zone),
                ambiguous=True,
                nonexistent="NaT",
            )

            b = naive.loc[mask].dt.tz_localize(
                str(zone),
                ambiguous=False,
                nonexistent="NaT",
            )

            u0.loc[mask] = a.dt.tz_convert("UTC")
            u1.loc[mask] = b.dt.tz_convert("UTC")

        except Exception:
            pass

    return u0, u1


def utc_to_local_naive(utc_s: pd.Series, tz: pd.Series):
    out = pd.Series(
        pd.NaT,
        index=utc_s.index,
        dtype="datetime64[ns]",
    )

    zones = tz.dropna().astype("string").unique().tolist()

    for zone in zones:
        mask = tz.eq(zone).fillna(False) & utc_s.notna()

        if not mask.any():
            continue

        try:
            out.loc[mask] = (
                utc_s.loc[mask]
                .dt.tz_convert(str(zone))
                .dt.tz_localize(None)
            )

        except Exception:
            pass

    return out


def classify_local(naive, tz, u0, u1):
    status = pd.Series(
        "UNCLASSIFIED",
        index=naive.index,
        dtype="string",
    )

    status.loc[naive.isna()] = "MISSING_LOCAL_DATETIME"

    status.loc[
        naive.notna() & tz.isna()
    ] = "MISSING_TIMEZONE"

    both_na = (
        naive.notna()
        & tz.notna()
        & u0.isna()
        & u1.isna()
    )

    status.loc[both_na] = "NONEXISTENT_LOCAL_TIME"

    both = u0.notna() & u1.notna()

    status.loc[
        both & u0.eq(u1)
    ] = "UNIQUE_LOCAL_TIME"

    status.loc[
        both & u0.ne(u1)
    ] = "AMBIGUOUS_LOCAL_TIME"

    status.loc[
        u0.notna() ^ u1.notna()
    ] = "ONE_FOLD_ONLY"

    return status


def minute_of_day(local_naive: pd.Series):
    return (
        local_naive.dt.hour.astype("float64") * 60
        + local_naive.dt.minute.astype("float64")
    )


def circular_diff(a, b):
    raw = a - b
    return ((raw + 720) % 1440) - 720


def main():
    root = Path(__file__).resolve().parents[2]

    staging = root / "03_STAGING_DATA" / "BTS"

    model_dir = (
        root
        / "06_DW_DESIGN"
        / "01_LOGICAL_MODEL"
    )

    mapping_path = (
        model_dir
        / "27B3B_airport_timezone_mapping_candidates.csv"
    )

    model_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Không ghi đè kết quả của lần chạy trước.
    existing = [
        str(model_dir / f)
        for f in OUTPUTS
        if (model_dir / f).exists()
    ]

    if existing:
        print(
            "STOP - các output C1R đã tồn tại. "
            "Hãy archive/đổi tên trước khi chạy lại:"
        )

        print("\n".join(existing))
        return 2

    log_path = model_dir / "27B3C1R_timestamp_audit.log"

    logger = logging.getLogger("step27b3c1r")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    fh = logging.FileHandler(
        log_path,
        encoding="utf-8",
    )

    fh.setFormatter(fmt)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)

    if not staging.exists() or not mapping_path.exists():
        logger.error("Thiếu BTS staging hoặc timezone mapping.")
        return 2

    files = sorted(
        staging.glob("stg_bts_2025_*.parquet")
    )

    if len(files) != 12:
        logger.error(
            "Cần đúng 12 file BTS Parquet; tìm thấy %d",
            len(files),
        )
        return 2

    mp = pd.read_csv(
        mapping_path,
        dtype="string",
        keep_default_na=False,
    )

    needed = {
        "BTSAirportID",
        "TimeZoneIANA",
    }

    if not needed.issubset(mp.columns):
        logger.error(
            "Mapping thiếu cột %s",
            sorted(needed - set(mp.columns)),
        )
        return 2

    mp["BTSAirportID"] = normalize_id(
        mp["BTSAirportID"]
    )

    mp["TimeZoneIANA"] = (
        mp["TimeZoneIANA"]
        .astype("string")
        .str.strip()
    )

    if mp["BTSAirportID"].duplicated().any():
        logger.error(
            "BTSAirportID bị trùng trong mapping."
        )
        return 2

    tz_map: Dict[str, str] = dict(
        zip(
            mp["BTSAirportID"].astype(str),
            mp["TimeZoneIANA"].astype(str),
        )
    )

    logger.info(
        "STEP 27B-3C1R - CORRECTED LOCAL-TIME / DST AUDIT"
    )

    logger.info(
        "BTS files=%d; timezone rows=%d; batch=%d",
        len(files),
        len(mp),
        BATCH_SIZE,
    )

    logger.info(
        "No RAW/STAGING mutation; no CLEAN timestamp promotion."
    )

    month_counts = Counter()
    tz_counts = Counter()
    profile_counts = Counter()
    sample_counts = Counter()
    samples = []

    total_rows = 0

    # -------------------------------------------------------------------------
    # Lấy mẫu phân tầng theo tháng và loại vấn đề.
    # Tối đa 8 dòng cho mỗi issue trong mỗi tháng.
    # -------------------------------------------------------------------------

    def capture(df, issue, mask, cols):
        if not mask.any():
            return

        for month, group in df.loc[mask].groupby(
            "SourceMonth",
            dropna=False,
        ):

            key = (issue, str(month))

            remaining = (
                SAMPLE_PER_ISSUE_MONTH
                - sample_counts[key]
            )

            if remaining <= 0:
                continue

            part = group.head(remaining)[cols].copy()

            part.insert(
                0,
                "AuditIssue",
                issue,
            )

            samples.append(part)
            sample_counts[key] += len(part)

    # =========================================================================
    # QUÉT 12 FILE BTS
    # =========================================================================

    for idx, path in enumerate(files, 1):

        logger.info(
            "Scanning %s (%d/12)",
            path.name,
            idx,
        )

        pf = pq.ParquetFile(path)
        file_rows = 0

        for batch in pf.iter_batches(
            columns=INPUT_COLUMNS,
            batch_size=BATCH_SIZE,
        ):

            df = batch.to_pandas()

            if df.empty:
                continue

            n = len(df)

            total_rows += n
            file_rows += n

            month = (
                df["SourceMonth"]
                .astype("string")
                .str.strip()
            )

            # -----------------------------------------------------------------
            # A. Parse ngày và giờ HHMM
            # -----------------------------------------------------------------

            fdate = pd.to_datetime(
                df["FlightDate"].astype("string").str.strip(),
                errors="coerce",
            )

            parsed = {
                c: parse_hhmm(df[c])
                for c in [
                    "CRSDepTime",
                    "DepTime",
                    "CRSArrTime",
                    "ArrTime",
                ]
            }

            for field, (_, _, st, special) in parsed.items():

                profile_counts[(field, "Rows")] += n

                profile_counts[
                    (field, "ValidHHMM")
                ] += int(st.eq("VALID_HHMM").sum())

                profile_counts[
                    (field, "MissingHHMM")
                ] += int(st.eq("MISSING_HHMM").sum())

                profile_counts[
                    (field, "InvalidHHMM")
                ] += int(st.eq("INVALID_HHMM").sum())

                profile_counts[
                    (field, "Special2400")
                ] += int(special.sum())

            profile_counts[
                ("FlightDate", "Rows")
            ] += n

            profile_counts[
                ("FlightDate", "ValidDate")
            ] += int(fdate.notna().sum())

            profile_counts[
                ("FlightDate", "MissingOrInvalidDate")
            ] += int(fdate.isna().sum())

            (
                cdep_min,
                cdep_add,
                cdep_st,
                _,
            ) = parsed["CRSDepTime"]

            (
                dep_min,
                dep_add,
                dep_st,
                _,
            ) = parsed["DepTime"]

            (
                carr_min,
                carr_add,
                carr_st,
                _,
            ) = parsed["CRSArrTime"]

            (
                arr_min,
                arr_add,
                arr_st,
                _,
            ) = parsed["ArrTime"]

            # -----------------------------------------------------------------
            # B. Numeric và timezone
            # -----------------------------------------------------------------

            crs_elapsed = numeric(df["CRSElapsedTime"])
            actual_elapsed = numeric(df["ActualElapsedTime"])

            dep_delay = numeric(df["DepDelay"])
            arr_delay = numeric(df["ArrDelay"])

            cancelled = numeric(
                df["Cancelled"]
            ).fillna(-1)

            diverted = numeric(
                df["Diverted"]
            ).fillna(-1)

            normal_flight = (
                cancelled.eq(0)
                & diverted.eq(0)
            )

            origin_id = normalize_id(
                df["OriginAirportID"]
            )

            dest_id = normalize_id(
                df["DestAirportID"]
            )

            origin_tz = (
                origin_id.map(tz_map)
                .astype("string")
            )

            dest_tz = (
                dest_id.map(tz_map)
                .astype("string")
            )

            # =================================================================
            # C. SCHEDULED DEPARTURE - GIỜ ĐỊA PHƯƠNG SÂN BAY ĐI
            # =================================================================

            sched_dep_local = build_local_naive(
                fdate,
                cdep_min,
                cdep_add,
            )

            sched0, sched1 = localize_folds(
                sched_dep_local,
                origin_tz,
            )

            sched_dst = classify_local(
                sched_dep_local,
                origin_tz,
                sched0,
                sched1,
            )

            # =================================================================
            # D. SCHEDULED ARRIVAL - DÙNG CRSElapsedTime
            # =================================================================

            crs_elapsed_valid = crs_elapsed.where(
                crs_elapsed.ge(0)
            )

            sched_arr0_utc = (
                sched0
                + pd.to_timedelta(
                    crs_elapsed_valid,
                    unit="m",
                )
            )

            sched_arr1_utc = (
                sched1
                + pd.to_timedelta(
                    crs_elapsed_valid,
                    unit="m",
                )
            )

            sched_arr0_local = utc_to_local_naive(
                sched_arr0_utc,
                dest_tz,
            )

            sched_arr1_local = utc_to_local_naive(
                sched_arr1_utc,
                dest_tz,
            )

            sched_arr0_m = minute_of_day(
                sched_arr0_local
            )

            sched_arr1_m = minute_of_day(
                sched_arr1_local
            )

            sched_match0 = (
                circular_diff(
                    sched_arr0_m,
                    carr_min,
                ).abs()
                <= TOLERANCE_MINUTES
            )

            sched_match1 = (
                circular_diff(
                    sched_arr1_m,
                    carr_min,
                ).abs()
                <= TOLERANCE_MINUTES
            )

            selected_sched_utc = sched0.copy()
            sched_final = sched_dst.copy()

            sched_amb = sched_dst.eq(
                "AMBIGUOUS_LOCAL_TIME"
            )

            choose_s0 = (
                sched_amb
                & sched_match0
                & ~sched_match1
            )

            choose_s1 = (
                sched_amb
                & sched_match1
                & ~sched_match0
            )

            unresolved_sched_amb = (
                sched_amb
                & ~(choose_s0 | choose_s1)
            )

            selected_sched_utc.loc[choose_s1] = (
                sched1.loc[choose_s1]
            )

            selected_sched_utc.loc[
                unresolved_sched_amb
            ] = pd.NaT

            sched_final.loc[
                choose_s0 | choose_s1
            ] = "AMBIGUOUS_RESOLVED_BY_CRS_ARRIVAL"

            sched_final.loc[
                unresolved_sched_amb
            ] = "AMBIGUOUS_UNRESOLVED"

            crs_elapsed_missing = crs_elapsed.isna()

            crs_elapsed_negative = (
                crs_elapsed.notna()
                & crs_elapsed.lt(0)
            )

            sched_arrival_local = utc_to_local_naive(
                selected_sched_utc
                + pd.to_timedelta(
                    crs_elapsed_valid,
                    unit="m",
                ),
                dest_tz,
            )

            sched_arrival_clock_resid = circular_diff(
                minute_of_day(sched_arrival_local),
                carr_min,
            )

            # Phân loại giờ CRSArrTime trên ngày đến suy ra từ elapsed.
            # Phát hiện trường hợp giờ nguồn rơi vào DST gap.
            sched_arr_clock_naive = pd.Series(
                pd.NaT,
                index=df.index,
                dtype="datetime64[ns]",
            )

            sched_arr_date_ok = (
                sched_arrival_local.notna()
                & carr_min.notna()
            )

            sched_arr_clock_naive.loc[sched_arr_date_ok] = (
                sched_arrival_local.loc[
                    sched_arr_date_ok
                ].dt.normalize()
                + pd.to_timedelta(
                    carr_min.loc[sched_arr_date_ok],
                    unit="m",
                )
            )

            sched_arr_u0, sched_arr_u1 = localize_folds(
                sched_arr_clock_naive,
                dest_tz,
            )

            sched_arr_dst_status = classify_local(
                sched_arr_clock_naive,
                dest_tz,
                sched_arr_u0,
                sched_arr_u1,
            )

            sched_arr_status = pd.Series(
                "MATCH_WITHIN_1_MINUTE",
                index=df.index,
                dtype="string",
            )

            sched_arr_status.loc[
                selected_sched_utc.isna()
            ] = "SCHEDULED_DEP_UTC_UNAVAILABLE"

            sched_arr_status.loc[
                crs_elapsed_missing
            ] = "CRSElapsedTime_MISSING"

            sched_arr_status.loc[
                crs_elapsed_negative
            ] = "CRSElapsedTime_INVALID_NEGATIVE"

            sched_arr_status.loc[
                carr_st.eq("MISSING_HHMM")
            ] = "CRSArrTime_MISSING"

            sched_arr_status.loc[
                carr_st.eq("INVALID_HHMM")
            ] = "CRSArrTime_INVALID"

            sched_arr_status.loc[
                dest_tz.isna()
            ] = "MISSING_DESTINATION_TIMEZONE"

            sched_comparable = (
                selected_sched_utc.notna()
                & crs_elapsed_valid.notna()
                & sched_arrival_local.notna()
                & carr_min.notna()
                & dest_tz.notna()
            )

            sched_arr_status.loc[
                sched_comparable
                & sched_arrival_clock_resid.abs().gt(
                    TOLERANCE_MINUTES
                )
            ] = "CLOCK_MISMATCH"

            sched_arr_status.loc[
                sched_comparable
                & sched_arrival_clock_resid.abs().le(
                    TOLERANCE_MINUTES
                )
            ] = "MATCH_WITHIN_1_MINUTE"

            sched_arr_dst_status.loc[
                sched_arr_dst_status.eq(
                    "AMBIGUOUS_LOCAL_TIME"
                )
                & sched_comparable
                & sched_arrival_clock_resid.abs().le(
                    TOLERANCE_MINUTES
                )
            ] = "AMBIGUOUS_LOCAL_CLOCK_RESOLVED_BY_CRS_ELAPSED"

            # =================================================================
            # E. ACTUAL DEPARTURE - KIỂM TRA DepDelay THEO GIỜ ĐỊA PHƯƠNG
            # =================================================================

            sched_wall_min = (
                cdep_min
                + cdep_add * 1440
            )

            actual_wall_min = (
                dep_min
                + dep_add * 1440
            )

            wall_clock_delta = (
                actual_wall_min
                - sched_wall_min
            )

            day_offset = np.rint(
                (dep_delay - wall_clock_delta) / 1440.0
            )

            actual_dep_total_min = (
                actual_wall_min
                + pd.Series(
                    day_offset,
                    index=df.index,
                ) * 1440
            )

            actual_dep_local = (
                fdate
                + pd.to_timedelta(
                    actual_dep_total_min,
                    unit="m",
                )
            )

            actual_dep_local = actual_dep_local.where(
                fdate.notna()
                & dep_min.notna()
                & dep_delay.notna()
            )

            dep_local_delta = (
                actual_dep_local - sched_dep_local
            ).dt.total_seconds() / 60.0

            dep_delay_local_resid = (
                dep_local_delta - dep_delay
            )

            dep_delay_status = pd.Series(
                "LOCAL_DELAY_MATCH",
                index=df.index,
                dtype="string",
            )

            dep_delay_status.loc[
                dep_st.eq("MISSING_HHMM")
            ] = "ACTUAL_DEPTIME_MISSING"

            dep_delay_status.loc[
                dep_st.eq("INVALID_HHMM")
            ] = "ACTUAL_DEPTIME_INVALID"

            dep_delay_status.loc[
                dep_st.eq("VALID_HHMM")
                & dep_delay.isna()
            ] = "DEPDELAY_MISSING"

            dep_delay_status.loc[
                dep_st.eq("VALID_HHMM")
                & dep_delay.notna()
                & dep_delay_local_resid.abs().gt(
                    TOLERANCE_MINUTES
                )
            ] = "LOCAL_DEP_DELAY_MISMATCH"

            actual0, actual1 = localize_folds(
                actual_dep_local,
                origin_tz,
            )

            actual_dst = classify_local(
                actual_dep_local,
                origin_tz,
                actual0,
                actual1,
            )

            actual_dep_final = dep_delay_status.copy()

            actual_dep_final.loc[
                actual_dst.eq("NONEXISTENT_LOCAL_TIME")
            ] = "NONEXISTENT_LOCAL_TIME"

            actual_dep_final.loc[
                actual_dst.eq("AMBIGUOUS_LOCAL_TIME")
            ] = "AMBIGUOUS_UNRESOLVED_PENDING_ARRIVAL_EVIDENCE"

            actual_dep_final.loc[
                actual_dst.eq("MISSING_TIMEZONE")
            ] = "MISSING_ORIGIN_TIMEZONE"

            actual_dep_final.loc[
                actual_dst.eq("UNIQUE_LOCAL_TIME")
                & dep_delay_status.eq("LOCAL_DELAY_MATCH")
            ] = "UNIQUE_LOCAL_TIME_LOCAL_DELAY_MATCH"

            actual_dep_final.loc[
                actual_dst.eq("UNIQUE_LOCAL_TIME")
                & dep_delay_status.ne("LOCAL_DELAY_MATCH")
            ] = dep_delay_status.loc[
                actual_dst.eq("UNIQUE_LOCAL_TIME")
                & dep_delay_status.ne("LOCAL_DELAY_MATCH")
            ]

            # =================================================================
            # F. DÙNG ARRIVAL EVIDENCE ĐỂ PHÂN GIẢI AMBIGUOUS ACTUAL DEPARTURE
            # =================================================================

            actual_elapsed_valid = actual_elapsed.where(
                actual_elapsed.ge(0)
            )

            arr0_utc = (
                actual0
                + pd.to_timedelta(
                    actual_elapsed_valid,
                    unit="m",
                )
            )

            arr1_utc = (
                actual1
                + pd.to_timedelta(
                    actual_elapsed_valid,
                    unit="m",
                )
            )

            arr0_local = utc_to_local_naive(
                arr0_utc,
                dest_tz,
            )

            arr1_local = utc_to_local_naive(
                arr1_utc,
                dest_tz,
            )

            arr0_clock_resid = circular_diff(
                minute_of_day(arr0_local),
                arr_min,
            )

            arr1_clock_resid = circular_diff(
                minute_of_day(arr1_local),
                arr_min,
            )

            clock0_match = (
                arr0_clock_resid.abs()
                <= TOLERANCE_MINUTES
            )

            clock1_match = (
                arr1_clock_resid.abs()
                <= TOLERANCE_MINUTES
            )

            schedule_arr_baseline_ok = (
                sched_arr_status.eq(
                    "MATCH_WITHIN_1_MINUTE"
                )
                & sched_arrival_local.notna()
                & arr_delay.notna()
            )

            arr0_delay_resid = (
                (
                    arr0_local - sched_arrival_local
                ).dt.total_seconds() / 60.0
            ) - arr_delay

            arr1_delay_resid = (
                (
                    arr1_local - sched_arrival_local
                ).dt.total_seconds() / 60.0
            ) - arr_delay

            delay0_match = (
                arr0_delay_resid.abs()
                <= TOLERANCE_MINUTES
            )

            delay1_match = (
                arr1_delay_resid.abs()
                <= TOLERANCE_MINUTES
            )

            # Nếu scheduled-arrival baseline đáng tin cậy và có ArrDelay,
            # yêu cầu đồng thời khớp ArrTime và ArrDelay.
            # Nếu không, chỉ dùng ArrTime làm bằng chứng.
            evidence0 = (
                clock0_match
                & (~schedule_arr_baseline_ok | delay0_match)
            )

            evidence1 = (
                clock1_match
                & (~schedule_arr_baseline_ok | delay1_match)
            )

            ambiguous_actual_clock = (
                actual_dst.eq("AMBIGUOUS_LOCAL_TIME")
                & normal_flight
            )

            ambiguous_delay_unreliable = (
                ambiguous_actual_clock
                & dep_delay_status.ne("LOCAL_DELAY_MATCH")
            )

            actual_dep_final.loc[
                ambiguous_delay_unreliable
            ] = "AMBIGUOUS_NOT_RESOLVED_DELAY_UNRELIABLE"

            # Chỉ dùng arrival evidence nếu ngày actual departure ứng viên
            # đã vượt qua phép kiểm tra DepDelay theo giờ địa phương.
            actual_amb = (
                ambiguous_actual_clock
                & dep_delay_status.eq("LOCAL_DELAY_MATCH")
            )

            resolve_a0 = (
                actual_amb
                & evidence0
                & ~evidence1
            )

            resolve_a1 = (
                actual_amb
                & evidence1
                & ~evidence0
            )

            unresolved_actual_amb = (
                actual_amb
                & ~(resolve_a0 | resolve_a1)
            )

            selected_actual_dep_utc = actual0.copy()

            selected_actual_dep_utc.loc[resolve_a1] = (
                actual1.loc[resolve_a1]
            )

            selected_actual_dep_utc.loc[
                unresolved_actual_amb
            ] = pd.NaT

            actual_dep_final.loc[
                resolve_a0 | resolve_a1
            ] = "AMBIGUOUS_RESOLVED_BY_ARRIVAL_EVIDENCE"

            actual_dep_final.loc[
                unresolved_actual_amb
            ] = "AMBIGUOUS_UNRESOLVED"

            actual_unique = (
                actual_dst.eq("UNIQUE_LOCAL_TIME")
                & dep_delay_status.eq("LOCAL_DELAY_MATCH")
            )

            accepted_actual_dep = (
                actual_unique
                | resolve_a0
                | resolve_a1
            )

            selected_actual_dep_utc.loc[
                ~accepted_actual_dep
            ] = pd.NaT

            # =================================================================
            # G. ACTUAL ARRIVAL - KIỂM TRA THEO ACTUAL ELAPSED TIME
            # =================================================================

            actual_arrival_utc = (
                selected_actual_dep_utc
                + pd.to_timedelta(
                    actual_elapsed_valid,
                    unit="m",
                )
            )

            actual_arrival_local = utc_to_local_naive(
                actual_arrival_utc,
                dest_tz,
            )

            actual_arr_clock_resid = circular_diff(
                minute_of_day(actual_arrival_local),
                arr_min,
            )

            actual_arr_delay_resid = (
                (
                    actual_arrival_local - sched_arrival_local
                ).dt.total_seconds() / 60.0
            ) - arr_delay

            actual_arr_status = pd.Series(
                "MATCH_WITHIN_1_MINUTE",
                index=df.index,
                dtype="string",
            )

            actual_arr_status.loc[
                cancelled.eq(1)
            ] = "CANCELLED_SEPARATE_HANDLING"

            actual_arr_status.loc[
                cancelled.ne(1)
                & diverted.eq(1)
            ] = "DIVERTED_SEPARATE_HANDLING"

            active = normal_flight

            actual_arr_status.loc[
                active & selected_actual_dep_utc.isna()
            ] = "ACTUAL_DEP_UTC_UNAVAILABLE_OR_UNRESOLVED"

            actual_arr_status.loc[
                active & actual_elapsed.isna()
            ] = "ACTUALELAPSEDTIME_MISSING"

            actual_arr_status.loc[
                active
                & actual_elapsed.notna()
                & actual_elapsed.lt(0)
            ] = "ACTUALELAPSEDTIME_INVALID_NEGATIVE"

            actual_arr_status.loc[
                active & arr_st.eq("MISSING_HHMM")
            ] = "ARRTIME_MISSING"

            actual_arr_status.loc[
                active & arr_st.eq("INVALID_HHMM")
            ] = "ARRTIME_INVALID"

            actual_arr_status.loc[
                active & dest_tz.isna()
            ] = "MISSING_DESTINATION_TIMEZONE"

            arr_clock_comparable = (
                active
                & selected_actual_dep_utc.notna()
                & actual_elapsed_valid.notna()
                & arr_min.notna()
                & actual_arrival_local.notna()
            )

            actual_arr_status.loc[
                arr_clock_comparable
                & actual_arr_clock_resid.abs().gt(
                    TOLERANCE_MINUTES
                )
            ] = "ARRIVAL_CLOCK_MISMATCH"

            actual_arr_status.loc[
                arr_clock_comparable
                & actual_arr_clock_resid.abs().le(
                    TOLERANCE_MINUTES
                )
            ] = "ARRIVAL_CLOCK_MATCH"

            arr_delay_comparable = (
                arr_clock_comparable
                & schedule_arr_baseline_ok
            )

            actual_arr_status.loc[
                arr_delay_comparable
                & actual_arr_clock_resid.abs().le(
                    TOLERANCE_MINUTES
                )
                & actual_arr_delay_resid.abs().gt(
                    TOLERANCE_MINUTES
                )
            ] = "ARRDELAY_LOCAL_RESIDUAL_MISMATCH"

            actual_arr_status.loc[
                arr_delay_comparable
                & actual_arr_clock_resid.abs().le(
                    TOLERANCE_MINUTES
                )
                & actual_arr_delay_resid.abs().le(
                    TOLERANCE_MINUTES
                )
            ] = "ARRIVAL_CLOCK_AND_DELAY_MATCH"

            actual_arr_status.loc[
                active
                & selected_actual_dep_utc.notna()
                & actual_elapsed_valid.notna()
                & arr_min.notna()
                & sched_arrival_local.isna()
                & actual_arr_clock_resid.abs().le(
                    TOLERANCE_MINUTES
                )
            ] = "ARRIVAL_CLOCK_MATCH_SCHEDULE_BASELINE_UNAVAILABLE"

            # -----------------------------------------------------------------
            # H. Phân loại DST cho giờ đến thực tế ở sân bay đích
            # -----------------------------------------------------------------

            actual_arr_clock_naive = pd.Series(
                pd.NaT,
                index=df.index,
                dtype="datetime64[ns]",
            )

            valid_arr_date = (
                actual_arrival_local.notna()
                & arr_min.notna()
            )

            actual_arr_clock_naive.loc[valid_arr_date] = (
                actual_arrival_local.loc[
                    valid_arr_date
                ].dt.normalize()
                + pd.to_timedelta(
                    arr_min.loc[valid_arr_date],
                    unit="m",
                )
            )

            arr_clock_u0, arr_clock_u1 = localize_folds(
                actual_arr_clock_naive,
                dest_tz,
            )

            actual_arr_dst = classify_local(
                actual_arr_clock_naive,
                dest_tz,
                arr_clock_u0,
                arr_clock_u1,
            )

            actual_arr_dst.loc[
                actual_arr_dst.eq("AMBIGUOUS_LOCAL_TIME")
                & actual_arr_status.isin([
                    "ARRIVAL_CLOCK_MATCH",
                    "ARRIVAL_CLOCK_AND_DELAY_MATCH",
                    "ARRIVAL_CLOCK_MATCH_SCHEDULE_BASELINE_UNAVAILABLE",
                ])
            ] = "AMBIGUOUS_LOCAL_CLOCK_RESOLVED_BY_ACTUAL_ELAPSED"

            # =================================================================
            # I. TỔNG HỢP THEO THÁNG VÀ TIMEZONE
            # =================================================================

            for dim, status in [
                ("SCHEDULED_DEPARTURE_ORIGIN", sched_final),
                ("SCHEDULED_ARRIVAL_DESTINATION", sched_arr_status),
                ("SCHEDULED_ARRIVAL_DST_CLOCK", sched_arr_dst_status),
                ("ACTUAL_DEPARTURE_ORIGIN", actual_dep_final),
                ("ACTUAL_ARRIVAL_DESTINATION", actual_arr_status),
                ("ACTUAL_ARRIVAL_DST_CLOCK", actual_arr_dst),
            ]:

                temp = pd.DataFrame({
                    "month": month,
                    "status": status,
                })

                grouped = temp.groupby(
                    ["month", "status"],
                    dropna=False,
                ).size()

                for (m, st), value in grouped.items():
                    month_counts[
                        (str(m), dim, str(st))
                    ] += int(value)

            for tz_name, dim, status in [
                (
                    origin_tz,
                    "SCHEDULED_DEPARTURE_ORIGIN",
                    sched_dst,
                ),
                (
                    dest_tz,
                    "SCHEDULED_ARRIVAL_CLOCK_DESTINATION",
                    sched_arr_dst_status,
                ),
                (
                    origin_tz,
                    "ACTUAL_DEPARTURE_ORIGIN",
                    actual_dst,
                ),
                (
                    dest_tz,
                    "ACTUAL_ARRIVAL_CLOCK_DESTINATION",
                    actual_arr_dst,
                ),
            ]:

                temp = pd.DataFrame({
                    "tz": tz_name,
                    "status": status,
                })

                grouped = temp.groupby(
                    ["tz", "status"],
                    dropna=False,
                ).size()

                for (tzv, st), value in grouped.items():
                    tz_counts[
                        (str(tzv), dim, str(st))
                    ] += int(value)

            # -----------------------------------------------------------------
            # J. Profile theo tháng cho từng trường HHMM
            # -----------------------------------------------------------------

            for field, (_, _, status, _) in parsed.items():

                temp = pd.DataFrame({
                    "month": month,
                    "status": status,
                })

                grouped = temp.groupby(
                    ["month", "status"],
                    dropna=False,
                ).size()

                for (m, st), value in grouped.items():
                    profile_counts[
                        (field, f"{m}|{st}")
                    ] += int(value)

            # =================================================================
            # K. LẤY MẪU CÁC TRƯỜNG HỢP CẦN REVIEW
            # =================================================================

            sample_df = pd.DataFrame({
                "SourceMonth": month,
                "FlightDate": df["FlightDate"],
                "Reporting_Airline": df["Reporting_Airline"],
                "Flight_Number_Reporting_Airline": df[
                    "Flight_Number_Reporting_Airline"
                ],
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
                "ScheduledDepDSTStatus": sched_dst,
                "ScheduledDepFinalStatus": sched_final,
                "DepDelayLocalResidualMin": dep_delay_local_resid,
                "ActualDepDSTStatus": actual_dst,
                "ActualDepFinalStatus": actual_dep_final,
                "ScheduledArrStatus": sched_arr_status,
                "ScheduledArrClockResidualMin": sched_arrival_clock_resid,
                "ScheduledArrDSTStatus": sched_arr_dst_status,
                "ActualArrDSTStatus": actual_arr_dst,
                "ActualArrStatus": actual_arr_status,
                "ActualArrClockResidualMin": actual_arr_clock_resid,
                "ActualArrDelayLocalResidualMin": actual_arr_delay_resid,
            })

            sample_cols = list(sample_df.columns)

            capture(
                sample_df,
                "SCHEDULED_DEP_AMBIGUOUS",
                sched_final.eq("AMBIGUOUS_UNRESOLVED"),
                sample_cols,
            )

            capture(
                sample_df,
                "SCHEDULED_DEP_NONEXISTENT",
                sched_final.eq("NONEXISTENT_LOCAL_TIME"),
                sample_cols,
            )

            capture(
                sample_df,
                "SCHEDULED_ARR_CLOCK_MISMATCH",
                sched_arr_status.eq("CLOCK_MISMATCH"),
                sample_cols,
            )

            capture(
                sample_df,
                "ACTUAL_DEP_LOCAL_DELAY_MISMATCH",
                dep_delay_status.eq("LOCAL_DEP_DELAY_MISMATCH"),
                sample_cols,
            )

            capture(
                sample_df,
                "ACTUAL_DEP_NONEXISTENT",
                actual_dep_final.eq("NONEXISTENT_LOCAL_TIME"),
                sample_cols,
            )

            capture(
                sample_df,
                "ACTUAL_DEP_AMBIGUOUS_UNRESOLVED",
                actual_dep_final.eq("AMBIGUOUS_UNRESOLVED"),
                sample_cols,
            )

            capture(
                sample_df,
                "ACTUAL_ARR_CLOCK_MISMATCH",
                actual_arr_status.eq("ARRIVAL_CLOCK_MISMATCH"),
                sample_cols,
            )

            capture(
                sample_df,
                "ACTUAL_ARR_DELAY_MISMATCH",
                actual_arr_status.eq(
                    "ARRDELAY_LOCAL_RESIDUAL_MISMATCH"
                ),
                sample_cols,
            )

        logger.info(
            "Completed %s; rows=%s",
            path.name,
            f"{file_rows:,}",
        )

    # =========================================================================
    # L. XUẤT CÁC FILE KẾT QUẢ
    # =========================================================================

    profile_rows = []

    for (field, metric), value in profile_counts.items():

        if "|" not in metric:
            profile_rows.append({
                "FieldName": field,
                "Metric": metric,
                "Rows": int(value),
            })

        else:
            m, st = metric.split("|", 1)

            profile_rows.append({
                "FieldName": field,
                "Metric": f"{m}:{st}",
                "Rows": int(value),
            })

    profile_df = pd.DataFrame(
        profile_rows
    ).sort_values(
        ["FieldName", "Metric"]
    )

    month_df = pd.DataFrame([
        {
            "SourceMonth": m,
            "AuditDimension": dim,
            "AuditStatus": st,
            "Rows": v,
        }
        for (m, dim, st), v in month_counts.items()
    ]).sort_values(
        ["SourceMonth", "AuditDimension", "AuditStatus"]
    )

    timezone_df = pd.DataFrame([
        {
            "IANA_Timezone": tz,
            "AuditDimension": dim,
            "AuditStatus": st,
            "Rows": v,
        }
        for (tz, dim, st), v in tz_counts.items()
    ]).sort_values(
        ["IANA_Timezone", "AuditDimension", "AuditStatus"]
    )

    samples_df = (
        pd.concat(samples, ignore_index=True)
        if samples
        else pd.DataFrame()
    )

    profile_df.to_csv(
        model_dir / OUTPUTS[0],
        index=False,
        encoding="utf-8-sig",
    )

    month_df.to_csv(
        model_dir / OUTPUTS[1],
        index=False,
        encoding="utf-8-sig",
    )

    timezone_df.to_csv(
        model_dir / OUTPUTS[2],
        index=False,
        encoding="utf-8-sig",
    )

    samples_df.to_csv(
        model_dir / OUTPUTS[3],
        index=False,
        encoding="utf-8-sig",
    )

    # =========================================================================
    # M. SUMMARY
    # =========================================================================

    lines = [
        "STEP 27B-3C1R - CORRECTED LOCAL-TIME / DST AUDIT SUMMARY",
        "=" * 78,
        f"BTS files scanned: {len(files)}",
        f"BTS records scanned: {total_rows:,}",
        f"Expected records: {EXPECTED_ROWS:,}",
        f"Timezone mapping rows: {len(mp):,}",
        "",
        "CORRECTED INTERPRETATION",
        "- BTS HHMM clock fields are local wall-clock times.",
        "- DepDelay is validated against local actual-vs-scheduled departure clock difference, not UTC difference.",
        "- ArrDelay is validated against local actual-vs-scheduled arrival clock difference when the scheduled-arrival baseline is reliable.",
        "- CRSElapsedTime and ActualElapsedTime are treated as elapsed durations with time-zone difference adjustment.",
        "- DST ambiguous actual departure is not resolved by DepDelay alone.",
        "- For a normal completed flight, ActualElapsedTime + ArrTime + usable ArrDelay may resolve exactly one actual-departure fold.",
        "- Nonexistent local times are not shifted or fabricated.",
        "- Cancelled/diverted records remain in Fact_Flight scope; actual arrival is classified separately.",
        "- Audit only: no RAW/STAGING mutation and no CLEAN timestamp creation.",
        "",
        "STATUS TOTALS BY DIMENSION",
    ]

    if not month_df.empty:

        totals = month_df.groupby(
            ["AuditDimension", "AuditStatus"],
            as_index=False,
        )["Rows"].sum()

        for dim in totals["AuditDimension"].drop_duplicates():

            lines.append(f"\n[{dim}]")

            for row in totals[
                totals["AuditDimension"] == dim
            ].itertuples(index=False):

                lines.append(
                    f"{row.AuditStatus}: {int(row.Rows):,}"
                )

    lines += [
        "",
        "OUTPUT FILES",
    ]

    lines.extend(
        str(model_dir / f)
        for f in OUTPUTS
        if not f.endswith(".log")
    )

    (model_dir / OUTPUTS[4]).write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )

    # =========================================================================
    # N. FINAL LOG
    # =========================================================================

    logger.info(
        "Completed. records=%s",
        f"{total_rows:,}",
    )

    if total_rows != EXPECTED_ROWS:
        logger.warning(
            "Record count differs from expected %s",
            f"{EXPECTED_ROWS:,}",
        )

    actual_dep_mismatch = int(
        month_df[
            (month_df.AuditDimension == "ACTUAL_DEPARTURE_ORIGIN")
            & (month_df.AuditStatus == "LOCAL_DEP_DELAY_MISMATCH")
        ].Rows.sum()
    ) if not month_df.empty else 0

    actual_dep_ambiguous = int(
        month_df[
            (month_df.AuditDimension == "ACTUAL_DEPARTURE_ORIGIN")
            & (month_df.AuditStatus == "AMBIGUOUS_UNRESOLVED")
        ].Rows.sum()
    ) if not month_df.empty else 0

    scheduled_arr_mismatch = int(
        month_df[
            (month_df.AuditDimension == "SCHEDULED_ARRIVAL_DESTINATION")
            & (month_df.AuditStatus == "CLOCK_MISMATCH")
        ].Rows.sum()
    ) if not month_df.empty else 0

    actual_arr_mismatch = int(
        month_df[
            (month_df.AuditDimension == "ACTUAL_ARRIVAL_DESTINATION")
            & (month_df.AuditStatus == "ARRIVAL_CLOCK_MISMATCH")
        ].Rows.sum()
    ) if not month_df.empty else 0

    logger.info(
        "Actual departure local-delay mismatch=%s",
        f"{actual_dep_mismatch:,}",
    )

    logger.info(
        "Actual departure unresolved ambiguous=%s",
        f"{actual_dep_ambiguous:,}",
    )

    logger.info(
        "Scheduled arrival clock mismatch=%s",
        f"{scheduled_arr_mismatch:,}",
    )

    logger.info(
        "Actual arrival clock mismatch=%s",
        f"{actual_arr_mismatch:,}",
    )

    logger.info(
        "Summary: %s",
        model_dir / OUTPUTS[4],
    )

    logger.info(
        "Log: %s",
        log_path,
    )

    print(
        "\nSTEP 27B-3C1R completed. "
        "Review all 6 outputs before proceeding."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())