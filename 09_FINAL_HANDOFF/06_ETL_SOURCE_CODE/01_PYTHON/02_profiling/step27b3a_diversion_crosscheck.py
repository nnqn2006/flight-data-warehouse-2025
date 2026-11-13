
from __future__ import annotations

import logging
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


# ============================================================
# 1. CONFIGURATION
# ============================================================

ROOT = Path(r"D:\DATA_WAREHOUSE")

BTS_DIR = ROOT / "03_STAGING_DATA" / "BTS"

OUT_DIR = ROOT / "06_DW_DESIGN" / "01_LOGICAL_MODEL"

BATCH_SIZE = 100_000
MAX_SAMPLES_PER_RULE = 50

SUMMARY_FILE = OUT_DIR / "27B3A_diversion_crosscheck_summary.txt"
FLAG_MATRIX_FILE = OUT_DIR / "27B3A_diversion_flag_matrix.csv"
SEGMENT_FILE = OUT_DIR / "27B3A_diversion_segment_completeness.csv"
ANOMALY_COUNT_FILE = OUT_DIR / "27B3A_diversion_anomaly_counts.csv"
ANOMALY_SAMPLE_FILE = OUT_DIR / "27B3A_diversion_anomaly_samples.csv"
LOG_FILE = OUT_DIR / "27B3A_diversion_crosscheck.log"


# ============================================================
# 2. DIVERSION FIELDS
# ============================================================

SEGMENT_FIELDS = {
    1: [
        "Div1Airport", "Div1AirportID", "Div1AirportSeqID",
        "Div1WheelsOn", "Div1TotalGTime", "Div1LongestGTime",
        "Div1WheelsOff", "Div1TailNum"
    ],
    2: [
        "Div2Airport", "Div2AirportID", "Div2AirportSeqID",
        "Div2WheelsOn", "Div2TotalGTime", "Div2LongestGTime",
        "Div2WheelsOff", "Div2TailNum"
    ],
    3: [
        "Div3Airport", "Div3AirportID", "Div3AirportSeqID",
        "Div3WheelsOn", "Div3TotalGTime", "Div3LongestGTime",
        "Div3WheelsOff", "Div3TailNum"
    ],
    4: [
        "Div4Airport", "Div4AirportID", "Div4AirportSeqID",
        "Div4WheelsOn", "Div4TotalGTime", "Div4LongestGTime",
        "Div4WheelsOff", "Div4TailNum"
    ],
    5: [
        "Div5Airport", "Div5AirportID", "Div5AirportSeqID",
        "Div5WheelsOn", "Div5TotalGTime", "Div5LongestGTime",
        "Div5WheelsOff", "Div5TailNum"
    ],
}

BASE_COLUMNS = [
    "Cancelled",
    "Diverted",
    "SourceFile",
    "SourceMonth"
]

ALL_COLUMNS = (
    BASE_COLUMNS
    + [c for cols in SEGMENT_FIELDS.values() for c in cols]
)


# ============================================================
# 3. LOGGER
# ============================================================

def setup_logger() -> logging.Logger:

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("step27b3a")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler = logging.FileHandler(
        LOG_FILE,
        encoding="utf-8"
    )
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# ============================================================
# 4. DATA PRESENCE HELPERS
# ============================================================

def is_present(series: pd.Series) -> pd.Series:
    """
    Xác định giá trị có dữ liệu hay không.

    Empty string và literal NA được xem là không có dữ liệu
    trong phép kiểm tra này.

    Không sửa dữ liệu gốc.
    """

    s = series.fillna("").astype("string").str.strip()

    return s.ne("") & s.str.upper().ne("NA")


def raw_category(series: pd.Series) -> pd.Series:
    """
    Giữ lại các trạng thái gốc để lập ma trận đối chiếu.

    Blank      -> <BLANK>
    Literal NA -> <LITERAL_NA>
    """

    s = series.fillna("").astype("string").str.strip()

    s = s.mask(s.eq(""), "<BLANK>")

    s = s.mask(
        s.str.upper().eq("NA"),
        "<LITERAL_NA>"
    )

    return s


# ============================================================
# 5. MAIN PROCESS
# ============================================================

def main() -> int:

    # Không cho phép ghi đè kết quả cũ.
    outputs = [
        SUMMARY_FILE,
        FLAG_MATRIX_FILE,
        SEGMENT_FILE,
        ANOMALY_COUNT_FILE,
        ANOMALY_SAMPLE_FILE,
        LOG_FILE
    ]

    existing = [p for p in outputs if p.exists()]

    if existing:
        raise FileExistsError(
            "Output đã tồn tại, không ghi đè. "
            "Hãy archive/đổi tên các file sau trước khi chạy lại:\n"
            + "\n".join(str(p) for p in existing)
        )

    logger = setup_logger()

    files = sorted(
        BTS_DIR.glob("stg_bts_2025_*.parquet")
    )

    if len(files) != 12:
        raise FileNotFoundError(
            f"Cần đúng 12 BTS Parquet files, "
            f"hiện tìm thấy {len(files)} tại {BTS_DIR}"
        )

    # --------------------------------------------------------
    # Initialize counters
    # --------------------------------------------------------

    flag_counts = Counter()

    segment_counts = {
        i: Counter()
        for i in SEGMENT_FIELDS
    }

    anomaly_counts = Counter()

    anomaly_samples = defaultdict(list)

    total_rows = 0

    # --------------------------------------------------------
    # Define anomaly rules
    # --------------------------------------------------------

    anomaly_rules = [
        "DIVERTED_1_NO_DIV1_AIRPORT",
        "DIVERTED_0_HAS_DIV1_AIRPORT",
        "CANCELLED_1_AND_DIVERTED_1",
        "CANCELLED_1_AND_HAS_DIV1_AIRPORT",
        "DIV1_AIRPORT_WITHOUT_ID",
        "DIV1_ID_WITHOUT_AIRPORT",
        "DIV2_WITHOUT_DIV1",
        "DIV3_WITHOUT_DIV2",
        "DIV4_WITHOUT_DIV3",
        "DIV5_WITHOUT_DIV4",
    ]

    logger.info(
        "STEP 27B-3A - DIVERSION CROSS-CHECK"
    )

    logger.info(
        "BTS files found: %s",
        len(files)
    )

    # ========================================================
    # 6. PROCESS EACH BTS FILE
    # ========================================================

    for file_idx, path in enumerate(files, start=1):

        pf = pq.ParquetFile(path)

        missing = [
            c for c in ALL_COLUMNS
            if c not in pf.schema.names
        ]

        if missing:
            raise ValueError(
                f"Thiếu cột trong {path.name}: {missing}"
            )

        file_rows = 0

        logger.info(
            "Scanning %s (%s/%s)",
            path.name,
            file_idx,
            len(files)
        )

        # ----------------------------------------------------
        # Read data by batches
        # ----------------------------------------------------

        for batch in pf.iter_batches(
            batch_size=BATCH_SIZE,
            columns=ALL_COLUMNS
        ):

            df = batch.to_pandas()

            n = len(df)

            if n == 0:
                continue

            file_rows += n
            total_rows += n

            # Vị trí dòng bắt đầu trong file Parquet,
            # tính từ 0.
            row_start = file_rows - n

            # ------------------------------------------------
            # Determine raw flags
            # ------------------------------------------------

            cancelled_raw = raw_category(df["Cancelled"])

            diverted_raw = raw_category(df["Diverted"])

            has_airport = {
                i: is_present(df[f"Div{i}Airport"])
                for i in range(1, 6)
            }

            has_airport_id = {
                i: is_present(df[f"Div{i}AirportID"])
                for i in range(1, 6)
            }

            # ------------------------------------------------
            # Build flag matrix
            # ------------------------------------------------

            fm = pd.DataFrame({
                "CancelledRaw": cancelled_raw,
                "DivertedRaw": diverted_raw,

                "HasDiv1Airport": has_airport[1].map({
                    True: "Y",
                    False: "N"
                }),

                "HasDiv1AirportID": has_airport_id[1].map({
                    True: "Y",
                    False: "N"
                }),
            })

            for key, count in fm.value_counts(
                dropna=False
            ).items():

                flag_counts[key] += int(count)

            # ------------------------------------------------
            # Segment completeness
            # ------------------------------------------------

            for seg, fields in SEGMENT_FIELDS.items():

                field_presence = {
                    c: is_present(df[c])
                    for c in fields
                }

                presence_df = pd.concat(
                    field_presence.values(),
                    axis=1
                )

                any_field = presence_df.any(axis=1)

                all_fields = presence_df.all(axis=1)

                segment_counts[seg][
                    "RowsWithAnySegmentField"
                ] += int(any_field.sum())

                segment_counts[seg][
                    "RowsWithAirport"
                ] += int(has_airport[seg].sum())

                segment_counts[seg][
                    "RowsWithAirportID"
                ] += int(has_airport_id[seg].sum())

                segment_counts[seg][
                    "RowsWithAll8Fields"
                ] += int(all_fields.sum())

                for c, present in field_presence.items():

                    segment_counts[seg][
                        f"{c}__Populated"
                    ] += int(present.sum())

                    segment_counts[seg][
                        f"{c}__BlankOrNA"
                    ] += int((~present).sum())

            # ------------------------------------------------
            # Define anomaly conditions
            # ------------------------------------------------

            c1 = cancelled_raw.eq("1.00")

            d1 = diverted_raw.eq("1.00")

            d0 = diverted_raw.eq("0.00")

            rules = {

                "DIVERTED_1_NO_DIV1_AIRPORT":
                    d1 & ~has_airport[1],

                "DIVERTED_0_HAS_DIV1_AIRPORT":
                    d0 & has_airport[1],

                "CANCELLED_1_AND_DIVERTED_1":
                    c1 & d1,

                "CANCELLED_1_AND_HAS_DIV1_AIRPORT":
                    c1 & has_airport[1],

                "DIV1_AIRPORT_WITHOUT_ID":
                    has_airport[1] & ~has_airport_id[1],

                "DIV1_ID_WITHOUT_AIRPORT":
                    has_airport_id[1] & ~has_airport[1],

                "DIV2_WITHOUT_DIV1":
                    has_airport[2] & ~has_airport[1],

                "DIV3_WITHOUT_DIV2":
                    has_airport[3] & ~has_airport[2],

                "DIV4_WITHOUT_DIV3":
                    has_airport[4] & ~has_airport[3],

                "DIV5_WITHOUT_DIV4":
                    has_airport[5] & ~has_airport[4],
            }

            # ------------------------------------------------
            # Sample columns for investigation
            # ------------------------------------------------

            sample_base = [
                "Cancelled",
                "Diverted",

                "Div1Airport",
                "Div1AirportID",
                "Div1AirportSeqID",
                "Div1WheelsOn",
                "Div1TotalGTime",
                "Div1LongestGTime",
                "Div1WheelsOff",
                "Div1TailNum",

                "Div2Airport",
                "Div2AirportID",
                "Div2AirportSeqID",
                "Div2WheelsOn",
                "Div2WheelsOff",

                "Div3Airport",
                "Div3AirportID",

                "Div4Airport",
                "Div5Airport",
            ]

            # ------------------------------------------------
            # Count anomalies and collect sample rows
            # ------------------------------------------------

            for rule, mask in rules.items():

                count = int(mask.sum())

                anomaly_counts[rule] += count

                remaining = (
                    MAX_SAMPLES_PER_RULE
                    - len(anomaly_samples[rule])
                )

                if count and remaining > 0:

                    positions = [
                        int(x)
                        for x in mask[mask].index[:remaining]
                    ]

                    for pos in positions:

                        offset = int(pos)

                        record = {
                            c: df.at[pos, c]
                            for c in sample_base
                        }

                        record.update({

                            "AnomalyRule": rule,

                            "StagingFile": path.name,

                            "SourceFile": df.at[
                                pos,
                                "SourceFile"
                            ],

                            "SourceMonth": df.at[
                                pos,
                                "SourceMonth"
                            ],

                            "ParquetRowNumber1Based":
                                row_start + offset + 1,

                            "CSVLineNumberIncludingHeader":
                                row_start + offset + 2,
                        })

                        anomaly_samples[rule].append(record)

        logger.info(
            "Completed %s; rows=%s",
            path.name,
            file_rows
        )

    # ========================================================
    # 7. VALIDATE TOTAL ROWS
    # ========================================================

    if total_rows != 7_001_619:

        raise ValueError(
            "Tổng số dòng BTS không khớp baseline "
            f"7,001,619: {total_rows:,}"
        )

    # ========================================================
    # 8. EXPORT FLAG MATRIX
    # ========================================================

    flag_rows = []

    for key, count in sorted(flag_counts.items()):

        flag_rows.append({

            "CancelledRaw": key[0],

            "DivertedRaw": key[1],

            "HasDiv1Airport": key[2],

            "HasDiv1AirportID": key[3],

            "Rows": count,
        })

    pd.DataFrame(flag_rows).to_csv(
        FLAG_MATRIX_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    # ========================================================
    # 9. EXPORT SEGMENT COMPLETENESS
    # ========================================================

    seg_rows = []

    for seg, counts in segment_counts.items():

        row = {
            "SegmentNumber": seg,
            "TotalBTSRows": total_rows
        }

        row.update(dict(counts))

        row["AirportPopulatedRate"] = (
            counts["RowsWithAirport"] / total_rows
        )

        row["AnySegmentFieldRate"] = (
            counts["RowsWithAnySegmentField"] / total_rows
        )

        seg_rows.append(row)

    pd.DataFrame(seg_rows).to_csv(
        SEGMENT_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    # ========================================================
    # 10. EXPORT ANOMALY COUNTS
    # ========================================================

    pd.DataFrame([

        {
            "AnomalyRule": rule,

            "Rows": anomaly_counts[rule],

            "SampleRowsWritten": len(
                anomaly_samples[rule]
            ),

            "Interpretation":
                "Candidate for review; not automatically a data error"
        }

        for rule in anomaly_rules

    ]).to_csv(
        ANOMALY_COUNT_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    # ========================================================
    # 11. EXPORT ANOMALY SAMPLES
    # ========================================================

    sample_rows = [
        r
        for rule in anomaly_rules
        for r in anomaly_samples[rule]
    ]

    pd.DataFrame(sample_rows).to_csv(
        ANOMALY_SAMPLE_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    # ========================================================
    # 12. BUILD SUMMARY REPORT
    # ========================================================

    diverted_no_div1 = anomaly_counts[
        "DIVERTED_1_NO_DIV1_AIRPORT"
    ]

    not_diverted_has_div1 = anomaly_counts[
        "DIVERTED_0_HAS_DIV1_AIRPORT"
    ]

    summary = [

        "STEP 27B-3A - DIVERSION CROSS-CHECK SUMMARY",

        "=" * 76,

        f"Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",

        f"BTS files scanned: {len(files)}",

        f"Total BTS rows scanned: {total_rows:,}",

        "",

        "KEY CROSS-CHECKS",

        f"Diverted=1 and Div1Airport absent/NA: {diverted_no_div1:,}",

        f"Diverted=0 and Div1Airport populated: {not_diverted_has_div1:,}",

        f"Cancelled=1 and Diverted=1: "
        f"{anomaly_counts['CANCELLED_1_AND_DIVERTED_1']:,}",

        f"Cancelled=1 and Div1Airport populated: "
        f"{anomaly_counts['CANCELLED_1_AND_HAS_DIV1_AIRPORT']:,}",

        f"Div1Airport populated but Div1AirportID absent: "
        f"{anomaly_counts['DIV1_AIRPORT_WITHOUT_ID']:,}",

        f"Div1AirportID populated but Div1Airport absent: "
        f"{anomaly_counts['DIV1_ID_WITHOUT_AIRPORT']:,}",

        "",

        "SEGMENT SEQUENCE CHECKS",

        f"Div2 present while Div1 absent: "
        f"{anomaly_counts['DIV2_WITHOUT_DIV1']:,}",

        f"Div3 present while Div2 absent: "
        f"{anomaly_counts['DIV3_WITHOUT_DIV2']:,}",

        f"Div4 present while Div3 absent: "
        f"{anomaly_counts['DIV4_WITHOUT_DIV3']:,}",

        f"Div5 present while Div4 absent: "
        f"{anomaly_counts['DIV5_WITHOUT_DIV4']:,}",

        "",

        "INTERPRETATION",

        "These are consistency indicators, not automatic error labels. "
        "Review the flag matrix and sample rows",

        "before deciding whether a source record is invalid "
        "or reflects BTS reporting semantics.",

        "Literal NA tokens are treated as unavailable for presence checks "
        "but are not modified in STAGING.",

        "Sample CSVLineNumberIncludingHeader is the row position "
        "in the original monthly CSV, assuming one header row.",

        "",

        "OUTPUT FILES",

        str(FLAG_MATRIX_FILE),

        str(SEGMENT_FILE),

        str(ANOMALY_COUNT_FILE),

        str(ANOMALY_SAMPLE_FILE),

        str(LOG_FILE),
    ]

    SUMMARY_FILE.write_text(
        "\n".join(summary) + "\n",
        encoding="utf-8"
    )

    logger.info(
        "Step 27B-3A completed. Rows scanned=%s",
        f"{total_rows:,}"
    )

    logger.info(
        "Summary: %s",
        SUMMARY_FILE
    )

    return 0


# ============================================================
# 13. ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        raise SystemExit(main())

    except Exception as exc:

        print(
            f"ERROR: {exc}",
            file=sys.stderr
        )

        raise
