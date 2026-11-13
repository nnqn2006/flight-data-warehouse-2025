from pathlib import Path
from collections import Counter
from datetime import datetime
import csv
import hashlib
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "01_RAW_DATA" / "BTS" / "csv"
CLEAN_DIR = ROOT / "04_CLEAN_DATA" / "BTS"
OUT_DIR = ROOT / "06_DW_DESIGN" / "01_LOGICAL_MODEL"

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
SUMMARY_PATH = OUT_DIR / f"27D1_bts_header_reconciliation_{RUN_ID}.txt"
MANIFEST_PATH = OUT_DIR / f"27D1_bts_header_manifest_{RUN_ID}.csv"


def get_month(path):
    match = re.search(r"2025_(\d{2})\.csv$", path.name, re.IGNORECASE)
    return match.group(1) if match else None


def read_header(path):
    """
    Read only the first CSV record (header).
    Does not load the dataset rows.
    """
    encodings = ("utf-8-sig", "cp1252")
    last_error = None

    for encoding in encodings:
        try:
            with path.open("r", encoding=encoding, newline="") as f:
                header = next(csv.reader(f), None)

            if header is None:
                raise ValueError("Empty file")

            return header, encoding

        except UnicodeDecodeError as exc:
            last_error = exc

    raise last_error or ValueError("Cannot read CSV header")


def header_hash(header):
    joined = "\x1f".join(header)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def inspect_group(files, group_name):
    records = []
    errors = []

    for path in files:
        month = get_month(path)

        try:
            header, encoding = read_header(path)
            counts = Counter(header)

            records.append({
                "FileGroup": group_name,
                "Month": month or "",
                "Filename": path.name,
                "ColumnCount": len(header),
                "BlankHeaderCount": sum(1 for x in header if not x.strip()),
                "DuplicateHeaderCount": sum(v - 1 for v in counts.values() if v > 1),
                "HeaderSHA256": header_hash(header),
                "FirstColumn": header[0] if header else "",
                "LastColumn": header[-1] if header else "",
                "EncodingUsed": encoding,
                "Header": header,
            })

        except Exception as exc:
            errors.append(f"{path.name}: {type(exc).__name__}: {exc}")

    return records, errors


def main():
    if not RAW_DIR.exists():
        raise FileNotFoundError(f"RAW directory not found: {RAW_DIR}")

    if not CLEAN_DIR.exists():
        raise FileNotFoundError(f"CLEAN directory not found: {CLEAN_DIR}")

    raw_files = sorted(RAW_DIR.glob("BTS_2025_*.csv"))
    clean_files = sorted(CLEAN_DIR.glob("BTS_CLEAN_2025_*.csv"))

    raw_records, raw_errors = inspect_group(raw_files, "RAW")
    clean_records, clean_errors = inspect_group(clean_files, "CLEAN")

    lines = []
    lines.append("STEP 27D1 - BTS RAW VS CLEAN HEADER RECONCILIATION")
    lines.append("=" * 90)
    lines.append(f"Run timestamp: {RUN_ID}")
    lines.append(f"RAW directory: {RAW_DIR}")
    lines.append(f"CLEAN directory: {CLEAN_DIR}")
    lines.append(f"RAW files found: {len(raw_files)}")
    lines.append(f"CLEAN files found: {len(clean_files)}")
    lines.append("")

    expected_months = {f"{i:02d}" for i in range(1, 13)}
    raw_months = {r["Month"] for r in raw_records}
    clean_months = {r["Month"] for r in clean_records}

    lines.append("MONTH INVENTORY")
    lines.append("-" * 90)
    lines.append(f"RAW missing months: {sorted(expected_months - raw_months)}")
    lines.append(f"CLEAN missing months: {sorted(expected_months - clean_months)}")
    lines.append("")

    for group_name, records, errors in [
        ("RAW", raw_records, raw_errors),
        ("CLEAN", clean_records, clean_errors),
    ]:
        lines.append(f"{group_name} HEADER PROFILE")
        lines.append("-" * 90)

        for r in records:
            lines.append(
                f'{r["Filename"]}: columns={r["ColumnCount"]}, '
                f'blank_headers={r["BlankHeaderCount"]}, '
                f'duplicate_headers={r["DuplicateHeaderCount"]}, '
                f'encoding={r["EncodingUsed"]}, '
                f'first={r["FirstColumn"]!r}, last={r["LastColumn"]!r}'
            )

        lines.append(f"Header read errors: {len(errors)}")
        for error in errors:
            lines.append(f"ERROR: {error}")
        lines.append("")

    raw_by_month = {r["Month"]: r for r in raw_records}
    clean_by_month = {r["Month"]: r for r in clean_records}

    lines.append("MONTHLY RAW VS CLEAN COMPARISON")
    lines.append("-" * 90)

    all_months = sorted(expected_months)

    for month in all_months:
        raw = raw_by_month.get(month)
        clean = clean_by_month.get(month)

        if raw is None or clean is None:
            lines.append(f"2025-{month}: COMPARISON_NOT_POSSIBLE")
            continue

        raw_header = raw["Header"]
        clean_header = clean["Header"]

        raw_counter = Counter(raw_header)
        clean_counter = Counter(clean_header)

        added = list((clean_counter - raw_counter).elements())
        removed = list((raw_counter - clean_counter).elements())
        same_order = raw_header == clean_header

        lines.append(f"2025-{month}:")
        lines.append(f"  RAW columns: {len(raw_header)}")
        lines.append(f"  CLEAN columns: {len(clean_header)}")
        lines.append(f"  Exact same order: {same_order}")
        lines.append(f"  Added in CLEAN: {added}")
        lines.append(f"  Missing from CLEAN: {removed}")

    lines.append("")
    lines.append("GROUP SCHEMA CONSISTENCY")
    lines.append("-" * 90)

    def group_consistency(records):
        if not records:
            return False, "NO_RECORDS"

        hashes = {r["HeaderSHA256"] for r in records}
        return len(hashes) == 1, f"distinct_header_hashes={len(hashes)}"

    raw_consistent, raw_detail = group_consistency(raw_records)
    clean_consistent, clean_detail = group_consistency(clean_records)

    lines.append(f"RAW consistent across files: {raw_consistent} ({raw_detail})")
    lines.append(f"CLEAN consistent across files: {clean_consistent} ({clean_detail})")

    lines.append("")
    lines.append("HEADER ANOMALY CHECKS")
    lines.append("-" * 90)

    for group_name, records in [("RAW", raw_records), ("CLEAN", clean_records)]:
        for r in records:
            if r["BlankHeaderCount"] > 0 or r["DuplicateHeaderCount"] > 0:
                lines.append(
                    f'{group_name} {r["Filename"]}: '
                    f'blank={r["BlankHeaderCount"]}, '
                    f'duplicate={r["DuplicateHeaderCount"]}'
                )

    if not any(
        r["BlankHeaderCount"] > 0 or r["DuplicateHeaderCount"] > 0
        for r in raw_records + clean_records
    ):
        lines.append("No blank or duplicate header names detected.")

    lines.append("")
    lines.append("INTERPRETATION")
    lines.append("-" * 90)
    lines.append("- Header count is based on CSV header fields, including blank trailing fields.")
    lines.append("- A CLEAN-only column may be a documented derived or lineage field.")
    lines.append("- A missing RAW column must be reviewed before approving source-to-target mapping.")
    lines.append("- This script does not read or modify data rows.")
    lines.append("- RAW, STAGING, and CLEAN data files were read-only.")
    lines.append("")

    SUMMARY_PATH.write_text("\n".join(lines), encoding="utf-8")

    manifest_fields = [
        "FileGroup", "Month", "Filename", "ColumnCount",
        "BlankHeaderCount", "DuplicateHeaderCount",
        "HeaderSHA256", "FirstColumn", "LastColumn", "EncodingUsed"
    ]

    with MANIFEST_PATH.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=manifest_fields)
        writer.writeheader()

        for r in raw_records + clean_records:
            writer.writerow({k: r[k] for k in manifest_fields})

    print("STEP 27D1 FINISHED")
    print(f"RAW files: {len(raw_files)}")
    print(f"CLEAN files: {len(clean_files)}")
    print(f"RAW schema consistent: {raw_consistent}")
    print(f"CLEAN schema consistent: {clean_consistent}")
    print(f"Summary: {SUMMARY_PATH}")
    print(f"Manifest: {MANIFEST_PATH}")
    print("RAW / STAGING / CLEAN data were not modified.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"STEP 27D1 FAILED: {type(exc).__name__}: {exc}")
        sys.exit(1)
