
"""STEP 26E - Promote fully validated BTS candidate Parquet files.

Safety controls:
- Requires all 12 Step 26D validation rows to be PASS.
- Requires exact full-data validation counters to be zero.
- Preserves all 12 current BTS Parquet files in a timestamped backup folder.
- Copies all candidates to temporary files and verifies SHA-256 before promotion.
- Checks candidate and target schemas and row counts before replacing anything.
- Uses os.replace for each final replacement.
- Attempts automatic rollback if any replacement fails.
- Candidate files are retained; RAW CSV files are never modified.

Run from D:\\DATA_WAREHOUSE:
python .\\08_SCRIPTS\\02_profiling\\step26e_promote_verified_bts_candidates.py
"""
from __future__ import annotations

import csv
import hashlib
import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
STAGING_DIR = ROOT / "03_STAGING_DATA" / "BTS"
CANDIDATE_DIR = ROOT / "03_STAGING_DATA" / "_REBUILD_BTS_26D" / "BTS"
CONTROL_DIR = ROOT / "03_STAGING_DATA" / "_CONTROL"
VALIDATION_CSV = CONTROL_DIR / "26d_bts_candidate_validation_summary.csv"

YEAR = 2025
EXPECTED_ROWS = {
    1: 539747, 2: 504884, 3: 600872, 4: 583950,
    5: 605648, 6: 611575, 7: 631428, 8: 602378,
    9: 562439, 10: 605844, 11: 570550, 12: 582304,
}

REQUIRED_VALIDATION_COLUMNS = {
    "Month", "CandidateFile", "RawRowsCompared", "ParquetRowsCompared",
    "ExpectedRows", "DataMismatchCells", "LineageNullCells",
    "LineageUnexpectedCells", "RowCountOK", "DataExactMatch",
    "LineageOK", "Status",
}


def make_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("step26e_promote_bts")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    for handler in (
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ):
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(
            lambda: f.read(8 * 1024 * 1024), b""
        ):
            h.update(block)
    return h.hexdigest()


def truthy(value: str) -> bool:
    return str(value).strip().lower() in {
        "true", "1", "yes"
    }


def load_and_validate_step26d() -> dict[int, dict[str, str]]:
    if not VALIDATION_CSV.is_file():
        raise FileNotFoundError(
            f"Step 26D validation report not found: {VALIDATION_CSV}"
        )

    with VALIDATION_CSV.open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)

        if reader.fieldnames is None:
            raise ValueError(
                "Step 26D validation CSV has no header"
            )

        missing = REQUIRED_VALIDATION_COLUMNS - set(
            reader.fieldnames
        )

        if missing:
            raise ValueError(
                f"Step 26D report is missing columns: {sorted(missing)}"
            )

        rows = list(reader)

    if len(rows) != 12:
        raise ValueError(
            f"Expected 12 Step 26D validation rows, found {len(rows)}"
        )

    by_month = {}

    for row in rows:
        month_text = row["Month"]

        try:
            month = int(month_text.split("-")[1])
        except Exception as exc:
            raise ValueError(
                f"Invalid month in Step 26D report: {month_text!r}"
            ) from exc

        if month not in EXPECTED_ROWS:
            raise ValueError(
                f"Unexpected month in Step 26D report: {month_text}"
            )

        if month in by_month:
            raise ValueError(
                f"Duplicate month in Step 26D report: {month_text}"
            )

        if row["Status"].strip().upper() != "PASS":
            raise ValueError(
                f"Month {month_text} is not PASS: {row['Status']!r}"
            )

        expected = EXPECTED_ROWS[month]

        if int(row["RawRowsCompared"]) != expected:
            raise ValueError(
                f"RAW compared row count mismatch for {month_text}"
            )

        if (
            int(row["ParquetRowsCompared"]) != expected
            or int(row["ExpectedRows"]) != expected
        ):
            raise ValueError(
                f"Parquet/expected row count mismatch for {month_text}"
            )

        if int(row["DataMismatchCells"]) != 0:
            raise ValueError(
                f"Nonzero data mismatch count for {month_text}"
            )

        if (
            int(row["LineageNullCells"]) != 0
            or int(row["LineageUnexpectedCells"]) != 0
        ):
            raise ValueError(
                f"Lineage validation failed for {month_text}"
            )

        if not all(
            truthy(row[c])
            for c in (
                "RowCountOK",
                "DataExactMatch",
                "LineageOK",
            )
        ):
            raise ValueError(
                f"A Step 26D boolean check is false for {month_text}"
            )

        expected_name = (
            f"stg_bts_{YEAR}_{month:02d}.parquet"
        )

        if row["CandidateFile"].strip() != expected_name:
            raise ValueError(
                f"Unexpected candidate file for {month_text}: "
                f"{row['CandidateFile']!r}"
            )

        by_month[month] = row

    if set(by_month) != set(range(1, 13)):
        raise ValueError(
            f"Step 26D report months are incomplete: {sorted(by_month)}"
        )

    return by_month


def parquet_info(path: Path):
    pf = pq.ParquetFile(path)
    return (
        pf.metadata.num_rows,
        pf.schema_arrow.names,
        pf.schema_arrow.types,
    )


def main() -> int:
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    backup_dir = (
        ROOT / "03_STAGING_DATA"
        / f"_BACKUP_BTS_PRE_26D_{stamp}"
    )

    log_path = CONTROL_DIR / "step26e_bts_promotion.log"
    manifest_path = (
        CONTROL_DIR / "26e_bts_promotion_manifest.csv"
    )

    logger = make_logger(log_path)

    promoted = []
    temp_files = []
    backup_created = False

    try:
        logger.info(
            "STEP 26E - PROMOTE VALIDATED BTS CANDIDATES"
        )

        logger.info(
            "Current BTS folder: %s", STAGING_DIR
        )
        logger.info(
            "Candidate BTS folder: %s", CANDIDATE_DIR
        )

        validation = load_and_validate_step26d()

        logger.info(
            "Step 26D gate passed: 12/12 months PASS"
        )

        # Preflight all source/target pairs before touching current Staging.
        plan = []
        reference_schema_names = None
        reference_schema_types = None

        for month in range(1, 13):
            name = f"stg_bts_{YEAR}_{month:02d}.parquet"

            current = STAGING_DIR / name
            candidate = CANDIDATE_DIR / name
            temp = STAGING_DIR / f"{name}.26e_promote.tmp"

            if not current.is_file():
                raise FileNotFoundError(
                    f"Current Staging file missing: {current}"
                )

            if not candidate.is_file():
                raise FileNotFoundError(
                    f"Validated candidate missing: {candidate}"
                )

            if temp.exists():
                raise FileExistsError(
                    f"Promotion temp file already exists: {temp}"
                )

            candidate_rows, candidate_names, candidate_types = (
                parquet_info(candidate)
            )

            current_rows, current_names, current_types = (
                parquet_info(current)
            )

            expected = EXPECTED_ROWS[month]

            if candidate_rows != expected:
                raise ValueError(
                    f"Candidate row count mismatch for {name}: "
                    f"{candidate_rows}"
                )

            if current_rows != expected:
                raise ValueError(
                    f"Current Staging row count mismatch for {name}: "
                    f"{current_rows}"
                )

            if (
                candidate_names != current_names
                or candidate_types != current_types
            ):
                raise ValueError(
                    f"Candidate schema differs from current Staging: {name}"
                )

            if reference_schema_names is None:
                reference_schema_names = candidate_names
                reference_schema_types = candidate_types

            elif (
                candidate_names != reference_schema_names
                or candidate_types != reference_schema_types
            ):
                raise ValueError(
                    f"Candidate schema inconsistent across months: {name}"
                )

            plan.append({
                "month": month,
                "name": name,
                "current": current,
                "candidate": candidate,
                "temp": temp,
                "expected_rows": expected,
            })

        logger.info(
            "Preflight passed for all 12 current and candidate files"
        )

        # Copy and verify all candidate temporary files first.
        for item in plan:
            shutil.copy2(
                item["candidate"],
                item["temp"]
            )

            temp_files.append(item["temp"])

            if sha256(item["candidate"]) != sha256(item["temp"]):
                raise IOError(
                    f"Temporary candidate checksum mismatch: {item['name']}"
                )

            temp_info = parquet_info(item["temp"])
            candidate_info = parquet_info(item["candidate"])

            if temp_info != candidate_info:
                raise IOError(
                    f"Temporary candidate Parquet verification failed: "
                    f"{item['name']}"
                )

        logger.info(
            "All 12 promotion temp files copied and checksum-verified"
        )

        # Back up all originals before replacing any of them.
        backup_dir.mkdir(parents=True, exist_ok=False)
        backup_created = True

        for item in plan:
            backup_path = backup_dir / item["name"]

            shutil.copy2(
                item["current"],
                backup_path
            )

            current_hash = sha256(item["current"])
            backup_hash = sha256(backup_path)

            if current_hash != backup_hash:
                raise IOError(
                    f"Backup checksum mismatch: {item['name']}"
                )

        logger.info(
            "Backed up and checksum-verified all 12 original files: %s",
            backup_dir
        )

        # Promotion phase.
        manifest_rows = []

        for item in plan:
            os.replace(
                item["temp"],
                item["current"]
            )

            promoted.append(item)

            new_hash = sha256(item["current"])
            candidate_hash = sha256(item["candidate"])

            if new_hash != candidate_hash:
                raise IOError(
                    f"Post-promotion checksum mismatch: {item['name']}"
                )

            new_rows, new_names, new_types = parquet_info(
                item["current"]
            )

            if new_rows != item["expected_rows"]:
                raise IOError(
                    f"Post-promotion row count mismatch: {item['name']}"
                )

            if (
                new_names != reference_schema_names
                or new_types != reference_schema_types
            ):
                raise IOError(
                    f"Post-promotion schema mismatch: {item['name']}"
                )

            manifest_rows.append({
                "Month": f"{YEAR}-{item['month']:02d}",
                "File": item["name"],
                "Rows": new_rows,
                "CandidateSHA256": candidate_hash,
                "PromotedSHA256": new_hash,
                "Status": "PROMOTED_VERIFIED",
            })

            logger.info(
                "PROMOTED %s rows=%s checksum verified",
                item["name"],
                new_rows
            )

        with manifest_path.open(
            "w", encoding="utf-8-sig", newline=""
        ) as f:
            writer = csv.DictWriter(
                f,
                fieldnames=list(manifest_rows[0].keys())
            )
            writer.writeheader()
            writer.writerows(manifest_rows)

        logger.info(
            "SUCCESS: 12/12 BTS Parquet files promoted and verified"
        )

        logger.info(
            "Original backup retained at: %s",
            backup_dir
        )

        logger.info(
            "Promotion manifest: %s",
            manifest_path
        )

        return 0

    except Exception as exc:
        logger.exception(
            "PROMOTION FAILED: %s", exc
        )

        if promoted and backup_created:
            logger.warning(
                "Attempting rollback of %s promoted file(s)",
                len(promoted)
            )

            rollback_ok = True

            for item in reversed(promoted):
                backup_path = backup_dir / item["name"]

                rollback_temp = (
                    STAGING_DIR
                    / f"{item['name']}.26e_rollback.tmp"
                )

                try:
                    if not backup_path.is_file():
                        raise FileNotFoundError(backup_path)

                    shutil.copy2(
                        backup_path,
                        rollback_temp
                    )

                    if sha256(backup_path) != sha256(rollback_temp):
                        raise IOError(
                            f"Rollback temp checksum mismatch: {item['name']}"
                        )

                    os.replace(
                        rollback_temp,
                        item["current"]
                    )

                    logger.info(
                        "ROLLBACK OK: %s", item["name"]
                    )

                except Exception:
                    rollback_ok = False
                    logger.exception(
                        "ROLLBACK FAILED: %s", item["name"]
                    )

                finally:
                    rollback_temp.unlink(missing_ok=True)

            logger.warning(
                "Rollback result: %s",
                "ALL RESTORED"
                if rollback_ok
                else "CHECK LOG - MANUAL REVIEW REQUIRED"
            )

        else:
            logger.info(
                "No current Staging file was replaced; "
                "no rollback required"
            )

        return 1

    finally:
        for path in temp_files:
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
