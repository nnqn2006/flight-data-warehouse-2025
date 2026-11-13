from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

import step29c_build_flight_weather_exposure_bridge as base

ROOT = base.ROOT
BTS_DIR = base.BTS_DIR
OUT_BASE = base.OUT_BASE
EVENT_NAMES = base.EVENT_NAMES
OUTPUT_FIELDS = base.OUTPUT_FIELDS
CURRENT_WORK_DIR: Path | None = None
EXPECTED_BTS_ROWS_BY_MONTH = base.EXPECTED_BTS_ROWS_BY_MONTH
EXPECTED_STATUS_COUNTS = base.EXPECTED_STATUS_COUNTS
EXPECTED_FLIGHTS = base.EXPECTED_FLIGHTS
EXPECTED_EVENT_ROWS = base.EXPECTED_EVENT_ROWS
EXPECTED_TOTAL_EXPOSURE_ROWS = base.EXPECTED_TOTAL_EXPOSURE_ROWS


def utc_run_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S_%f")


def atomic_text(path: Path, content: str) -> None:
    tmp = path.with_name(path.name + f".tmp_{utc_run_id()}")
    with tmp.open("x", encoding="utf-8", newline="") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def atomic_json(path: Path, payload: dict) -> None:
    tmp = path.with_name(path.name + f".tmp_{utc_run_id()}")
    with tmp.open("x", encoding="utf-8", newline="") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def atomic_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    tmp = path.with_name(path.name + f".tmp_{utc_run_id()}")
    with tmp.open("x", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def file_stat_signature(path: Path) -> dict:
    st = path.stat()
    return {"size_bytes": st.st_size, "mtime_ns": st.st_mtime_ns}


def chunk_filename(event: str, month: str, run_id: str) -> str:
    return f"29C_bridge_{event.lower()}_2025_{month}_{run_id}.csv"


def checkpoint_path(work_dir: Path, month: str) -> Path:
    return work_dir / f"checkpoint_2025_{month}.json"


def validate_chunk(path: Path, event: str, month: str,
                   first_key: int, expected_rows: int) -> dict:
    count = 0
    first_seen = None
    last_seen = None
    previous = None
    matched = 0
    statuses: Counter[str] = Counter()
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != OUTPUT_FIELDS:
            raise RuntimeError(f"{path.name}: output header/schema mismatch")
        for row in reader:
            count += 1
            key = int(row["FlightKey"])
            expected_key = first_key + count - 1
            if key != expected_key:
                raise RuntimeError(
                    f"{path.name}: FlightKey sequence mismatch at row {count:,}; "
                    f"expected {expected_key:,}, got {key:,}"
                )
            if row["SourceMonth"] != month:
                raise RuntimeError(f"{path.name}: SourceMonth mismatch at row {count:,}")
            if row["ExposureType"] != event:
                raise RuntimeError(f"{path.name}: ExposureType mismatch at row {count:,}")
            if not row["SourceRowNumber"].strip():
                raise RuntimeError(f"{path.name}: blank SourceRowNumber at row {count:,}")
            is_matched = row["WeatherCoverageStatus"] == "MATCHED_WEATHER_HOUR"
            if is_matched:
                matched += 1
                if not row["WeatherNaturalKey"].strip() or not row["WeatherHourUTC"].strip():
                    raise RuntimeError(f"{path.name}: matched row missing weather key/hour")
            elif row["WeatherNaturalKey"].strip():
                raise RuntimeError(f"{path.name}: unmatched row has WeatherNaturalKey")
            statuses[row["WeatherCoverageStatus"]] += 1
            if first_seen is None:
                first_seen = key
            last_seen = key
            previous = key
    if count != expected_rows:
        raise RuntimeError(f"{path.name}: expected {expected_rows:,} rows, read back {count:,}")
    return {
        "rows": count,
        "first_flight_key": first_seen,
        "last_flight_key": last_seen,
        "matched_rows": matched,
        "status_counts": dict(statuses),
        "file_size_bytes": path.stat().st_size,
        "sha256": base.sha256_file(path),
    }


def move_uncommitted_attempts(work_dir: Path, month: str, run_id: str) -> None:
    """Preserve non-checkpointed remnants before retrying a month."""
    candidates = []
    for event in EVENT_NAMES:
        final_chunk = work_dir / chunk_filename(event, month, run_id)
        part_chunk = work_dir / (chunk_filename(event, month, run_id) + ".part")
        if final_chunk.exists():
            candidates.append(final_chunk)
        if part_chunk.exists():
            candidates.append(part_chunk)
    if not candidates:
        return
    archive_root = work_dir / "_orphaned_attempts"
    archive_root.mkdir(exist_ok=True)
    archive = archive_root / f"month_{month}_attempt_{utc_run_id()}"
    archive.mkdir()
    for p in candidates:
        shutil.move(str(p), str(archive / p.name))
    print(f"Preserved uncommitted month {month} remnants at: {archive}", flush=True)


def load_and_validate_checkpoint(work_dir: Path, month: str, run_id: str,
                                 bts_path: Path, expected_first_key: int) -> dict:
    cp_path = checkpoint_path(work_dir, month)
    payload = json.loads(cp_path.read_text(encoding="utf-8"))
    if payload.get("month") != month:
        raise RuntimeError(f"Checkpoint month mismatch: {cp_path.name}")
    if payload.get("source_file") != bts_path.name:
        raise RuntimeError(f"Checkpoint source-file mismatch for month {month}")
    if payload.get("source_signature") != file_stat_signature(bts_path):
        raise RuntimeError(f"BTS CLEAN source file changed since checkpoint for month {month}")
    if payload.get("first_flight_key") != expected_first_key:
        raise RuntimeError(f"Checkpoint FlightKey start mismatch for month {month}")
    expected_rows = EXPECTED_BTS_ROWS_BY_MONTH[month]
    if payload.get("rows") != expected_rows:
        raise RuntimeError(f"Checkpoint row-count mismatch for month {month}")
    for event in EVENT_NAMES:
        info = payload.get("files", {}).get(event)
        if not info:
            raise RuntimeError(f"Checkpoint missing {event} file info for month {month}")
        p = work_dir / info["filename"]
        if not p.exists():
            raise RuntimeError(f"Checkpoint output missing: {p}")
        if base.sha256_file(p) != info["sha256"]:
            raise RuntimeError(f"Checkpoint SHA256 mismatch: {p.name}")
        readback = validate_chunk(p, event, month, expected_first_key, expected_rows)
        if readback["sha256"] != info["sha256"]:
            raise RuntimeError(f"Checkpoint read-back hash mismatch: {p.name}")
    return payload


def process_month(bts_path: Path, month: str, first_key: int,
                  airport_map: dict[str, str], weather_ranges,
                  work_dir: Path, run_id: str) -> dict:
    expected_rows = EXPECTED_BTS_ROWS_BY_MONTH[month]
    move_uncommitted_attempts(work_dir, month, run_id)

    part_paths = {
        event: work_dir / (chunk_filename(event, month, run_id) + ".part")
        for event in EVENT_NAMES
    }
    handles = {}
    writers = {}
    status_counts: Counter[tuple[str, str]] = Counter()
    event_counts: Counter[str] = Counter()
    blank_lineage = 0
    rows_this_month = 0
    zone_cache = {}
    source_sig = file_stat_signature(bts_path)

    try:
        for event, p in part_paths.items():
            handles[event] = p.open("x", encoding="utf-8-sig", newline="")
            writers[event] = csv.DictWriter(
                handles[event], fieldnames=OUTPUT_FIELDS, extrasaction="raise",
                lineterminator="\n",
            )
            writers[event].writeheader()

        print(f"Processing month {month} | first FlightKey={first_key:,}", flush=True)
        with bts_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows_this_month += 1
                flight_key = first_key + rows_this_month - 1
                source_month_raw = base.audit.txt(row.get("SourceMonth")) or month
                source_month = f"{int(source_month_raw):02d}" if source_month_raw.isdigit() else month
                if source_month != month:
                    raise RuntimeError(
                        f"SourceMonth mismatch in {bts_path.name}: {source_month!r}"
                    )
                source_file = base.audit.txt(row.get("SourceFile")) or bts_path.name
                source_row_number = base.audit.txt(row.get("SourceRowNumber"))
                if not source_row_number:
                    blank_lineage += 1
                events = base.resolve_event_rows(row, airport_map, zone_cache)
                if len(events) != 4:
                    raise RuntimeError(f"Expected four events for FlightKey={flight_key:,}")
                for event_tuple in events:
                    event = event_tuple[0]
                    rec = base.build_row(
                        flight_key, source_file, month, source_row_number,
                        event_tuple, airport_map, weather_ranges,
                    )
                    event_counts[event] += 1
                    status_counts[(event, rec["WeatherCoverageStatus"])] += 1
                    writers[event].writerow(rec)
                if rows_this_month % 100_000 == 0:
                    print(
                        f"  month {month}: {rows_this_month:,}/{expected_rows:,} flights",
                        flush=True,
                    )

        if rows_this_month != expected_rows:
            raise RuntimeError(
                f"{bts_path.name}: expected {expected_rows:,} rows, got {rows_this_month:,}"
            )
        if blank_lineage:
            raise RuntimeError(f"Month {month}: blank SourceRowNumber={blank_lineage:,}")
    finally:
        # Always flush/fsync and close every writer before read-back or rename.
        close_errors = []
        for event, handle in handles.items():
            try:
                handle.flush()
                os.fsync(handle.fileno())
            except Exception as exc:
                close_errors.append(f"{event}: flush/fsync: {exc}")
            try:
                handle.close()
            except Exception as exc:
                close_errors.append(f"{event}: close: {exc}")
        if close_errors:
            if sys.exc_info()[0] is None:
                raise RuntimeError("Output close/flush errors: " + " | ".join(close_errors))
            print("Additional output close/flush issue(s): " + " | ".join(close_errors), file=sys.stderr)

    if any(event_counts[e] != expected_rows for e in EVENT_NAMES):
        raise RuntimeError(f"Month {month}: one or more event row counts mismatch: {dict(event_counts)}")

    file_info = {}
    # Validate all four .part outputs before promoting any of them.
    for event in EVENT_NAMES:
        info = validate_chunk(part_paths[event], event, month, first_key, expected_rows)
        file_info[event] = info

    # Per-month chunks are atomically renamed within the same volume.
    for event in EVENT_NAMES:
        final_chunk = work_dir / chunk_filename(event, month, run_id)
        if final_chunk.exists():
            raise FileExistsError(f"Refusing to overwrite monthly chunk: {final_chunk}")
        os.replace(part_paths[event], final_chunk)
        file_info[event]["filename"] = final_chunk.name

    cp = {
        "step": "29C2",
        "run_id": run_id,
        "month": month,
        "source_file": bts_path.name,
        "source_signature": source_sig,
        "rows": rows_this_month,
        "first_flight_key": first_key,
        "last_flight_key": first_key + rows_this_month - 1,
        "event_counts": dict(event_counts),
        "status_counts": {
            f"{event}|{status}": n for (event, status), n in sorted(status_counts.items())
        },
        "files": file_info,
    }
    atomic_json(checkpoint_path(work_dir, month), cp)
    print(
        f"  MONTH {month} CHECKPOINT PASS | rows={rows_this_month:,} | "
        f"FlightKey={first_key:,}..{first_key + rows_this_month - 1:,}",
        flush=True,
    )
    return cp


def build_final_reports(work_dir: Path, run_id: str, checkpoints: list[dict]) -> tuple[str, list[str]]:
    failures = []
    total_flights = sum(cp["rows"] for cp in checkpoints)
    total_exposures = 0
    aggregate_status: Counter[tuple[str, str]] = Counter()
    manifest_rows = []
    month_manifest = []
    expected_first = 1

    for cp in checkpoints:
        month = cp["month"]
        rows = cp["rows"]
        first_key = cp["first_flight_key"]
        last_key = cp["last_flight_key"]
        if first_key != expected_first:
            failures.append(
                f"Month {month}: non-contiguous FlightKey; expected {expected_first}, got {first_key}"
            )
        if last_key - first_key + 1 != rows:
            failures.append(f"Month {month}: FlightKey range length mismatch")
        expected_first = last_key + 1
        month_manifest.append({
            "SourceMonth": month,
            "SourceFile": cp["source_file"],
            "RowsProcessed": rows,
            "FirstFlightKey": first_key,
            "LastFlightKey": last_key,
        })

        for event in EVENT_NAMES:
            info = cp["files"][event]
            path = work_dir / info["filename"]
            readback = validate_chunk(path, event, month, first_key, rows)
            if readback["sha256"] != info["sha256"]:
                failures.append(f"Month {month}/{event}: SHA256 differs from checkpoint")
            if readback["rows"] != rows:
                failures.append(f"Month {month}/{event}: read-back row count mismatch")
            total_exposures += readback["rows"]
            for status, n in readback["status_counts"].items():
                aggregate_status[(event, status)] += n
            manifest_rows.append({
                "SourceMonth": month,
                "ExposureType": event,
                "Filename": info["filename"],
                "RowsWritten": info["rows"],
                "RowsReadBack": readback["rows"],
                "ExpectedRows": rows,
                "FirstFlightKey": first_key,
                "LastFlightKey": last_key,
                "MatchedRows": readback["matched_rows"],
                "UnmatchedRows": rows - readback["matched_rows"],
                "FileSizeBytes": readback["file_size_bytes"],
                "SHA256": readback["sha256"],
                "Validation": "PASS",
            })

    if total_flights != EXPECTED_FLIGHTS:
        failures.append(f"Total BTS flights expected {EXPECTED_FLIGHTS:,}; got {total_flights:,}")
    if total_exposures != EXPECTED_TOTAL_EXPOSURE_ROWS:
        failures.append(
            f"Total exposure rows expected {EXPECTED_TOTAL_EXPOSURE_ROWS:,}; got {total_exposures:,}"
        )
    if len(checkpoints) != 12:
        failures.append(f"Expected 12 monthly checkpoints; got {len(checkpoints)}")
    if expected_first != EXPECTED_FLIGHTS + 1:
        failures.append("Final FlightKey range does not end at 7,001,619")

    for event in EVENT_NAMES:
        event_total = sum(cp["event_counts"].get(event, 0) for cp in checkpoints)
        if event_total != EXPECTED_EVENT_ROWS:
            failures.append(f"{event}: total expected {EXPECTED_EVENT_ROWS:,}; got {event_total:,}")
        expected_status = EXPECTED_STATUS_COUNTS[event]
        actual_statuses = {
            status: n for (ev, status), n in aggregate_status.items() if ev == event
        }
        for status, expected_n in expected_status.items():
            actual_n = actual_statuses.get(status, 0)
            if actual_n != expected_n:
                failures.append(
                    f"{event}/{status}: STEP29B4 expected {expected_n:,}; got {actual_n:,}"
                )
        for status, actual_n in actual_statuses.items():
            if status not in expected_status and actual_n:
                failures.append(f"{event}: unexpected status {status}={actual_n:,}")

    result = "PASS" if not failures else "REVIEW"
    fields = [
        "SourceMonth", "ExposureType", "Filename", "RowsWritten", "RowsReadBack",
        "ExpectedRows", "FirstFlightKey", "LastFlightKey", "MatchedRows",
        "UnmatchedRows", "FileSizeBytes", "SHA256", "Validation",
    ]
    manifest_path = work_dir / f"29C2_bridge_output_manifest_{run_id}.csv"
    atomic_csv(manifest_path, fields, manifest_rows)

    fm_path = work_dir / f"29C2_flightkey_month_manifest_{run_id}.csv"
    fields2 = ["SourceMonth", "SourceFile", "RowsProcessed", "FirstFlightKey", "LastFlightKey"]
    atomic_csv(fm_path, fields2, month_manifest)

    matched_total = sum(aggregate_status[(event, "MATCHED_WEATHER_HOUR")] for event in EVENT_NAMES)
    summary = [
        "STEP 29C2 - CHECKPOINTED FLIGHT / WEATHER EXPOSURE BRIDGE",
        "=" * 100,
        f"Run ID: {run_id}",
        "Output layout: 48 monthly CSV partitions (12 SourceMonth x 4 ExposureType)",
        f"BTS flights processed: {total_flights:,} / {EXPECTED_FLIGHTS:,}",
        f"Exposure rows: {total_exposures:,} / {EXPECTED_TOTAL_EXPOSURE_ROWS:,}",
        f"Matched exposure rows: {matched_total:,}",
        f"Unmatched / separately handled rows: {total_exposures - matched_total:,}",
        f"Monthly checkpoints: {len(checkpoints)}/12",
        "",
        "EVENT STATUS RECONCILIATION",
        "-" * 100,
    ]
    for event in EVENT_NAMES:
        summary.append(f"{event}: total={sum(cp['event_counts'].get(event, 0) for cp in checkpoints):,}")
        for status in sorted(set(EXPECTED_STATUS_COUNTS[event]) | {
            s for ev, s in aggregate_status if ev == event
        }):
            summary.append(
                f"  {status}: {aggregate_status[(event, status)]:,} "
                f"(STEP29B4 expected {EXPECTED_STATUS_COUNTS[event].get(status, 0):,})"
            )
    summary += [
        "",
        "DATA SAFETY",
        "- RAW, STAGING, CLEAN were read-only.",
        "- Each monthly partition was flushed, closed, read back, schema checked, row checked, and SHA256 recorded.",
        "- Checkpoint is committed only after all four event partitions for that month validate.",
        "- WeatherNaturalKey is present only for MATCHED_WEATHER_HOUR rows.",
        "- FlightKey is sequential over sorted BTS CLEAN months and source row order.",
        "- SourceFile + SourceRowNumber are retained as lineage.",
        "- No pandas, PyArrow, Parquet, network, or third-party packages are used by this script.",
        "",
        f"FINAL RESULT: {result}",
    ]
    if failures:
        summary += ["FAILED CHECKS:"] + [f"- {x}" for x in failures]
    summary_path = work_dir / f"29C2_bridge_validation_summary_{run_id}.txt"
    atomic_text(summary_path, "\n".join(summary) + "\n")
    return result, failures


def run(execute: bool, resume_id: str | None) -> int:
    bts_files = sorted(BTS_DIR.glob("BTS_CLEAN_2025_*.csv"))
    if len(bts_files) != 12:
        raise RuntimeError(f"Expected 12 BTS CLEAN files; found {len(bts_files)}")
    airport_map = base.read_airport_crosswalk(base.AIRPORT_MAP_PATH)
    weather_ranges, weather_ts_col, weather_airport_col = base.validate_weather_inventory()
    if set(weather_ranges) != set(airport_map):
        raise RuntimeError("Weather airport scope does not equal resolved BTS airport scope")
    for p in bts_files:
        month = p.stem[-2:]
        if month not in EXPECTED_BTS_ROWS_BY_MONTH:
            raise RuntimeError(f"Unexpected month file: {p.name}")
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            missing = [c for c in base.audit.REQUIRED_BTS_COLUMNS if c not in (reader.fieldnames or [])]
            if missing:
                raise RuntimeError(f"{p.name}: required columns missing: {missing}")

    run_id = resume_id or utc_run_id()
    work_dir = OUT_BASE / f"_building_{run_id}"
    global CURRENT_WORK_DIR
    CURRENT_WORK_DIR = work_dir
    final_dir = OUT_BASE / f"run_{run_id}"
    if resume_id:
        if not work_dir.exists():
            raise FileNotFoundError(f"No resumable working directory: {work_dir}")
        if final_dir.exists():
            raise FileExistsError(f"Final run directory already exists: {final_dir}")
    else:
        OUT_BASE.mkdir(parents=True, exist_ok=True)
        if work_dir.exists() or final_dir.exists():
            raise FileExistsError(f"Run directory collision: {work_dir} or {final_dir}")
        # PREVIEW must be strictly read-only: create a work directory only for EXECUTE.
        if execute:
            work_dir.mkdir()

    # Preflight estimate and disk check. On resume, also check for sufficient free space.
    projected_bytes, required_free, sample_rows = base.estimate_output_size(
        bts_files, airport_map, weather_ranges
    )
    free_bytes = shutil.disk_usage(ROOT).free
    if execute and free_bytes < required_free:
        raise RuntimeError(
            f"Insufficient free disk: available={free_bytes / 1024**3:.2f} GiB; "
            f"required={required_free / 1024**3:.2f} GiB"
        )

    print("STEP 29C2 - CHECKPOINTED BRIDGE BUILD", flush=True)
    print(f"Run ID: {run_id}", flush=True)
    print(f"Mode: {'RESUME' if resume_id else ('EXECUTE' if execute else 'PREVIEW')}", flush=True)
    print(f"BTS files: {len(bts_files)} | Weather airports: {len(weather_ranges)}", flush=True)
    print(f"Estimated CSV: {projected_bytes / 1024**3:.2f} GiB", flush=True)
    print(f"Free space: {free_bytes / 1024**3:.2f} GiB", flush=True)
    print(f"Weather columns: time / {weather_airport_col or 'filename'}", flush=True)
    if not execute:
        print("PREVIEW PASS: no bridge output files were created.")
        return 0

    checkpoints = []
    first_key = 1
    for bts_path in bts_files:
        month = bts_path.stem[-2:]
        cp_path = checkpoint_path(work_dir, month)
        if cp_path.exists():
            cp = load_and_validate_checkpoint(work_dir, month, run_id, bts_path, first_key)
            print(
                f"SKIP CHECKPOINT {month}: PASS | rows={cp['rows']:,} | "
                f"FlightKey={cp['first_flight_key']:,}..{cp['last_flight_key']:,}",
                flush=True,
            )
        else:
            cp = process_month(
                bts_path, month, first_key, airport_map, weather_ranges, work_dir, run_id
            )
        checkpoints.append(cp)
        first_key += EXPECTED_BTS_ROWS_BY_MONTH[month]

    result, failures = build_final_reports(work_dir, run_id, checkpoints)
    if result != "PASS":
        print(f"STEP 29C2 FINAL RESULT: {result}", flush=True)
        for msg in failures:
            print(f"  FAIL: {msg}", flush=True)
        return 2

    try:
        work_dir.rename(final_dir)
        print(f"PROMOTED: {final_dir}", flush=True)
    except OSError as exc:
        # Do not mask the completed result; retain the complete _building_ directory.
        atomic_text(
            work_dir / f"PROMOTION_PENDING_{run_id}.txt",
            f"All chunk and global validations PASS, but final directory rename failed.\n{type(exc).__name__}: {exc}\n",
        )
        print(
            f"VALIDATION PASS; PROMOTION PENDING. Outputs safely remain at: {work_dir}\n"
            f"Rename error: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 3

    print("STEP 29C2 EXECUTE RESULT: PASS", flush=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="STEP 29C2 checkpointed Flight-Weather Bridge build")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--execute", action="store_true", help="Start a new checkpointed build")
    group.add_argument("--resume", metavar="RUN_ID", help="Resume an existing _building_RUN_ID directory")
    args = parser.parse_args()
    if args.resume:
        return run(execute=True, resume_id=args.resume)
    return run(execute=args.execute, resume_id=None)


if __name__ == "__main__":
    try:
        exit_code = main()
    except Exception as exc:
        # Preserve the original failure; never rename the working folder from this handler.
        try:
            msg = (
                f"Run failure timestamp: {datetime.now().isoformat()}\n"
                f"Exception: {type(exc).__name__}: {exc}\n"
            )
            # Record diagnostics only in this run's working directory; never guess
            # another concurrent or older _building_ directory.
            if CURRENT_WORK_DIR is not None and CURRENT_WORK_DIR.exists():
                err = CURRENT_WORK_DIR / "RUN_ERROR.txt"
                with err.open("a", encoding="utf-8") as f:
                    f.write(msg)
                    f.flush()
                    os.fsync(f.fileno())
        except Exception:
            pass
        print(f"STEP 29C2 FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise
    raise SystemExit(exit_code)
