#!/usr/bin/env python3
"""
Experimental conservative Coinbase merger.

Primary is the base stream. Backup is used only inside detected primary gaps.
This avoids the inflation caused by merging both complete level2_batch streams.
Raw captures are never modified.
"""

import argparse
import gzip
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def parse_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat().replace("+00:00", "Z") if dt else None


def open_text(path: Path):
    return gzip.open(path, "rt", encoding="utf-8") if path.suffix == ".gz" else open(path, "rt", encoding="utf-8")


def event_time(record: Dict[str, Any]) -> Optional[datetime]:
    data = record.get("data") or {}
    return parse_time(data.get("time") or record.get("recv_time"))


def is_l2(record: Dict[str, Any]) -> bool:
    return (record.get("data") or {}).get("type") == "l2update"


def change_key(time_str: str, change: List[str]) -> Optional[Tuple[str, str, str, str]]:
    if len(change) < 3:
        return None
    return (time_str, change[0], change[1], change[2])


def iter_records(paths: Iterable[Path], source: str):
    for path in paths:
        with open_text(path) as f:
            for line_no, line in enumerate(f, 1):
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    yield None, {"source": source, "path": str(path), "line": line_no}
                    continue
                record["_source_meta_tmp"] = {"source": source, "path": str(path), "line": line_no}
                yield record, record["_source_meta_tmp"]


def load_records(paths: List[Path], source: str, start: Optional[datetime], end: Optional[datetime]):
    records = []
    json_errors = 0
    for record, meta in iter_records(paths, source):
        if record is None:
            json_errors += 1
            continue
        t = event_time(record)
        if t is not None:
            if start and t < start:
                continue
            if end and t > end:
                continue
        records.append(record)
    return records, json_errors


def detect_gaps_from_records(primary_records: List[Dict[str, Any]], threshold: float):
    gaps = []
    prev_t = None
    for record in primary_records:
        if not is_l2(record):
            continue
        t = event_time(record)
        if t is None:
            continue
        if prev_t is not None:
            seconds = (t - prev_t).total_seconds()
            if seconds >= threshold:
                gaps.append({"start": prev_t, "end": t, "seconds": seconds})
        prev_t = t
    return gaps


def detect_gaps_from_paths(paths: List[Path], start: Optional[datetime], end: Optional[datetime], threshold: float):
    gaps = []
    prev_t = None
    json_errors = 0
    records = 0
    l2updates = 0
    for record, _meta in iter_records(paths, "primary"):
        if record is None:
            json_errors += 1
            continue
        t = event_time(record)
        if not in_range_time(t, start, end):
            continue
        records += 1
        if not is_l2(record):
            continue
        l2updates += 1
        if t is None:
            continue
        if prev_t is not None:
            seconds = (t - prev_t).total_seconds()
            if seconds >= threshold:
                gaps.append({"start": prev_t, "end": t, "seconds": seconds})
        prev_t = t
    return gaps, {"records": records, "l2updates": l2updates, "json_errors": json_errors}


def in_range_time(t: Optional[datetime], start: Optional[datetime], end: Optional[datetime]) -> bool:
    if t is None:
        return True
    if start and t < start:
        return False
    if end and t >= end:
        return False
    return True


def in_gap(t: datetime, gaps: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for gap in gaps:
        if gap["start"] < t < gap["end"]:
            return gap
    return None


def gap_stats(times: List[datetime], thresholds: List[float]) -> Dict[str, Any]:
    times = sorted(times)
    stats = {str(th): {"count": 0, "total_seconds": 0.0, "max_seconds": 0.0} for th in thresholds}
    examples = []
    prev = None
    for t in times:
        if prev is not None:
            seconds = (t - prev).total_seconds()
            for th in thresholds:
                if seconds >= th:
                    stats[str(th)]["count"] += 1
                    stats[str(th)]["total_seconds"] += seconds
                    stats[str(th)]["max_seconds"] = max(stats[str(th)]["max_seconds"], seconds)
            if seconds >= 2 and len(examples) < 20:
                examples.append({"seconds": seconds, "from": iso(prev), "to": iso(t)})
        prev = t
    return {"thresholds": stats, "examples_ge_2s": examples}


def clean_for_output(record: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(record)
    out.pop("_source_meta_tmp", None)
    return out


def collect_backup_patches(paths: List[Path], start: Optional[datetime], end: Optional[datetime], gaps: List[Dict[str, Any]]):
    patch_records = []
    patch_change_keys = set()
    stats = {
        "records": 0,
        "json_errors": 0,
        "l2updates_seen": 0,
        "l2updates_inside_primary_gaps": 0,
        "changes_seen_inside_gaps": 0,
        "changes_kept_inside_gaps": 0,
        "changes_deduped_inside_gaps": 0,
    }
    for record, _meta in iter_records(paths, "backup"):
        if record is None:
            stats["json_errors"] += 1
            continue
        t = event_time(record)
        if not in_range_time(t, start, end):
            continue
        stats["records"] += 1
        if not is_l2(record):
            continue
        stats["l2updates_seen"] += 1
        if t is None:
            continue
        gap = in_gap(t, gaps)
        if gap is None:
            continue
        stats["l2updates_inside_primary_gaps"] += 1
        data = record.get("data") or {}
        time_str = data.get("time")
        kept_changes = []
        for change in data.get("changes") or []:
            stats["changes_seen_inside_gaps"] += 1
            key = change_key(time_str, change)
            if key is None:
                continue
            if key in patch_change_keys:
                stats["changes_deduped_inside_gaps"] += 1
                continue
            patch_change_keys.add(key)
            kept_changes.append(change)
        if not kept_changes:
            continue
        patched = clean_for_output(record)
        patched_data = dict(patched["data"])
        patched_data["changes"] = kept_changes
        patched["data"] = patched_data
        patched["merge_meta"] = {
            "source": "backup_patch",
            "sources": ["backup"],
            "gap_filled": True,
            "gap_start": iso(gap["start"]),
            "gap_end": iso(gap["end"]),
            "gap_seconds": gap["seconds"],
            "source_meta": {"backup": record.get("_source_meta_tmp")},
        }
        patch_records.append(patched)
        stats["changes_kept_inside_gaps"] += len(kept_changes)
    patch_records.sort(key=lambda r: event_time(r) or datetime.min)
    return patch_records, stats


def write_streaming_output(
    primary_paths: List[Path],
    patch_records: List[Dict[str, Any]],
    start: Optional[datetime],
    end: Optional[datetime],
    out_path: Path,
):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    patch_idx = 0
    output_records = 0
    primary_records = 0
    patch_written = 0
    primary_json_errors = 0
    merged_times: List[datetime] = []
    primary_times: List[datetime] = []
    patch_times = [event_time(r) for r in patch_records if event_time(r) is not None]

    def write_record(handle, record):
        handle.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n")

    with gzip.open(out_path, "wt", encoding="utf-8", compresslevel=6) as out:
        for record, _meta in iter_records(primary_paths, "primary"):
            if record is None:
                primary_json_errors += 1
                continue
            t = event_time(record)
            if not in_range_time(t, start, end):
                continue
            while patch_idx < len(patch_records):
                pt = event_time(patch_records[patch_idx])
                if t is not None and pt is not None and pt <= t:
                    write_record(out, patch_records[patch_idx])
                    output_records += 1
                    patch_written += 1
                    if is_l2(patch_records[patch_idx]) and pt:
                        merged_times.append(pt)
                    patch_idx += 1
                else:
                    break
            primary_out = clean_for_output(record)
            primary_out["merge_meta"] = {
                "source": "primary",
                "sources": ["primary"],
                "gap_filled": False,
                "source_meta": {"primary": record.get("_source_meta_tmp")},
            }
            write_record(out, primary_out)
            output_records += 1
            primary_records += 1
            if is_l2(primary_out) and t:
                primary_times.append(t)
                merged_times.append(t)

        while patch_idx < len(patch_records):
            pt = event_time(patch_records[patch_idx])
            write_record(out, patch_records[patch_idx])
            output_records += 1
            patch_written += 1
            if is_l2(patch_records[patch_idx]) and pt:
                merged_times.append(pt)
            patch_idx += 1

    return {
        "output_records": output_records,
        "primary_records": primary_records,
        "patch_written": patch_written,
        "primary_json_errors": primary_json_errors,
        "merged_times": merged_times,
        "primary_times": primary_times,
        "patch_times": patch_times,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--primary", nargs="+", required=True)
    parser.add_argument("--backup", nargs="+", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--gap-threshold", type=float, default=2.0)
    args = parser.parse_args()

    start = parse_time(args.start) if args.start else None
    end = parse_time(args.end) if args.end else None
    primary_paths = [Path(p) for p in args.primary]
    backup_paths = [Path(p) for p in args.backup]

    gaps, primary_scan = detect_gaps_from_paths(primary_paths, start, end, args.gap_threshold)
    patch_records, backup_stats = collect_backup_patches(backup_paths, start, end, gaps)
    out_path = Path(args.output)
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    write_stats = write_streaming_output(primary_paths, patch_records, start, end, out_path)
    merged_times = write_stats["merged_times"]
    primary_times = write_stats["primary_times"]
    patch_times = write_stats["patch_times"]

    report = {
        "start": args.start,
        "end": args.end,
        "gap_threshold": args.gap_threshold,
        "primary": {
            "records": write_stats["primary_records"],
            "l2updates": len(primary_times),
            "json_errors": primary_scan["json_errors"] + write_stats["primary_json_errors"],
            "gaps_detected": [{"start": iso(g["start"]), "end": iso(g["end"]), "seconds": g["seconds"]} for g in gaps],
            "gap_stats": gap_stats(primary_times, [1, 2, 5, 10, 30, 60]),
        },
        "backup": {
            "records": backup_stats["records"],
            "l2updates_seen": backup_stats["l2updates_seen"],
            "l2updates_inside_primary_gaps": backup_stats["l2updates_inside_primary_gaps"],
            "json_errors": backup_stats["json_errors"],
            "changes_seen_inside_gaps": backup_stats["changes_seen_inside_gaps"],
            "changes_kept_inside_gaps": backup_stats["changes_kept_inside_gaps"],
            "changes_deduped_inside_gaps": backup_stats["changes_deduped_inside_gaps"],
        },
        "merged": {
            "output": str(out_path),
            "records": write_stats["output_records"],
            "primary_records": write_stats["primary_records"],
            "backup_patch_records": write_stats["patch_written"],
            "l2updates": len(merged_times),
            "patch_l2updates": len(patch_times),
            "time_first": iso(min(merged_times)) if merged_times else None,
            "time_last": iso(max(merged_times)) if merged_times else None,
            "gap_stats": gap_stats(merged_times, [1, 2, 5, 10, 30, 60]),
        },
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(json.dumps(report["merged"], indent=2, ensure_ascii=False))
    print("report", report_path)


if __name__ == "__main__":
    main()
