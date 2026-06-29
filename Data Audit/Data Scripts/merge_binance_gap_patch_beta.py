#!/usr/bin/env python3
"""
Experimental conservative Binance primary/backup merger.

Primary is the base stream. Backup is used only when a primary sequence gap is
detected via Binance depthUpdate U/u identifiers. Raw captures are never modified.
"""

import argparse
import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def parse_recv(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def ms_time(value: Optional[int]) -> Optional[datetime]:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, timezone.utc)


def iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat().replace("+00:00", "Z") if dt else None


def open_text(path: Path):
    return gzip.open(path, "rt", encoding="utf-8") if path.suffix == ".gz" else open(path, "rt", encoding="utf-8")


def is_depth(record: Dict[str, Any]) -> bool:
    data = record.get("data") or {}
    return record.get("kind") == "ws_depth_update" or data.get("e") == "depthUpdate"


def event_time(record: Dict[str, Any]) -> Optional[datetime]:
    data = record.get("data") or {}
    return ms_time(data.get("E")) or parse_recv(record.get("recv_time"))


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
    for record, _meta in iter_records(paths, source):
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


def depth_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = [r for r in records if is_depth(r)]
    out.sort(key=lambda r: ((r.get("data") or {}).get("U", -1), (r.get("data") or {}).get("u", -1)))
    return out


def detect_sequence_gaps(records: List[Dict[str, Any]]):
    gaps = []
    prev = None
    for record in depth_records(records):
        data = record.get("data") or {}
        U = data.get("U")
        u = data.get("u")
        if U is None or u is None:
            continue
        if prev is not None and U != prev["u"] + 1:
            gaps.append(
                {
                    "missing_from": prev["u"] + 1,
                    "missing_to": U - 1,
                    "prev_u": prev["u"],
                    "next_U": U,
                    "next_u": u,
                    "prev_time": prev["time"],
                    "next_time": event_time(record),
                }
            )
        prev = {"u": u, "time": event_time(record)}
    return gaps


def covers_gap(record: Dict[str, Any], gap: Dict[str, Any]) -> bool:
    data = record.get("data") or {}
    U = data.get("U")
    u = data.get("u")
    if U is None or u is None:
        return False
    return not (u < gap["missing_from"] or U > gap["missing_to"])


def clean_for_output(record: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(record)
    out.pop("_source_meta_tmp", None)
    return out


def sequence_gap_stats(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    gaps = detect_sequence_gaps(records)
    return {
        "count": len(gaps),
        "examples": [
            {
                "missing_from": g["missing_from"],
                "missing_to": g["missing_to"],
                "prev_time": iso(g["prev_time"]),
                "next_time": iso(g["next_time"]),
            }
            for g in gaps[:20]
        ],
    }


def temporal_gap_stats(records: List[Dict[str, Any]], thresholds: List[float]) -> Dict[str, Any]:
    times = [event_time(r) for r in records if is_depth(r) and event_time(r) is not None]
    times.sort()
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
            if seconds >= 1 and len(examples) < 20:
                examples.append({"seconds": seconds, "from": iso(prev), "to": iso(t)})
        prev = t
    return {"thresholds": stats, "examples_ge_1s": examples}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--primary", nargs="+", required=True)
    parser.add_argument("--backup", nargs="+", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()

    start = parse_recv(args.start) if args.start else None
    end = parse_recv(args.end) if args.end else None
    primary_records, primary_json_errors = load_records([Path(p) for p in args.primary], "primary", start, end)
    backup_records, backup_json_errors = load_records([Path(p) for p in args.backup], "backup", start, end)
    primary_gaps = detect_sequence_gaps(primary_records)

    backup_by_range = {}
    for record in depth_records(backup_records):
        data = record.get("data") or {}
        backup_by_range[(data.get("U"), data.get("u"))] = record

    patch_records = []
    patched_gap_reports = []
    for gap in primary_gaps:
        candidates = []
        for record in backup_by_range.values():
            if covers_gap(record, gap):
                candidates.append(record)
        candidates.sort(key=lambda r: ((r.get("data") or {}).get("U", -1), (r.get("data") or {}).get("u", -1)))
        expected = gap["missing_from"]
        kept = []
        failed = False
        for record in candidates:
            data = record.get("data") or {}
            U = data.get("U")
            u = data.get("u")
            if U != expected:
                failed = True
                break
            patched = clean_for_output(record)
            patched["merge_meta"] = {
                "source": "backup_patch",
                "sources": ["backup"],
                "gap_filled": True,
                "missing_from": gap["missing_from"],
                "missing_to": gap["missing_to"],
                "source_meta": {"backup": record.get("_source_meta_tmp")},
            }
            kept.append(patched)
            expected = u + 1
            if expected > gap["missing_to"]:
                break
        complete = expected > gap["missing_to"] and not failed
        if complete:
            patch_records.extend(kept)
        patched_gap_reports.append(
            {
                "missing_from": gap["missing_from"],
                "missing_to": gap["missing_to"],
                "complete": complete,
                "patch_records": len(kept) if complete else 0,
                "candidate_records": len(candidates),
                "prev_time": iso(gap["prev_time"]),
                "next_time": iso(gap["next_time"]),
            }
        )

    output_records = []
    for record in primary_records:
        out = clean_for_output(record)
        out["merge_meta"] = {
            "source": "primary",
            "sources": ["primary"],
            "gap_filled": False,
            "source_meta": {"primary": record.get("_source_meta_tmp")},
        }
        output_records.append(out)
    output_records.extend(patch_records)
    output_records.sort(key=lambda r: ((r.get("data") or {}).get("U", -1), (r.get("data") or {}).get("u", -1), event_time(r) or datetime.min))

    out_path = Path(args.output)
    report_path = Path(args.report)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out_path, "wt", encoding="utf-8", compresslevel=6) as out:
        for record in output_records:
            out.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n")

    primary_depth = depth_records(primary_records)
    backup_depth = depth_records(backup_records)
    merged_depth = depth_records(output_records)
    report = {
        "start": args.start,
        "end": args.end,
        "primary": {
            "records": len(primary_records),
            "depth_updates": len(primary_depth),
            "json_errors": primary_json_errors,
            "sequence_gaps": sequence_gap_stats(primary_records),
            "temporal_gaps": temporal_gap_stats(primary_records, [0.5, 1, 2, 5, 10]),
        },
        "backup": {
            "records": len(backup_records),
            "depth_updates": len(backup_depth),
            "json_errors": backup_json_errors,
            "sequence_gaps": sequence_gap_stats(backup_records),
            "temporal_gaps": temporal_gap_stats(backup_records, [0.5, 1, 2, 5, 10]),
        },
        "patch": {
            "primary_sequence_gaps_detected": len(primary_gaps),
            "backup_patch_records": len(patch_records),
            "gaps": patched_gap_reports,
        },
        "merged": {
            "output": str(out_path),
            "records": len(output_records),
            "depth_updates": len(merged_depth),
            "sequence_gaps": sequence_gap_stats(output_records),
            "temporal_gaps": temporal_gap_stats(output_records, [0.5, 1, 2, 5, 10]),
        },
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(json.dumps(report["merged"], indent=2, ensure_ascii=False))
    print("report", report_path)


if __name__ == "__main__":
    main()
