#!/usr/bin/env python3
"""
Experimental Coinbase primary/backup merger.

This script does not modify raw captures. It creates a separate merged JSONL.GZ
stream plus a quality report. The first beta deduplicates exact l2update batches
by product_id + exchange time + hash(changes), orders by exchange time, and marks
whether a message came from primary, backup, or both.
"""

import argparse
import gzip
import hashlib
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
    if dt is None:
        return None
    return dt.isoformat().replace("+00:00", "Z")


def open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "rt", encoding="utf-8")


def canonical_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def event_time(record: Dict[str, Any]) -> Optional[datetime]:
    data = record.get("data") or {}
    if data.get("time"):
        return parse_time(data.get("time"))
    return parse_time(record.get("recv_time"))


def dedup_key(record: Dict[str, Any]) -> Tuple[str, str, str, str]:
    data = record.get("data") or {}
    msg_type = data.get("type") or record.get("kind") or "unknown"
    product_id = data.get("product_id") or record.get("symbol") or ""
    t = data.get("time") or record.get("recv_time") or ""
    if msg_type == "l2update":
        payload_hash = canonical_hash(data.get("changes", []))
    elif msg_type == "snapshot":
        payload_hash = canonical_hash({
            "bids_top": (data.get("bids") or [])[:10],
            "asks_top": (data.get("asks") or [])[:10],
            "bids_len": len(data.get("bids") or []),
            "asks_len": len(data.get("asks") or []),
        })
    else:
        payload_hash = canonical_hash(data)
    return (msg_type, product_id, t, payload_hash)


def iter_records(paths: Iterable[Path], source: str):
    for path in paths:
        with open_text(path) as f:
            for line_no, line in enumerate(f, 1):
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    yield None, {"source": source, "path": str(path), "line": line_no}
                    continue
                data = record.get("data") or {}
                msg_type = data.get("type")
                kind = record.get("kind")
                if msg_type not in {"snapshot", "l2update", "subscriptions"} and kind != "control_event":
                    continue
                yield record, {"source": source, "path": str(path), "line": line_no}


def gap_stats(times: List[datetime], thresholds: List[float]) -> Dict[str, Any]:
    out = {str(th): {"count": 0, "total_seconds": 0.0, "max_seconds": 0.0} for th in thresholds}
    examples = []
    prev = None
    nonmono = 0
    for t in sorted(times):
        if prev is not None:
            gap = (t - prev).total_seconds()
            if gap < -0.001:
                nonmono += 1
            for th in thresholds:
                if gap >= th:
                    item = out[str(th)]
                    item["count"] += 1
                    item["total_seconds"] += gap
                    item["max_seconds"] = max(item["max_seconds"], gap)
            if gap >= 2 and len(examples) < 20:
                examples.append({"seconds": gap, "from": iso(prev), "to": iso(t)})
        prev = t
    return {"thresholds": out, "examples_ge_2s": examples, "nonmonotone": nonmono}


def load_stream(paths: List[Path], source: str, start: Optional[datetime], end: Optional[datetime]):
    records = {}
    stats = {
        "source": source,
        "files": [str(p) for p in paths],
        "json_errors": 0,
        "records_seen": 0,
        "l2updates": 0,
        "snapshots": 0,
        "controls": 0,
        "subscriptions": 0,
        "time_first": None,
        "time_last": None,
    }
    times: List[datetime] = []
    for record, err in iter_records(paths, source):
        if record is None:
            stats["json_errors"] += 1
            continue
        t = event_time(record)
        if t is not None:
            if start and t < start:
                continue
            if end and t > end:
                continue
        stats["records_seen"] += 1
        data = record.get("data") or {}
        msg_type = data.get("type")
        if msg_type == "l2update":
            stats["l2updates"] += 1
            if t:
                times.append(t)
        elif msg_type == "snapshot":
            stats["snapshots"] += 1
        elif msg_type == "subscriptions":
            stats["subscriptions"] += 1
        elif record.get("kind") == "control_event":
            stats["controls"] += 1
        key = dedup_key(record)
        records[key] = {"record": record, "source_meta": err}
    if times:
        stats["time_first"] = iso(min(times))
        stats["time_last"] = iso(max(times))
    stats["gaps"] = gap_stats(times, [1, 2, 5, 10, 30, 60])
    return records, stats, times


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--primary", nargs="+", required=True)
    parser.add_argument("--backup", nargs="+", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()

    start = parse_time(args.start) if args.start else None
    end = parse_time(args.end) if args.end else None
    primary_paths = [Path(p) for p in args.primary]
    backup_paths = [Path(p) for p in args.backup]

    primary, primary_stats, primary_times = load_stream(primary_paths, "primary", start, end)
    backup, backup_stats, backup_times = load_stream(backup_paths, "backup", start, end)

    merged: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for key, item in primary.items():
        merged[key] = {
            "record": item["record"],
            "sources": {"primary"},
            "source_meta": {"primary": item["source_meta"]},
        }
    for key, item in backup.items():
        if key in merged:
            merged[key]["sources"].add("backup")
            merged[key]["source_meta"]["backup"] = item["source_meta"]
        else:
            merged[key] = {
                "record": item["record"],
                "sources": {"backup"},
                "source_meta": {"backup": item["source_meta"]},
            }

    def sort_key(item):
        _key, payload = item
        t = event_time(payload["record"]) or datetime.min
        data = payload["record"].get("data") or {}
        type_rank = {"subscriptions": 0, "snapshot": 1, "l2update": 2}.get(data.get("type"), 3)
        source_rank = 0 if "primary" in payload["sources"] else 1
        return (t, type_rank, source_rank)

    out_path = Path(args.output)
    report_path = Path(args.report)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    merged_times: List[datetime] = []
    source_counts = {"primary_only": 0, "backup_only": 0, "both": 0}
    with gzip.open(out_path, "wt", encoding="utf-8", compresslevel=6) as out:
        for _key, payload in sorted(merged.items(), key=sort_key):
            record = dict(payload["record"])
            sources = sorted(payload["sources"])
            if sources == ["primary"]:
                source_counts["primary_only"] += 1
            elif sources == ["backup"]:
                source_counts["backup_only"] += 1
            else:
                source_counts["both"] += 1
            record["merge_meta"] = {
                "sources": sources,
                "source": "both" if len(sources) == 2 else sources[0],
                "source_meta": payload["source_meta"],
            }
            data = record.get("data") or {}
            if data.get("type") == "l2update":
                t = event_time(record)
                if t:
                    merged_times.append(t)
            out.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n")

    report = {
        "start": args.start,
        "end": args.end,
        "primary": primary_stats,
        "backup": backup_stats,
        "merged": {
            "output": str(out_path),
            "records": len(merged),
            "l2updates": len(merged_times),
            "time_first": iso(min(merged_times)) if merged_times else None,
            "time_last": iso(max(merged_times)) if merged_times else None,
            "source_counts": source_counts,
            "gaps": gap_stats(merged_times, [1, 2, 5, 10, 30, 60]),
        },
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(json.dumps(report["merged"], indent=2, ensure_ascii=False))
    print("report", report_path)


if __name__ == "__main__":
    main()
