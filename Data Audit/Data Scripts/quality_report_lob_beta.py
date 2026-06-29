#!/usr/bin/env python3
"""
Beta quality reporter for the LOB capture system.

Reads raw JSONL(.gz) files for Binance/Coinbase primary and backup streams,
checks continuity and data quality, and writes a JSON report. It never modifies
raw captures.
"""

import argparse
import gzip
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


SOURCES = {
    "binance": ("binance", "BTCUSDT"),
    "binance_backup": ("binance_backup", "BTCUSDT"),
    "coinbase": ("coinbase", "BTC-USD"),
    "coinbase_backup": ("coinbase_backup", "BTC-USD"),
}


def parse_iso(value: Optional[str]) -> Optional[datetime]:
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


def source_paths(root: Path, source: str, symbol: str) -> List[Path]:
    paths = []
    for base in ("raw_done", "raw_live"):
        folder = root / base / source / symbol
        if folder.exists():
            paths.extend(sorted(folder.glob("*.jsonl.gz")))
            paths.extend(sorted(folder.glob("*.jsonl")))
    return sorted(paths, key=lambda p: p.name)


def candidate_paths(paths: List[Path], start: Optional[datetime], end: Optional[datetime]) -> List[Path]:
    if start is None and end is None:
        return paths
    dates = set()
    if start:
        dates.add(start.strftime("%Y-%m-%d"))
    if end:
        dates.add(end.strftime("%Y-%m-%d"))
    if start and end:
        current = start.date()
        while current <= end.date():
            dates.add(current.isoformat())
            current = datetime.fromordinal(current.toordinal() + 1).date()
    selected = [p for p in paths if any(d in p.name for d in dates)]
    return selected or paths


def record_time(source: str, record: Dict[str, Any]) -> Optional[datetime]:
    data = record.get("data") or {}
    if source.startswith("binance"):
        return ms_time(data.get("E")) or parse_iso(record.get("recv_time"))
    return parse_iso(data.get("time")) or parse_iso(record.get("recv_time"))


def in_range(t: Optional[datetime], start: Optional[datetime], end: Optional[datetime]) -> bool:
    if t is None:
        return True
    if start and t < start:
        return False
    if end and t >= end:
        return False
    return True


def gap_summary(times: List[datetime], thresholds: List[float]) -> Dict[str, Any]:
    times = sorted(times)
    summary = {str(th): {"count": 0, "total_seconds": 0.0, "max_seconds": 0.0} for th in thresholds}
    examples = []
    nonmonotone = 0
    prev = None
    for t in times:
        if prev is not None:
            seconds = (t - prev).total_seconds()
            if seconds < -0.001:
                nonmonotone += 1
            for th in thresholds:
                if seconds >= th:
                    item = summary[str(th)]
                    item["count"] += 1
                    item["total_seconds"] += seconds
                    item["max_seconds"] = max(item["max_seconds"], seconds)
            if seconds >= 2 and len(examples) < 20:
                examples.append({"seconds": seconds, "from": iso(prev), "to": iso(t)})
        prev = t
    return {"thresholds": summary, "examples_ge_2s": examples, "nonmonotone": nonmonotone}


def status_from_gaps(
    source: str,
    json_errors: int,
    seq_gaps: int,
    temporal_ge_2: int,
    snapshots: int,
    updates: int,
    bad_segments: int,
) -> str:
    if json_errors:
        return "REJECT_CORRUPT_JSON"
    if updates == 0:
        return "NO_UPDATES"
    if source.startswith("binance") and seq_gaps:
        return "CAPTURE_GAP_UNRESOLVED"
    if source.startswith("binance") and bad_segments:
        return "DIAGNOSTIC_ONLY"
    if snapshots == 0 and updates == 0:
        return "REJECT_NO_SNAPSHOT"
    if temporal_ge_2:
        return "SEGMENTED"
    return "OK"


def analyze_source(root: Path, source: str, symbol: str, start: Optional[datetime], end: Optional[datetime]) -> Dict[str, Any]:
    paths = candidate_paths(source_paths(root, source, symbol), start, end)
    report: Dict[str, Any] = {
        "source": source,
        "symbol": symbol,
        "files": [],
        "totals": {
            "lines": 0,
            "json_errors": 0,
            "updates": 0,
            "snapshots": 0,
            "control_events": 0,
            "subscriptions": 0,
            "bytes": 0,
        },
        "controls": {},
        "time_first": None,
        "time_last": None,
        "status": "UNKNOWN",
    }

    all_times: List[datetime] = []
    seq_gaps = []
    prev_u = None
    prev_record_time = None
    segments = []
    current_segment = None
    controls = Counter()

    for path in paths:
        file_info = {
            "path": str(path),
            "bytes": path.stat().st_size,
            "lines": 0,
            "json_errors": 0,
            "updates": 0,
            "snapshots": 0,
            "control_events": 0,
            "time_first": None,
            "time_last": None,
        }
        report["totals"]["bytes"] += file_info["bytes"]
        with open_text(path) as f:
            for line_no, line in enumerate(f, 1):
                file_info["lines"] += 1
                report["totals"]["lines"] += 1
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    file_info["json_errors"] += 1
                    report["totals"]["json_errors"] += 1
                    continue

                t = record_time(source, record)
                if not in_range(t, start, end):
                    continue
                data = record.get("data") or {}
                kind = record.get("kind")
                msg_type = data.get("type")

                if kind == "control_event":
                    event = data.get("event", "unknown")
                    controls[event] += 1
                    file_info["control_events"] += 1
                    report["totals"]["control_events"] += 1
                    continue

                if msg_type == "subscriptions":
                    report["totals"]["subscriptions"] += 1
                    continue

                is_update = False
                if source.startswith("binance"):
                    is_update = kind == "ws_depth_update" or data.get("e") == "depthUpdate"
                    is_snapshot = kind == "rest_snapshot"
                else:
                    is_update = msg_type == "l2update"
                    is_snapshot = msg_type == "snapshot"

                if is_snapshot:
                    file_info["snapshots"] += 1
                    report["totals"]["snapshots"] += 1
                    if source.startswith("binance"):
                        if current_segment:
                            segments.append(current_segment)
                        current_segment = {
                            "snapshot_file": str(path),
                            "snapshot_line": line_no,
                            "snapshot_time": iso(t),
                            "snapshot_id": data.get("lastUpdateId"),
                            "updates": 0,
                            "bridges_snapshot": None,
                            "sequence_gaps": 0,
                        }
                        prev_u = None
                    continue

                if not is_update:
                    continue

                file_info["updates"] += 1
                report["totals"]["updates"] += 1
                if t:
                    all_times.append(t)
                    if file_info["time_first"] is None:
                        file_info["time_first"] = iso(t)
                    file_info["time_last"] = iso(t)

                if source.startswith("binance"):
                    U = data.get("U")
                    u = data.get("u")
                    if current_segment and U is not None and u is not None:
                        current_segment["updates"] += 1
                        if current_segment["bridges_snapshot"] is None:
                            sid = current_segment["snapshot_id"]
                            current_segment["bridges_snapshot"] = bool(sid is not None and U <= sid + 1 <= u)
                    if prev_u is not None and U is not None and U != prev_u + 1:
                        gap = {
                            "prev_u": prev_u,
                            "next_U": U,
                            "next_u": u,
                            "missing": U - prev_u - 1,
                            "time": iso(t),
                            "previous_time": iso(prev_record_time),
                            "file": str(path),
                            "line": line_no,
                        }
                        seq_gaps.append(gap)
                        if current_segment:
                            current_segment["sequence_gaps"] += 1
                    if u is not None:
                        prev_u = u
                        prev_record_time = t

        report["files"].append(file_info)

    if current_segment:
        segments.append(current_segment)
    if all_times:
        report["time_first"] = iso(min(all_times))
        report["time_last"] = iso(max(all_times))

    gap_info = gap_summary(all_times, [0.5, 1, 2, 5, 10, 30, 60])
    report["temporal_gaps"] = gap_info
    report["controls"] = dict(controls)
    bad_segments = []
    if source.startswith("binance"):
        report["sequence_gaps"] = {"count": len(seq_gaps), "examples": seq_gaps[:20]}
        report["segments"] = segments
        bad_segments = [s for s in segments if s.get("bridges_snapshot") is False or s.get("sequence_gaps")]
        report["bad_segments"] = bad_segments
    else:
        report["sequence_gaps"] = None
        report["segments"] = None
        report["bad_segments"] = []

    ge_2 = gap_info["thresholds"]["2"]["count"]
    seq_count = len(seq_gaps)
    report["status"] = status_from_gaps(
        source,
        report["totals"]["json_errors"],
        seq_count,
        ge_2,
        report["totals"]["snapshots"],
        report["totals"]["updates"],
        len(bad_segments),
    )
    report["status_rules"] = {
        "OK": "No JSON errors, no unresolved sequence gaps, and no temporal gaps above threshold.",
        "SEGMENTED": "Temporal gaps were observed; use by segments or attempt backup patch before reconstruction.",
        "CAPTURE_GAP_UNRESOLVED": "A Binance U/u sequence discontinuity was observed in the capture and is not patched in this raw report.",
        "DIAGNOSTIC_ONLY": "Data exists but at least one segment does not bridge correctly from its snapshot; keep for diagnostics, exclude from formal reconstruction.",
        "REJECT_CORRUPT_JSON": "At least one JSON line could not be parsed.",
        "REJECT_NO_SNAPSHOT": "No usable snapshot/update basis was found.",
    }
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/opt/lob_system")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    root = Path(args.root)
    start = parse_iso(args.start) if args.start else None
    end = parse_iso(args.end) if args.end else None
    report = {
        "created_at": iso(datetime.now(timezone.utc)),
        "root": str(root),
        "start": args.start,
        "end": args.end,
        "sources": {},
    }
    for source, (folder, symbol) in SOURCES.items():
        report["sources"][source] = analyze_source(root, folder, symbol, start, end)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    compact = {
        source: {
            "status": data["status"],
            "updates": data["totals"]["updates"],
            "snapshots": data["totals"]["snapshots"],
            "json_errors": data["totals"]["json_errors"],
            "time_first": data["time_first"],
            "time_last": data["time_last"],
            "gaps_ge_2s": data["temporal_gaps"]["thresholds"]["2"]["count"],
            "seq_gaps": data["sequence_gaps"]["count"] if data["sequence_gaps"] else None,
        }
        for source, data in report["sources"].items()
    }
    print(json.dumps(compact, indent=2, ensure_ascii=False))
    print("report", out)


if __name__ == "__main__":
    main()
