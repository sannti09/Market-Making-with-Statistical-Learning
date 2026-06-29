#!/usr/bin/env python3
"""
Run conservative daily gap patches from quality reports.

This wrapper reads daily quality JSON reports and, when a primary source is
segmented and backup data exists, runs the corresponding conservative gap-patch
merger. It writes derived files under merged_beta and reports/merge_beta. It never
modifies raw captures.
"""

import argparse
import json
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional


CONFIG = {
    "binance": {
        "symbol": "BTCUSDT",
        "backup": "binance_backup",
        "script": "merge_binance_gap_patch_beta.py",
        "out_symbol": "BTCUSDT",
        "product_label": "BTCUSDT",
    },
    "coinbase": {
        "symbol": "BTC-USD",
        "backup": "coinbase_backup",
        "script": "merge_coinbase_gap_patch_beta.py",
        "out_symbol": "BTC-USD",
        "product_label": "BTC-USD",
    },
}


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def discover_report_dates(report_dir: Path) -> List[date]:
    dates = []
    for path in sorted(report_dir.glob("quality_????-??-??.json")):
        stem = path.stem.replace("quality_", "")
        try:
            dates.append(parse_date(stem))
        except ValueError:
            continue
    return sorted(set(dates))


def raw_paths(root: Path, source: str, symbol: str, day: date) -> List[Path]:
    paths: List[Path] = []
    day_str = day.isoformat()
    for base in ("raw_done", "raw_live"):
        folder = root / base / source / symbol
        if not folder.exists():
            continue
        paths.extend(sorted(folder.glob(f"*{day_str}*.jsonl.gz")))
        paths.extend(sorted(folder.glob(f"*{day_str}*.jsonl")))
    return sorted(set(paths), key=lambda p: p.name)


def should_patch(exchange: str, primary: Dict, backup: Dict) -> bool:
    if primary.get("status") not in {"SEGMENTED", "CAPTURE_GAP_UNRESOLVED"}:
        return False
    if (primary.get("totals") or {}).get("updates", 0) <= 0:
        return False
    if (backup.get("totals") or {}).get("updates", 0) <= 0:
        return False
    if exchange == "binance":
        seq = primary.get("sequence_gaps")
        return isinstance(seq, dict) and int(seq.get("count") or 0) > 0
    return True


def run_patch(
    root: Path,
    scripts_dir: Path,
    merged_dir: Path,
    merge_report_dir: Path,
    exchange: str,
    day: date,
    quality: Dict,
    force: bool,
) -> Dict:
    cfg = CONFIG[exchange]
    backup = cfg["backup"]
    symbol = cfg["symbol"]
    q_primary = quality["sources"].get(exchange, {})
    q_backup = quality["sources"].get(backup, {})

    result = {
        "date": day.isoformat(),
        "exchange": exchange,
        "primary_status": q_primary.get("status"),
        "backup_status": q_backup.get("status"),
        "status": "skipped",
        "reason": "",
    }
    if not should_patch(exchange, q_primary, q_backup):
        result["reason"] = "primary not patchable or backup unavailable"
        return result

    primary_paths = raw_paths(root, exchange, symbol, day)
    backup_paths = raw_paths(root, backup, symbol, day)
    if not primary_paths or not backup_paths:
        result["reason"] = "missing primary or backup files"
        result["primary_paths"] = [str(p) for p in primary_paths]
        result["backup_paths"] = [str(p) for p in backup_paths]
        return result

    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    start_s = start.isoformat().replace("+00:00", "Z")
    end_s = end.isoformat().replace("+00:00", "Z")

    out_dir = merged_dir / exchange / cfg["out_symbol"]
    rep_dir = merge_report_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{exchange}_{cfg['product_label']}_{day.isoformat()}_gap_patch.jsonl.gz"
    report = rep_dir / f"{exchange}_{cfg['product_label']}_{day.isoformat()}_gap_patch_report.json"
    if out.exists() and report.exists() and not force:
        result.update({"status": "skipped_exists", "output": str(out), "report": str(report)})
        return result

    script = scripts_dir / cfg["script"]
    cmd = [
        sys.executable,
        str(script),
        "--primary",
        *[str(p) for p in primary_paths],
        "--backup",
        *[str(p) for p in backup_paths],
        "--start",
        start_s,
        "--end",
        end_s,
        "--output",
        str(out),
        "--report",
        str(report),
    ]
    completed = subprocess.run(cmd, text=True, capture_output=True)
    result.update(
        {
            "status": "ok" if completed.returncode == 0 else "failed",
            "returncode": completed.returncode,
            "output": str(out),
            "report": str(report),
            "primary_paths": [str(p) for p in primary_paths],
            "backup_paths": [str(p) for p in backup_paths],
            "stdout_tail": completed.stdout[-4000:],
            "stderr_tail": completed.stderr[-4000:],
        }
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/opt/lob_system")
    parser.add_argument("--scripts-dir", default="/opt/lob_system/scripts")
    parser.add_argument("--quality-dir", default="/opt/lob_system/reports/quality_beta")
    parser.add_argument("--merged-dir", default="/opt/lob_system/merged_beta")
    parser.add_argument("--merge-report-dir", default="/opt/lob_system/reports/merge_beta")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--skip-today", action="store_true")
    args = parser.parse_args()

    root = Path(args.root)
    scripts_dir = Path(args.scripts_dir)
    quality_dir = Path(args.quality_dir)
    merged_dir = Path(args.merged_dir)
    merge_report_dir = Path(args.merge_report_dir)

    dates = discover_report_dates(quality_dir)
    if args.start_date:
        start = parse_date(args.start_date)
    elif dates:
        start = dates[0]
    else:
        raise SystemExit("No quality reports found")

    if args.end_date:
        end = parse_date(args.end_date)
    elif dates:
        end = dates[-1]
    else:
        end = start

    if args.skip_today:
        today_utc = datetime.now(timezone.utc).date()
        end = min(end, today_utc - timedelta(days=1))

    results = []
    for day in date_range(start, end):
        q_path = quality_dir / f"quality_{day.isoformat()}.json"
        if not q_path.exists():
            results.append({"date": day.isoformat(), "status": "skipped", "reason": "missing quality report"})
            continue
        with open(q_path, encoding="utf-8") as f:
            quality = json.load(f)
        for exchange in ("binance", "coinbase"):
            results.append(run_patch(root, scripts_dir, merged_dir, merge_report_dir, exchange, day, quality, args.force))

    summary = {
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "force": args.force,
        "results": results,
    }
    summary_path = merge_report_dir / "daily_gap_patch_run_summary.json"
    merge_report_dir.mkdir(parents=True, exist_ok=True)
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(json.dumps({"summary": str(summary_path), "results": results}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
