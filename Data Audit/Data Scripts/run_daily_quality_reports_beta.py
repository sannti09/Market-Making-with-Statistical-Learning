#!/usr/bin/env python3
"""
Run daily LOB quality reports over all available raw capture dates.

This wrapper calls quality_report_lob_beta.py once per UTC day. It only writes
JSON reports under reports/quality_beta and never modifies raw captures.
"""

import argparse
import json
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Set


RAW_SOURCES = [
    ("raw_done", "binance", "BTCUSDT"),
    ("raw_live", "binance", "BTCUSDT"),
    ("raw_done", "coinbase", "BTC-USD"),
    ("raw_live", "coinbase", "BTC-USD"),
    ("raw_done", "binance_backup", "BTCUSDT"),
    ("raw_live", "binance_backup", "BTCUSDT"),
    ("raw_done", "coinbase_backup", "BTC-USD"),
    ("raw_live", "coinbase_backup", "BTC-USD"),
]


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def discover_dates(root: Path) -> List[date]:
    dates: Set[date] = set()
    for base, source, symbol in RAW_SOURCES:
        folder = root / base / source / symbol
        if not folder.exists():
            continue
        for path in folder.glob("*.jsonl*"):
            for part in path.name.split("_"):
                try:
                    if len(part) == 10 and part[4] == "-" and part[7] == "-":
                        dates.add(parse_date(part))
                except ValueError:
                    pass
    return sorted(dates)


def run_report(script: Path, root: Path, out_dir: Path, day: date, force: bool) -> dict:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    out = out_dir / f"quality_{day.isoformat()}.json"
    if out.exists() and not force:
        return {"date": day.isoformat(), "status": "skipped_exists", "output": str(out)}

    cmd = [
        sys.executable,
        str(script),
        "--root",
        str(root),
        "--start",
        start.isoformat().replace("+00:00", "Z"),
        "--end",
        end.isoformat().replace("+00:00", "Z"),
        "--output",
        str(out),
    ]
    completed = subprocess.run(cmd, text=True, capture_output=True)
    return {
        "date": day.isoformat(),
        "status": "ok" if completed.returncode == 0 else "failed",
        "returncode": completed.returncode,
        "output": str(out),
        "stdout_tail": completed.stdout[-4000:],
        "stderr_tail": completed.stderr[-4000:],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/opt/lob_system")
    parser.add_argument("--script", default="/opt/lob_system/scripts/quality_report_lob_beta.py")
    parser.add_argument("--output-dir", default="/opt/lob_system/reports/quality_beta")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--skip-today", action="store_true", help="Only report fully closed UTC days")
    args = parser.parse_args()

    root = Path(args.root)
    script = Path(args.script)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    dates = discover_dates(root)
    if args.start_date:
        start = parse_date(args.start_date)
    elif dates:
        start = dates[0]
    else:
        raise SystemExit("No raw dates found")

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
        results.append(run_report(script, root, out_dir, day, args.force))

    summary_path = out_dir / "daily_quality_run_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "root": str(root),
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "force": args.force,
                "results": results,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    print(json.dumps({"summary": str(summary_path), "results": results}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
