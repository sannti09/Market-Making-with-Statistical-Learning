#!/usr/bin/env python3
"""Run daily manifest generation for available quality reports."""

import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/opt/lob_system")
    parser.add_argument("--script", default="/opt/lob_system/scripts/daily_manifest_lob_beta.py")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--skip-today", action="store_true")
    parser.add_argument("--no-hash", action="store_true")
    args = parser.parse_args()

    root = Path(args.root)
    qdir = root / "reports" / "quality_beta"
    dates = sorted(p.stem.replace("quality_", "") for p in qdir.glob("quality_????-??-??.json"))
    if args.start_date:
        dates = [d for d in dates if d >= args.start_date]
    if args.end_date:
        dates = [d for d in dates if d <= args.end_date]
    if args.skip_today:
        today = datetime.now(timezone.utc).date().isoformat()
        dates = [d for d in dates if d < today]

    results = []
    for day in dates:
        cmd = [sys.executable, args.script, "--root", args.root, "--date", day]
        if args.no_hash:
            cmd.append("--no-hash")
        completed = subprocess.run(cmd, text=True, capture_output=True)
        results.append({
            "date": day,
            "status": "ok" if completed.returncode == 0 else "failed",
            "returncode": completed.returncode,
            "stdout_tail": completed.stdout[-2000:],
            "stderr_tail": completed.stderr[-2000:],
        })

    out = root / "manifests" / "daily_manifest_run_summary.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump({"created_at": datetime.now(timezone.utc).isoformat(), "results": results}, f, indent=2)
    print(json.dumps({"summary": str(out), "results": results}, indent=2))


if __name__ == "__main__":
    main()
