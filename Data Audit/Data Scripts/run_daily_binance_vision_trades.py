#!/usr/bin/env python3
"""Download yesterday's Binance Vision daily trades when available."""

import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/opt/lob_system")
    parser.add_argument("--script", default="/opt/lob_system/scripts/download_binance_vision_trades.py")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--date", help="Override date YYYY-MM-DD; default is yesterday UTC")
    args = parser.parse_args()

    day = args.date or (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
    cmd = [
        sys.executable,
        args.script,
        "--root",
        args.root,
        "--symbol",
        args.symbol,
        "--start-date",
        day,
        "--end-date",
        day,
        "--retries",
        "1",
        "--sleep",
        "10",
    ]
    completed = subprocess.run(cmd, text=True, capture_output=True)
    result = {
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "date": day,
        "symbol": args.symbol,
        "status": "ok" if completed.returncode == 0 else "failed",
        "returncode": completed.returncode,
        "stdout_tail": completed.stdout[-4000:],
        "stderr_tail": completed.stderr[-4000:],
    }
    out_dir = Path(args.root) / "trades_backfill" / "binance" / args.symbol.upper()
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "daily_binance_vision_trades_run_summary.json"
    out.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({"summary": str(out), **result}, indent=2))
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


if __name__ == "__main__":
    main()
