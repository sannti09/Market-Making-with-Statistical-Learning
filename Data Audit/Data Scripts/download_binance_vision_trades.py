#!/usr/bin/env python3
"""Download Binance Vision daily Spot trades and verify checksums."""

import argparse
import hashlib
import json
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List


BASE_URL = "https://data.binance.vision/data/spot/daily/trades"


def parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def download(url: str, path: Path, timeout: int = 60) -> Dict:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with urllib.request.urlopen(url, timeout=timeout) as response:
        tmp.write_bytes(response.read())
    tmp.replace(path)
    return {"url": url, "path": str(path), "bytes": path.stat().st_size}


def checksum_hash(checksum_text: str) -> str:
    return checksum_text.strip().split()[0]


def date_range(start: datetime, end: datetime):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/opt/lob_system")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--sleep", type=float, default=5.0)
    args = parser.parse_args()

    symbol = args.symbol.upper()
    start = parse_date(args.start_date)
    end = parse_date(args.end_date)
    out_dir = Path(args.root) / "trades_backfill" / "binance" / symbol
    out_dir.mkdir(parents=True, exist_ok=True)

    results: List[Dict] = []
    for day in date_range(start, end):
        day_s = day.strftime("%Y-%m-%d")
        filename = f"{symbol}-trades-{day_s}.zip"
        url = f"{BASE_URL}/{symbol}/{filename}"
        checksum_url = f"{url}.CHECKSUM"
        zip_path = out_dir / filename
        checksum_path = out_dir / f"{filename}.CHECKSUM"

        item = {
            "date": day_s,
            "symbol": symbol,
            "zip_path": str(zip_path),
            "checksum_path": str(checksum_path),
            "url": url,
            "checksum_url": checksum_url,
        }
        try:
            for attempt in range(1, args.retries + 2):
                try:
                    if not zip_path.exists():
                        download(url, zip_path)
                    if not checksum_path.exists():
                        download(checksum_url, checksum_path)
                    break
                except urllib.error.HTTPError as exc:
                    if attempt > args.retries + 1:
                        raise
                    item["last_http_error"] = exc.code
                    time.sleep(args.sleep)
                except urllib.error.URLError as exc:
                    if attempt > args.retries + 1:
                        raise
                    item["last_url_error"] = str(exc)
                    time.sleep(args.sleep)

            expected = checksum_hash(checksum_path.read_text(encoding="utf-8"))
            actual = sha256_file(zip_path)
            item.update({
                "status": "ok" if expected == actual else "checksum_mismatch",
                "bytes": zip_path.stat().st_size,
                "sha256": actual,
                "expected_sha256": expected,
            })
        except urllib.error.HTTPError as exc:
            item.update({"status": "http_error", "http_status": exc.code})
        except Exception as exc:
            item.update({"status": "error", "error": repr(exc)})
        results.append(item)
        print(json.dumps(item, ensure_ascii=False))

    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": "binance_vision",
        "dataset": "spot_daily_trades",
        "symbol": symbol,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "results": results,
    }
    manifest_path = out_dir / f"binance_vision_trades_{args.start_date}_{args.end_date}_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    ok = sum(1 for item in results if item.get("status") == "ok")
    print(json.dumps({"manifest": str(manifest_path), "ok": ok, "total": len(results)}, indent=2))


if __name__ == "__main__":
    main()
