#!/usr/bin/env python3
"""
Live raw LOB capture for Coinbase Exchange and Binance Spot.

This script intentionally captures raw market data first and leaves order book
reconstruction to later offline jobs. It writes active JSONL files, rotates on
fixed UTC windows, and compresses closed files to .jsonl.gz.
"""

import argparse
import asyncio
import gzip
import json
import logging
import os
import shutil
import signal
import sys
import threading
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException


STOP = asyncio.Event()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat(timespec="microseconds").replace("+00:00", "Z")


def safe_symbol(symbol: str) -> str:
    return symbol.replace("/", "-").replace("_", "-")


def setup_logging(log_dir: Path, name: str, verbose: bool) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    level = logging.DEBUG if verbose else logging.INFO
    fmt = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s", "%Y-%m-%dT%H:%M:%S")
    root = logging.getLogger()
    root.setLevel(level)

    stdout = logging.StreamHandler(sys.stdout)
    stdout.setFormatter(fmt)
    stdout.setLevel(level)
    root.addHandler(stdout)

    file_handler = logging.FileHandler(log_dir / f"{name}.log")
    file_handler.setFormatter(fmt)
    file_handler.setLevel(level)
    root.addHandler(file_handler)


def window_bounds(now: datetime, rotate_hours: int) -> Tuple[datetime, datetime]:
    if 24 % rotate_hours != 0:
        raise ValueError("--rotate-hours must divide 24")
    start_hour = (now.hour // rotate_hours) * rotate_hours
    start = now.replace(hour=start_hour, minute=0, second=0, microsecond=0)
    end = start + timedelta(hours=rotate_hours)
    return start, end


def window_label(start: datetime, end: datetime) -> str:
    return f"{start:%Y-%m-%d}_{start:%H}-{end:%H}"


def gzip_file(src: Path, dst: Path) -> None:
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    logging.info("compressing src=%s dst=%s", src, dst)
    with open(src, "rb") as inp, gzip.open(tmp, "wb", compresslevel=6) as out:
        shutil.copyfileobj(inp, out, length=1024 * 1024)
    tmp.replace(dst)
    src.unlink()
    logging.info("compressed ok dst=%s bytes=%s", dst, dst.stat().st_size)


class RotatingJsonlWriter:
    def __init__(self, root: Path, exchange: str, symbol: str, rotate_hours: int):
        self.root = root
        self.exchange = exchange
        self.symbol = safe_symbol(symbol)
        self.rotate_hours = rotate_hours
        self.raw_live = root / "raw_live" / exchange / self.symbol
        self.raw_done = root / "raw_done" / exchange / self.symbol
        self.raw_live.mkdir(parents=True, exist_ok=True)
        self.raw_done.mkdir(parents=True, exist_ok=True)
        self.current_start: Optional[datetime] = None
        self.current_end: Optional[datetime] = None
        self.current_path: Optional[Path] = None
        self.file = None
        self.lock = threading.Lock()
        self.compress_threads = []
        self.compress_old_live_files()

    def compress_old_live_files(self) -> None:
        now_start, _ = window_bounds(utc_now(), self.rotate_hours)
        current_prefix = f"{self.exchange}_{self.symbol}_{now_start:%Y-%m-%d}_{now_start:%H}-"
        for path in self.raw_live.glob("*.jsonl"):
            if not path.name.startswith(current_prefix):
                dst = self.raw_done / (path.name + ".gz")
                if dst.exists():
                    dst = self.raw_done / f"{path.stem}_{int(time.time())}.jsonl.gz"
                thread = threading.Thread(target=gzip_file, args=(path, dst), daemon=True)
                thread.start()
                self.compress_threads.append(thread)

    def _open_for_now(self) -> None:
        now = utc_now()
        start, end = window_bounds(now, self.rotate_hours)
        if self.file and self.current_start == start:
            return

        old_path = self.current_path
        if self.file:
            self.file.flush()
            os.fsync(self.file.fileno())
            self.file.close()
            self.file = None

        self.current_start, self.current_end = start, end
        label = window_label(start, end)
        self.current_path = self.raw_live / f"{self.exchange}_{self.symbol}_{label}.jsonl"
        self.file = open(self.current_path, "a", encoding="utf-8", buffering=1)
        logging.info("active_file path=%s window_start=%s window_end=%s", self.current_path, start, end)

        if old_path and old_path.exists() and old_path != self.current_path:
            dst = self.raw_done / (old_path.name + ".gz")
            if dst.exists():
                dst = self.raw_done / f"{old_path.stem}_{int(time.time())}.jsonl.gz"
            thread = threading.Thread(target=gzip_file, args=(old_path, dst), daemon=True)
            thread.start()
            self.compress_threads.append(thread)

    def write(self, record: Dict[str, Any]) -> None:
        with self.lock:
            self._open_for_now()
            self.file.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n")

    def flush(self) -> None:
        with self.lock:
            if self.file:
                self.file.flush()

    def close(self) -> None:
        with self.lock:
            if self.file:
                self.file.flush()
                os.fsync(self.file.fileno())
                self.file.close()
                self.file = None
        for thread in self.compress_threads:
            thread.join(timeout=120)


def envelope(exchange: str, symbol: str, channel: str, kind: str, data: Any) -> Dict[str, Any]:
    return {
        "recv_time": iso_now(),
        "recv_ns": time.time_ns(),
        "exchange": exchange,
        "symbol": symbol,
        "channel": channel,
        "kind": kind,
        "data": data,
    }


def binance_snapshot(symbol: str, limit: int) -> Dict[str, Any]:
    url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit={limit}"
    with urllib.request.urlopen(url, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


async def capture_binance(args: argparse.Namespace, writer: RotatingJsonlWriter) -> None:
    symbol = args.symbol.upper()
    channel = f"depth@{args.binance_interval}"
    url = f"wss://data-stream.binance.vision/ws/{symbol.lower()}@depth@{args.binance_interval}"
    async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=None) as ws:
        logging.info("binance connected url=%s", url)
        snapshot_task = asyncio.create_task(asyncio.to_thread(binance_snapshot, symbol, args.binance_snapshot_limit))
        buffered = []

        while not snapshot_task.done():
            raw = await asyncio.wait_for(ws.recv(), timeout=args.idle_timeout)
            data = json.loads(raw)
            buffered.append(envelope("binance", symbol, channel, "ws_depth_update", data))

        snapshot = await snapshot_task
        last_update_id = snapshot.get("lastUpdateId")
        writer.write(envelope("binance", symbol, channel, "rest_snapshot", snapshot))
        logging.info(
            "binance snapshot lastUpdateId=%s bids=%s asks=%s buffered=%s",
            last_update_id,
            len(snapshot.get("bids", [])),
            len(snapshot.get("asks", [])),
            len(buffered),
        )

        def find_binance_start(records):
            for idx, record in enumerate(records):
                data = record.get("data", {})
                U = data.get("U")
                u = data.get("u")
                if u is not None and last_update_id is not None and u <= last_update_id:
                    continue
                if (
                    U is not None
                    and u is not None
                    and last_update_id is not None
                    and U <= last_update_id + 1 <= u
                ):
                    return idx
                raise RuntimeError(
                    f"Binance sequence gap after snapshot: lastUpdateId={last_update_id} first_U={U} first_u={u}"
                )
            return None

        start_index = find_binance_start(buffered)
        while start_index is None:
            raw = await asyncio.wait_for(ws.recv(), timeout=args.idle_timeout)
            data = json.loads(raw)
            buffered.append(envelope("binance", symbol, channel, "ws_depth_update", data))
            start_index = find_binance_start(buffered)

        logging.info("binance applying buffered updates start_index=%s kept=%s", start_index, len(buffered) - start_index)
        count = 0
        for record in buffered[start_index:]:
            writer.write(record)
            count += 1
            data = record["data"]
            if count <= args.print_first:
                logging.info(
                    "binance buffered[%s] U=%s u=%s bids=%s asks=%s",
                    count,
                    data.get("U"),
                    data.get("u"),
                    len(data.get("b", [])),
                    len(data.get("a", [])),
                )

        while not STOP.is_set():
            if writer.current_end and utc_now() >= writer.current_end:
                logging.info(
                    "binance rotation boundary reached; reconnecting for fresh snapshot window_end=%s",
                    writer.current_end,
                )
                return
            raw = await asyncio.wait_for(ws.recv(), timeout=args.idle_timeout)
            data = json.loads(raw)
            writer.write(envelope("binance", symbol, channel, "ws_depth_update", data))
            count += 1
            if count <= args.print_first:
                logging.info(
                    "binance first[%s] U=%s u=%s bids=%s asks=%s",
                    count,
                    data.get("U"),
                    data.get("u"),
                    len(data.get("b", [])),
                    len(data.get("a", [])),
                )
            if count % args.flush_every == 0:
                writer.flush()


async def capture_coinbase(args: argparse.Namespace, writer: RotatingJsonlWriter) -> None:
    product = args.product
    channel = args.coinbase_channel
    url = "wss://ws-feed.exchange.coinbase.com"
    sub = {"type": "subscribe", "product_ids": [product], "channels": [channel]}

    async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=None) as ws:
        logging.info("coinbase connected url=%s subscribe=%s", url, sub)
        writer.write(envelope("coinbase", product, channel, "control_event", {
            "event": "connection_open",
            "url": url,
            "subscribe": sub,
        }))
        await ws.send(json.dumps(sub))
        count = 0
        while not STOP.is_set():
            raw = await asyncio.wait_for(ws.recv(), timeout=args.idle_timeout)
            data = json.loads(raw)
            if data.get("type") == "snapshot":
                writer.write(envelope("coinbase", product, channel, "control_event", {
                    "event": "segment_start",
                    "reason": "snapshot_received",
                    "product_id": data.get("product_id"),
                    "bids": len(data.get("bids", [])),
                    "asks": len(data.get("asks", [])),
                }))
            writer.write(envelope("coinbase", product, channel, "ws_message", data))
            count += 1
            if count <= args.print_first:
                logging.info(
                    "coinbase first[%s] type=%s product=%s bytes=%s",
                    count,
                    data.get("type"),
                    data.get("product_id"),
                    len(raw),
                )
            if count % args.flush_every == 0:
                writer.flush()


async def run_forever(args: argparse.Namespace) -> None:
    symbol = args.symbol if args.exchange == "binance" else args.product
    channel = f"depth@{args.binance_interval}" if args.exchange == "binance" else args.coinbase_channel
    storage_exchange = args.storage_exchange or args.exchange
    writer = RotatingJsonlWriter(Path(args.root), storage_exchange, symbol, args.rotate_hours)
    backoff = 1.0
    attempt = 0
    try:
        while not STOP.is_set():
            attempt += 1
            try:
                logging.info("starting exchange=%s attempt=%s", args.exchange, attempt)
                if args.exchange == "binance":
                    await capture_binance(args, writer)
                else:
                    await capture_coinbase(args, writer)
                backoff = 1.0
            except asyncio.TimeoutError:
                logging.exception("idle timeout; reconnecting")
                writer.write(envelope(args.exchange, symbol, channel, "control_event", {
                    "event": "connection_timeout",
                    "attempt": attempt,
                    "idle_timeout": args.idle_timeout,
                }))
            except (ConnectionClosed, OSError, WebSocketException):
                logging.exception("connection closed/error; reconnecting")
                writer.write(envelope(args.exchange, symbol, channel, "control_event", {
                    "event": "connection_closed",
                    "attempt": attempt,
                }))
            except Exception:
                logging.exception("unexpected error; reconnecting")
                writer.write(envelope(args.exchange, symbol, channel, "control_event", {
                    "event": "unexpected_error",
                    "attempt": attempt,
                }))

            if not STOP.is_set():
                sleep_for = min(backoff, args.reconnect_max)
                logging.info("sleeping before reconnect seconds=%s", sleep_for)
                writer.write(envelope(args.exchange, symbol, channel, "control_event", {
                    "event": "reconnect_sleep",
                    "attempt": attempt,
                    "seconds": sleep_for,
                }))
                await asyncio.sleep(sleep_for)
                backoff = min(backoff * 2, args.reconnect_max)
    finally:
        writer.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/opt/lob_system")
    parser.add_argument("--exchange", choices=["binance", "coinbase"], required=True)
    parser.add_argument("--storage-exchange", help="Folder/file prefix used for storage, e.g. coinbase_backup")
    parser.add_argument("--rotate-hours", type=int, default=12)
    parser.add_argument("--symbol", default="BTCUSDT", help="Binance symbol")
    parser.add_argument("--product", default="BTC-USD", help="Coinbase product")
    parser.add_argument("--binance-interval", default="100ms", choices=["100ms", "1000ms"])
    parser.add_argument("--binance-snapshot-limit", type=int, default=5000)
    parser.add_argument("--coinbase-channel", default="level2_batch", choices=["level2", "level2_batch"])
    parser.add_argument("--idle-timeout", type=float, default=60.0)
    parser.add_argument("--reconnect-max", type=float, default=5.0)
    parser.add_argument("--flush-every", type=int, default=100)
    parser.add_argument("--print-first", type=int, default=5)
    parser.add_argument("--verbose", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    storage_exchange = args.storage_exchange or args.exchange
    name = f"capture_{storage_exchange}_{safe_symbol(args.symbol if args.exchange == 'binance' else args.product)}"
    setup_logging(Path(args.root) / "logs", name, args.verbose)
    logging.info("boot pid=%s python=%s", os.getpid(), sys.version.replace("\n", " "))

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, STOP.set)
        except NotImplementedError:
            pass
    try:
        loop.run_until_complete(run_forever(args))
    finally:
        loop.close()


if __name__ == "__main__":
    main()
