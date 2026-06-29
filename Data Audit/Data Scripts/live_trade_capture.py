#!/usr/bin/env python3
"""
Live raw trade capture for Binance Spot and Coinbase Exchange.

The script captures public trade/match streams into rotating JSONL files and
compresses closed windows. Raw trade captures are kept separate from raw LOB
captures under trades_live/ and trades_done/.
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
    root.handlers.clear()

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


class RotatingTradeWriter:
    def __init__(self, root: Path, stream_name: str, symbol: str, rotate_hours: int):
        self.root = root
        self.stream_name = stream_name
        self.symbol = safe_symbol(symbol)
        self.rotate_hours = rotate_hours
        self.live_dir = root / "trades_live" / stream_name / self.symbol
        self.done_dir = root / "trades_done" / stream_name / self.symbol
        self.live_dir.mkdir(parents=True, exist_ok=True)
        self.done_dir.mkdir(parents=True, exist_ok=True)
        self.current_start: Optional[datetime] = None
        self.current_end: Optional[datetime] = None
        self.current_path: Optional[Path] = None
        self.file = None
        self.lock = threading.Lock()
        self.compress_threads = []
        self.compress_old_live_files()

    def compress_old_live_files(self) -> None:
        now_start, _ = window_bounds(utc_now(), self.rotate_hours)
        current_prefix = f"{self.stream_name}_{self.symbol}_{now_start:%Y-%m-%d}_{now_start:%H}-"
        for path in self.live_dir.glob("*.jsonl"):
            if not path.name.startswith(current_prefix):
                dst = self.done_dir / (path.name + ".gz")
                if dst.exists():
                    dst = self.done_dir / f"{path.stem}_{int(time.time())}.jsonl.gz"
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
        self.current_path = self.live_dir / f"{self.stream_name}_{self.symbol}_{label}.jsonl"
        self.file = open(self.current_path, "a", encoding="utf-8", buffering=1)
        logging.info("active_file path=%s window_start=%s window_end=%s", self.current_path, start, end)

        if old_path and old_path.exists() and old_path != self.current_path:
            dst = self.done_dir / (old_path.name + ".gz")
            if dst.exists():
                dst = self.done_dir / f"{old_path.stem}_{int(time.time())}.jsonl.gz"
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


async def capture_binance_trades(args: argparse.Namespace, writer: RotatingTradeWriter) -> None:
    symbol = args.symbol.upper()
    stream = args.binance_stream
    channel = stream
    url = f"wss://data-stream.binance.vision/ws/{symbol.lower()}@{stream}"
    async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=None) as ws:
        logging.info("binance trades connected url=%s", url)
        count = 0
        while not STOP.is_set():
            raw = await asyncio.wait_for(ws.recv(), timeout=args.idle_timeout)
            data = json.loads(raw)
            writer.write(envelope("binance", symbol, channel, "ws_trade", data))
            count += 1
            if count <= args.print_first:
                logging.info(
                    "binance trade first[%s] id=%s price=%s qty=%s event_time=%s",
                    count,
                    data.get("t") or data.get("a"),
                    data.get("p"),
                    data.get("q"),
                    data.get("E"),
                )
            if count % args.flush_every == 0:
                writer.flush()


async def capture_coinbase_matches(args: argparse.Namespace, writer: RotatingTradeWriter) -> None:
    product = args.product
    channel = args.coinbase_channel
    url = "wss://ws-feed.exchange.coinbase.com"
    sub = {"type": "subscribe", "product_ids": [product], "channels": [channel]}
    async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=None) as ws:
        logging.info("coinbase trades connected url=%s subscribe=%s", url, sub)
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
            kind = "ws_match" if data.get("type") in {"match", "last_match"} else "ws_message"
            writer.write(envelope("coinbase", product, channel, kind, data))
            count += 1
            if count <= args.print_first:
                logging.info(
                    "coinbase message first[%s] type=%s trade_id=%s price=%s size=%s side=%s",
                    count,
                    data.get("type"),
                    data.get("trade_id"),
                    data.get("price"),
                    data.get("size"),
                    data.get("side"),
                )
            if count % args.flush_every == 0:
                writer.flush()


async def run_forever(args: argparse.Namespace) -> None:
    symbol = args.symbol if args.exchange == "binance" else args.product
    channel = args.binance_stream if args.exchange == "binance" else args.coinbase_channel
    storage_stream = args.storage_stream or f"{args.exchange}_trades"
    writer = RotatingTradeWriter(Path(args.root), storage_stream, symbol, args.rotate_hours)
    backoff = 1.0
    attempt = 0
    try:
        while not STOP.is_set():
            attempt += 1
            try:
                logging.info("starting exchange=%s attempt=%s", args.exchange, attempt)
                if args.exchange == "binance":
                    await capture_binance_trades(args, writer)
                else:
                    await capture_coinbase_matches(args, writer)
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
    parser.add_argument("--storage-stream", help="Folder/file prefix, e.g. coinbase_trades_backup")
    parser.add_argument("--rotate-hours", type=int, default=12)
    parser.add_argument("--symbol", default="BTCUSDT", help="Binance symbol")
    parser.add_argument("--product", default="BTC-USD", help="Coinbase product")
    parser.add_argument("--binance-stream", default="trade", choices=["trade", "aggTrade"])
    parser.add_argument("--coinbase-channel", default="matches", choices=["matches", "ticker"])
    parser.add_argument("--idle-timeout", type=float, default=60.0)
    parser.add_argument("--reconnect-max", type=float, default=5.0)
    parser.add_argument("--flush-every", type=int, default=100)
    parser.add_argument("--print-first", type=int, default=5)
    parser.add_argument("--verbose", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    storage_stream = args.storage_stream or f"{args.exchange}_trades"
    symbol = args.symbol if args.exchange == "binance" else args.product
    name = f"capture_{storage_stream}_{safe_symbol(symbol)}"
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
