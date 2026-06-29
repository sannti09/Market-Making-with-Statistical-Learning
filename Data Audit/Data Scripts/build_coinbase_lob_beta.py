#!/usr/bin/env python3
"""
Beta Coinbase LOB reconstructor.

Reads raw Coinbase Exchange level2/level2_batch JSONL(.gz), reconstructs the
aggregated L2 book by segment, and emits 1-second features. The raw data remains
the source of truth; this script creates a first derived layer for diagnostics and
model prototyping.
"""

import argparse
import csv
import gzip
import heapq
import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def parse_time(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def sec_floor(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)


def open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "rt", encoding="utf-8")


def iter_paths(inputs: List[str]) -> List[Path]:
    paths: List[Path] = []
    for item in inputs:
        p = Path(item)
        if p.is_dir():
            paths.extend(sorted(p.glob("*.jsonl.gz")))
            paths.extend(sorted(p.glob("*.jsonl")))
        else:
            paths.append(p)
    return sorted(paths, key=lambda x: x.name)


class BookSide:
    def __init__(self, is_bid: bool):
        self.is_bid = is_bid
        self.levels: Dict[float, float] = {}
        self.heap: List[float] = []

    def set_level(self, price_str: str, size_str: str) -> None:
        price = float(price_str)
        size = float(size_str)
        if size <= 0.0:
            self.levels.pop(price, None)
            return
        self.levels[price] = size
        heap_value = -price if self.is_bid else price
        heapq.heappush(self.heap, heap_value)

    def load_snapshot(self, rows: Iterable[List[str]]) -> None:
        self.levels.clear()
        self.heap.clear()
        for price_str, size_str in rows:
            price = float(price_str)
            size = float(size_str)
            if size > 0.0:
                self.levels[price] = size
                heapq.heappush(self.heap, -price if self.is_bid else price)

    def top_n(self, n: int) -> List[Tuple[float, float]]:
        out: List[Tuple[float, float]] = []
        seen = set()
        popped: List[float] = []
        while self.heap and len(out) < n:
            raw = heapq.heappop(self.heap)
            price = -raw if self.is_bid else raw
            size = self.levels.get(price)
            if size is None or size <= 0.0 or price in seen:
                continue
            out.append((price, size))
            seen.add(price)
            popped.append(raw)
        for raw in popped:
            heapq.heappush(self.heap, raw)
        return out


class CoinbaseBook:
    def __init__(self):
        self.bids = BookSide(is_bid=True)
        self.asks = BookSide(is_bid=False)
        self.segment_id = 0
        self.in_segment = False
        self.segment_start_time: Optional[datetime] = None
        self.segment_start_reason = ""
        self.gap_from_previous_segment = 0.0

    def apply_snapshot(
        self,
        data: Dict[str, Any],
        event_time: Optional[datetime],
        reason: str,
        gap_from_previous_segment: float,
    ) -> None:
        self.bids.load_snapshot(data.get("bids", []))
        self.asks.load_snapshot(data.get("asks", []))
        self.segment_id += 1
        self.in_segment = True
        self.segment_start_time = event_time
        self.segment_start_reason = reason
        self.gap_from_previous_segment = max(0.0, gap_from_previous_segment)

    def apply_l2update(self, data: Dict[str, Any]) -> None:
        for side, price, size in data.get("changes", []):
            if side == "buy":
                self.bids.set_level(price, size)
            elif side == "sell":
                self.asks.set_level(price, size)

    def features(self, top_n: int) -> Optional[Dict[str, Any]]:
        bids = self.bids.top_n(top_n)
        asks = self.asks.top_n(top_n)
        if not bids or not asks:
            return None

        best_bid, best_bid_size = bids[0]
        best_ask, best_ask_size = asks[0]
        mid = (best_bid + best_ask) / 2.0
        spread = best_ask - best_bid
        depth_bid = sum(size for _, size in bids)
        depth_ask = sum(size for _, size in asks)
        denom = depth_bid + depth_ask
        imbalance = (depth_bid - depth_ask) / denom if denom else math.nan
        micro_denom = best_bid_size + best_ask_size
        microprice = (
            (best_ask * best_bid_size + best_bid * best_ask_size) / micro_denom
            if micro_denom
            else math.nan
        )

        row: Dict[str, Any] = {
            "segment_id": self.segment_id,
            "segment_start_time": (
                self.segment_start_time.isoformat().replace("+00:00", "Z")
                if self.segment_start_time
                else ""
            ),
            "segment_start_reason": self.segment_start_reason,
            "gap_from_previous_segment": round(self.gap_from_previous_segment, 6),
            "best_bid": best_bid,
            "best_ask": best_ask,
            "best_bid_size": best_bid_size,
            "best_ask_size": best_ask_size,
            "mid": mid,
            "spread": spread,
            "microprice": microprice,
            f"bid_depth_top{top_n}": depth_bid,
            f"ask_depth_top{top_n}": depth_ask,
            f"imbalance_top{top_n}": imbalance,
        }
        for i in range(top_n):
            bp, bs = bids[i] if i < len(bids) else (math.nan, math.nan)
            ap, az = asks[i] if i < len(asks) else (math.nan, math.nan)
            row[f"bid_px_{i+1}"] = bp
            row[f"bid_sz_{i+1}"] = bs
            row[f"ask_px_{i+1}"] = ap
            row[f"ask_sz_{i+1}"] = az
        return row


def write_row(
    writer: csv.DictWriter,
    book: CoinbaseBook,
    ts: datetime,
    top_n: int,
    update_count: int,
    changed_buy: int,
    changed_sell: int,
    removed_buy: int,
    removed_sell: int,
    gap_flag: int,
    gap_seconds: float,
    row_quality: str,
) -> bool:
    feats = book.features(top_n)
    if feats is None:
        return False
    feats.update(
        {
            "ts": ts.isoformat().replace("+00:00", "Z"),
            "updates": update_count,
            "changed_buy_levels": changed_buy,
            "changed_sell_levels": changed_sell,
            "removed_buy_levels": removed_buy,
            "removed_sell_levels": removed_sell,
            "gap_flag": gap_flag,
            "gap_seconds": round(gap_seconds, 6),
            "row_quality": row_quality,
        }
    )
    writer.writerow(feats)
    return True


def build(args: argparse.Namespace) -> None:
    paths = iter_paths(args.inputs)
    if not paths:
        raise SystemExit("No input files found")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    book = CoinbaseBook()
    last_event_time: Optional[datetime] = None
    last_seen_l2_time: Optional[datetime] = None
    current_sec: Optional[datetime] = None
    update_count = changed_buy = changed_sell = removed_buy = removed_sell = 0
    gap_flag = 0
    gap_seconds = 0.0
    row_quality = "clean"
    rows_written = 0
    lines = 0
    snapshots = 0
    updates = 0
    controls = 0
    skipped_before_snapshot = 0
    json_errors = 0

    base_fields = [
        "ts",
        "segment_id",
        "segment_start_time",
        "segment_start_reason",
        "gap_from_previous_segment",
        "row_quality",
        "updates",
        "changed_buy_levels",
        "changed_sell_levels",
        "removed_buy_levels",
        "removed_sell_levels",
        "gap_flag",
        "gap_seconds",
        "best_bid",
        "best_ask",
        "best_bid_size",
        "best_ask_size",
        "mid",
        "spread",
        "microprice",
        f"bid_depth_top{args.top_n}",
        f"ask_depth_top{args.top_n}",
        f"imbalance_top{args.top_n}",
    ]
    level_fields: List[str] = []
    for i in range(args.top_n):
        level_fields.extend([f"bid_px_{i+1}", f"bid_sz_{i+1}", f"ask_px_{i+1}", f"ask_sz_{i+1}"])

    with open(out_path, "w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=base_fields + level_fields)
        writer.writeheader()

        for path in paths:
            with open_text(path) as f:
                for line in f:
                    lines += 1
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        json_errors += 1
                        continue
                    data = obj.get("data") or {}
                    kind = obj.get("kind")
                    msg_type = data.get("type")

                    if kind == "control_event":
                        controls += 1
                        continue
                    if msg_type == "snapshot":
                        recv_time = parse_time(obj["recv_time"]) if obj.get("recv_time") else None
                        gap_from_previous_segment = (
                            (recv_time - last_seen_l2_time).total_seconds()
                            if recv_time and last_seen_l2_time
                            else 0.0
                        )
                        reason = "initial_snapshot" if snapshots == 0 else "resync_snapshot"
                        book.apply_snapshot(data, recv_time, reason, gap_from_previous_segment)
                        snapshots += 1
                        last_event_time = None
                        current_sec = None
                        update_count = changed_buy = changed_sell = removed_buy = removed_sell = 0
                        gap_flag = 0
                        gap_seconds = 0.0
                        row_quality = "segment_start"
                        continue
                    if msg_type != "l2update":
                        continue
                    if not book.in_segment:
                        skipped_before_snapshot += 1
                        continue

                    event_time = parse_time(data["time"])
                    event_sec = sec_floor(event_time)
                    if last_event_time is not None:
                        diff = (event_time - last_event_time).total_seconds()
                        if diff >= args.gap_threshold:
                            gap_flag = 1
                            gap_seconds = max(gap_seconds, diff)
                            row_quality = "gap_in_segment"
                    last_event_time = event_time
                    last_seen_l2_time = event_time

                    if current_sec is None:
                        current_sec = event_sec
                    while current_sec is not None and event_sec > current_sec:
                        if write_row(
                            writer,
                            book,
                            current_sec,
                            args.top_n,
                            update_count,
                            changed_buy,
                            changed_sell,
                            removed_buy,
                            removed_sell,
                            gap_flag,
                            gap_seconds,
                            row_quality,
                        ):
                            rows_written += 1
                        current_sec = current_sec + timedelta(seconds=1)
                        update_count = changed_buy = changed_sell = removed_buy = removed_sell = 0
                        gap_flag = 0
                        gap_seconds = 0.0
                        row_quality = "clean"

                    for side, _price, size in data.get("changes", []):
                        is_remove = float(size) <= 0.0
                        if side == "buy":
                            changed_buy += 1
                            removed_buy += int(is_remove)
                        elif side == "sell":
                            changed_sell += 1
                            removed_sell += int(is_remove)
                    book.apply_l2update(data)
                    updates += 1
                    update_count += 1

        if current_sec is not None and book.in_segment:
            if write_row(
                writer,
                book,
                current_sec,
                args.top_n,
                update_count,
                changed_buy,
                changed_sell,
                removed_buy,
                removed_sell,
                gap_flag,
                gap_seconds,
                row_quality,
            ):
                rows_written += 1

    print("inputs", len(paths))
    print("lines", lines)
    print("json_errors", json_errors)
    print("snapshots", snapshots)
    print("l2updates", updates)
    print("control_events", controls)
    print("skipped_before_snapshot", skipped_before_snapshot)
    print("rows_written", rows_written)
    print("output", out_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+", help="Input JSONL(.gz) files or folders")
    parser.add_argument("--output", required=True, help="Output CSV path")
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--gap-threshold", type=float, default=2.0)
    args = parser.parse_args()
    build(args)


if __name__ == "__main__":
    main()
