#!/usr/bin/env python3
"""
Backfill Coinbase 5m candles for a product across a date range.

Coinbase API allows at most 300 candles per request, so we chunk the range.

Usage (BTC 5m since 2016):
  ./scripts/fetch_cb_history.py --product BTC-USD --granularity 300 \\
      --start 2016-01-01T00:00:00Z --end now --out data/cb_btc_5m.csv

Usage (ETH 5m since 2016):
  ./scripts/fetch_cb_history.py --product ETH-USD --granularity 300 \\
      --start 2016-01-01T00:00:00Z --end now --out data/cb_eth_5m.csv
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List

import requests


def parse_iso(ts: str) -> datetime:
    if ts.lower() == "now":
        return datetime.now(timezone.utc)
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def fetch_cb_candles(product: str, start: datetime, end: datetime, granularity: int) -> List[List[float]]:
    params = {
        "granularity": granularity,
        "start": start.isoformat(),
        "end": end.isoformat(),
    }
    resp = requests.get(
        f"https://api.exchange.coinbase.com/products/{product}/candles",
        params=params,
        timeout=15,
        headers={"User-Agent": "polymarket-btc-collector"},
    )
    resp.raise_for_status()
    return resp.json()  # list of [time, low, high, open, close, volume]


def daterange_chunks(start: datetime, end: datetime, granularity: int) -> Iterable[tuple[datetime, datetime]]:
    # Coinbase caps at 300 candles per request
    max_span = timedelta(seconds=granularity * 300)
    cur = start
    while cur < end:
        nxt = min(cur + max_span, end)
        yield cur, nxt
        cur = nxt


def ensure_header(path: Path) -> None:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
            writer.writeheader()


def append_rows(path: Path, rows: Iterable[dict]) -> None:
    with path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
        for row in rows:
            writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--product", default="BTC-USD", help="Coinbase product id (BTC-USD, ETH-USD, etc.)")
    parser.add_argument("--granularity", type=int, default=300, help="Bar size in seconds (300=5m)")
    parser.add_argument("--start", default="2016-01-01T00:00:00Z", help="ISO start (e.g., 2016-01-01T00:00:00Z)")
    parser.add_argument("--end", default="now", help="ISO end (e.g., 2025-01-01T00:00:00Z or 'now')")
    parser.add_argument("--out", required=True, help="Output CSV path")
    args = parser.parse_args()

    start_dt = parse_iso(args.start)
    end_dt = parse_iso(args.end)
    out_path = Path(args.out)
    ensure_header(out_path)

    total_chunks = 0
    for chunk_start, chunk_end in daterange_chunks(start_dt, end_dt, args.granularity):
        candles = fetch_cb_candles(args.product, chunk_start, chunk_end, args.granularity)
        # Coinbase returns newest-first; sort ascending for consistency
        candles = sorted(candles, key=lambda c: c[0])
        rows = []
        for ts, low, high, opn, close, vol in candles:
            rows.append(
                {
                    "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                    "open": opn,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": vol,
                }
            )
        append_rows(out_path, rows)
        total_chunks += 1
    print(f"Done. Wrote to {out_path}. Chunks: {total_chunks}")


if __name__ == "__main__":
    main()
