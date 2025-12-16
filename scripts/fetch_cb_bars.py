#!/usr/bin/env python3
"""
Fetch recent Coinbase 5m candles for a product (default BTC-USD).

Usage:
  ./scripts/fetch_cb_bars.py --product BTC-USD --granularity 300 --limit 200
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path

import requests


def fetch_cb_candles(product: str, granularity: int = 300, limit: int = 200):
    params = {"granularity": granularity, "limit": limit}
    resp = requests.get(f"https://api.exchange.coinbase.com/products/{product}/candles", params=params, timeout=10)
    resp.raise_for_status()
    # Coinbase returns [time, low, high, open, close, volume]
    return resp.json()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--product", default="BTC-USD", help="Coinbase product id (e.g., BTC-USD, ETH-USD)")
    parser.add_argument("--granularity", type=int, default=300, help="Bar size in seconds (300=5m, 60=1m, etc.)")
    parser.add_argument("--limit", type=int, default=200, help="Number of candles to fetch (Coinbase caps to ~300)")
    parser.add_argument("--out", default="", help="Optional CSV output path; prints to stdout if empty")
    args = parser.parse_args()

    candles = fetch_cb_candles(args.product, args.granularity, args.limit)

    rows = []
    for c in candles:
        ts, low, high, opn, close, vol = c
        rows.append(
            {
                "timestamp": datetime.utcfromtimestamp(ts).isoformat() + "Z",
                "low": low,
                "high": high,
                "open": opn,
                "close": close,
                "volume": vol,
            }
        )

    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
    else:
        for r in rows:
            print(r)


if __name__ == "__main__":
    main()
