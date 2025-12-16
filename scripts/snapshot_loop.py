#!/usr/bin/env python3
"""
Periodic snapshot of BTC/ETH daily up/down odds to ndjson.

Usage:
  ./scripts/snapshot_loop.py --out data/daily_snapshots.ndjson --interval 600 --iterations 72
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
import json
import time
from polymarket_btc.api import build_time_series, get_all_btc_daily_events, get_all_eth_daily_events  # noqa: E402


def snapshot_once() -> list[dict]:
    btc_events = get_all_btc_daily_events(load_details=True)
    eth_events = get_all_eth_daily_events(load_details=True)
    ts = []
    ts += [{"series": "BTC", **row} for row in build_time_series(btc_events)]
    ts += [{"series": "ETH", **row} for row in build_time_series(eth_events)]
    return ts


def append_ndjson(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="data/daily_snapshots.ndjson", help="Output ndjson file")
    parser.add_argument("--interval", type=int, default=600, help="Seconds between snapshots")
    parser.add_argument("--iterations", type=int, default=72, help="How many snapshots to take")
    args = parser.parse_args()

    out_path = Path(args.out)
    for i in range(args.iterations):
        ts = snapshot_once()
        stamped = [{"ts": time.time(), **row} for row in ts]
        append_ndjson(out_path, stamped)
        if i < args.iterations - 1:
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
