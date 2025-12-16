#!/usr/bin/env python3
"""
Export a snapshot of all BTC and ETH daily up/down events with current odds.

Usage:
  ./scripts/export_daily_snapshot.py > daily_snapshot.csv
"""

from __future__ import annotations

import csv
from datetime import datetime
from typing import Dict, List

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from polymarket_btc.api import (  # noqa: E402
    build_time_series,
    get_all_btc_daily_events,
    get_all_eth_daily_events,
)


def rows_for_series(series_name: str, events: List[Dict]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    ts = build_time_series(events)
    for item in ts:
        rows.append(
            {
                "series": series_name,
                "slug": item.get("slug", ""),
                "endDate": item.get("endDate", ""),
                "outcomes": "/".join(item.get("outcomes") or []),
                "outcomePrices": ",".join(str(p) for p in (item.get("outcomePrices") or [])),
                "bestBid": str(item.get("bestBid") or ""),
                "bestAsk": str(item.get("bestAsk") or ""),
                "lastTradePrice": str(item.get("lastTradePrice") or ""),
                "oneDayPriceChange": str(item.get("oneDayPriceChange") or ""),
                "oneHourPriceChange": str(item.get("oneHourPriceChange") or ""),
                "volume": str(item.get("volume") or ""),
                "volume24hr": str(item.get("volume24hr") or ""),
                "liquidity": str(item.get("liquidity") or ""),
            }
        )
    return rows


def main() -> None:
    btc_events = get_all_btc_daily_events(load_details=True)
    eth_events = get_all_eth_daily_events(load_details=True)

    fieldnames = [
        "series",
        "slug",
        "endDate",
        "outcomes",
        "outcomePrices",
        "bestBid",
        "bestAsk",
        "lastTradePrice",
        "oneDayPriceChange",
        "oneHourPriceChange",
        "volume",
        "volume24hr",
        "liquidity",
    ]

    writer = csv.DictWriter(
        f=open("/dev/stdout", "w", newline=""),
        fieldnames=fieldnames,
    )
    writer.writeheader()
    for row in rows_for_series("BTC", btc_events):
        writer.writerow(row)
    for row in rows_for_series("ETH", eth_events):
        writer.writerow(row)


if __name__ == "__main__":
    main()
