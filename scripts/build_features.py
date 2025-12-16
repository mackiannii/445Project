#!/usr/bin/env python3
"""
Build feature tables by merging Polymarket snapshots with Coinbase 5m bars.

Requirements:
  pip install pandas pyarrow

Usage:
  ./scripts/build_features.py \
    --snapshots data/daily_snapshots.ndjson \
    --btc-bars data/cb_btc_5m.csv \
    --eth-bars data/cb_eth_5m.csv \
    --out-btc data/features_btc.parquet \
    --out-eth data/features_eth.parquet
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def load_snapshots(path: Path) -> pd.DataFrame:
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["ts"], unit="s", utc=True)

    def to_prob_up(x):
        if isinstance(x, list):
            return float(x[0]) if x else None
        try:
            vals = json.loads(x)
            return float(vals[0])
        except Exception:
            return None

    df["prob_up"] = df["outcomePrices"].apply(to_prob_up)
    for col in ["volume", "volume24hr", "liquidity", "bestBid", "bestAsk", "lastTradePrice", "oneDayPriceChange", "oneHourPriceChange"]:
        df[col] = pd.to_numeric(df.get(col), errors="coerce")
    df["mid"] = (df["bestBid"] + df["bestAsk"]) / 2
    df["spread"] = df["bestAsk"] - df["bestBid"]
    df["bar_time"] = df["ts"].dt.floor("5min")
    return df


def load_bars(path: Path) -> pd.DataFrame:
    bars = pd.read_csv(path, parse_dates=["timestamp"])
    bars = bars.rename(columns={"timestamp": "bar_time"})
    return bars


def merge_and_save(df_snap: pd.DataFrame, bars: pd.DataFrame, out_path: Path) -> None:
    merged = df_snap.merge(bars, on="bar_time", how="left", suffixes=("", "_spot"))
    merged.to_parquet(out_path, index=False)
    print(f"saved {len(merged)} rows -> {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshots", required=True, help="Path to Polymarket snapshots ndjson")
    parser.add_argument("--btc-bars", required=True, help="Path to Coinbase BTC bars CSV")
    parser.add_argument("--eth-bars", required=True, help="Path to Coinbase ETH bars CSV")
    parser.add_argument("--out-btc", default="data/features_btc.parquet", help="Output Parquet for BTC")
    parser.add_argument("--out-eth", default="data/features_eth.parquet", help="Output Parquet for ETH")
    args = parser.parse_args()

    snaps = load_snapshots(Path(args.snapshots))

    # BTC
    btc_snaps = snaps[snaps["series"] == "BTC"].copy()
    btc_bars = load_bars(Path(args.btc_bars))
    merge_and_save(btc_snaps, btc_bars, Path(args.out_btc))

    # ETH
    eth_snaps = snaps[snaps["series"] == "ETH"].copy()
    eth_bars = load_bars(Path(args.eth_bars))
    merge_and_save(eth_snaps, eth_bars, Path(args.out_eth))


if __name__ == "__main__":
    main()
