#!/usr/bin/env python3
"""
Resample saved Polymarket price-history JSON files to a regular cadence (default 30m).
Input: data/prices_history/*.json written by fetch_prices_history.py
Output: data/prices_history_30m/{slug}_{token_id}.csv with columns:
  ts (UTC datetime), p (probability), elapsed_hours (since market open)
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List

import pandas as pd


def load_history(path: Path) -> pd.DataFrame:
    payload = json.loads(path.read_text())
    hist = payload.get("history", [])
    if not hist:
        return pd.DataFrame(columns=["ts", "p"])
    df = pd.DataFrame(hist)
    df["ts"] = pd.to_datetime(df["t"], unit="s", utc=True)
    df = df.rename(columns={"p": "prob"})
    return df[["ts", "prob"]].sort_values("ts")


def resample_df(df: pd.DataFrame, rule: str, method: str) -> pd.DataFrame:
    if df.empty:
        return df
    origin = df["ts"].iloc[0]
    sr = df.set_index("ts")["prob"]
    rs = sr.resample(rule, origin=origin).mean()
    if method == "ffill":
        rs = rs.ffill()
    elif method == "linear":
        rs = rs.interpolate()
    return rs.to_frame(name="prob").reset_index()


def process_files(src_dir: Path, out_dir: Path, rule: str, method: str) -> List[Path]:
    out_paths: List[Path] = []
    for path in sorted(src_dir.glob("*.json")):
        df = load_history(path)
        rs = resample_df(df, rule, method)
        if rs.empty:
            continue
        start = rs["ts"].iloc[0]
        rs["elapsed_hours"] = (rs["ts"] - start).dt.total_seconds() / 3600.0
        out_path = out_dir / f"{path.stem}.csv"
        rs.to_csv(out_path, index=False)
        out_paths.append(out_path)
    return out_paths


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", default="data/prices_history", help="Input directory of JSON history files")
    parser.add_argument("--out", default="data/prices_history_30m", help="Output directory for resampled CSVs")
    parser.add_argument("--rule", default="30min", help="Pandas resample rule (e.g., 30min, 1h)")
    parser.add_argument("--method", default="linear", choices=["linear", "ffill"], help="Interpolation method")
    args = parser.parse_args()

    src_dir = Path(args.src)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    out_paths = process_files(src_dir, out_dir, args.rule, args.method)
    print(f"Wrote {len(out_paths)} files to {out_dir}")


if __name__ == "__main__":
    main()
