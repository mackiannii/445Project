#!/usr/bin/env python3
"""
Fetch full Polymarket price history for each BTC/ETH daily market slug in the local features files.

Requires network access. Writes JSON files under data/prices_history/{slug}_{token_id}.json
with the raw history returned by https://clob.polymarket.com/prices-history.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests


GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"


def load_slugs(data_dir: Path) -> List[str]:
    slugs = []
    for fname in ["features_btc.parquet", "features_eth.parquet"]:
        path = data_dir / fname
        if not path.exists():
            continue
        df = pd.read_parquet(path)
        slugs.extend(df["slug"].dropna().unique().tolist())
    return sorted(set(slugs))


def fetch_event(slug: str) -> Optional[Dict]:
    url = f"{GAMMA_BASE}/events/slug/{slug}"
    resp = requests.get(url, timeout=10)
    if resp.status_code != 200:
        return None
    try:
        return resp.json()
    except Exception:
        return None


def parse_event(event: Dict) -> Optional[Tuple[List[str], int, int]]:
    """Return token ids, startTs, endTs (epoch seconds)."""
    markets = event.get("markets") or []
    if not markets:
        return None
    m0 = markets[0]
    token_ids_raw = m0.get("clobTokenIds") or m0.get("tokenIds")
    try:
        token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else list(token_ids_raw)
    except Exception:
        return None
    def to_ts(key: str) -> Optional[int]:
        val = m0.get(key) or event.get(key)
        if not val:
            return None
        try:
            return int(pd.to_datetime(val, utc=True).timestamp())
        except Exception:
            return None
    start_ts = to_ts("startDate") or to_ts("creationDate")
    end_ts = to_ts("endDate")
    if start_ts is None or end_ts is None:
        return None
    return token_ids, start_ts, end_ts


def fetch_history(token_id: str, start_ts: int, end_ts: int, interval: str, fidelity: Optional[int]) -> List[Dict]:
    params = {
        "market": token_id,
        "startTs": start_ts,
        "endTs": end_ts,
        "interval": interval,
        "order": "asc",
    }
    if fidelity is not None:
        params["fidelity"] = fidelity
    resp = requests.get(f"{CLOB_BASE}/prices-history", params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    return data.get("history", [])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data", help="Directory containing features_btc.parquet / features_eth.parquet")
    parser.add_argument("--out-dir", default="data/prices_history", help="Directory to write per-token history JSON files")
    parser.add_argument("--interval", default="1m", choices=["1m", "1h", "6h", "1d", "1w"], help="prices-history interval")
    parser.add_argument("--fidelity", type=int, default=10, help="fidelity parameter required for 1m interval")
    parser.add_argument("--slugs", help="Comma-separated list of slugs to fetch (override auto-discovery)")
    parser.add_argument("--max-markets", type=int, default=None, help="Optional cap on number of slugs to fetch")
    parser.add_argument("--sleep", type=float, default=0.1, help="Sleep seconds between requests to be polite")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.slugs:
        slugs = [s.strip() for s in args.slugs.split(",") if s.strip()]
    else:
        slugs = load_slugs(data_dir)
    if args.max_markets:
        slugs = slugs[: args.max_markets]
    print(f"Found {len(slugs)} slugs")

    fetched = 0
    skipped = 0
    for slug in slugs:
        event = fetch_event(slug)
        if not event:
            print(f"[skip] {slug}: no event data")
            skipped += 1
            continue
        parsed = parse_event(event)
        if not parsed:
            print(f"[skip] {slug}: missing token ids or dates")
            skipped += 1
            continue
        token_ids, start_ts, end_ts = parsed
        for token_id in token_ids:
            try:
                hist = fetch_history(
                    token_id=token_id,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    interval=args.interval,
                    fidelity=args.fidelity if args.interval == "1m" else None,
                )
            except Exception as e:
                print(f"[error] {slug} token {token_id}: {e}")
                continue
            out_path = out_dir / f"{slug}_{token_id}.json"
            payload = {
                "slug": slug,
                "token_id": token_id,
                "startTs": start_ts,
                "endTs": end_ts,
                "interval": args.interval,
                "fidelity": args.fidelity if args.interval == "1m" else None,
                "history": hist,
            }
            out_path.write_text(json.dumps(payload))
            fetched += 1
            print(f"[ok] {slug} token {token_id}: {len(hist)} points -> {out_path}")
            time.sleep(args.sleep)
    print(f"Done. Files written: {fetched}, skipped: {skipped}")


if __name__ == "__main__":
    main()
