"""Example usage for BTC daily helpers."""

from __future__ import annotations

import asyncio

from .api import (
    build_time_series,
    get_all_btc_daily_events,
    get_all_eth_daily_events,
    get_latest_btc_daily_event,
    get_latest_eth_daily_event,
    summarize_event,
)
from .orderbook import extract_token_ids_from_event, fetch_latest_orderbooks, stream_orderbook


def load_full_series():
    full_events = get_all_btc_daily_events(load_details=True)
    print("loaded", len(full_events), "events")
    return full_events


def print_latest_summary():
    latest = get_latest_btc_daily_event()
    print(latest["slug"], latest["endDate"])
    print("markets:", [m.get("slug") for m in latest.get("markets", [])])
    print("token ids:", extract_token_ids_from_event(latest))


def print_latest_eth_summary():
    latest = get_latest_eth_daily_event()
    print(latest["slug"], latest["endDate"])
    print("markets:", [m.get("slug") for m in latest.get("markets", [])])
    print("token ids:", extract_token_ids_from_event(latest))


def summarize_time_series():
    full_events = load_full_series()
    ts = build_time_series(full_events)
    print("recent summary:", ts[-1])


def summarize_eth_time_series():
    full_events = get_all_eth_daily_events(load_details=True)
    ts = build_time_series(full_events)
    print("recent summary:", ts[-1])


def fetch_latest_books():
    full_events = load_full_series()
    books = fetch_latest_orderbooks(full_events)
    print({k: list(v.keys()) if isinstance(v, dict) else v for k, v in books.items()})


def demo_websocket(messages: int = 3, ws_url: str | None = None):
    full_events = load_full_series()
    tokens = extract_token_ids_from_event(full_events[-1])
    if not tokens:
        print("no tokens found on latest event")
        return
    token_id = tokens[0]
    print("streaming token:", token_id)
    asyncio.run(stream_orderbook(token_id, messages=messages, ws_url=ws_url))


if __name__ == "__main__":
    print_latest_summary()
