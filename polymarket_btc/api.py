"""Gamma API helpers for BTC daily markets."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import requests

from .config import (
    BTC_DAILY_SERIES_ID,
    CALL_DELAY_SEC,
    ETH_DAILY_SERIES_ID,
    GAMMA_BASE,
    REQUEST_TIMEOUT,
)


def _as_list(val: Any) -> List[Any]:
    if isinstance(val, list):
        return val
    if val is None:
        return []
    try:
        return json.loads(val)
    except Exception:
        return []


def fetch_json(url: str, params: Optional[Dict[str, Any]] = None, session: Optional[requests.Session] = None) -> Any:
    """GET JSON with a small delay to respect rate limits."""
    sess = session or requests
    resp = sess.get(url, params=params or {}, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    time.sleep(CALL_DELAY_SEC)
    return resp.json()


def fetch_markets(
    limit: int = 100,
    offset: int = 0,
    term: Optional[str] = None,
    category: Optional[str] = None,
    closed: Optional[bool] = False,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """Wrapper around Gamma /markets search."""
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    if term:
        params["search"] = term
    if category:
        params["category"] = category
    if closed is not None:
        params["closed"] = str(bool(closed)).lower()
    return fetch_json(f"{GAMMA_BASE}/markets", params=params, session=session)


def btc_daily_slug_for(day: datetime) -> str:
    month = day.strftime("%B").lower()
    return f"bitcoin-up-or-down-on-{month}-{day.day}"


def eth_daily_slug_for(day: datetime) -> str:
    month = day.strftime("%B").lower()
    return f"ethereum-up-or-down-on-{month}-{day.day}"


def _parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _end_date(evt: Dict[str, Any]) -> datetime:
    ts = evt.get("endDate")
    if not ts:
        return datetime.min.replace(tzinfo=timezone.utc)
    return _parse_ts(ts)


def get_all_btc_daily_events(
    series_id: int = BTC_DAILY_SERIES_ID,
    load_details: bool = False,
    closed_only: bool = False,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """Return all BTC up/down events; optionally hydrate each with full payload."""
    series = fetch_json(f"{GAMMA_BASE}/series/{series_id}", session=session)
    events = series.get("events", [])
    if closed_only:
        events = [evt for evt in events if evt.get("closed")]
    ordered = sorted(events, key=_end_date)
    if not load_details:
        return ordered
    detailed: List[Dict[str, Any]] = []
    for evt in ordered:
        try:
            detailed.append(fetch_json(f"{GAMMA_BASE}/events/slug/{evt['slug']}", session=session))
        except Exception as exc:  # keep the pipe going if one fails
            detailed.append({"slug": evt.get("slug"), "error": str(exc)})
    return detailed


def get_all_eth_daily_events(
    load_details: bool = False,
    closed_only: bool = False,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """Return all ETH up/down events; optionally hydrate each with full payload."""
    return get_all_btc_daily_events(
        series_id=ETH_DAILY_SERIES_ID,
        load_details=load_details,
        closed_only=closed_only,
        session=session,
    )


def get_latest_btc_daily_event(
    now: Optional[datetime] = None,
    series_id: int = BTC_DAILY_SERIES_ID,
    load_details: bool = True,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """Fetch the nearest-open (or most recent closed) BTC daily event."""
    base = get_all_btc_daily_events(series_id=series_id, load_details=False, session=session)
    if not base:
        raise ValueError("No events found for btc-up-or-down-daily series")
    now = now or datetime.now(timezone.utc)
    candidates = [evt for evt in base if not evt.get("closed") and _end_date(evt) > now]
    if candidates:
        latest_meta = sorted(candidates, key=_end_date)[0]
    else:
        latest_meta = sorted(base, key=_end_date, reverse=True)[0]
    if not load_details:
        return latest_meta
    return fetch_json(f"{GAMMA_BASE}/events/slug/{latest_meta['slug']}", session=session)


def get_latest_eth_daily_event(
    now: Optional[datetime] = None,
    load_details: bool = True,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """Fetch the nearest-open (or most recent closed) ETH daily event."""
    return get_latest_btc_daily_event(
        now=now,
        series_id=ETH_DAILY_SERIES_ID,
        load_details=load_details,
        session=session,
    )


def get_next_btc_daily_event(
    now: Optional[datetime] = None,
    series_id: int = BTC_DAILY_SERIES_ID,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """Alias: return the freshest BTC up/down event details."""
    return get_latest_btc_daily_event(now=now, series_id=series_id, load_details=True, session=session)


def get_next_eth_daily_event(
    now: Optional[datetime] = None,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """Alias: return the freshest ETH up/down event details."""
    return get_latest_eth_daily_event(now=now, load_details=True, session=session)


def summarize_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a single event into odds + liquidity metrics."""
    markets = event.get("markets", []) or []
    mkt = markets[0] if markets else {}
    outcomes = _as_list(mkt.get("outcomes"))
    prices = _as_list(mkt.get("outcomePrices"))
    return {
        "slug": event.get("slug"),
        "endDate": event.get("endDate"),
        "volume": event.get("volume"),
        "volume24hr": event.get("volume24hr"),
        "liquidity": mkt.get("liquidity") or event.get("liquidity"),
        "outcomes": outcomes,
        "outcomePrices": prices,
        "bestBid": mkt.get("bestBid"),
        "bestAsk": mkt.get("bestAsk"),
        "lastTradePrice": mkt.get("lastTradePrice"),
        "oneDayPriceChange": mkt.get("oneDayPriceChange"),
        "oneHourPriceChange": mkt.get("oneHourPriceChange"),
        "clobTokenIds": _as_list(mkt.get("clobTokenIds")),
    }


def build_time_series(events: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build a list of per-day summaries for quick time-series analysis."""
    return [summarize_event(evt) for evt in events]
