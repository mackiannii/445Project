"""Order book access for Polymarket BTC markets (REST + optional websocket)."""

from __future__ import annotations

import json
import ssl
from typing import Any, Dict, List, Optional

import requests

from .config import CLOB_BASE, CLOB_WS_URL, REQUEST_TIMEOUT
from .api import _as_list

try:
    import asyncio
    import websockets
except ImportError:  # optional dependency
    websockets = None  # type: ignore
    asyncio = None  # type: ignore


def fetch_orderbook_rest(token_id: str, session: Optional[requests.Session] = None) -> Dict[str, Any]:
    """Fetch bids/asks for a single token via REST."""
    sess = session or requests
    resp = sess.get(f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def extract_token_ids_from_event(event: Dict[str, Any]) -> List[str]:
    """Return all clobTokenIds from the event's markets."""
    tokens: List[str] = []
    for mkt in event.get("markets", []) or []:
        tokens.extend(_as_list(mkt.get("clobTokenIds")))
    return tokens


def fetch_latest_orderbooks(full_events: List[Dict[str, Any]], session: Optional[requests.Session] = None) -> Dict[str, Any]:
    """Fetch order books for the newest event in a list of hydrated events."""
    if not full_events:
        return {}
    latest_evt = full_events[-1]
    tokens = extract_token_ids_from_event(latest_evt)
    books: Dict[str, Any] = {}
    for token_id in tokens:
        try:
            books[token_id] = fetch_orderbook_rest(token_id, session=session)
        except Exception as exc:
            books[token_id] = {"error": str(exc)}
    return books


async def stream_orderbook(token_id: str, messages: int = 5, ws_url: Optional[str] = None) -> None:
    """Subscribe to live order book updates via websocket.

    Requires `pip install websockets`.
    """
    if websockets is None or asyncio is None:
        raise RuntimeError("websockets package not installed; pip install websockets")
    url = ws_url or CLOB_WS_URL
    ssl_ctx = ssl.create_default_context()
    try:
        import certifi

        ssl_ctx.load_verify_locations(certifi.where())
    except Exception:
        pass
    try:
        async with websockets.connect(url, ping_interval=20, ssl=ssl_ctx) as ws:
            await ws.send(json.dumps({"type": "subscribe", "channel": "orderbook", "token_id": token_id}))
            for _ in range(messages):
                msg = await ws.recv()
                print(msg)
    except Exception as exc:
        raise RuntimeError(f"Websocket connection failed (url={url}): {exc}")
