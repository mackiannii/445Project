"""Simple recorders for Polymarket orderbooks and trades."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from .config import CLOB_BASE, REQUEST_TIMEOUT

try:
    import websockets
except ImportError:
    websockets = None  # type: ignore


def fetch_trades(token_id: str, limit: int = 200, session: Optional[requests.Session] = None) -> Dict[str, Any]:
    """Fetch recent trades for a token. The API returns a limited recent window."""
    sess = session or requests
    resp = sess.get(f"{CLOB_BASE}/trades", params={"token_id": token_id, "limit": limit}, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def append_ndjson(path: Path, obj: Dict[str, Any]) -> None:
    """Append a single JSON object to ndjson file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(obj) + "\n")


async def stream_orderbook_to_file(token_id: str, out_path: str, messages: int = 100, ws_url: Optional[str] = None) -> None:
    """Stream orderbook updates to an ndjson file.

    Requires `pip install websockets`.
    """
    if websockets is None:
        raise RuntimeError("websockets package not installed; pip install websockets")
    url = ws_url or CLOB_BASE.replace("https://", "wss://")
    path = Path(out_path)
    try:
        async with websockets.connect(url, ping_interval=20) as ws:
            await ws.send(json.dumps({"type": "subscribe", "channel": "orderbook", "token_id": token_id}))
            for _ in range(messages):
                msg = await ws.recv()
                append_ndjson(path, {"ts": time.time(), "token_id": token_id, "msg": json.loads(msg)})
    except Exception as exc:
        append_ndjson(Path(out_path), {"ts": time.time(), "token_id": token_id, "error": str(exc)})
        raise


def poll_trades_to_file(token_id: str, out_path: str, interval_sec: int = 10, iterations: int = 30) -> None:
    """Poll recent trades periodically and append to ndjson."""
    path = Path(out_path)
    for i in range(iterations):
        try:
            trades = fetch_trades(token_id)
            append_ndjson(path, {"ts": time.time(), "token_id": token_id, "trades": trades})
        except Exception as exc:
            append_ndjson(path, {"ts": time.time(), "token_id": token_id, "error": str(exc)})
        if i < iterations - 1:
            time.sleep(interval_sec)
