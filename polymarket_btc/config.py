"""Shared configuration for Polymarket BTC helpers."""

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
CLOB_WS_URL = "wss://clob.polymarket.com/ws"
BTC_DAILY_SERIES_ID = 41
ETH_DAILY_SERIES_ID = 40

# Sleep between calls to play nicely with public API limits.
CALL_DELAY_SEC = 0.1
REQUEST_TIMEOUT = 10
