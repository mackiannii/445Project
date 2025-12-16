#!/usr/bin/env bash
set -euo pipefail

# Fetch BTC daily orderbook via REST.
# Usage: ./scripts/fetch_btc_orderbook.sh [TOKEN_ID]

TOKEN="${1:-67050971065985824971774868248045189851067925582529445299029729719421794932546}"

curl -s "https://clob.polymarket.com/book?token_id=${TOKEN}"
