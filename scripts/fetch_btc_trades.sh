#!/usr/bin/env bash
set -euo pipefail

# Fetch recent BTC daily trades via REST.
# Usage: ./scripts/fetch_btc_trades.sh [TOKEN_ID] [LIMIT]

TOKEN="${1:-67050971065985824971774868248045189851067925582529445299029729719421794932546}"
LIMIT="${2:-200}"

curl -s "https://clob.polymarket.com/trades?token_id=${TOKEN}&limit=${LIMIT}"
