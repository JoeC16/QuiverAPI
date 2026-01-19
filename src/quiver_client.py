import time
import requests
from config import QUIVER_API_KEY

BASE = "https://api.quiverquant.com/beta"

DEFAULT_TIMEOUT = 30
MAX_RETRIES = 3
BACKOFF_S = 1.5


def _headers():
    # Quiver auth commonly works with Authorization: Token <key>
    # Some setups use Bearer. We support both by sending Token.
    return {
        "Authorization": f"Token {QUIVER_API_KEY}",
        "Accept": "application/json",
        "User-Agent": "qq-trade-monitor/1.1",
    }


def _get_json(path: str, params=None):
    url = f"{BASE}{path}"
    last_err = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=_headers(), params=params, timeout=DEFAULT_TIMEOUT)
            ct = (r.headers.get("Content-Type") or "").lower()
            preview = (r.text or "")[:300]

            if r.status_code >= 400:
                raise RuntimeError(
                    f"Quiver API error {r.status_code} for {url}. "
                    f"Content-Type={ct}. Body (first 300 chars): {preview}"
                )

            # Try to parse JSON even if content-type is missing/mis-set
            try:
                return r.json()
            except Exception as e:
                raise RuntimeError(
                    f"Quiver returned non-JSON for {url}. Content-Type={ct}. "
                    f"Body (first 300 chars): {preview}"
                ) from e

        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_S * attempt)
                continue
            raise


# -----------------------
# V1 feeds (latest)
# -----------------------

def fetch_government_trades():
    # Latest congress trades feed
    return _get_json("/live/congresstrading")


def fetch_insider_trades():
    # Insider endpoint varies by plan. Start here; if it 404s, we’ll adjust once from logs.
    return _get_json("/live/insiders")


def fetch_contracts():
    # Latest contract awards feed across all tickers
    return _get_json("/live/govcontractsall")


# -----------------------
# Optional (V1.2+): historical by ticker
# -----------------------

def fetch_congress_trades_by_ticker(ticker: str):
    t = ticker.upper().strip()
    return _get_json(f"/historical/congresstrading/{t}")
