import requests
from config import QUIVER_API_KEY

BASE = "https://api.quiverquant.com/beta"

def _headers():
    # Quiver authentication can vary by plan/version.
    # We send both common header styles to be robust.
    return {
        "Authorization": f"Bearer {QUIVER_API_KEY}",
        "X-API-Key": QUIVER_API_KEY,
        "Accept": "application/json",
        "User-Agent": "quiver-trade-monitor/1.1"
    }

def _get_json(path: str):
    url = f"{BASE}{path}"
    r = requests.get(url, headers=_headers(), timeout=30)

    # Fail fast with useful diagnostics
    ct = (r.headers.get("Content-Type") or "").lower()
    if r.status_code != 200:
        raise RuntimeError(
            f"Quiver API error {r.status_code} for {url}. "
            f"Content-Type={ct}. Body (first 300 chars): {r.text[:300]}"
        )

    if "application/json" not in ct:
        raise RuntimeError(
            f"Quiver API returned non-JSON for {url}. "
            f"Content-Type={ct}. Body (first 300 chars): {r.text[:300]}"
        )

    return r.json()

def fetch_government_trades():
    return _get_json("/historical/congresstrading")

def fetch_insider_trades():
    return _get_json("/insidertrading")

def fetch_contracts():
    return _get_json("/governmentcontracts")
