import time
import requests
from config import QUIVER_API_KEY

BASE = "https://api.quiverquant.com/beta"

DEFAULT_TIMEOUT = 30
MAX_RETRIES = 3
BACKOFF_S = 1.5


def _headers():
    # Hobbyist auth commonly works with Authorization: Token <key>
    # If your account ever requires X-API-Key, add it here too.
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
            raise last_err


def _safe_dataset(callable_fn, dataset_name: str):
    """
    Hobbyist plans will 403 certain datasets.
    Some endpoints may also 404 depending on plan/version.
    We skip those rather than failing the whole run.
    """
    try:
        return callable_fn()
    except RuntimeError as e:
        msg = str(e).lower()
        if "upgrade your subscription plan" in msg or "error 403" in msg:
            print(f"[quiver_client] {dataset_name}: not available on current plan (403). Skipping.")
            return []
        if "error 404" in msg or "not found" in msg:
            print(f"[quiver_client] {dataset_name}: endpoint not found (404). Skipping.")
            return []
        raise


# -----------------------
# Hobbyist-compliant endpoints
# -----------------------

def fetch_government_trades():
    # Hobbyist: LIVE congress trades feed (historical congresstrading 404s on hobbyist)
    return _safe_dataset(lambda: _get_json("/live/congresstrading"), "government_trades")


def fetch_insider_trades():
    # Hobbyist: historical insider trading (live insiders is typically gated)
    return _safe_dataset(lambda: _get_json("/historical/insidertrading"), "insider_trades")


def fetch_contracts():
    # Contracts are commonly gated (403) on hobbyist, but keep safe fallbacks.
    def _fetch():
        try:
            return _get_json("/historical/governmentcontracts")
        except RuntimeError as e:
            # If historical path not present, try alternate common route
            if "error 404" in str(e).lower() or "not found" in str(e).lower():
                return _get_json("/governmentcontracts")
            raise

    return _safe_dataset(_fetch, "contracts")
