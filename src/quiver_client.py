import time
import requests
from config import QUIVER_API_KEY

BASE = "https://api.quiverquant.com/beta"

DEFAULT_TIMEOUT = 30
MAX_RETRIES = 3
BACKOFF_S = 1.5


def _headers():
    # Quiver auth for hobbyist commonly works with Authorization: Token <key>
    # If your account ever requires X-API-Key, add it back.
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
                # Raise with diagnostics for Actions logs
                raise RuntimeError(
                    f"Quiver API error {r.status_code} for {url}. "
                    f"Content-Type={ct}. Body (first 300 chars): {preview}"
                )

            # Parse JSON even if content-type is mis-set
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
    We skip those rather than failing the whole run.
    """
    try:
        return callable_fn()
    except RuntimeError as e:
        msg = str(e)
        if "Upgrade your subscription plan" in msg or "error 403" in msg:
            print(f"[quiver_client] {dataset_name}: not available on current plan. Skipping.")
            return []
        raise


# -----------------------
# Hobbyist-compliant (historical) endpoints
# -----------------------

def fetch_government_trades():
    # Hobbyist: use historical congress trading
    return _safe_dataset(lambda: _get_json("/historical/congresstrading"), "government_trades")


def fetch_insider_trades():
    # Hobbyist: use historical insider trading (NOT /live/insiders)
    return _safe_dataset(lambda: _get_json("/historical/insidertrading"), "insider_trades")


def fetch_contracts():
    # Hobbyist: contracts endpoint varies; try the most common historical path first,
    # then fall back to the non-historical route if needed.
    def _fetch():
        try:
            return _get_json("/historical/governmentcontracts")
        except RuntimeError as e:
            # If historical path not present for your account, try the alternate
            if "error 404" in str(e) or "Not Found" in str(e):
                return _get_json("/governmentcontracts")
            raise

    return _safe_dataset(_fetch, "contracts")
