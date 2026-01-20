import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from storage import init_db, get_conn
from quiver_client import fetch_government_trades, fetch_insider_trades, fetch_contracts
from scoring import score_government_trade, score_insider_trade
from telegram import send_message
from config import CONFIG


def hash_id(*args) -> str:
    return hashlib.sha256("".join(map(str, args)).encode("utf-8")).hexdigest()


def safe_get(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


def norm_ticker(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip().upper()
    return s or None


def _parse_dt(value: Any) -> Optional[datetime]:
    """
    Parse Quiver-ish dates safely into UTC-aware datetime.
    Accepts YYYY-MM-DD or ISO datetimes. Returns None if missing/unparseable.
    """
    if value in (None, ""):
        return None

    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        # try date-only
        try:
            dt = datetime.strptime(s[:10], "%Y-%m-%d")
        except Exception:
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def to_iso_date(x: Any) -> str:
    if x in (None, ""):
        return datetime.now(timezone.utc).date().isoformat()
    return str(x)


def already_alerted(conn, alert_hash: str):
    return conn.execute(
        "SELECT 1 FROM alerts_sent WHERE alert_hash=?", (alert_hash,)
    ).fetchone()


def mark_alerted(conn, alert_hash: str):
    conn.execute(
        "INSERT INTO alerts_sent VALUES (?,?)",
        (alert_hash, datetime.now(timezone.utc).isoformat())
    )


def _within_last_hours(dt: Optional[datetime], hours: int) -> bool:
    if not dt:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    return dt >= cutoff


def run():
    init_db()
    conn = get_conn()
    cur = conn.cursor()

    high_conv = CONFIG["thresholds"]["high_conviction"]
    window_hours = int(CONFIG.get("windows", {}).get("lookback_hours", 24))
