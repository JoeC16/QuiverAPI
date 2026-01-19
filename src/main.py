import hashlib
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from storage import init_db, get_conn
from quiver_client import fetch_government_trades, fetch_insider_trades
from scoring import score_government_trade, score_insider_trade
from telegram import send_message
from config import CONFIG


def hash_id(*args) -> str:
    return hashlib.sha256("".join(map(str, args)).encode("utf-8")).hexdigest()


def safe_get(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """
    Return the first non-empty value for the given keys.
    Treats None/"" as missing.
    """
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


def norm_ticker(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip().upper()
    return s or None


def to_iso_date(x: Any) -> str:
    """
    Keep it simple: Quiver often returns ISO-ish strings already.
    If missing, return UTC now for storage (but this should be rare).
    """
    if x in (None, ""):
        return datetime.utcnow().date().isoformat()
    return str(x)


def already_alerted(conn, alert_hash: str):
    return conn.execute(
        "SELECT 1 FROM alerts_sent WHERE alert_hash=?", (alert_hash,)
    ).fetchone()


def mark_alerted(conn, alert_hash: str):
    conn.execute(
        "INSERT INTO alerts_sent VALUES (?,?)",
        (alert_hash, datetime.utcnow().isoformat())
    )


def run():
    init_db()
    conn = get_conn()
    cur = conn.cursor()

    high_conv = CONFIG["thresholds"]["high_conviction"]

    # -------------------------
    # Government trades
    # -------------------------
    for t in fetch_government_trades() or []:
        # Field normalization / fallbacks
        ticker = norm_ticker(safe_get(t, "Ticker", "ticker", "Symbol", default=None))
        if not ticker:
            # Skip malformed rows rather than crashing the run
            continue

        rep = safe_get(t, "Representative", "RepresentativeName", "Name", default="Unknown")
        # LIVE feed often does not include Chamber; infer a safe default
        chamber = safe_get(t, "Chamber", "chamber", default="Congress")

        side = safe_get(t, "TransactionType", "Transaction", "transaction", default="Unknown")
        amt = safe_get(t, "Amount", "amount", "AmountRange", default=0)

        tx_date = to_iso_date(safe_get(t, "TransactionDate", "TransactionDateTime", "Date", default=None))
        disc_date = to_iso_date(safe_get(t, "DisclosureDate", "DisclosedDate", "ReportDate", default=None))

        link = safe_get(t, "Link", "FilingLink", "URL", default="")

        # Build stable ID from best-available fields
        tid = hash_id("gov", ticker, tx_date, disc_date, rep, side, amt)

        if cur.execute("SELECT 1 FROM trades WHERE id=?", (tid,)).fetchone():
            continue

        # Insert (make sure your DB schema matches this column order)
        cur.execute(
            "INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                tid,
                "government",
                rep,
                chamber,
                ticker,
                side,
                amt,
                tx_date,
                disc_date,
                link,
            )
        )

        score, reasons = score_government_trade({
            "side": side,
            "amount": amt,
            "disclosed_date": disc_date,
            "ticker": ticker,
            "actor": rep,
            "chamber": chamber,
            "transaction_date": tx_date,
        })

        if score >= high_conv:
            ah = hash_id("gov_alert", tid)
            if not already_alerted(conn, ah):
                send_message(
                    "🚨 HIGH CONVICTION (Gov)\n\n"
                    f"{ticker} — {score}\n"
                    f"{rep} ({chamber})\n"
                    f"Txn: {side} | Amt: {amt}\n"
                    f"Txn Date: {tx_date} | Disclosed: {disc_date}\n\n"
                    "Reasons:\n- " + "\n- ".join(reasons[:8]) +
                    (f"\n\nLink: {link}" if link else "")
                )
                mark_alerted(conn, ah)

    # -------------------------
    # Insider trades
    # -------------------------
    for t in fetch_insider_trades() or []:
        ticker = norm_ticker(safe_get(t, "Ticker", "ticker", "Symbol", default=None))
        if not ticker:
            continue

        insider = safe_get(t, "InsiderName", "Insider", "Name", default="Unknown")
        title = safe_get(t, "Title", "OfficerTitle", "Role", default="")

        side = safe_get(t, "TransactionType", "Transaction", "transaction", default="Unknown")
        value = safe_get(t, "Value", "value", "TotalValue", "TransactionValue", default=0)

        tx_date = to_iso_date(safe_get(t, "TransactionDate", "TransactionDateTime", "Date", default=None))
        link = safe_get(t, "Link", "FilingLink", "URL", default="")

        tid = hash_id("insider", ticker, tx_date, insider, side, value, title)

        if cur.execute("SELECT 1 FROM insider_trades WHERE id=?", (tid,)).fetchone():
            continue

        cur.execute(
            "INSERT INTO insider_trades VALUES (?,?,?,?,?,?,?,?)",
            (
                tid,
                insider,
                title,
                ticker,
                side,
                value,
                tx_date,
                link,
            )
        )

        score, reasons = score_insider_trade({
            "side": side,
            "value": value,
            "transaction_date": tx_date,
            "role": title,
            "ticker": ticker,
            "actor": insider,
        })

        if score >= high_conv:
            ah = hash_id("insider_alert", tid)
            if not already_alerted(conn, ah):
                send_message(
                    "🚨 HIGH CONVICTION (Insider)\n\n"
                    f"{ticker} — {score}\n"
                    f"{insider} ({title})\n"
                    f"Txn: {side} | Value: {value}\n"
                    f"Date: {tx_date}\n\n"
                    "Reasons:\n- " + "\n- ".join(reasons[:8]) +
                    (f"\n\nLink: {link}" if link else "")
                )
                mark_alerted(conn, ah)

    conn.commit()


if __name__ == "__main__":
    run()
