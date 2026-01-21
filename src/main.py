import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, List

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


def _within_last_days(dt: Optional[datetime], days: int) -> bool:
    if not dt:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return dt >= cutoff


def _format_pick(i: int, p: Dict[str, Any]) -> str:
    who = p.get("actor", "Unknown")
    role = p.get("role") or p.get("chamber") or ""
    role_str = f" ({role})" if role else ""
    filed = p.get("filed", "")
    side = p.get("side", "")
    amt_label = "Amt" if p.get("kind") == "government" else "Val"
    amt = p.get("amount") if p.get("kind") == "government" else p.get("value")
    amt = amt if amt not in (None, "") else ""
    reasons = "; ".join((p.get("reasons") or [])[:3])
    link = p.get("link") or ""

    s = (
        f"{i}) {p['ticker']} — {p['score']} — {side}\n"
        f"   {who}{role_str} | {amt_label}: {amt} | Filed: {filed}\n"
        f"   {reasons}"
    )
    if link:
        s += f"\n   {link}"
    return s


def run():
    init_db()
    conn = get_conn()
    cur = conn.cursor()

    # Config
    high_conv = int(CONFIG.get("thresholds", {}).get("high_conviction", 85))
    min_digest = int(CONFIG.get("thresholds", {}).get("digest_min_score", 0))
    lookback_days = int(CONFIG.get("windows", {}).get("lookback_days", 7))
    top_n = int(CONFIG.get("digest", {}).get("top_n", 10))

    # Optional: warm contracts cache / patterns (safe to call; may return [])
    try:
        _ = fetch_contracts()
    except Exception as e:
        print(f"[main] contracts fetch failed (non-fatal): {e}")

    gov_picks: List[Dict[str, Any]] = []
    insider_picks: List[Dict[str, Any]] = []

    # -------------------------
    # Government trades (historical)
    # -------------------------
    for t in fetch_government_trades() or []:
        ticker = norm_ticker(safe_get(t, "Ticker", "ticker", "Symbol", default=None))
        if not ticker:
            continue

        rep = safe_get(t, "Representative", "RepresentativeName", "Name", default="Unknown")
        chamber = safe_get(t, "Chamber", "chamber", "House", "Senate", default="Congress")

        side = safe_get(t, "TransactionType", "Transaction", "transaction", default="Unknown")
        amt = safe_get(t, "Amount", "amount", "AmountRange", default=0)

        tx_date_raw = safe_get(t, "TransactionDate", "TransactionDateTime", "Date", default=None)
        disc_date_raw = safe_get(t, "DisclosureDate", "DisclosedDate", "ReportDate", "FilingDate", default=None)

        tx_date = to_iso_date(tx_date_raw)
        disc_date = to_iso_date(disc_date_raw)

        disc_dt = _parse_dt(disc_date_raw) or _parse_dt(disc_date)
        if lookback_days and not _within_last_days(disc_dt, lookback_days):
            continue

        link = safe_get(t, "Link", "FilingLink", "URL", default="")

        tid = hash_id("gov", ticker, tx_date, disc_date, rep, side, amt)
        if cur.execute("SELECT 1 FROM trades WHERE id=?", (tid,)).fetchone():
            continue

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

        if score >= min_digest:
            gov_picks.append({
                "kind": "government",
                "ticker": ticker,
                "score": score,
                "side": side,
                "amount": amt,
                "actor": rep,
                "chamber": chamber,
                "filed": disc_date,
                "link": link,
                "reasons": reasons,
                "tid": tid,
            })

        # Optional: keep high conviction as immediate-style alert
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
                    (f"\n\nLink: {link}" if link else "") +
                    "\n\nNot financial advice."
                )
                mark_alerted(conn, ah)

    # -------------------------
    # Insider trades (historical)
    # -------------------------
    for t in fetch_insider_trades() or []:
        ticker = norm_ticker(safe_get(t, "Ticker", "ticker", "Symbol", default=None))
        if not ticker:
            continue

        insider = safe_get(t, "InsiderName", "Insider", "Name", default="Unknown")
        title = safe_get(t, "Title", "OfficerTitle", "Role", default="")

        side = safe_get(t, "TransactionType", "Transaction", "transaction", default="Unknown")
        value = safe_get(t, "Value", "value", "TotalValue", "TransactionValue", "Amount", default=0)

        tx_date_raw = safe_get(t, "TransactionDate", "TransactionDateTime", "Date", default=None)
        filing_raw = safe_get(t, "FilingDate", "DisclosureDate", "ReportedDate", "ReportDate", default=None)

        tx_date = to_iso_date(tx_date_raw)
        filing_date = to_iso_date(filing_raw) if filing_raw else tx_date

        filing_dt = _parse_dt(filing_raw) or _parse_dt(filing_date)
        if lookback_days and not _within_last_days(filing_dt, lookback_days):
            continue

        link = safe_get(t, "Link", "FilingLink", "URL", default="")

        tid = hash_id("insider", ticker, tx_date, filing_date, insider, side, value, title)
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

        if score >= min_digest:
            insider_picks.append({
                "kind": "insider",
                "ticker": ticker,
                "score": score,
                "side": side,
                "value": value,
                "actor": insider,
                "role": title,
                "filed": filing_date,
                "link": link,
                "reasons": reasons,
                "tid": tid,
            })

        if score >= high_conv:
            ah = hash_id("insider_alert", tid)
            if not already_alerted(conn, ah):
                send_message(
                    "🚨 HIGH CONVICTION (Insider)\n\n"
                    f"{ticker} — {score}\n"
                    f"{insider} ({title})\n"
                    f"Txn: {side} | Value: {value}\n"
                    f"Txn Date: {tx_date} | Filed: {filing_date}\n\n"
                    "Reasons:\n- " + "\n- ".join(reasons[:8]) +
                    (f"\n\nLink: {link}" if link else "") +
                    "\n\nNot financial advice."
                )
                mark_alerted(conn, ah)

    # -------------------------
    # Digest: Top N in last X days
    # -------------------------
    all_picks = sorted(gov_picks + insider_picks, key=lambda x: x["score"], reverse=True)
    top = all_picks[:top_n]

    header = (
        f"📌 Digest (Top {top_n}) — last {lookback_days} days\n"
        f"Min score: {min_digest} | Candidates: {len(all_picks)}\n\n"
    )

    if not top:
        send_message(header + "No trades met the minimum score.\n\nNot financial advice.")
    else:
        lines = []
        for i, p in enumerate(top, start=1):
            lines.append(_format_pick(i, p))
        send_message(header + "\n\n".join(lines) + "\n\nNot financial advice.")

    conn.commit()


if __name__ == "__main__":
    run()
