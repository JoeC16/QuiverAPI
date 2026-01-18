import hashlib
from datetime import datetime
from storage import init_db, get_conn
from quiver_client import fetch_government_trades, fetch_insider_trades
from scoring import score_government_trade, score_insider_trade
from telegram import send_message
from config import CONFIG

def hash_id(*args):
    return hashlib.sha256("".join(map(str, args)).encode()).hexdigest()

def already_alerted(conn, alert_hash):
    return conn.execute(
        "SELECT 1 FROM alerts_sent WHERE alert_hash=?", (alert_hash,)
    ).fetchone()

def mark_alerted(conn, alert_hash):
    conn.execute(
        "INSERT INTO alerts_sent VALUES (?,?)",
        (alert_hash, datetime.utcnow().isoformat())
    )

def run():
    init_db()
    conn = get_conn()
    cur = conn.cursor()

    # Government trades
    for t in fetch_government_trades():
        tid = hash_id(t["Ticker"], t["TransactionDate"], t["Representative"])
        if cur.execute("SELECT 1 FROM trades WHERE id=?", (tid,)).fetchone():
            continue

        cur.execute(
            "INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                tid, "government", t["Representative"], t["Chamber"],
                t["Ticker"], t["TransactionType"], t.get("Amount", 0),
                t["TransactionDate"], t["DisclosureDate"], t["Link"]
            )
        )

        score, reasons = score_government_trade({
            "side": t["TransactionType"],
            "amount": t.get("Amount", 0),
            "disclosed_date": t["DisclosureDate"],
            "ticker": t["Ticker"]
        })

        if score >= CONFIG["thresholds"]["high_conviction"]:
            ah = hash_id("gov", tid)
            if not already_alerted(conn, ah):
                send_message(
                    f"🚨 HIGH CONVICTION (Gov)\n\n{t['Ticker']} — {score}\n"
                    f"{t['Representative']} ({t['Chamber']})\n"
                    f"Reasons:\n- " + "\n- ".join(reasons)
                )
                mark_alerted(conn, ah)

    # Insider trades
    for t in fetch_insider_trades():
        tid = hash_id(t["Ticker"], t["TransactionDate"], t["InsiderName"])
        if cur.execute("SELECT 1 FROM insider_trades WHERE id=?", (tid,)).fetchone():
            continue

        cur.execute(
            "INSERT INTO insider_trades VALUES (?,?,?,?,?,?,?,?)",
            (
                tid, t["InsiderName"], t.get("Title", ""),
                t["Ticker"], t["TransactionType"],
                t.get("Value", 0), t["TransactionDate"], t["Link"]
            )
        )

        score, reasons = score_insider_trade({
            "side": t["TransactionType"],
            "value": t.get("Value", 0),
            "transaction_date": t["TransactionDate"],
            "role": t.get("Title", "")
        })

        if score >= CONFIG["thresholds"]["high_conviction"]:
            ah = hash_id("insider", tid)
            if not already_alerted(conn, ah):
                send_message(
                    f"🚨 HIGH CONVICTION (Insider)\n\n{t['Ticker']} — {score}\n"
                    f"{t['InsiderName']} ({t.get('Title','')})\n"
                    f"Reasons:\n- " + "\n- ".join(reasons)
                )
                mark_alerted(conn, ah)

    conn.commit()

if __name__ == "__main__":
    run()
