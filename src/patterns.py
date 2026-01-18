from datetime import datetime, timedelta
from storage import get_conn

def detect_cluster(ticker, days=10):
    conn = get_conn()
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    count = conn.execute(
        "SELECT COUNT(*) c FROM trades WHERE ticker=? AND disclosed_date>=?",
        (ticker, since)
    ).fetchone()["c"]
    return count >= 3

def detect_contract_timing(ticker, trade_date, window=14):
    conn = get_conn()
    start = (trade_date - timedelta(days=window)).isoformat()
    end = (trade_date + timedelta(days=window)).isoformat()
    count = conn.execute(
        "SELECT COUNT(*) c FROM contracts WHERE ticker=? AND award_date BETWEEN ? AND ?",
        (ticker, start, end)
    ).fetchone()["c"]
    return count > 0
