from datetime import datetime
from config import CONFIG
from patterns import detect_cluster, detect_contract_timing

def score_government_trade(trade):
    score = 0
    reasons = []

    if trade["side"] == "BUY":
        score += CONFIG["scoring"]["buy_base"]
        reasons.append("Government buy")
    else:
        score += CONFIG["scoring"]["sell_penalty"]

    if trade["amount"] and trade["amount"] > 50000:
        score += CONFIG["scoring"]["large_trade_bonus"]
        reasons.append("Large disclosed amount")

    disclosed = datetime.fromisoformat(trade["disclosed_date"])
    if (datetime.utcnow() - disclosed).days <= 5:
        score += CONFIG["scoring"]["recency_bonus"]
        reasons.append("Recent disclosure")

    if detect_cluster(trade["ticker"]):
        score += CONFIG["scoring"]["cluster_bonus"]
        reasons.append("Cluster buying")

    if detect_contract_timing(trade["ticker"], disclosed):
        score += CONFIG["scoring"]["contract_bonus"]
        reasons.append("Contract timing")

    return min(score, 100), reasons


def score_insider_trade(trade):
    score = 0
    reasons = []

    if trade["side"] == "BUY":
        score += CONFIG["scoring"]["insider_buy_bonus"]
        reasons.append("Insider buy")
    else:
        score += CONFIG["scoring"]["sell_penalty"]

    if trade["role"] and any(r in trade["role"].lower() for r in ["ceo", "cfo", "cto", "president"]):
        score += CONFIG["scoring"]["exec_role_bonus"]
        reasons.append("Executive role")

    if trade["value"] and trade["value"] > 100000:
        score += CONFIG["scoring"]["large_trade_bonus"]
        reasons.append("Large insider purchase")

    tx_date = datetime.fromisoformat(trade["transaction_date"])
    if (datetime.utcnow() - tx_date).days <= 5:
        score += CONFIG["scoring"]["recency_bonus"]
        reasons.append("Recent transaction")

    return min(score, 100), reasons
