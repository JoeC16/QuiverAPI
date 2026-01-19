from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Tuple, List

from config import CONFIG
from patterns import detect_cluster, detect_contract_timing


# -----------------------
# Helpers
# -----------------------

def _parse_money_to_int(value: Any) -> int:
    """
    Convert Quiver-style amount/value strings into a numeric proxy.
    Examples:
      "50,001 - 100,000" -> 50001 (lower bound)
      "1,000,000+"       -> 1000000
      "250,000"          -> 250000
      None / "" / "--"   -> 0
      12345              -> 12345
    """
    if value in (None, "", "--"):
        return 0

    if isinstance(value, (int, float)):
        return int(value)

    s = str(value).strip()
    if not s:
        return 0

    s = s.replace(",", "")

    # Handle ranges: take the lower bound
    if "-" in s:
        left = s.split("-", 1)[0].strip()
        nums = re.findall(r"\d+", left)
        return int(nums[0]) if nums else 0

    # Handle plus values: "100000+"
    if "+" in s:
        nums = re.findall(r"\d+", s)
        return int(nums[0]) if nums else 0

    nums = re.findall(r"\d+", s)
    return int(nums[0]) if nums else 0


def _parse_iso_dt(value: Any) -> datetime:
    """
    Parse ISO-ish date/datetime safely.
    - Accepts date-only 'YYYY-MM-DD'
    - Accepts full ISO datetime, with or without Z
    - Returns timezone-aware UTC datetime
    """
    if value in (None, ""):
        return datetime.now(timezone.utc)

    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        # If it's date-only but not ISO compliant somehow, fallback:
        try:
            dt = datetime.strptime(s[:10], "%Y-%m-%d")
        except Exception:
            dt = datetime.now(timezone.utc)

    # Make timezone-aware UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt


def _norm_side(side: Any) -> str:
    """
    Normalize side to BUY/SELL/UNKNOWN from various Quiver strings.
    """
    s = (str(side or "")).strip().lower()
    if any(k in s for k in ["buy", "purchase", "acquire"]):
        return "BUY"
    if any(k in s for k in ["sell", "sale", "dispose"]):
        return "SELL"
    return "UNKNOWN"


# -----------------------
# Scoring
# -----------------------

def score_government_trade(trade) -> Tuple[int, List[str]]:
    score = 0
    reasons: List[str] = []

    side = _norm_side(trade.get("side"))
    amount = _parse_money_to_int(trade.get("amount"))

    if side == "BUY":
        score += CONFIG["scoring"]["buy_base"]
        reasons.append("Government buy")
    else:
        score += CONFIG["scoring"]["sell_penalty"]

    if amount > 50000:
        score += CONFIG["scoring"]["large_trade_bonus"]
        reasons.append("Large disclosed amount")

    disclosed = _parse_iso_dt(trade.get("disclosed_date"))
    if (datetime.now(timezone.utc) - disclosed).days <= 5:
        score += CONFIG["scoring"]["recency_bonus"]
        reasons.append("Recent disclosure")

    ticker = trade.get("ticker")
    if ticker and detect_cluster(ticker):
        score += CONFIG["scoring"]["cluster_bonus"]
        reasons.append("Cluster buying")

    if ticker and detect_contract_timing(ticker, disclosed):
        score += CONFIG["scoring"]["contract_bonus"]
        reasons.append("Contract timing")

    return min(score, 100), reasons


def score_insider_trade(trade) -> Tuple[int, List[str]]:
    score = 0
    reasons: List[str] = []

    side = _norm_side(trade.get("side"))
    value = _parse_money_to_int(trade.get("value"))
    role = (trade.get("role") or "").lower()

    if side == "BUY":
        score += CONFIG["scoring"]["insider_buy_bonus"]
        reasons.append("Insider buy")
    else:
        score += CONFIG["scoring"]["sell_penalty"]

    if role and any(r in role for r in ["ceo", "cfo", "cto", "president"]):
        score += CONFIG["scoring"]["exec_role_bonus"]
        reasons.append("Executive role")

    if value > 100000:
        score += CONFIG["scoring"]["large_trade_bonus"]
        reasons.append("Large insider purchase")

    tx_date = _parse_iso_dt(trade.get("transaction_date"))
    if (datetime.now(timezone.utc) - tx_date).days <= 5:
        score += CONFIG["scoring"]["recency_bonus"]
        reasons.append("Recent transaction")

    return min(score, 100), reasons
