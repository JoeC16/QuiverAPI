import requests
from config import QUIVER_API_KEY

BASE = "https://api.quiverquant.com/beta"
HEADERS = {"Authorization": f"Bearer {QUIVER_API_KEY}"}

def fetch_government_trades():
    return requests.get(f"{BASE}/historical/congresstrading", headers=HEADERS).json()

def fetch_insider_trades():
    return requests.get(f"{BASE}/insidertrading", headers=HEADERS).json()

def fetch_contracts():
    return requests.get(f"{BASE}/governmentcontracts", headers=HEADERS).json()
