import sqlite3
from pathlib import Path

DB_PATH = Path("data.db")

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.executescript("""
    CREATE TABLE IF NOT EXISTS trades (
        id TEXT PRIMARY KEY,
        source TEXT,
        person TEXT,
        chamber TEXT,
        ticker TEXT,
        side TEXT,
        amount REAL,
        transaction_date TEXT,
        disclosed_date TEXT,
        url TEXT
    );

    CREATE TABLE IF NOT EXISTS insider_trades (
        id TEXT PRIMARY KEY,
        insider TEXT,
        role TEXT,
        ticker TEXT,
        side TEXT,
        value REAL,
        transaction_date TEXT,
        url TEXT
    );

    CREATE TABLE IF NOT EXISTS contracts (
        id TEXT PRIMARY KEY,
        ticker TEXT,
        award_date TEXT,
        amount REAL,
        agency TEXT,
        description TEXT
    );

    CREATE TABLE IF NOT EXISTS prices (
        ticker TEXT,
        date TEXT,
        close REAL,
        PRIMARY KEY (ticker, date)
    );

    CREATE TABLE IF NOT EXISTS alerts_sent (
        alert_hash TEXT PRIMARY KEY,
        sent_at TEXT
    );
    """)
    conn.commit()
    conn.close()
