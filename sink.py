"""Sink: читает витрину из sales.mart и складывает её в SQLite (idемпотентно)."""
import sqlite3

from config import DB_PATH, TOPIC_MART

DDL = """
CREATE TABLE IF NOT EXISTS mart_sales_minutes (
    category     TEXT NOT NULL,
    window_start TEXT NOT NULL,
    revenue      REAL NOT NULL,
    orders       INTEGER NOT NULL,
    quantity     INTEGER NOT NULL,
    PRIMARY KEY (category, window_start)
)
"""


def create_db(path):
    conn = sqlite3.connect(path)
    conn.execute(DDL)
    return conn


def write_rows(conn, rows):
    conn.executemany(
        "INSERT OR REPLACE INTO mart_sales_minutes "
        "(category, window_start, revenue, orders, quantity) VALUES (?, ?, ?, ?, ?)",
        [(r["category"], r["window_start"], r["revenue"], r["orders"], r["quantity"]) for r in rows],
    )
    conn.commit()


def run_from_broker(broker, db_path=DB_PATH):
    conn = create_db(db_path)
    rows = []
    for key, value in broker.consume(TOPIC_MART, group_id="etl-sink"):
        if isinstance(value, dict) and value.get("__eos__"):
            break
        rows.append(value)
    write_rows(conn, rows)
    conn.close()
    return rows
