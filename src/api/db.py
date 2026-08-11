import os
import duckdb
from contextlib import contextmanager

DB_PATH = os.environ.get("AFV_DB_PATH", "data/finance_data.db")


MEM_LIMIT = os.environ.get("AFV_DUCKDB_MEMORY", "500MB")


def get_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(DB_PATH, read_only=True)
    conn.execute(f"SET memory_limit='{MEM_LIMIT}'")
    return conn


@contextmanager
def db_cursor():
    conn = get_conn()
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def db_write_cursor():
    conn = duckdb.connect(DB_PATH, read_only=False)
    try:
        yield conn
    finally:
        conn.close()
