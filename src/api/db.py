import os
import duckdb
from contextlib import contextmanager

DB_PATH = os.environ.get("AFV_DB_PATH", "data/finance_data.db")


def get_conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH, read_only=True)


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
