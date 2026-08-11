from pathlib import Path
import sys
import duckdb

DB_PATH = str(Path(__file__).resolve().parent.parent.parent / 'data' / 'finance_data.db')


def connect(db_path: str = DB_PATH, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection. Pass read_only=True when no writes are needed."""
    try:
        return duckdb.connect(db_path, read_only=read_only)
    except duckdb.IOException as e:
        if 'lock' in str(e).lower():
            print(f"ERROR: {db_path} is locked by another process.")
            print("You probably have an open DuckDB connection in a Jupyter notebook.")
            print("Close it with:  con.close()")
            print("or restart the notebook kernel, then re-run this command.")
            sys.exit(1)
        raise


def migrate(db_path: str = DB_PATH):
    """Run migrations for existing databases."""
    with connect(db_path) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS fx_rates (
                date     DATE    NOT NULL,
                currency TEXT    NOT NULL,
                eur_rate REAL    NOT NULL,
                PRIMARY KEY (date, currency)
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS holding_thesis (
                symbol     TEXT PRIMARY KEY,
                thesis     TEXT NOT NULL DEFAULT '',
                updated_at TIMESTAMP DEFAULT current_timestamp
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                symbol  VARCHAR,
                date    DATE,
                open    REAL,
                high    REAL,
                low     REAL,
                close   REAL,
                volume  BIGINT
            )
        """)
        con.execute("ALTER TABLE tickers ADD COLUMN IF NOT EXISTS is_dead boolean default false")
        con.execute("ALTER TABLE tickers ADD COLUMN IF NOT EXISTS dead_reason varchar")
        con.execute("ALTER TABLE tickers ADD COLUMN IF NOT EXISTS dead_since TIMESTAMP")
        con.execute("ALTER TABLE tickers ADD COLUMN IF NOT EXISTS last_revival_attempt TIMESTAMP")
        con.execute("ALTER TABLE technical_analysis ADD COLUMN IF NOT EXISTS ma50_bottom_days_ago INTEGER")
        con.execute("ALTER TABLE technical_analysis ADD COLUMN IF NOT EXISTS ma200_trend TEXT")
        con.execute("ALTER TABLE technical_analysis ADD COLUMN IF NOT EXISTS ma200_bottom_days_ago INTEGER")
        con.execute("""
            CREATE TABLE IF NOT EXISTS holdings (
                fetched_at   TIMESTAMP DEFAULT current_timestamp,
                yahoo_symbol TEXT,
                symbol       TEXT,
                description  TEXT,
                isin         TEXT,
                exchange     TEXT,
                currency     TEXT,
                position     REAL,
                mark_price   REAL,
                pos_value    REAL,
                cost_basis   REAL
            )
        """)

def init_schema(db_path: str = DB_PATH):
    with connect(db_path) as con:
        con.execute("""
            create table if not exists tickers (
                ticker_pk varchar primary key, --sha1(isin + mic)
                updated_at timestamp default current_timestamp,
                isin varchar,
                mic varchar,
                raw_ticker varchar,
                yahoo_ticker varchar,
                asset_name text,
                is_dead boolean default false,
                dead_reason varchar
            );
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS yahoo_data (
                symbol varchar,
                dataset varchar, -- 'info', 'quarterly_cashflow', 'balance_sheet', etc.
                data json,
                ts timestamp default current_timestamp,
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS afv_20_scores (
            symbol TEXT,
            afv REAL,
            rp REAL,
            fcf_yield REAL,
            ocf_margin REAL,
            min_ocf_margin REAL,
            ocf_margin_volatility REAL,
            sector_score REAL,
            geo_score REAL,
            debt_score REAL,
            trend_score REAL,
            vd_score REAL,
            computed_at TIMESTAMP DEFAULT current_timestamp
        );""")

        con.execute("""
            CREATE TABLE IF NOT EXISTS afv_21_scores (
            symbol TEXT,
            afv REAL,
            afv21 REAL,
            rp REAL,
            rp21 REAL,
            fcf_yield REAL,
            ocf_margin REAL,
            min_ocf_margin REAL,
            ocf_margin_volatility REAL,
            sector_score REAL,
            geo_score REAL,
            debt_score REAL,
            trend_score REAL,
            vd_score REAL,
            computed_at TIMESTAMP DEFAULT current_timestamp
        );""")

        con.execute("""
            CREATE TABLE IF NOT EXISTS holdings (
                fetched_at   TIMESTAMP DEFAULT current_timestamp,
                yahoo_symbol TEXT,
                symbol       TEXT,
                description  TEXT,
                isin         TEXT,
                exchange     TEXT,
                currency     TEXT,
                position     REAL,
                mark_price   REAL,
                pos_value    REAL,
                cost_basis   REAL
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                symbol  VARCHAR,
                date    DATE,
                open    REAL,
                high    REAL,
                low     REAL,
                close   REAL,
                volume  BIGINT
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS fx_rates (
                date     DATE    NOT NULL,
                currency TEXT    NOT NULL,
                eur_rate REAL    NOT NULL,
                PRIMARY KEY (date, currency)
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS technical_analysis (
                symbol              TEXT,
                computed_at         TIMESTAMP DEFAULT current_timestamp,
                afv21_score         REAL,
                close_price         REAL,
                ma50                REAL,
                ma200               REAL,
                ma_cross_signal     TEXT,
                ma_cross_days_ago   INTEGER,
                ma_distance_pct     REAL,
                rsi14               REAL,
                macd_line           REAL,
                macd_signal_line    REAL,
                macd_histogram      REAL,
                macd_sentiment      TEXT,
                obv                 REAL,
                obv_trend           TEXT,
                ma50_bottom_days_ago  INTEGER,
                ma200_trend           TEXT,
                ma200_bottom_days_ago INTEGER
            )
        """)

def drop_schema(db_path: str = DB_PATH):
    with connect(db_path) as con:
        con.execute("DROP TABLE IF EXISTS tickers;")
        con.execute("DROP TABLE IF EXISTS yahoo_data;")
        con.execute("DROP TABLE IF EXISTS afv_20_scores;")

def truncate_schema(db_path: str = DB_PATH):
    with connect(db_path) as con:
        con.execute("DELETE FROM tickers;")
        con.execute("DELETE FROM yahoo_data;")
        con.execute("DELETE FROM afv_20_scores;")

if __name__ == "__main__":
    init_schema()
    migrate()
