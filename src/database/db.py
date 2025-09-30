import duckdb

DB_PATH = '../../data/finance_data.db'

def init_schema():
    with duckdb.connect(DB_PATH) as con:
        con.execute("""
            create table if not exists tickers (
                ticker_pk varchar primary key, --sha1(isin + mic)
                updated_at timestamp default current_timestamp,
                isin varchar,
                mic varchar,
                raw_ticker varchar,
                yahoo_ticker varchar,
                asset_name text
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

def drop_schema():
    with duckdb.connect(DB_PATH) as con:
        con.execute("DROP TABLE IF EXISTS tickers;")
        con.execute("DROP TABLE IF EXISTS yahoo_data;")
        con.execute("DROP TABLE IF EXISTS afv_20_scores;")

def truncate_schema():
    with duckdb.connect(DB_PATH) as con:
        con.execute("DELETE FROM tickers;")
        con.execute("DELETE FROM yahoo_data;")
        con.execute("DELETE FROM afv_20_scores;")

# Example usage
if __name__ == "__main__":
    #drop_schema()
    init_schema()
