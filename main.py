import duckdb
import yfinance as yf
import pandas as pd


def create_database():
    conn = duckdb.connect('finance_data.db')
    return conn


def fetch_stock_data(symbol, period='1y'):
    ticker = yf.Ticker(symbol)
    data = ticker.history(period=period)
    return data


def store_data_in_duckdb(conn, data, symbol):
    data.reset_index(inplace=True)
    data['Symbol'] = symbol
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            Date DATE,
            Open DOUBLE,
            High DOUBLE,
            Low DOUBLE,
            Close DOUBLE,
            Volume BIGINT,
            Dividends DOUBLE,
            Stock_Splits DOUBLE,
            Symbol VARCHAR
        )
    """)
    conn.execute("INSERT INTO stock_prices SELECT * FROM data")


if __name__ == "__main__":
    conn = create_database()

    symbol = "AAPL"
    data = fetch_stock_data(symbol)
    store_data_in_duckdb(conn, data, symbol)

    result = conn.execute("SELECT * FROM stock_prices LIMIT 5").fetchall()
    print(f"Sample data for {symbol}:")
    for row in result:
        print(row)

    conn.close()