#!/usr/bin/env python3
"""
Ticker management utilities: add individual tickers, mark dead, revive, refresh from GitHub/CBOE.

Usage:
    python manage_tickers.py [--db PATH] add SPCX --mic XNYS --name "SPDR S&P 500 Carbon Efficiency ETF"
    python manage_tickers.py [--db PATH] dead SPCX
    python manage_tickers.py [--db PATH] revive SPCX
    python manage_tickers.py [--db PATH] list-dead
    python manage_tickers.py [--db PATH] refresh-us
    python manage_tickers.py [--db PATH] refresh-eu
"""
import argparse
import hashlib
import io
import json
import sys
import urllib.request
from pathlib import Path

import duckdb

DB_PATH = str(Path(__file__).resolve().parent.parent.parent / 'data' / 'finance_data.db')


def _connect(db_path: str) -> duckdb.DuckDBPyConnection:
    try:
        return duckdb.connect(db_path)
    except duckdb.IOException as e:
        if 'lock' in str(e).lower():
            print(f"ERROR: {db_path} is locked by another process.")
            print("You probably have an open DuckDB connection in a Jupyter notebook.")
            print("Close it with:  con.close()")
            print("or restart the notebook kernel, then re-run this command.")
            sys.exit(1)
        raise

US_MIC_MAP = {
    'NYSE': 'XNYS',
    'NASDAQ': 'XNMS',
    'AMEX': 'XASE',
    'XNYS': 'XNYS',
    'XNMS': 'XNMS',
    'XASE': 'XASE',
}

_GITHUB_BASE = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main"

_US_SOURCES = [
    ("amex/amex_tickers.json",    "XASE"),
    ("nasdaq/nasdaq_tickers.json", "XNMS"),
    ("nyse/nyse_tickers.json",    "XNYS"),
]

_EU_CSV_URL = "https://accounteu.cboe.com/cxe/market_data/symbol_listing/csv/"

_EU_YAHOO_SUFFIX = {
    'MTAA': '.MI',
    'XWAR': '.WA',
    'XLON': '.L',
    'XETR': '.DE',
    'XPAR': '.PA',
    'XAMS': '.AS',
    'XSWX': '.SW',
    'XMIL': '.MI',
    'XMAD': '.MC',
    'XSTO': '.ST',
    'XOSL': '.OL',
    'XCSE': '.CO',
    'XHEL': '.HE',
    'XBRU': '.BR',
    'XLIS': '.LS',
    'XWBO': '.VI',
    'XATH': '.AT',
}


def _eu_raw_ticker(bloomberg_primary: str) -> str | None:
    """Extract symbol from Bloomberg 'SYMBOL COUNTRY Equity' format."""
    s = (bloomberg_primary or '').strip()
    return s.split(' ', 1)[0] if s else None


def _eu_yahoo_ticker(raw: str, mic: str) -> str | None:
    if not raw:
        return None
    suffix = _EU_YAHOO_SUFFIX.get(mic, '')
    return f"{raw.strip()}{suffix}" if suffix else raw.strip()


def md5_hash(value: str) -> str:
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def add_ticker(symbol: str, mic: str = 'XNMS', name: str = '', isin: str = '',
               yahoo_ticker: str = '', db_path: str = DB_PATH):
    """
    Add a single ticker to the database.

    For US tickers the yahoo_ticker is the same as the symbol.
    For EU tickers, pass yahoo_ticker explicitly (e.g. 'SSABBH.HE').
    mic can be NYSE/NASDAQ/AMEX or the raw MIC code (XNYS/XNMS/XASE).
    """
    mic = US_MIC_MAP.get(mic.upper(), mic.upper())
    yt = yahoo_ticker if yahoo_ticker else symbol
    ticker_pk = md5_hash(symbol + mic)

    with _connect(db_path) as con:
        existing = con.execute(
            "select ticker_pk, is_dead from tickers where ticker_pk = ?", (ticker_pk,)
        ).fetchone()

        if existing:
            if existing[1]:
                print(f"{symbol} already exists but is marked dead. Use 'revive' to re-enable it.")
            else:
                print(f"{symbol} ({mic}) already exists in the database.")
            return

        con.execute(
            """
            insert into tickers (ticker_pk, isin, mic, raw_ticker, yahoo_ticker, asset_name, is_dead)
            values (?, ?, ?, ?, ?, ?, false)
            """,
            (ticker_pk, isin, mic, symbol, yt, name)
        )
        print(f"Added: {symbol} ({mic}) -> yahoo: {yt}" + (f" | {name}" if name else ""))


def mark_dead(symbol: str, reason: str = 'manually marked dead', db_path: str = DB_PATH):
    """Mark a ticker as dead so it is skipped during score calculation."""
    with _connect(db_path) as con:
        rows = con.execute(
            "select ticker_pk from tickers where yahoo_ticker = ? or raw_ticker = ?",
            (symbol, symbol)
        ).fetchall()

        if not rows:
            print(f"No ticker found matching '{symbol}'.")
            return

        con.execute(
            "update tickers set is_dead = true, dead_reason = ? where yahoo_ticker = ? or raw_ticker = ?",
            (reason, symbol, symbol)
        )
        print(f"Marked dead: {symbol} (reason: {reason})")


def revive(symbol: str, db_path: str = DB_PATH):
    """Re-enable a dead ticker so it is included in score calculation again."""
    with _connect(db_path) as con:
        rows = con.execute(
            "select ticker_pk from tickers where yahoo_ticker = ? or raw_ticker = ?",
            (symbol, symbol)
        ).fetchall()

        if not rows:
            print(f"No ticker found matching '{symbol}'.")
            return

        con.execute(
            "update tickers set is_dead = false, dead_reason = null where yahoo_ticker = ? or raw_ticker = ?",
            (symbol, symbol)
        )
        print(f"Revived: {symbol}")


def list_dead(db_path: str = DB_PATH):
    """Print all tickers currently marked as dead."""
    with _connect(db_path) as con:
        rows = con.execute(
            "select yahoo_ticker, raw_ticker, mic, asset_name, dead_reason "
            "from tickers where is_dead = true order by mic, yahoo_ticker"
        ).fetchall()

    if not rows:
        print("No dead tickers.")
        return

    print(f"{'Yahoo Ticker':<20} {'Raw':<15} {'MIC':<8} {'Name':<35} {'Reason'}")
    print("-" * 100)
    for yt, raw, mic, name, reason in rows:
        print(f"{yt or '':<20} {raw or '':<15} {mic or '':<8} {(name or '')[:34]:<35} {reason or ''}")
    print(f"\nTotal: {len(rows)} dead ticker(s)")


def refresh_us(db_path: str = DB_PATH):
    """Download latest US ticker lists from GitHub and insert any new symbols."""
    total_new = 0
    total_seen = 0

    with _connect(db_path) as con:
        for path, mic in _US_SOURCES:
            url = f"{_GITHUB_BASE}/{path}"
            print(f"Fetching {url} ...")

            try:
                with urllib.request.urlopen(url, timeout=30) as resp:
                    symbols = json.loads(resp.read().decode())
            except Exception as e:
                print(f"  ERROR downloading {path}: {e}")
                continue

            symbols = [s.strip() for s in symbols if s and s.strip()]

            existing_pks = {
                row[0] for row in
                con.execute("select ticker_pk from tickers where mic = ?", (mic,)).fetchall()
            }

            new_symbols = [s for s in symbols if md5_hash(s + mic) not in existing_pks]

            for symbol in new_symbols:
                ticker_pk = md5_hash(symbol + mic)
                con.execute(
                    """
                    insert into tickers (ticker_pk, isin, mic, raw_ticker, yahoo_ticker, asset_name, is_dead)
                    values (?, '', ?, ?, ?, '', false)
                    """,
                    (ticker_pk, mic, symbol, symbol)
                )

            total_seen += len(symbols)
            total_new += len(new_symbols)
            print(f"  {mic}: {len(symbols):,} symbols in source, {len(new_symbols):,} new")

    print(f"\nDone. {total_new:,} new tickers added ({total_seen:,} total symbols seen).")


def refresh_eu(db_path: str = DB_PATH):
    """Download the latest EU ticker CSV from CBOE and insert any new symbols."""
    import pandas as pd

    print(f"Fetching {_EU_CSV_URL} ...")
    try:
        req = urllib.request.Request(_EU_CSV_URL, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=60) as resp:
            content = resp.read()
    except Exception as e:
        print(f"ERROR downloading EU CSV: {e}")
        return

    try:
        df = pd.read_csv(io.BytesIO(content), skiprows=1)
    except Exception as e:
        print(f"ERROR parsing EU CSV: {e}")
        return

    equity_df = df[df['asset_class'] == 'EQTY'].copy()
    print(f"  {len(df):,} total rows, {len(equity_df):,} equities")

    new_count = 0
    skipped = 0

    with _connect(db_path) as con:
        existing_pks = {
            row[0] for row in con.execute("select ticker_pk from tickers").fetchall()
        }

        for _, row in equity_df.iterrows():
            company_name = str(row.get('company_name', '') or '').strip()
            raw_ticker   = _eu_raw_ticker(str(row.get('bloomberg_primary', '') or ''))
            isin         = str(row.get('isin', '') or '').strip()
            mic          = str(row.get('mic', '') or '').strip()

            if not company_name or not raw_ticker or not mic:
                skipped += 1
                continue

            yahoo_ticker = _eu_yahoo_ticker(raw_ticker, mic)
            if not yahoo_ticker:
                skipped += 1
                continue

            ticker_pk = md5_hash(raw_ticker + mic)
            if ticker_pk in existing_pks:
                continue

            try:
                con.execute(
                    """
                    insert into tickers (ticker_pk, isin, mic, raw_ticker, yahoo_ticker, asset_name, is_dead)
                    values (?, ?, ?, ?, ?, ?, false)
                    """,
                    (ticker_pk, isin, mic, raw_ticker, yahoo_ticker, company_name)
                )
                existing_pks.add(ticker_pk)
                new_count += 1
            except Exception:
                skipped += 1

    print(f"  New: {new_count:,}  Skipped: {skipped:,}")
    print(f"Done. {new_count:,} new EU tickers added.")


def main():
    parser = argparse.ArgumentParser(description="Manage tickers in the AFV database")
    parser.add_argument('--db', default=DB_PATH, metavar='PATH', help='Path to DuckDB file')
    sub = parser.add_subparsers(dest='cmd', required=True)

    p_add = sub.add_parser('add', help='Add a ticker')
    p_add.add_argument('symbol', help='Ticker symbol, e.g. SPCX')
    p_add.add_argument('--mic', default='XNMS', help='Exchange MIC or alias (NYSE/NASDAQ/AMEX), default XNMS')
    p_add.add_argument('--name', default='', help='Company/fund name')
    p_add.add_argument('--isin', default='', help='ISIN (optional)')
    p_add.add_argument('--yahoo-ticker', default='', dest='yahoo_ticker',
                       help='Override Yahoo Finance symbol (for EU tickers)')

    p_dead = sub.add_parser('dead', help='Mark a ticker as dead')
    p_dead.add_argument('symbol', help='Yahoo ticker or raw ticker')
    p_dead.add_argument('--reason', default='manually marked dead')

    p_revive = sub.add_parser('revive', help='Revive a dead ticker')
    p_revive.add_argument('symbol', help='Yahoo ticker or raw ticker')

    sub.add_parser('list-dead', help='List all dead tickers')
    sub.add_parser('refresh-us', help='Download latest US tickers from GitHub and add new ones')
    sub.add_parser('refresh-eu', help='Download latest EU tickers from CBOE and add new ones')

    args = parser.parse_args()

    if args.cmd == 'add':
        add_ticker(args.symbol, args.mic, args.name, args.isin, args.yahoo_ticker, args.db)
    elif args.cmd == 'dead':
        mark_dead(args.symbol, args.reason, args.db)
    elif args.cmd == 'revive':
        revive(args.symbol, args.db)
    elif args.cmd == 'list-dead':
        list_dead(args.db)
    elif args.cmd == 'refresh-us':
        refresh_us(args.db)
    elif args.cmd == 'refresh-eu':
        refresh_eu(args.db)


if __name__ == '__main__':
    main()
