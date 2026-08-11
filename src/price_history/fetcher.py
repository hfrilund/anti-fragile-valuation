import random
import sys
import time
from pathlib import Path

import pandas as pd
import yfinance as yf

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database.db import connect

_BATCH_SIZE = 50
_MIN_DAYS = 400  # below this a symbol gets a 2y backfill; ~1.6 years trading days


def _ta_symbols(con, top: int) -> list[str]:
    """Top N by recent AFV21 score, plus current holdings — the exact set TA will run on."""
    rows = con.execute("""
        SELECT symbol FROM (
            WITH latest AS (
                SELECT symbol, afv21,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY computed_at DESC) AS rn
                FROM afv_21_scores
                WHERE afv != -1000
                  AND afv21 IS NOT NULL
                  AND computed_at >= current_timestamp - INTERVAL 35 DAYS
            )
            SELECT symbol FROM latest WHERE rn = 1
            ORDER BY afv21 DESC
            LIMIT $top
        )

        UNION

        SELECT DISTINCT yahoo_symbol AS symbol
        FROM holdings
        WHERE fetched_at::DATE = (SELECT MAX(fetched_at::DATE) FROM holdings)
          AND yahoo_symbol IS NOT NULL
    """, {"top": top}).fetchall()
    return [r[0] for r in rows]


def _coverage(con, symbols: list[str]) -> pd.DataFrame:
    """Return a DataFrame with columns [symbol, days] for the given symbol list."""
    sym_df = pd.DataFrame({'symbol': symbols})
    con.register('_cov_syms', sym_df)
    try:
        return con.execute("""
            SELECT s.symbol, COUNT(ph.date) AS days
            FROM _cov_syms s
            LEFT JOIN price_history ph ON ph.symbol = s.symbol
            GROUP BY s.symbol
        """).fetchdf()
    finally:
        con.unregister('_cov_syms')


def _download(symbols: list[str], period: str) -> dict[str, pd.DataFrame]:
    histories: dict[str, pd.DataFrame] = {}
    total_batches = (len(symbols) + _BATCH_SIZE - 1) // _BATCH_SIZE

    for i in range(0, len(symbols), _BATCH_SIZE):
        batch = symbols[i:i + _BATCH_SIZE]
        batch_num = i // _BATCH_SIZE + 1
        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} symbols)…")

        try:
            data = yf.download(batch, period=period, auto_adjust=True, progress=False)
            if data.empty:
                continue
            for symbol in batch:
                try:
                    df = data.xs(symbol, axis=1, level=1).dropna(how='all')
                    if not df.empty and 'Close' in df.columns:
                        histories[symbol] = df
                except KeyError:
                    pass
        except Exception as e:
            print(f"  Batch {batch_num} error: {e}")

        if i + _BATCH_SIZE < len(symbols):
            time.sleep(random.uniform(2, 4))

    return histories


def _store(con, histories: dict[str, pd.DataFrame]) -> None:
    for symbol, df in histories.items():
        price_df = df[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
        price_df.index.name = 'date'
        price_df = price_df.reset_index()
        price_df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        price_df['symbol'] = symbol
        price_df['date'] = pd.to_datetime(price_df['date']).dt.date

        con.register('_ph_tmp', price_df)
        con.execute("""
            DELETE FROM price_history
            WHERE (symbol, date) IN (SELECT symbol, date FROM _ph_tmp)
        """)
        con.execute("""
            INSERT INTO price_history (symbol, date, open, high, low, close, volume)
            SELECT symbol, date, open, high, low, close, volume FROM _ph_tmp
        """)
        con.unregister('_ph_tmp')


def ensure_for_ta(db_path: str, top: int = 500):
    """
    Called by the daily run before TA. For each symbol TA will use:
    - fewer than _MIN_DAYS of history → fetch 2y (self-healing backfill)
    - otherwise → fetch 7d (daily top-up)

    No manual backfill command needed: any new symbol entering the top list
    automatically gets a full history on its first appearance.
    """
    with connect(db_path) as con:
        symbols = _ta_symbols(con, top)
        if not symbols:
            print("  No scored symbols found. Run the AFV processor first.")
            return

        cov = _coverage(con, symbols)
        needs_backfill = cov[cov['days'] < _MIN_DAYS]['symbol'].tolist()
        needs_topup    = cov[cov['days'] >= _MIN_DAYS]['symbol'].tolist()

        if needs_backfill:
            print(f"  {len(needs_backfill)} symbol(s) need backfill (<{_MIN_DAYS} days) — fetching 2y…")
            _store(con, _download(needs_backfill, period='2y'))

        if needs_topup:
            print(f"  {len(needs_topup)} symbol(s) up to date — fetching 7d top-up…")
            _store(con, _download(needs_topup, period='7d'))

        print(f"  Price history ready for {len(symbols)} symbols.")


def fetch_and_store(db_path: str, period: str = '7d', top: int = 500,
                    symbols: list[str] | None = None):
    """
    Manual price fetch with explicit period. Used by the `prices fetch` CLI command.
    For the daily run, use ensure_for_ta() instead.
    """
    with connect(db_path) as con:
        syms = symbols if symbols is not None else _ta_symbols(con, top)
        if not syms:
            print("No scored symbols found in DB. Run the AFV processor first.")
            return

        print(f"Fetching price history (period={period}) for {len(syms)} symbols…")
        _store(con, _download(syms, period))
        print("Price history updated.")
