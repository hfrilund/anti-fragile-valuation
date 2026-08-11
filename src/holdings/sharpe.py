"""Portfolio Sharpe ratio calculator using holdings + price_history from DB."""
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database.db import connect, DB_PATH

_TRADING_DAYS = 252


def _load_holdings_weights(con) -> dict[str, float]:
    rows = con.execute("""
        SELECT yahoo_symbol, pos_value
        FROM holdings
        WHERE fetched_at::DATE = (SELECT MAX(fetched_at)::DATE FROM holdings)
          AND pos_value > 0
    """).fetchall()
    return {sym: val for sym, val in rows if sym}


def _load_prices(con, symbols: list[str], cutoff: date) -> pd.DataFrame:
    sym_df = pd.DataFrame({'symbol': symbols})
    con.register('_sharpe_syms', sym_df)
    try:
        raw = con.execute("""
            SELECT p.symbol, p.date, p.close
            FROM price_history p
            JOIN _sharpe_syms s ON p.symbol = s.symbol
            WHERE p.date >= ?
            ORDER BY p.date ASC
        """, [cutoff]).fetchdf()
    finally:
        con.unregister('_sharpe_syms')

    if raw.empty:
        return pd.DataFrame()

    pivot = raw.pivot(index='date', columns='symbol', values='close')
    pivot.index = pd.to_datetime(pivot.index)
    return pivot


def calculate(db_path: str = DB_PATH, lookback_days: int = 365,
              risk_free_rate: float = 0.04) -> dict | None:
    """
    Compute portfolio Sharpe ratio stats and return them as a dict, or None
    if data is insufficient. Raises ValueError with a human-readable message
    if holdings or price history are missing.
    """
    with connect(db_path, read_only=True) as con:
        weights_raw = _load_holdings_weights(con)
        if not weights_raw:
            raise ValueError("No holdings in DB. Run 'holdings save' first.")

        cutoff = date.today() - timedelta(days=lookback_days)
        prices = _load_prices(con, list(weights_raw.keys()), cutoff)

    if prices.empty:
        raise ValueError("No price history found for holdings. Run 'prices fetch --period 2y' first.")

    prices = prices.dropna(axis=1, thresh=30)
    available = set(prices.columns)

    weights = {s: v for s, v in weights_raw.items() if s in available}
    missing = [s for s in weights_raw if s not in available]

    if not weights:
        raise ValueError("No overlapping symbols between holdings and price history.")

    total_value = sum(weights.values())
    weight_vec = pd.Series({s: v / total_value for s, v in weights.items()})
    weight_vec = weight_vec.reindex(prices.columns).fillna(0.0)

    # Fill NaN with 0 for local holidays / non-trading days
    daily_returns = prices.pct_change().fillna(0.0)
    portfolio_returns = daily_returns.dot(weight_vec)

    # Trim leading zeros before price data begins
    portfolio_returns = portfolio_returns.loc[portfolio_returns.ne(0.0).cummax()]

    if len(portfolio_returns) < 30:
        raise ValueError("Insufficient return history (< 30 trading days).")

    mean_daily = portfolio_returns.mean()
    std_daily = portfolio_returns.std()
    ann_return = mean_daily * _TRADING_DAYS
    ann_vol = std_daily * np.sqrt(_TRADING_DAYS)
    sharpe = (ann_return - risk_free_rate) / ann_vol if ann_vol > 0 else None

    cum = (1 + portfolio_returns).cumprod()
    rolling_max = cum.expanding().max()
    max_drawdown = float(((cum - rolling_max) / rolling_max).min())

    covered_value = sum(weights_raw.get(s, 0) for s in weights)
    coverage_pct = covered_value / sum(weights_raw.values()) * 100

    return {
        "sharpe":           round(sharpe, 4) if sharpe is not None else None,
        "ann_return":       round(ann_return, 4),
        "ann_vol":          round(ann_vol, 4),
        "max_drawdown":     round(max_drawdown, 4),
        "trading_days":     len(portfolio_returns),
        "lookback_days":    lookback_days,
        "risk_free_rate":   risk_free_rate,
        "positions_total":  len(weights_raw),
        "positions_covered": len(weights),
        "coverage_pct":     round(coverage_pct, 1),
        "missing_symbols":  missing,
        "period_start":     str(portfolio_returns.index[0].date()),
        "period_end":       str(portfolio_returns.index[-1].date()),
    }


def calculate_history(db_path: str = DB_PATH, window: int = 252,
                      risk_free_rate: float = 0.04) -> list[dict]:
    """
    Compute rolling portfolio stats (Sharpe, ann_return, ann_vol, max_drawdown)
    for every date in price history. Uses current holdings as fixed weights.
    window = rolling window in trading days (default 252 = 1 year).
    """
    with connect(db_path, read_only=True) as con:
        weights_raw = _load_holdings_weights(con)
        if not weights_raw:
            raise ValueError("No holdings in DB. Run 'holdings save' first.")
        cutoff = date(2000, 1, 1)  # all available history
        prices = _load_prices(con, list(weights_raw.keys()), cutoff)

    if prices.empty:
        raise ValueError("No price history found for holdings.")

    prices = prices.dropna(axis=1, thresh=30)
    weights = {s: v for s, v in weights_raw.items() if s in set(prices.columns)}
    if not weights:
        raise ValueError("No overlapping symbols between holdings and price history.")

    total_value = sum(weights.values())
    weight_vec = pd.Series({s: v / total_value for s, v in weights.items()})
    weight_vec = weight_vec.reindex(prices.columns).fillna(0.0)

    daily_returns = prices.pct_change().fillna(0.0)
    portfolio_returns = daily_returns.dot(weight_vec)
    portfolio_returns = portfolio_returns.loc[portfolio_returns.ne(0.0).cummax()]

    min_periods = min(30, window)

    ann_return = portfolio_returns.rolling(window, min_periods=min_periods).mean() * _TRADING_DAYS
    ann_vol    = portfolio_returns.rolling(window, min_periods=min_periods).std() * np.sqrt(_TRADING_DAYS)
    sharpe     = (ann_return - risk_free_rate) / ann_vol.replace(0, np.nan)

    def _max_dd(arr):
        cum = np.cumprod(1 + arr)
        peak = np.maximum.accumulate(cum)
        return float(((cum - peak) / peak).min())

    max_dd = portfolio_returns.rolling(window, min_periods=min_periods).apply(_max_dd, raw=True)

    cum_value = (1 + portfolio_returns).cumprod()
    cum_value = cum_value / cum_value.iloc[0]

    result = []
    for ts, sh, ar, av, md in zip(
        sharpe.index, sharpe.values, ann_return.values, ann_vol.values, max_dd.values
    ):
        if any(np.isnan(v) for v in [sh, ar, av, md]):
            continue
        result.append({
            "date":            str(ts.date()),
            "sharpe":          round(float(sh), 4),
            "ann_return":      round(float(ar), 4),
            "ann_vol":         round(float(av), 4),
            "max_drawdown":    round(float(md), 4),
            "portfolio_value": round(float(cum_value.loc[ts]), 4),
        })

    return result


def compute(db_path: str = DB_PATH, lookback_days: int = 365,
            risk_free_rate: float = 0.04) -> None:
    try:
        r = calculate(db_path, lookback_days, risk_free_rate)
    except ValueError as e:
        print(e)
        return

    print(f"\n{'='*55}")
    print(f"  PORTFOLIO SHARPE RATIO")
    print(f"{'='*55}")
    print(f"  Period              : {r['period_start']} → {r['period_end']}  ({r['trading_days']} trading days)")
    print(f"  Risk-free rate      : {r['risk_free_rate']:.1%} annualised")
    print(f"  Holdings coverage   : {r['positions_covered']}/{r['positions_total']} positions  ({r['coverage_pct']:.0f}% by value)")
    print()
    print(f"  Annualised return   : {r['ann_return']:+.2%}")
    print(f"  Annualised volatility: {r['ann_vol']:.2%}")
    print(f"  Max drawdown        : {r['max_drawdown']:.2%}")
    print()
    sharpe_s = f"{r['sharpe']:.2f}" if r['sharpe'] is not None else "N/A"
    print(f"  Sharpe ratio        : {sharpe_s}")
    if r['missing_symbols']:
        print(f"\n  No price data for   : {', '.join(r['missing_symbols'])}")
    print(f"{'='*55}\n")
