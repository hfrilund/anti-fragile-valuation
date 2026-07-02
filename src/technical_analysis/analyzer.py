import random
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database.db import connect

_TOP_N = 500
_BATCH_SIZE = 50


def _top_symbols(con) -> list[tuple[str, float]]:
    return con.execute("""
        WITH latest_run AS (
            SELECT symbol, afv21,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY computed_at DESC) AS rn
            FROM afv_21_scores
            WHERE afv != -1000
              AND afv21 IS NOT NULL
              AND computed_at >= current_timestamp - INTERVAL 35 DAYS
        )
        SELECT symbol, afv21
        FROM latest_run
        WHERE rn = 1
        ORDER BY afv21 DESC
        LIMIT ?
    """, (_TOP_N,)).fetchall()


def _fetch_histories(symbols: list[str]) -> dict[str, pd.DataFrame]:
    histories = {}
    total_batches = (len(symbols) + _BATCH_SIZE - 1) // _BATCH_SIZE

    for i in range(0, len(symbols), _BATCH_SIZE):
        batch = symbols[i:i + _BATCH_SIZE]
        batch_num = i // _BATCH_SIZE + 1
        print(f"  Fetching price history batch {batch_num}/{total_batches} ({len(batch)} symbols)...")

        try:
            data = yf.download(batch, period='1y', auto_adjust=True, progress=False)
            if not data.empty:
                for symbol in batch:
                    try:
                        df = data.xs(symbol, axis=1, level=1).dropna(how='all')
                        if not df.empty and 'Close' in df.columns:
                            histories[symbol] = df
                    except KeyError:
                        pass
        except Exception as e:
            print(f"  Batch {batch_num} download error: {e}")

        if i + _BATCH_SIZE < len(symbols):
            time.sleep(random.uniform(2, 4))

    return histories


def _rsi(close: pd.Series, period: int = 14) -> float | None:
    if len(close) < period + 1:
        return None
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss
    val = (100 - 100 / (1 + rs)).iloc[-1]
    return None if pd.isna(val) else float(val)


def _macd(close: pd.Series) -> tuple[float | None, float | None, float | None, str]:
    if len(close) < 26:
        return None, None, None, 'neutral'
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    histogram = macd_line - signal_line

    m = float(macd_line.iloc[-1]) if not pd.isna(macd_line.iloc[-1]) else None
    s = float(signal_line.iloc[-1]) if not pd.isna(signal_line.iloc[-1]) else None
    h = float(histogram.iloc[-1]) if not pd.isna(histogram.iloc[-1]) else None

    if m is None or s is None:
        sentiment = 'neutral'
    elif m > s:
        sentiment = 'bullish'
    elif m < s:
        sentiment = 'bearish'
    else:
        sentiment = 'neutral'

    return m, s, h, sentiment


def _obv(close: pd.Series, volume: pd.Series) -> tuple[float | None, str]:
    if len(close) < 20:
        return None, 'neutral'
    direction = close.diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    obv_series = (volume * direction).cumsum()
    obv_val = float(obv_series.iloc[-1])

    # Trend: compare the most recent 20-bar OBV average to the prior 20-bar average
    if len(obv_series) >= 40:
        recent = obv_series.iloc[-20:].mean()
        prior  = obv_series.iloc[-40:-20].mean()
        if recent > prior * 1.01:
            trend = 'rising'
        elif recent < prior * 0.99:
            trend = 'falling'
        else:
            trend = 'neutral'
    else:
        trend = 'neutral'

    return obv_val, trend


def _ma_cross_info(
    ma50: pd.Series, ma200: pd.Series
) -> tuple[str, int | None, float | None]:
    """Returns (signal, days_since_cross, distance_pct)."""
    valid = ma50.dropna().index.intersection(ma200.dropna().index)
    if len(valid) < 2:
        return 'none', None, None

    ma50_v  = ma50[valid]
    ma200_v = ma200[valid]

    cur50  = float(ma50_v.iloc[-1])
    cur200 = float(ma200_v.iloc[-1])

    signal       = 'golden_cross' if cur50 > cur200 else 'death_cross'
    distance_pct = (cur50 - cur200) / cur200 * 100

    # Locate the most recent bar where the two MAs changed sides
    diff         = ma50_v - ma200_v
    sign_changes = (diff.shift(1) * diff) < 0
    cross_dates  = diff.index[sign_changes]

    if len(cross_dates) == 0:
        return signal, None, distance_pct

    raw_diff = ma50_v.index[-1] - cross_dates[-1]
    days_ago = raw_diff.days if hasattr(raw_diff, 'days') else int(raw_diff)
    return signal, int(days_ago), distance_pct


def _ma_trend(ma: pd.Series, slope_window: int = 10) -> str:
    """Current slope direction of a moving average."""
    clean = ma.dropna()
    if len(clean) < slope_window + 1:
        return 'unknown'
    return 'rising' if float(clean.diff(slope_window).iloc[-1]) > 0 else 'falling'


def _ma_days_since_bottom(ma: pd.Series, lookback_days: int = 30,
                          min_decline_pct: float = 0.02, min_recovery_bars: int = 5) -> int | None:
    """
    Detects a cup-like trough in the MA within the last lookback_days.

    Requires:
    - A genuine prior decline: the MA fell at least min_decline_pct from its
      pre-trough peak (rules out flat periods with tiny wiggles)
    - A confirmed recovery: the MA has been rising for at least min_recovery_bars
      trading days since the trough (rules out single-day reversals)
    - The trough is not at the very edge of the search window (needs context on both sides)

    Returns days since the trough, or None if no valid cup bottom is found.
    """
    clean = ma.dropna()
    # Need enough history: lookback window + pre-trough context + post-trough recovery
    if len(clean) < lookback_days + 30:
        return None

    # Search window: lookback_days back from today, but keep 20 bars of pre-trough context
    search_start = -(lookback_days + 20)
    window = clean.iloc[search_start:]

    trough_idx = window.idxmin()
    trough_pos = window.index.get_loc(trough_idx)

    # Trough must have at least 10 bars before it (for pre-decline context)
    # and at least min_recovery_bars bars after it (to confirm recovery)
    if trough_pos < 10 or trough_pos > len(window) - min_recovery_bars - 1:
        return None

    trough_val = window[trough_idx]

    # Pre-trough: the MA must have been genuinely higher (real decline into trough)
    pre_peak = window.iloc[:trough_pos].max()
    if (pre_peak - trough_val) / pre_peak < min_decline_pct:
        return None

    # Post-trough: the MA must be monotonically or consistently rising
    # (check that the current value is above the trough and the post-trough
    # segment has a positive slope over its full span)
    post = window.iloc[trough_pos:]
    if clean.iloc[-1] <= trough_val:
        return None
    post_slope = float(post.iloc[-1]) - float(post.iloc[0])
    if post_slope <= 0:
        return None

    days_since = (clean.index[-1] - trough_idx).days
    # Only report if the trough itself falls within lookback_days
    return days_since if days_since <= lookback_days else None


def _analyze(symbol: str, df: pd.DataFrame, afv21: float) -> dict | None:
    if df.empty or 'Close' not in df.columns or 'Volume' not in df.columns:
        return None

    close  = df['Close'].dropna()
    volume = df['Volume'].dropna()

    if len(close) < 50:
        return None

    ma50  = close.rolling(50).mean()
    ma200 = close.rolling(200).mean()

    signal, days_ago, distance_pct = _ma_cross_info(ma50, ma200)
    rsi14                          = _rsi(close)
    macd_line, macd_sig, macd_hist, macd_sentiment = _macd(close)
    obv_val, obv_trend             = _obv(close, volume)
    ma50_bottom                    = _ma_days_since_bottom(ma50,  lookback_days=30)
    ma200_trend                    = _ma_trend(ma200, slope_window=10)
    ma200_bottom                   = _ma_days_since_bottom(ma200, lookback_days=60, min_decline_pct=0.03)

    return {
        'symbol':               symbol,
        'afv21_score':          afv21,
        'close_price':          float(close.iloc[-1]),
        'ma50':                 float(ma50.iloc[-1]) if not pd.isna(ma50.iloc[-1]) else None,
        'ma200':                float(ma200.iloc[-1]) if not pd.isna(ma200.iloc[-1]) else None,
        'ma_cross_signal':      signal,
        'ma_cross_days_ago':    days_ago,
        'ma_distance_pct':      distance_pct,
        'rsi14':                rsi14,
        'macd_line':            macd_line,
        'macd_signal_line':     macd_sig,
        'macd_histogram':       macd_hist,
        'macd_sentiment':       macd_sentiment,
        'obv':                  obv_val,
        'obv_trend':            obv_trend,
        'ma50_bottom_days_ago':  ma50_bottom,
        'ma200_trend':           ma200_trend,
        'ma200_bottom_days_ago': ma200_bottom,
    }


def _print_summary(results: list[dict]):
    # Recent golden crosses (cross happened within the last 30 calendar days)
    recent_golden = [
        r for r in results
        if r['ma_cross_signal'] == 'golden_cross'
        and r.get('ma_cross_days_ago') is not None
        and r['ma_cross_days_ago'] <= 30
    ]

    # Potential crosses forming: in golden territory but very close (distance < 2%)
    # or in death-cross territory but rapidly closing (distance > -3%)
    forming = [
        r for r in results
        if r.get('ma_distance_pct') is not None
        and -3.0 < r['ma_distance_pct'] < 2.0
        and r['ma_cross_signal'] == 'death_cross'
    ]

    print(f"\n{'='*60}")
    print(f"  TECHNICAL ANALYSIS SUMMARY  ({len(results)} symbols)")
    print(f"{'='*60}")

    print(f"\n  Recent golden crosses (≤30 days ago): {len(recent_golden)}")
    for r in sorted(recent_golden, key=lambda x: x['ma_cross_days_ago']):
        rsi_str  = f"{r['rsi14']:.1f}" if r['rsi14'] is not None else 'N/A'
        macd_str = r['macd_sentiment'] or 'N/A'
        print(f"    {r['symbol']:<15} {r['ma_cross_days_ago']:>3}d ago  "
              f"AFV21={r['afv21_score']:+.2f}  RSI={rsi_str}  MACD={macd_str}  OBV={r['obv_trend']}")

    print(f"\n  Potential golden crosses forming (death cross, distance > -3%): {len(forming)}")
    for r in sorted(forming, key=lambda x: x['ma_distance_pct'], reverse=True):
        rsi_str = f"{r['rsi14']:.1f}" if r['rsi14'] is not None else 'N/A'
        print(f"    {r['symbol']:<15} dist={r['ma_distance_pct']:+.2f}%  "
              f"AFV21={r['afv21_score']:+.2f}  RSI={rsi_str}  MACD={r['macd_sentiment']}  OBV={r['obv_trend']}")

    # Early recovery: 200MA rising or recently bottomed, AND 50MA recently bottomed
    early_recovery = [
        r for r in results
        if r.get('ma50_bottom_days_ago') is not None
        and (r.get('ma200_trend') == 'rising' or r.get('ma200_bottom_days_ago') is not None)
    ]
    print(f"\n  Early recovery signal (200MA rising/bottomed + 50MA bottomed): {len(early_recovery)}")
    for r in sorted(early_recovery, key=lambda x: x['afv21_score'], reverse=True):
        ma200_str = (f"bottomed {r['ma200_bottom_days_ago']}d ago"
                     if r.get('ma200_bottom_days_ago') is not None
                     else "rising")
        rsi_str = f"{r['rsi14']:.1f}" if r['rsi14'] is not None else 'N/A'
        print(f"    {r['symbol']:<15} 50MA bot {r['ma50_bottom_days_ago']:>2}d ago  200MA {ma200_str:<22}"
              f"  AFV21={r['afv21_score']:+.2f}  RSI={rsi_str}  MACD={r['macd_sentiment']}")

    print(f"{'='*60}\n")


def _insert_results(con, results: list[dict]) -> None:
    for r in results:
        con.execute("""
            INSERT INTO technical_analysis (
                symbol, afv21_score, close_price,
                ma50, ma200,
                ma_cross_signal, ma_cross_days_ago, ma_distance_pct,
                rsi14,
                macd_line, macd_signal_line, macd_histogram, macd_sentiment,
                obv, obv_trend,
                ma50_bottom_days_ago, ma200_trend, ma200_bottom_days_ago
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            r['symbol'], r['afv21_score'], r['close_price'],
            r['ma50'], r['ma200'],
            r['ma_cross_signal'], r['ma_cross_days_ago'], r['ma_distance_pct'],
            r['rsi14'],
            r['macd_line'], r['macd_signal_line'], r['macd_histogram'], r['macd_sentiment'],
            r['obv'], r['obv_trend'],
            r['ma50_bottom_days_ago'], r['ma200_trend'], r['ma200_bottom_days_ago'],
        ))


def _latest_afv21(con, symbols: list[str]) -> dict[str, float]:
    """Return the most recent afv21 score for each symbol in the list."""
    if not symbols:
        return {}
    placeholders = ', '.join('?' * len(symbols))
    rows = con.execute(f"""
        WITH latest AS (
            SELECT symbol, afv21,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY computed_at DESC) AS rn
            FROM afv_21_scores
            WHERE afv != -1000 AND afv21 IS NOT NULL
        )
        SELECT symbol, afv21 FROM latest WHERE rn = 1 AND symbol IN ({placeholders})
    """, symbols).fetchall()
    return {s: sc for s, sc in rows}


def run(db_path: str):
    with connect(db_path) as con:
        symbol_scores = _top_symbols(con)
        if not symbol_scores:
            print("No AFV21 scores found. Run the AFV processor first.")
            return

        symbols = [s for s, _ in symbol_scores]
        print(f"Running technical analysis on top {len(symbols)} symbols by AFV21 score...")

        histories = _fetch_histories(symbols)
        print(f"Price history fetched: {len(histories)}/{len(symbols)} symbols.")

        results = []
        for symbol, afv21 in symbol_scores:
            df = histories.get(symbol)
            if df is None:
                continue
            row = _analyze(symbol, df, afv21)
            if row:
                results.append(row)

        if not results:
            print("No results to store.")
            return

        con.execute("DELETE FROM technical_analysis WHERE computed_at::DATE = current_date")
        _insert_results(con, results)
        print(f"Stored {len(results)} records in technical_analysis.")
        _print_summary(results)


def run_holdings(db_path: str):
    """Run TA for the current holdings snapshot and print a holdings-focused summary."""
    with connect(db_path) as con:
        rows = con.execute("""
            SELECT DISTINCT yahoo_symbol
            FROM holdings
            WHERE fetched_at::DATE = (SELECT MAX(fetched_at)::DATE FROM holdings)
        """).fetchall()
        symbols = [r[0] for r in rows]

        if not symbols:
            print("No holdings in DB. Run 'holdings save' first.")
            return

        print(f"Running technical analysis for {len(symbols)} holdings: {', '.join(symbols)}")
        score_map = _latest_afv21(con, symbols)

        histories = _fetch_histories(symbols)

        results = []
        for symbol in symbols:
            df = histories.get(symbol)
            if df is None:
                continue
            row = _analyze(symbol, df, score_map.get(symbol))
            if row:
                results.append(row)

        if not results:
            print("No TA results for holdings.")
            return

        computed_syms = [r['symbol'] for r in results]
        placeholders  = ', '.join('?' * len(computed_syms))
        con.execute(
            f"DELETE FROM technical_analysis "
            f"WHERE computed_at::DATE = current_date AND symbol IN ({placeholders})",
            computed_syms,
        )
        _insert_results(con, results)

    print(f"Stored {len(results)} holdings TA records.")
    _print_holdings_summary(results)


def _print_holdings_summary(results: list[dict]) -> None:
    print(f"\n{'='*60}")
    print(f"  HOLDINGS TECHNICAL ANALYSIS  ({len(results)} positions)")
    print(f"{'='*60}")

    for r in sorted(results, key=lambda x: x['symbol']):
        symbol       = r['symbol']
        afv21_s      = f"{r['afv21_score']:+.2f}" if r['afv21_score'] is not None else " N/A"
        price_s      = f"{r['close_price']:.2f}"
        rsi_s        = f"{r['rsi14']:.1f}" if r['rsi14'] is not None else "N/A"
        cross        = r['ma_cross_signal'] or "N/A"
        days_ago_s   = f"{r['ma_cross_days_ago']}d ago" if r['ma_cross_days_ago'] is not None else "N/A"
        dist_s       = f"{r['ma_distance_pct']:+.1f}%" if r['ma_distance_pct'] is not None else "N/A"
        ma200_trend  = r.get('ma200_trend') or "N/A"
        ma200_bot    = (f"bottomed {r['ma200_bottom_days_ago']}d ago"
                        if r.get('ma200_bottom_days_ago') is not None else "")
        ma50_bot     = (f"50MA bot {r['ma50_bottom_days_ago']}d ago"
                        if r.get('ma50_bottom_days_ago') is not None else "")
        macd_s       = r['macd_sentiment'] or "N/A"

        flags = []
        if cross == 'death_cross' and r.get('ma_cross_days_ago') is not None and r['ma_cross_days_ago'] <= 30:
            flags.append("!! RECENT DEATH CROSS")
        if cross == 'death_cross' and r.get('ma_distance_pct') is not None and r['ma_distance_pct'] > -3.0:
            flags.append("! death cross forming")
        if ma200_trend == 'falling' and r.get('ma200_bottom_days_ago') is None:
            flags.append("200MA falling")
        if ma200_bot:
            flags.append(f"200MA {ma200_bot}")
        if ma50_bot:
            flags.append(ma50_bot)

        flag_str = "  |  " + "  ".join(flags) if flags else ""

        print(f"\n  {symbol:<12} AFV21={afv21_s}  price={price_s}  RSI={rsi_s}  MACD={macd_s}")
        print(f"             {cross:<14} {days_ago_s}  50/200 dist={dist_s}  200MA={ma200_trend}{flag_str}")

    print(f"\n{'='*60}\n")
