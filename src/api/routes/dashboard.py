from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from api.db import db_cursor, DB_PATH as API_DB_PATH
from holdings.sharpe import calculate as calculate_sharpe, calculate_history as calculate_sharpe_history

router = APIRouter(prefix="/dashboard", tags=["dashboard"])

TOP_PICKS_SQL = """
WITH latest_ta AS (
    SELECT DISTINCT ON (symbol) *
    FROM technical_analysis
    ORDER BY symbol, computed_at DESC
),
latest_scores AS (
    SELECT DISTINCT ON (symbol)
        symbol, afv21, rp21, fcf_yield, ocf_margin, sector_score,
        geo_score, debt_score, trend_score, vd_score, computed_at
    FROM afv_21_scores
    ORDER BY symbol, computed_at DESC
)
SELECT
    ta.symbol,
    t.asset_name,
    s.afv21,
    s.rp21,
    s.fcf_yield,
    s.ocf_margin,
    s.sector_score,
    s.geo_score,
    s.debt_score,
    s.trend_score,
    s.vd_score,
    ta.close_price,
    ta.ma50,
    ta.ma200,
    ta.ma_cross_signal,
    ta.ma_cross_days_ago,
    ta.rsi14,
    ta.macd_sentiment,
    ta.obv_trend,
    ta.ma200_trend,
    ta.ma200_bottom_days_ago,
    ta.computed_at AS ta_computed_at
FROM latest_ta ta
JOIN latest_scores s ON ta.symbol = s.symbol
LEFT JOIN tickers t ON ta.symbol = t.yahoo_ticker
WHERE s.afv21 > $min_afv21
  AND ($ma_cross IS NULL OR ta.ma_cross_signal = $ma_cross)
ORDER BY s.afv21 DESC
LIMIT $limit
"""

HOLDINGS_SQL = """
WITH latest_holdings AS (
    SELECT DISTINCT ON (symbol) *
    FROM holdings
    ORDER BY symbol, fetched_at DESC
),
latest_scores AS (
    SELECT DISTINCT ON (symbol)
        symbol, afv21, computed_at
    FROM afv_21_scores
    ORDER BY symbol, computed_at DESC
),
latest_ta AS (
    SELECT DISTINCT ON (symbol)
        symbol, ma_cross_signal, ma_cross_days_ago, rsi14,
        macd_sentiment, obv_trend, ma200_trend, computed_at
    FROM technical_analysis
    ORDER BY symbol, computed_at DESC
),
-- For each holding, find the best matching yahoo_ticker via the tickers table.
-- Prefer the ticker that matches holdings.yahoo_symbol, fall back to any match.
holding_tickers AS (
    SELECT
        h.symbol,
        h.yahoo_symbol,
        h.description,
        h.position,
        h.mark_price,
        h.pos_value,
        h.cost_basis,
        h.currency,
        h.fetched_at,
        t.yahoo_ticker,
        ROW_NUMBER() OVER (
            PARTITION BY h.symbol
            ORDER BY (t.yahoo_ticker = h.yahoo_symbol) DESC
        ) AS pref
    FROM latest_holdings h
    JOIN tickers t ON h.symbol = t.raw_ticker
    JOIN latest_scores s ON t.yahoo_ticker = s.symbol
)
SELECT
    ht.symbol,
    ht.yahoo_symbol,
    ht.description,
    ht.position,
    ht.mark_price,
    ht.pos_value,
    ht.cost_basis,
    ht.currency,
    ROUND(((ht.mark_price - (ht.cost_basis / ht.position)) / (ht.cost_basis / ht.position)) * 100, 2) AS pnl_pct,
    s.afv21,
    ta.ma_cross_signal,
    ta.ma_cross_days_ago,
    ta.rsi14,
    ta.macd_sentiment,
    ta.obv_trend,
    ta.ma200_trend,
    ht.fetched_at
FROM holding_tickers ht
JOIN latest_scores s ON ht.yahoo_ticker = s.symbol
LEFT JOIN latest_ta ta ON ht.yahoo_ticker = ta.symbol
WHERE ht.pref = 1
ORDER BY ht.pos_value DESC
"""


POTENTIAL_CROSSES_SQL = """
WITH latest_ta AS (
    SELECT DISTINCT ON (symbol) *
    FROM technical_analysis
    ORDER BY symbol, computed_at DESC
),
latest_scores AS (
    SELECT DISTINCT ON (symbol)
        symbol, afv21, rp21
    FROM afv_21_scores
    ORDER BY symbol, computed_at DESC
)
SELECT
    ta.symbol,
    t.asset_name,
    s.afv21,
    ta.close_price,
    ta.ma_distance_pct,
    ta.rsi14,
    ta.macd_sentiment,
    ta.obv_trend,
    ta.ma200_trend,
    ta.computed_at AS ta_computed_at
FROM latest_ta ta
JOIN latest_scores s ON ta.symbol = s.symbol
LEFT JOIN tickers t ON ta.symbol = t.yahoo_ticker
WHERE ta.ma_cross_signal = 'death_cross'
  AND ta.ma_distance_pct IS NOT NULL
  AND ta.ma_distance_pct > -3.0
  AND s.afv21 > $min_afv21
ORDER BY ta.ma_distance_pct DESC
LIMIT $limit
"""

EARLY_RECOVERY_SQL = """
WITH latest_ta AS (
    SELECT DISTINCT ON (symbol) *
    FROM technical_analysis
    ORDER BY symbol, computed_at DESC
),
latest_scores AS (
    SELECT DISTINCT ON (symbol)
        symbol, afv21
    FROM afv_21_scores
    ORDER BY symbol, computed_at DESC
)
SELECT
    ta.symbol,
    t.asset_name,
    s.afv21,
    ta.close_price,
    ta.ma50_bottom_days_ago,
    ta.ma200_trend,
    ta.ma200_bottom_days_ago,
    ta.ma_cross_signal,
    ta.rsi14,
    ta.macd_sentiment,
    ta.computed_at AS ta_computed_at
FROM latest_ta ta
JOIN latest_scores s ON ta.symbol = s.symbol
LEFT JOIN tickers t ON ta.symbol = t.yahoo_ticker
WHERE ta.ma50_bottom_days_ago IS NOT NULL
  AND (ta.ma200_trend = 'rising' OR ta.ma200_bottom_days_ago IS NOT NULL)
  AND s.afv21 > $min_afv21
ORDER BY s.afv21 DESC
LIMIT $limit
"""


TA_DETAIL_SQL = """
SELECT
    ta.symbol,
    ta.close_price,
    ta.ma50,
    ta.ma200,
    ta.ma_cross_signal,
    ta.ma_cross_days_ago,
    ta.ma_distance_pct,
    ta.rsi14,
    ta.macd_line,
    ta.macd_signal_line,
    ta.macd_histogram,
    ta.macd_sentiment,
    ta.obv,
    ta.obv_trend,
    ta.ma50_bottom_days_ago,
    ta.ma200_trend,
    ta.ma200_bottom_days_ago,
    ta.computed_at AS ta_computed_at
FROM technical_analysis ta
WHERE ta.symbol = $symbol
ORDER BY ta.computed_at DESC
LIMIT 1
"""


def _rows_to_dicts(conn, sql: str, params: dict) -> list[dict]:
    import json
    result = conn.execute(sql, params).fetchdf()
    return json.loads(result.to_json(orient="records", date_format="iso"))


@router.get("/top-picks")
def top_picks(
    limit: int = Query(50, ge=1, le=500),
    min_afv21: float = Query(0.0),
    ma_cross: Optional[str] = Query(None, pattern="^(golden_cross|death_cross)$"),
):
    with db_cursor() as conn:
        rows = _rows_to_dicts(conn, TOP_PICKS_SQL, {
            "limit": limit,
            "min_afv21": min_afv21,
            "ma_cross": ma_cross,
        })
    return {"count": len(rows), "results": rows}


@router.get("/potential-crosses")
def potential_crosses(
    limit: int = Query(50, ge=1, le=500),
    min_afv21: float = Query(0.0),
):
    with db_cursor() as conn:
        rows = _rows_to_dicts(conn, POTENTIAL_CROSSES_SQL, {
            "limit": limit,
            "min_afv21": min_afv21,
        })
    return {"count": len(rows), "results": rows}


@router.get("/early-recovery")
def early_recovery(
    limit: int = Query(50, ge=1, le=500),
    min_afv21: float = Query(0.0),
):
    with db_cursor() as conn:
        rows = _rows_to_dicts(conn, EARLY_RECOVERY_SQL, {
            "limit": limit,
            "min_afv21": min_afv21,
        })
    return {"count": len(rows), "results": rows}


@router.get("/ta/{symbol}")
def ta_detail(symbol: str):
    with db_cursor() as conn:
        import json
        df = conn.execute(TA_DETAIL_SQL, {"symbol": symbol}).fetchdf()
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No technical analysis data for {symbol}")
        rows = json.loads(df.to_json(orient="records", date_format="iso"))
        return rows[0]


@router.get("/sharpe/history")
def sharpe_history(
    window: int = Query(252, ge=30, le=504),
    risk_free_rate: float = Query(0.04, ge=0.0, le=1.0),
):
    try:
        rows = calculate_sharpe_history(db_path=API_DB_PATH, window=window, risk_free_rate=risk_free_rate)
    except ValueError as e:
        return {"data": [], "message": str(e)}
    return {"data": rows, "message": None}


@router.get("/holdings")
def holdings():
    with db_cursor() as conn:
        rows = _rows_to_dicts(conn, HOLDINGS_SQL, {})
    return {"count": len(rows), "results": rows}


@router.get("/sharpe")
def sharpe(
    lookback_days: int = Query(365, ge=30, le=1825),
    risk_free_rate: float = Query(0.04, ge=0.0, le=1.0),
):
    try:
        result = calculate_sharpe(db_path=API_DB_PATH, lookback_days=lookback_days, risk_free_rate=risk_free_rate)
    except ValueError as e:
        return {"data": None, "message": str(e)}
    return {"data": result, "message": None}
