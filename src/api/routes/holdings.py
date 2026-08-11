from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from api.db import db_cursor, db_write_cursor

router = APIRouter(prefix="/holdings", tags=["holdings"])

HOLDING_DETAIL_SQL = """
WITH latest_holding AS (
    SELECT DISTINCT ON (symbol) *
    FROM holdings
    WHERE symbol = $symbol
    ORDER BY symbol, fetched_at DESC
),
resolved AS (
    SELECT
        t.yahoo_ticker,
        ROW_NUMBER() OVER (ORDER BY (t.yahoo_ticker = h.yahoo_symbol) DESC) AS pref
    FROM latest_holding h
    JOIN tickers t ON h.symbol = t.raw_ticker
    WHERE EXISTS (SELECT 1 FROM afv_21_scores WHERE symbol = t.yahoo_ticker)
),
latest_score AS (
    SELECT DISTINCT ON (symbol) *
    FROM afv_21_scores
    WHERE symbol = (SELECT yahoo_ticker FROM resolved WHERE pref = 1)
    ORDER BY symbol, computed_at DESC
)
SELECT
    h.symbol,
    h.yahoo_symbol,
    h.description,
    h.isin,
    h.exchange,
    h.currency,
    h.position,
    h.mark_price,
    h.pos_value,
    h.cost_basis,
    ROUND(h.cost_basis / NULLIF(h.position, 0), 4)                                              AS cost_per_share,
    ROUND(h.pos_value - h.cost_basis, 2)                                                         AS pnl,
    ROUND(((h.mark_price - (h.cost_basis / NULLIF(h.position, 0)))
           / NULLIF(h.cost_basis / NULLIF(h.position, 0), 0)) * 100, 2)                         AS pnl_pct,
    h.fetched_at,
    r.yahoo_ticker,
    s.afv21,
    s.rp21,
    s.fcf_yield,
    s.ocf_margin,
    s.min_ocf_margin,
    s.ocf_margin_volatility,
    s.sector_score,
    s.geo_score,
    s.debt_score,
    s.trend_score,
    s.vd_score,
    s.computed_at AS score_computed_at,
    th.thesis,
    th.updated_at AS thesis_updated_at
FROM latest_holding h
LEFT JOIN (SELECT yahoo_ticker FROM resolved WHERE pref = 1) r ON true
LEFT JOIN latest_score s ON true
LEFT JOIN holding_thesis th ON th.symbol = h.symbol
"""


class ThesisBody(BaseModel):
    thesis: str


@router.get("/{symbol}")
def holding_detail(symbol: str):
    with db_cursor() as conn:
        import json
        df = conn.execute(HOLDING_DETAIL_SQL, {"symbol": symbol}).fetchdf()
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No holding found for symbol {symbol}")
        rows = json.loads(df.to_json(orient="records", date_format="iso"))
        return rows[0]


@router.put("/{symbol}/thesis")
def save_thesis(symbol: str, body: ThesisBody):
    with db_write_cursor() as conn:
        conn.execute("""
            INSERT INTO holding_thesis (symbol, thesis, updated_at)
            VALUES (?, ?, current_timestamp)
            ON CONFLICT (symbol) DO UPDATE SET
                thesis     = excluded.thesis,
                updated_at = current_timestamp
        """, [symbol, body.thesis])
    return {"ok": True}
