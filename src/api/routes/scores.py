from fastapi import APIRouter, HTTPException
from api.db import db_cursor

router = APIRouter(prefix="/scores", tags=["scores"])

DETAIL_SQL = """
WITH latest AS (
    SELECT DISTINCT ON (symbol) *
    FROM afv_21_scores
    WHERE symbol = $symbol
      AND afv21 > -1000
    ORDER BY symbol, computed_at DESC
),
latest_info AS (
    SELECT DISTINCT ON (symbol) symbol, data
    FROM yahoo_data
    WHERE dataset = 'info' AND symbol = $symbol
    ORDER BY symbol, ts DESC
)
SELECT
    l.symbol,
    t.asset_name,
    l.afv21,
    l.rp21,
    l.fcf_yield,
    l.ocf_margin,
    l.min_ocf_margin,
    l.ocf_margin_volatility,
    l.sector_score,
    l.geo_score,
    l.debt_score,
    l.trend_score,
    l.vd_score,
    l.computed_at AS score_computed_at,
    json_extract_string(yi.data, '$.sector.0')   AS sector,
    json_extract_string(yi.data, '$.industry.0') AS industry,
    CAST(json_extract(yi.data, '$.marketCap.0')     AS BIGINT) AS market_cap,
    CAST(json_extract(yi.data, '$.averageVolume.0') AS BIGINT) AS avg_volume_3m
FROM latest l
LEFT JOIN tickers t ON l.symbol = t.yahoo_ticker
LEFT JOIN latest_info yi ON l.symbol = yi.symbol
"""

HISTORY_SQL = """
SELECT
    computed_at::DATE::VARCHAR AS date,
    afv21,
    rp21,
    fcf_yield,
    ocf_margin,
    sector_score,
    geo_score,
    debt_score,
    trend_score,
    vd_score
FROM afv_21_scores
WHERE symbol = $symbol
  AND afv21 > -1000
ORDER BY computed_at ASC
"""


@router.get("/{symbol}/detail")
def score_detail(symbol: str):
    with db_cursor() as conn:
        import json, numpy as np
        df = conn.execute(DETAIL_SQL, {"symbol": symbol}).fetchdf()
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No score data for {symbol}")
        df = df.replace([np.inf, -np.inf], np.nan)
        rows = json.loads(df.to_json(orient="records", date_format="iso"))
        return rows[0]


@router.get("/{symbol}/history")
def score_history(symbol: str):
    with db_cursor() as conn:
        import json, numpy as np
        df = conn.execute(HISTORY_SQL, {"symbol": symbol}).fetchdf()
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No score history for {symbol}")
        df = df.replace([np.inf, -np.inf], np.nan)
        return json.loads(df.to_json(orient="records"))
