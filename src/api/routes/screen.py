from fastapi import APIRouter, HTTPException, Query, Request
from typing import Optional, List
from api.db import db_cursor
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/screen", tags=["screen"])

CAP_RANGES = {
    "micro":  (0,          300_000_000),
    "small":  (300_000_000,  2_000_000_000),
    "mid":    (2_000_000_000, 10_000_000_000),
    "large":  (10_000_000_000, 200_000_000_000),
    "mega":   (200_000_000_000, None),
}

OPTIONS_SQL = """
WITH scored_symbols AS (
    SELECT DISTINCT symbol FROM afv_21_scores WHERE afv21 > -1000
)
SELECT sector, industry
FROM (
    SELECT DISTINCT ON (yd.symbol)
        yd.symbol,
        json_extract_string(yd.data, '$.sector.0')   AS sector,
        json_extract_string(yd.data, '$.industry.0') AS industry
    FROM yahoo_data yd
    JOIN scored_symbols s ON yd.symbol = s.symbol
    WHERE yd.dataset = 'info'
    ORDER BY yd.symbol, yd.ts DESC
) latest
WHERE sector IS NOT NULL AND sector != ''
GROUP BY sector, industry
ORDER BY sector, industry
"""

SCREEN_SQL = """
WITH latest_scores AS (
    SELECT DISTINCT ON (symbol)
        symbol, afv21, rp21, computed_at
    FROM afv_21_scores
    WHERE afv21 > -1000
    ORDER BY symbol, computed_at DESC
),
latest_info AS (
    SELECT DISTINCT ON (symbol)
        symbol,
        json_extract_string(data, '$.sector.0')   AS sector,
        json_extract_string(data, '$.industry.0') AS industry,
        CAST(json_extract(data, '$.marketCap.0')     AS BIGINT) AS market_cap,
        CAST(json_extract(data, '$.averageVolume.0') AS BIGINT) AS avg_volume_3m
    FROM yahoo_data
    WHERE dataset = 'info'
    ORDER BY symbol, ts DESC
),
latest_ta AS (
    SELECT DISTINCT ON (symbol)
        symbol, ma_cross_signal, ma_cross_days_ago, ma_distance_pct,
        rsi14, macd_sentiment, obv_trend, ma200_trend, close_price
    FROM technical_analysis
    ORDER BY symbol, computed_at DESC
)
SELECT
    s.symbol,
    t.asset_name,
    s.afv21,
    s.rp21,
    i.sector,
    i.industry,
    i.market_cap,
    i.avg_volume_3m,
    ta.ma_cross_signal,
    ta.ma_cross_days_ago,
    ta.ma_distance_pct,
    ta.rsi14,
    ta.macd_sentiment,
    ta.obv_trend,
    ta.ma200_trend,
    ta.close_price
FROM latest_scores s
JOIN latest_info i ON s.symbol = i.symbol
LEFT JOIN latest_ta ta ON s.symbol = ta.symbol
LEFT JOIN tickers t ON s.symbol = t.yahoo_ticker
WHERE s.afv21 >= $min_afv21
  AND ($sector   IS NULL OR i.sector   = $sector)
  AND ($industry IS NULL OR i.industry = $industry)
  AND ($min_cap  IS NULL OR i.market_cap >= $min_cap)
  AND ($max_cap  IS NULL OR i.market_cap <  $max_cap)
  AND ($ma_cross IS NULL OR ta.ma_cross_signal = $ma_cross)
  AND ($potential_cross = false OR (ta.ma_cross_signal = 'death_cross' AND ta.ma_distance_pct > -3.0))
  AND ($macd     IS NULL OR ta.macd_sentiment  = $macd)
  AND ($min_rsi  IS NULL OR ta.rsi14 >= $min_rsi)
  AND ($max_rsi  IS NULL OR ta.rsi14 <= $max_rsi)
ORDER BY s.afv21 DESC
LIMIT $limit
"""


def _to_dicts(conn, sql: str, params: dict) -> list[dict]:
    import json
    df = conn.execute(sql, params).fetchdf()
    return json.loads(df.to_json(orient="records", date_format="iso"))


@router.get("/options")
def screen_options():
    with db_cursor() as conn:
        import json
        try:
            df = conn.execute(OPTIONS_SQL).fetchdf()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        rows = json.loads(df.to_json(orient="records"))

    sectors = sorted({r["sector"] for r in rows if r["sector"]})
    by_sector: dict[str, list[str]] = {}
    for r in rows:
        if r["sector"] and r["industry"]:
            by_sector.setdefault(r["sector"], []).append(r["industry"])

    return {"sectors": sectors, "industries_by_sector": by_sector}


@router.get("")
@limiter.limit("20/minute")
def screen(
    request:   Request,
    sector:    Optional[str]   = Query(None),
    industry:  Optional[str]   = Query(None),
    cap_size:  Optional[str]   = Query(None, description="micro|small|mid|large|mega"),
    min_afv21: float           = Query(0.0),
    ma_cross:  Optional[str]   = Query(None, pattern="^(golden_cross|death_cross|potential_cross)$"),
    macd:      Optional[str]   = Query(None, pattern="^(bullish|bearish|neutral)$"),
    min_rsi:   Optional[float] = Query(None),
    max_rsi:   Optional[float] = Query(None),
    limit:     int             = Query(100, ge=1, le=500),
):
    min_cap, max_cap = None, None
    if cap_size and cap_size in CAP_RANGES:
        min_cap, max_cap = CAP_RANGES[cap_size]

    potential_cross = ma_cross == "potential_cross"
    ma_cross_param  = None if potential_cross else ma_cross

    with db_cursor() as conn:
        rows = _to_dicts(conn, SCREEN_SQL, {
            "min_afv21":       min_afv21,
            "sector":          sector,
            "industry":        industry,
            "min_cap":         min_cap,
            "max_cap":         max_cap,
            "ma_cross":        ma_cross_param,
            "potential_cross": potential_cross,
            "macd":            macd,
            "min_rsi":         min_rsi,
            "max_rsi":         max_rsi,
            "limit":           limit,
        })
    return {"count": len(rows), "results": rows}
