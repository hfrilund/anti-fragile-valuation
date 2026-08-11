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

_SECTORS_BY_INDUSTRY: dict[str, list[str]] = {
    "Basic Materials": ["Agricultural Inputs","Aluminum","Building Materials","Chemicals","Coking Coal","Copper","Gold","Lumber & Wood Production","Other Industrial Metals & Mining","Other Precious Metals & Mining","Paper & Paper Products","Silver","Specialty Chemicals","Steel"],
    "Communication Services": ["Advertising Agencies","Broadcasting","Electronic Gaming & Multimedia","Entertainment","Internet Content & Information","Publishing","Telecom Services"],
    "Consumer Cyclical": ["Apparel Manufacturing","Apparel Retail","Auto & Truck Dealerships","Auto Manufacturers","Auto Parts","Department Stores","Footwear & Accessories","Furnishings, Fixtures & Appliances","Gambling","Home Improvement Retail","Internet Retail","Leisure","Lodging","Luxury Goods","Packaging & Containers","Personal Services","Recreational Vehicles","Residential Construction","Resorts & Casinos","Restaurants","Specialty Retail","Textile Manufacturing","Travel Services"],
    "Consumer Defensive": ["Beverages - Brewers","Beverages - Non-Alcoholic","Beverages - Wineries & Distilleries","Confectioners","Discount Stores","Education & Training Services","Farm Products","Food Distribution","Grocery Stores","Household & Personal Products","Packaged Foods","Tobacco"],
    "Energy": ["Oil & Gas Drilling","Oil & Gas E&P","Oil & Gas Equipment & Services","Oil & Gas Integrated","Oil & Gas Midstream","Oil & Gas Refining & Marketing","Thermal Coal","Uranium"],
    "Financial Services": ["Asset Management","Banks - Diversified","Banks - Regional","Capital Markets","Credit Services","Financial Conglomerates","Financial Data & Stock Exchanges","Insurance - Diversified","Insurance - Life","Insurance - Property & Casualty","Insurance - Reinsurance","Insurance - Specialty","Insurance Brokers","Mortgage Finance","Shell Companies"],
    "Healthcare": ["Biotechnology","Diagnostics & Research","Drug Manufacturers - General","Drug Manufacturers - Specialty & Generic","Health Information Services","Healthcare Plans","Medical Care Facilities","Medical Devices","Medical Distribution","Medical Instruments & Supplies","Pharmaceutical Retailers"],
    "Industrials": ["Aerospace & Defense","Airlines","Airports & Air Services","Building Products & Equipment","Business Equipment & Supplies","Conglomerates","Consulting Services","Electrical Equipment & Parts","Engineering & Construction","Farm & Heavy Construction Machinery","Industrial Distribution","Infrastructure Operations","Integrated Freight & Logistics","Marine Shipping","Metal Fabrication","Pollution & Treatment Controls","Railroads","Rental & Leasing Services","Security & Protection Services","Specialty Business Services","Specialty Industrial Machinery","Staffing & Employment Services","Tools & Accessories","Trucking","Waste Management"],
    "Real Estate": ["REIT - Diversified","REIT - Healthcare Facilities","REIT - Hotel & Motel","REIT - Industrial","REIT - Mortgage","REIT - Office","REIT - Residential","REIT - Retail","REIT - Specialty","Real Estate - Development","Real Estate - Diversified","Real Estate Services"],
    "Technology": ["Communication Equipment","Computer Hardware","Consumer Electronics","Electronic Components","Electronics & Computer Distribution","Information Technology Services","Scientific & Technical Instruments","Semiconductor Equipment & Materials","Semiconductors","Software - Application","Software - Infrastructure","Solar"],
    "Utilities": ["Utilities - Diversified","Utilities - Independent Power Producers","Utilities - Regulated Electric","Utilities - Regulated Gas","Utilities - Regulated Water","Utilities - Renewable"],
}

SCREEN_SQL = """
WITH latest_scores AS (
    SELECT symbol, afv21, rp21
    FROM (
        SELECT DISTINCT ON (symbol)
            symbol, afv21, rp21
        FROM afv_21_scores
        WHERE afv21 > -1000
        ORDER BY symbol, computed_at DESC
    )
    ORDER BY afv21 DESC
    LIMIT $candidate_limit
),
latest_info AS (
    SELECT DISTINCT ON (yd.symbol)
        yd.symbol,
        json_extract_string(yd.data, '$.sector.0')   AS sector,
        json_extract_string(yd.data, '$.industry.0') AS industry,
        CAST(json_extract(yd.data, '$.marketCap.0')     AS BIGINT) AS market_cap,
        CAST(json_extract(yd.data, '$.averageVolume.0') AS BIGINT) AS avg_volume_3m
    FROM yahoo_data yd
    JOIN latest_scores ls ON yd.symbol = ls.symbol
    WHERE yd.dataset = 'info'
    ORDER BY yd.symbol, yd.ts DESC
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
    return {
        "sectors": sorted(_SECTORS_BY_INDUSTRY.keys()),
        "industries_by_sector": _SECTORS_BY_INDUSTRY,
    }


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
            "candidate_limit": max(1500, limit * 10),
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
