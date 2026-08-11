import json

from fastapi import APIRouter, HTTPException

from api.db import db_cursor

router = APIRouter(prefix="/prices", tags=["prices"])


@router.get("/{symbol}")
def price_history(symbol: str):
    with db_cursor() as conn:
        df = conn.execute(
            """
            SELECT date::VARCHAR AS date, open, high, low, close, volume
            FROM price_history
            WHERE symbol = $symbol
            ORDER BY date ASC
            """,
            {"symbol": symbol},
        ).fetchdf()

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No price history for {symbol}")

    return json.loads(df.to_json(orient="records"))
