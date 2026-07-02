#!/usr/bin/env python3
"""
AFV21 score lookup for current IBKR holdings.

Reads a Flex Query XML export from IBKR and prints the latest AFV21 score
for each stock position. Options are skipped. Stocks not yet scored are
flagged as missing.

Usage:
    python holdings_report.py [XML_FILE]

    Defaults to ../../holdings_data/Current_holdings.xml relative to this file.
"""
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database.db import connect, DB_PATH

DEFAULT_XML = Path(__file__).resolve().parent.parent.parent / "holdings_data" / "Current_holdings.xml"

# IBKR listingExchange codes → Yahoo Finance ticker suffix
_IBKR_EXCHANGE_SUFFIX = {
    'SBF':   '.PA',   # Euronext Paris
    'AEB':   '.AS',   # Euronext Amsterdam
    'IBIS':  '.DE',   # Xetra
    'IBIS2': '.DE',
    'VSE':   '.VI',   # Vienna
    'SWX':   '.SW',   # Swiss Exchange
    'BVL':   '.LS',   # Lisbon
    'BM':    '.MC',   # Madrid
    'BVME':  '.MI',   # Milan
    'HEX':   '.HE',   # Helsinki
    'OSE':   '.OL',   # Oslo
    'SFB':   '.ST',   # Stockholm
    'KSE':   '.CO',   # Copenhagen
    'LSE':   '.L',    # London
}


def _yahoo_symbol(ibkr_symbol: str, ibkr_exchange: str) -> str:
    """Map an IBKR symbol + exchange to the Yahoo Finance ticker used in the DB."""
    suffix = _IBKR_EXCHANGE_SUFFIX.get(ibkr_exchange, '')
    return f"{ibkr_symbol}{suffix}" if suffix else ibkr_symbol


def _positions_from_root(root) -> list[dict]:
    positions = []
    for pos in root.iter("OpenPosition"):
        if pos.attrib.get("assetCategory") != "STK":
            continue
        ibkr_sym = pos.attrib.get("symbol", "")
        exchange  = pos.attrib.get("listingExchange", "")
        positions.append({
            "symbol":       ibkr_sym,
            "yahoo_symbol": _yahoo_symbol(ibkr_sym, exchange),
            "description":  pos.attrib.get("description", ""),
            "isin":         pos.attrib.get("isin", ""),
            "exchange":     exchange,
            "currency":     pos.attrib.get("currency", ""),
            "position":     float(pos.attrib.get("position", 0)),
            "mark_price":   float(pos.attrib.get("markPrice", 0)),
            "pos_value":    float(pos.attrib.get("positionValue", 0)),
            "cost_basis":   float(pos.attrib.get("costBasisMoney", 0)),
        })
    return positions


def parse_stock_positions(xml_path: Path) -> list[dict]:
    return _positions_from_root(ET.parse(xml_path).getroot())


def parse_stock_positions_from_string(xml_content: str) -> list[dict]:
    return _positions_from_root(ET.fromstring(xml_content))


def fetch_latest_scores(symbols: list[str], db_path: str = DB_PATH) -> dict[str, dict]:
    """Return a dict keyed by symbol with the latest and previous afv_21_scores rows."""
    if not symbols:
        return {}
    placeholders = ", ".join("?" * len(symbols))
    query = f"""
        SELECT symbol, afv21, rp21, fcf_yield, computed_at,
               ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY computed_at DESC) AS rn
        FROM afv_21_scores
        WHERE symbol IN ({placeholders})
        QUALIFY rn <= 2
        ORDER BY symbol, rn
    """
    with connect(db_path) as con:
        rows = con.execute(query, symbols).fetchall()

    result: dict[str, dict] = {}
    for row in rows:
        sym, afv21, rp21, fcf_yield, computed_at, rn = row
        if rn == 1:
            result[sym] = {
                "afv21":      afv21,
                "rp21":       rp21,
                "fcf_yield":  fcf_yield,
                "computed_at": computed_at.strftime("%Y-%m-%d") if computed_at else "",
                "prev_afv21": None,
            }
        elif sym in result:
            result[sym]["prev_afv21"] = afv21
    return result


def save_holdings(positions: list[dict], db_path: str = DB_PATH) -> None:
    """Persist today's holdings snapshot, replacing any earlier snapshot from today."""
    rows = [
        (
            p["yahoo_symbol"], p["symbol"], p["description"], p["isin"],
            p["exchange"], p["currency"], p["position"],
            p["mark_price"], p["pos_value"], p["cost_basis"],
        )
        for p in positions
    ]
    with connect(db_path) as con:
        con.execute("DELETE FROM holdings WHERE fetched_at::DATE = current_date")
        con.executemany("""
            INSERT INTO holdings
                (yahoo_symbol, symbol, description, isin, exchange, currency,
                 position, mark_price, pos_value, cost_basis)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
    print(f"Saved {len(rows)} position(s) to holdings table.")


def print_report(positions: list[dict], scores: dict[str, dict]) -> None:
    header = (
        f"{'Symbol':<10} {'Description':<30} {'Exch':<6} {'CCY':<5}"
        f" {'Pos':>8} {'Mark':>10} {'Value':>10} {'Cost':>10}"
        f"   {'AFV21':>7} {'Prev':>7} {'Trend':<6} {'RP21':>6} {'FCFYld':>7} {'Scored':<12}"
    )
    divider = "-" * len(header)

    print(divider)
    print(header)
    print(divider)

    found = 0
    missing = []

    for p in sorted(positions, key=lambda x: x["yahoo_symbol"]):
        sym   = p["symbol"]
        ysym  = p["yahoo_symbol"]
        score = scores.get(ysym)

        if score:
            afv21_s  = f"{score['afv21']:>7.2f}"
            rp21_s   = f"{score['rp21']:>6.2f}"
            fcfy_s   = f"{score['fcf_yield']:>7.2%}" if score["fcf_yield"] is not None else f"{'N/A':>7}"
            scored_s = score["computed_at"]
            prev     = score["prev_afv21"]
            prev_s   = f"{prev:>7.2f}" if prev is not None else f"{'N/A':>7}"
            if prev is not None:
                trend_s = "rising" if score["afv21"] > prev else "falling"
            else:
                trend_s = "N/A"
            found += 1
        else:
            afv21_s  = f"{'N/A':>7}"
            prev_s   = f"{'N/A':>7}"
            trend_s  = "N/A"
            rp21_s   = f"{'N/A':>6}"
            fcfy_s   = f"{'N/A':>7}"
            scored_s = "not in DB"
            missing.append(ysym)

        pnl_pct = (p["pos_value"] - p["cost_basis"]) / p["cost_basis"] * 100 if p["cost_basis"] else 0
        display_sym = ysym
        desc = p["description"][:29]

        print(
            f"{display_sym:<10} {desc:<30} {p['exchange']:<6} {p['currency']:<5}"
            f" {p['position']:>8.0f} {p['mark_price']:>10.3f} {p['pos_value']:>10.2f} {p['cost_basis']:>10.2f}"
            f"   {afv21_s} {prev_s} {trend_s:<6} {rp21_s} {fcfy_s} {scored_s:<12}"
            f"  ({pnl_pct:+.1f}%)"
        )

    print(divider)
    print(f"\n{found}/{len(positions)} positions have AFV21 scores.")
    if missing:
        print(f"Not in DB: {', '.join(missing)}")
        print("  -> Run the AFV21 scorer for these tickers, or they may be EU-listed stocks not yet covered.")


def main():
    xml_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_XML

    if not xml_path.exists():
        print(f"ERROR: XML file not found: {xml_path}", file=sys.stderr)
        sys.exit(1)

    positions = parse_stock_positions(xml_path)
    if not positions:
        print("No stock positions found in the XML.")
        return

    symbols = [p["yahoo_symbol"] for p in positions]
    scores = fetch_latest_scores(symbols)
    print_report(positions, scores)


if __name__ == "__main__":
    main()
