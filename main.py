#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / 'src'))

from database import db
from ticker_management.manage_tickers import refresh_us, refresh_eu
from afv20.afv_processor import process, process_single
from holdings.holdings_report import (
    parse_stock_positions,
    parse_stock_positions_from_string,
    fetch_latest_scores,
    print_report,
    save_holdings,
    save_fx_rates,
)
from technical_analysis.analyzer import run as run_ta, run_holdings as run_holdings_ta
from price_history.fetcher import fetch_and_store as fetch_prices, ensure_for_ta
from holdings.sharpe import compute as compute_sharpe

DEFAULT_DB = str(Path(__file__).resolve().parent / 'data' / 'finance_data.db')


def cmd_db_update_schema(args):
    db.init_schema(args.db)
    print("Schema initialised.")
    db.migrate(args.db)
    print("Migrations applied.")


def cmd_tickers_refresh_us(args):
    refresh_us(args.db)


def cmd_tickers_refresh_eu(args):
    refresh_eu(args.db)


def cmd_tickers_refresh_all(args):
    print("=== US tickers ===")
    refresh_us(args.db)
    print()
    print("=== EU tickers ===")
    refresh_eu(args.db)


def cmd_score_symbol(args):
    process_single(args.symbol, db_file_path=args.db, save=args.save)


def cmd_score_history(args):
    rows = db.connect(args.db).execute("""
        SELECT computed_at, afv21, rp21, fcf_yield,
               sector_score, geo_score, debt_score, trend_score, vd_score
        FROM afv_21_scores
        WHERE symbol = ? AND afv != -1000
        ORDER BY computed_at ASC
    """, (args.symbol,)).fetchall()

    if not rows:
        print(f"No AFV21 history found for {args.symbol}.")
        return

    header = (f"{'Date':<12} {'AFV21':>7} {'RP21':>6} {'FCFYld':>7}"
              f" {'Sect':>5} {'Geo':>5} {'Debt':>5} {'Trend':>5} {'VD':>5}")
    print(f"\n  AFV21 history — {args.symbol}  ({len(rows)} entries)")
    print("-" * len(header))
    print(header)
    print("-" * len(header))

    prev_afv21 = None
    for computed_at, afv21, rp21, fcf_yield, sect, geo, debt, trend, vd in rows:
        date_s   = computed_at.strftime("%Y-%m-%d") if computed_at else "N/A"
        afv21_s  = f"{afv21:>7.2f}" if afv21 is not None else f"{'N/A':>7}"
        rp21_s   = f"{rp21:>6.2f}"  if rp21  is not None else f"{'N/A':>6}"
        fcfy_s   = f"{fcf_yield:>7.2%}" if fcf_yield is not None else f"{'N/A':>7}"
        sect_s   = f"{sect:>5.2f}"  if sect  is not None else f"{'N/A':>5}"
        geo_s    = f"{geo:>5.2f}"   if geo   is not None else f"{'N/A':>5}"
        debt_s   = f"{debt:>5.2f}"  if debt  is not None else f"{'N/A':>5}"
        trend_s  = f"{trend:>5.2f}" if trend is not None else f"{'N/A':>5}"
        vd_s     = f"{vd:>5.2f}"    if vd    is not None else f"{'N/A':>5}"

        arrow = ""
        if prev_afv21 is not None and afv21 is not None:
            arrow = " ▲" if afv21 > prev_afv21 else (" ▼" if afv21 < prev_afv21 else " –")
        prev_afv21 = afv21

        print(f"{date_s:<12} {afv21_s}{arrow:<2} {rp21_s} {fcfy_s} {sect_s} {geo_s} {debt_s} {trend_s} {vd_s}")

    print("-" * len(header))


def cmd_run_daily(args):
    print("=== Step 1: Update schema ===")
    db.init_schema(args.db)
    db.migrate(args.db)
    print("Schema up to date.")
    print()

    print("=== Step 2: Refresh tickers ===")
    refresh_us(args.db)
    print()
    refresh_eu(args.db)
    print()

    print("=== Step 3: Save holdings from IBKR ===")
    with db.connect(args.db) as con:
        already_saved = con.execute(
            "SELECT COUNT(*) FROM holdings WHERE fetched_at::DATE = current_date"
        ).fetchone()[0]
    if already_saved:
        print(f"  Holdings already saved today ({already_saved} positions). Skipping IBKR call.")
    else:
        try:
            from ibkr.flex_client import fetch_flex_xml
            xml_content = fetch_flex_xml()
            positions   = parse_stock_positions_from_string(xml_content)
            save_holdings(positions, db_path=args.db)
        except (ValueError, RuntimeError, TimeoutError) as e:
            print(f"  Skipping holdings update: {e}")

    print("  Saving FX rates…")
    with db.connect(args.db) as con:
        ccys = [r[0] for r in con.execute("""
            SELECT DISTINCT currency FROM holdings
            WHERE fetched_at::DATE = current_date AND currency IS NOT NULL
        """).fetchall()]
    if ccys:
        save_fx_rates(ccys, db_path=args.db)
    print()

    print("=== Step 4: Score all tickers ===")
    process(args.db)
    print()

    print("=== Step 5: Ensure price history ===")
    ensure_for_ta(args.db)
    print()

    print("=== Step 6: Technical analysis (top 500) ===")
    run_ta(args.db)
    print()

    print("=== Step 7: Holdings technical analysis ===")
    run_holdings_ta(args.db)


def cmd_prices_fetch(args):
    fetch_prices(args.db, period=args.period, top=args.top)


def cmd_ta_run(args):
    run_ta(args.db)


def _fetch_positions(args) -> list[dict]:
    """Shared helper: parse positions from Flex API or a local XML file."""
    if args.flex:
        from ibkr.flex_client import fetch_flex_xml
        print("Fetching holdings from IBKR Flex Web Service…")
        xml_content = fetch_flex_xml(
            token=getattr(args, "token", None),
            query_id=getattr(args, "query_id", None),
        )
        return parse_stock_positions_from_string(xml_content)
    else:
        xml_path = Path(args.file)
        if not xml_path.exists():
            print(f"ERROR: XML file not found: {xml_path}", file=sys.stderr)
            sys.exit(1)
        return parse_stock_positions(xml_path)


def cmd_holdings_report(args):
    positions = _fetch_positions(args)
    if not positions:
        print("No stock positions found in the XML.")
        return
    scores = fetch_latest_scores([p["yahoo_symbol"] for p in positions], db_path=args.db)
    print_report(positions, scores)


def cmd_holdings_save(args):
    positions = _fetch_positions(args)
    if not positions:
        print("No stock positions found.")
        return
    save_holdings(positions, db_path=args.db)


def cmd_holdings_ta(args):
    run_holdings_ta(args.db)


def cmd_holdings_sharpe(args):
    compute_sharpe(
        db_path=args.db,
        lookback_days=args.lookback,
        risk_free_rate=args.risk_free_rate,
    )


def main():
    parser = argparse.ArgumentParser(prog='afv', description='AFV management CLI')
    parser.add_argument('--db', default=DEFAULT_DB, metavar='PATH',
                        help=f'Path to DuckDB file (default: {DEFAULT_DB})')

    sub = parser.add_subparsers(dest='group', required=True)

    # --- db group ---
    db_parser = sub.add_parser('db', help='Database commands')
    db_sub = db_parser.add_subparsers(dest='cmd', required=True)
    db_sub.add_parser('update-schema', help='Initialise schema and apply migrations')

    # --- tickers group ---
    tickers_parser = sub.add_parser('tickers', help='Ticker management commands')
    tickers_sub = tickers_parser.add_subparsers(dest='cmd', required=True)
    tickers_sub.add_parser('refresh-us',  help='Download latest US tickers from GitHub')
    tickers_sub.add_parser('refresh-eu',  help='Download latest EU tickers from CBOE')
    tickers_sub.add_parser('refresh-all', help='Refresh both US and EU tickers')

    # --- score group ---
    score_parser = sub.add_parser('score', help='Score commands')
    score_sub = score_parser.add_subparsers(dest='cmd', required=True)
    p_score = score_sub.add_parser('symbol', help='Compute AFV score for a single symbol')
    p_score.add_argument('symbol', help='Yahoo Finance ticker, e.g. AAPL or SSABBH.HE')
    p_score.add_argument('--save', action='store_true', help='Save the computed score to the database')

    p_history = score_sub.add_parser('history', help='Show full AFV21 score history for a symbol')
    p_history.add_argument('symbol', help='Yahoo Finance ticker, e.g. AAPL or SSABBH.HE')

    # --- run group ---
    run_parser = sub.add_parser('run', help='Run commands')
    run_sub = run_parser.add_subparsers(dest='cmd', required=True)
    run_sub.add_parser('daily', help='Full daily run: update schema, refresh tickers, score all, technical analysis')

    # --- prices group ---
    prices_parser = sub.add_parser('prices', help='Price history commands')
    prices_sub = prices_parser.add_subparsers(dest='cmd', required=True)
    p_fetch = prices_sub.add_parser('fetch', help='Fetch and store OHLCV price history')
    p_fetch.add_argument(
        '--period', default='7d', metavar='PERIOD',
        help='yfinance period string (default: 7d for daily updates, use 2y for initial backfill)',
    )
    p_fetch.add_argument(
        '--top', type=int, default=2000, metavar='N',
        help='fetch price history for top N symbols by AFV21 score (default: 2000)',
    )

    # --- ta group ---
    ta_parser = sub.add_parser('ta', help='Technical analysis commands')
    ta_sub = ta_parser.add_subparsers(dest='cmd', required=True)
    ta_sub.add_parser('run', help='Run technical analysis on top 500 AFV21 symbols')

    # --- holdings group ---
    DEFAULT_XML = str(Path(__file__).resolve().parent / 'holdings_data' / 'Current_holdings.xml')
    holdings_parser = sub.add_parser('holdings', help='Holdings commands')
    holdings_sub = holdings_parser.add_subparsers(dest='cmd', required=True)
    def _add_source_args(p):
        p.add_argument('--flex', action='store_true',
                       help='Fetch live from IBKR Flex Web Service instead of a local file')
        p.add_argument('--file', default=DEFAULT_XML, metavar='PATH',
                       help=f'Path to IBKR Flex Query XML (default: {DEFAULT_XML})')
        p.add_argument('--token', default=None, metavar='TOKEN',
                       help='Flex Web Service token (overrides IBKR_FLEX_TOKEN env var)')
        p.add_argument('--query-id', default=None, metavar='ID',
                       help='Flex Query ID to run (overrides IBKR_FLEX_QUERY_ID env var)')

    p_report = holdings_sub.add_parser('report', help='Print AFV21 report for current holdings')
    _add_source_args(p_report)

    p_save = holdings_sub.add_parser('save', help='Fetch holdings and store them in the database')
    _add_source_args(p_save)

    holdings_sub.add_parser('ta', help='Run technical analysis on current holdings')

    p_sharpe = holdings_sub.add_parser('sharpe', help='Compute portfolio Sharpe ratio from price history')
    p_sharpe.add_argument(
        '--lookback', type=int, default=365, metavar='DAYS',
        help='Number of calendar days of price history to use (default: 365)',
    )
    p_sharpe.add_argument(
        '--risk-free-rate', type=float, default=0.04, metavar='RATE',
        help='Annualised risk-free rate as a decimal (default: 0.04 = 4%%)',
    )

    args = parser.parse_args()

    if args.group == 'db':
        if args.cmd == 'update-schema':
            cmd_db_update_schema(args)

    elif args.group == 'tickers':
        if args.cmd == 'refresh-us':
            cmd_tickers_refresh_us(args)
        elif args.cmd == 'refresh-eu':
            cmd_tickers_refresh_eu(args)
        elif args.cmd == 'refresh-all':
            cmd_tickers_refresh_all(args)

    elif args.group == 'score':
        if args.cmd == 'symbol':
            cmd_score_symbol(args)
        elif args.cmd == 'history':
            cmd_score_history(args)

    elif args.group == 'run':
        if args.cmd == 'daily':
            cmd_run_daily(args)

    elif args.group == 'prices':
        if args.cmd == 'fetch':
            cmd_prices_fetch(args)

    elif args.group == 'ta':
        if args.cmd == 'run':
            cmd_ta_run(args)

    elif args.group == 'holdings':
        if args.cmd == 'report':
            cmd_holdings_report(args)
        elif args.cmd == 'save':
            cmd_holdings_save(args)
        elif args.cmd == 'ta':
            cmd_holdings_ta(args)
        elif args.cmd == 'sharpe':
            cmd_holdings_sharpe(args)


if __name__ == '__main__':
    main()
