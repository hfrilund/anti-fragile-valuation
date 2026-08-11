import random
import time

import duckdb
import pandas as pd

from finance_data_sources import yahoo

_NEEDED_DATASETS = {'cashflow', 'financials', 'balance_sheet', 'info'}
_FINANCIAL_DATASETS = {'cashflow', 'financials', 'balance_sheet'}
_CONSECUTIVE_FAILURE_THRESHOLD = 2

_REVIVAL_SAMPLE_SIZE = 50
_REVIVAL_MIN_INTERVAL_DAYS = 30
_REVIVAL_GIVE_UP_AFTER_DAYS = 365


def _mark_dead(con, symbol: str, reason: str):
    # Preserve original dead_since so revival tracking stays accurate
    con.execute("""
        UPDATE tickers SET
            is_dead = true,
            dead_reason = ?,
            dead_since = CASE WHEN dead_since IS NULL THEN current_timestamp ELSE dead_since END
        WHERE yahoo_ticker = ?
    """, (reason, symbol))
    con.commit()
    print(f"Marked dead: {symbol} ({reason})")


def _pick_revival_candidates(con, n: int = _REVIVAL_SAMPLE_SIZE) -> list[str]:
    """Return up to n auto-dead tickers eligible for a revival attempt today."""
    rows = con.execute("""
        SELECT yahoo_ticker FROM tickers
        WHERE is_dead = true
          AND dead_reason != 'manually marked dead'
          AND (dead_since IS NULL OR dead_since >= current_timestamp - (? * INTERVAL '1 day'))
          AND (last_revival_attempt IS NULL
               OR last_revival_attempt < current_timestamp - (? * INTERVAL '1 day'))
        ORDER BY random()
        LIMIT ?
    """, (_REVIVAL_GIVE_UP_AFTER_DAYS, _REVIVAL_MIN_INTERVAL_DAYS, n)).fetchall()
    return [r[0] for r in rows]


def _consecutive_failures(con, symbol: str) -> int:
    """Count how many of the most recent scores for this symbol are -1000."""
    rows = con.execute(
        "select afv from afv_21_scores where symbol = ? order by computed_at desc limit ?",
        (symbol, _CONSECUTIVE_FAILURE_THRESHOLD)
    ).fetchall()
    count = 0
    for (afv,) in rows:
        if afv == -1000:
            count += 1
        else:
            break
    return count


def process(db_file_path: str = '../../data/finance_data.db'):
    con = duckdb.connect(db_file_path)
    yf = yahoo.YahooFinanceDataSource(con)

    revival_candidates = _pick_revival_candidates(con)
    if revival_candidates:
        print(f"\n--- Attempting revival of {len(revival_candidates)} dead ticker(s) ---")
        placeholders = ', '.join('?' * len(revival_candidates))
        con.execute(
            f"UPDATE tickers SET is_dead = false WHERE yahoo_ticker IN ({placeholders})",
            revival_candidates
        )
        con.commit()

    tickers = con.execute("select * from tickers where is_dead is not true").fetchdf()

    for idx, row in tickers.iterrows():
        symbol = row['yahoo_ticker']

        exists = con.execute(
            "select count(*) from afv_21_scores where symbol = ? and computed_at > current_timestamp - interval 1 month and afv != -1000",
            (symbol,)
        ).fetchone()
        if exists and exists[0] > 0:
            print(f"AFV score for {symbol} already computed within a month, skipping...")
            continue

        # Dead by accumulated failures across runs
        if _consecutive_failures(con, symbol) >= _CONSECUTIVE_FAILURE_THRESHOLD:
            _mark_dead(con, symbol, f'{_CONSECUTIVE_FAILURE_THRESHOLD} consecutive processing failures')
            continue

        try:
            cached_datasets = {
                row[0] for row in con.execute(
                    "select distinct dataset from yahoo_data where symbol = ? and ts > current_timestamp - interval 1 month",
                    (symbol,)
                ).fetchall()
            }
            all_cached = _NEEDED_DATASETS.issubset(cached_datasets)

            if all_cached:
                print(f"All Yahoo data for {symbol} cached, skipping fetch...")
            else:
                time.sleep(random.uniform(1, 3.5))
                can_be_found = yf.can_be_found(symbol)

                if not can_be_found:
                    _mark_dead(con, symbol, 'not found on Yahoo Finance')
                    continue

                failed = yf.prefetch(symbol)
                if failed & _FINANCIAL_DATASETS:
                    _mark_dead(con, symbol, f'prefetch failed for: {", ".join(sorted(failed & _FINANCIAL_DATASETS))}')
                    continue

            con.execute("BEGIN")
            fcf_yield = yf.fcf_yield(symbol)
            ocf_margin, min_ocf_margin = yf.ocf_margin(symbol)
            ocf_margin_volatility = yf.ocf_margin_volatility(symbol)
            has_negative_net_income, avg_net_margin = yf.net_income_check(symbol)
            scaled_rp = yf.scaled_rp(fcf_yield, ocf_margin, min_ocf_margin, ocf_margin_volatility)
            scaled_rp21 = yf.scaled_rp_21(fcf_yield, ocf_margin, min_ocf_margin, ocf_margin_volatility, has_negative_net_income, avg_net_margin)
            sector_score = yf.sector_score(symbol)
            geo_score = yf.geo_score(symbol)
            debt_score = yf.debt_score(symbol)
            trend_score = yf.trend_score(symbol)
            vd_score = yf.vd_score(symbol)

            sector = yf.sector(symbol)
            industry_score = yf.industry_score(symbol)

            if sector == 'Financial Services':
                scaled_rp = scaled_rp21 = debt_score = vd_score = 0
            elif sector == 'Industrials' and industry_score == -1:
                vd_score = 0

            afv_score = scaled_rp + sector_score + geo_score + debt_score + trend_score + vd_score
            afv21_score = scaled_rp21 + sector_score + geo_score + debt_score + trend_score + vd_score

            print(f"AFV Score for {symbol}: {afv_score}")
            print(f"AFV 2.1 Score for {symbol}: {afv21_score}\n")

            con.execute("""
                insert into afv_21_scores (symbol, afv, afv21, rp, rp21, fcf_yield, ocf_margin, min_ocf_margin, ocf_margin_volatility, sector_score, geo_score, debt_score, trend_score, vd_score, computed_at)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
            """, (symbol, afv_score, afv21_score, scaled_rp, scaled_rp21, fcf_yield, ocf_margin, min_ocf_margin, ocf_margin_volatility, sector_score, geo_score, debt_score, trend_score, vd_score))
            con.commit()

        except Exception as e:
            print(f"Error processing {symbol}: {e}, storing AFV -1000")
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
            con.execute("""
                insert into afv_21_scores (symbol, afv, afv21, rp, rp21, fcf_yield, ocf_margin, min_ocf_margin, ocf_margin_volatility, sector_score, geo_score, debt_score, trend_score, vd_score, computed_at)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
            """, (symbol, -1000, -1000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
            con.commit()

    if revival_candidates:
        revived, failed = [], []
        for symbol in revival_candidates:
            still_dead = con.execute(
                "SELECT is_dead FROM tickers WHERE yahoo_ticker = ?", (symbol,)
            ).fetchone()
            if still_dead and still_dead[0]:
                failed.append(symbol)
            else:
                revived.append(symbol)

        if revived:
            print(f"\n--- Revived {len(revived)} ticker(s): {', '.join(revived)} ---")

        if failed:
            placeholders = ', '.join('?' * len(failed))
            con.execute(
                f"UPDATE tickers SET last_revival_attempt = current_timestamp WHERE yahoo_ticker IN ({placeholders})",
                failed
            )
            con.commit()
            print(f"--- {len(failed)} revival attempt(s) failed; will retry in {_REVIVAL_MIN_INTERVAL_DAYS} days ---")

    con.close()

def process_single(symbol: str, db_file_path: str = '../../data/finance_data.db', save: bool = False):
    con = duckdb.connect(db_file_path)
    yf_ds = yahoo.YahooFinanceDataSource(con)

    cached_datasets = {
        row[0] for row in con.execute(
            "select distinct dataset from yahoo_data where symbol = ? and ts > current_timestamp - interval 1 month",
            (symbol,)
        ).fetchall()
    }
    if not _NEEDED_DATASETS.issubset(cached_datasets):
        print(f"Fetching Yahoo data for {symbol}...")
        can_be_found = yf_ds.can_be_found(symbol)
        if not can_be_found:
            print(f"Symbol {symbol} not found on Yahoo Finance.")
            con.close()
            return
        yf_ds.prefetch(symbol)
    else:
        print(f"Using cached Yahoo data for {symbol}.")

    fcf_yield = yf_ds.fcf_yield(symbol)
    ocf_margin_result = yf_ds.ocf_margin(symbol)
    ocf_margin, min_ocf_margin = ocf_margin_result if ocf_margin_result else (None, None)
    ocf_margin_volatility = yf_ds.ocf_margin_volatility(symbol)
    has_negative_net_income, avg_net_margin = yf_ds.net_income_check(symbol)
    scaled_rp = yf_ds.scaled_rp(fcf_yield, ocf_margin, min_ocf_margin, ocf_margin_volatility)
    scaled_rp21 = yf_ds.scaled_rp_21(fcf_yield, ocf_margin, min_ocf_margin, ocf_margin_volatility, has_negative_net_income, avg_net_margin)
    sector_score = yf_ds.sector_score(symbol)
    geo_score = yf_ds.geo_score(symbol)
    debt_score = yf_ds.debt_score(symbol)
    trend_score = yf_ds.trend_score(symbol)
    vd_score = yf_ds.vd_score(symbol)

    sector = yf_ds.sector(symbol)
    industry_score = yf_ds.industry_score(symbol)

    if sector == 'Financial Services':
        scaled_rp = scaled_rp21 = debt_score = vd_score = 0
    elif sector == 'Industrials' and industry_score == -1:
        vd_score = 0

    afv_score = scaled_rp + sector_score + geo_score + debt_score + trend_score + vd_score
    afv21_score = scaled_rp21 + sector_score + geo_score + debt_score + trend_score + vd_score

    print(f"\n{'='*50}")
    print(f"  {symbol}")
    print(f"{'='*50}")
    print(f"  FCF Yield:           {fcf_yield:.4f}" if fcf_yield is not None else "  FCF Yield:           N/A")
    print(f"  OCF Margin (avg):    {ocf_margin:.4f}" if ocf_margin is not None else "  OCF Margin (avg):    N/A")
    print(f"  OCF Margin (min):    {min_ocf_margin:.4f}" if min_ocf_margin is not None else "  OCF Margin (min):    N/A")
    print(f"  OCF Margin Vol:      {ocf_margin_volatility:.4f}" if ocf_margin_volatility is not None else "  OCF Margin Vol:      N/A")
    print(f"  Neg Net Income:      {has_negative_net_income}")
    print(f"  Avg Net Margin:      {avg_net_margin:.4f}")
    print(f"  Sector:              {sector}")
    print(f"{'─'*50}")
    print(f"  RP (2.0):    {scaled_rp:+.3f}")
    print(f"  RP (2.1):    {scaled_rp21:+.3f}")
    print(f"  Sector:      {sector_score:+.3f}")
    print(f"  Geo:         {geo_score:+.3f}")
    print(f"  Debt:        {debt_score:+.3f}")
    print(f"  Trend:       {trend_score:+.3f}")
    print(f"  VD:          {vd_score:+.3f}")
    print(f"{'─'*50}")
    print(f"  AFV 2.0:     {afv_score:+.3f}")
    print(f"  AFV 2.1:     {afv21_score:+.3f}")
    print(f"{'='*50}\n")

    if save:
        con.execute("""
            insert into afv_21_scores (symbol, afv, afv21, rp, rp21, fcf_yield, ocf_margin, min_ocf_margin, ocf_margin_volatility, sector_score, geo_score, debt_score, trend_score, vd_score, computed_at)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
        """, (symbol, afv_score, afv21_score, scaled_rp, scaled_rp21, fcf_yield, ocf_margin, min_ocf_margin, ocf_margin_volatility, sector_score, geo_score, debt_score, trend_score, vd_score))
        con.commit()
        print(f"Score saved to database.")

    con.close()

    con.close()


if __name__ == "__main__":
    process()