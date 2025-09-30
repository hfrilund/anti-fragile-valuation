import random
import time

import duckdb
import pandas as pd

from finance_data_sources import yahoo

def process():
    con = duckdb.connect('../../data/finance_data.db')
    yf = yahoo.YahooFinanceDataSource(con)

    tickers = con.execute("select * from tickers").fetchdf()

    for idx, row in tickers.iterrows():
        symbol = row['yahoo_ticker']

        exists = con.execute("select count(*) as cnt from afv_20_scores where symbol = ? and computed_at > current_timestamp - interval 1 month", (symbol,)).fetchone()

        if exists and exists[0] > 0:
            print(f"AFV Score for {symbol} already computed within a month, skipping...")
            continue

        try:
            we_have_data = con.execute(
                "select count(*) as cnt from yahoo_data where symbol = ? and ts > current_timestamp - interval 1 month",
                (symbol,)).fetchone()

            if we_have_data and we_have_data[0] > 0:
                print(f"Yahoo data for {symbol} already exists within a month, skipping data fetch...")

            else:
                time.sleep(random.uniform(1, 3.5))
                can_be_found = yf.can_be_found(symbol)

                if not can_be_found:
                    print(f"Symbol {symbol} cannot be found on Yahoo Finance, skipping...")
                    raise Exception("Symbol not found")

            con.execute("BEGIN")
            fcf_yield = yf.fcf_yield(symbol)
            ocf_margin, min_ocf_margin = yf.ocf_margin(symbol)
            ocf_margin_volatility = yf.ocf_margin_volatility(symbol)
            scaled_rp = yf.scaled_rp(fcf_yield, ocf_margin, min_ocf_margin, ocf_margin_volatility)
            sector_score = yf.sector_score(symbol)
            geo_score = yf.geo_score(symbol)
            debt_score = yf.debt_score(symbol)
            trend_score = yf.trend_score(symbol)
            vd_score = yf.vd_score(symbol)

            #make adjustments
            sector = yf.sector(symbol)
            industry_score = yf.industry_score(symbol)

            if sector == 'Financial Services':
                scaled_rp = 0
                debt_score = 0
                vd_score = 0
            elif sector == 'Industrials' and industry_score == -1:
                vd_score = 0

            afv_score = scaled_rp + sector_score + geo_score + debt_score + trend_score + vd_score
            print(f"AFV Score for {symbol}: {afv_score}\n")

            con.execute("""
                insert into afv_20_scores (symbol, afv, rp, fcf_yield, ocf_margin, min_ocf_margin, ocf_margin_volatility, sector_score, geo_score, debt_score, trend_score, vd_score, computed_at)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
            """, (symbol, afv_score, scaled_rp, fcf_yield, ocf_margin, min_ocf_margin, ocf_margin_volatility, sector_score, geo_score, debt_score, trend_score, vd_score))

            con.commit()
        except Exception as e:
            print(f"Error processing {symbol}: {e}, storing AFV -1000")

            con.execute("""
                insert into afv_20_scores (symbol, afv, rp, fcf_yield, ocf_margin, min_ocf_margin, ocf_margin_volatility, sector_score, geo_score, debt_score, trend_score, vd_score, computed_at)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
            """, (symbol, -1000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

            con.commit()
    con.close()

if __name__ == "__main__":
    process()