import random
import time

import duckdb

from finance_data_sources import yahoo

def run_basic_afv_scoring_tests():
    con = duckdb.connect('../../data/finance_data.db')
    yf = yahoo.YahooFinanceDataSource(con)

    test_symbols = [
        'AAPL',  # Apple Inc.
        'MSFT',  # Microsoft Corporation
        'GOOGL', # Alphabet Inc.
        'AMZN',  # Amazon.com, Inc.
        'TSLA',  # Tesla, Inc.
        'BRK-B', # Berkshire Hathaway Inc.
        'JPM',   # JPMorgan Chase & Co.
        'JNJ',   # Johnson & Johnson
        'V',     # Visa Inc.
        'WMT',    # Walmart Inc.
        'CAT',
        '0QF.DE',
        'NVDA',
        'NOVO-B.CO',
        'AM.PA',
        'LHA.DE',
        'CALT.MI',
        'CKN.L',
        'CNE.L',
        'XOM',
        'MO',
        'LGPS',
        'GEMD.L',
        'BEDU'
    ]

    for symbol in test_symbols:
        try:
            print(f"Processing AFV score for {symbol}...")

            time.sleep(random.uniform(1, 3.5))
            can_be_found = yf.can_be_found(symbol)

            if not can_be_found:
                print(f"Symbol {symbol} cannot be found on Yahoo Finance, skipping...")
                continue

            fcf_yield = yf.fcf_yield(symbol)
            ocf_margin, min_ocf_margin = yf.ocf_margin(symbol)
            ocf_margin_volatility = yf.ocf_margin_volatility(symbol)
            scaled_rp = yf.scaled_rp(fcf_yield, ocf_margin, min_ocf_margin, ocf_margin_volatility)
            sector_score = yf.sector_score(symbol)
            geo_score = yf.geo_score(symbol)
            debt_score = yf.debt_score(symbol)
            trend_score = yf.trend_score(symbol)
            vd_score = yf.vd_score(symbol)

            print(f"AFV Score components for {symbol}:")
            print(f"  FCF Yield: {fcf_yield}")
            print(f"  OCF Margin: {ocf_margin}")
            print(f"  Min OCF Margin: {min_ocf_margin}")
            print(f"  OCF Margin Volatility: {ocf_margin_volatility}")
            print(f"  Scaled RP: {scaled_rp}")
            print(f"  Sector Score: {sector_score}")
            print(f"  Geo Score: {geo_score}")
            print(f"  Debt Score: {debt_score}")
            print(f"  Trend Score: {trend_score}")
            print(f"  VD Score: {vd_score}")

            # make adjustments
            sector = yf.sector(symbol)
            industry_score = yf.industry_score(symbol)

            if sector == 'Financial Services':
                scaled_rp = 0
                debt_score = 0
                vd_score = 0
            elif sector == 'Industrials' and industry_score == -1:
                vd_score = 0

            print(f"Adjusted Scores for {symbol}: , Sector: {sector}, Industry Score: {industry_score}, Scaled RP: {scaled_rp}, Debt Score: {debt_score}, VD Score: {vd_score}, Trend Score: {trend_score}, Geo Score: {geo_score}, Sector Score: {sector_score}, ocf_margin_volatility: {ocf_margin_volatility}")

            afv_score = scaled_rp + sector_score + geo_score + debt_score + trend_score + vd_score
            scaled_afv = max(0, min(10, (afv_score + 2) * 2))

            print(f"AFV Score for {symbol}: {afv_score}\n")
            print(f"Scaled AFV Score for {symbol}: {scaled_afv}\n")

        except Exception as e:
            print(f"Error processing {symbol}: {e}")
            continue
    con.close()

if __name__ == "__main__":
    run_basic_afv_scoring_tests()