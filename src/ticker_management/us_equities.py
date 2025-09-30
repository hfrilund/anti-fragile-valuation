#!/usr/bin/env python3
import hashlib

import duckdb
import pandas as pd

def md5_hash(value: str) -> str:
    return hashlib.md5(value.encode("utf-8")).hexdigest()

def read_files():
    # read amex_tickers.json
    amex_df = pd.read_json('amex_tickers.json')
    amex_df = amex_df.rename(columns={0: "ticker"})
    amex_df['mic'] = 'XASE'

    nyse_df = pd.read_json('nyse_tickers.json')
    nyse_df = nyse_df.rename(columns={0: "ticker"})
    nyse_df['mic'] = 'XNYS'

    nasdaq_df = pd.read_json('nasdaq_tickers.json')
    nasdaq_df = nasdaq_df.rename(columns={0: "ticker"})
    nasdaq_df['mic'] = 'XNMS'

    combined = pd.concat([amex_df, nyse_df, nasdaq_df], ignore_index=True)
    cleaned = combined[combined["ticker"].notna() & (combined["ticker"].str.strip() != "")]

    skipped_count = 0
    inserted_count = 0

    with duckdb.connect('../../data/finance_data.db') as db:
        for index, row in cleaned.iterrows():
            try:
                raw_ticker = row.get('ticker', '').strip()
                mic = row.get('mic', '').strip()
                ticker_pk = md5_hash(raw_ticker + mic)

                # Skip if missing essential data
                if not  raw_ticker or not mic:
                    skipped_count += 1
                    continue

                db.execute(
                    """
                    insert into tickers (ticker_pk, isin, mic, raw_ticker, yahoo_ticker, asset_name)
                    values (?, ?, ?, ?, ?, ?)
                    on conflict (ticker_pk) do nothing
                    """,
                    (ticker_pk, '', mic, raw_ticker, raw_ticker, '')
                )

                inserted_count += 1

                # Progress update
                if inserted_count % 1000 == 0:
                    print(f"  📊 Inserted {inserted_count:,} tickers...")

            except Exception as e:
                skipped_count += 1
                continue

if __name__ == "__main__":
    read_files()