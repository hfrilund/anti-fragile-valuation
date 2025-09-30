#!/usr/bin/env python3
import hashlib

import duckdb
import pandas as pd

def map_mic_to_exchange(mic: str) -> str:
    """Map MIC (Market Identifier Code) to exchange name"""
    exchange_mapping = {
        'XLON': 'London Stock Exchange',
        'XETR': 'XETRA',
        'XPAR': 'Euronext Paris', 
        'XAMS': 'Euronext Amsterdam',
        'XSWX': 'SIX Swiss Exchange',
        'XMIL': 'Borsa Italiana',
        'XMAD': 'BME Spanish Exchanges',
        'XSTO': 'Nasdaq Stockholm',
        'XOSL': 'Oslo Stock Exchange',
        'XCSE': 'Nasdaq Copenhagen',
        'XHEL': 'Nasdaq Helsinki',
        'XBRU': 'Euronext Brussels',
        'XLIS': 'Euronext Lisbon',
        'XWBO': 'Wiener Börse',
        'XATH': 'Athens Exchange'
    }
    
    return exchange_mapping.get(mic, f'European Exchange ({mic})')

def bloomberg_to_ticker(bbg_primary: str) -> str | None:
    return bbg_primary.strip().split(" ", 1)[0] if bbg_primary else None

def create_yahoo_symbol(raw_ticker: str, mic: str) -> str:
    """Create Yahoo Finance compatible symbol"""
    if not raw_ticker:
        return None
    
    # Clean the symbol
    symbol = str(raw_ticker).strip()
    
    # Standard Yahoo Finance suffixes for European exchanges
    yahoo_suffix_mapping = {
        'MTAA': '.MI',    # Milan (cross-listings)
        'XWAR': '.WA',     # Warsaw
        'XLON': '.L',      # London
        'XETR': '.DE',     # Germany
        'XPAR': '.PA',     # Paris
        'XAMS': '.AS',     # Amsterdam
        'XSWX': '.SW',     # Switzerland
        'XMIL': '.MI',     # Milan
        'XMAD': '.MC',     # Madrid
        'XSTO': '.ST',     # Stockholm
        'XOSL': '.OL',     # Oslo
        'XCSE': '.CO',     # Copenhagen
        'XHEL': '.HE',     # Helsinki
        'XBRU': '.BR',     # Brussels
        'XLIS': '.LS',     # Lisbon
        'XWBO': '.VI',     # Vienna
        'XATH': '.AT'      # Athens
    }
    
    yahoo_suffix = yahoo_suffix_mapping.get(mic, '')
    
    # Return symbol with appropriate suffix
    if yahoo_suffix:
        return f"{symbol}{yahoo_suffix}"
    else:
        return symbol  # Return as-is if no mapping found

def md5_hash(value: str) -> str:
    return hashlib.md5(value.encode("utf-8")).hexdigest()
def import_csv_tickers():
    """Import European equity tickers from CSV file"""
    print("🌍 IMPORTING EUROPEAN TICKERS FROM CSV")
    print("=" * 60)
    
    csv_path = "CXESymbols-PROD.csv"
    
    if not pd.io.common.file_exists(csv_path):
        print(f"❌ CSV file not found: {csv_path}")
        return

    # Read CSV file
    print("📁 Reading CSV file...")
    try:
        # Read CSV, skipping the first line (environment info)
        df = pd.read_csv(csv_path, skiprows=1)
        print(f"📊 Total rows in CSV: {len(df):,}")
        
        # Filter for equity (EQTY) only
        equity_df = df[df['asset_class'] == 'EQTY'].copy()
        print(f"📈 Equity rows (EQTY): {len(equity_df):,}")
        
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return
    
    # Process equity tickers
    print("💾 Processing equity tickers...")
    inserted_count = 0
    skipped_count = 0
    
    exchange_counts = {}
    tier_counts = {}
    
    with duckdb.connect('../../data/finance_data.db') as db:
        for index, row in equity_df.iterrows():
            try:
                # Extract key fields
                company_name = row.get('company_name', '').strip()
                raw_ticker = bloomberg_to_ticker(row.get('bloomberg_primary', '').strip())
                isin = row.get('isin', '').strip()
                mic = row.get('mic', '').strip()

                ticker_pk = md5_hash(raw_ticker + mic)
                
                # Skip if missing essential data
                if not company_name or not raw_ticker or not mic:
                    skipped_count += 1
                    continue
                
                # Create Yahoo Finance symbol
                yahoo_symbol = create_yahoo_symbol(raw_ticker, mic)

                if not yahoo_symbol:
                    skipped_count += 1
                    continue
                
                # Store in database
                db.execute(
                    """
                    insert into tickers (ticker_pk, isin, mic, raw_ticker, yahoo_ticker, asset_name)
                    values (?, ?, ?, ?, ?, ?)
                    on conflict (ticker_pk) do nothing
                    """,
                    (ticker_pk, isin, mic, raw_ticker, yahoo_symbol, company_name)
                )
                
                inserted_count += 1
                
                # Progress update
                if inserted_count % 1000 == 0:
                    print(f"  📊 Inserted {inserted_count:,} tickers...")
                    
            except Exception as e:
                skipped_count += 1
                continue
    
    # Final statistics
    print(f"\n🎯 CSV IMPORT COMPLETE")
    print(f"=" * 40)
    print(f"✅ Successfully imported: {inserted_count:,} equity tickers")
    print(f"⚠️  Skipped: {skipped_count:,} tickers")
    print(f"📊 Total in database: {inserted_count:,}")
    
    return inserted_count

if __name__ == "__main__":
    result = import_csv_tickers()
    print(f"\n🌍 CSV ticker import completed: {result:,} European equity tickers imported")