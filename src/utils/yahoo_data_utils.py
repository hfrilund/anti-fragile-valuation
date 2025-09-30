#!/usr/bin/env python3
import hashlib
import json
from io import StringIO

import duckdb
import pandas as pd

def list_all_industries() -> list[str]:
    """List all distinct industries from the stock_info table"""
    with duckdb.connect('../../data/finance_data.db') as db:
        result = db.execute("SELECT data  FROM yahoo_data WHERE dataset = 'info'").fetchall()

        industries = set()
        for row in result:
            json_data = row[0]
            df = pd.read_json(StringIO(json_data))
            if "industry" in df.columns:
                industries.update(df["industry"].dropna().unique().tolist())

        return sorted(industries)

def list_all_info_fields() -> list[str]:
    """List all distinct fields available in the stock_info table"""
    with duckdb.connect('../../data/finance_data.db') as db:
        result = db.execute("SELECT data  FROM yahoo_data WHERE dataset = 'info'").fetchall()

        fields = set()
        for row in result:
            json_data = row[0]
            if json_data:
                parsed = json.loads(json_data)  # <--- parse as dict
                fields.update(parsed.keys())

        return sorted(fields)

if __name__ == "__main__":
    print("\nInfo Fields:")
    fields = list_all_info_fields()
    for field in fields:
        print(f" - {field}")

