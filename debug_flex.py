#!/usr/bin/env python3
"""One-shot diagnostic: prints the raw IBKR Flex SendRequest response."""
import os, sys
from pathlib import Path
import requests
import xml.etree.ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parent / 'src'))

token    = os.environ.get("IBKR_FLEX_TOKEN")
query_id = os.environ.get("IBKR_FLEX_QUERY_ID")

print(f"Token    : {token}")
print(f"Query ID : {query_id}")
print()

url = "https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.SendRequest"
resp = requests.get(url, params={"t": token, "q": query_id, "v": "3"}, timeout=30)

print(f"HTTP status : {resp.status_code}")
print(f"Raw response:\n{resp.text}")
print()

try:
    root = ET.fromstring(resp.text)
    print(f"Status     : {root.findtext('Status')}")
    print(f"ErrorCode  : {root.findtext('ErrorCode')}")
    print(f"ErrorMessage: {root.findtext('ErrorMessage')}")
    print(f"ReferenceCode: {root.findtext('ReferenceCode')}")
except Exception as e:
    print(f"XML parse error: {e}")
