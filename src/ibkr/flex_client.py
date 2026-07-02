"""
IBKR Flex Web Service client.

Fetches a Flex Query XML report programmatically using a token and query ID.
Set up the query in IBKR Account Management → Reports → Flex Queries.

Required credentials (env vars or explicit args):
  IBKR_FLEX_TOKEN     – Flex Web Service token (Account Management → Settings)
  IBKR_FLEX_QUERY_ID  – the numeric Flex Query ID to run
"""
import os
import time
import xml.etree.ElementTree as ET
from typing import Optional

import requests

_REQUEST_URL = "https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.SendRequest"
_GET_URL     = "https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.GetStatement"

_MAX_POLLS        = 10
_POLL_SECONDS     = 5
_MAX_SEND_RETRIES = 3
_SEND_RETRY_WAIT  = 20  # seconds between SendRequest retries


def fetch_flex_xml(
    token: Optional[str] = None,
    query_id: Optional[str] = None,
) -> str:
    """
    Request and return the raw XML string for a Flex Query report.

    Falls back to IBKR_FLEX_TOKEN / IBKR_FLEX_QUERY_ID env vars when
    token / query_id are not passed explicitly.
    """
    token    = token    or os.environ.get("IBKR_FLEX_TOKEN")
    query_id = query_id or os.environ.get("IBKR_FLEX_QUERY_ID")

    if not token:
        raise ValueError(
            "IBKR Flex token is required. "
            "Set the IBKR_FLEX_TOKEN env var or pass --token."
        )
    if not query_id:
        raise ValueError(
            "IBKR Flex query ID is required. "
            "Set the IBKR_FLEX_QUERY_ID env var or pass --query-id."
        )

    for attempt in range(1, _MAX_SEND_RETRIES + 1):
        try:
            ref_code, get_url = _send_request(token, query_id)
            return _poll_until_ready(token, ref_code, get_url)
        except RuntimeError as e:
            if "could not be generated" in str(e).lower() and attempt < _MAX_SEND_RETRIES:
                print(f"  IBKR not ready, retrying in {_SEND_RETRY_WAIT}s "
                      f"(attempt {attempt}/{_MAX_SEND_RETRIES})…")
                time.sleep(_SEND_RETRY_WAIT)
            else:
                raise


def _send_request(token: str, query_id: str) -> tuple[str, str]:
    resp = requests.get(
        _REQUEST_URL,
        params={"t": token, "q": query_id, "v": "3"},
        timeout=30,
    )
    resp.raise_for_status()

    root = ET.fromstring(resp.text)
    status = root.findtext("Status")
    if status != "Success":
        error = root.findtext("ErrorMessage") or resp.text
        raise RuntimeError(f"Flex request rejected: {error}")

    ref_code = root.findtext("ReferenceCode")
    if not ref_code:
        raise RuntimeError(f"No ReferenceCode in Flex response: {resp.text}")

    # IBKR returns the poll URL in the response; fall back to the known default
    get_url = root.findtext("Url") or _GET_URL
    return ref_code, get_url


def _poll_until_ready(token: str, ref_code: str, get_url: str) -> str:
    for attempt in range(1, _MAX_POLLS + 1):
        resp = requests.get(
            get_url,
            params={"q": ref_code, "t": token, "v": "3"},
            timeout=60,
        )
        resp.raise_for_status()

        # <FlexQueryResponse> is the root element of a completed statement.
        # <FlexStatementResponse> is used for status/error responses — do not match it.
        if "<FlexQueryResponse" in resp.text:
            return resp.text

        try:
            root = ET.fromstring(resp.text)
            status = root.findtext("Status")
            error  = root.findtext("ErrorMessage")
        except ET.ParseError:
            status = error = None

        if status == "Processing":
            print(f"  Report generating… (attempt {attempt}/{_MAX_POLLS})")
            time.sleep(_POLL_SECONDS)
            continue

        raise RuntimeError(f"Flex polling failed: {error or resp.text}")

    raise TimeoutError(
        f"Flex report not ready after {_MAX_POLLS} attempts "
        f"({_MAX_POLLS * _POLL_SECONDS}s). Try again later."
    )
