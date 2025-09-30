import random
import time

import yfinance as yf
import numpy as np
import pandas as pd
from io import StringIO

class YahooFinanceDataSource:
    def __init__(self, con):
        self.active_ticker = None
        self.con = con

    def _get_ticker(self, symbol: str):
        if self.active_ticker is None or self.active_ticker.ticker != symbol:
            self.active_ticker = yf.Ticker(symbol)
        return self.active_ticker

    def _get_history(self, symbol: str):
        data = self._get_yahoo_data(symbol, 'history')

        if data is None or data.empty:
            ticker = self._get_ticker(symbol)
            time.sleep(random.uniform(1, 3.5))
            cashflow = ticker.cashflow
            self._store_yahoo_data(symbol, 'history', cashflow)
            return cashflow

        return data
    def _get_cashflow(self, symbol: str):
        data = self._get_yahoo_data(symbol, 'cashflow')

        if data is None or data.empty:
            ticker = self._get_ticker(symbol)
            time.sleep(random.uniform(1, 3.5))
            cashflow = ticker.cashflow
            self._store_yahoo_data(symbol, 'cashflow', cashflow)
            return cashflow

        return data

    def _get_quarterly_cashflow(self, symbol: str):
        data = self._get_yahoo_data(symbol, 'quarterly_cashflow')

        if data is None or data.empty:
            ticker = self._get_ticker(symbol)
            time.sleep(random.uniform(1, 3.5))
            cashflow = ticker.quarterly_cashflow
            self._store_yahoo_data(symbol, 'quarterly_cashflow', cashflow)
            return cashflow

        return data

    def _get_info(self, symbol: str):
        data = self._get_yahoo_data(symbol, 'info')

        if data is None or data.empty:
            ticker = self._get_ticker(symbol)
            time.sleep(random.uniform(1, 3.5))
            info = pd.DataFrame([ticker.info])
            self._store_yahoo_data(symbol, 'info', info)
            return info.iloc[0].to_dict()

        return data.iloc[0].to_dict()

    def _get_financials(self, symbol: str):
        data = self._get_yahoo_data(symbol, 'financials')

        if data is None or data.empty:
            ticker = self._get_ticker(symbol)
            time.sleep(random.uniform(1, 3.5))
            income = ticker.financials
            self._store_yahoo_data(symbol, 'financials', income)
            return income

        return data
    def _get_balance_sheet(self, symbol: str):
        data = self._get_yahoo_data(symbol, 'balance_sheet')

        if data is None or data.empty:
            ticker = self._get_ticker(symbol)
            time.sleep(random.uniform(1, 3.5))
            bs = ticker.balance_sheet
            self._store_yahoo_data(symbol, 'balance_sheet', bs)
            return bs

        return data

    def _store_yahoo_data(self, symbol: str, dataset: str, data: pd.DataFrame):
        json_str = data.to_json()

        self.con.execute(
            "insert into yahoo_data (symbol, dataset, data, ts) values (?, ?, ?, ?)",
            (symbol, dataset, json_str, pd.Timestamp.now())
        )
    def _get_yahoo_data(self, symbol: str, dataset: str):
        # Check if we have cached data
        result = self.con.execute(
            "select data from yahoo_data where symbol = ? and dataset = ? and ts + interval 1 month > current_date order by ts desc limit 1",
            (symbol, dataset,)
        ).fetchone()

        if result:
            json_data = result[0]
            df = pd.read_json(StringIO(json_data))

            return df

    def normalize_to_eur(self, value: float, currency: str) -> float:
        if currency == "EUR":
            return value

        fx_pair = f"{currency}EUR=X"  # e.g. "ZAREUR=X"
        fx_rate = self._get_ticker(fx_pair).history(period="1d")["Close"].iloc[-1]

        return value * fx_rate

    def can_be_found(self, symbol: str) -> bool:
        ticker = yf.Ticker(symbol)
        fi = ticker.fast_info  # very lightweight
        return fi is not None and "lastPrice" in fi and fi["lastPrice"] is not None

    def fcf_yield(self, symbol: str) -> float | None:
        """Fetch Operating Cash Flow (TTM) from Yahoo Finance"""
        try:

            cashflow = self._get_cashflow(symbol)

            if cashflow.empty or cashflow.shape[1] == 0:
                print(f"No cashflow data for {symbol}")
                return None

            ocf_series = cashflow.loc['Operating Cash Flow'].dropna().iloc[:4]

            if ocf_series.empty:
                ocf_mean = None  # Or raise an exception or skip this stock
            else:
                ocf_mean = ocf_series.mean()

            capex_series = cashflow.loc['Capital Expenditure'].dropna().iloc[:4]

            if capex_series.empty:
                return None

            capex_mean = capex_series.mean()

            fcf_mean = ocf_mean + capex_mean  # CapEx is negative in cash flow statement

            info = self._get_info(symbol)
            financial_currency = info.get("financialCurrency", None)
            market_currency = info.get("currency", None)
            market_cap = info.get("marketCap", None)

            fcf_mean_eur = self.normalize_to_eur(fcf_mean, financial_currency)
            market_cap_eur = self.normalize_to_eur(market_cap, market_currency)

            fcf_yield = fcf_mean_eur / market_cap_eur

            return fcf_yield
        except Exception as e:
            print(f"Error fetching OCF for {symbol}: {e}")
            return None

    def ocf_margin(self, symbol: str) -> tuple[float, float] | None:
        try:
            cashflow = self._get_cashflow(symbol)
            financials = self._get_financials(symbol)  # for revenue

            ocf_series = cashflow.loc['Operating Cash Flow'].dropna().iloc[:4]
            rev_series = financials.loc['Total Revenue'].dropna().iloc[:4]

            if len(ocf_series) == 0 or len(rev_series) == 0:
                print(f"No OCF/revenue values for {symbol}, skipping OCF margin calculation")
                return None

            financial_currency = self._get_info(symbol).get("financialCurrency", None)

            ocf_margins = []
            for ocf, rev in zip(ocf_series, rev_series):
                if rev and rev != 0:
                    ocf_eur = self.normalize_to_eur(ocf, financial_currency)
                    rev_eur = self.normalize_to_eur(rev, financial_currency)
                    ocf_margins.append(ocf_eur / rev_eur)

            if not ocf_margins:
                return None

            avg_margin = np.mean(ocf_margins)
            min_margin = np.min(ocf_margins)

            # Cap margins to avoid distortion
            avg_margin = max(min(avg_margin, 2), -2)
            min_margin = max(min(min_margin, 2), -2)

            return avg_margin, min_margin

        except Exception as e:
            print(f"Error calculating OCF margin for {symbol}: {e}")
            return None

    def ocf_margin_volatility(self, symbol: str) -> float:
        try:
            cashflow = self._get_cashflow(symbol)  # annual cashflow
            financials = self._get_financials(symbol)  # annual income statement

            ocf_series = cashflow.loc['Operating Cash Flow'].dropna()
            rev_series = financials.loc['Total Revenue'].dropna()

            # Ensure alignment on years
            common_idx = ocf_series.index.intersection(rev_series.index)
            if len(common_idx) < 3:  # need at least 3 years for volatility
                return None

            ocf_series = ocf_series[common_idx]
            rev_series = rev_series[common_idx]

            # Build yearly OCF margins
            ocf_margins = ocf_series / rev_series
            ocf_margins = ocf_margins.dropna()

            if len(ocf_margins) < 3:
                return None

            mean_margin = ocf_margins.mean()
            stdev_margin = ocf_margins.std()

            if mean_margin == 0:
                return None

            # Coefficient of Variation (normalized volatility)
            cv = stdev_margin / abs(mean_margin)

            return cv

        except Exception as e:
            print(f"Error calculating OCF margin volatility for {symbol}: {e}")
            return None
    def scaled_rp(self, fcf_yield: float, ocf_margin: float, min_ocf_margin: float, ocf_margin_volatility: float) -> float:
        if fcf_yield <= 0:
            score = -2.0
        else:
            # logistic squash: caps around +5
            score = 7 / (1 + np.exp(-30 * (fcf_yield - 0.15))) - 2

         # Margin adjustment
        if ocf_margin is not None:
            if ocf_margin < 0:
                score = -2.0  # override if negative OCF
            elif ocf_margin < 0.05:
                score *= 0.7
            elif ocf_margin > 0.20:
                score *= 1.2

        # --- Stress test with min margin ---
        if min_ocf_margin is not None:
            if min_ocf_margin < 0:
                score = -2.0  # any negative margin year = fragile
            elif ocf_margin and min_ocf_margin < 0.5 * ocf_margin:
                score *= 0.7  # recent deterioration

        # margin volatility adjustment
        if ocf_margin_volatility is not None:
            if ocf_margin_volatility > 1.0:
                score = -2  # for extreme volatility
            elif ocf_margin_volatility > 0.3:
                score *= 0.5  # strong penalty
            elif ocf_margin_volatility > 0.15:
                score *= 0.8  # mild penalty
            elif ocf_margin_volatility < 0.05:
                score *= 1.1  # small bonus

        # Final clamp
        return max(min(score, 5), -3)

    def sector(self, symbol: str) -> str:
        info = self._get_info(symbol)
        return info.get("sector", None)

    def industry_score(self, symbol: str) -> float:
        industry_scores = {
            # ✅ Anti-fragile, essential, or resource-linked
            "Agricultural Inputs": 1,
            "Farm Products": 1,
            "Food Distribution": 1,
            "Grocery Stores": 1,
            "Packaged Foods": 1,
            "Beverages - Brewers": 1,
            "Beverages - Non-Alcoholic": 1,
            "Beverages - Wineries & Distilleries": 0,  # somewhat cyclical, not core
            "Oil & Gas Drilling": 1,
            "Oil & Gas E&P": 1,
            "Oil & Gas Equipment & Services": 1,
            "Oil & Gas Integrated": 1,
            "Oil & Gas Midstream": 1,
            "Oil & Gas Refining & Marketing": 1,
            "Utilities - Diversified": 1,
            "Utilities - Independent Power Producers": 1,
            "Utilities - Regulated Electric": 1,
            "Utilities - Regulated Gas": 1,
            "Utilities - Regulated Water": 1,
            "Utilities - Renewable": 1,
            "Waste Management": 1,
            "Marine Shipping": 1,
            "Railroads": 1,
            "Integrated Freight & Logistics": 1,
            "Farm & Heavy Construction Machinery": 1,
            "Building Materials": 1,
            "Building Products & Equipment": 1,
            "Specialty Industrial Machinery": 1,
            "Electrical Equipment & Parts": 1,
            "Metal Fabrication": 1,
            "Steel": 1,
            "Copper": 1,
            "Aluminum": 1,
            "Gold": 1,
            "Other Industrial Metals & Mining": 1,
            "Other Precious Metals & Mining": 1,
            "Pollution & Treatment Controls": 1,
            "Tools & Accessories": 1,
            "Security & Protection Services": 1,
            "Aerospace & Defense": 1,

            # ⚪ Neutral, mixed resilience
            "Chemicals": 0,
            "Specialty Chemicals": 0,
            "Packaging & Containers": 0,
            "Paper & Paper Products": 0,
            "Lumber & Wood Production": 0,
            "Scientific & Technical Instruments": 0,
            "Business Equipment & Supplies": 0,
            "Industrial Distribution": 0,
            "Rental & Leasing Services": 0,
            "Specialty Business Services": 0,
            "Staffing & Employment Services": 0,
            "Consulting Services": 0,
            "Conglomerates": 0,
            "Education & Training Services": 0,
            "Textile Manufacturing": 0,
            "Personal Services": 0,
            "Home Improvement Retail": 0,
            "Furnishings, Fixtures & Appliances": 0,
            "Restaurants": 0,
            "Travel Services": 0,
            "Leisure": 0,
            "Resorts & Casinos": 0,
            "Lodging": 0,

            # ❌ Fragile / bubble-prone / consumer cyclical
            "Apparel Manufacturing": -1,
            "Apparel Retail": -1,
            "Luxury Goods": -1,
            "Footwear & Accessories": -1,
            "Auto & Truck Dealerships": -1,
            "Auto Manufacturers": -1,
            "Auto Parts": -1,
            "Recreational Vehicles": -1,
            "Advertising Agencies": -1,
            "Electronic Gaming & Multimedia": -1,
            "Entertainment": -1,
            "Publishing": -1,
            "Internet Content & Information": -1,
            "Internet Retail": -1,
            "Gambling": -1,

            # ❌ Speculative tech
            "Semiconductors": -1,
            "Semiconductor Equipment & Materials": -1,
            "Computer Hardware": -1,
            "Electronic Components": -1,
            "Electronics & Computer Distribution": -1,
            "Consumer Electronics": -1,
            "Software - Application": -1,
            "Software - Infrastructure": -1,
            "Information Technology Services": -1,

            # ❌ Fragile transport (highly cyclical, fuel sensitive)
            "Airlines": -1,
            "Airports & Air Services": -1,

            # ❓ Financials — neutral/fragile depending on philosophy
            "Banks - Diversified": 0,
            "Banks - Regional": 0,
            "Asset Management": 0,
            "Capital Markets": 0,
            "Credit Services": 0,
            "Financial Data & Stock Exchanges": 0,
            "Insurance - Diversified": 0,
            "Insurance - Life": 0,
            "Insurance - Property & Casualty": 0,
            "Insurance - Reinsurance": 0,
            "Insurance - Specialty": 0,
            "Mortgage Finance": 0,
            "Real Estate Services": 0,
            "Real Estate - Development": 0,
            "Real Estate - Diversified": 0,
            "REIT - Diversified": 0,
            "REIT - Healthcare Facilities": 0,
            "REIT - Hotel & Motel": 0,
            "REIT - Industrial": 0,
            "REIT - Office": 0,
            "REIT - Residential": 0,
            "REIT - Retail": 0,

            # ❓ Healthcare — neutral (not truly anti-fragile, not speculative like biotech)
            "Biotechnology": -1,
            "Diagnostics & Research": 0,
            "Drug Manufacturers - General": 0,
            "Drug Manufacturers - Specialty & Generic": 0,
            "Health Information Services": 0,
            "Healthcare Plans": 0,
            "Medical Care Facilities": 0,
            "Medical Devices": 0,
            "Medical Distribution": 0,
            "Medical Instruments & Supplies": 0,
            "Pharmaceutical Retailers": 0,

            # ❓ Telecom
            "Telecom Services": 0,

            # ❓ Tobacco (declining but still cash cows)
            "Tobacco": 0,
        }

        info = self._get_info(symbol)
        industry = info.get("industry", None)
        print(f"Industry for {symbol}: {industry}")

        if not industry:
            return 0  # Neutral if unknown

        return industry_scores.get(industry, 0)  # Default to 0 if industry is unmapped

    def sector_score(self, symbol: str) -> float:
        SECTOR_SCORE_MAP = {
            "Utilities": 1,
            "Energy": 1,
            "Industrials": 1,
            "Healthcare": 1,
            "Consumer Defensive": 0.5,
            "Basic Materials": 0.5,
            "Technology": 0,
            "Communication Services": 0,
            "Consumer Cyclical": -0.5,
            "Real Estate": -1,
            "Financial Services": -1
        }

        try:
            info = self._get_info(symbol)
            sector = info.get("sector", None)
            industry_score = self.industry_score(symbol)
            print(f"Sector for {symbol}: {sector}, Industry score: {industry_score}")

            if not sector:
                return 0  # Neutral if unknown

            if sector == "Industrials":
                return industry_score

            return SECTOR_SCORE_MAP.get(sector, 0)  # Default to 0 if sector is unmappe
        except Exception as e:
            print(f"Error fetching sector for {symbol}: {e}")
            return None

    def geo_score(self, symbol: str) -> float | None:
        try:

            info = self._get_info(symbol)
            country = info.get("country", None)
            print(f"Country for {symbol}: {country}")

            if not country:
                return 0  # Neutral if unknown

            geo_scores = {
                'Germany': 1.0,
                'France': 1.0,
                'Finland': 1.0,
                'Sweden': 1.0,
                'Netherlands': 1.0,
                'United Kingdom': 1.0,
                'Switzerland': 1.0,
                'Norway': 1.0,
                'United States': 0.8,
                'Canada': 0.8,
                'Japan': 0.8,
                'Australia': 0.7,
                'Hong Kong': 0.6,
                'Singapore': 0.6,
                'South Korea': 0.6,
                'India': 0.4,
                'Brazil': 0.4,
                'China': 0.3,
                'Russia': -1,
                'Turkey': -1,
                'South Africa': -0.5,
            }

            return geo_scores.get(country, 0.6)  # Default: neutral score
        except Exception as e:
            print(f"Error fetching country for {symbol}: {e}")
            return None

    def _geo_score_from_isin(self, isin: str) -> float:
        """
        Returns a geography score based on the ISIN country code.
        """
        country_code = isin[:2].upper()

        geo_scores = {
            'DE': 1.0,  # Germany
            'FR': 1.0,  # France
            'FI': 1.0,  # Finland
            'SE': 1.0,  # Sweden
            'NL': 1.0,  # Netherlands
            'GB': 1.0,  # United Kingdom
            'CH': 1.0,  # Switzerland
            'NO': 1.0,  # Norway
            'US': 0.8,  # United States
            'CA': 0.8,  # Canada
            'JP': 0.8,  # Japan
            'AU': 0.7,  # Australia
            'HK': 0.6,  # Hong Kong
            'SG': 0.6,  # Singapore
            'KR': 0.6,  # South Korea
            'IN': 0.4,  # India
            'BR': 0.4,  # Brazil
            'CN': 0.3,  # China
            'RU': 0.2,  # Russia
            'TR': 0.2,  # Turkey
        }

        return geo_scores.get(country_code, 0.6)  # Default: neutral score

    def debt_score_old(self, symbol: str) -> float:
        try:
            bs = self._get_balance_sheet(symbol)

            # Ensure we have enough history (most recent columns are on the left)
            if bs.empty or bs.shape[1] == 0:
                return None

            # Use most recent column
            bs_latest = bs.iloc[:, 0]

            # Get core metrics (use .get for safety)
            total_debt = bs_latest.get("Total Debt", 0)
            cash = bs_latest.get("Cash And Cash Equivalents", 0)
            equity = bs_latest.get("Stockholders Equity", 0)
            total_assets = bs_latest.get("Total Assets", 1)  # Avoid division by zero
            long_term_receivables = bs_latest.get("Other Long Term Assets", 0)  # proxy

            # print(f"Debt data for {symbol}: Total Debt={total_debt}, Cash={cash}, Equity={equity}, Total Assets={total_assets}, Long Term Receivables={long_term_receivables}")

            # Net debt to equity
            net_debt = total_debt - cash
            if equity == 0:
                net_de_ratio = float('inf')
            else:
                net_de_ratio = net_debt / equity

            print(f"Net Debt to Equity Ratio for {symbol}: {net_de_ratio}")

            # Heuristic: Is this company finance-heavy?
            finance_ratio = long_term_receivables / total_assets
            finance_heavy = finance_ratio > 0.15

            if equity <= 0:
                score = -2.0  # structurally fragile balance sheet
            elif net_de_ratio < 0:
                score = +1.0  # net cash
            elif net_de_ratio < 0.5:
                score = +0.5
            elif net_de_ratio < 1.5:
                score = 0
            elif net_de_ratio < 3.0:
                score = -0.5
            else:
                score = -1.5

            # Adjustment if finance-heavy
            if finance_heavy and score < 0:
                score += 0.5  # soften penalty

            return round(score, 2)

        except Exception as e:
            print(f"Error fetching debt data for {symbol}: {e}, returning 0 ")
            return 0

    def debt_score(self, symbol: str) -> float:
        try:
            bs = self._get_balance_sheet(symbol)
            if bs.empty or bs.shape[1] == 0:
                return None
            bs_latest = bs.iloc[:, 0]

            total_debt = bs_latest.get("Total Debt", 0)
            cash = bs_latest.get("Cash And Cash Equivalents", 0)
            equity = bs_latest.get("Stockholders Equity", 0)
            total_assets = bs_latest.get("Total Assets", 1)

            net_debt = total_debt - cash

            # fallback: use OCF if available
            ocf = bs_latest.get("Operating Cash Flow", None)
            leverage_cashflow = None
            if ocf and ocf > 0:
                leverage_cashflow = net_debt / ocf

            # Case 1: equity is healthy → use equity ratio
            if equity > 0.1 * total_assets:
                net_de_ratio = net_debt / equity
                if net_de_ratio < 0:
                    return +1.0
                elif net_de_ratio < 0.5:
                    return +0.5
                elif net_de_ratio < 1.5:
                    return 0
                elif net_de_ratio < 3.0:
                    return -0.5
                else:
                    return -1.5

            # Case 2: equity tiny/negative → fallback to cashflow leverage
            elif leverage_cashflow is not None:
                if leverage_cashflow < 1:
                    return +1.0
                elif leverage_cashflow < 2:
                    return +0.5
                elif leverage_cashflow < 3:
                    return 0
                elif leverage_cashflow < 4:
                    return -0.5
                else:
                    return -1.5

            # Case 3: no usable data
            else:
                return 0

        except Exception as e:
            print(f"Error fetching debt data for {symbol}: {e}, returning 0")
            return 0

    def trend_score(self, symbol: str) -> float:
        try:
            cashflow = self._get_cashflow(symbol)
            financials = self._get_financials(symbol)  # for revenue

            ocf_series = cashflow.loc['Operating Cash Flow']
            ocf_values = ocf_series.iloc[:4].dropna()

            if len(ocf_values) < 4:
                print(f"Not enough data for trend score for {symbol}, returning 0")
                return 0.0

            ocf_list = ocf_values[::-1].tolist()  # oldest → newest

            # --- ROC calculation ---
            rocs = [
                (ocf_list[i + 1] - ocf_list[i]) / abs(ocf_list[i])
                if ocf_list[i] != 0 else 0
                for i in range(3)
            ]

            weights = [0.1, 0.3, 0.6]  # emphasize recent periods
            weighted_roc = sum(w * r for w, r in zip(weights, rocs))

            if weighted_roc >= 0.5:
                base_score = +1.0
            elif weighted_roc >= 0.2:
                base_score = +0.5
            elif weighted_roc >= -0.2:
                base_score = 0.0
            elif weighted_roc >= -0.5:
                base_score = -0.5
            else:
                base_score = -1.0

            # --- OCF margin adjustment ---
            try:
                rev_series = financials.loc['Total Revenue']
                rev_values = rev_series.iloc[:4].dropna()
                avg_rev = rev_values.mean()
                avg_ocf = sum(ocf_list) / len(ocf_list)

                ocf_margin = avg_ocf / avg_rev if avg_rev > 0 else None
            except Exception:
                ocf_margin = None

            if ocf_margin is not None:
                if ocf_margin > 0.10:
                    return base_score  # strong margin, keep ROC score
                elif ocf_margin > 0:  # weakly positive
                    return max(base_score - 0.5, -1.0)
                else:  # negative margin
                    return max(base_score - 1.0, -1.0)
            else:
                return base_score

        except Exception as e:
            print(f"Error in trend_score for {symbol}: {e}")
            return 0.0

    def vd_score(self, symbol: str) -> float:
        info = self._get_info(symbol)
        pe = info.get("trailingPE", None)
        dividend_yield = info.get("dividendYield", 0) or 0.0  # None-safe

        # Default = no penalty
        score = 0.0

        if pe is None or pe <= 0:
            # No earnings or invalid P/E → cautious penalty
            score = -0.5
        elif pe > 50:
            score = -1.0
        elif pe > 20:
            if dividend_yield < 1.0:  # <1% dividend yield
                score = -1.0
            else:
                score = -0.5
        else:
            if dividend_yield >= 3.0:
                score = 0.5

        return score

if __name__ == "__main__":
    ticker = 'BEDU'
    y = YahooFinanceDataSource()
    fcf = y.fcf_yield(ticker)
    ocf_margin, min_ofc_margin = y.ocf_margin(ticker)
    ocf_margin_volatility = y.ocf_margin_volatility(ticker)
    rp = y.scaled_rp(fcf, ocf_margin, min_ofc_margin, ocf_margin_volatility)
    sector = y.sector_score(ticker)
    geo = y.geo_score(ticker)
    trend = y.trend_score(ticker)
    debt = y.debt_score(ticker)
    vd = y.vd_score(ticker)

    print(f"FCF Yield: {fcf}, OCF Margin: {ocf_margin}, Scaled RP: {rp}, Sector: {sector}, Geo: {geo}, Trend: {trend}, Debt: {debt}, VD: {vd}, OCF Margin Volatility: {ocf_margin_volatility}, Min OCF Margin: {min_ofc_margin}")

    afv20 = rp + sector + geo + trend + debt + vd
    scaled_afv = max(0, min(10, (afv20 + 2) * 2))
    print(f"AFV-20: {afv20}")
    print(f"Scaled AFV-20: {scaled_afv}")
    #ticker = yf.Ticker("AAPL")
    #bs = ticker.balance_sheet
    #print(bs.index)
