
# Overview

The Antifragile Value (AFV) scoring system evolves from AFV 2.1, building on its proprietary, rules-based metric inspired by Nassim Nicholas Taleb's antifragility principles. AFV 2.2 aims to enhance robustness by incorporating new components and refinements to better address emerging market nuances, governance risks, and valuation contexts. This version prioritizes deeper risk subtraction (e.g., political dependencies, operational exposures) while expanding on convexity and crowding ideas proposed in v2.1 documentation.

Key goals for AFV 2.2:
- Integrate governance to penalize fragility from state control or poor practices.
- Refine valuations for relative benchmarks, reducing absolute threshold biases.
- Enhance geographic and volatility handling for regional investments (e.g., Central/South America).
- Explicitly add proposed features: crowding penalties, convexity bonuses, and precious metals boosts.
- Maintain monthly computation using TTM/historical data from enhanced sources like Polygon.io for accuracy.

Updated equation: [ AFV_{2.2} = RP_{2.1} + S + G_{op} + D + T + VD_{rel} + Gov + C ] Where new/refined components are detailed below. Scores remain -10 to +10, with overrides for data issues.

1. Core Retention from AFV 2.1

AFV 2.2 retains the foundational structure:
- Refined Return Potential (RP_{2.1}): Cash flow efficiency with volatility penalties (capped [-3,5]).
- Sector/Industry Score (S): +1 for essentials like commodities, -1 for fragile sectors.
- Debt Resilience Score (D): Balance sheet strength ([-1.5,1]).
- Trend Momentum Score (T): OCF rate-of-change ([-1,1]).
- Valuation-Dividend Score (VD): Overvaluation guards, now refined (see below).

2. Proposed Features from AFV 2.1 Documentation

The original AFV 2.1 PDF outlined v2.2 ideas to add crowding, convexity, and precious metals enhancements. These are formalized here as a new Convexity/Crowding Score (C) [-1,1].

- **Crowding Penalty**: Penalizes stocks with sudden volume surges indicating speculative bubbles or over-ownership, increasing fragility. Compute as average daily volume over last month vs. 3-month average; if >2x, -0.5; >3x, -1.
- **Convexity Bonus**: Rewards low-beta stocks (beta <0.8: +0.5; <0.5: +1) for limited downside in volatility, aligning with "gains from disorder."
- **Precious Metals Boost**: For mining stocks in gold/silver/platinum (S already +1), add +0.5 if primary exposure >50% (from filings), as these thrive in inflation/geopolitical stress.

C = Sum of above (clamped [-1,1]). Data from Polygon/Yahoo (beta from 5-year history).

3. New Governance Score (Gov)

To address risks like state-owned enterprise (SOE) dependencies (e.g., policy interference in dividends/capex), add Gov [-1.5,1]. This subtracts fragility from governance weaknesses, common in emerging markets.

- Scoring: Based on ESG governance ratings (e.g., MSCI/Sustainalytics/LSEG).
  - +1: Top quartile (independent board, aligned incentives).
  - 0: Average.
  - -0.5: Moderate issues (e.g., family control).
  - -1: High risks (e.g., SOE with controversy history).
  - -1.5: Severe (e.g., corruption probes).
- Overrides: Auto -1 for >50% government ownership (from stock info APIs).
- Data Sources: MSCI ESG, Sustainalytics (via APIs); defaults to 0 if unavailable.

 </PAGE>
<PAGE 2> 4. Refined Valuation-Dividend Score (VD_{rel})

Original VD uses absolute P/E thresholds (>20/-0.5; >50/-1) with dividend rewards. Refine for relative context to avoid penalizing growth stocks unfairly in low-P/E regions.

- Base: Retain absolute rules.
- Relative Adjustment: Fetch 5-year average P/E (Yahoo/Polygon history). If current >1.5x average: -0.5 extra; <0.8x: +0.25 bonus.
- Clamped [-1.5,0.75] to emphasize guards against overvaluation.
- Rationale: Aligns with antifragility by flagging deviations from "normal" without rigid absolutes.

5. Enhanced Geographic Stability Score (G_{op})

Original G is HQ-based (e.g., Brazil 0.6). Enhance for operational exposures in volatile regions.

- Weighted: G_{op} = (HQ score × 0.6) + (Revenue-weighted average of operational countries × 0.4).
- Country Scores: Stable +1 (e.g., US/Germany), moderate 0.6 (Brazil/Colombia), high-risk -1 (e.g., Argentina/Venezuela).
- Data: Revenue breakdowns from annual filings (APIs/parse PDFs).
- Defaults to original G if data insufficient.

6. Strengthened Volatility in RP_{2.1}

To better handle emerging market cyclicality (e.g., commodity swings), tighten OCF margin volatility (CV) penalties.

- Lower Thresholds: CV >0.10: ×0.8; >0.20: ×0.5; <0.05: ×1.1 (original >0.15).
- Extend History: Require 5+ years for CV; if <5, apply -0.5 flat penalty.
- Min Margin: Add check for absolute min <0.10 over 4q: ×0.6.

This amplifies penalties for inconsistent cash flows, as seen in PBR's score declines.

7. General Adjustments and Overrides

- **SOE Flag in S**: For sectors like energy, if >50% state-owned: S -=0.5 (synergizes with Gov).
- **Data Sourcing**: Fully transition to Polygon.io for all metrics (enhanced accuracy vs. Yahoo).
- **Financial/Overrides**: Expand skips (e.g., Gov=0 for financials if non-comparable).
- **Computation**: Retain monthly caching; add backtesting module for historical validation.

## Limitations & Rationale for AFV 2.2

AFV 2.2 advances the "preparation over prediction" ethos by subtracting more risks (governance, relative overvaluation, operational geo-fragility) while boosting convexity rewards. It better suits regional investments (e.g., Latin America) by penalizing SOEs like PBR without discarding commodity upside. Backtest on historical data (e.g., 5 years) to calibrate.

For v2.3 ideas: AI-driven sentiment from X posts; ESG integration beyond governance.


