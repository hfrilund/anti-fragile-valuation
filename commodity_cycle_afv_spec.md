# Commodity Cycle Context Layer for AFV

## 1. Purpose

This document specifies a new **Commodity Cycle Context Layer** to be stored alongside the existing **Antifragile Value (AFV)** score.

The purpose is **not** to modify the AFV score itself. AFV remains a stock-quality, resilience, cash-flow, debt, valuation, and antifragility scoring framework. The commodity-cycle layer adds a separate macro/industry-cycle datapoint that helps interpret commodity-sensitive stocks.

The intended result is that each monthly stock score can be evaluated using two separate perspectives:

```text
AFV score                  = company resilience / antifragile quality
Commodity Cycle Context    = external commodity-cycle tailwind/headwind
```

This separation is important because commodity companies can look financially excellent near the top of a cycle and financially weak near the bottom. The commodity-cycle context should help avoid being fooled by peak-cycle free cash flow, low P/E ratios, or temporarily high dividends.

---

## 2. Relationship to AFV

The existing AFV system computes a monthly score based on components such as:

- refined return potential
- sector/industry score
- geographic stability
- debt resilience
- operating-cash-flow trend momentum
- valuation/dividend score

The commodity-cycle layer should be stored beside these scores, not added into them.

Example:

```text
afv_scores
- ticker
- score_date
- afv_score
- rp_score
- sector_score
- geography_score
- debt_score
- trend_score
- valuation_dividend_score

commodity_cycle_context
- ticker
- score_date
- primary_commodity
- commodity_cycle_score
- commodity_cycle_phase
- commodity_cycle_risk_flag
- data_quality
```

A high AFV score combined with an early commodity upcycle may indicate a high-priority accumulation candidate.

A high AFV score combined with a late-cycle commodity context may indicate a good company whose current financials may be peak-cycle inflated.

---

## 3. Design Principles

### 3.1 Keep AFV and commodity timing separate

Commodity timing should not contaminate the AFV score. AFV should continue to represent company quality and resilience.

### 3.2 Prefer slow monthly signals

This system is not meant for trading intraday or weekly commodity moves. It should support monthly stock selection and portfolio-positioning decisions.

### 3.3 Be explicit about data quality

Some commodity-cycle datapoints are easily available from Yahoo Finance. Others require public datasets, manual collection, or professional data.

Every score should carry a data-quality marker.

Example:

```text
price_trend_source       = yahoo_finance
inventory_source         = eia
futures_curve_source     = missing_stub
producer_cost_source     = manual_company_report
macro_demand_source      = fred_oecd
```

### 3.4 Avoid false precision

If inventories, futures curves, or production-cost data are not available, the system should not pretend that the commodity-cycle score is complete.

Use fields like:

```text
commodity_cycle_score_partial
commodity_cycle_data_quality
missing_cycle_components
```

### 3.5 Prefer robustness over cleverness

A simple, explainable cycle score is better than a complex score that cannot be manually verified.

---

## 4. Target Use Cases

### 4.1 Monthly stock scoring

For every monthly AFV run, compute commodity-cycle context for commodity-sensitive stocks.

### 4.2 Watchlist prioritization

Use commodity-cycle context to distinguish between:

- high-AFV stocks in early upcycles
- high-AFV stocks with late-cycle risk
- low-AFV speculative turnarounds
- commodity stocks that should remain on watchlist

### 4.3 Peak-cycle warning

Flag stocks where current financials may be inflated by commodity-cycle conditions.

### 4.4 Manual review support

Generate comments that explain why the cycle phase was assigned.

Example:

```text
Copper cycle appears to be in early upcycle: copper price has crossed above its 12-month moving average, LME inventories are falling, and producer margins are improving. Futures curve data unavailable.
```

---

## 5. Commodity Exposure Model

Before scoring a commodity cycle, the system must determine whether the stock has meaningful commodity exposure.

### 5.1 Required fields

```text
commodity_exposure
- ticker
- primary_commodity
- secondary_commodity
- commodity_group
- exposure_type
- revenue_exposure_pct
- cost_exposure
- commodity_future_ticker
- exposure_confidence
- source
- source_date
- notes
```

### 5.2 Commodity groups

```text
energy
industrial_metals
precious_metals
fertilizer
steel
agriculture
shipping
other
```

### 5.3 Exposure types

```text
producer
integrated_producer
refiner
processor
royalty_streaming
service_provider
transporter
trader
mixed
unknown
```

### 5.4 Exposure confidence

```text
high      = verified from company report or manual table
medium    = inferred from industry and business description
low       = weak heuristic inference
unknown   = insufficient data
```

### 5.5 Manual mapping first

Yahoo Finance can provide sector, industry, and business summaries, but it usually cannot reliably provide the exact revenue split by commodity.

Therefore, version 1 should use a manually curated exposure table.

Example:

```csv
ticker,primary_commodity,commodity_group,exposure_type,revenue_exposure_pct,commodity_future_ticker,source,source_date
FCX,copper,industrial_metals,producer,,HG=F,manual,2026-06-03
NEM,gold,precious_metals,producer,,GC=F,manual,2026-06-03
XOM,crude_oil,energy,integrated_producer,,CL=F,manual,2026-06-03
SHEL.L,brent_oil,energy,integrated_producer,,BZ=F,manual,2026-06-03
```

---

## 6. Commodity Cycle Phases

The system should classify each commodity-sensitive stock into one of the following phases.

### 6.1 Downturn

The commodity price is weak, inventories are high or rising, margins are poor, and sentiment is negative.

Typical interpretation:

```text
Avoid, or keep on watchlist unless the company is exceptionally strong and the cycle is nearing stabilization.
```

### 6.2 Late downturn / watchlist

The cycle is still weak, but price or physical-market data is no longer deteriorating.

Typical interpretation:

```text
Begin monitoring closely. Do not assume recovery yet.
```

### 6.3 Early upcycle

The preferred entry zone. Commodity price trend has improved, inventories may be falling, margins are recovering, but investor sentiment may still be cautious.

Typical interpretation:

```text
Best area for accumulation if AFV is also strong.
```

### 6.4 Mid upcycle

Commodity price and margins are strong. Financials are improving. The stock may still be attractive, but the easy asymmetry may already have partly played out.

Typical interpretation:

```text
Hold or accumulate carefully. Monitor for late-cycle signs.
```

### 6.5 Late cycle

Commodity price is high versus history, margins are excellent, P/E ratios may look artificially low, and capex/M&A/sentiment may be heating up.

Typical interpretation:

```text
Beware peak-cycle financials. Consider trimming, reducing position size, or requiring a larger margin of safety.
```

### 6.6 Neutral / unclear

Signals conflict or data quality is insufficient.

Typical interpretation:

```text
Do not use cycle context strongly in position decisions.
```

---

## 7. Scoring Components

The commodity-cycle score should be composed of separate explainable components.

```text
CommodityCycleScore =
    price_trend_score
  + futures_curve_score
  + inventory_score
  + producer_margin_score
  + supply_capex_score
  + macro_demand_score
  - euphoria_penalty
```

The initial version should allow unavailable components to default to neutral while recording that they are missing.

---

## 8. Component Specification

## 8.1 Price Trend Score

### Purpose

Determine whether the commodity price trend is improving, deteriorating, or neutral.

### Data source

Initial source:

```text
Yahoo Finance / yfinance
```

Examples:

```text
CL=F   WTI crude oil futures
BZ=F   Brent crude oil futures
NG=F   natural gas futures
GC=F   gold futures
SI=F   silver futures
HG=F   copper futures
PL=F   platinum futures
PA=F   palladium futures
ZC=F   corn futures
ZW=F   wheat futures
ZS=F   soybean futures
```

Possible later source:

```text
World Bank Pink Sheet
```

### Metrics

```text
latest_price
sma_12m
sma_36m
price_vs_12m_sma
price_vs_36m_sma
roc_6m
roc_12m
price_z_5y
```

### Score logic

```text
+1  price above 12-month moving average and 6m/12m momentum positive
 0  mixed or neutral trend
-1  price below 12-month and 36-month averages with negative momentum
```

### Manual test checkpoint

For a selected commodity ticker, manually verify:

- the latest close price
- 12-month moving average
- 36-month moving average
- 6-month return
- 12-month return
- assigned score

Expected manual test output:

```text
Commodity: copper / HG=F
Latest price: X
12m SMA: Y
36m SMA: Z
6m ROC: A%
12m ROC: B%
Price trend score: +1 / 0 / -1
Manual result: pass/fail
```

---

## 8.2 Futures Curve Score

### Purpose

Detect whether the physical market is loose or tight using contango/backwardation.

### Data source

Yahoo Finance is not ideal for this.

Initial implementation:

```text
stub/manual/neutral
```

Later possible sources:

```text
CME DataMine
Nasdaq Data Link
broker/exported futures curves
paid commodity data provider
```

### Metrics

```text
front_month_price
six_month_future_price
twelve_month_future_price
curve_slope_6m
curve_slope_12m
curve_slope_change_6m
```

### Score logic

```text
+1  backwardation or contango improving
 0  flat, mixed, or unavailable
-1  contango worsening
```

### Version 1 behavior

```text
Return 0.0
Record futures_curve_source = missing_stub
Add futures_curve_score to missing_components
```

### Manual test checkpoint

Use a manually entered front and deferred contract price.

Example:

```text
front_month_price = 4.50
12m_future_price  = 4.20
curve_slope       = 4.50 / 4.20 - 1 = +7.14%
expected_score    = +1
```

Test cases:

```text
front > deferred and slope improving     => +1
front < deferred and slope worsening     => -1
mixed / no data                          => 0
```

---

## 8.3 Inventory Score

### Purpose

Determine whether physical commodity inventories are tightening or loosening.

### Data sources

Energy:

```text
EIA
```

Metals:

```text
LME warehouse stocks
COMEX stocks, if available
SHFE stocks, if available
```

Initial implementation:

```text
manual/stub for non-energy and non-LME commodities
```

### Metrics

```text
inventory_level
inventory_3m_change
inventory_12m_change
inventory_vs_5y_average
weeks_of_supply
```

### Score logic

```text
+1  inventories falling and/or below historical average
 0  neutral, mixed, or unavailable
-1  inventories rising and/or above historical average
```

### Manual test checkpoint

For one energy commodity and one metal:

```text
Commodity: crude oil
Source: EIA
Latest inventory: X
Inventory 12m ago: Y
YoY change: Z%
Inventory score: +1 / 0 / -1
Manual result: pass/fail
```

```text
Commodity: copper
Source: LME
Latest warehouse stock: X
Stock 12m ago: Y
YoY change: Z%
Inventory score: +1 / 0 / -1
Manual result: pass/fail
```

---

## 8.4 Producer Margin Score

### Purpose

Estimate whether the company benefits from current commodity prices after production costs.

### Data sources

Initial source:

```text
Yahoo Finance financial statements
```

Better source:

```text
company annual reports
quarterly reports
investor presentations
manual cost table
```

### Metrics

Commodity-specific if available:

```text
commodity_price
cash_cost
AISC
breakeven_price
margin_per_unit
margin_pct
```

Yahoo proxy metrics:

```text
revenue
operating_income
operating_margin
operating_cash_flow
ocf_margin
free_cash_flow
fcf_margin
```

### Score logic

Using true cost data:

```text
+1  margin positive and improving
 0  margin thin or normal
-1  margin negative or deteriorating
```

Using Yahoo financial-statement proxy:

```text
+1  OCF/operating margin improving for at least 2 periods and positive
 0  mixed or normal
-1  OCF/operating margin deteriorating or negative
```

### Manual test checkpoint

For one stock:

```text
Ticker: FCX
Revenue latest year: X
OCF latest year: Y
OCF margin latest: Z%
OCF margin previous: A%
OCF margin older: B%
Producer margin proxy score: +1 / 0 / -1
Manual result: pass/fail
```

If true cost data is manually entered:

```text
Commodity price: X
Company cash cost/AISC: Y
Margin pct: Z%
Expected score: +1 / 0 / -1
Manual result: pass/fail
```

---

## 8.5 Supply / Capex Score

### Purpose

Detect whether future supply is likely to increase or tighten.

### Data source

Initial implementation:

```text
manual/stub
```

Possible sources:

```text
company reports
industry reports
USGS for structural supply
EIA for energy production/capex proxies
paid commodity research providers
```

### Metrics

```text
industry_capex_trend
new_project_pipeline
production_growth_guidance
mine_closures
rig_count
reserve_replacement
```

### Score logic

```text
+1  underinvestment, closures, delayed projects, falling rig count
 0  neutral or unavailable
-1  capex boom, major new supply, aggressive production growth
```

### Version 1 behavior

```text
Return 0.0
Record supply_capex_source = missing_stub
```

### Manual test checkpoint

Create manual test rows:

```text
commodity,manual_supply_signal,expected_score
copper,underinvestment,+1
oil,rig_count_falling,+1
lithium,new_supply_wave,-1
gold,neutral,0
```

Verify that the parser maps each manual signal correctly.

---

## 8.6 Macro Demand Score

### Purpose

Determine whether the global macro backdrop supports commodity demand.

### Data sources

Initial sources:

```text
FRED
OECD CLI
World Bank commodity indices
```

Possible indicators:

```text
industrial_production
manufacturing_cycle
global_cli_momentum
usd_strength
real_rates
credit_conditions
china_proxy_indicators
```

### Commodity-specific macro drivers

Industrial metals:

```text
manufacturing activity
China demand proxies
industrial production
infrastructure cycle
global CLI
```

Gold:

```text
real interest rates
USD strength
financial stress
central bank buying, if available
```

Oil:

```text
global demand
inventories
refinery utilization
transport demand
industrial activity
```

### Score logic

```text
+1  macro backdrop improving for the commodity
 0  mixed or unavailable
-1  macro backdrop weakening
```

### Version 1 behavior

```text
Optional.
Can be stubbed to 0.0 until FRED/OECD integrations are complete.
```

### Manual test checkpoint

For one broad commodity group:

```text
Commodity group: industrial metals
OECD CLI 6m trend: positive
Industrial production 6m trend: positive
USD trend: neutral
Expected macro demand score: +1
Manual result: pass/fail
```

---

## 8.7 Euphoria Penalty

### Purpose

Flag late-cycle risk when financials and prices look excellent but may be cyclically inflated.

### Data sources

Initial source:

```text
Yahoo Finance price data
Yahoo Finance financial statements
manual flags
```

Later sources:

```text
news/sentiment
industry capex announcements
M&A activity
analyst estimate revisions
```

### Metrics

```text
commodity_price_z_5y
stock_12m_return
stock_24m_return
margin_z_score
dividend_yield_spike
manual_late_cycle_flag
```

### Score logic

```text
0   no euphoria penalty
-1  commodity price very high versus 5-year history and stock price very strong
-1  manual late-cycle flag active
```

### Manual test checkpoint

Test cases:

```text
commodity_z_5y = 2.0, stock_12m_return = 90%  => -1
commodity_z_5y = 0.5, stock_12m_return = 20%  => 0
manual_late_cycle_flag = true                 => -1
```

---

## 9. Combined Score

### 9.1 Initial weighting

Version 1 should overweight the components available from Yahoo and underweight missing/stub components.

```text
raw_score =
    1.25 * price_trend_score
  + 1.00 * producer_margin_score
  + 0.75 * futures_curve_score
  + 0.75 * inventory_score
  + 0.50 * supply_capex_score
  + 0.50 * macro_demand_score
  + 1.00 * euphoria_penalty
```

Clamp:

```text
commodity_cycle_score = max(-3.0, min(+3.0, raw_score))
```

### 9.2 Version 1 minimum viable score

If only Yahoo Finance is used:

```text
commodity_cycle_score_v1 =
    1.25 * price_trend_score
  + 1.00 * producer_margin_proxy_score
  + 1.00 * euphoria_penalty
```

The record should then be marked:

```text
data_quality = partial_yahoo_only
```

---

## 10. Phase Classification Logic

```text
if euphoria_penalty < 0:
    phase = late_cycle

elif total_score >= 2.0:
    phase = mid_upcycle

elif total_score >= 1.0 and price_trend_score > 0:
    phase = early_upcycle

elif total_score <= -2.0:
    phase = downturn

elif total_score < 0 and price_trend_score >= 0:
    phase = late_downturn_watchlist

else:
    phase = neutral_or_unclear
```

This logic should be treated as a first implementation. It should be manually reviewed using known historical commodity cycles.

---

## 11. Decision Interpretation Layer

The commodity-cycle layer should produce decision-support labels, not automatic buy/sell signals.

```text
AFV high + early upcycle       => high_priority_accumulation_candidate
AFV high + mid upcycle         => quality_hold_or_accumulate_carefully
AFV high + late cycle          => good_company_but_peak_cycle_risk
AFV low + early upcycle        => speculative_turnaround_only
Any AFV + downturn             => watchlist_or_avoid_until_cycle_improves
No commodity context           => non_commodity_or_no_cycle_context
```

Suggested AFV thresholds:

```text
high AFV     >= 3
medium AFV   1 to 3
low AFV      < 1
```

These can be adjusted after manual testing.

---

## 12. Database / Storage Specification

## 12.1 commodity_exposure table

```sql
CREATE TABLE commodity_exposure (
    ticker TEXT NOT NULL,
    primary_commodity TEXT NOT NULL,
    secondary_commodity TEXT,
    commodity_group TEXT NOT NULL,
    exposure_type TEXT NOT NULL,
    revenue_exposure_pct REAL,
    cost_exposure TEXT,
    commodity_future_ticker TEXT,
    exposure_confidence TEXT NOT NULL,
    source TEXT NOT NULL,
    source_date DATE,
    notes TEXT,
    PRIMARY KEY (ticker)
);
```

## 12.2 commodity_cycle_context table

```sql
CREATE TABLE commodity_cycle_context (
    ticker TEXT NOT NULL,
    score_date DATE NOT NULL,

    primary_commodity TEXT NOT NULL,
    commodity_group TEXT NOT NULL,
    exposure_type TEXT NOT NULL,

    commodity_cycle_score REAL NOT NULL,
    commodity_cycle_phase TEXT NOT NULL,
    cycle_risk_flag TEXT NOT NULL,

    price_trend_score REAL NOT NULL,
    futures_curve_score REAL NOT NULL,
    inventory_score REAL NOT NULL,
    producer_margin_score REAL NOT NULL,
    supply_capex_score REAL NOT NULL,
    macro_demand_score REAL NOT NULL,
    euphoria_penalty REAL NOT NULL,

    price_trend_source TEXT,
    futures_curve_source TEXT,
    inventory_source TEXT,
    producer_margin_source TEXT,
    supply_capex_source TEXT,
    macro_demand_source TEXT,

    data_quality TEXT NOT NULL,
    missing_components TEXT,
    comment TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (ticker, score_date)
);
```

## 12.3 commodity_price_metrics table

Optional but useful for auditability.

```sql
CREATE TABLE commodity_price_metrics (
    commodity_ticker TEXT NOT NULL,
    score_date DATE NOT NULL,

    latest_price REAL,
    sma_12m REAL,
    sma_36m REAL,
    roc_6m REAL,
    roc_12m REAL,
    price_z_5y REAL,

    source TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (commodity_ticker, score_date)
);
```

---

## 13. Python Module Structure

Suggested package structure:

```text
afv_project/
    afv/
        afv_engine.py
        afv_models.py

    commodity_cycle/
        __init__.py
        models.py
        exposure_resolver.py
        yahoo_provider.py
        world_bank_provider.py
        eia_provider.py
        lme_provider.py
        fred_oecd_provider.py
        price_trend_scorer.py
        futures_curve_scorer.py
        inventory_scorer.py
        producer_margin_scorer.py
        supply_capex_scorer.py
        macro_demand_scorer.py
        euphoria_scorer.py
        cycle_engine.py
        decision_interpreter.py
        repositories.py

    tests/
        test_price_trend_scorer.py
        test_futures_curve_scorer.py
        test_inventory_scorer.py
        test_producer_margin_scorer.py
        test_euphoria_scorer.py
        test_cycle_engine.py
        manual_checkpoints/
            copper_cycle_manual_check.md
            oil_cycle_manual_check.md
            gold_cycle_manual_check.md
```

---

## 14. Implementation Phases

# Phase 0: Design Lockdown

## Goal

Agree the scope and prevent the implementation from becoming too broad.

## Tasks

- Confirm that commodity-cycle context is separate from AFV.
- Confirm initial commodity groups.
- Confirm initial score range: -3 to +3.
- Confirm initial cycle phases.
- Confirm initial data-quality labels.

## Deliverables

- Finalized specification document.
- Initial list of supported commodities.
- Initial manual exposure CSV.

## Manual checkpoint

Verify that the following statements are true:

```text
[ ] AFV score formula is unchanged.
[ ] Commodity cycle score is stored separately.
[ ] Missing data is recorded explicitly.
[ ] The first version can run with Yahoo-only data.
[ ] Non-Yahoo components are allowed to be neutral/stubbed.
```

---

# Phase 1: Yahoo-Only Prototype

## Goal

Build the first working commodity-cycle layer using only Yahoo Finance and manual exposure mapping.

## Included components

```text
price_trend_score
producer_margin_proxy_score
euphoria_penalty
commodity_cycle_score_partial
commodity_cycle_phase
cycle_risk_flag
data_quality
comment
```

## Excluded/stubbed components

```text
futures_curve_score = 0
inventory_score = 0
supply_capex_score = 0
macro_demand_score = 0
```

## Tasks

1. Create commodity exposure data model.
2. Create manual commodity exposure CSV.
3. Implement Yahoo price history provider.
4. Implement Yahoo financial statement provider.
5. Implement price trend scorer.
6. Implement financial-statement margin proxy scorer.
7. Implement euphoria penalty scorer.
8. Implement combined score calculation.
9. Implement phase classification.
10. Save results to local database or flat file.

## Manual testing checkpoint 1: exposure mapping

Input:

```text
FCX
NEM
XOM
SHEL.L
AAPL
```

Expected:

```text
FCX     => commodity context created, copper
NEM     => commodity context created, gold
XOM     => commodity context created, crude oil / energy
SHEL.L  => commodity context created, brent oil / energy
AAPL    => no commodity context
```

## Manual testing checkpoint 2: price trend

For each commodity ticker:

```text
HG=F
GC=F
CL=F
BZ=F
```

Manually inspect:

```text
latest price
12m SMA
36m SMA
6m ROC
12m ROC
assigned price trend score
```

Expected:

```text
The score should match the documented rule.
```

## Manual testing checkpoint 3: margin proxy

For each stock:

```text
FCX
NEM
XOM
```

Manually inspect:

```text
revenue
operating cash flow
OCF margin latest
OCF margin previous
OCF margin older
assigned margin proxy score
```

Expected:

```text
Improving positive margins => +1
Deteriorating or negative margins => -1
Mixed data => 0
```

## Manual testing checkpoint 4: full context record

For one stock, manually verify the final record:

```text
ticker
score_date
primary_commodity
commodity_cycle_score
commodity_cycle_phase
cycle_risk_flag
all component scores
data_quality
missing_components
comment
```

Expected:

```text
All unavailable components are listed as missing/stubbed.
The comment matches the component scores.
```

## Exit criteria

```text
[ ] System runs for at least 10 manually mapped stocks.
[ ] No commodity context is created for clearly non-commodity stocks.
[ ] Component scores can be manually recalculated.
[ ] Missing data is transparent.
[ ] Results are stored beside AFV scores.
```

---

# Phase 2: World Bank Pink Sheet Integration

## Goal

Add a clean monthly commodity reference-price source independent of Yahoo futures tickers.

## Why

Yahoo futures are useful but can be affected by futures rolling, contract behavior, and ticker availability. World Bank monthly commodity prices are better for slow monthly cycle context.

## Tasks

1. Implement World Bank Pink Sheet provider.
2. Normalize commodity names to internal commodity IDs.
3. Store monthly commodity reference prices.
4. Add option to use World Bank price series for price trend scoring.
5. Compare Yahoo and World Bank price trend outputs.

## Manual testing checkpoint

For selected commodities:

```text
crude oil
copper
gold
natural gas
fertilizer index, if used
```

Manually verify:

```text
latest monthly price
12m SMA
36m SMA
6m ROC
12m ROC
score from Yahoo
score from World Bank
```

Expected:

```text
Yahoo and World Bank scores should usually agree directionally.
When they differ, the system should show which source was used.
```

## Exit criteria

```text
[ ] World Bank price series can be fetched and cached.
[ ] At least three commodities match internal commodity IDs.
[ ] Price trend score can use either Yahoo or World Bank.
[ ] Source is stored in the output record.
```

---

# Phase 3: EIA Energy Inventory Integration

## Goal

Add real physical-market inventory signals for energy commodities.

## Included commodities

```text
crude oil
natural gas
gasoline
distillates / diesel
propane, optional
```

## Tasks

1. Implement EIA API/data provider.
2. Fetch crude oil inventory series.
3. Fetch natural gas storage series.
4. Fetch gasoline/distillate inventory series if relevant.
5. Compute YoY inventory change.
6. Compute inventory vs historical average.
7. Implement energy inventory score.
8. Integrate into commodity-cycle engine.

## Manual testing checkpoint

For crude oil:

```text
latest_inventory
inventory_12m_ago
yoy_change_pct
inventory_vs_5y_avg
inventory_score
```

Expected scoring:

```text
inventories falling and/or below average => +1
inventories rising and/or above average  => -1
mixed                                    => 0
```

For natural gas:

```text
latest_storage
storage_12m_ago
yoy_change_pct
storage_vs_5y_avg
inventory_score
```

## Exit criteria

```text
[ ] Energy inventory data is stored with source and date.
[ ] Oil/gas stocks receive non-zero inventory scores when justified.
[ ] Missing energy data fails gracefully.
[ ] Manual check confirms calculations.
```

---

# Phase 4: LME Metals Inventory Integration

## Goal

Add warehouse-stock signals for industrial metals.

## Included commodities

```text
copper
aluminium
zinc
nickel
lead
tin
```

## Tasks

1. Implement LME warehouse report importer.
2. Normalize metal names to internal commodity IDs.
3. Store warehouse stock levels.
4. Compute 3m and 12m changes.
5. Compute stock level versus historical average.
6. Implement metals inventory score.
7. Integrate score into commodity-cycle engine.

## Manual testing checkpoint

For copper:

```text
latest_lme_stock
stock_3m_ago
stock_12m_ago
3m_change_pct
12m_change_pct
inventory_score
```

Expected:

```text
stocks falling materially => +1
stocks rising materially  => -1
mixed                     => 0
```

## Exit criteria

```text
[ ] LME stock data imports correctly.
[ ] Copper inventory score is available.
[ ] At least three metals have inventory series.
[ ] Manual check confirms score calculation.
```

---

# Phase 5: Macro Demand Layer

## Goal

Add macro-cycle context for commodity demand.

## Initial sources

```text
FRED
OECD CLI
```

## Tasks

1. Implement FRED provider.
2. Implement OECD CLI provider.
3. Define commodity-group macro mappings.
4. Implement macro demand scorer.
5. Store macro indicators and score.

## Example mappings

Industrial metals:

```text
global CLI momentum
industrial production momentum
USD trend
```

Gold:

```text
real rates trend
USD trend
financial stress proxy
```

Energy:

```text
industrial production
transport/fuel demand proxy
CLI momentum
```

## Manual testing checkpoint

For industrial metals:

```text
OECD CLI latest
OECD CLI 6m ago
industrial production latest
industrial production 6m ago
USD trend
macro_demand_score
```

Expected:

```text
Improving CLI and industrial production => +1
Weakening indicators                    => -1
Mixed                                   => 0
```

## Exit criteria

```text
[ ] Macro score is available by commodity group.
[ ] Indicator sources are stored.
[ ] Score can be manually explained.
[ ] Commodity-cycle comment includes macro context when available.
```

---

# Phase 6: Manual Company Cost and Exposure Data

## Goal

Improve producer-margin scoring using company-specific data instead of only Yahoo financial statement proxies.

## Tasks

1. Extend manual company exposure table.
2. Add production unit and cost fields.
3. Add source document and source date fields.
4. Implement true producer-margin score.
5. Prefer true margin score over Yahoo margin proxy when available.

## Table fields

```text
ticker
primary_commodity
production_unit
annual_production
cash_cost
AISC
breakeven_price
reserve_life_years
main_jurisdictions
hedging_notes
source_document
source_date
```

## Manual testing checkpoint

For one gold miner:

```text
gold_price
AISC
margin_per_ounce
margin_pct
expected_margin_score
actual_margin_score
```

For one oil producer:

```text
oil_price
breakeven_price
margin_per_barrel
margin_pct
expected_margin_score
actual_margin_score
```

## Exit criteria

```text
[ ] Manual company cost data is loaded.
[ ] True margin score overrides Yahoo proxy when available.
[ ] Output record states whether margin score is true-cost or proxy.
[ ] Manual check passes for at least two companies.
```

---

# Phase 7: Futures Curve Integration

## Goal

Add proper contango/backwardation scoring.

## Data source options

```text
CME DataMine
Nasdaq Data Link
broker export
paid commodity API
manual futures curve CSV
```

## Tasks

1. Choose futures curve data source.
2. Implement futures curve provider.
3. Store front, 6m, and 12m contract prices.
4. Compute curve slope.
5. Compute change in curve slope.
6. Implement futures curve score.
7. Integrate with cycle engine.

## Manual testing checkpoint

Input:

```text
front_month_price
six_month_price
twelve_month_price
front_vs_12m_slope
slope_6m_ago
slope_change
```

Expected:

```text
backwardation and improving tightness => +1
contango worsening                    => -1
mixed                                 => 0
```

## Exit criteria

```text
[ ] Futures curve score is non-stubbed for at least oil and copper.
[ ] Curve source is stored.
[ ] Manual calculations match implementation.
[ ] Late-cycle and early-upcycle classifications improve after adding curve data.
```

---

# Phase 8: Historical Backtesting / Sanity Testing

## Goal

Check whether the commodity-cycle layer behaves sensibly across known historical cycles.

## Test periods

Suggested historical windows:

```text
Oil downturn: 2014-2016
Oil recovery: 2020-2022
Copper downturn/recovery: 2015-2017
Copper upcycle: 2020-2021
Gold upcycle: 2019-2020
Commodity inflation period: 2021-2022
```

## Tasks

1. Run historical monthly commodity-cycle scores.
2. Compare scores to known commodity price behavior.
3. Check whether late-cycle warnings appear near obvious peaks.
4. Check whether early-upcycle labels appear after troughs, not after the whole move is over.
5. Compare stock performance after each phase label.

## Manual testing checkpoint

For each commodity:

```text
commodity
historical_month
expected_phase
actual_phase
manual_pass_fail
notes
```

Example:

```text
commodity: oil
month: 2020-05
expected_phase: late_downturn_watchlist or early_upcycle
actual_phase: early_upcycle
manual result: pass
```

## Exit criteria

```text
[ ] Historical phase labels are directionally sensible.
[ ] Obvious late-cycle periods receive warnings.
[ ] Obvious downturns are not classified as strong upcycles.
[ ] The score does not overreact to one-month price moves.
```

---

# Phase 9: AFV Decision Integration

## Goal

Use commodity-cycle context beside AFV to create decision-support labels.

## Tasks

1. Join AFV score records with commodity-cycle records.
2. Implement decision interpretation layer.
3. Add output fields to monthly report.
4. Add filters for high-priority candidates.
5. Add warnings for peak-cycle risk.

## Decision labels

```text
high_priority_accumulation_candidate
quality_hold_or_accumulate_carefully
good_company_but_peak_cycle_risk
speculative_turnaround_only
watchlist_or_avoid_until_cycle_improves
neutral_research_required
non_commodity_or_no_cycle_context
```

## Manual testing checkpoint

Test matrix:

```text
AFV score | cycle phase      | expected label
-------------------------------------------------------------
4.0       | early_upcycle    | high_priority_accumulation_candidate
4.0       | late_cycle       | good_company_but_peak_cycle_risk
0.5       | early_upcycle    | speculative_turnaround_only
2.0       | downturn         | watchlist_or_avoid_until_cycle_improves
3.5       | neutral          | neutral_research_required
```

## Exit criteria

```text
[ ] Decision labels are generated.
[ ] AFV score itself is unchanged.
[ ] Commodity-cycle context appears in monthly output.
[ ] Manual matrix tests pass.
```

---

# Phase 10: Reporting and Review

## Goal

Make the output usable for monthly investing decisions.

## Suggested report columns

```text
ticker
company_name
afv_score
primary_commodity
commodity_cycle_score
commodity_cycle_phase
cycle_risk_flag
data_quality
missing_components
decision_label
comment
```

## Suggested filters

```text
AFV >= 3 and cycle_phase = early_upcycle
AFV >= 3 and cycle_risk_flag = high_peak_cycle_risk
cycle_phase = late_downturn_watchlist
missing_components contains inventory_score
```

## Manual testing checkpoint

Open the monthly report and verify:

```text
[ ] Commodity-sensitive stocks have context.
[ ] Non-commodity stocks are not polluted with fake context.
[ ] Missing data is visible.
[ ] Comments are understandable.
[ ] Decision labels match AFV + cycle phase.
```

---

## 15. Pseudo-Code Overview

```python
class MonthlyScoringPipeline:
    def run(self, tickers, score_date):
        for ticker in tickers:
            afv_record = afv_engine.compute(ticker, score_date)
            repository.save_afv_score(afv_record)

            exposure = exposure_resolver.resolve(ticker)

            if exposure is None:
                repository.save_decision_label(
                    ticker=ticker,
                    score_date=score_date,
                    label="non_commodity_or_no_cycle_context"
                )
                continue

            cycle_context = commodity_cycle_engine.compute(
                ticker=ticker,
                exposure=exposure,
                score_date=score_date
            )

            repository.save_commodity_cycle_context(cycle_context)

            decision_label = decision_interpreter.interpret(
                afv_record=afv_record,
                cycle_context=cycle_context
            )

            repository.save_decision_label(
                ticker=ticker,
                score_date=score_date,
                label=decision_label
            )
```

---

## 16. Example Output Record

```json
{
  "ticker": "FCX",
  "score_date": "2026-06-30",
  "primary_commodity": "copper",
  "commodity_group": "industrial_metals",
  "exposure_type": "producer",
  "commodity_cycle_score": 1.75,
  "commodity_cycle_phase": "early_upcycle",
  "cycle_risk_flag": "improving_asymmetry",
  "price_trend_score": 1.0,
  "futures_curve_score": 0.0,
  "inventory_score": 1.0,
  "producer_margin_score": 1.0,
  "supply_capex_score": 0.0,
  "macro_demand_score": 0.0,
  "euphoria_penalty": 0.0,
  "data_quality": "medium",
  "missing_components": "futures_curve_score,supply_capex_score,macro_demand_score",
  "comment": "Copper cycle appears to be improving. Price trend is positive, inventories are falling, and producer margins are improving. Futures curve, supply/capex, and macro-demand data are unavailable or stubbed."
}
```

---

## 17. Minimum Viable Implementation

The smallest useful version is:

```text
manual commodity exposure table
Yahoo commodity price history
Yahoo stock financial statements
price trend score
producer margin proxy score
euphoria penalty
combined partial cycle score
phase label
data-quality field
manual testing checkpoints
```

This gives useful cycle context without requiring a large external data platform.

---

## 18. Recommended First Ten Test Stocks

Use a mix of commodity and non-commodity stocks.

```text
FCX       copper miner
NEM       gold miner
XOM       integrated oil
SHEL.L    integrated oil / gas
RIO       diversified mining
BHP       diversified mining
VALE      iron ore / base metals
NTR       fertilizer / agriculture inputs
AAPL      non-commodity control
MSFT      non-commodity control
```

Expected behavior:

```text
Commodity context should be created for the first eight.
No commodity context should be created for AAPL or MSFT unless manually mapped.
```

---

## 19. Known Limitations

### 19.1 Yahoo Finance limitations

Yahoo Finance is useful for:

```text
stock prices
commodity front-month futures prices
financial statements
basic metadata
```

Yahoo Finance is weak or insufficient for:

```text
inventories
futures curves
cost curves
mine-level economics
true revenue exposure
macro demand data
supply pipeline
capex pipeline
```

### 19.2 Commodity classification risk

Automated classification based on sector/industry can be wrong. Manual exposure mapping is recommended.

### 19.3 Peak-cycle accounting risk

Financial statement data may look best near the top of a cycle. The euphoria penalty and normalized valuation checks are intended to reduce this risk, but they cannot remove it completely.

### 19.4 Data frequency mismatch

Different sources update at different frequencies:

```text
Yahoo prices               daily/monthly
financial statements        quarterly/annual
EIA inventories             weekly/monthly
LME inventories             daily
World Bank prices           monthly
OECD/FRED macro             monthly/quarterly
company cost data           quarterly/annual/manual
```

For AFV compatibility, convert everything to a monthly score date.

---

## 20. Future Enhancements

Potential later improvements:

```text
normalized valuation using mid-cycle commodity prices
stress valuation using trough commodity prices
company-specific reserve life scoring
jurisdiction risk for mines and oil fields
hedging-book adjustment
crowding score using volume/open interest
analyst estimate revision score
commodity beta estimation
position sizing overlay
historical backtest dashboard
```

---

## 21. Final Implementation Checklist

```text
[ ] AFV score unchanged
[ ] Commodity exposure table exists
[ ] Commodity cycle table exists
[ ] Yahoo price trend scoring works
[ ] Yahoo margin proxy scoring works
[ ] Euphoria penalty works
[ ] Missing/stubbed components are visible
[ ] Monthly records are stored beside AFV scores
[ ] Manual checkpoints pass
[ ] Decision labels are generated
[ ] Report output is understandable
```

---

## 22. Practical First Milestone

The first milestone should not be a perfect commodity-cycle model.

It should be this:

```text
For every stock in the monthly AFV run, identify whether it is commodity-sensitive.
For commodity-sensitive stocks, store a transparent Yahoo-based partial cycle context.
Show whether the stock appears to be in downturn, early upcycle, mid-upcycle, late-cycle, or unclear.
Clearly mark which signals are real and which are missing.
```

Once that is working and manually verified, add inventories, macro data, and futures curves in later phases.
