# us_fed_fred — Seed Series List (v1)

FRED aggregates 800k+ series; many are third-party **copyrighted** and cannot be
redistributed (OECD, S&P Dow Jones, ICE/BofA, University of Michigan, CBOE, …).
This onboarding ships **only the openly-redistributable subset**: series produced
by U.S. federal agencies, whose works are public domain (17 U.S.C. §105).

## License gate — two filters, both applied at download

1. **Source allowlist.** Keep a series only if its FRED `source` (from
   `fred/series/source` / `fred/source`) is one of:
   - Board of Governors of the Federal Reserve System (US)
   - U.S. Bureau of Labor Statistics
   - U.S. Bureau of Economic Analysis
   - U.S. Census Bureau
   - U.S. Department of the Treasury. Fiscal Service
   - U.S. Office of Management and Budget
   - U.S. Employment and Training Administration (Dept. of Labor)
   - Federal Reserve Bank of St. Louis (its own derived public series)

   (Exact FRED `source` strings — verified against the API. GFDEGDQ188S and FYFSD
   resolve to OMB, not Treasury; T10Y2Y/T10Y3M/USREC resolve to the St. Louis Fed.)
2. **"Copyright"-in-notes exclusion.** Drop any series whose `/series` `notes`
   field contains the word "Copyright" (FRED's own marker for restricted series),
   even if the source is allowlisted. Belt-and-suspenders.

Every series dropped by either filter is logged (id, source, reason) to
`code/excluded_series.csv` at download time. No restricted series is published.

## Seed set (~50 high-value macro series)

Verified programmatically at download; any entry failing a filter is removed and
logged. `freq` shown for reference.

### Board of Governors of the Federal Reserve System

| series_id | title | freq |
|---|---|---|
| FEDFUNDS | Federal Funds Effective Rate | M |
| DFF | Federal Funds Effective Rate | D |
| DGS10 | 10-Year Treasury Constant Maturity Rate | D |
| DGS2 | 2-Year Treasury Constant Maturity Rate | D |
| DGS3MO | 3-Month Treasury Constant Maturity Rate | D |
| DTB3 | 3-Month Treasury Bill, Secondary Market | D |
| T10Y2Y | 10Y minus 2Y Treasury spread | D |
| T10Y3M | 10Y minus 3M Treasury spread | D |
| WALCL | Federal Reserve Total Assets | W |
| M2SL | M2 Money Stock | M |
| M1SL | M1 Money Stock | M |
| BOGMBASE | Monetary Base | M |
| INDPRO | Industrial Production Index | M |
| TCU | Capacity Utilization: Total Industry | M |
| TOTALSL | Total Consumer Credit | M |
| DEXUSEU | U.S. / Euro Foreign Exchange Rate | D |
| DEXJPUS | Japan / U.S. Foreign Exchange Rate | D |
| DEXCHUS | China / U.S. Foreign Exchange Rate | D |

### U.S. Bureau of Labor Statistics

| series_id | title | freq |
|---|---|---|
| CPIAUCSL | CPI, All Urban Consumers, SA | M |
| CPILFESL | Core CPI (less Food & Energy), SA | M |
| UNRATE | Unemployment Rate | M |
| U6RATE | U-6 Unemployment Rate | M |
| CIVPART | Labor Force Participation Rate | M |
| EMRATIO | Employment-Population Ratio | M |
| PAYEMS | All Employees, Total Nonfarm | M |
| MANEMP | All Employees, Manufacturing | M |
| CES0500000003 | Avg Hourly Earnings, Total Private | M |
| JTSJOL | Job Openings: Total Nonfarm (JOLTS) | M |
| PPIACO | Producer Price Index: All Commodities | M |

### U.S. Bureau of Economic Analysis

| series_id | title | freq |
|---|---|---|
| GDP | Gross Domestic Product | Q |
| GDPC1 | Real Gross Domestic Product | Q |
| A191RL1Q225SBEA | Real GDP, % change annualized | Q |
| PCE | Personal Consumption Expenditures | M |
| PCEPI | PCE Price Index | M |
| PCEPILFE | Core PCE Price Index | M |
| DSPIC96 | Real Disposable Personal Income | M |
| PSAVERT | Personal Saving Rate | M |
| CP | Corporate Profits After Tax | Q |

### U.S. Census Bureau

| series_id | title | freq |
|---|---|---|
| HOUST | Housing Starts: Total | M |
| PERMIT | New Private Housing Building Permits | M |
| RSAFS | Advance Retail Sales | M |
| DGORDER | Manufacturers' New Orders: Durable Goods | M |
| TTLCONS | Total Construction Spending | M |
| BUSINV | Total Business Inventories | M |

### U.S. Dept. of the Treasury / Fiscal Service

| series_id | title | freq |
|---|---|---|
| GFDEBTN | Federal Debt: Total Public Debt | Q |
| GFDEGDQ188S | Federal Debt: Total Public Debt as % of GDP | Q |
| MTSDS133FMS | Federal Surplus or Deficit (monthly) | M |
| FYFSD | Federal Surplus or Deficit (annual) | A |

### U.S. Employment and Training Administration (DOL)

| series_id | title | freq |
|---|---|---|
| ICSA | Initial Unemployment Claims | W |

### Federal Reserve Bank of St. Louis (derived, public)

| series_id | title | freq |
|---|---|---|
| USREC | NBER-based Recession Indicator | M |

## Explicitly excluded (restricted — do NOT onboard)
Recorded here as known third-party/copyrighted series a future contributor might
be tempted to add:
- UMCSENT — University of Michigan Consumer Sentiment (copyrighted)
- SP500 — S&P 500 (S&P Dow Jones Indices, copyrighted)
- VIXCLS — CBOE Volatility Index (CBOE, copyrighted)
- BAMLH0A0HYM2 and other ICE BofA indices (ICE Data Indices, copyrighted)
- Any series with a `source` outside the allowlist, or "Copyright" in `notes`.
