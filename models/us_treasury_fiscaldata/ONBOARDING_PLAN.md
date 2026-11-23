# us_treasury_fiscaldata — Onboarding Plan

U.S. Department of the Treasury **FiscalData** API — the aggregate federal fiscal
position. Keyless, paginated REST API with a per-endpoint field dictionary
(`meta.dataTypes` / `meta.labels`) that drives the architecture. Complements
`us_treasury_usaspending` (award-level outlays); this dataset is the aggregate
debt, receipts/outlays, interest, and exchange-rate position.

- Backend dataset slug: **`u_s_debt_to_the_penny`** (existing prod shell, id
  `6c564fc4-98db-4bf3-b787-b1bf7856933c`), broadened to the wider FiscalData
  scope. GCP dataset id: **`us_treasury_fiscaldata`**.
- Organization: `u_s_department_of_treasury`.
- License: U.S. Government public domain (open, no restriction).

## Tables (one per endpoint; MTS split into four wide tables)

| Table | Endpoint(s) | Grain | Rows | Span |
|-------|-------------|-------|------|------|
| `debt_outstanding` | `v2/accounting/od/debt_to_penny` | daily (business days) | 8,391 | 1993– |
| `historical_debt_outstanding` | `v2/accounting/od/debt_outstanding` | annual (fiscal-year-end) | 237 | 1790– |
| `mts_summary` | `v1/accounting/mts/mts_table_1` | monthly × statement line | 3,117 | FY2015– |
| `mts_receipts` | `v1/accounting/mts/mts_table_4` | monthly × receipt source | 7,604 | FY2015– |
| `mts_outlays` | `v1/accounting/mts/mts_table_5` | monthly × agency | 110,158 | FY2015– |
| `mts_means_of_financing` | `v1/accounting/mts/mts_table_6` | monthly × financing account | 6,759 | FY2015– |
| `average_interest_rate` | `v2/accounting/od/avg_interest_rates` | monthly × security | 5,009 | 2001– |
| `exchange_rate` | `v1/accounting/od/rates_of_exchange` | quarterly × country/currency | 18,980 | 2001– |

## Design decisions

- **MTS: four wide tables, not a melt.** The nine `mts_table_*` endpoints share one
  key/hierarchy skeleton (`record_date, parent_id, classification_id,
  classification_desc, src_line_nbr, sequence_level_nbr`) and differ only in their
  `*_amt` columns. Rather than melt them (which would collapse receipts, outlays,
  balances, and forward budget estimates into one `amount` column — unsafe to
  aggregate), the four tables carrying distinct actuals are kept wide, each with
  named typed columns: Table 1 (`mts_summary`), Table 4 (`mts_receipts`), Table 5
  (`mts_outlays`), Table 6 (`mts_means_of_financing`). The overlapping summaries
  and estimate/cross-tab tables (2, 3, 7, 8, 9) are left out. Each is keyed by
  `(record_date, src_line_nbr)` (`classification_id` regenerates each release, so
  `src_line_nbr` is the stable key). No dictionary table — the wide tables have no
  coded columns.
- **exchange_rate key** is `(record_date, country, currency, effective_date)`: a
  rate can be revised mid-quarter under a new `effective_date`. One byte-for-byte
  duplicate row across the full history is dropped in cleaning.
- **All-STRING staging, source values verbatim.** Amounts keep the API's exact
  decimal text (Debt to the Penny keeps its cents; no scientific notation, no
  float rounding); the dbt model `safe_cast`s every column to its real type.
- **Types by arithmetic meaning.** Money → FLOAT64 (USD); interest → FLOAT64
  (percent); `exchange_rate` → FLOAT64 (unit varies by row, per USD, documented in
  `observations`); `src_line_nbr`, `sequence_level_nbr`, `table_nbr`, ids → STRING.
  `year`/`month`/`fiscal_year` → INT64.
- **Country directory.** FiscalData `country` is a free-text English name with no
  ISO code; matching to `br_bd_diretorios_mundo.pais` would be lossy, so no
  directory link (noted in the architecture).

## Pipeline (recurring)

`pipelines/datasets/us_treasury_fiscaldata/` — one flow, each table **polled
independently** (debt daily, MTS + interest monthly, exchange quarterly,
historical annual) and rebuilt as a **full replace** (`dump_mode="overwrite"`). BD
Pro rolling window (6-month `free_lag`) on the monthly/daily tables
(`debt_outstanding`, the four `mts_*`, `average_interest_rate`);
`exchange_rate` (quarterly) and `historical_debt_outstanding` (annual) stay
AllFree. Schedule: daily
`50 18 * * *` America/Sao_Paulo; the per-table poll guard no-ops until each
source advances.

## DRY

The download + cleaning transform lives once in
`pipelines/datasets/us_treasury_fiscaldata/utils.py`; the one-shot bootstrap in
`code/` imports it. Architecture CSVs in `code/architecture/` are the schema
source of truth for both.
