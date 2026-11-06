# us_eia_consumption — design notes

U.S. Energy Information Administration **Form EIA-861** and **Form EIA-861M** —
the retail (demand) side of the U.S. electric power industry: electricity sold
to end-use customers, with revenue, sales and customer counts by utility, state
and customer sector, the utility frame, and county-level service territory.
Source, cleaning code and dbt models live under this directory and
`pipelines/datasets/us_eia_consumption/`.

This is the demand side of `us_eia_electricity` (the supply side — plants,
generators, generation, fuel); the two join on `utility_id`. SEDS, the all-fuel
state energy consumption system, is a separate dataset, `us_eia_seds`. Each of
the three datasets' descriptions points at the other two.

## Scope decision — separate dataset, split from SEDS, EIA-861 floored at 2001

Three choices, all made explicitly with the user:

1. **Separate dataset, not tables on `us_eia_electricity`.** That dataset is
   named for what it holds (electricity generation) and is already published
   with fixed coverage, BD Pro tiers and a deployed pipeline; EIA-861 is the
   demand side, joined only by `utility_id`.
2. **Split from SEDS.** A prior iteration of this onboarding built a *combined*
   `us_eia_consumption` holding both EIA-861 and SEDS (see the abandoned branch
   `data/us_eia_consumption`). The user chose instead to keep this dataset to
   EIA-861 and give SEDS its own dataset `us_eia_seds`, on the grounds that SEDS
   covers every fuel and is a distinct publication with its own grain and
   refresh. The combined build's staging registration was overwritten to this
   EIA-861-only design and its `seds_consumption` table removed.
3. **EIA-861 annual tables floored at 2001.** Before 2001 EIA-861 is a fixed
   record-type survey (`F861TYPn.xls` + `LAYOUT90.TXT`) with no by-sector
   utility grain — a differently shaped form, deferred as a clean follow-up, the
   same reason `us_eia_electricity` floors at 2001. EIA-861M keeps its full
   1990-present history because EIA maintains it as one harmonised file.

## Tables

| table | grain | years | source |
|---|---|---:|---|
| `utility` | utility × year | 2001–2025 | EIA-861 utility frame (`file1` / `Utility_Data`) |
| `retail_sales` | utility × year × state × service type × sector | 2001–2025 | EIA-861 sales to ultimate customers (`file2` / `Sales_Ult_Cust`) |
| `service_territory` | utility × year × county | 2012–2025 | EIA-861 service territory (its own file only from 2012) |
| `eia861m` | state × year × month × sector | 1990–present | EIA-861M `sales_revenue.xlsx` + archived `HS861M 1990-2009.xlsx` |
| `dicionario` | — | — | coded values of `part`, `data_type`, `short_form` |

Licence: **US Government public domain**, registered `cc0`
(<https://www.eia.gov/about/copyrights_reuse.php>), as for `us_eia_electricity`.

## The traps

### 1. EIA renames the current file every release; three layout eras

EIA stamps the release into the file name and renames it on every publication,
so a hardcoded name is stale within weeks (the trap the brief warned about, and
the same one PUDL hits for EIA-860/923). A year's ZIP is resolved by trying a
few URL templates and the workbook inside by **pattern** (`MEMBER_PATTERNS`),
never a literal name; a pattern matching zero or two members raises.

The annual files come in **three layout eras**, handled by header *detection*
(not a fixed row) plus a synonym map, so all three normalise to one schema:

- **2001–2007** flat `file2.xls`: single-row header. 2001–2006 abbreviate the
  sector (`Res Revenue (000)`); **2007 spells it out** (`RESIDENTIAL_REVENUES`,
  `SCHED4PART`) — both are mapped.
- **2008–2011** `file2_YYYY.xls`: three-row banner (sector / measure / id), no
  BA code.
- **2012–2025** `Sales_Ult_Cust_YYYY.xlsx`: three-row banner, adds `BA Code`.

### 2. Summing across `service_type` double-counts

`retail_sales` is published wide by sector and long here (Total sector dropped).
But a utility files under a `service_type`, and in restructured markets the same
energy is reported as **Delivery** (by the wires utility) *and* **Energy** (by
the competitive supplier). Summing sales or revenue over the whole `service_type`
column double-counts. **For a de-duplicated total, sum only Bundled and Energy,
excluding Delivery.** Verified against EIA's published figure: US residential
2020 = Bundled 1,355.8 + Energy 108.8 = 1,464.6 million MWh, against EIA's ~1,462
(summing all service types gives 1,573, ~7.6% too high). The table description
says this in all three languages.

### 3. The natural key needs `ba_code`

`retail_sales` keys on `(year, utility_id, state_id, part, service_type,
ba_code, customer_sector)`. From 2013 a utility files one row per balancing
authority; without `ba_code` the key collides on 2.75% of rows, with it on
0.05% (genuine repeated filings). `ba_code` is null before 2013 and where a
utility operates in one BA.

### 4. EIA-861M: two files, and a historical "Other" sector

`eia861m` unions the current `sales_revenue.xlsx` (Monthly-States/Ter, 2010→) and
the archived `HS861M 1990-2009.xlsx` (same banner). The current file's fifth
sector column is a **Total** (dropped); the historical file's is **Other**, a
real residual sector kept and confined to 1990–2009. `price` is taken from the
source (¢/kWh); `retail_sales` has no price column, so its `average_price_cents_kwh`
is derived as revenue ÷ sales.

## Coverage tier

The house rule paywalls the most recent window of any table refreshing monthly
or more often. That is `eia861m` alone: **`PartBdpro`, 6-month free lag** (free
≤ 2025-12, pro 2026-01→ at onboarding; the window rolls each run). Its
`(year, month)` are guaranteed non-null by the builder, so no row is paywalled
forever by a NULL date. The three annual tables are `AllFree`.

## Layout

Mirrors `us_eia_electricity`: architecture CSVs are the schema source of truth;
`gen_dbt.py` / `gen_columns_json.py` generate the models and backend payloads;
the transform lives in `pipelines/datasets/us_eia_consumption/utils.py` and is
imported by `code/common.py`. Scratch data → `~/Downloads/us_eia_consumption_data/`
(`US_EIA_CONSUMPTION_DATA_DIR`), never in the repo or Dropbox.

## Recurring pipeline

`pipelines/datasets/us_eia_consumption/flows.py`, daily poll at 09:31 BRT. Two
source clocks polled separately (EIA-861 annual via `retail_sales`, EIA-861M
monthly via `eia861m`). Every run rebuilds every affected year from the newest
files, so an early-release → final supersession cannot double-count.

## What is deferred

- **EIA-861 pre-2001** (the `F861TYPn` record-type era) — a differently shaped
  survey.
- **The rest of the EIA-861 schedules**: advanced metering, demand response,
  distributed generation, dynamic pricing, energy efficiency, net metering,
  mergers, reliability, operational data, balancing authority — each a clean
  follow-up table.
- **Utility activity flags and multi-region membership** (has_generation, the
  RTO/ISO columns) that the 2012+ Utility_Data file carries — the abandoned
  combined branch included these; kept out here to keep `utility` a stable
  cross-era dimension.
