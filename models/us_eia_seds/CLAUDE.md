# us_eia_seds — design notes

U.S. Energy Information Administration **State Energy Data System (SEDS)**: the
complete state-by-state series of energy consumption, prices, expenditures, and
related indicators across every fuel, 1960 to the latest complete year. Source,
cleaning code and dbt models live under this directory and
`pipelines/datasets/us_eia_seds/`.

## Scope decision — why a separate dataset, and why split from us_eia_consumption

SEDS was deferred by the `us_eia_electricity` onboarding as "a different
publication entirely, and the natural `us_eia_seds`" (see that dataset's
`CLAUDE.md`, *What is deferred*). It covers **every fuel**, so it cannot sit under
a name scoped to electricity.

It was also split out from `us_eia_consumption` (the EIA-861 electricity retail /
demand-side dataset built in the same effort): SEDS is a distinct publication with
its own file, its own grain (state × year × series, no utility), and its own
annual full-restate refresh. The two share only the EIA organization. The user
chose the split explicitly; the alternative (SEDS as a third table under
`us_eia_consumption`) is recorded here as the overridable minority view.

## Source

One long file plus a codes workbook. No archaeology, no per-year layout drift.

| file | url | what |
|---|---|---|
| `Complete_SEDS.csv` | <https://www.eia.gov/state/seds/CDF/Complete_SEDS.csv> | the entire system, long: `Data_Status, MSN, StateCode, Year, Data` |
| `Codes_and_Descriptions.xlsx` | <https://www.eia.gov/state/seds/CDF/Codes_and_Descriptions.xlsx> | MSN → description + unit (968 series); state codes (54) |

**2,574,644 rows**, 1960–2024, single vintage stamp `2024F`. Released each June
(2024 estimates released 2026-06; next 2027-06).

Licence: **US Government public domain.** EIA publications are not subject to
copyright (<https://www.eia.gov/about/copyrights_reuse.php>). Registered `cc0`,
the house mapping for a US federal source, matching `us_eia_electricity`.

## The one table

`seds_consumption` — LONG, one row per (state, year, MSN).

| column | type | notes |
|---|---|---|
| `year` | INT64 | partition; dir `br_bd_diretorios_data_tempo.ano` |
| `state_id` | STRING | FIPS via `br_bd_diretorios_us.state`; null for the non-state aggregates |
| `state_code` | STRING | raw SEDS 2-char code, dict-covered — 50 states, DC, `US` (national), `X3`/`X5` (federal offshore Gulf / West Coast) |
| `msn` | STRING | 5-char Mnemonic Series Name, dict-covered (full description in `dicionario`) |
| `measure_type` | STRING | derived from MSN char 5, dict-covered — consumption (Btu), consumption (physical), price, expenditure, CO2 emissions, electricity, capacity, conversion factor, number, other |
| `value` | FLOAT64 | **carries no `measurement_unit`** — units differ per MSN; observations say so and point at `measurement_unit` |
| `measurement_unit` | STRING | the unit of `value` for this MSN (Billion Btu, Thousand barrels, Dollars per million Btu, Million dollars, …) |
| `data_status` | STRING | vintage/status stamp, dict-covered (e.g. `2024F` = 2024 final) |

### MSN is the coded dimension, not fabricated fuel/sector splits

The MSN is a five-character code: chars 1–2 = energy source, 3–4 = sector, 5 =
measure/unit type. **Only the full MSN carries an authoritative label** in EIA's
codes workbook — there is no published lookup for the 1–2 or 3–4 substrings, and
the 3–4 field takes 74 fuel-specific values that are not a clean sector list. So
this dataset keeps `msn` as the coded dimension (full description in `dicionario`)
rather than inventing `energy_source_code` / `sector_code` columns with
home-made labels, which would violate the house rule that `covered_by_dictionary`
requires a real value→label set. `measure_type` (char 5) is the one component
that *does* decompose cleanly and is exposed as its own dict-covered column.
Adding a fuel/sector decomposition later, from EIA's series-structure
documentation, is a clean follow-up.

### The value column mixes units on purpose

SEDS is one long series stacking consumption (in Btu and in physical units),
prices, expenditures, CO2 emissions, capacity, degree days and conversion
factors. Their units genuinely differ per MSN, so a single column-level
`measurement_unit` would be wrong for most rows. This follows the
`us_eia_electricity` `fuel_receipts_costs` precedent exactly: the numeric column
carries no unit, the per-row unit lives beside it, and the observations explain.

## Layout

Mirrors `us_eia_electricity` (architecture CSV = schema source of truth;
`gen_*.py` write the dbt models and backend payloads from it; the transform lives
in `pipelines/datasets/us_eia_seds/utils.py` and is imported by
`code/common.py`). Scratch data → `~/Downloads/us_eia_seds_data/`
(`US_EIA_SEDS_DATA_DIR`), never in the repo or Dropbox.

## Recurring pipeline

Annual. SEDS **restates the whole 1960–latest series on every release**, so the
refresh **replaces** the table rather than appending — the transform rebuilds
every year partition from the newest `Complete_SEDS.csv`, which makes a
double-count structurally impossible (the same discipline `us_eia_electricity`
uses per report year). Annual cadence → `AllFree`, no BD-pro paywall.
