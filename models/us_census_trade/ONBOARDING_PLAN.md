# us_census_trade — onboarding plan

U.S. Census Bureau (Foreign Trade Division) monthly merchandise trade statistics,
from the International Trade timeseries API.

Status: DESIGN. No data downloaded yet — see "The API key blocks local work".

## Source

| | |
|---|---|
| API root | `https://api.census.gov/data/timeseries/intltrade/` |
| Coverage | **2010-01 to present** (Census: "statistics from January 2010 to present") |
| Release | monthly, on press-release day, ~5 weeks after the reference month |
| Revision | year-to-date months revised each release; **all previously released data revised annually with the publication of April statistics** |
| Licence | U.S. Government work — public domain (17 U.S.C. §105). API ToS imposes no redistribution limit and no non-commercial clause |
| Attribution | ToS requires the notice: "This product uses the Census Bureau Data API but is not endorsed or certified by the Census Bureau" — recorded on the raw data source |
| Code lists | Schedule C countries `foreign-trade/schedules/c/country.txt` (code → name → ISO2), Schedule D districts/ports `schedules/d/dist.txt` — both public, no key |

### The API key blocks local work

`api.census.gov` now returns **HTTP 302 → "Missing Key"** for every anonymous
request; the free-tier anonymous allowance is gone. The key that
`models/us_census_acs` used (`/Users/rdahis/acs_data/.census_api_key`) no longer
exists on this machine, and no `CENSUS_API_KEY` name appears in the sealed
secrets under `iac/k8s/`.

The `iac` Prefect agents mount `vault-credentials` (`VAULT_ADDRESS`,
`VAULT_TOKEN`), so this pipeline follows the **`us_bea` contract**:
`CENSUS_API_KEY` from the environment when present, otherwise Vault at secret
path `us_census_trade`, key `CENSUS_API_KEY`.

**Consequence for ordering.** Steps 3–5 of the onboarding workflow (download,
clean, upload) normally run locally. They cannot here. The data lands via the
**deployed dev flow run** instead, which is also the step-12 verification gate.
So the order is: design → architecture → code → dbt → PR + `deploy-flow` →
dev run lands data in `basedosdados-dev` → measure → metadata → checkpoint.

## Tables (7)

Grain is HS6 × partner country × place × month, on three place dimensions.

| Table | Endpoint | Place | Measures |
|---|---|---|---|
| `import` | `imports/hs` | customs district | general + consumption value, CIF, duty, quantity ×2, mode value/weight |
| `export` | `exports/hs` | customs district | total value, quantity ×2, mode value/weight, domestic/foreign split |
| `import_port` | `imports/porths` | port | general value, mode value/weight — **no quantity, no consumption** |
| `export_port` | `exports/porths` | port | total value, mode value/weight — **no quantity** |
| `import_state` | `imports/statehs` | state | general + consumption value, mode value/weight — **no quantity** |
| `export_state` | `exports/statehs` | state | total value, mode value/weight — **no quantity** |
| `dicionario` | derived | — | dbt model over the facts, per house rule |

Quantity and duty exist **only** on the district-level `hs` endpoints. That is a
property of the source, not an omission.

## Decisions, and where they depart from the brief

1. **Commodity detail: HS6.** Decided with the user. The API pre-aggregates via
   `COMM_LVL`, so HS6 is served directly — no roll-up from HS10 by us, and no
   risk of us mis-summing suppressed cells. HS6 is the level BACI carries, so
   the two datasets join product-for-product. HS10 stays available as a later
   table if demand appears.

2. **HS is multi-vintage, and the column records which.** The WCO revises HS
   every five years and the US adopts each revision, so the code space is not
   continuous across 2010–2026. Every fact table carries `hs_revision`
   (`HS2007` 2010–2011, `HS2012` 2012–2016, `HS2017` 2017–2021, `HS2022` 2022–).
   `hs6_code` is STRING — leading zeros are meaningful and arithmetic on an HS
   code is meaningless.

3. **No `transport_mode` column — the source is wide, not long.** The brief
   asked for transport mode as a dictionary-covered STRING. The API does not
   carry mode as a dimension; it carries parallel measure columns per mode
   (`AIR_*`, `VES_*`, `CNT_*`). Reshaping to long would invent rows the source
   does not publish and would break the value identity, so the mode columns stay
   wide. Note also that Census reports shipping **weight** for air and vessel
   only — land-mode shipments have no weight, so air + vessel weight is not a
   total.

4. **Import and export measures are not symmetric.** The brief specified
   "general and consumption value" for both. Only imports have that pair.
   Exports have a single value (`ALL_VAL_MO`) plus a domestic/foreign dimension
   (`DF`: domestic exports vs re-exports), which is kept as a column.

5. **Refresh window covers the annual revision, not just year-to-date.** The
   brief assumed trailing months of the current year. Census revises *all*
   previously released data with the April release, so the pipeline refreshes
   **January of the previous year through the newest month** on every run. The
   refresh replaces only the affected `year` partitions — never the whole table.

6. **Country joins on ISO2, not ISO3.** Schedule C ships an ISO2 column, so
   ISO2 is the zero-inference join to `br_bd_diretorios_mundo.pais:sigla_iso2`
   (precedent: `gb_eric_ess`). The native `country_code` is kept alongside it.
   `SUMMARY_LVL='DET'` filters out the country *groupings* (OPEC, EU, …), which
   would otherwise double-count against their members.

7. **`hs6_code` gets no hard dbt `relationships` test at first.** The only HS
   directory in the repo, `br_bd_diretorios_comercio_internacional.sistema_harmonizado`,
   is single-vintage, so codes retired in earlier revisions would fail. The
   backend `directory_column` link is still declared. The dev run measures the
   miss rate, and the test is added with a measured tolerance after.

## Correctness traps to verify on the dev run

These decide whether the numbers are right, and none can be checked without the key.

- `COMM_LVL` returns HS2/HS4/HS6/HS10 rows **in the same response**. Must filter
  to `HS6` or every total is inflated several-fold.
- `SUMMARY_LVL` = `DET` vs `CGP` (country grouping). Must filter to `DET`.
- Imports carry `RP` (rate provision) and `CTY_SUBCODE` as required predicates.
  If they vary within a key, the grain is finer than assumed and the uniqueness
  test will catch it.
- Exports carry `DF` (domestic/foreign) — a genuine dimension, kept.
- `*_FLAG` columns mark true zeros vs suppression. A bare 0 is not a true 0.

Each is enforced by a `dbt_utils.unique_combination_of_columns` test on the
declared grain, so a wrong assumption fails the dev run rather than shipping.

## BD Pro

All six fact tables refresh monthly, so per the house business rule each is
`PartBdpro(free_lag=6 months)`; `dicionario` takes no coverage spec. This needs
a **pro Coverage (`is_closed=True`) created at onboarding** or the first armed
run hard-fails at `assert_coverage_topology`.

## Positioning

Complementary to the already-onboarded `world_cepii_baci`, not a duplicate:
BACI is the reconciled annual world panel at HS6; this is the US national
source at monthly frequency with customs district and port detail that BACI
does not carry. The dataset description must say this explicitly.
Commercial angle: supply-chain and trade analytics.
