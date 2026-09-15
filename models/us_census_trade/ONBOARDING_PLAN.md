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

6. **Country joins on ISO3, though Schedule C publishes ISO2.** Two measured
   reasons. A `directory_column` that does not target the directory's primary
   key is silently dropped on write, and `pais`'s primary key is
   `sigla_pais_iso3`; and `pais.sigla_iso2` is NULL for Namibia, because the
   literal "NA" was read as a null sentinel when that directory was built. So
   `country_iso3_code` is derived through a committed 238-code map and carries
   the link, while `country_iso2_code` is kept as the source-native value with
   no link. Note also that backend column names differ from BigQuery ones —
   `sigla_pais_iso3` versus `sigla_iso3`. The native `country_code` is kept too.
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

---

## Backend metadata

Registered on **staging** by `code/register_metadata.py`.

| | |
|---|---|
| Backend slug | `foreign_trade` — `trade` is taken by CITES |
| Organization | `us_census` |
| Theme | `economics` |
| Tags | `comercio`, `importacao`, `exportacao`, `balanca_comercial`, `porto`, `transporte`, `frete` — all pre-existing, none created |
| Licence | `cc0`, the closest slug the backend carries for a US Government public-domain work; same choice as `us_census_lodes` |
| Status | `under_review` until the PR merges and the prod tables materialise |
| Raw data source | one, shared by all seven tables — a table with two sources cannot run a recurring pipeline |

**New shared vocabulary:** the entity `port` did not exist and was created under
the `spatial` category, alongside `state` and `country`. `customs` already
existed and is used for the district grain. Conflating the two would erase the
distinction the port tables exist for. Flagging it because it is shared
vocabulary, the same way a new tag would be.

**Not registered yet, deliberately:** Coverage, DateTimeRange and Update. They
need the real maximum month, which requires reading the API. `code/register_coverage.py`
does it after the first dev run, creating the free and pro Coverage pair that a
`part_bdpro` pipeline needs before its first armed run.

## Sequencing, and why it differs from the standard workflow

Steps 3–5 (download, clean, upload) normally run locally. They cannot: there is
no API key on this machine. So the data lands via the deployed dev flow run,
which is also the step-12 verification gate.

1. ✅ Design, architecture, transform, dbt models, pipeline, backend metadata
2. ⬜ **Provision `CENSUS_API_KEY` in Vault at secret path `us_census_trade`**
3. ⬜ PR with the **`deploy-flow`** label — without it the staging deploy is
   skipped and the job still reports `pass`
4. ⬜ Dev run, backfilling from 2010-01:
   `{"materialize_to_prod": false, "update_metadata": false, "force_run": true, "first_month": "2010-01"}`
   All four matter. The flow defaults to `materialize_to_prod=true,
   update_metadata=true`, and the metadata tasks are pinned `env="prod"` even
   from the dev pool, so a run triggered with `{}` writes prod data and prod
   metadata. Consider slicing by `tables` — six fact tables over sixteen years
   is a long single run.
5. ⬜ Measure: row counts per table, the real sparsity of every column, the
   `hs6_code` miss rate against the single-vintage HS directory. Correct
   `ignore_values` in `code/build_dbt.py` from measurements, not guesses
6. ⬜ `register_coverage.py --max-month <measured>`; publish the dataset on
   **staging** only; verification checkpoint
7. ⬜ Prod metadata, merge, table-approve materialises prod, verify, publish
8. ⬜ Arm the schedule in Django admin, watching the first armed run — it is the
   first-ever execution of the prod upload and of the Row Access Policies
9. ⬜ Delete `~/Downloads/us_census_trade_data/` if any local scratch was created
