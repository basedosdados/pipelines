# us_census_trade

U.S. Census Bureau (Foreign Trade Division) monthly merchandise trade, from the
International Trade timeseries API. GCP id `us_census_trade` · org `us_census` ·
backend slug `foreign_trade` · licence public domain, registered as `cc0`, the
closest slug the backend carries.

Seven tables at HS6 × partner country × place × month:

| Table | Endpoint | Place | Has quantity? |
|---|---|---|---|
| `import` | `imports/hs` | customs district | yes, plus duty |
| `export` | `exports/hs` | customs district | yes |
| `import_port` | `imports/porths` | port | no |
| `export_port` | `exports/porths` | port | no |
| `import_state` | `imports/statehs` | state | no |
| `export_state` | `exports/statehs` | state | no |
| `dicionario` | Schedule C + D | — | — |

## The API key gates everything

`api.census.gov` returns **HTTP 302 → "Missing Key"** for every anonymous
request. There is no anonymous tier any more. `CENSUS_API_KEY` resolves from the
environment first, then from Vault at secret path `us_census_trade`, key
`CENSUS_API_KEY` — the `us_bea` contract.

Because no key is available on the development machine, **no row of this dataset
has ever been downloaded locally**. Everything in the repo is built against the
documented API contract and validated against fabricated responses. The first
real read is the dev flow run, which is therefore both the data-landing step and
the correctness gate.

## Things that will be wrong if the API differs from the contract

These are the assumptions the dev run has to confirm. Each is enforced by a test
rather than trusted, so a wrong assumption fails loudly instead of shipping.

1. **`COMM_LVL` mixes aggregation levels.** The API returns HS2, HS4, HS6 and
   HS10 rows in one response. Filtered as a predicate and again client-side. If
   this filter were dropped, every total would inflate several-fold and nothing
   would raise.
2. **`SUMMARY_LVL` mixes detail with country groupings.** `CGP` rows aggregate
   their member countries. Filtered to `DET`.
3. **Imports carry `RP` (rate provision) and `CTY_SUBCODE`** as required
   predicates. If either varies within the declared grain, rows are summed onto
   it — with `min_count=1`, so an unpublished month stays NULL rather than
   becoming a published zero. The uniqueness test on the grain is what catches a
   grain that is finer than assumed.
4. **`DF` on exports is a dimension, not a filter.** 1 = domestic exports,
   2 = re-exports. Total exports is the sum of both.
5. **True-zero flags are not carried.** `*_FLAG` columns distinguish a true zero
   from suppression. They are not in the schema, so a 0 in a value column should
   not be read as certainly-zero.

## Revision policy drives the refresh window

Census revises year-to-date months at every release **and revises all previously
released data with the publication of April statistics**. So the pipeline
refreshes January of the previous year through the newest published month, every
run — not just the current year to date.

The refresh replaces only the affected `year=` partitions. This works because
`upload_to_gcs` replaces GCS objects by path and each year is exactly one file.
`dump_mode` must stay `"append"`: `"overwrite"` deletes the whole table and would
rebuild it from the window alone, erasing every earlier year.

## HS is not one code space

The WCO revises the Harmonized System every five years and the United States
adopts each revision. `hs_revision` records which applies: HS2007 for 2010–2011,
HS2012 for 2012–2016, HS2017 for 2017–2021, HS2022 from 2022.

Consequently `hs6_code` carries a `directory_column` link to
`br_bd_diretorios_comercio_internacional.sistema_harmonizado` but **no dbt
`relationships` test**: that directory is single-vintage, so codes retired in
earlier revisions would fail. Measure the miss rate on the dev run and add a
`custom_relationships` test with a measured tolerance.

## Known upstream defect: Namibia

`br_bd_diretorios_mundo.pais` has `sigla_iso2 = NULL` for Namibia (`sigla_iso3`
is `NAM`) — the literal `"NA"` was read as a null sentinel when that directory
was built. All 241 Schedule C ISO2 codes were checked; 237 join. The
`custom_relationships` test on `country_iso2_code` excludes `GZ`, `WE`, `KV`
(none are ISO 3166-1 countries) and `NA` (the directory defect). **Drop `NA`
from `code/build_dbt.py` once the directory is fixed.**

## Regenerating

```bash
python models/us_census_trade/code/build_architecture.py   # architecture CSVs
python models/us_census_trade/code/build_dbt.py            # .sql + schema.yml
uv run pre-commit run --files models/us_census_trade/*.sql models/us_census_trade/schema.yml
```

The architecture CSVs are the single source of truth. The API variables to
request are derived from each column's `original_name`, so adding a column means
editing `build_architecture.py` and regenerating — never hand-editing the `.sql`,
`schema.yml`, or the request list.

## Output organization

Cleaned parquet goes to `~/Downloads/us_census_trade_data/output/<table>/year=<YYYY>/data.parquet`
when run locally. On the worker it goes to a temp dir that is removed in a
`finally` block. Never under Dropbox or the repo.
