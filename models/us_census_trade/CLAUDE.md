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

## Country joins on ISO3, and why not ISO2

Schedule C publishes an ISO **alpha-2** column, but the country join is on
**alpha-3**, through a derived `country_iso3_code`. Two separate reasons, both
found the hard way:

1. **The directory's primary key is ISO3.** A `directory_column` that does not
   target the directory's primary key is **silently dropped on write** — the
   registration reports success and the link simply is not there. `pais`'s
   primary key is `sigla_pais_iso3`.
2. **`pais.sigla_iso2` is NULL for Namibia.** The literal `"NA"` was read as a
   null sentinel when that directory was built. An ISO2 join loses Namibia; the
   ISO3 join does not, because `sigla_iso3` is `NAM`.

Backend column names are **not** the BigQuery column names: the directory is
`sigla_pais_iso3` in the backend and `sigla_iso3` in BigQuery. The architecture's
`directory_column` needs the backend name; the dbt `relationships` test needs the
BigQuery one.

`country_iso2_code` is kept as the source-native value, with no directory link.
`pipelines/datasets/us_census_trade/country_iso3.json` maps 238 of the 241
Schedule C codes; Kosovo, the Gaza Strip and the West Bank are absent because
ISO 3166-1 assigns them no country code, so `country_iso3_code` is null there and
the relationship test skips them as nulls — no exclusion list needed.

The Namibia defect in `br_bd_diretorios_mundo` still affects other datasets
(`gb_eric_ess` has 11 ISO2 relationship tests) and is worth fixing separately,
but this dataset no longer depends on it.

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
