# us_treasury_usaspending

USAspending.gov federal award transactions — every US federal contract and financial
assistance action, published under the DATA Act of 2014. Public domain (CC0).
225,131,794 rows across FY2007–FY2026 at onboarding. The source rebuilds its bulk
archive **monthly**, so only the open fiscal year is refreshed; earlier years are frozen
and never re-downloaded. Staging upload is `append` (the run adds the current fiscal
year's partition), source format `parquet`.

## Refresh cadence
- `20 17 8,9,10,11,12 * *` — 17:20 America/Sao_Paulo, days 8–12 monthly

The archive is stamped with a build **date** and no time, so the hour carries no
meaning from the source. It is 17:20 rather than the usual 16:xx because every
five-minute slot in hour 16 was taken.

## Tables
| table | partition | materialization | coverage tier | columns |
|---|---|---|---|---|
| `contract_transaction` | `fiscal_year` (int64) | table | PartBdpro, 6-month free lag | 297 |
| `assistance_transaction` | `fiscal_year` (int64) | table | PartBdpro, 6-month free lag | 112 |
| `dicionario` | — | table | AllFree | 5 |

## Where the code lives
- `pipelines/datasets/us_treasury_usaspending/` — `constants.py` (URLs, table list),
  `utils.py` (pure download + cleaning transform), `tasks.py` (Prefect wrappers),
  `flows.py` (the flow + its inline schedule).
- `models/us_treasury_usaspending/` — dbt models and `schema.yml`, both **generated**
  by `code/build_dbt.py`. The architecture CSVs under `code/architecture/` are the
  schema source of truth; `build_architecture.py` regenerates them from the DATA Act
  element dictionary and the output is byte-stable, so a diff means a real change.

## Source
- https://www.usaspending.gov/download_center/award_data_archive (bulk, primary)
- https://api.usaspending.gov/ (element dictionary, current archive version)

## Source quirks — none of these are documented upstream
1. **The Contracts files invert 24 code/label pairs.** `action_type_code` holds
   "OTHER ADMINISTRATIVE ACTION" while `action_type` holds `M`; Assistance is
   consistent. Column names cannot be trusted here, so orientation is *measured* by
   `code/code_label_orientation.py`, which scores each column's values against the
   published domain's keys versus its labels. Re-run it if the source adds columns.
2. **County FIPS arrives float-corrupted**: Ohio/Franklin `39049` reads as `3949.0`
   — the leading zero of the county part is lost and `.0` gained. Repaired in the dbt
   model. There is deliberately **no** `relationships` test on county: state-wide
   aggregates use an `<state>000` sentinel and retired codes (pre-2022 Connecticut,
   Dade, Ormsby NV) legitimately do not resolve.
3. **Quoted values contain embedded newlines from FY2017 on.** pyarrow aborts the file
   without `ParseOptions(newlines_in_values=True)`.
4. **Booleans are stored lowercase** (`f`/`t`) while the published domain writes
   `F`/`T`, so the dictionary keys follow the data, not the spec.
5. `period_of_performance_potential_end_date` reaches the year **3036** — a filer
   typo. It does not touch coverage, which is computed from `action_date`.

`files.usaspending.gov` rate-limits hard: use **1–2 download streams, never more**
(eight drew a total refusal of GETs from this IP that outlasted the burst by ~25
minutes). `curl -C -` is unsafe here — on a 200-instead-of-206 it truncates and
restarts — so the downloader uses an explicit `Range` header and rolls back on 200.

## Design notes
Both transaction tables are high-frequency by Data Basis' rule, so each carries the
BD Pro rolling window: the most recent 6 months are pro-only, everything older is free.
The `dicionario` has no date column and stays fully free.

`DATE` columns cast as `date(safe_cast(col as datetime))`, not
`safe_cast(col as date)`. The archive ships some of them as full timestamps
(`2027-06-15 07:20:58`), which the plain DATE cast rejects — returning NULL rather than
raising, so a column can arrive 100% empty with every test green. That happened to
`period_of_performance_potential_end_date` across all 96,976,021 contract rows.

`not_null_proportion_multiple_columns` is scoped with
`where: __most_recent_fiscal_year__`. Unscoped it sums a CASE expression over every
column of both tables — 364 GB per pass, enough to exhaust the project-wide daily query
quota in one go, and it runs at `dbt compile`, not just `dbt test`. Its exemptions are
unioned across FY2023–FY2026 (`code/null_proportions.py --from-parquet --fiscal-year
2023 2024 2025 2026`) so the moving scope does not make the list churn.

**Measure exemptions from the raw parquet, never from the built table.** Reading the
parquet footers costs nothing (`null_count` is metadata) and keeps the two sides
independent — which is the only reason the silent DATE-cast loss above was ever caught.

## Operating reminders
- A `COMPLETED` run is not proof of an ingest: the source poll returns early and still
  completes. Read the logs, or check whether coverage moved.
- The poll compares the source's *coverage date* against `Table.Update.latest`, a
  *wall clock*, so a freshly-onboarded table can no-op until the source's stamp
  overtakes it. Watch the first armed runs.
- The dev materialization runs only when `materialize_to_prod=False` — that is the
  pre-arm validation path; an armed run goes straight to prod.
- Validate with
  `{"materialize_to_prod": false, "update_metadata": false, "force_run": true}`
  on the dev pool, and remember the PR needs the `deploy-flow` label — which only
  deploys a PR that changes `flows.py`.
- The staging dataset needs `allUsers` dataViewer or the worker cannot read the
  external tables (`403 … bigquery.tables.get denied … or it may not exist`).
  `code/upload_staging.py --publicize-only` grants it; run it with
  `GOOGLE_APPLICATION_CREDENTIALS` pointing at the service account that owns the
  dataset, since `bq` authenticates through gcloud instead and misleads you into
  thinking an org admin is required.

<!-- Generated from constants.py / flows.py / the dbt models. Extend by hand with
     source-specific gotchas as they are discovered. -->
