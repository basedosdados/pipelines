# world_bis_property_prices

BIS Selected residential property prices (dataset code WS_SPP). One LONG table
`price_index` built from a single bulk flat SDMX CSV. The BIS ships the full
history every release, so each pipeline run is a **full replace**
(`dump_mode="append"`, which replaces each same-named blob — never `overwrite`,
which would drop the materialized prod table). 57 economies; the four BIS
reference-area aggregates (World, advanced economies, emerging market economies,
euro area) are excluded at onboarding, so every row is keyed to an ISO3.

## Refresh cadence
- `47 15 19,20,21,22,23 * *` — 15:47 America/Sao_Paulo, days 19–23 every month.
  The BIS publishes quarterly; the poll no-ops until a new quarter lands.

Staging upload: dump mode `append`, source format `parquet`.

## Tables
| table | partition | materialization | coverage tier | cast columns |
|---|---|---|---|---|
| `price_index` | `year` (int64) | table | AllFree | 10 |

Quarterly → AllFree everywhere: the BD Pro rolling window applies only to tables
refreshed monthly or more often, so no Row Access Policies are issued. The poll
compares a `%Y-%m` string against the coverage (a YearQuarter column stored as
`MAX(DATE(year, quarter*3, 1))`), so `clean_all` reports the latest period as a
year-MONTH (`max_year_month`), not a year-quarter.

## Key columns
`year` (INT64, partition, FK `data_tempo.ano`) · `quarter` (INT64 1–4, matches
`au_abs_prices_inflation.dwelling_value` for a clean `USING(year, quarter)` join)
· `country_id` (STRING ISO3, FK `br_bd_diretorios_mundo.pais:sigla_iso3`) ·
`reference_area_code`/`reference_area_name` · `value_type` (Nominal/Real) ·
`measure` (index / year-on-year change) · `unit` · `bis_series_key`
(`Q.AU.N.628` verbatim) · `value` (FLOAT64, mixed unit → no measurement_unit).
Index levels are on the common 2010 = 100 base; cross-country **levels are not
comparable**, only growth rates and rebased indices.

## Where the code lives
- `pipelines/datasets/world_bis_property_prices/` — `constants.py` (URLs,
  ISO2→ISO3 map, table list), `utils.py` (pure download + cleaning transform),
  `tasks.py` (Prefect wrappers), `flows.py` (the flow + inline schedule).
- `models/world_bis_property_prices/` — dbt model and `schema.yml`. The
  architecture CSV under `code/architecture/` is the schema source of truth;
  `code/clean_data.py` imports the shared transform, `code/upload.py` loads dev,
  `code/build_columns_json.py` emits the metadata payload.

## Source
- https://data.bis.org/topics/RPP — bulk file
  https://data.bis.org/static/bulk/WS_SPP_csv_flat.zip (flat SDMX CSV).
- License: BIS Terms of Permitted Use of Statistics (attribution required).

## Scope note
Only the **selected** dataset (WS_SPP) is onboarded. The **detailed** dataset
(WS_DPP) is deferred: same terms and also all-Free in the public bulk file, but
347 series with mixed frequencies (Q/M/A/H), 72 base years and currency-level
series — it cannot form one clean comparable table. Re-onboard it as its own
dataset if the granular sub-national / by-type breakdowns are ever wanted.

Scratch data (never committed): `~/Downloads/world_bis_property_prices_data/`
(delete at step 14).
