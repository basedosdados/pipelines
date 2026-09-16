# world_bis_property_prices — Onboarding Plan

BIS **Selected** residential property prices (dataset code `WS_SPP`), one LONG
table `price_index`, quarterly, with a recurring quarterly Prefect pipeline.

## Source

- BIS Data Portal bulk download: `https://data.bis.org/static/bulk/WS_SPP_csv_flat.zip`
  (flat/long SDMX CSV, whole dataset in one file).
- Topic page: `https://data.bis.org/topics/RPP`.
- License: **BIS Terms of Permitted Use of Statistics (Attribution Required)** —
  reuse permitted provided the BIS is cited as the source.

## Selected vs Detailed — scope decision

The BIS publishes two residential property price datasets:

- `WS_SPP` (**selected**) — headline national series, uniform: all quarterly, a
  single common **2010 = 100** base, Nominal/Real × Index/YoY, 57 economies (the
  4 BIS aggregates are dropped at onboarding). Every observation is `OBS_CONF = Free`.
- `WS_DPP` (**detailed**) — 347 series across 15 real-estate types × 13 covered
  areas, **mixed frequencies** (Q/M/A/H), **72 different base years**, some in
  currency levels (EUR, PLN). Also entirely `Free` / `All users` in the public
  bulk file (the BIS strips any source-restricted series before publishing it).

**Both are redistributable** under the same attribution terms. We ship **selected
only**: the detailed set cannot become one clean comparable table without
per-series base-year and frequency normalization, and the scoped use (a long
Australian house-price index spliced with ABS 6432.0) needs only the headline
national index. The detailed set is deferred, re-onboardable later as its own
dataset if the granular breakdowns are wanted.

## Table `price_index` (LONG)

One row per (economy, quarter, value type, statistic). 33,976 rows, 57
economies, 1927-Q1 → 2026-Q1. Australia present 1970-Q1 → 2026-Q1. The four BIS
reference-area aggregates (World, advanced economies, emerging market economies,
euro area) are excluded — every row is an individual economy keyed to an ISO3.

| column | type | notes |
|---|---|---|
| `year` | INT64 (partition) | FK `br_bd_diretorios_data_tempo.ano` |
| `quarter` | INT64 (1–4) | matches `au_abs_prices_inflation.dwelling_value` (year+quarter both INT64) for a clean `USING(year, quarter)` join |
| `country_id` | STRING (ISO3) | FK `br_bd_diretorios_mundo.pais:sigla_iso3`; always populated |
| `reference_area_code` | STRING | BIS ISO2 code verbatim (`AU`, `US`, …) |
| `reference_area_name` | STRING | BIS label |
| `value_type` | STRING | `Nominal` / `Real` |
| `measure` | STRING | `index` / `year-on-year change` |
| `unit` | STRING | BIS UNIT_MEASURE label (`Index, 2010 = 100` / `Year-on-year changes, in per cent`) |
| `bis_series_key` | STRING | canonical BIS key `Q.AU.N.628`, verbatim, for traceability |
| `value` | FLOAT64 | mixed unit by row (index points vs %) → no fixed measurement_unit; the standard LONG exception |

**Cross-country levels are not comparable** (national sources/methods differ) —
only growth rates and rebased indices are. Stated in the dataset/table
descriptions.

## Pipeline (quarterly)

The BIS re-publishes RPP quarterly. `pipelines/datasets/world_bis_property_prices/`
holds the shared transform (`utils.py`, imported by the bootstrap) plus
`tasks.py`/`flows.py`. Refresh cadence is quarterly, so the table stays
**AllFree** (BD-Pro rolling window applies only to monthly-or-more-often tables).
Poll on the latest `TIME_PERIOD` (`YYYY-Qn`).

## Steps

1. Architecture CSV (`code/architecture/price_index.csv`) — schema source of truth.
2. Download + clean (`code/clean_data.py` → `utils.clean_all`) → all-STRING
   partitioned parquet under `~/Downloads/world_bis_property_prices_data/`.
3. Upload to dev (`code/upload.py --env dev`).
4. dbt `run` + `test` (dev).
5. Register metadata in dev (`bulk_upsert_columns` from `code/columns_json/`),
   publish dev/staging.
6. Verification checkpoint → prod metadata → PR → merge/materialize → verify →
   publish prod.
7. Recurring quarterly pipeline (`flows.py`), dev run, then arm.
