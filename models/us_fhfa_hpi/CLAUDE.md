# us_fhfa_hpi — locked onboarding context

Full plan: `ONBOARDING_PLAN.md`. This file is quick-resume context for any session.

**Dataset:** GCP `us_fhfa_hpi` · backend slug `house_price_index` · org `fhfa` · `cc0` (US public domain, 17 USC §105)
**Source:** FHFA House Price Index®, https://www.fhfa.gov/data/hpi/datasets
**Coverage:** master 1975Q1–2026Q2 quarterly and 1991M01–2026M06 monthly; annual developmental 1975–2025.
`www.fhfa.gov` serves the data files to plain HTTP clients — no User-Agent workaround needed.

## Locked decisions

1. **Two source products, eleven tables + `dicionario`.** The master file
   (`hpi_master.csv`, everything FHFA publishes monthly and quarterly) is split by
   geography level; the annual developmental indexes are one table per level.
   Splitting follows `us_bea`, gives each table one observation level, and keeps
   every column dense.
2. **Puerto Rico folds into `quarterly_state`.** Its `place_id` is `PR`, it is absent
   from the 51 rows at `level='State'` (50 states + DC), and it is a state equivalent.
   Its rows carry `index_type = 'developmental'`.
3. English column names; partition `year` INT64; metadata PT/EN/ES.
4. `index_nsa` / `index_sa` keep FHFA's own field names — standard terms, and they keep
   the annual and master tables commensurable.
5. **BD Pro per table:** `monthly_national` refreshes monthly →
   `PartBdpro(free_lag=6 months)`. Everything else is quarterly or annual → `AllFree`.

## Keys (all verified, 0 duplicates)

- master tables: `year, <period>, <place column>, index_type, index_flavor`
- annual tables: `year, <place column>`; `annual_national`: `year`
- `dicionario`: `id_tabela, nome_coluna, chave`

## Source quirks the code handles

1. **`note` is a literal tab on every row outside the metro tables.** Stripped to NULL.
   Only 451 metro rows carry a real footnote (0.31%), so `note` is in `ignore_values`
   for the null-proportion test.
2. **`rstderr` exists only for the expanded-data metropolitan series** (58,220 of
   145,480 rows), so `relative_standard_error` lives in `quarterly_metro` alone.
3. **`index_sa` is null wherever FHFA publishes no seasonally adjusted variant** —
   every all-transactions series, among others. 47–56% non-null by table.
4. **Annual workbooks carry a five-row title preamble**; the header is row 6. The
   census tract file ships as CSV with its own short column names, remapped onto the
   workbook headers so one architecture-driven rename covers every annual table.
5. **FHFA publishes annual rows with the index suppressed** (109,777 tract rows have a
   null `index_nsa`). Kept faithfully rather than dropped.
6. **`hpi_type` mixes index variant with geographic subset** — `non-metro` is the
   nonmetropolitan remainder of a state, `manufactured` is a property-type cut. Kept as
   the source publishes it, dictionary-covered.

## Directory links, and the four deliberate omissions

Linked: `year`→`…data_tempo.ano:ano`, `month`→`…data_tempo.mes:mes`,
`quarterly_state.state_abbreviation`→`…us.state:abbreviation`,
`annual_state.state_id`→`…us.state:id_state`,
`annual_county.county_id`→`…us.county:id_county` (vintage matches — CT planning
regions `091xx`, AK `02063`/`02066`).

Not linked, each for a checked reason:

| Column | Why |
|---|---|
| `quarterly_metro.cbsa_id` | 37 of 410 are Metropolitan **Division** codes; `cbsa_2023` carries CBSAs only (373 of 410 resolve) |
| `annual_cbsa.cbsa_id` | all 922 five-digit codes resolve, but 44 more are two-digit state FIPS standing for the state's non-CBSA remainder |
| `zip_code_3`, `zip_code_5` | USPS ZIP codes, not ZCTAs — `zcta_2020` is a different universe |
| `census_tract_id` | FHFA builds the tract index on 2010 tract boundaries (WP 16-04); only `census_tract_2020` exists |

## Naming conventions applied here

- **Table display names carry no em dash.** The geography goes in parentheses, singular,
  matching the table's observation level: `Índice anual (estado)`, `Índice trimestral
  (área metropolitana)`. The trilingual names are recorded in `code/table_metadata.json`,
  which is the reference when re-registering on another backend.
- **`state_abbreviation` precedes the finer geography identifier** it qualifies, in both
  the architecture and BigQuery: `annual_county` is `year, state_abbreviation, county_id,
  county_name, …` and `annual_tract` is `year, state_abbreviation, census_tract_id, …`.

## Where things live

- transform: `pipelines/datasets/us_fhfa_hpi/utils.py` (pure, shared with the pipeline)
- bootstrap: `code/clean_data.py` → `~/Downloads/us_fhfa_hpi_data/{input,output}`
- schema source of truth: `code/architecture/*.csv`; `code/build_dbt.py` regenerates the models
- upload: `code/upload.py` (dev only — prod tables come from table-approve on merge)

## Running dbt locally

`profiles.yml` reads the service account from `BD_SERVICE_ACCOUNT_DEV` and falls back to
`/credentials-dev/dev.json`, which only exists on the deployed worker. Locally:

```
BD_SERVICE_ACCOUNT_DEV="$HOME/.basedosdados/credentials/staging.json" uv run dbt run --select us_fhfa_hpi
```

The first parse of this repo takes ~13 minutes; later runs reuse `target/partial_parse.msgpack`.
