# au_abs_national_accounts — onboarding plan & status

**Dataset:** Australian System of National Accounts (ABS catalogue **5204.0**)
**Source:** https://www.abs.gov.au/statistics/economy/national-accounts/australian-system-national-accounts/latest-release
**Org:** `au_abs` · **Licence:** CC BY 4.0 · **GCP dataset id / slug:** `au_abs_national_accounts`

## Shape (locked)

Long two-table design keyed on the ABS **Series ID** (mirrors FRED's series +
observations, and the `au_abs_community_profiles` precedent). Every one of the 72
ASNA workbooks shares one machine layout, so a single universal parser ingests
all of them.

| Table | Grain | Rows | Notes |
|---|---|---|---|
| `series` | one row per `series_id` | 4,454 | dictionary: description, unit, source_table_no/name, series_start/end |
| `observations` | `year` × `series_id` | 233,873 | long fact, partitioned by `year`; `value` FLOAT64, unit lives in `series` |

- **Frequency:** annual (each late October; next 2026-10-23). All tables `AllFree`
  (annual → no BD Pro rolling window).
- **Coverage:** FY 1959-60 → 2024-25 (`year` = period-**end** calendar year, e.g.
  FY2024-25 → 2025; plus `financial_year` STRING "2024-25").
- **Scope:** all 71 standard tables. **Table 14** (Market Sector Productivity)
  dropped — it is a growth-cycle summary with no Series IDs.
- **Dropped constant columns:** Series Type (Original), Data Type (DERIVED),
  Frequency (Annual) are constant across all ~5,190 series, so not stored.

## Files

```
code/
  download.py     fetch + extract the 72 xlsx (browser UA required) -> input/
  clean_data.py   universal parser: input xlsx -> output/series + output/observations
  upload.py       output/* -> basedosdados-dev staging (bd.Table)
  architecture/   series.csv, observations.csv  (source of truth for columns)
au_abs_national_accounts__series.sql
au_abs_national_accounts__observations.sql
schema.yml
```

`input/` and `output/` are gitignored (data never committed).

## Reproduce

```bash
uv run python models/au_abs_national_accounts/code/download.py
uv run python models/au_abs_national_accounts/code/clean_data.py \
    models/au_abs_national_accounts/input models/au_abs_national_accounts/output
GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/prod.json \
  uv run python models/au_abs_national_accounts/code/upload.py --env dev
```

## Status

- [x] 1–2. Architecture + universal parser (verified: 4,454 series / 233,873 obs;
  spot-checked GDP growth exact; 0 conflicts; download→clean reproducible)
- [x] 5. Uploaded to `basedosdados-dev` staging: `series` (4,454), `observations`
  (233,873). Stale `values` staging table removed after the rename.
- [x] 6. dbt models + `schema.yml` + `dbt_project.yml` entry written (NOT run yet).
- [ ] 7. **NEXT — dev dbt checks** (stopped here per request):
  ```bash
  GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/prod.json \
    uv run dbt run  --select au_abs_national_accounts --profiles-dir ~/.dbt --target dev
  GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/prod.json \
    uv run dbt test --select au_abs_national_accounts --profiles-dir ~/.dbt --target dev
  ```
- [ ] 8–9. Discover IDs + register dev metadata (dataset `under_review`; 2 tables,
  columns via `bulk_upsert_columns` with PT/EN/ES; OL linking; cloud tables;
  coverage 1960–2025; table Update). Then verification checkpoint.
- [ ] 10–13. Prod metadata → PR (needs `deploy-flow`? no — static onboarding) →
  merge → table-approve → verify → publish.
- [ ] 12 (optional). Annual Prefect refresh pipeline (poll late-October window).

## Open notes

- `series` is an unpartitioned dimension (no temporal column); coverage handling
  for it decided at the metadata step.
- Column descriptions need PT/EN/ES at metadata time; architecture CSV holds EN.
