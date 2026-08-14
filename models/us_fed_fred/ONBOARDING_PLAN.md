# us_fed_fred — Onboarding Plan

**Source:** FRED (Federal Reserve Bank of St. Louis) REST API — https://api.stlouisfed.org/fred/
**Org slug:** `fred` (Federal Reserve Bank of St. Louis) — to be resolved/created in backend.
**License:** FRED Terms of Use. Ship only the **public-domain U.S.-federal-agency subset**
(see `SEED_SERIES.md`). Two filters at download: source allowlist + "Copyright"-in-notes
exclusion. Availability = free (`AllFree`); no BD Pro on v1 seed (revisit if a daily table
warrants a paywall in the pipeline step).

## Architecture (single dataset, 2 tables)
- **observation** (long): `year` (INT64, partition), `date` (DATE), `series_id` (STRING),
  `value` (FLOAT64). **Partition by `year`, cluster by `series_id`** — table can get very
  large; per-series scans stay cheap. Latest revision only (no ALFRED vintages in v1).
- **series** (catalog): one row per series — id, title, units, frequency, seasonal
  adjustment, source_name, release_name, observation_start/end, last_updated, notes.

Column language: English names (US dataset). Descriptions PT in architecture sheet,
EN/ES added at metadata step. Precedent: `world_wb_wdi` (long data + indicators catalog).

## Credential
`FRED_API_KEY` — **env var only**, never committed.
- Local onboarding run (steps 3–5): exported in the shell / gitignored `.env` at scratch dir.
- Recurring pipeline (step 12): injected on the deployed worker via **HashiCorp Vault**
  (`pipelines/utils/vault.py`), not a GitHub repo secret. Repo/CI secret is deploy-time only.

## Scratch data
`~/Downloads/us_fed_fred_data/` — `input/` (raw JSON), `output/` (partitioned parquet).
Never under the repo or Dropbox. Deleted at step 14.

## Step tracker
- [x] 0. Plan + seed list + architecture CSVs (this dir)
- [ ] 1. context — org/source URLs, coverage
- [x] 2. architecture — 2 Google Sheets created (Drive: Conjuntos/us_fed_fred/)
      - observation: https://docs.google.com/spreadsheets/d/1NxOTi3jwUZX8OH7bE1PwlAqlHrBb6CiJYcbCqquJecU/edit
      - series:      https://docs.google.com/spreadsheets/d/1tHbj-wuhXUKXemAOKBs6WkEUCPaLCecNYchy98L7TmQ/edit
- [x] 3+4. download + clean — 50 series kept, 0 restricted, 171,801 obs; all-STRING
      partitioned parquet at ~/Downloads/us_fed_fred_data/output/. License filters ran
      (source allowlist + Copyright-in-notes); excluded_series.csv empty.
- [x] 5. upload — dev staging OK (series 50, observation 171,801; all-STRING)
- [x] 6. dbt — built in dev: observation (partition year + cluster series_id, 171,801),
      series (50). setIamPolicy grant error is benign (dev SA lacks it; grants apply in prod).
- [x] 7. validate — dbt test 10/10 PASS (unique keys, not_null, both FK relationships)
- [x] 8. discover — IDs resolved on STAGING backend (dev backend 503 down). OL design
      mirrors world_wb_wdi: observation → [date, unknown(series)]; series → [unknown].
      License=cc0 (US federal public domain), availability=online, theme=economics, 10 tags.
- [x] 9. metadata — registered on staging (verified via get_dataset): org fred, 10 tags,
      observation (4 cols, OL date+unknown, coverage 1854-12→2026-08, cloud→basedosdados-dev),
      series (14 cols, OL unknown, cloud→basedosdados-dev). One raw source linked to both.
- [x] 9b. published on staging (preview). dataset id ea171847-2a6d-4b58-99c8-f6b34393dfdd
- [ ] [PAUSE — verification checkpoint: awaiting user "approved"]

- [ ] [PAUSE — verification checkpoint]
- [ ] 10. metadata --env prod (after approval)
- [ ] 11. PR
- [ ] 12. pipeline — recurring daily refresh (Prefect 3, modeled on us_bls_cpi)
- [ ] 13. publish prod (post-merge)
- [ ] 14. cleanup scratch data

## Reuse note
Cleaning transform (download_series, download_metadata, clean_all, write_partitioned)
lives in `pipelines/datasets/us_fed_fred/utils.py` at step 12 and is imported by
`models/us_fed_fred/code/` — do not duplicate. `fredapi`/`fredr` wrap the same endpoints;
we call the REST API directly with `requests` to avoid an extra dependency, matching the
normalized long schema those libraries produce.
