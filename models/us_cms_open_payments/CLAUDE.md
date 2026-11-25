# us_cms_open_payments — dataset context

Open Payments, the disclosure programme created by the Physician Payments Sunshine Act:
payments and other transfers of value from drug and medical device manufacturers and group
purchasing organizations to US physicians, non-physician practitioners and teaching
hospitals. Published by the **Centers for Medicare & Medicaid Services**.

US federal government work, published under `https://www.usa.gov/government-works` → licensed
as `cc0` in the backend, following `us_fed_fred` and `us_cfpb_hmda`. GCP id
`us_cms_open_payments` · org `centers_for_medicare_medicaid_services_cms` · backend slug
`open_payments`.

`code/layout.py` is the single source of truth for the table and column inventory. The
architecture CSVs, the dbt models, the metadata payloads and the cleaning SQL are all
generated from it, so a column is renamed in exactly one place. Only the architecture CSVs
are committed; the payloads `bulk_upsert_columns` wants are built in memory at registration
time, so there is no second copy to drift.

## Two schema eras, two publication regimes — and they do not line up

* **Schema eras.** PY 2013-2015 is the legacy schema (`Physician_*` columns; associated
  products listed as separate drug-name / NDC / device-name arrays). PY 2016 onwards is the
  modern schema (`Covered_Recipient_*`; five unified product slots of six fields each).
  Headers within each era are byte-identical.
* **Publication regimes.** PY 2013-2018 are *archived*: one ZIP per program year, each frozen
  by a different publication cycle, so the file names are not derivable. PY 2019-2025 are
  *current*: loose CSVs under a per-year prefix, restamped every cycle.

The boundaries differ — 2016-2018 are archived but modern. `constants.py` keeps the two
splits separate for that reason.

## Tables (23; `year` = INT64 partition unless noted)

Detail, one row per reported payment record:
- `general` 2016-2025, 91 cols · `general_legacy` 2013-2015, 66 cols
- `research` 2016-2025, 92 cols · `research_legacy` 2013-2015, 67 cols
- `research_principal_investigator` 2013-2025, 35 cols — the child table below
- `ownership` 2013-2025, 30 cols

Entities, one snapshot of the current publication cycle (no partition, coverage 2019-2025):
- `covered_recipient_profile` 32 · `teaching_hospital_profile` 12 ·
  `reporting_entity_profile` 9 · `provider_profile_mapping` 2

Summary reports, 2019-2025 only (CMS rebuilds them per cycle): twelve `summary_*` tables.

- `dicionario` — Portuguese gloss of every value in the dictionary-covered columns.

## Two deliberate reshapes

1. **Principal investigators are rows, not columns.** CMS repeats a ~28-column block five
   times per research payment, which is why the source `research` file is 252 columns wide.
   Lifting the blocks into `research_principal_investigator` leaves `research` at 92. The melt
   is verified against the source per slot: PY 2013 gives 384,428 / 2,712 / 1,366 / 706 / 569,
   matching the source counts exactly. Empty slots are dropped rather than emitted as null rows.
2. **The dashboard is stored long.** CMS publishes one column per program year, so a new year
   would change the table's schema. Stored as (year, metric, value) instead.

`ownership` and `research_principal_investigator` span both eras because the legacy columns
are a strict subset of the modern ones — PY 2013-2014 simply lack `Physician_NPI`, and
pre-2016 investigators lack covered recipient type and the 2nd-6th type/specialty slots.
`general` and `research` do **not**: their product blocks are structurally different, so the
eras stay in separate tables rather than being silently reconciled.

## Pipeline (`code/`, via `uv run --with duckdb`)

`constants.py` (URLs, both regimes) → `download.py` → `clean.py` → `run_all.py`
(download, clean and delete one program year at a time; peak disk ~10 GB rather than 105 GB)
→ `normalise_parquet.py` → `profile_data.py` → `gen_dicionario.py` → `upload.py`.

Generators: `gen_architecture.py` (writes `code/architecture/*.csv`), `gen_dbt.py`,
`gen_metadata_payloads.py` (a module — `payload(table)`, no output files).
Backend registration: `register_metadata.py` (drives the databasis MCP module directly).
Backend IDs in `code/discovered_ids.md`. `code/cms_dictionary/*.json` are CMS's own
published field definitions, kept as the provenance for the 651 descriptions in
`descriptions.py`; each file records the CMS metastore identifier it was fetched by.

Scratch lives in `~/Downloads/us_cms_open_payments_data`, never in the repo or Dropbox.

## Non-obvious things that will bite you

1. **A bare `NULL` is not a string.** Columns CMS had not started collecting in a given year
   are written as NULL literals, and duckdb types those INTEGER — producing parquet where
   `physician_npi` is INT32 in the 2013 partition and STRING in 2015. BigQuery builds the
   staging external table from one file's schema, so dbt then reads the whole table against
   the wrong type. `clean.py` casts them with `CAST(NULL AS VARCHAR)`; `normalise_parquet.py`
   fixes anything written before that and runs as a final pass.
2. **Do not set `strict_mode=false` on `read_csv`.** It forces duckdb's single-threaded
   reader, which rejects these files outright ("Parallel CSV Reader does not support a full
   read on this file"). The default reader parses all 13 years cleanly.
3. **Download in parallel byte ranges.** A single stream from download.cms.gov sustains about
   2 MB/s; eight ranges reach 15-39 MB/s. Across 60 GB that is seven hours versus one.
   `download.py` checks the assembled size against Content-Range before renaming into place.
4. **Dates need rewriting, not casting.** CMS writes MM/DD/YYYY, which `safe_cast(x as date)`
   turns into NULL. `clean.py` rewrites them to ISO; everything else stays untouched because
   staging is all-STRING by convention.
5. **State columns carry non-state codes.** AA, AE and AP (armed forces) plus a few malformed
   entries appear alongside real abbreviations — 792 rows out of 27M in `general_legacy`.
   `br_bd_diretorios_us.state` holds none of them, so the `directory_column` link is kept as
   metadata but **no dbt relationships test is emitted** for these columns.
6. **The entity tables are narrower in time than the payments.** CMS rebuilds the profile
   files per publication cycle and includes only recipients with a payment published in that
   cycle, so they cover 2019-2025 even though the detail tables reach back to 2013.
7. **Dictionary coverage is a translation, not a decode.** Open Payments stores readable
   English labels, so `chave` and the English label are the same string and `valor` carries
   the Portuguese meaning. Columns whose value set turned out to be free text
   (`interest_terms`, 468 values) or a classification system (the specialty family, 136-547
   NUCC taxonomy paths) were demoted by `profile_data.py` and are not dictionary-covered.
8. **`00_header.parquet` is load-bearing.** `upload.py` seeds a 0-row file that sorts ahead of
   `year=...` in the staging prefix. Without it the table-approve action reads the first
   multi-million-row partition into pandas just to learn column names, and OOMs the runner.

Refresh is annual, in June. A recurring Prefect pipeline is a separate step after the static
onboarding is verified.
