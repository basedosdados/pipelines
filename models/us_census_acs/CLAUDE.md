# us_census_acs — American Community Survey (Census Bureau)

Onboarding of the U.S. Census Bureau's **American Community Survey (ACS)** to Data Basis.
Org `us_census`, GCP dataset id `us_census_acs`, backend slug `acs`.
**License: U.S. Public Domain** (17 U.S.C. §105). `has_sensitive_data = no`.

> **Status & step checklist live in [`ONBOARDING_PLAN.md`](ONBOARDING_PLAN.md).** Read it first.
> Cross-session decisions/gotchas also in memory `project_us_census_acs.md`.

## Review updates (2026-07-16) — English names + observation levels

Post-checkpoint review changes (all names English now):
- **`ano` → `year`** (partition col, all 11 tables). **Staging keeps the `ano=` hive path**; dbt renames on read via `STAGING_SRC` in `build_dbt.py` (`safe_cast(ano as int64) year`). No re-clean/re-upload — just `dbt run`.
- **`id_pais` → `id_country`** (nation). **`variables.variable_code` → `variables.code`** (profile FK columns stay `variable_code`; relationship test targets `variables.code`).
- **Observation levels** now link each entity to its identifying column(s) in the backend (via `update_column(observation_level_id=...)`): person→`serialno`+`sporder`, household→`serialno`, and each geo id column. Backend column names are **immutable** via `update_column` — rename = `bulk_upsert_columns` (create) + `delete_column` (drop old). `get_dataset` caps columns at ~200/table; use `reorder_columns` (returns full name→id map) to reach hidden ids (e.g. person `serialno`/`sporder`/`year`).
- **Coverage confirmed** through 2024 (the true frontier; ACS 1-yr 2025 → Sept 2026, 5-yr 2025 → Dec 2026). No change.
- **Status:** dbt rebuilt 11/13 with new names; `microdata_person`/`_household` BQ rename **pending BQ daily-quota reset** (background job re-runs after ~07:10 UTC). Backend (staging) metadata fully updated.

## Tables (13)

| Table | Shape | Grain / notes |
|-------|-------|---------------|
| `microdata_person` | wide | PUMS person records, 357 cols (hybrid schema) |
| `microdata_household` | wide | PUMS housing records, 267 cols |
| `data_profile_<level>` ×9 | long | DP02/03/04/05 melted; levels: nation, state, county, place, puma, cbsa, congressional_district, zcta, school_district |
| `variables` | catalog | decodes profile `variable_code` → profile/label/concept/unit (1,084 codes) |
| `dicionario` | DB-standard | value→label for PUMS coded columns |

- **Column names are English** (US dataset): partition col is **`year`** (not `ano`); nation geo id is **`id_country`** (not `id_pais`); the `variables` catalog key is **`code`** (not `variable_code`). NB: profile tables still carry a `variable_code` FK column that points to `variables.code`.
- **Staging keeps the ORIGINAL names** (`ano`, `id_pais`, `variable_code`) and the hive path is still `ano=`. The English names are produced as **dbt rename-on-read aliases** (`safe_cast(ano …) year`, `id_pais → id_country`, `variable_code → code` in the variables model) — see `STAGING_SRC`/`STAGING_SRC_TABLE` in `build_dbt.py`. This means the renames needed **no re-clean / re-upload**, only a dbt re-materialize.
- **Partition:** `year` (INT64 range) on microdata + profiles. `year` is the **hive path only** (dir `ano=YYYY`), NOT a parquet column (matches `us_ed_ipeds`). 5-year `year` = end year; `period` column (`1-year`/`5-year`) disambiguates.
- **Clustering (profiles):** `cluster_by` geography-first then `variable_code` (e.g. state `[id_state, variable_code]`, county `[id_state, id_county, variable_code]`).
- **Profile long schema:** `year, period, <geo ids>, variable_code, estimate, margin_of_error`. `estimate`/`margin_of_error` carry **blank `measurement_unit`** (varies by row → unit lives in `variables.unit`). MOE sentinels `-555555555` etc. → NULL. Melt emits `…E` and `…PE` codes as separate rows.

## Data locations (this machine)

- **Raw + parquet live OUTSIDE the repo** (repo is under Dropbox — never stage GBs here):
  `/Users/rdahis/acs_data/` → `pums/` (64 GB zips), `profiles/` (gzipped JSON), `output/` (partitioned parquet), `work/` (dict parse outputs).
- Census API key: `/Users/rdahis/acs_data/.census_api_key` (git-ignored; the API now requires a key for every request).

## Code (`code/`) — run order

| Script | Does |
|--------|------|
| `fetch_legacy_dicts.py` | download the **PDF-only** PUMS dicts (2005–2012 + the 2006–2010 5-year) and render them to `.txt` via pypdf, so `parse_pums_dict.py` can read them. Run **before** `parse_pums_dict.py` |
| `parse_pums_dict.py` | parse **CSV + TXT** PUMS dicts → `work/pums_vardict.json` + person/housing column unions (case-normalized) |
| `build_pums_architecture.py` | → `code/architecture/microdata_{person,household}.csv` (types from dict C/N; hybrid RENAME `ST→STATE,BDS→BDSP,RMS→RMSP,VAL→VALP`) |
| `build_profile_architecture.py` | → 9 profile + `variables` + `dicionario` architecture CSVs |
| `build_variables_catalog.py` | vars_*.json.gz → `output/variables/data.parquet` |
| `build_dicionario.py` | PUMS dicts → `output/dicionario/data.parquet` |
| `clean_pums.py <vintage>` | zip → partitioned parquet (chunked, no disk extract) |
| `clean_profiles.py <level>` | melt group() JSON → long parquet |
| `batch_fetch_zcta.py` | backfill ZCTA cells the API truncates (49-var chunks → reassemble) |
| `upload.py [tables]` | parquet → `basedosdados-dev.us_census_acs_staging` (has `chunk_size`; honors `ACS_OUTPUT_ROOT`) |
| `upload_person_resume.py` | resilient resumable upload for the 30 GB person table |
| `build_dbt.py` | → `../us_census_acs__*.sql` + `../schema.yml` |

## Commands

```bash
# clean (parallel over vintages / levels)
python3 code/clean_pums.py 2023_1yr
python3 code/clean_profiles.py state

# upload to dev (from pipelines worktree root)
GOOGLE_APPLICATION_CREDENTIALS=~/.basedosdados/credentials/prod.json \
  uv run python models/us_census_acs/code/upload.py <table>

# dbt (default target = dev)
BD_SERVICE_ACCOUNT_DEV=~/.basedosdados/credentials/prod.json \
  uv run dbt run  --select us_census_acs
  uv run dbt test --select us_census_acs
```

## Gotchas & lessons (hard-won)

- **GCS SSL drops on large uploads:** the 30 GB `microdata_person` monolithic upload dies mid-file on `SSLEOFError`, and `tb.create(if_storage_data_exists="replace")` restarts from zero. Fix = `upload_person_resume.py`: `bd.Storage.upload(if_exists="pass", chunk_size=50MB)` in a retry loop (skips already-uploaded files → progress **accumulates**), then `tb.create(if_storage_data_exists="pass")`.
- **Never run person upload + ZCTA downloads concurrently** — bandwidth contention causes SSL drops *and* starves ZCTA (can't finish a 300 MB response in the timeout → infinite retry). **Serialize.**
- **ZCTA `group(DP02)` truncates server-side** (~300 MB response for 33k ZCTAs). Fix = `batch_fetch_zcta.py` (49-variable chunks, reassemble to group() JSON). Not a bandwidth issue.
- **Download validation must check JSON *completeness*** (ends with `]`), not just `gzip -t` — a truncated response is a valid gzip of incomplete JSON. (193 profile files were silently corrupt this way.)
- **`ano` partition = path only**, never a parquet column (else pyarrow hive-read conflicts).
- **PUMS schema drift:** old years use lowercase headers + different names (`ST`→`STATE`); union is case-normalized; vintage code vars (`OCCP02/10/12`, `PUMA00`) kept separate (hybrid).
- **PUMS dictionaries come in three formats, and you need all three.** CSV exists only from 2013 (and is the only source of the `C|N` type); TXT from 2009; **2005–2012 is PDF-only**. Parsing CSV alone leaves 58 vintage columns undefined. `fetch_legacy_dicts.py` renders the PDFs to `.txt` (identical layout), and `parse_pums_dict.py` reads CSV+TXT. Never render a PDF over a year the Census publishes as real `.txt` (2013+) — it would clobber the authoritative file.
- **Do not guess a legacy variable's meaning from its name.** Two that name-inference got materially wrong before the dicts were parsed: **`MODEM` is "Cable Internet service"** (not a dial-up modem) and **`SSPA` is "Same sex spouse recode"** (not Social Security, despite the `SSP` sibling). Where a column lives is also non-obvious: `INDP02/07`+`NAICSP02/07` are defined **only** in the 2006-2010 5-year dict (they appear in the 2010/2011 5-year files, which straddle the 2002→2007 industry-code change), and `SSPA` **only** in the 2012 1-year dict.
- **TXT/PDF dicts carry no `C|N` type**, so vars found only there default to `C` (STRING) — which is what the architecture already assumed. That default is load-bearing: it keeps adding the legacy dicts a *description-only* change with **zero BigQuery type churn** (no re-materialization).
- **Value-label lines are indented in the published `.txt` but flush-left in the PDF render.** The label regex must not require the indent, or `n_labels` silently reads 0 and `covered_by_dictionary` flips to `no` on label-bearing columns.
- **grep on the Census dicts needs `LC_ALL=C grep -a`** — several are "Non-ISO extended-ASCII", and macOS grep treats them as binary and prints *nothing* (not even `0`), which reads exactly like "the variable is absent". Always run a known-present control (e.g. `RT`, `SERIALNO`) before concluding a variable is missing.
- **PUMA/CD/ZCTA have no directory FK** (vintage varies by year — D6/D6b); carried as STRING codes.
- **The sandbox blocks `rm -rf` / `find -delete` / `os.remove`** even for own scratch files — use `mv` to move aside instead.
- **Backend column RENAME = delete + recreate.** `update_column` CANNOT rename a column (its `column_name` is ignored for renaming); it only sets flags/OL/directory. To rename: `bulk_upsert_columns` (create new name) then `delete_column` (old). `update_column` sets `is_partition` + `observation_level_id` afterward.
- **Observation-level ↔ column link** is a column-side FK set via `update_column(observation_level_id=…)` (the entity OL itself carries no column list). Identifying cols linked: profiles → their geo id; puma → `id_state`+`puma`; household → `serialno`; person → `serialno`+`sporder`.
- **`get_dataset` caps columns at ~200/table (alphabetical).** On `microdata_person` (357) and `microdata_household` (267) the tail cols (`serialno`,`sporder`,`year`) aren't readable, so 3 backend items stay unset on staging: `is_partition` on both microdata `year`, and person `serialno`/`sporder`→OL. `bulk_upsert_columns` (uncapped, name-matched) is the workaround for anything settable without an id.
- **BQ daily query quota (`QueryUsagePerDay`)** can block the big microdata `dbt run` (`microdata_person`/`_household` scan ~30 GB each). Re-run those two models after the quota resets (or gets raised): `uv run dbt run --select us_census_acs__microdata_person us_census_acs__microdata_household`.
- Credential `~/.basedosdados/credentials/prod.json` is what works for **dev** (`GOOGLE_APPLICATION_CREDENTIALS` for upload, `BD_SERVICE_ACCOUNT_DEV` for dbt).
