# au_abs_productivity — onboarding plan & status

**Dataset:** Estimates of Industry Multifactor Productivity (ABS catalogue **5260.0.55.002**)
**Source:** https://www.abs.gov.au/statistics/industry/industry-overview/estimates-industry-multifactor-productivity/latest-release
**Org:** `abs` · **Licence:** CC BY 4.0 · **GCP dataset id:** `au_abs_productivity` · **backend slug:** `productivity`

## Shape (locked)

Three tables. A long `indicator` + fact design, because the 42 source tables are
heterogeneous, but with industry, state, labour-input basis and unit **parsed into
real columns** so the 16 market-sector industries and 8 states are queryable
without string matching.

| Table | Grain | Rows | Notes |
|---|---|---|---|
| `indicator` | one row per indicator | 1,808 | dimension; `indicator_id` = sha1(table_no, section, item_path)[:16] |
| `observations` | `year` × `indicator_id` | 56,849 | long annual fact, partitioned by `year` |
| `growth_cycles` | `period` × `indicator_id` | 632 | tables 3, 5 and 26 — peak-to-peak cycles |

- **Frequency:** annual. The 2024-25 issue was released 2026-02-06; next 2027-02-19.
- **Coverage:** FY1973-74 → 2024-25 (`year` = period-**end** calendar year, so
  FY2024-25 → 2025). Market-sector aggregate from 1973-74, by industry from
  1989-90, by state from 1994-95.
- **Scope:** all 42 tables — standard (1–19), experimental industry (20–26) and
  experimental state (27–42). `table_group` records which.
- **All free.** Annual, so no BD Pro rolling window.

## Why this needed its own parser

This is **not** an ABS time-series workbook. Unlike `au_abs_national_accounts`
(5204.0), it has no Series IDs and its tables are hand-formatted for reading, so
the ASNA parser does not transfer. Three source conventions, each checked against
all 42 tables:

1. **Indentation nests, in non-breaking spaces** (U+00A0), two per level. A row
   with no numeric cells is a heading.
2. **Two logical levels share indent 0** and nothing in the file separates them.
   Table 2 puts "Indexes of productivity and related measures" and its sub-group
   "Productivity indexes" at the same indent, with no blank row between; bolding
   is not a signal either, since the same sub-group label is bold in the first
   block and not in the second. Collapsing the two levels maps the index *levels*
   and their *percentage changes* onto one key. The sub-groups are therefore named
   in `NEST_AT_ROOT`, derived from all 107 distinct indent-0 headings in the
   release.
3. **Tables 21–23 carry no indentation at all** and mark the component a block of
   industries belongs to with a **bold data row**. There bold *is* consistent.

If a later release adds a sub-group not in `NEST_AT_ROOT`, two series collide on
one key and the duplicate check in `clean_data.py` **fails the run** rather than
silently overwriting. That is the intended failure mode.

## Data quality notes

- The ABS spells two ANZSIC division names inconsistently across tables
  ("Rental, hiring and real estate **S**ervices" in Table 26 versus
  "**s**ervices" elsewhere). The division letter is authoritative; names are
  normalised from `INDUSTRY_NAMES`.
- `unit` is nowhere stated as a field — it is derived from the block heading, the
  row path and the in-sheet title. All 1,808 indicators resolve to one of: Index,
  Percentage change, Percentage point contribution, Proportion, $ million,
  $ per unit of capital.
- Tables 12–13 split each industry into `(incorporated)` / `(unincorporated)`.
  That split lives in `item_path`, not in its own column, since it applies to only
  2 of 42 tables.
- Indicators appearing only in tables 3, 5 and 26 have no annual observations, so
  `first_financial_year` / `last_financial_year` are NULL for them (6.1%).
- `basis` is populated for 4.9% of indicators, which is below the dbt
  `not_null_proportion` floor, so it is listed in that test's `ignore_values`.

## Files

```
code/
  download.py     fetch + extract the 3 xlsx (browser UA required) -> input/
  clean_data.py   parser: input xlsx -> output/{indicator,observations,growth_cycles}
  upload.py       output/* -> basedosdados-dev staging (bd.Table)
  architecture/   indicator.csv, observations.csv, growth_cycles.csv
au_abs_productivity__indicator.sql
au_abs_productivity__observations.sql
au_abs_productivity__growth_cycles.sql
schema.yml
```

Scratch data lives in `~/Library/Caches/au_abs_productivity_data/` — **not**
`~/Downloads`, which is a Dropbox symlink on this machine, and never the repo.
Override with `AU_ABS_PRODUCTIVITY_DATA_DIR`.

## Reproduce

```bash
D=~/Library/Caches/au_abs_productivity_data
uv run python models/au_abs_productivity/code/download.py "$D/input"
uv run python models/au_abs_productivity/code/clean_data.py "$D/input" "$D/output"
uv run python models/au_abs_productivity/code/upload.py --env dev
BD_SERVICE_ACCOUNT_DEV=$HOME/.basedosdados/credentials/staging.json \
  uv run dbt run  --select au_abs_productivity --profiles-dir .
BD_SERVICE_ACCOUNT_DEV=$HOME/.basedosdados/credentials/staging.json \
  uv run dbt test --select au_abs_productivity --profiles-dir .
```

## Validation

Computed from the parsed index levels and checked against figures the ABS and the
Productivity Commission published independently of this pipeline:

| Check | This dataset | Published |
|---|---|---|
| Market-sector MFP, hours basis, %Δ 2024-25 | −0.53 | ABS: "fell 0.5%" |
| Market-sector labour productivity, hours basis, %Δ 2024-25 | −0.17 | ABS: "declined 0.2%" |
| Mining MFP, %Δ 2024-25 | −3.20 | PC 2026 bulletin: "3.2%" fall, largest of any industry |
| Largest MFP rise 2024-25 | Agriculture, forestry and fishing (+10.35) | ABS: largest rise in Agriculture |

Parser invariants, all enforced as hard failures: 0 duplicate
`(year, indicator_id)`, 0 duplicate `(indicator_id, period)`, 0 orphan fact rows,
42 of 42 tables covered, every indicator carries a unit.

dbt on dev: 3 models built with exactly 1,808 / 56,849 / 632 rows; **17 of 17
tests pass**, including the `state_id` FK to `br_bd_diretorios_au.state` and both
internal `indicator_id` FKs.

## Status

- [x] 1. Context — org, licence, themes, tags, coverage
- [x] 2. Architecture — local CSVs under `code/architecture/`, and Google Sheets in
      `Base dos Dados - Geral/Dados/Conjuntos/au_abs_productivity/`
      ([folder](https://drive.google.com/drive/folders/1wRinCdxdwUlmG44NE5SOML7ZE9AnccH1)):
      [indicator](https://docs.google.com/spreadsheets/d/1vYT8fH0U6w1d9srBtDzSTTGbFtUbgxoIg_cYQn-FaE8/edit) ·
      [observations](https://docs.google.com/spreadsheets/d/1mA489L1KUz8GLeAN_meD0MW3KgZ5YnW9oUgF4Wjddzo/edit) ·
      [growth_cycles](https://docs.google.com/spreadsheets/d/1ubT4t2CBGYEL0ZjJ8CpKH7nUqrmbBz2XewCFBsDanxE/edit).
      One spreadsheet per table named by table slug, following `au_geoscape_gnaf`;
      `au_ato_abr` uses the other convention (one spreadsheet, a tab per table),
      which does not work with `architecture_url` without a gid. The sheets carry
      `description_pt/en/es` and `observations_pt/en/es`, which the local CSVs do
      not — the CSVs hold EN only.
- [x] 3. Download — 3 workbooks, 766 KB
- [x] 4. Clean — 1,808 / 56,849 / 632, validated against published figures
- [x] 5. Upload — `basedosdados-dev` staging, all 3 tables
- [x] 6. dbt — 3 models + `schema.yml` + `dbt_project.yml` entry, run on dev
- [x] 7. Validate — `dbt test` 17/17 PASS on dev
- [x] 8. Discover IDs — on **staging**, because the dev backend returns HTTP 503
- [x] 9. Metadata — dataset (`under_review`), 1 raw data source, 3 tables,
      observation levels, cloud tables, coverage, datetime ranges, table and
      source Updates. Verified with `get_dataset`.
- [x] 9a. Columns — 24 across the three tables, with types, all three languages,
      `directory_column` on `state_id` and `year`, `measurement_unit` on `year`,
      `is_partition` on `observations.year`, and observation levels linked.
      Registered with `upload_columns_from_sheet` (which also takes the
      `observation_levels` map) followed by
      `bulk_upsert_columns(architecture_url=…, update_only=true)` to add
      `description_en/es` and `observations_pt/en/es`, then one `update_column` for
      `is_partition` on `observations.year`, which no sheet schema carries.

      **The Google Sheet was probably not necessary.** A measured audit on
      `br_mj_sisdepen` (95 columns, both backends) found that
      `bulk_upsert_columns` with **`columns_json`** does write `bigquery_type` and
      `directory_column`, leaving only `is_partition` for `update_column` — so the
      JSON path alone would likely have done this. `dry_run`'s `plan[].sets`
      omits `bigqueryType` even though the real call writes it, which is what
      misled the earlier attempt here into treating Drive as a hard blocker. Do
      not read `sets` as authoritative.

      Verify against the backend rather than `get_dataset`, which returns only
      `id`/`name`/`is_partition` per column and cannot confirm a type:

      ```graphql
      { allColumn(first: 100, table_Id: "<id>") { edges { node {
          name isPartition bigqueryType { name } measurementUnit
          observationLevel { id } directoryPrimaryKey { id } } } } }
      ```

      Audited 2026-09-29 on staging: 24/24 columns correct — types, all three
      description languages, `state_id` and `year` directory links, `year`
      partition and unit, and observation levels on all four identifying columns.
- [x] 9b. Published on staging (`under_review` → `published`)
- [ ] 10–13. Prod metadata → PR → merge → table-approve → verify → publish
- [ ] 14. Delete `~/Library/Caches/au_abs_productivity_data/`

## Open notes

- **No ANZSIC directory exists in Data Basis.** `industry_code` /
  `industry_name` are stored inline, matching `au_abs_labour_force`, which does
  the same. A `br_bd_diretorios_au.anzsic_division` directory would let both
  datasets share one FK; noted in the architecture `observations` field.
- **Prod tags are English, staging tags are Portuguese.** The 7 tags attached
  here (`produtividade`, `atividade_economica`, `producao`, `trabalho`,
  `carga_horaria`, `investimento`, `crescimento`) must be re-resolved to their
  English equivalents before registering on prod, or they will silently drop.
- Cloud tables point at `basedosdados-dev`; they must be repointed to
  `basedosdados` at prod promotion, before the table-approve action materialises
  the prod tables.
- A recurring Prefect pipeline (step 12) is worth adding: the source is annual
  with a predictable February release window. The cleaning transform in
  `code/clean_data.py` would move to `pipelines/datasets/au_abs_productivity/utils.py`
  and be imported here, rather than duplicated.
