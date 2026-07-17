# Metadata Schema

Reference for all agents that register or update metadata in the Data Basis backend.

## MCP server

All backend operations use the `mcp__databasis__*` MCP tools. Never write raw HTTP requests. Always call `mcp__databasis__auth` first (or rely on auto-auth) before any other MCP tool.

## Environment

| env | Backend URL | GCP project |
|-----|-------------|-------------|
| dev | `development.backend.basedosdados.org` | `basedosdados-dev` |
| prod | `backend.basedosdados.org` | `basedosdados` |

Default: `dev`. Only switch to `prod` after explicit user approval.

## ID resolution (always run first)

Before registering any metadata, resolve all reference IDs with:

```
mcp__databasis__discover_ids(env=<env>)
```

This returns IDs for: `status`, `bigquery_type`, `entity`, `license`, `availability`, `organization`, `theme`, `tag`, `entity_category`, `language`, `measurement_unit_category`.

**Never hardcode IDs or guess slugs.** IDs differ between dev and prod.

For individual lookups: `mcp__databasis__lookup_id(env=<env>, slug=<slug>, type=<type>)`

## Operation sequence

### Once per dataset

1. `get_dataset(slug, env)` — check if dataset exists; get existing IDs
2. `get_authenticated_account(env)` — get current user ID for `publishedBy`/`dataCleanedBy`
3. `get_raw_data_sources(dataset_slug, env)` — check for existing raw source IDs
4. `create_update_dataset(...)` — create or update dataset record
5. `create_update_raw_data_source(...)` — one call per raw source

### Per table (repeat for each)

6. `create_update_table(...)` — create table record (without `raw_data_source_ids`)
7. `create_update_observation_level(...)` — one call per entity level
8. `reorder_observation_levels(...)` — if human specifies order
9. `upload_columns_from_sheet(...)` — bulk upload columns from architecture Google Sheet
10. `update_column(...)` — per-column follow-up for `is_partition`, `is_primary_key`, EN/ES descriptions
11. `create_update_cloud_table(...)` — link to BigQuery table
12. `create_update_coverage(...)` — coverage for area "br" (see `is_closed` below)
13. `create_update_datetime_range(...)` — temporal range
14. `create_update_update(...)` — update frequency/lag record
15. `create_update_table(...)` again — link `raw_data_source_ids` (deferred update)

### After all tables

16. `reorder_tables(...)` — if human specifies order

## `create_update_dataset` fields

| Field | Type | Notes |
|-------|------|-------|
| `slug` | string | Bare slug (e.g. `cnuc`, NOT `br_mma_cnuc`) |
| `name_pt/en/es` | string | Dataset display names (all 3 languages) |
| `description_pt/en/es` | string | 1–3 sentences, technical, no bullet lists |
| `organization_ids` | list | From `discover_ids` using org slug(s) |
| `theme_ids` | list | From `discover_ids` |
| `tag_ids` | list | From `discover_ids`; empty list if none |
| `status_id` | string | Use `status.published` |
| `id` | string | Pass when updating an existing record |

## `create_update_table` fields

| Field | Type | Notes |
|-------|------|-------|
| `slug` | string | Table slug |
| `name_pt/en/es` | string | All 3 languages |
| `description_pt/en/es` | string | 1–3 sentences |
| `dataset_id` | string | From dataset step |
| `status_id` | string | `status.published` |
| `published_by_ids` | list | Authenticated account ID |
| `data_cleaned_by_ids` | list | Authenticated account ID |
| `id` | string | Pass when updating |

Do **not** pass `raw_data_source_ids` in the initial creation — link them in the deferred update (step 15).

## `upload_columns_from_sheet` fields

| Field | Notes |
|-------|-------|
| `table_id` | Table ID from step 6 |
| `architecture_url` | Google Sheets URL for this table's architecture |
| `observation_levels` | JSON string: `{"col_name": "<ol_id>", ...}` |
| `env` | Current env |

Note: `directoryPrimaryKey` is silently skipped in dev if the directory dataset is absent. This is expected behavior.

## `update_column` fields (per-column follow-up)

Only set these fields — do NOT re-set fields already handled by `upload_columns_from_sheet`:

| Field | Notes |
|-------|-------|
| `column_id` | From upload response |
| `column_name` | Column name |
| `table_id` | Table ID |
| `is_partition` | `True` for partition columns |
| `is_primary_key` | `True` **only in directory tables** (`br_bd_diretorios_*`). Leave `False` everywhere else — see rule below. |
| `description_en` | Translated from Portuguese |
| `description_es` | Translated from Portuguese |

### Primary keys: directory tables only

`is_primary_key = True` is reserved for the key column(s) of **directory tables** (datasets under `br_bd_diretorios_*`). In every non-directory table (i.e. ordinary data tables), **no column may have `is_primary_key = True`** — not even the columns that form the logical key (`ano`, `sigla_uf`, `id_municipio`, `country_iso3_code`, `age`, etc.).

In a data table, a column's relationship to a key is expressed **only** through its `directoryPrimaryKey` link (the directory column it references), set via the `directory_column` field in the architecture sheet / `upload_columns_from_sheet`. Columns with no directory (e.g. `age`, `age_group` when no age directory exists) simply carry neither `is_primary_key` nor a directory link.

Uniqueness of the logical key is still enforced separately in dbt (`dbt_utils.unique_combination_of_columns`); that is independent of the backend `is_primary_key` flag.

## `create_update_cloud_table` fields

| Field | Notes |
|-------|-------|
| `table_id` | Table ID |
| `gcp_project_id` | `basedosdados-dev` (dev) or `basedosdados` (prod) |
| `gcp_dataset_id` | Full GCP dataset ID (e.g. `br_mma_cnuc`) |
| `gcp_table_id` | Table slug |
| `id` | Pass when updating |

## `create_update_coverage` — `is_closed` is the free/pro split

`Coverage.is_closed` marks whether the coverage describes open or BD Pro data. The
polarity is counterintuitive:

| Coverage | `is_closed` | Meaning |
|----------|-------------|---------|
| free | `False` (default) | Open/public data |
| pro | `True` | BD Pro data — drives `Table.contains_closed_data`, the site's Pro badge |

A fully public table has **one** coverage (`is_closed=False`). A table paywalling a
rolling window has **two** — free *and* pro — each with its own `DateTimeRange`. Omit
the argument on routine updates: it leaves the stored value untouched, so a metadata
edit cannot silently un-paywall data.

Two things that are easy to miss:

- **Set `is_closed` on the `DateTimeRange` as well**, matching its Coverage. They are
  separate fields on separate records; the pro range needs `is_closed=True` on both.
- **The free and pro ranges must not overlap.** Free ends at `free_end` *inclusive*,
  so pro starts the **next** period — free `1913-01..2025-12`, pro `2026-01..2026-06`,
  never `2025-12..2026-06`.

For the pipeline side (`PartBdpro`, rolling windows, Row Access Policies), see the
"BD Pro rolling window" section of `prefect-pipeline-conventions`. Both coverages
must exist **before** a `part_bdpro` pipeline runs, or it hard-fails at
`assert_coverage_topology`.

## `create_update_datetime_range` fields

| Field | Notes |
|-------|-------|
| `coverage_id` | From coverage step |
| `start_year` | Integer |
| `end_year` | Integer or null if ongoing |
| `start_month` / `end_month` | 1–12 — **required for monthly (and daily) tables** |
| `start_day` / `end_day` | 1–31 — **required for daily tables** |
| `interval` | 1 for annual |
| `is_closed` | `False` unless series has ended |

### Match the range's granularity to the table's

**A monthly table needs months; a daily table needs months and days.** Year-only is
correct *only* for genuinely annual tables. Registering a month-granular table with
year-only bounds understates its coverage and renders wrong on the site — e.g.
`us_bls_cpi.monthly` spans `1913-01..2026-06` but was first registered as
`1913..2026`, losing both endpoints' months.

Read the real min/max from the **data**, not from the table's year partitions. A day
requires a month and a month requires a year, on each side independently.

Correct shape for a monthly table (cf. `br_ibge_ipca.mes_brasil`, free
`1979-12..2025-11`):

```
start_year=1913, start_month=1, end_year=2026, end_month=6
```

## Update and Poll — three records, three meanings

`Update` hangs off **either** a table **or** a raw data source, and they mean different
things. `Poll` hangs off the raw data source. A dataset with a recurring pipeline needs
**all three**; a one-off dataset needs only the table Update.

| Record | Anchor | `latest` means | Written by |
|--------|--------|----------------|------------|
| `Update` | table | When **we** last refreshed the table (wall clock) | `register_table_materialization_task`, or by hand at onboarding |
| `Update` | raw data source | What the **source** last published — its **max coverage date**, e.g. `2026-06-01` for June data | `commit_source_update_task` |
| `Poll` | raw data source | When we last **looked** at the source (wall clock), whether or not it had anything new | `poll_source_for_update_task` |

The distinction that matters: **the source Update is a coverage date, the table Update
and the Poll are wall clocks.** Putting today's date on the source Update says the
publisher released data today, which is almost never true.

"Did they release anything new?" is not a boolean field — it is `Poll.latest` (when we
looked) versus `RawDataSource.Update.latest` (what they had). A Poll newer than the
source Update means we checked and found nothing new.

Reference (`br_ibge_ipca`, and now `cpi`):

```
Table.Update          entity=month  frequency=1  lag=1     latest=2026-07-17  (wall clock)
RawDataSource.Update  entity=month  frequency=1  lag=None  latest=2026-06-01  (coverage date)
RawDataSource.Poll    entity=day    frequency=1            latest=2026-07-17  (wall clock)
```

`frequency` is how many `entity` units between releases (1 + `month` = monthly). `lag` is
the publication delay in the same units (CPI for month M lands in M+1, so `lag=1`);
source-anchored Updates conventionally leave it unset.

Pass exactly one of `table_id` / `raw_data_source_id` to `create_update_update`.

## `create_update_update` — `latest` semantics

**Table-anchored Update:** `latest` is **when the table was last updated at Data Basis** —
not the max date in the raw data. Set it to **today's date** (when the onboarding or update
is performed). Never derive it from the data's temporal coverage or the raw source's
extraction date.

**Source-anchored Update:** the opposite — `latest` is exactly the **max coverage date of
the raw source** (`2026-06-01` for June data). Today's date here would claim the publisher
released data today.

> **Caveat for datasets with a recurring pipeline.** `poll_source_for_update` compares the
> source's max coverage date against **`Table.Update.latest`** — a coverage date against a
> wall clock. Setting `Table.Update.latest` to today at onboarding can therefore make the
> poll return "no new data" until the source's coverage overtakes that timestamp, so the
> pipeline runs green while ingesting nothing. See `project_metadata_update_latest_semantics`;
> the read/write split (poll reads Table.Update, commit writes RawDataSource.Update) is a
> known open bug, not something to work around per dataset.

## Known issues

**M2M fields (organizations, themes, tags, raw_data_source_ids):** These are Django ManyToManyFields. Pass them and verify with `get_dataset` after saving. If they appear empty despite being passed, it is a backend deployment issue — note and continue; do not retry indefinitely.

**Deferred raw_data_source linking:** Always link raw data sources in a second `create_update_table` call (step 15), after all raw sources exist. Pass ALL required fields again — the API does not support partial updates.

## Description writing guidelines

- Be technical: state what the data contains, the source system, the entity level, and the time period.
- Prefer direct translation from raw data source documentation.
- Format: 1–3 sentences per entity. No bullet lists.
- Translate Portuguese descriptions to English and Spanish using domain knowledge of Brazilian public administration and statistics.
- **Column** descriptions must not end with a period (trailing full stop) in any language — see `data-basis-style.md`. Dataset/table descriptions (full sentences) are unaffected.

## Verification after dev registration

After completing all dev steps, output:

```text
=== METADATA REGISTRATION COMPLETE (env=dev) ===
Dataset: <slug> (id=<id>)
Tables: ✓ <table_slug> — N OLs, N columns, cloud table, coverage, update
Next: verify at https://development.basedosdados.org/dataset/<slug>
```
