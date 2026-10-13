# Resolved backend reference IDs (staging)

Resolved 2026-08-28. IDs differ per backend — re-resolve before registering on prod.

| Kind | Slug | ID |
|---|---|---|
| area | `cl` | `b08e39e7-966c-4d33-ae58-b513c47291d2` |
| account (publishedBy / dataCleanedBy) | rdahis@basedosdados.org | `57` |
| theme | `economics` | `ad6a413a-e882-4dd6-a497-8a62eec8511b` |
| theme | `government` | `6dd730bb-89ab-4dba-a1bf-a25ca1c35003` |
| status | `under_review` | `47208305-325a-4da9-9222-ac6849405b78` |
| status | `published` | `e16221de-ac30-4926-83d3-de219998dab3` |
| availability | `online` | `dd396d7d-0264-4c1f-bf0d-6efe2dc89cbe` |

## Tags — all already exist, none need creating

| Slug | ID |
|---|---|
| `licitacao` | `4b76d0d7-7a4b-4a73-a2c5-33a08853dc77` |
| `compra` | `c6416645-6aeb-43d4-a60c-8a5fddaf959a` |
| `contrato` | `0831b835-2079-44f3-b5e8-3f598435bbe0` |
| `empresa` | `536be6c2-7fc6-4409-a029-fb8e1c771dec` |
| `gasto` | `83b37841-83b0-45e7-9455-b7b1008f1e30` |
| `financas_publicas` | `5dce4b1d-131b-452a-a419-bdd587a8c272` |
| `administracao_publica` | `94b742db-a2c2-468b-b83e-2f223bb98fe7` |
| `concorrencia` | `886e7c5e-65ed-4def-946c-eb3786bf6371` |

A `chile` tag exists (`52e56a56-0c26-4674-9cf0-9d290f226bff`) and is deliberately
**not** attached: tag conventions forbid tagging place names, which are already carried
as the dataset's `Coverage`/`area`. Likewise `governo` is skipped because the
`government` theme already covers it.

## Created on staging (2026-09-02)

| Kind | Slug | ID |
|---|---|---|
| organization | `chilecompra` | `d460a2df-df48-4d41-8660-29f298dee108` |
| license | `libre_uso_cl` | `c902369b-1310-45f4-a624-934b43adaa0f` |

`libre_uso_cl` mirrors the existing `libre_uso_mx` precedent: ChileCompra publishes no
CC licence, but its terms permit reuse with mandatory attribution ("deberán indicar
claramente que la fuente de los datos es la Dirección ChileCompra") and contemplate
commercial use. URL points at the terms page itself, since there is no licence deed.

Both still need creating on **prod** before the prod promotion — reference IDs differ
per backend.

## Geography directory (resolved)

`br_bd_diretorios_cl` exists: dataset `1c24b829-616b-48f7-8824-9d34b0de5b10`, with
`region` (`id_region`, 16 rows), `provincia` and `comuna` (`id_comuna`, 346 rows).
The architecture now carries five `directory_column` FKs onto it, populated from the
name-to-CUT crosswalk in `code/geografia_crosswalk.csv`.

## Traps to respect when registering

**Link exactly ONE raw data source per table.** `client._raw_source_id` resolves a
table's source through `_query_id`, which raises when a table has two or more
(`allRawdatasource: mais de um nó encontrado`). Both `poll_source_for_update_task` and
`commit_source_update_task` go through it, so a table with two sources cannot run the
recurring pipeline at all — it fails at the first poll. There are two natural sources
here (the `oc-da` and `lic-da` containers); link the matching one to each table and
record the other at dataset level only.

**Link each observation level to its identifying column**, or the site renders the
level's columns as "Não informado". `bulk_upsert_columns` does not do this — it needs a
separate `update_column(..., observation_level_id=...)` per grain column. And
`update_column`'s boolean args default to `False`, so re-pass `is_partition=True` on
`ano`/`mes` in the same call or it silently clears them.

**No `is_primary_key` on these tables.** That flag is reserved for directory tables.
The logical keys are expressed through `directory_column` links and enforced in dbt by
`dbt_utils.unique_combination_of_columns`.

**Datetime ranges need month granularity.** These tables are monthly, so registering
year-only bounds understates coverage and renders wrong. Read the real min/max from the
data, not from the year partitions: `start_year`/`start_month` and `end_year`/`end_month`.

**`Update.latest` semantics differ by anchor.** The table-anchored Update is a wall
clock (when we refreshed); the raw-source Update is the source's **max coverage date**
(e.g. `2026-08-01`), not today.

## Coverage tier

All three tables refresh weekly, so the house rule paywalls the recent window:
`PartBdpro` with a 6-month free lag. That requires **both** a free Coverage
(`is_closed=False`) and a pro Coverage (`is_closed=True`) to exist before the pipeline
runs, or `assert_coverage_topology` hard-fails. Set `is_closed` on the `DateTimeRange`
too, matching its Coverage, and keep the ranges non-overlapping — free ends inclusive,
pro starts the next month.
