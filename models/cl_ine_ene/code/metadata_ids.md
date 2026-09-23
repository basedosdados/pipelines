# cl_ine_ene — resolved backend IDs (staging, 2026-09-23)

IDs differ between staging and prod. Re-resolve on prod before promoting; never copy
these across environments.

| What | Slug | Staging ID |
|---|---|---|
| License | `cc_by_sa` — "Creative Commons Atribuição-CompartilhaIgual 4.0 (CC BY-SA 4.0)" | `f8c67681-1674-4a48-a1b9-61e33d15cc7f` |
| Status | `under_review` | `47208305-325a-4da9-9222-ac6849405b78` |
| Status | `published` | `e16221de-ac30-4926-83d3-de219998dab3` |
| Availability | `online` | `dd396d7d-0264-4c1f-bf0d-6efe2dc89cbe` |
| Language | `es` | `fb7729d4-e2fc-411c-bc15-4f9308dba57d` |
| Theme | `economics` | `ad6a413a-e882-4dd6-a497-8a62eec8511b` |
| Theme | `population` | `ad7203a8-fe66-4c33-b9f8-2f8690975c21` |
| Entity | `year` | `e1bf146e-b6bb-4b65-bee7-c800876e80a5` |
| Entity | `month` | `f9659fea-e9bb-4177-9ca0-54076a8c0932` |
| Entity | `person` | `b4e76213-888b-40ea-b877-d82ce76d71a2` |
| bigquery_type | `STRING` | `8488363a-4887-40a0-afd8-d0d5021538b1` |
| bigquery_type | `INT64` | `ace7bf06-4a20-4cee-a8ca-369cdf7a55a7` |
| bigquery_type | `FLOAT64` | `a5f6c9c2-b631-4583-8b4a-c1912c21b1f2` |

## Organization

**`cl_ine` DOES NOT EXIST and must be created.** Do NOT reuse
`instituto_nacional_de_estadistica` (`5b2d2c40-a3bb-4fb0-ba3f-379032e0e85a`) — that is
**Spain's** INE. Country-prefix the slug and put the country inside the display name:

- slug `cl_ine`
- `name_pt` "Instituto Nacional de Estatísticas do Chile (INE)"
- `name_en` "Chilean National Statistics Institute (INE)"
- `name_es` "Instituto Nacional de Estadísticas de Chile (INE)"
- `website` https://www.ine.gob.cl
- area `cl`

## Tags (all pre-existing — none created)

`emprego` 38b082a8-8448-4ae6-b38e-3b726eb5cd9c ·
`trabalho` 161d4c2e-a61e-481d-8821-3f70b534c063 ·
`ocupacao` cc30e4cc-f168-42dc-ac29-ff0da39e4763 ·
`informalidade` dbf16af0-a8dd-4826-aad4-5d438f3e9a18 ·
`unemployment` 236cd853-a3f9-4df6-8af8-25e48981a6f2 ·
`carga_horaria` aa2bc646-7cad-484b-a1dd-e39869fd30fa ·
`escolaridade` 0191a1d6-b557-4ea7-9032-30d065be5971 ·
`pesquisa` 4ae52b90-bc5e-49b3-92f6-c5e86ae5a241

No `chile` tag: area names are the `Coverage`/`area` metadata, not a tag. There is no
`desemprego` tag; the existing English `unemployment` is reused rather than creating a
near-duplicate.

## Themes

There is no labour theme in the vocabulary (23 total), so `economics` + `population`.

---

## Registered on STAGING, 2026-09-23

These are staging IDs. Prod IDs differ — re-resolve everything before promoting,
and re-create the organization there too.

| Record | ID |
|---|---|
| Organization `cl_ine` (CREATED) | `feffd51b-40a5-4710-a48f-e10e77af4a0e` |
| Dataset `cl_ine_ene` | `2e85e10c-9504-4d05-9598-f027c6a2e97d` |
| Raw data source | `6b0c1b7c-addb-446a-8fb7-78477428ee5a` |
| Table `microdato` | `2d98657d-47eb-4348-a1d3-a4a811b49408` |
| Table `dicionario` | `b1ac45a4-7ffd-4f98-9657-0453ffadfa82` |
| OL year / month / person | `e8318279…` / `b1b423fb…` / `eddd3503…` |
| Coverage free / pro | `952dd73b…` / `bbf68928…` |

## Order of operations that worked

1. Organization, then dataset (`status = under_review`), then raw data source.
2. Both tables, then the three observation levels.
3. Columns via `bulk_upsert_columns` reading `columns_microdato.json` from disk
   through a direct `server.py` import — 198 KB is far too large to paste through
   a tool argument.
4. `update_column` for the three grain columns, **re-passing `is_partition`** in
   the same call: its boolean arguments default to False, so linking an
   observation level on `ano` would otherwise silently clear its partition flag.
5. Cloud tables, coverages, datetime ranges, updates.
6. The deferred `raw_data_source_ids` link as a second `create_update_table`,
   re-passing every field — the API does no partial updates and omitted names
   would be blanked.
7. `reorder_tables`, then dataset `status = published` on staging only.

The historical bug where `create_update_table` failed on a table that already had
a Coverage is **fixed on staging** as of this date; step 6 succeeded with two
coverages already in place.

## BD Pro topology

`microdato` is `PartBdpro(free_lag=6 months)`, so it needs both coverages before
the pipeline's first armed run or `assert_coverage_topology` hard-fails. Verified
in place and non-overlapping, at month granularity:

```
FREE 2010-02 .. 2025-12   coverage.is_closed=False  range.is_closed=False
PRO  2026-01 .. 2026-06   coverage.is_closed=True   range.is_closed=True
```

Free ends inclusive at `source_end - 6 months`; pro starts the next month.

## Update records

| Record | entity | frequency | lag | latest |
|---|---|---|---|---|
| Table update (wall clock) | month | 1 | 2 | 2026-09-23 |
| Raw source update (source max coverage date) | month | 1 | — | 2026-06-01 |

The `Poll` is written by the flow's first run. `latest` needs a full datetime:
a bare `2026-09-23` is rejected with "DateTime cannot represent value".
