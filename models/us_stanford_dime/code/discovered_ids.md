# Backend IDs — us_stanford_dime (staging)

The `dime` dataset already existed as an empty v3-era shell and was **updated in
place**, not recreated, so the slug and id are unchanged. Staging and prod share
the same record ids; only the tag slugs differ (Portuguese on staging, English
on prod), which is the documented convention.

## Dataset and source

| Object | ID |
|---|---|
| dataset `dime` | `da75c900-e1bf-4ea9-a263-ad9228909892` |
| raw data source | `6e1a0988-817b-4add-b745-101924fd5fcd` |
| organization `stanford` | `f3e2c137-3252-4089-abb3-f2d0a5c0bd56` |
| theme `politics` | `c4e75f8c-29de-4b76-9607-657d4ac7f490` |
| area `us` | `61a2c232-c649-4b41-a5a3-1467b7393e11` |
| licence `odc_by` | `4c3dcb4c-def5-46a1-a2c8-88eebb5d59f8` |
| availability `online` | `dd396d7d-0264-4c1f-bf0d-6efe2dc89cbe` |
| language `en` | `0420c9c1-3ba3-4620-a074-56207eba5ae9` |
| status `under_review` | `47208305-325a-4da9-9222-ac6849405b78` |
| status `published` | `e16221de-ac30-4926-83d3-de219998dab3` |
| account (published/cleaned by) | `57` |

## Tables

| Table | ID |
|---|---|
| contribution | `57af8929-8ec4-40b0-bb93-7a57bcf97e75` |
| recipient | `c047bc81-686e-40dc-b7b7-9be23e90cd50` |
| contributor | `091281c1-1181-4cac-a11b-aced29dd70b3` |
| contributor_cycle | `c80240ff-3eed-4986-a8ff-da414c1f02bd` |
| dicionario | `04970356-9a32-4f70-8d56-e2c5835c077c` |

## Entities used for observation levels

| Entity | ID |
|---|---|
| year | `e1bf146e-b6bb-4b65-bee7-c800876e80a5` |
| donation | `add96ebe-acb8-43e2-b665-43c03882027e` |
| person | `b4e76213-888b-40ea-b877-d82ce76d71a2` |
| state | `839765a7-9c7a-44bd-bb88-357cedba03f6` |
| election | `1cf89a2d-6fd0-44af-a115-dc10dc5f0cb5` |

## Tags attached

`campanha`/`campaign`, `doacao`/`donation`, `eleicao`/`elections`,
`sistema_eleitoral`/`electoral-system`, `campaign-finance`, `ideologia`,
`lobby`, `partido`, `candidatura` — nine, all pre-existing. None duplicates the
organization, theme or area, per the tag rules.

## Registered on staging

| Object | contribution | recipient | contributor | contributor_cycle | dicionario |
|---|---|---|---|---|---|
| columns | 45 | 65 | 20 | 3 | 5 |
| observation levels | year, donation | year, person, state, election | person | year, person | — |
| cloud table | ✓ | ✓ | ✓ | ✓ | ✓ |
| coverage + range | us, 1980–2024 (interval 2) | idem | idem | idem | — |
| table Update | year, freq 2 | idem | idem | idem | — |

Observation levels are linked to their identifying columns via `update_column`
(`bulk_upsert_columns` does not set the FK), and `is_partition` is re-passed on
every `cycle` column in the same call because `update_column`'s booleans default
to False and would otherwise clear it.

`dicionario` deliberately carries no coverage, range or Update: it has no date
column and no cadence.

The Update is table-anchored only. A raw-data-source Update and Poll belong to
datasets with a recurring pipeline; DIME is a static biennial release.
