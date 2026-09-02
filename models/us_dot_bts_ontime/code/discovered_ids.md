# Resolved backend IDs — us_dot_bts_ontime

Backend: **staging** (`https://staging.backend.basedosdados.org`). IDs differ
between staging and prod — re-resolve everything before the prod promotion.

## Created for this dataset

| Object | Slug | ID (staging) |
|---|---|---|
| Organization | `bts` | `3ddbab41-586d-435d-b22a-e8b22e373d7f` |
| Entity | `flight` (category `transportation`) | `ef41a936-8494-4b4c-affc-afa2c136ecad` |
| Entity | `airport` (category `establishment`) | `fd481fc1-3d43-4d79-ba61-d674ff44cf2f` |
| Dataset | `airline_ontime_performance` | `90ecbfba-8801-4b8d-a3c8-2d8aa552f485` |
| Raw data source | On-Time Performance (monthly) | `c96cc263-af77-4b45-9465-ffb02e356715` |
| Raw data source | TranStats lookup tables | `96d66812-91f4-4368-a319-133ab45cb14f` |

The organization, and both entities, did not exist. `station` sits in the
`establishment` category, so `airport` follows it; `aircraft`, `train` and `ship`
sit in `transportation`, so `flight` follows those.

**One raw source per table.** `client._raw_source_id` raises when a table has two
or more, which would make the recurring pipeline fail at its first poll. `flight`
takes the on-time source; `airport` and `dicionario` take the lookup source.

## Reference IDs

| Category | Slug | ID |
|---|---|---|
| Status | `under_review` | `47208305-325a-4da9-9222-ac6849405b78` |
| Status | `published` | `e16221de-ac30-4926-83d3-de219998dab3` |
| License | `cc0` | `7fb71004-2abe-4fc8-a258-e2aac27c71d9` |
| Availability | `online` | `dd396d7d-0264-4c1f-bf0d-6efe2dc89cbe` |
| Theme | `infrastructure-transportation` | `a08f705e-0df0-4143-8c40-14c00020171a` |
| Area | `us` | `61a2c232-c649-4b41-a5a3-1467b7393e11` |
| Entity | `year` | `e1bf146e-b6bb-4b65-bee7-c800876e80a5` |
| Entity | `month` | `f9659fea-e9bb-4177-9ca0-54076a8c0932` |
| Entity category | `transportation` | `2dbaf692-d8c8-472b-8dbb-7d43da6d1b8f` |
| Entity category | `establishment` | `3811c85e-32a2-4d6c-ba0a-ce711854bb63` |

## Tags

All six already existed; none were created. Area (`us`) and theme-mirroring tags
are deliberately absent, since both are already structured metadata.

| Slug | English | ID |
|---|---|---|
| `aviacao` | aviation | `bc8dc154-6dc4-4916-b99c-e103d955a7c6` |
| `voo` | flight | `320335eb-b021-4349-bd58-352d53c78322` |
| `aeroporto` | airport | `9717a33c-1823-4bef-b473-87e56d18f620` |
| `atraso` | delay | `987ed618-8c6b-4152-9b3d-c030bec89eb4` |
| `trafego_aereo` | air traffic | `3e4d2ded-d920-4387-8f12-b0229b9ea7ad` |
| `aviao` | airplane | `febe38b4-5c92-4b41-95b5-707bf78ef4b5` |

## MCP workaround

`discover_ids` and `lookup_id` fail for `entity_category`: the client queries
`allEntityCategory` but the backend field is `allEntitycategory`. Query the
GraphQL endpoint directly until the client is fixed.

## Tables and their records (staging)

`create_update_*` is NOT idempotent without an `id`: re-running without one
creates a duplicate observation level, cloud table, coverage or update, and
duplicate coverages then break `CreateUpdateTable`. Pass these ids to update.

| Table | id | cloud table | observation levels |
|---|---|---|---|
| `flight` | `245b6498-5295-44df-8f4d-62496f2ba898` | `66b75297-1676-4c68-bc9e-14570a492e90` | flight `5b869f8d-4369-4b41-9a41-08081b17cf51`, year `bb44a10c-1f01-49ee-b850-bbd0181e28fc` |
| `airport` | `2a0d9769-c080-4a84-a77e-016996a3fae8` | `10b4dd33-4f3d-483a-bbca-259ddff90f09` | airport `6c322fad-39ad-4ad0-9ccf-f048cd68f4f1` |
| `dicionario` | `fe828676-962b-4354-9605-fb83acd2b3b3` | `f90c8426-0bd5-4183-ae86-1fd2564a80be` | none |

## Coverage

`flight` is `PartBdpro(free_lag=6 months)`, so it carries **both** a free and a pro
Coverage. The pipeline's `assert_coverage_topology` hard-fails before writing
anything if either is missing. Note the counterintuitive polarity: free is
`is_closed=False`, pro is `is_closed=True`, and the flag is set on the
`DateTimeRange` as well as the Coverage.

The two ranges are mutually exclusive: free ends at `free_end` *inclusive*, so pro
starts the following month.

| Table | Coverage | `is_closed` | id | DateTimeRange | id |
|---|---|---|---|---|---|
| `flight` | free | `False` | `c017d4e2-29a9-433f-bc83-d5dc44590dc2` | 1987-10 .. 2025-12 | `c9413d79-228b-4fca-995e-42d312084e9b` |
| `flight` | pro | `True` | `9556d92f-95d8-46a9-8451-6bbdad77f53e` | 2026-01 .. 2026-06 | `9707ca13-55b1-4247-8590-d2c374df1d1f` |
| `airport` | free | `False` | `83d91d0a-d45e-474c-8f33-8f922cebe57d` | none (no date column) | — |
| `dicionario` | free | `False` | `772d8ed9-4447-467b-afbd-877686c4c116` | none (no date column) | — |

The ranges are month-granular, not year-only: the table really spans 1987-10 to
2026-06, and a year-only range would understate both endpoints.

The flow polls with `compare_against="coverage"`, which reads
`Coverage.DateTimeRange` rather than `Table.Update.latest`. That is the correct
setting here because `source_max_date` is a competency (`YYYY-MM`), not a
publication timestamp — and it sidesteps the known bug where a wall-clock
`Table.Update.latest` makes the poll report "no new data" indefinitely.

## Update records

| Record | Anchor | `latest` | Meaning | id |
|---|---|---|---|---|
| `Update` | table `flight` | `2026-09-02` | wall clock: when we refreshed | `235fe13b-0603-4f7e-b99a-b1c7732e50e8` |
| `Update` | raw source (on-time) | `2026-06-01` | coverage date: what BTS published | `fad41f76-aa4c-4135-8db8-71e8c2479663` |
| `Poll` | raw source (on-time) | written by the first pipeline run | wall clock: when we looked | — |

`lag=2` on the table Update: BTS publishes month M around M+2 (2026-06 landed
2026-08-12). The source-anchored Update leaves `lag` unset by convention.
