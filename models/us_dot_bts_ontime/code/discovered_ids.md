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
