# us_epa_ghgrp — locked onboarding context

Full plan: `ONBOARDING_PLAN.md`. This file is quick-resume context for any session.

**Dataset:** GCP `us_epa_ghgrp` · backend slug `ghgrp` (pre-existing shell, same id on staging
and prod: `35d072bc-ddf5-4975-b8ab-9a75f6d2fcfa`) · org `epa` · `cc0` (US public domain)
**Source:** Envirofacts GHG REST API, https://data.epa.gov/efservice/ (`pub_dim_facility`,
`pub_facts_subp_ghg_emission`, `pub_facts_sector_ghg_emission` + dimensions). FLIGHT data
sets page is the secondary, dataset-level source.
**Coverage:** 2010–2023 (2024 not published by EPA as of 2026-09-03).

## Locked decisions

1. **Four tables.** `facility` (facility × year, 136,005), `emission_subpart`
   (facility × year × subpart × gas, 395,894), `emission_sector`
   (facility × year × sector × subsector × gas, 320,667), `dicionario` (144).
2. English column names, `_id` suffix, partition `year` INT64; metadata PT/EN/ES.
3. **Reporter type travels with the category**: `subpart_type` / `sector_type` (E direct
   emitter, S supplier, I CO2 injection). Only E rows are direct emissions; summing all
   three double counts (facility 1013701, 2023: C+W = 43,895.8 t, RR adds 9,797.9 t).
4. **Stopped-reporting facilities stay** in `facility` with `reporting_status` set; their
   gas-less/value-less sector placeholder rows (25,950) are dropped from `emission_sector`.
5. **Duplicate sector keys are summed** (81 keys) — reproduces subpart facility totals exactly.
6. Geography as reported: `state_id` from the abbreviation; `county_id` kept even when it
   sits in another state (622 rows). NAICS FK → `naics_2017`, tested by vintage.
7. All tables `AllFree`; annual pipeline, full replace (`dump_mode="overwrite"`).

## Keys (all verified, 0 duplicates)

- facility: `year, facility_id` · emission_subpart: `year, facility_id, subpart, gas`
- emission_sector: `year, facility_id, sector, subsector, gas` · dicionario: `id_tabela, nome_coluna, chave`

## Staging metadata (registered 2026-09-03, env="staging")

- dataset `35d072bc-ddf5-4975-b8ab-9a75f6d2fcfa`, status published, name_es/description_es filled
- raw sources: envirofacts `071a9546-0433-4d6d-8650-2118e3ab5be3` (linked to every table),
  flight `a0745076-b206-488b-aa84-b853132e4954` (dataset-level; carries a legacy empty Update
  `f55b40bf-…` that only Django admin can delete)
- tables: facility `97edbbd9-7711-4103-8bf2-06e7c96ee191`, emission_subpart
  `c6fe5d0e-f80b-407c-ab9c-ef259cceaafb`, emission_sector `1a3ae1e1-5669-44e0-82fd-b54701349beb`,
  dicionario `6b5cf8ab-852a-4552-bb64-41d2e15a72b7`
- OLs: facility year/establishment/state/county; emission_* year/establishment/sector — all
  linked to their columns. Cloud tables → `basedosdados-dev`. Coverage `us`, 2010–2023.
- source Update on envirofacts `2c353fcd-44cb-431d-b854-3b5c4a37268e` (latest 2023-01-01).

## Prod promotion (pending approval)

Update the SAME dataset id on prod (`35d072bc-…`) — never create a second `ghgrp`. Re-resolve
every reference id on prod (English tag slugs; cc0 id differs: `afd7b13d-98f5-4023-9cb3-e9b91b1962ca`;
account 4). Cloud tables → `basedosdados`. Dataset stays `under_review` on prod until the PR
merges, table-approve materializes, and the tables are verified.

## Local commands

```bash
PYTHONPATH=. uv run python models/us_epa_ghgrp/code/clean_data.py [--download]
PYTHONPATH=. uv run python models/us_epa_ghgrp/code/upload.py
BD_SERVICE_ACCOUNT_DEV=~/.basedosdados/credentials/prod.json uv run dbt run --select us_epa_ghgrp
BD_SERVICE_ACCOUNT_DEV=~/.basedosdados/credentials/prod.json uv run dbt test --select us_epa_ghgrp
```

Scratch data: `~/Downloads/us_epa_ghgrp_data/` (`input/api`, `input/summary`, `output`,
`auxiliary_files`). Delete after prod publish (step 14).
