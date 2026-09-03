# Backend ids — us_irs_form990 (slug `form990`)

## staging (env="staging"), account 57

- organization `irs`: c158f9f1-26c3-4453-ac6a-48f466643d50 (created 2026-09-03)
- dataset `form990`: 65b0a332-503b-4864-a75d-6351384b0fae (status under_review 47208305-…)
- themes: economics ad6a413a-…, government 6dd730bb-…
- tags: terceiro_setor 5cc3edd7-…, registry b7cd4276-…, financial-statement 208714aa-…, balance-sheet 2399b8b7-…, taxacao c9d46f34-…
- license cc0 7fb71004-…; availability online dd396d7d-…; language en 0420c9c1-…; area us 61a2c232-c649-4b41-a5a3-1467b7393e11
- raw data sources: efile 5fa0e301-8daa-44c9-821a-76b281f7c022; bmf 60ba6956-efc7-48bb-8284-e54142ee81d0; revocation bf1aa50c-8f06-46bf-97c8-33954eddbcc3

| table | id | OLs (entity → id) | cloud table |
|---|---|---|---|
| organization | bfbd8cf0-7bdf-4e8c-8fab-5749d3a7a097 | ngo ab6abfdb-506c-4b04-aa63-408fb6f6782b; month 5db8c5f3-8c0c-43af-ab29-787fcf8fa224 | dd5d9232-… |
| return_financial | cb85f0ad-7f5e-4fc0-b597-2cca1313ee57 | ngo 5c680a95-a9b0-4f27-a78a-4f5f81dc5677; year 635429a3-344d-43ee-bf07-a0c7657f41e3 | f366ddd2-… |
| compensation | 2f484ec2-b62f-4724-984c-c0352a7a1a66 | ngo 10507d8a-3ec3-4ab0-af60-f383751f11e1; person 30cb293f-37dd-48f1-8b77-2cff0e83f290; year e567fdde-f731-4ac9-804f-3fd10235acc8 | 0fb6e0f8-… |
| revocation | 66e97eea-c8d9-4600-97ba-fc82b46d39e4 | ngo 61bedfea-f9a4-41af-8ea4-10cf8a63f5d6 | 56dd7a2f-… |
| dicionario | 8e5493af-5a3e-45a8-9332-cce4f2e277d3 | — | e3c0fb56-… |

entities: ngo 8dd563db-4e1e-4295-83cd-328565337bae, month f9659fea-…, year e1bf146e-…, person b4e76213-…

coverages (staging, area us, free): organization b6216cd1-b2b3-4b78-bebb-af5c5e06cda7 (range 2a6d0dff-…, 2026-08-10),
return_financial 9dcdf7b3-d3a0-42e1-83b9-970a01211c58 (range ec670adc-…, 2014–2025),
compensation b90dccb4-e57f-47f2-8009-f71e3c013cd9 (range 220084a0-…, 2014–2025),
revocation 2a238e2e-be3b-4c16-8bdb-28244ec0253e (range 8423ed11-…, 2010-05-15..2026-05-15)
updates: tables (month, latest 2026-09-03) organization fc876acd-…, return_financial f47383fe-…,
compensation b3534327-…, revocation 22574fc5-…; sources efile c525f2a8-… (2026-08-19),
bmf 9986ca9a-… (2026-08-10), revocation 53df28f3-… (2026-09-01)
Drive folder: BD/Dados/Conjuntos/us_irs_form990 = 1Hw8DUxQjEGOA1692tf5r1yuLSpYI42mG (5 architecture CSVs)

## prod (env="prod"), account 4

- org `irs`: 3797243b-fedb-4f02-a6f2-c4d891f9abdf (created 2026-09-03)
- dataset `form990`: e81c3465-adfd-45af-84de-a2adfd01899b (status under_review)
- tags differ from staging: prod uses `ngo` 956756f8-905d-45f8-8cee-df0807a7c335 where
  staging uses `terceiro_setor`; financial-statement 36ddc456-…, balance-sheet 0403592d-…,
  registry 09a24850-…, taxation c9d46f34-… (same id as staging)
- license cc0 afd7b13d-98f5-4023-9cb3-e9b91b1962ca (differs from staging)
- raw sources: efile 22f8c2d3-c651-4823-900c-bdfb9313571e, bmf f2ccd536-3b6e-4253-b1c2-fd5ca3c72bbb,
  revocation 03652fcf-a34e-49d1-b996-d2ac9d4c6b27

| table | id | cloud table | coverage |
|---|---|---|---|
| organization | 75f8c3b8-d89d-4196-8131-9c64afab298f | 287c0f73-… | 33387aa7-… / range 7f03885c-… |
| return_financial | 2dde900d-60cc-493e-a5d9-29e874fed030 | bf456f4e-… | bb0dc5f3-… / range 8071b9d9-… |
| compensation | e8722aec-643f-4fca-9fb2-61561fa834d1 | e43b032e-… | 7c2a22cd-… / range 43c7eca1-… |
| revocation | 691a5765-fee6-4046-b722-61e5ec46b6dd | 086f354c-… | b5a631b7-… / range 4990a1d1-… |
| dicionario | 887ccfbb-0296-442f-9456-fcf5ce8b933f | 8a73407f-… | — |

prod OLs: organization ngo 60411b14-…, month 8dadac6b-…; return_financial ngo d0f34d0e-…,
year 423ae74b-…; compensation ngo e0d89a42-…, person 2bf98a56-…, year 3c3c9066-…;
revocation ngo 315fce3b-…

## Directory links — what the backend accepts

`year` links to `diretorios_data_tempo.ano:ano` on both backends (note the backend slug,
not the GCP dataset id `br_bd_diretorios_data_tempo`). The state columns carry **no**
directory link: the directory's primary key is `id_state` (FIPS) and the backend rejects
any other target with `Faça uma escolha válida`. `bulk_upsert_columns` reports `updated`
for a `directory_column` it does not actually write — only `update_column` writes it, and
it errors loudly on an invalid target. (ids differ; re-resolve everything)
