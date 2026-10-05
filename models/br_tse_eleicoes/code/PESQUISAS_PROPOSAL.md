# Pesquisas eleitorais — architecture proposal for `br_tse_eleicoes`

Status: research + proposal only (2026-10-06). Nothing written to BigQuery or the backend.
Scratch data: `~/Library/Caches/br_tse_eleicoes_data/input/pesquisa_eleitoral/`
(zips, `ex/` extracted CSVs, `D.pkl` = deduplicated frames used for every number below).

## 1. What the TSE publishes

CKAN packages `pesquisas-eleitorais-{2012,2014,2016,2018,2020,2022,2024,2026}` exist; nothing
before 2012 (`package_search?q=pesquisa` returns exactly these eight). Each package has up to six
resources under `https://cdn.tse.jus.br/estatistica/sead/odsele/pesquisa_eleitoral/<family>_<ano>.zip`.

| Family | Content | Years | Zip size |
|---|---|---|---|
| `pesquisa_eleitoral` | CSV, one row per registered poll | 2012–2026 | 1–26 MB |
| `pesquisa_contratante` | CSV, one row per poll × contracting party | 2012–2026 | 0.2–0.9 MB |
| `pesquisa_pagante` | CSV, one row per poll × contracting party × payer | 2012–2026 | 0.2–0.6 MB |
| `questionario_pesquisa` | **PDF only** — the questionnaire uploaded by the pollster | 2012–2026 | 0.6–5.2 GB |
| `bairro_municipio` | **PDF only** (2012/2014 also a few .doc/.xls/.jpg) — neighbourhood/area list | 2012–2026 | 0.1–5.1 GB |
| `nota_fiscal` | **PDF only** — invoice for the poll | 2016–2026 | 0.1–1.7 GB |

The three attachment families contain no tabular data. Member lists were read remotely (HTTP range
requests on the zip central directory); nothing over 30 MB was downloaded. File names follow
`<protocolo>_<id interno TSE>_<tipo>.pdf` (2022+ `nota_fiscal`: `<protocolo>_<id>_<id nota>_nota_fiscal.pdf`).
The second token is an internal poll id that **is unique** and disambiguates protocol collisions
(see §3), but it does not appear in any CSV.

| Year | questionário PDFs | bairro/município files | nota fiscal PDFs | polls in CSV |
|---|---|---|---|---|
| 2012 | 10,978 | 4,607 | — | 10,981 |
| 2014 | 2,429 | 1,404 | — | 2,456 |
| 2016 | 8,370 | 3,894 | 4,615 | 8,259 |
| 2018 | 1,799 | 1,158 | 869 | 1,807 |
| 2020 | 11,002 | 6,092 | 5,063 | 10,971 |
| 2022 | 2,971 | 2,193 | 3,029 | 2,971 |
| 2024 | 14,893 | 13,212 | 11,298 | 14,893 |
| 2026 | 3,463 | 3,120 | 2,496 | 3,463 |

Format of the CSVs: `;`-separated, every field double-quoted (numerics included), Latin-1, CRLF,
with **embedded LF inside quoted free-text fields** (up to 7,942 rows per year in
`DS_PLANO_AMOSTRAL`). A naive line reader breaks; use a real CSV parser.

### 1.1 Which files to read (the BRASIL file is not reliable)

Each zip carries per-UF files plus a `_BRASIL.csv`. Neither is complete on its own:

- 2026 `pesquisa_eleitoral`/`contratante`/`pagante`: no `_BR.csv` or `_DF.csv`; national (1,297) and DF (75) polls exist only in `_BRASIL.csv`.
- 2016 and 2018 `pesquisa_eleitoral`: no `_BRASIL.csv` at all (2018 has `_BR`, `_DF`, and a header-only `_ZZ`).
- 2022 and 2024 `pesquisa_pagante`: `_BRASIL.csv` is empty (header only).
- 2016 and 2022 `pesquisa_contratante`: `_BRASIL.csv` and the UF union differ by 178 and 23 rows.
- 2014/2016/2018/2024 child files contain exact duplicate rows inside the same file (4 / 942 / 48 / 1).

**Rule:** read every CSV in the zip, concatenate, drop exact duplicates ignoring `DT_GERACAO`/`HH_GERACAO`.

### 1.2 Row counts after that rule

| Year | `pesquisa_eleitoral` | `contratante` | `pagante` | TSE extraction date |
|---|---|---|---|---|
| 2012 | 10,981 | 10,981 | 10,981 | 27/02/2023 |
| 2014 | 2,456 | 2,456 | 2,456 | 27/02/2023 |
| 2016 | 8,259 (8,412 raw, see §2) | 8,431 | 8,447 | 27/06/2018 |
| 2018 | 1,807 (1,926 raw, see §2) | 1,927 | 1,927 | 24/08/2022 |
| 2020 | 10,971 | 11,022 | 11,022 | 27/02/2023 |
| 2022 | 2,971 | 3,019 | 2,996 | 26/10/2024 |
| 2024 | 14,893 | 15,108 | **5,087** | 04/10/2026 |
| 2026 | 3,463 | 3,574 | 3,574 | 05/10/2026 (regenerated daily) |
| **Total** | **55,801** | **56,518** | **46,490** | |

`AA_ELEICAO` is the reference ordinary-election year: supplementary-election polls registered in
later calendar years are filed under it (2016 file: registrations to 2018-06-17; 2018 file: to
2021-12-31; 2024 file: to 2026-09-20). So 2024 is still growing.

## 2. Schema drift

### `pesquisa_eleitoral`

| Column | 2012 | 2014 | 2016 | 2018 | 2020 | 2022 | 2024 | 2026 | Note |
|---|---|---|---|---|---|---|---|---|---|
| DT_GERACAO, HH_GERACAO | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | drop |
| AA_ELEICAO, CD_ELEICAO, NM_ELEICAO | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | |
| SG_UF, SG_UE, NM_UE | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | SG_UE = 5-digit TSE municipality code (municipal years) or UF/BR (general years) |
| NR_PROTOCOLO_REGISTRO, DT_REGISTRO | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | |
| ST_PESQUISA_PROPRIA | `#NE` | `#NE` | — | — | ✓ | ✓ | ✓ | ✓ | 2012/14 filled only for 3/47 supplementary polls |
| NR_CNPJ_EMPRESA, NM_EMPRESA, NM_EMPRESA_FANTASIA | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | fantasia `#NE` in 2012/14 |
| DS_CARGO | ✓ | ✓ | **DS_CARGOS** | **DS_CARGOS** | ✓ | ✓ | ✓ | ✓ | renamed |
| DT_INICIO_PESQUISA, DT_FIM_PESQUISA | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | |
| DT_DIVULGACAO | — | — | — | — | — | ✓ | ✓ | ✓ | added 2022 |
| QT_ENTREVISTADO | ✓ | ✓ | **QT_ENTREVISTADOS** | **QT_ENTREVISTADOS** | ✓ | ✓ | ✓ | ✓ | renamed |
| CD_CONRE, NM_ESTATISTICO_RESP, VR_PESQUISA | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | |
| NR_CPF_CNPJ_CONTRATANTE, NM_CONTRATANTE, DS_ORIGEM_RECURSO, NR_CPF_CNPJ_PAGANTE, NM_PAGANTE_PESQUISA | — | — | ✓ | ✓ | — | — | — | — | **embedded child data, 2016/18 only**: one row per poll × contratante × pagante |
| DS_METODOLOGIA_PESQUISA, DS_PLANO_AMOSTRAL, DS_SISTEMA_CONTROLE, DS_DADO_MUNICIPIO | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | free text, capped at 4,000 chars (source truncation) |

Dropping the five embedded columns and deduplicating turns 2016 from 8,412 to 8,259 rows and 2018
from 1,926 to 1,807 rows.

### `pesquisa_contratante` and `pesquisa_pagante`

Identical headers in all eight years:
- contratante: `DT_GERACAO; HH_GERACAO; AA_ELEICAO; NR_PROTOCOLO_REGISTRO; CD_CONTRATANTE; NR_CPF_CNPJ_CONTRATANTE; NM_CONTRATANTE; VR_PAGO_CONTRATANTE; ST_CONTRATANTE_PAGANTE; DS_ORIGEM_RECURSO`
- pagante: `DT_GERACAO; HH_GERACAO; AA_ELEICAO; NR_PROTOCOLO_REGISTRO; CD_CONTRATANTE; DS_ORIGEM_RECURSO; NR_CPF_CNPJ_PAGANTE; NM_PAGANTE`

The drift is in content, not headers (see §4): `CD_CONTRATANTE`, `NR_CPF_CNPJ_CONTRATANTE`
and `VR_PAGO_CONTRATANTE` are sentinels in 2012/2014; `DS_ORIGEM_RECURSO` is free text in
2012/2014 (2,548 distinct values in 2012) and a closed list from 2016.

## 3. Keys and joins (checked empirically)

**Poll id = `NR_PROTOCOLO_REGISTRO`** (format `UU#####AAAA`, e.g. `RN022772026` = UF + sequence + year).
Unique within year in 2012, 2020, 2022, 2024, 2026. Not unique in three years:

| Year | Protocols reused by different polls | Extra rows | What differs |
|---|---|---|---|
| 2014 | 1 (`BR008842014`: IBOPE and MARK) | 1 | firm, dates, sample, cost |
| 2016 | 221 | 226 | municipality (216), firm (204), dates, cost |
| 2018 | 12 | 12 | firm, dates, cost |

These are distinct polls sharing a protocol, not duplicates; the questionário zip confirms it
(220 protocols in 2016 and 12 in 2018 have two or more internal TSE ids). The CSVs carry no field
that tells them apart, so (1) the main table keeps every row, (2) the uniqueness test needs a 5%
tolerance, and (3) child rows of a reused protocol cannot be attributed to one of its polls. For
2016/18 the embedded `NR_CPF_CNPJ_CONTRATANTE` in the main file can match a child row to its poll;
that resolves most but not all of them.

**Child joins** (all on `AA_ELEICAO` + `NR_PROTOCOLO_REGISTRO`):

| Year | contratante key `(prot, CD_CONTRATANTE)` unique | contratante protocols not in main | main protocols without contratante | pagante key `(prot, CD_CONTRATANTE, NR_CPF_CNPJ_PAGANTE)` unique |
|---|---|---|---|---|
| 2012 | yes (CD = −1, 1 row per poll) | 0 | 0 | yes |
| 2014 | 1 dup (the reused protocol) | 0 | 0 | 1 dup |
| 2016 | yes | 114 | 35 | yes |
| 2018 | yes | 1 | 0 | yes |
| 2020 | yes | 0 | 0 | yes |
| 2022 | yes | 0 | 0 | yes |
| 2024 | yes | 0 | 0 | 4 dups (differ only in `DS_ORIGEM_RECURSO`) |
| 2026 | yes | 0 | 0 | yes |

- `CD_CONTRATANTE` is the TSE's sequence for the contracting **entity**, reused across polls (e.g. 215 appears on 398 polls in 2026). It is not a row id. It is `-1` for 10,978 of 10,981 rows in 2012 and 2,409 of 2,456 in 2014.
- Contratantes per poll: 1 for 97–99% of polls; maximum 6.
- Every pagante row matches a contratante `(prot, CD_CONTRATANTE)`. Pagante is 1:1 with contratante except 2016, where one contratante has up to 6 individual payers (e.g. `CE043962016`, six private CPFs, "Doações Eleitorais").
- **Pagante adds almost no information after 2016.** Its document equals the contratante's in 100% of matched rows in 2018, 2020, 2022 and 2026 (2012: 94.7%, 2014: 97.8%, 2016: 99.5%, 2024: 99.8%), including when `ST_CONTRATANTE_PAGANTE = 'N'`.
- **Pagante 2024 is broken at the source:** 17 of 26 UF files are header-only and `_BRASIL.csv` is empty. Only RJ, RN, RO, RR, RS, SC, SE, SP and TO have rows, so 10,032 of 15,108 contratante rows have no pagante. 2022 pagante lacks AC (23 rows).
- `sum(VR_PAGO_CONTRATANTE)` equals `VR_PESQUISA` for 94–98% of polls in 2016–2022; 87% in 2024 and 55% in 2026, because non-paying contratantes switched from `-1` to `0,00`.

## 4. Data-quality traps

1. **Sentinels.** Leiame: `#NULO`/`#NULO#` = blank (numeric `-1`); `#NE`/`#NE#` = not collected that year (numeric `-3`). Observed: `#NE` (no trailing #) in `ST_PESQUISA_PROPRIA` and `NM_EMPRESA_FANTASIA` 2012/14; `#NULO#` in `NM_EMPRESA_FANTASIA`, `DS_CARGO` (15 rows over 2020–2026), `DS_DADO_MUNICIPIO` (194–3,131 per year), `DS_ORIGEM_RECURSO`; `-1` in `CD_CONTRATANTE`, `NR_CPF_CNPJ_*`, `VR_PAGO_CONTRATANTE`; `-3` in `VR_PAGO_CONTRATANTE` 2012/14; empty strings in `DS_DADO_MUNICIPIO`, `CD_CONRE`, `NM_ESTATISTICO_RESP`. Map all to NULL.
2. **Masked documents.** 2020 contratante/pagante have 6 documents of underscores (`______________`, `___________`). NULL.
3. **Two date formats.** 2016 and 2018 use `DD/MM/YYYY`; every other year uses `YYYY-MM-DD HH:MM:SS`. `DT_REGISTRO` carries a real time only from 2022 (2020: 31 rows); earlier times are `00:00:00`. `DT_INICIO`/`DT_FIM`/`DT_DIVULGACAO` are always midnight. Checks: `inicio > fim` 0 rows; `divulgacao < registro` 0 rows; `divulgacao - registro` = 5 days (min 1, max 6), the legal wait. `registro > inicio` is common (up to 6,332 rows in 2024) and legal.
4. **Money.** `VR_PESQUISA`, `VR_PAGO_CONTRATANTE`: comma decimal, no thousands separator (`46204,00`). `VR_PESQUISA = 0` in 559 polls in 2016 (in 9 in 2012, 2 in 2014, 1 in 2018); keep as 0. `VR_PAGO_CONTRATANTE`: `-3` 2012/14, `-1` when `ST_CONTRATANTE_PAGANTE='N'` in 2016–2022, `0,00` for the same case in 2024–2026. Recommend `-1`/`-3` → NULL, keep 0, and document the 2024 switch.
5. **Negative sample sizes.** `QT_ENTREVISTADO` is `-300`, `-401`, `-260` (2020) and `-2000` (2026). Recommend NULL plus a note (the absolute value is plausible but undocumented). Maximum is 451,790 (2016); flag, don't drop.
6. **Multi-valued cargo.** `DS_CARGO` is a comma list in unstable order (`Vereador, Prefeito` 1,728 vs `Prefeito, Vereador` 512 in 2016; `Prefeito, Prefeito` once). 2018 has 80 distinct orderings. Normalize: split, `clean_string`, dedupe, sort in a fixed order (presidente, governador, senador, deputado federal, deputado estadual, deputado distrital, prefeito, vereador), and join with `, `.
7. **`DS_ORIGEM_RECURSO`** is free text in 2012/14 (`proprio` 1,371, `proprios` 1,261, `recursos proprios` 907, … after `clean_string`). From 2016 it takes 5 values (`Recursos Próprios`, `Fundo Partidário`, `Doações Eleitorais`, `Outros`, `#NULO#`), sometimes joined with ` / `. Keep the cleaned text and do not attempt to harmonize 2012/14.
8. **`CD_CONRE`** is free text (`6208-A`, `7877-A 1° REG`, `9999 / 9ª região`). Keep as STRING; do not parse.
9. **Free-text fields capped at 4,000 chars.** Maximum lengths are 3,999–4,000, so some text is truncated at the source. They contain CR/LF; normalize CRLF → LF and keep the text verbatim (no `clean_string`).
10. **`id_eleicao` is coarse.** All supplementary polls of a cycle share one `CD_ELEICAO` (2012 `2`, 2014 `4`, 2016 `3`, 2018 `7`, 2020 `8`, 2024 `61`). It does not identify the specific supplementary election.
11. **Zero-padded identifiers.** CNPJ (14), CPF (11) and `SG_UE` (5) must be read as strings.
12. **Live file.** 2026 is regenerated daily (`DT_GERACAO` 05/10/2026), and the 2024 file still receives supplementary polls. A recurring pipeline is warranted (overwrite per year).

## 5. Proposed tables

Four candidate tables were considered. A table for the questionários, bairros and notas fiscais is
**not** proposed: they are 0.1–5.2 GB of PDFs per year. They should be listed as link-only documents in
the auxiliary-files README of `pesquisa_eleitoral`, with the zip URLs and the filename pattern.
An optional fifth table, `pesquisa_eleitoral_documento` (ano, id_pesquisa, id_pesquisa_tse,
tipo_documento, nome_arquivo), could be built from the zip central directories without downloading
the PDFs. Its only analytic value is the internal id that splits reused protocols, and it cannot be
joined back to those CSV rows. Defer it.

General choices, following `candidatos`/`receitas_candidato`:
- Partition `ano` INT64 (= `AA_ELEICAO`), range `start 2012, end 2030, interval 2`.
- `sigla_uf` keeps `BR` for national polls (precedent: `custom_relationships … ignore_values: [BR]` in `schema.yml`).
- `id_municipio_tse` = `SG_UE` when it is 5 digits (municipal years); NULL in general years. `id_municipio` is mapped from it through the directory, as the other tables do.
- `id_eleicao` ← `CD_ELEICAO`; `tipo_eleicao` ← `clean_election_type_series(clean_string_series(NM_ELEICAO))`, which yields `eleicao ordinaria` and, for example, `eleicoes municipais suplementares 2024`.
- Categorical/name strings pass through `clean_string`; long free-text descriptions do not.
- Column names follow the dataset (`cpf_cnpj_doador` → `cpf_cnpj_contratante`; `cnpj_candidato` → `cnpj_empresa`).
- Dropped from the source: `DT_GERACAO`, `HH_GERACAO`, `NM_ELEICAO` (→ `tipo_eleicao`), `NM_UE` (directory), and the five embedded child columns of 2016/18.

Coverage notation: `2012(2)2026` = every file; empty cell = same as table.

### 5.1 `pesquisa_eleitoral` — one row per registered poll (55,801 rows)

| name | bigquery_type | description | temporal_coverage | covered_by_dictionary | directory_column | measurement_unit | has_sensitive_data | original_name |
|---|---|---|---|---|---|---|---|---|
| ano | INT64 | Ano da eleição ordinária de referência | | no | br_bd_diretorios_data_tempo.ano:ano | year | no | AA_ELEICAO |
| sigla_uf | STRING | Sigla da unidade da federação da pesquisa (BR para pesquisas nacionais) | | no | br_bd_diretorios_brasil.uf:sigla | | no | SG_UF |
| id_municipio | STRING | ID Município - IBGE 7 dígitos (pesquisas municipais) | 2012(4)2024 | no | br_bd_diretorios_brasil.municipio:id_municipio | | no | derived from SG_UE |
| id_municipio_tse | STRING | ID Município - TSE (pesquisas municipais) | 2012(4)2024 | no | br_bd_diretorios_brasil.municipio:id_municipio_tse | | no | SG_UE |
| id_eleicao | STRING | ID da eleição no TSE | | no | | | no | CD_ELEICAO |
| tipo_eleicao | STRING | Tipo da eleição (ordinária ou suplementar) | | no | | | no | NM_ELEICAO |
| id_pesquisa | STRING | Número do protocolo de registro da pesquisa no TSE | | no | | | no | NR_PROTOCOLO_REGISTRO |
| cnpj_empresa | STRING | CNPJ da empresa contratada para realizar a pesquisa | | no | | | no | NR_CNPJ_EMPRESA |
| nome_empresa | STRING | Razão social da empresa contratada para realizar a pesquisa | | no | | | no | NM_EMPRESA |
| nome_fantasia_empresa | STRING | Nome fantasia da empresa contratada para realizar a pesquisa | 2016(2)2026 | no | | | no | NM_EMPRESA_FANTASIA |
| pesquisa_propria | STRING | Indica se a pesquisa foi realizada pela própria empresa registrante | 2020(2)2026 | yes | | | no | ST_PESQUISA_PROPRIA |
| cargos | STRING | Cargos eleitorais abrangidos pela pesquisa, separados por vírgula | | no | | | no | DS_CARGO / DS_CARGOS |
| data_registro | DATE | Data do registro da pesquisa no TSE | | no | br_bd_diretorios_data_tempo.data:data | | no | DT_REGISTRO |
| hora_registro | TIME | Hora do registro da pesquisa no TSE | 2022(2)2026 | no | | | no | DT_REGISTRO |
| data_inicio | DATE | Data de início da coleta da pesquisa | | no | br_bd_diretorios_data_tempo.data:data | | no | DT_INICIO_PESQUISA |
| data_fim | DATE | Data de término da coleta da pesquisa | | no | br_bd_diretorios_data_tempo.data:data | | no | DT_FIM_PESQUISA |
| data_divulgacao | DATE | Data a partir da qual o resultado da pesquisa pode ser divulgado | 2022(2)2026 | no | br_bd_diretorios_data_tempo.data:data | | no | DT_DIVULGACAO |
| quantidade_entrevistados | INT64 | Número de pessoas entrevistadas | | no | | person | no | QT_ENTREVISTADO / QT_ENTREVISTADOS |
| valor_pesquisa | FLOAT64 | Custo declarado da pesquisa | | no | | BRL | no | VR_PESQUISA |
| registro_conre_estatistico | STRING | Número de registro do estatístico responsável no Conselho Regional de Estatística (CONRE) | | no | | | no | CD_CONRE |
| nome_estatistico | STRING | Nome do estatístico responsável pela pesquisa | | no | | | yes | NM_ESTATISTICO_RESP |
| descricao_metodologia | STRING | Descrição da metodologia da pesquisa | | no | | | no | DS_METODOLOGIA_PESQUISA |
| descricao_plano_amostral | STRING | Plano amostral e ponderação por sexo, idade, instrução e nível econômico, com intervalo de confiança e margem de erro | | no | | | no | DS_PLANO_AMOSTRAL |
| descricao_sistema_controle | STRING | Sistema interno de controle, verificação e fiscalização da coleta de dados | | no | | | no | DS_SISTEMA_CONTROLE |
| descricao_area_abrangencia | STRING | Bairros ou área de coleta abrangidos pela pesquisa | | no | | | no | DS_DADO_MUNICIPIO |

Observations to record: `pesquisa_propria` also exists for 47 (2014) and 3 (2012) supplementary
polls, and its dictionary keys are `S`/`N`. `quantidade_entrevistados` negatives → NULL (4 rows).
`id_pesquisa` is reused by distinct polls in 2014 (1), 2016 (221) and 2018 (12). The description
fields are truncated by the TSE at 4,000 characters.

dbt tests: `custom_unique_combinations_of_columns [ano, id_pesquisa]`
`proportion_allowed_failures: 0.05`; `not_null` on `ano`, `id_pesquisa`; `custom_relationships` on
`sigla_uf` ignoring `BR`; `not_null_proportion_multiple_columns at_least 0.05` with
`ignore_values` = `[id_municipio, id_municipio_tse, hora_registro, data_divulgacao, pesquisa_propria]`
(structurally NULL in some years).

### 5.2 `pesquisa_eleitoral_contratante` — one row per poll × contracting party (56,518 rows)

| name | bigquery_type | description | temporal_coverage | covered_by_dictionary | directory_column | measurement_unit | has_sensitive_data | original_name |
|---|---|---|---|---|---|---|---|---|
| ano | INT64 | Ano da eleição ordinária de referência | | no | br_bd_diretorios_data_tempo.ano:ano | year | no | AA_ELEICAO |
| id_pesquisa | STRING | Número do protocolo de registro da pesquisa no TSE | | no | | | no | NR_PROTOCOLO_REGISTRO |
| id_contratante | STRING | Código sequencial do contratante no cadastro do TSE | 2016(2)2026 | no | | | no | CD_CONTRATANTE |
| cpf_cnpj_contratante | STRING | CPF ou CNPJ do contratante da pesquisa | 2016(2)2026 | no | | | yes | NR_CPF_CNPJ_CONTRATANTE |
| nome_contratante | STRING | Nome do contratante da pesquisa na Receita Federal | | no | | | yes | NM_CONTRATANTE |
| contratante_pagante | STRING | Indica se o contratante é também o pagante da pesquisa | | yes | | | no | ST_CONTRATANTE_PAGANTE |
| valor_pago | FLOAT64 | Valor pago pelo contratante | 2016(2)2026 | no | | BRL | no | VR_PAGO_CONTRATANTE |
| origem_recurso | STRING | Origem dos recursos usados para pagar a pesquisa | | no | | | no | DS_ORIGEM_RECURSO |

Observations: `id_contratante` is `-1` (NULL) in all but 3 (2012) and 47 (2014) rows, and
`cpf_cnpj_contratante` is `-1` in all but 863 (2012) and 242 (2014) rows. `valor_pago` is NULL
(`-1`) when `contratante_pagante = 'N'` in 2016–2022, and 0 in 2024–2026. `origem_recurso` is free
text in 2012/2014. Dictionary keys for `contratante_pagante`: `S`/`N`.

dbt tests: `custom_unique_combinations_of_columns [ano, id_pesquisa, id_contratante, cpf_cnpj_contratante]`
with 0.05 tolerance; `relationships`-style coverage check of `(ano, id_pesquisa)` against
`pesquisa_eleitoral` is **not** advisable as a hard test (2016 has 114 orphan protocols).

### 5.3 `pesquisa_eleitoral_pagante` — one row per poll × contratante × payer (46,490 rows)

| name | bigquery_type | description | temporal_coverage | covered_by_dictionary | directory_column | measurement_unit | has_sensitive_data | original_name |
|---|---|---|---|---|---|---|---|---|
| ano | INT64 | Ano da eleição ordinária de referência | | no | br_bd_diretorios_data_tempo.ano:ano | year | no | AA_ELEICAO |
| id_pesquisa | STRING | Número do protocolo de registro da pesquisa no TSE | | no | | | no | NR_PROTOCOLO_REGISTRO |
| id_contratante | STRING | Código sequencial do contratante no cadastro do TSE | 2016(2)2026 | no | | | no | CD_CONTRATANTE |
| cpf_cnpj_pagante | STRING | CPF ou CNPJ do pagante da pesquisa | 2016(2)2026 | no | | | yes | NR_CPF_CNPJ_PAGANTE |
| nome_pagante | STRING | Nome do pagante da pesquisa na Receita Federal | | no | | | yes | NM_PAGANTE |
| origem_recurso | STRING | Origem dos recursos usados para pagar a pesquisa | | no | | | no | DS_ORIGEM_RECURSO |

Observation (must appear in the table description): 2024 covers only RJ, RN, RO, RR, RS, SC, SE,
SP and TO (5,087 of 15,108 contratante rows); 2022 lacks AC. `origem_recurso` is `#NULO#` for
95–100% of rows from 2018 on.

**Alternative (recommended for discussion):** do not publish this table. From 2018 the payer equals
the contracting party in 100% of matched rows. The 2024 file is two-thirds empty, and the only
substantive content is 2012–2016 (where payer ≠ contratante in 0.5–5.3% of rows). Folding it into
the contratante table is impossible because of the 2016 1:n cases.

### 5.4 `dicionario` additions

| id_tabela | nome_coluna | chave | cobertura_temporal | valor |
|---|---|---|---|---|
| pesquisa_eleitoral | pesquisa_propria | S | | Sim (pesquisa realizada pela própria empresa) |
| pesquisa_eleitoral | pesquisa_propria | N | | Não (empresa diferente) |
| pesquisa_eleitoral_contratante | contratante_pagante | S | | Sim (contratante é o pagante) |
| pesquisa_eleitoral_contratante | contratante_pagante | N | | Não |

## 6. Sensitive data

- `cpf_cnpj_contratante` holds an 11-digit CPF of a natural person in 567 (2016), 11 (2018),
  518 (2020), 17 (2022), 1,054 (2024) and 53 (2026) rows. These are mostly candidates and individual
  donors. `cpf_cnpj_pagante` holds CPFs in 590, 11, 518, 17, 134 and 53 rows. The 2016 example
  `CE043962016` lists six private individuals as payers.
- `nome_contratante`/`nome_pagante` hold the matching personal names; `nome_estatistico` is a
  natural person acting in a professional capacity.
- Precedent: `receitas_candidato.cpf_cnpj_doador` and `candidatos.cpf` are published unmasked,
  as the TSE publishes them. Recommendation: publish as-is for consistency and set `has_sensitive_data = yes`.
  The alternative is to mask 11-digit values to the government's LGPD pattern (`***.456.789-**`) and leave CNPJs intact.

## 7. Open questions

1. Publish `pesquisa_eleitoral_pagante` at all (§5.3), given it duplicates contratante from 2018 and 2024 is two-thirds missing?
2. CPF treatment (§6): unmasked as in `receitas_candidato`, or masked?
3. Name of the poll id: `id_pesquisa` (style-conformant, joinable) vs `protocolo_registro` (closer to the source and to how polls are cited, e.g. "BR-02316/2026"), given the id is not unique in 2014/2016/2018.
4. Reused protocols in 2016/18: accept them as-is with a tolerance test, or attempt attribution of child rows via the embedded contratante document in the 2016/18 main file? The second option resolves most but not all of the 226 + 12 cases.
5. `cargos` as one normalized comma-separated string (proposed), or a long bridge table `pesquisa_eleitoral_cargo` (ano, id_pesquisa, cargo)? The bridge table matches `cargo` semantics in the rest of the dataset.
6. Recurring pipeline: 2026 is regenerated daily and 2024 still grows with supplementary polls. A refresh that overwrites the current and previous cycle would keep both current.
7. `QT_ENTREVISTADO` negatives (4 rows): NULL (proposed) or absolute value?
