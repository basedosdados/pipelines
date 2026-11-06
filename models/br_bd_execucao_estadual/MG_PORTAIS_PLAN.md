# MG Portais — architecture plan (PROPOSED, awaiting approval)

Extends `br_bd_execucao_estadual` (dataset `d909c505-0508-475c-b1a4-459e683567f8`)
with the nine `github.com/transparencia-mg` open-data repositories.

Status: **DRAFT — not approved, nothing built.**

---

## 1. What MG already has in this dataset

MG is **already onboarded**, from the CKAN `compras_contratos` star schema
(`age7`) on `dados.mg.gov.br` — 24 staging tables `mg_dm_*` / `mg_ft_*` / `mg_fl_*`,
downloaded by `code/download_mg.py`, four state models:

| Model | Feeds | MG coverage (prod) |
|---|---|---|
| `licitacao_mg` | `licitacao` | 2009–2024 |
| `licitacao_item_mg` | `licitacao_item` | 2009–2025 |
| `despesa_mg` | `despesa` | 2002–2026 |
| `relacionamentos_mg` | `relacionamentos` | — |

**So the CKAN route is done.** The nine repos are a *different* export of the same
SIAD system — the flat "NOVA CONSULTA" spreadsheets — plus several subject areas
SIAD's dimensional model does not publish at all. That distinction drives everything
below.

### One gap already sitting in staging

`mg_ft_compras_contrato` is downloaded and loaded into MG staging
(`code/constants.py:98`) but **no dbt model reads it**. MG is consequently absent
from `contrato`, whose union is ES + SC + RS + RO only. This is fixable with no new
ingestion at all.

---

## 2. The new sources

All CC-BY-4.0, declared refresh **daily**, each carrying a Frictionless
`dataset/datapackage.json`. Data is committed to the repos, so it fetches from
`raw.githubusercontent.com` — no portal, no CAPTCHA, no Brazilian-IP requirement
(contrast `SOURCE_LESSONS.md` §1, which applies to `dados.mg.gov.br`).

| Repo | Resources | Span | Size |
|---|---|---|---|
| `portal_plano_anual_contratacao` | `pac_<ano>_{inicial,revisaoN}` ×35 | 2023–2026 | 1,397 MB |
| `portal_notas_fiscais` | `notas_<mes><aa>` ×57, `itensnota_<mes><aa>` ×57 | 2022-01–2026-09 | 889 MB |
| `portal_licitacoes_mg` | `licitacoes<ano>`, `item<ano>` | 2022–2026 | 394 MB |
| `portal_despesa_empenho` | `empenho<ano>`, `orgao<ano>` | 2022–2026 | 105 MB |
| `portal_contratos` | `contratos<ano>`, `itens<ano>` | 2022–2026 | 42 MB |
| `portal_convenios_saida` | `convenios_saida`, `pagamento<ano>`, `pagamentorp<ano>` | 2022–2026 | 24 MB |
| `portal_fiscais_contratos` | `fiscais_contratos_<ano>` | 2022–2026 | 12 MB |
| `portal_obras` | `contratos`, `contratos_siad`, `obra`, `itens`, `trechos`, `municipios`, `coordenadas`, `fiscais`, `situacao` | — | 3.7 MB |
| `portal_cafimp` / `portal_empresas_sancionadas` | `cafimp`, `empresas_sancionadas` | — | 0.8 MB |

---

## 3. The overlap decision

Three repos restate what the CKAN model already gives us for 2022–2026:

| Repo | Overlaps | Recommendation |
|---|---|---|
| `portal_licitacoes_mg` | `licitacao`, `licitacao_item` (CKAN, from 2009) | **Do not union** |
| `portal_despesa_empenho` | `despesa` (CKAN, from 2002) | **Do not union** |
| `portal_obras/contratos_siad` | `portal_contratos` subset | **Do not union** |

Reasons to keep CKAN authoritative for these three tables:

1. **Coverage.** CKAN runs 2009/2002→; the portal exports start 2022. Unioning a
   shorter series into a longer one buys nothing and creates a seam.
2. **Duplicate keys.** Both keys resolve to `numero_processo_formatado`. A union
   without a dedupe silently doubles 2022–2026; a dedupe needs a precedence rule
   maintained forever.
3. **Marginal columns are few.** Against `licitacao_mg`, the portal export adds only
   `numero_parecer_juridico` + `data_parecer_juridico`, `numero_contrato`,
   `valor_total_atualizado`, and a process-level supplier. Three of those are better
   placed elsewhere (see §4.1 and §4.4).

Instead of unioning, harvest the three columns that are genuinely new and route them
to where they belong. If you would rather union and dedupe, say so — it is a
different build and I will re-plan §4.

---

## 4. Proposed architecture

Conventions inherited without change: `ano` INT64 partition, `cluster_by=["sigla_uf"]`,
`labels={"tema": "economia"}`; one ephemeral `states/..._mg.sql` per table unioned by
the parent; `id_<x>_bd` surrogate keys prefixed `MG-`; every state model projects the
canonical columns **in the parent's exact order** (positional union — see the warning
in `licitacao_mg.sql`); columns the source lacks are explicit typed NULLs.

Table names follow MiDES vocabulary wherever MiDES already has the concept, per your
instruction to conform.

### 4.1 Existing tables gaining MG rows

**`contrato`** — new `states/br_bd_execucao_estadual__contrato_mg.sql`.
Two inputs: `mg_ft_compras_contrato` (already staged, full history) as the base, and
`portal_contratos/contratos<ano>` (2022–2026) for fields CKAN omits. Fills MG's
absence from a table that already exists for four states.

| Canonical column | From |
|---|---|
| `ano` | `ano_assinatura_contrato` |
| `data_assinatura` | `data_assinatura_contrato` |
| `data_inicio_vigencia` | `data_inicio_vigencia_contrato` |
| `data_fim_vigencia` | `data_termino_vigencia_contrato` |
| `documento_contratado` | `cnpj_cpf_fornecedor_formatado` (digits only) |
| `nome_contratado` | `nome_empresarial_nome_fornecedor` |
| `id_unidade_gestora` | `codigo_orgao_entidade_contratante` |
| `nome_unidade_gestora` | `nome_orgao_entidade_contratante` |
| `numero_contrato` | `numero_contrato` |
| `numero_processo` | `numero_processo_formatado` |
| `objeto` | `objeto_contrato` |
| `situacao` | `situacao_contrato` |
| `tipo_contrato` | `descricao_tipo_de_contrato` |
| `modalidade` | `procedimento_contratacao_especializacao` (verbatim, not recoded) |
| `valor_atual` | `valor_total_atualizado` |
| `valor_inicial` | `dm_contrato.vr_homologado` (CKAN) |
| `id_contrato_bd` | `concat('MG-', numero_processo_formatado, '-', numero_contrato)` |
| `sigla_uf` | `'MG'` |

From CKAN alone this yields **17 of 18** canonical columns; only `data_assinatura`
is absent (`dm_contrato` publishes `dt_publicacao`, not the signature date), and
`portal_contratos.data_assinatura_contrato` supplies it. `portal_fiscais_contratos`
is therefore an oversight table only, not needed for any canonical column.

**`relacionamentos`** — extend `relacionamentos_mg`.
`portal_despesa_empenho.empenho<ano>` publishes `numero_processo_compra_siad` and
`contratoconvenio_saida` beside `numero_empenho`, giving an empenho→processo→contrato
link the CKAN `fl_compras_empenho` does not carry. This is the best use of that repo.

**`dicionario`** — add MG key/value entries for every new coded column
(`situacao_contrato`, `tipo_de_documento`, `natureza`, `dstipo_penalidade`,
`situacao` in PAC, …), per the existing crosswalk convention.

### 4.2 New tables — Tier 1 (core procurement)

**`contrato_item`** ← `portal_contratos/itens<ano>`, 2022–2026. MiDES has this table;
`execucao_estadual` does not yet. Grain: contract × item.
Columns: `ano`, `sigla_uf`, `id_contrato_bd`, `id_item_bd`, `numero_processo`,
`numero_contrato`, `numero_item`, `codigo_catalogo` (`codigo_item_material_servico_numerico`),
`descricao` (`item_material_servico`), `item_despesa` + `nome_item_despesa`
(`codigo_elemento_item_despesa`, `nome_elemento_item_despesa`), `quantidade`,
`valor_unitario_referencia`, `valor_total_referencia`, `valor_total_homologado`,
`documento_contratado`, `nome_contratado`.

**`nota_fiscal`** ← `portal_notas_fiscais/notas_<mes><aa>`, 57 monthly files,
2022-01 → 2026-09. MiDES name. Grain: invoice.
Columns: `ano`, `mes`, `sigla_uf`, `id_nota_fiscal_bd`, `id_unidade_gestora`
(`orgao_emissor`), `tipo_documento`, `documento_emissor` (`fornecedor_cnpj_cpf`),
`nome_emissor`, `numero`, `serie`, `sequencial`, `situacao`, `natureza`,
`indicador_orcamento`, `data_emissao`, `data_recebimento`, `data_registro`,
`valor_total`.

**`nota_fiscal_item`** ← `itensnota_<mes><aa>`, 57 files. MiDES name. Grain: invoice × line.
Columns: `ano`, `mes`, `sigla_uf`, `id_nota_fiscal_bd`, `id_item_bd`, `numero_item`,
`codigo_catalogo`, `descricao`, `unidade_medida`, `unidade_orcamentaria`,
`item_despesa`, `quantidade`, `valor_unitario`, `valor_total`.

This pair is the largest genuinely new thing here: invoice-line detail is published
by no other state in the dataset, and it closes the chain
licitação → contrato → empenho → **nota fiscal** → liquidação → pagamento.

### 4.3 New tables — Tier 2 (planning and oversight)

**`plano_contratacao_item`** ← `portal_plano_anual_contratacao`, 2023–2026, 1.4 GB.
Grain: plan × item × revision. No equivalent in MiDES or `execucao_estadual`.
Keeps `ano_base_planejamento_de_solicitacoes` as `ano`, plus a `revisao` column
derived from the file name (`inicial` → 0, `revisaoN` → N) so planned-versus-executed
comparisons can pick a vintage. Carries `codigo_do_item_de_material_ou_servico`
(joins to `contrato_item` / `licitacao_item` on `codigo_catalogo`), `quantidade`,
`valor_unitario_previsto_r`, `valor_total_previsto_r`, and
`numero_formatado_do_planejamento_de_processo`, which links a plan line to the
process it became.

**`contrato_fiscal`** ← `portal_fiscais_contratos`, 2022–2026. Grain: contract ×
named officer. Columns: `ano`, `sigla_uf`, `id_contrato_bd`, `numero_contrato`,
`numero_processo`, `nome_gestor`, `nome_fiscal`, `id_unidade_gestora`,
`orgaos_participantes`, `url_contrato`.
Note `gestores_do_contrato_portal_de_compras` and `fiscais_do_contrato` are
multi-valued text; proposal is to keep them verbatim as STRING rather than explode,
and revisit if you want one row per officer.

**`fornecedor_sancionado`** ← `portal_cafimp` ∪ `portal_empresas_sancionadas`.
Two registries with different shapes (impediment vs administrative penalty), unioned
under one table with an `origem` discriminator. Columns: `sigla_uf`, `origem`,
`documento_fornecedor`, `nome_fornecedor`, `tipo_penalidade`, `motivo`, `sancao`,
`data_inicio`, `data_fim`, `data_publicacao`, `id_unidade_gestora`,
`nome_unidade_gestora`, `valor_multa`, `processo_sei`.
Naming is a judgement call — `sancao` is the alternative. Flagging for your call.

### 4.4 New tables — Tier 3 (adjacent)

**`convenio`** ← `portal_convenios_saida/convenios_saida` (27 cols, grant agreements
out). **`convenio_pagamento`** ← `pagamento<ano>` ∪ `pagamentorp<ano>` (2022–2026),
with a `restos_pagar` boolean distinguishing the two, since the `rp` variant swaps
`valor_pago_financeiro` for `valor_pago_processado` / `valor_pago_nao_processado`.

**`obra`** ← `portal_obras`. Nine resources, 3.7 MB. Proposal: one `obra` table from
`obra` + `contratos` + `coordenadas` + `municipios` (latitude/longitude and
municipality are the parts nothing else in the dataset has), and **drop**
`contratos_siad` (duplicates `portal_contratos`) and `itens` (duplicates
`contrato_item`). `trechos`, `situacao`, `fiscais` are small and road-specific —
I would leave them out of v1 unless you want them.

### 4.5 Deliberately not onboarded

`portal_despesa_empenho/orgao<ano>` — a 9-column agency-level aggregate, fully
derivable from `despesa`. `portal_obras/contratos_siad` and `portal_obras/itens` —
duplicates as above.

---

## 5. Source defects found while surveying

These are design constraints, not surprises to discover at clean time.

1. **CORRECTED — the `unnamed_N` columns do not exist in the published CSVs.** They
   appear only in the repos' `dataset/datapackage.json`/`.yaml`, which are generated
   from the upstream Excel and are stale. Measured on the real files: `contratos2024.csv`
   has 24 clean columns against 28 in its schema; `item2024.csv` 14 against 17;
   `licitacoes2024.csv` 26 against 27. Worse, `portal_contratos`'s schema also **omits a
   real CSV column** (`indicador_fornecedor_estrangeiro`). **Never take a column list
   from the datapackage — read the CSV header.** Nothing needs dropping.
2. **CORRECTED — the CSVs do not drift year to year.** `contratos2022` through
   `contratos2026` are byte-identical in header: same 24 columns, same order. The
   "drift" I first reported was drift between the stale *schema files*, not the data.
   `union_by_name` is still used because it costs nothing and turns a future upstream
   column addition into a widened table rather than a shifted one. The advice not to
   slice positionally stands on its own merit: the repos' `processar.py` does
   `df.iloc[:, 1:]`, which applied to these CSVs would silently discard
   `ano_assinatura_contrato`.
3. **`cafimp.cnpj_cpf_fornecedor` is typed `integer`** in the source datapackage.
   Leading zeros in CPFs are already lost there. Ingest as STRING and zero-pad;
   record the defect rather than pretending the round-trip is clean.
4. **Decimal comma, `;` delimiter, UTF-8 BOM** throughout. Dates are ISO in
   `licitacoes*` but must be checked per file — not assumed.
5. **The R$81.76 trillion row** documented in `licitacao_mg.sql` is a CKAN-side
   defect and does not affect these files, but the same process appears in
   `portal_licitacoes_mg`. Worth checking whether the flat export carries the same
   corrupted `valor_total_referencia_item_processo`; if it does not, that is an
   argument for revisiting §3.
6. **Git, not a portal.** These files are committed to Git, so a refresh is a
   `git` fetch of changed paths, not a scrape. Cheap, and it sidesteps the
   `dados.mg.gov.br` 403-to-bare-User-Agent trap in `SOURCE_LESSONS.md` §1.

---

## 6. Build order

Follows the 14-step onboarding workflow; MG is an **extension**, so architecture
tables and metadata are updates to an existing dataset, not a new registration.

1. `code/download_portais_mg.py` — fetch the nine repos' `dataset/data/*.csv` by
   pinned commit SHA, recording the SHA for reproducibility.
2. `code/clean_portais_mg.py` — name-aligned load, `unnamed_*` dropped, decimal and
   date normalisation, partitioned parquet, **all-STRING staging** per
   `bigquery-conventions`.
3. Architecture tables on Drive for the 9 new/changed tables; existing-table
   additions reuse the current architecture.
4. Upload to `basedosdados-dev` staging as `mg_portais_*`.
5. dbt: 1 new state model (`contrato_mg`), 2 amended (`relacionamentos_mg`,
   `dicionario`), 9 new tables + their `_mg` state models; add every new table to
   `constants.PUBLISHED_TABLES` and `TABLES_BY_STATE["MG"]`.
6. `dbt run` all, then `dbt test` all — **separate loops**, per
   `prefect-pipeline-conventions` (cross-table `relationships` tests read siblings).
7. Metadata in dev → verification checkpoint → prod → PR → merge → publish.
8. Recurring pipeline: the source is daily, so this warrants a Prefect flow. All
   tables `AllFree` — nothing here is high-frequency enough for `PartBdpro`.

Estimated: ~11 tables touched, ~2.9 GB raw, one new download/clean pair.

---

## 7. Questions before I build

1. **§3 overlap** — accept "CKAN stays authoritative for `licitacao`,
   `licitacao_item`, `despesa`", or do you want the portal exports unioned and
   deduped instead?
2. **Tier 3** — build `convenio`, `convenio_pagamento`, `obra` now, or defer?
   They are adjacent to procurement rather than part of it.
3. **`fornecedor_sancionado`** — that name, or `sancao`?
4. **PAC revisions** — keep every revision (35 files, 1.4 GB, `revisao` column), or
   keep only the latest vintage per year? Keeping all is bigger but lets you study
   plan revision, which is the interesting part.
5. **Municipality** — PAC and `portal_obras` carry municipality. Every existing
   table in this dataset is state-level with no `id_municipio`. Add the column on
   just those tables, or drop it?


---

## 8. Findings after drafting (supersede the above where they conflict)

**8.1 MG's counterparty identity is currently anonymised — the repos fix it.**
`mg_dm_contratado` and `mg_dm_favorecido` publish only `nr_documento_anonimizado`
and `nome_anonimizado`, and the existing models pass those straight through:
`licitacao_item_mg.sql:69-70` (`documento_vencedor`, `nome_vencedor`) and
`despesa_mg.sql:108-109` (`documento_credor`, `nome_credor`). So MG rows in
`licitacao_item` and `despesa` cannot today support any supplier-level analysis, and
nothing in the codebase says so.

The GitHub exports publish real, unmasked identity. Verified on
`portal_contratos/contratos2024.csv`: 5,333 contracts, 2,416 distinct suppliers,
zero masking markers, e.g. `21.475.971/0001-68 ACTIVIT TECNOLOGIA LTDA`.

This reframes the whole onboarding: de-anonymising MG's counterparties is arguably
worth more than any single new table, and it argues for back-filling
`licitacao_item` / `despesa` supplier fields from the repos where keys permit.

**8.2 Consequence for `contrato_mg`.** Buildable from CKAN alone, but its
`documento_contratado` / `nome_contratado` would be anonymised — a poor property for
a contracts table. Recommend building it with `portal_contratos` joined from the
start (42 MB, cheap) for real identity plus `data_assinatura`.

**8.3 The invoice tables carry no contract or process key.** Checked all 114
resources: no `numero_processo`, `numero_contrato`, `numero_empenho` or tender field
exists. `nota_fiscal` / `nota_fiscal_item` therefore join to the rest of the dataset
only probabilistically, on (agency × supplier CNPJ × catalogue code × period).
They stand up as a delivered-price panel; they do **not** give a keyed
contract-to-delivery chain. §4.2's "closes the chain" claim is too strong and is
withdrawn.

**8.4 The build DOES run locally — §8.4's earlier claim was wrong.** The 403 came
from the MCP BigQuery tool, which uses gcloud ADC (unauthenticated here, project
`pessoal-rd`). The repo's own path uses a service account and works:
`~/.basedosdados/credentials/staging.json`
(`chave-subidores-de-dados@basedosdados-dev`). dbt needs it via the env var its
`profiles.yml` reads, which is simply unset by default:

```
export BD_SERVICE_ACCOUNT_DEV="$HOME/.basedosdados/credentials/staging.json"   # dbt
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.basedosdados/credentials/staging.json"
export EXEC_ESTADUAL_DATA_DIR="$HOME/Downloads/br_state_budget_data"
```

With those set, `dbt debug` reports all checks passed. Upload to dev staging and
`dbt run` are therefore both available without any gcloud login. A gcloud login on
rdahis@basedosdados.org is still worth having for the MCP tools and ad-hoc queries, but
it is not on the critical path for this build.

**8.5 `~/Downloads` is a Dropbox symlink.** It resolves to
`.../Monash Uni Enterprise Dropbox/Ricardo Dahis/Mac/Downloads`, and `DATA_DIR`
defaults under it. Harmless for `mg_contrato` (4.2 MB), a real problem at the ~2.9 GB
full-tier scale: point `EXEC_ESTADUAL_DATA_DIR` outside Dropbox before the large repos.

**8.6 `contrato_mg` as built and verified in dev (2026-09-30).**
`mg_contrato` staging: 25,468 rows, 24 columns, all STRING, from contratos2022-2026 at
`portal_contratos@3998d827`. Per year: 5,270 / 5,375 / 5,333 / 5,794 / 3,696 (2026
partial). `numero_contrato` is unique across all five years (25,468 distinct in 25,468
rows), all-numeric, no nulls — so the join in `contrato_mg` cannot fan out and the
defensive dedupe is belt-and-braces. 6,987 distinct suppliers, zero masking markers,
100% of rows carry a signature date. All five CKAN inputs (`mg_dm_contrato`,
`mg_ft_compras_contrato`, `mg_dm_processo`, `mg_dm_orgao_contrato`, `mg_dm_contratado`,
`mg_dm_situacao_cont`) were already present in dev staging.
