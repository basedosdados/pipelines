# Onboarding Plan — br_mf_divida_ativa (PGFN Dívida Ativa da União)

**Status:** DRAFT (awaiting approval)
**Org:** `br_mf` (Ministério da Fazenda) — new org, may need creation in backend
**GCP dataset id:** `br_mf_divida_ativa` · **backend slug:** `divida_ativa`
**Source:** https://www.gov.br/pgfn/pt-br/assuntos/divida-ativa-da-uniao/transparencia-fiscal-1/dados-abertos
**Raw host:** `https://dadosabertos.pgfn.gov.br/<YEAR>_trimestre_<QQ>/Dados_abertos_<CATEGORY>.zip`
**License:** LAI / dados abertos (public). **Cadence:** quarterly. **BD Pro:** `part_bdpro` on all three tables — last **two quarters** paywalled (user directive).
**Coverage:** 2020 Q1 → 2026 Q2 (26 quarters; pre-2020 = 404). FGTS absent in 2020 Q1.

## Decisions (approved)
1. **3 tables**, one per source system.
2. **Full history** backfill 2020 Q1 → 2026 Q2.
3. **New dataset** `br_mf_divida_ativa`; leave `lista_de_devedores_da_pgfn` (org `me`) stub untouched.
4. **BD-Pro:** last two quarters paywalled on all tables. Run scope this session: **through dev metadata only — stop before the PR and before any prod promotion.**

## BD-Pro (part_bdpro) — how the two-quarter paywall works
- CoverageSpec (all 3 tables): `PartBdpro(date_column=YearQuarter(year="ano", quarter="trimestre"), date_format=YEAR_MONTH, free_lag=FreeLag("months", 6))`.
- Framework builds the filter date as `DATE(ano, trimestre*3, 1)` (Q1→Mar…Q4→Dec). With `source_end`=2026 Q2 (2026-06-01), `free_end` = 2025-12-01 ⇒ free ≤ 2025 Q4, pro = 2026 Q1–Q2. Rolls forward automatically each quarter.
- Requires **two coverages per table** in the backend: free (`is_closed=False`) + pro (`is_closed=True`), each with a DateTimeRange (`is_closed` set to match). Register both at dev metadata; `assert_coverage_topology` hard-fails otherwise.
- **Enforcement (Row Access Policies) is a prod-worker action** applied later by `register_table_materialization_task`; it is NOT applied in dev and is out of scope this session. Dev metadata only encodes the free/pro topology.

## Tables
| Table slug | System | Source file | Rows/qtr (est.) | Compressed/qtr | Cols |
|---|---|---|---|---|---|
| `nao_previdenciario` | SIDA | `Dados_abertos_Nao_Previdenciario.zip` | ~40–50 M | 1.27 GB | 15 |
| `previdenciario` | PREV | `Dados_abertos_Previdenciario.zip` | ~2–3 M | 80 MB | 15 |
| `fgts` | FGTS | `Dados_abertos_FGTS.zip` | ~0.5 M | 17 MB | 17 |

Grain = one row per (inscrição × devedor) in the quarterly snapshot; a single `numero_inscricao`
can repeat across `tipo_devedor` (Principal / Corresponsável / Solidário).

## Column schema (Portuguese; Brazilian dataset)
Common core (SIDA & PREV = these 15; FGTS = these + 2):
1. `ano` INT64 — partition — folder year
2. `trimestre` INT64 — 1–4 — folder quarter
3. `sigla_uf` STRING — dir FK `br_bd_diretorios_brasil.uf:sigla_uf` ← `UF_DEVEDOR`
4. `cpf_cnpj` STRING — CPF (mascarado) / CNPJ · has_sensitive_data=yes
5. `tipo_pessoa` STRING ← `TIPO_PESSOA`
6. `tipo_devedor` STRING ← `TIPO_DEVEDOR`
7. `nome_devedor` STRING · has_sensitive_data=yes ← `NOME_DEVEDOR`
8. `unidade_responsavel` STRING ← `UNIDADE_RESPONSAVEL`
9. `numero_inscricao` STRING ← `NUMERO_INSCRICAO`
10. `tipo_situacao_inscricao` STRING ← `TIPO_SITUACAO_INSCRICAO`
11. `situacao_inscricao` STRING ← `SITUACAO_INSCRICAO`
12. `receita_principal` STRING ← `RECEITA_PRINCIPAL` (PREV source col = `TIPO_CREDITO`)
13. `data_inscricao` DATE ← `DATA_INSCRICAO` (dd/mm/yyyy; `01/01/1000` → NULL)
14. `indicador_ajuizado` STRING (SIM/NAO) ← `INDICADOR_AJUIZADO`
15. `valor_consolidado` FLOAT64 · measurement_unit=BRL ← `VALOR_CONSOLIDADO`

FGTS-only, inserted after `unidade_responsavel`:
- `entidade_responsavel` STRING ← `ENTIDADE_RESPONSAVEL`
- `unidade_inscricao` STRING ← `UNIDADE_INSCRICAO`

All categorical columns store readable labels ⇒ `covered_by_dictionary = no`; **no dicionario table**.

## Data location & cleanup
- All raw archives + cleaned parquet live under **`~/Downloads/br_mf_divida_ativa_data/`** (`input/`, `output/`) — outside Dropbox/the repo; override via `PGFN_DATA_ROOT`.
- **Final step of this session: delete `~/Downloads/br_mf_divida_ativa_data/` entirely** once dev upload + dbt + dev metadata are done (data is fully reproducible from source). Codified as step 14 in `onboarding-workflow.md`.

## Phase C complete — dev/staging metadata registered (staging backend, status under_review)
Registered on `staging` backend (env=staging; dev backend 503). Dataset `divida_ativa` id `744fc0f1-c805-43d6-ad1a-a0fcabf7fac4`, org `mf`, theme `economics`.
- Tables: `nao_previdenciario` `f5585610-42b1-4b6a-bed1-f3cc7cb8a2f8` · `previdenciario` `609fa07e-ca10-4e15-96b6-eb7659b8c4fb` · `fgts` `b9223d6e-0192-4d30-a65a-6942cccd3d53` (status published; gated by dataset under_review).
- Per table: columns (17/15/15, `measurement_unit=real` accepted), OL entity `other`←`numero_inscricao`, `ano` is_partition, cloud_table→`basedosdados-dev.br_mf_divida_ativa.*`, raw source `970a83ef-...` linked, quarterly table Update.
- **Paywall topology** per table: free coverage 2020-03..2025-12 (`is_closed=False`) + pro coverage 2026-03..2026-06 (`is_closed=True`).
- Scratch data (`~/Downloads/br_mf_divida_ativa_data`, 32 GB) deleted (step 14).

### Remaining (NOT done this session — needs decisions/approval)
1. **dbt run/test in dev** was skipped (query quota). Either re-run when quota resets / raised, or rely on prod table-approve to materialize + validate the `safe_cast` model on merge.
2. **PR** (step 11): commit the code and open the onboarding PR. **Nothing is committed yet** — all code is in the worktree only.
3. **Prod promotion** (step 10) + **publish** (step 13): register prod metadata `under_review`, merge → table-approve materialises `basedosdados.br_mf_divida_ativa.*`, verify, then flip to published. Update cloud tables to `basedosdados` at prod.
4. **Recurring quarterly pipeline** (step 12): build `pipelines/datasets/br_mf_divida_ativa/` reusing `code/divida_ativa.py`; the `part_bdpro` coverage topology is already in place so the rolling paywall + Row Access Policies apply on the deployed worker.
5. Refine before publish: observation-level entity (`other` is provisional — consider a custom debt/inscrição entity); dataset tags; raw-source license (`unknown` → confirm).
6. Known FK fix applied: UF directory column is `sigla` (not `sigla_uf` as the style doc says) — consider correcting `data-basis-style.md`.

## Cleaning notes
- Encoding Latin-1 → UTF-8; `;` delimiter; process each CSV part **streaming** (SIDA never loaded whole).
- `valor_consolidado`: dot-decimal already; `.00` → 0.0; safe_cast float.
- `data_inscricao`: parse dd/mm/yyyy; `01/01/1000` and unparseable → NULL.
- Trim whitespace; normalize the 2022 Q4 malformed link (`Nao_Previdenciariozip.zip`) and `http://http://` typos in the URL builder.
- Parquet: snappy, hive-partitioned `ano=/trimestre=/…`; explicit `pa.Schema`; BQ `partition_by ano` INT64 range 2020–2035.

## Execution phases
**A. Prove one quarter (2026 Q2, all 3 tables)** — architecture sheets → clean → upload dev → dbt run+test → validate. **Checkpoint with user.**
**B. Full backfill** — stream all 26 quarters (SIDA quarter-by-quarter), upload dev, dbt.
**C. Metadata** — discover/create `br_mf` org; register dev (status under_review) → **verification checkpoint** → prod metadata under_review → **PR**.
**D. Recurring pipeline** — quarterly Prefect flow under `pipelines/datasets/br_mf_divida_ativa/` (poll new `YYYY_trimestre_QQ`, append), DRY with cleaning transform; dev-pool test run.
**Post-merge** — table-approve materialises prod → verify prod tables/rows → publish (status published) → arm schedule.

## Phase A results (2026 Q2, dev) — findings
- Row counts (dev staging == local parquet): fgts 541,698 · previdenciario 3,606,529 · nao_previdenciario 45,553,971.
- dbt run: 3/3 materialized in `basedosdados-dev.br_mf_divida_ativa` (SIDA 45.6M, 9.2 GiB).
- **Bugs found & fixed during Phase A:**
  1. Index-union bug in the chunked transform (injected `ano`/`trimestre` misaligned with offset chunk index → phantom all-NULL rows). Fixed with `reset_index`.
  2. `sigla_uf` source drift (confirmed across all 26 quarters × 3 tables): `UF_UNIDADE_RESPONSAVEL` (2020 Q1–2022 Q3) → `UF_DEVEDOR` (2022 Q4+). Added `COLUMN_ALIASES` so both map to `sigla_uf`; the earlier values are the responsible-unit UF (documented). Only these two variants exist — no third.
  3. Invalid UFs (e.g. `Si`, tied to "SEM INFORMACAO") nulled so the UF directory FK stays clean.
  4. Shared macro `get_where_subquery` quoted `ano` for `__most_recent_year__` (`ano = '2026'`) → `INT64 = STRING` error. Fixed to unquoted, matching the `_en`/`_year_month` branches (no existing model used `__most_recent_year__`, so zero blast radius).
- Uniqueness: the initial 6-col key `[ano, trimestre, numero_inscricao, cpf_cnpj, tipo_devedor, receita_principal]` had 0 dups on fgts/prev but **10 dups on SIDA** (masked-CPF collisions between distinct corresponsáveis). **Superseded** by the resolved 7-col key below (adds `nome_devedor`), which the dbt tests use.

## Phase B notes (backfill)
- Full source header scan (26 quarters × 3 tables): exactly two variants each (UF column rename at 2022 Q3→Q4); both handled. FGTS 2020 Q1 exists on the server (not linked on the page).
- 2020–2022 Q3 `sigla_uf` is ~35–40% null by design: the column is the responsible-unit UF, and the per-quarter `*_NA_*` split carries `UF_UNIDADE_RESPONSAVEL = "NA"` (nulled). Non-null still ≫ 5% floor.
- **Two Phase-B bugs fixed:** (a) the `sigla_uf` observation contained an unquoted comma → CSV misparse shifted `original_name` → all quarters failed (now quoted). (b) `clean_quarter_zip` left a corrupt 1689-byte `part_000.parquet` on failure, which `--skip-existing` then treated as done → made writes **atomic** (temp dir → publish on success, remove on failure). `--skip-existing` now reliable.

## Phase B results — full backfill complete (26 quarters × 3 tables, ~30 GB parquet)
- **nao_previdenciario (SIDA): 685,239,967 rows**
- **previdenciario: 131,607,772 rows**
- **fgts: 11,543,880 rows**
- **Total: 828,391,619 rows.** All 26 quarters (2020 Q1–2026 Q2) present per table (FGTS 2020 Q1 exists on the server despite not being linked on the page).
- Parallelized across 3 workers (round-robin quarters, `--skip-existing`, atomic writes, download retries) after laptop-sleep interruptions; one transient FGTS 2026 Q1 failure was re-run successfully.

## Dev BigQuery query-quota blocker (decision: skip dev dbt)
- `basedosdados-dev` has an admin-set **`QueryUsagePerDay`** quota that got exhausted (Phase A dbt run/tests + SIDA dup-check scans). `dbt run`/`test` are query jobs → blocked; materializing 685M SIDA rows is itself a ~tens–130 GB single query.
- **User decision: skip the dev dbt run/test.** Upload data to dev `_staging` (GCS — no query quota), verify via the staging external tables + local parquet counts (authoritative, built from the same files), register Phase C metadata via the backend API (no BQ queries — set coverage datetime ranges manually), and defer full materialization to the **prod table-approve on PR merge**.
- Consequence: no materialized `basedosdados-dev.br_mf_divida_ativa.*` this session; cloud-table metadata points at the tables the merge will create.

## Backend facts for Phase C (verified)
- **Backend env = `staging`** (dev backend returns 503; staging is up). Data was uploaded to GCP `basedosdados-dev`; cloud tables point there.
- **Org `mf` already exists**: id `0f5a3a39-33ff-49e7-9f56-b9332a7722c0` (Ministério da Fazenda) — no creation needed.
- `measurement_unit = real` for `valor_consolidado` is unverified (MCP `lookup_id`/`discover_ids` hit a casing bug on `measurement_unit_category`; direct GraphQL endpoint unknown). Verify at `bulk_upsert_columns` time; if rejected, the error lists valid slugs — fix `columns_json`.
- Observation levels: geographic (`sigla_uf`) + temporal (`ano`/`trimestre`); the debtor/inscrição grain may need a custom entity — decide at Phase C.
- Uniqueness key resolved: `[ano, trimestre, numero_inscricao, cpf_cnpj, nome_devedor, tipo_devedor, receita_principal]` (strict, 0 dups) — masked-CPF collisions required adding `nome_devedor`.
