# br_tse_eleicoes — Bug-Fix Master Plan

Fix every bug found in the Stata → Python refactor (PR #1476), validate the
Python pipeline cell-by-cell against the Stata reference, and repair the
corrupted production cells. This plan is the execution contract for new
sessions and subagents: read this file first, then execute the numbered work
orders in sequence.

## Context (read once)

- Dataset: `br_tse_eleicoes` — one of Data Basis's most important datasets.
- The Python refactor (`code/python/`) must reproduce the Stata outputs
  (`~/Downloads/dados_TSE/output/*.dta`) exactly, cell by cell, when run on
  the same raw inputs.
- Pisa's schema-validation harness (`code/python/diagnostics/`) found
  **62 header-confirmed FAIL cells** across 10 builders. Root cause (single):
  TSE silently re-republishes historical files in its current-generation
  layout; the positional column mappings transcribed from Stata index the
  *previous* republication. Full narrative: [`../DIAGNOSIS.md`](../DIAGNOSIS.md);
  per-table breakdown: [`../diagnosis/`](../diagnosis/).
- Prod (`basedosdados.br_tse_eleicoes`) is already corrupted in **11
  table × year cells** (6 at source + 5 by propagation through merges and
  aggregations). All other FAIL cells are latent: they corrupt on the next
  rebuild from fresh downloads unless the builders are fixed first.

## Bug inventory

| # | Bug | Source | Work order |
|---|---|---|---|
| B1 | 62 positional-mapping FAIL cells (10 builders) | DIAGNOSIS.md, issue #1563 | 02 |
| B2 | 28 WARN cells (low-confidence layouts, unverified variants) | DIAGNOSIS.md | 02 |
| B3 | `resultados_partido_secao` misaligned columns in prod, 1994–2006 | issue #1568 | 03 |
| B4 | `esfera_partidaria_fornecedor` 100% empty in `despesas_candidato` | issue #1046 | 03 |
| B5 | `nome_candidato` in metadata/API but absent from the BQ table `resultados_candidato` | issue #1463 | 03 |
| B6 | `cargo = vice-prefeito` rows missing in `resultados_candidato_municipio` | issue #1155 | 03 |
| B7 | 11 corrupted prod cells (see DIAGNOSIS.md "Prod-data validation") | DIAGNOSIS.md | 05 |

Issue #1563 is a subset of B1 (candidatos 2018/2020/2022) — close it when B1
lands.

## Work orders (execute in this order)

| File | What | Depends on |
|---|---|---|
| [01_setup_and_data.md](01_setup_and_data.md) | Branch, environment, hydrate Dropbox reference data | — |
| [02_header_parsing_refactor.md](02_header_parsing_refactor.md) | Fix B1/B2: header-aware parsing in all builders | 01 |
| [03_side_issues.md](03_side_issues.md) | Investigate + fix B3–B6 | 01 (02 for fixes touching parsers) |
| [04_validation_protocol.md](04_validation_protocol.md) | Gate A (Stata parity) + Gate B (fresh downloads) | 02, 03 |
| [05_rebuild_and_prod_repair.md](05_rebuild_and_prod_repair.md) | Rebuild in topological order, dev upload, prod repair | 04 |

## Hard rules

1. **Never run validations or table builds in parallel** — RAM explosion.
   One table × year at a time.
2. **Atomic vintages**: never mix raw-download generations across a build
   batch. Mixed `candidatos` vintages produced the `bens_candidato` 2014
   smoking gun.
3. **Topological repair order**: upstream before downstream —
   `candidatos` → `norm_candidatos` → phase-2 merges → phase-3 aggregations.
4. **Gate rebuilds**: `python -m diagnostics run --tier 3` must exit 0 before
   any build against freshly downloaded data.
5. **Commit discipline**: atomic conventional commits
   (`fix(br_tse_eleicoes): ...`), no Co-Authored-By, never commit data files
   or `.ipynb`. Wait for the user's manual test before committing anything
   they asked to verify.
6. **Do not push or touch prod without explicit user approval.**

## Status board

Update this table as work completes (mark with date + session note).

| Work order | Status | Notes |
|---|---|---|
| 01 setup + data hydration | DONE (2026-07-30) | all input zips hydrated (extracted dirs were evicted by Dropbox — extract from zips into a scratch root, TSE_DATA_DIR points there). prestacao_contas_2006.zip is a **RAR with a .zip extension** (TSE packaging quirk) — extract with `tar -xf` (libarchive), not unzip |
| 02 header-aware parsing | DONE (2026-07-30) | all builders header-aware; tier 3 = 392 OK · 59 advisory WARN · **0 FAIL**. ~60 table-year cells byte-verified vs pre-refactor outputs, incl. every previously corrupted cell. despesas_candidato 2006 byte-identical (1.10M rows; the zip is a mislabeled RAR, see 01). perfil_eleitorado_secao 2024 byte-identical (75.7M rows); 2008 matches on values with the intended post-4481f185 column order (its Mar-21 reference parquet predates that fix) |
| 03 side issues B3–B6 | DONE (2026-07-30) | causes pinned for all four (see 03_side_issues.md): #1463 fixed in dbt model; #1568 stale prod vintage (rebuild repairs); #1046 source-empty field; #1155 no vice votes at source. Draft replies in issue_replies.md await user review |
| 04 Gate A parity vs Stata | DONE (2026-07-30) | 236 pairs full-cell order-independent compare: 158 MATCH, all 76 MISMATCH classified (gate_a_triage.md), zero unexplained python bugs; regenerate stale 2014/2022 secao parquets in the rebuild |
| 04 Gate B fresh downloads | TODO | |
| 05 rebuild + dev upload | TODO | |
| 05 prod repair | TODO | blocked on user approval |

### Session log

- **2026-07-30** — Pending re-checks completed from the hydrated zips
  (extract → build → byte-compare → delete, one family-year at a time):
  rcmz 1994/2010/2018; perfil mun-zona 2008/2016/2020/2022/2024 (2020
  needed the CD_MUN_SIT_BIOMETRIA name variant); receitas+despesas
  2004/2008/2010/2012/2016/2018/2020/2022/2024 (2008/2012 got their named
  dicts from the zip headers; receitas 2018 named path excludes SG_UE to
  keep reference column placement); resultados_*_secao 1996/1998 (incl.
  the legacy BR branch, 24M rows); detalhes_votacao_secao 1998/2010/2024;
  perfil_eleitorado_local_votacao all years combined (5.97M rows). All
  byte-identical. perfil_eleitorado_secao 2008: values identical, column
  order matches the post-4481f185 schema (reference parquet is 3 days
  staler than that fix).

- **2026-07-29** — Work order 02 executed on branch `fix/br_tse_eleicoes_diagnosis`
  (11 commits). Design: `read_raw_csv` detects the header row per file
  (whitelist + fail-loud heuristics, accent/whitespace-normalized names);
  every builder gained a named (`keep_cols`) path with the positional blocks
  as headerless fallback; `bens` recovered the pre-#1564 12-column fallback.
  Harness upgraded: named-site affinity check, positional-fallback skip,
  `**CONST` dict spreads, runtime-identical header normalization. Every
  refactor byte-verified against the pre-refactor outputs on the hydrated
  vintage (~35 table-year cells). Note: Pisa's earlier header attempt
  (0d57249c, reverted in 6a576a32) renamed columns from artifact layouts —
  fragile against vintage mismatch; this design reads each file's own header.

## Decisions (settled by Ricardo, 2026-07-29)

1. **Parity target**: parity except cells where the Stata-era output itself
   is corrupted; every deviation documented in the parity matrix (04).
2. **Repair scope**: full rebuild of all tables from one fresh, uniform
   download vintage.
3. **Where the work lands**: push fixes onto PR #1476's branch
   (`feat/refactor_eleicoes`). Local work branch:
   `fix/br_tse_eleicoes_diagnosis` (tracking it).
