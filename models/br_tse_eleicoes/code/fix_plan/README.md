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
| 01 setup + data hydration | TODO | needs user action for Dropbox hydration |
| 02 header-aware parsing | TODO | |
| 03 side issues B3–B6 | TODO | |
| 04 Gate A parity vs Stata | TODO | |
| 04 Gate B fresh downloads | TODO | |
| 05 rebuild + dev upload | TODO | |
| 05 prod repair | TODO | blocked on user approval |

## Open decisions (user)

1. **Parity target**: strict cell-by-cell parity with Stata everywhere, or
   parity except the cells where the Stata-era output itself is corrupted
   (e.g. `bens_candidato` 2014)? Recommended: parity-except-known-corruption,
   with every deviation documented in the parity matrix (04).
2. **Repair scope**: targeted repair of the 11 corrupted prod cells vs full
   rebuild of all tables from one fresh, uniform download vintage.
   Recommended: full rebuild — uniform vintage eliminates the mixed-vintage
   failure class permanently.
3. **Where the work lands**: push fixes to PR #1476's branch
   (`feat/refactor_eleicoes`) vs a stacked PR. Current work branch:
   `fix/br_tse_eleicoes_diagnosis` (off `feat/refactor_eleicoes`).
