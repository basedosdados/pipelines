# 03 — Side issues (B3–B6)

Four open GitHub issues not (fully) covered by the schema harness. Each is an
investigation first; fix only after the cause is pinned. File findings as a
short section appended to this file (keep the issue trail here, link the
issue).

## FINDINGS (2026-07-30) — all four causes pinned

| Issue | Cause | Fix |
|---|---|---|
| B3 #1568 | Stale prod build vintage, not a live code bug | full rebuild (05) |
| B4 #1046 | Source ships the field 100% `#NULO#` | document (or drop column) |
| B5 #1463 | dbt model missed the column | **fixed** — added to model + schema.yml |
| B6 #1155 | Source has no vice votes (titular-only ballots) | document |

Details:

- **B3 (`resultados_partido_secao` misaligned 1994–2006).** The current code
  parses the current votacao_secao files correctly: byte-parity vs the
  validated outputs for 1996 (1.0M rows) and 1998 (11.4M rows, incl. the
  legacy BR variant), and the harness finds no mismatch. The prod anomaly
  (cargo values inside `zona`, etc.) is the signature of an *older*
  raw-file generation parsed with mappings for a newer one — a stale build,
  same class as the DIAGNOSIS.md prod findings. The full rebuild (05)
  repairs it; no code change needed.
- **B4 (`esfera_partidaria_fornecedor` empty).** `DS_ESFERA_PART_FORNECEDOR`
  is `#NULO#` in every row of the TSE candidate-expense files (checked
  632,668 rows SP 2020 and 309,654 rows SP 2022; field only exists 2018+).
  Esfera partidária describes party-organ suppliers, which do not occur in
  candidate expense records. Pipeline maps it correctly; the source is
  empty. Options: keep + document in column `observations`, or drop the
  column from architecture/dbt/metadata — user decision.
- **B5 (`nome_candidato` in API but not in BQ).** `aggregation.py` produces
  the column and the metadata registers it, but
  `br_tse_eleicoes__resultados_candidato.sql` did not select it. Fixed:
  added `safe_cast(nome_candidato as string)` after `numero_candidato`
  (PLAN.md schema order) + schema.yml entry. Lands on the next dbt run.
- **B6 (missing vice-prefeito in `resultados_candidato_municipio`).**
  The TSE vote files (`votacao_candidato_munzona`) contain only
  Prefeito/Vereador rows (checked 2000 and 2020); vices are not voted
  separately, so no vote results exist for them. Vice-prefeito candidates
  do appear in `candidatos` (97 in AC 2020). Works as the source works;
  answer the issue and note it in the table description.

Draft issue replies (PT) for user review before posting: see
[issue_replies.md](issue_replies.md).

## B3 — issue #1568: `resultados_partido_secao` misaligned 1994–2006

Symptom (prod): `cargo` values inside `zona`, `sigla_partido` inside `cargo`,
years < 2015. The harness marks seção results **clean statically** — so the
corruption is a build-vintage artifact in prod, not (necessarily) a live code
bug.

- [ ] Reproduce with the issue's SQL against prod; record affected years/rows.
- [ ] Check `sub/results_section.py` mapping for 1994–2006 against the
      official layouts (`diagnostics/artifacts/layouts/votacao_secao_*.json`).
- [ ] Determine the vintage prod was built from (column-shift pattern
      identifies the generation).
- [ ] Outcome: if code is clean → add the affected cells to work order 05's
      rebuild list; if a mapping bug exists → fix as a 02-style item.

## B4 — issue #1046: `esfera_partidaria_fornecedor` 100% empty

`sub/campaign_finance.py:1881` maps `v35 → esfera_partidaria_fornecedor` in
one year-block only; prod shows the column entirely empty.

- [ ] Check which years the raw source actually carries the field
      (`ST_ESFERA_PARTIDARIA_FORNECEDOR` or similar) — layouts JSONs +
      one real file.
- [ ] Check whether the Stata code ever populated it (old
      `sdk/bases/br_tse_eleicoes/code`), i.e. was it always empty by design?
- [ ] Outcome: populate for the years the source has it, or drop the column
      from architecture + dbt + metadata (user decision), and answer the
      issue either way.

## B5 — issue #1463: `nome_candidato` in metadata but not in the BQ table

`aggregation.py` produces `nome_candidato` in `resultados_candidato` (also in
PLAN.md's schema), but the dbt model
`br_tse_eleicoes__resultados_candidato.sql` does not select it, so the BQ
table lacks a column the metadata/API advertises.

- [ ] Confirm the parquet output carries `nome_candidato`.
- [ ] Add the column to the dbt model + `schema.yml` (position per
      architecture), or remove it from backend metadata — the Stata-era
      table defines which is correct.
- [ ] Re-run dbt for the model; verify the API/table agree; answer the issue.

## B6 — issue #1155: missing `cargo = vice-prefeito` rows

`resultados_candidato_municipio` lacks vice-prefeito. `sub/candidates.py`
normalizes the label, but results tables may never receive those rows: TSE
vote files attribute votes to the titular, not the vice.

- [ ] Check raw `votacao_candidato_munzona` for any `vice-prefeito` rows
      across a sample of years.
- [ ] Check what Stata-era outputs contained (reference .dta) — was the data
      ever there?
- [ ] Outcome: if the source simply has no vice votes (likely — vices are not
      voted separately), document in table/column metadata + answer the
      issue (possibly "works as intended, docs clarified"); if rows are being
      dropped in `aggregation.py` or the candidato merge, fix there.

## Acceptance

- [ ] Each of B3–B6 has: cause identified, fix landed or explicit
      user-approved wontfix, and a drafted reply on the GitHub issue
      (user reviews before posting).
