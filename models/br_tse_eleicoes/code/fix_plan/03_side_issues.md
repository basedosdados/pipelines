# 03 — Side issues (B3–B6)

Four open GitHub issues not (fully) covered by the schema harness. Each is an
investigation first; fix only after the cause is pinned. File findings as a
short section appended to this file (keep the issue trail here, link the
issue).

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
