# 05 — Rebuild and prod repair (B7)

Only after 02–04 are green. Nothing here touches prod without explicit user
approval.

## Corrupted prod cells (from DIAGNOSIS.md, confirmed by queries Q1–Q18)

Source cells:

| Cell | Symptom |
|---|---|
| `bens_candidato` 2014 | education codes in `titulo_eleitoral_candidato` (all 83,053 rows) |
| `resultados_candidato_municipio_zona` 1994 | `votos = 0` in all 842,223 rows |
| `candidatos` 1996 | demographic block 100% NULL |
| `detalhes_votacao_municipio_zona` 1996 | 548 rows only + `-3` sentinels |
| `perfil_eleitorado_municipio_zona` 2008, 2016 | two build generations mixed in one partition |

Propagated cells: `resultados_candidato_municipio` 1994;
`resultados_candidato` 1994, 1996; `resultados_candidato_municipio_zona` 1996
(titulo col); `resultados_candidato_secao` 1996 (titulo col);
`detalhes_votacao_municipio` 1996. Plus whatever 03/B3 adds
(`resultados_partido_secao` 1994–2006).

## Strategy (pending user decision in README)

Preferred: **full rebuild of all tables from one fresh, uniform download
vintage** — eliminates mixed-vintage corruption permanently. Fallback:
targeted rebuild of the corrupted cells only, strictly upstream-first:

1. `candidatos` 1996 → `norm_candidatos` → `resultados_candidato_secao` /
   `resultados_candidato_municipio_zona` 1996 → `resultados_candidato` 1996
2. `resultados_candidato_municipio_zona` 1994 →
   `resultados_candidato_municipio` / `resultados_candidato` 1994
3. `detalhes_votacao_municipio_zona` 1996 → `detalhes_votacao_municipio` 1996
4. Leaves: `bens_candidato` 2014, `perfil_eleitorado_municipio_zona`
   2008/2016 (single clean `candidatos` vintage for the bens merge)

Either way: one `candidatos` vintage per batch, `--tier 3` gate before every
build, sequential (no parallel builds).

## Steps

1. Pre-build gate: `python -m diagnostics run --tier 3` exits 0.
2. Build (per strategy above); Gate B invariants on every rebuilt cell.
3. Upload to `basedosdados-dev.br_tse_eleicoes` staging (all-string staging
   convention; delete stale GCS prefix first — see bigquery-conventions).
4. `uv run dbt run --select br_tse_eleicoes` + `uv run dbt test` on dev.
5. Re-run the prod-anomaly probes (`diagnostics/prod_validation.sql`
   Q1–Q18) **against dev** — every previously-failing probe must now pass.
6. **PAUSE — present dev results to user; wait for explicit approval.**
7. Prod: upload + materialize the affected tables; re-run probes against
   prod; verify row counts.
8. Update PR #1476 (or stacked PR): summary of fixes, parity matrix,
   freshness report; draft closing comments for issues #1563, #1568, #1046,
   #1463, #1155 (user reviews before posting).

## Acceptance

- [ ] Dev probes Q1–Q18 all clean
- [ ] User approved prod promotion
- [ ] Prod probes clean; row counts verified
- [ ] Issues answered/closed; DIAGNOSIS.md updated with repair record
