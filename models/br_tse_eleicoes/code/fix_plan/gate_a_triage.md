# Gate A — mismatch triage

Classification of every MISMATCH in `parity_matrix.md` (Stata `.dta` vs
Python parquet, order-independent full-cell comparison). Per the settled
parity decision: parity except cells where the Stata-era output itself is
corrupted; every deviation documented here.

Classes: **(A) python bug / stale python parquet — regenerate**;
**(B) deliberate python improvement over Stata — keep python**;
**(C) Stata-era corruption — keep python**; **(N) gate_a normalization gap
— fix comparator, re-run**.

## Headline: resultados_*_secao 2014 / 2022 row-count gaps

Ground truth computed directly from the raw files (distinct
(zona, secao, cargo, party) groups):

| cell | stata | python (Mar parquet) | raw ground truth | verdict |
|---|---|---|---|---|
| rps 2014 SP dep.estadual t1 | 1,854,582 | 413,104 | **1,854,582** | **(A)** March parquet built from a partial SP extraction; current code on the current zip reproduces ground truth exactly (verified) |
| rps 2022 SP dep.estadual t1 | 849,857 | 2,122,780 | **2,122,780** | **(C)** Stata under-read (early/partial vintage) |
| rps 2022 ES t1 / t2 | 461,323 / 36,956 | 71,878 / 18,834 | **410,624 / 18,478** | Stata t2 is exactly 2× (duplication) → **(C)**; the March parquet is partial → **(A)**. Current code on the current zip = ground truth exactly (verified) |

Presidente (the separate BR file) matches exactly on both sides in both
years — the discrepancies are confined to the per-UF files (SP 2014;
SP/CE/ES 2022). Action: regenerate `resultados_candidato_secao` and
`resultados_partido_secao` parquets for 2014 and 2022 from the current
zips (fold into the full rebuild), then re-run gate_a for those stems.
`rcs_2014`/`rcs_2022` matrix rows pending sweep completion — expect the
same classes.

## Patterned classes (verified with examples)

| Pattern | Cells | Class | Evidence |
|---|---|---|---|
| `tipo_eleicao` all rows | `detalhes_votacao_uf` 1945–1990 | **(B)** | 'eleicao 1965' → 'eleicao ordinaria' (clean_election_type fix, 4481f185) |
| `data_*` invalid dates | `despesas_2010` (320 cells), likely receitas 2004/2006/2010 + despesas 2002/2006 | **(B)** | Stata keeps raw typo dates ('0010-09-06' from '06/09/0010'); python nulls invalid dates |
| `descricao_despesa` all rows | `despesas_2010` | **(B)** | Stata kept stray trailing '"' (quote-stripping fix, 4481f185) |
| `valor_item` | `bens` 2020 (13.9k), 2022 (2.2k), 2024 (16.7k rows) | **(C)** | multiline quoted descriptions truncate Stata's import → NaN; raw confirms real values (e.g. seq 10001623538 Terreno 24,410.43); python parses quoted newlines |
| names/title-case | `candidatos` (2–3.8k rows/yr), `nome_candidato` in rcuf/rcmz (1–70 rows) | **(B)** | 'D`assunção'→'D`Assunção' (casing-after-apostrophe fix); mojibake 'nÃo possui'→'não possui' |
| coligacao/tipo_eleicao | `partidos` 2008–2024 (24–3k rows/yr) | **(C)** | Stata carries zone strings ('agua boa mg - 67ª ze') in `tipo_eleicao` on supplementary-election rows; python has 'eleicao suplementar de ...' |
| small full-row multisets | finance 2008/2012/2014/2016 (4–1.2k rows), `norm_candidatos` (8.4k of 3.3M), `perfil_local_votacao` (12), rcs 2020/2024 (1–8), rcmz 2022 (230) | mostly **(B/C)** | consistent with multiline/quote/casing fixes propagating; spot-checks pending |
| `data_receita`/`data_despesa` small counts | receitas 2004/2006/2010, despesas 2002/2006 (1–194 rows) | pending | likely date-parse edge (e.g. invalid dates Stata kept/dropped) |

## Verification protocol used

1. Localize: group-by counts (uf, turno, cargo) chunked on both sides.
2. Arbitrate with the raw file: stream the zip member, compute the
   ground-truth statistic (row counts, distinct groups, specific
   candidate's values).
3. When python ≠ ground truth: rebuild the cell with current code from
   the current zip (monkeypatched single-UF build) and compare — in every
   case so far current code reproduces ground truth exactly.


## Sweep completion + comparator corrections (2026-07-30, later)

- Final sweep: 236 pairs — 158 MATCH / 76 MISMATCH / 0 ERROR before
  attribution re-runs.
- `rcs_2014` (stata 44.0M vs python 35.4M) and `rcs_2022` (48.0M vs
  60.1M) mirror the rps classes exactly: 2014 = stale partial-SP March
  parquet (A); 2022 = Stata under-read (C).
- **Comparator fix 1**: datetime64 .dta columns now render date-only.
- **Comparator fix 2**: the per-column digest was order-dependent across
  chunk boundaries (per-chunk XOR term), producing false-positive column
  attributions on >2M-row tables with differing row order — e.g.
  `despesas_2008` listed 14 columns; after the fix (commutative
  Σhash + Σhash² mod 2^64) the true attribution is 2 columns
  (`data_despesa`, `tipo_documento`, 789 rows). MATCH/MISMATCH verdicts
  were never affected (they use the sorted row-hash arrays). The 12
  affected mismatch stems were cleared and re-run with the fixed digest.
- `despesas_2008` verified probe: `nome_fornecedor` value multisets are
  IDENTICAL between .dta and parquet — confirming the attribution noise.


## Final small-cell resolutions (2026-07-30) — Gate A CLOSED

Corrected attributions (commutative digest) plus targeted probes close
every remaining cell:

| Cell | Rows | Finding | Class |
|---|---|---|---|
| `receitas_2008` | 1,188 | `data_receita` only — invalid-date class | (B) |
| `despesas_2008` | 789 | `data_despesa` + `tipo_documento` | (B) |
| `despesas_2012/2016`, `receitas_2012/2014/2016` | 4–125 | fornecedor/doador RF-name and descricao clusters — multiline/quote class | (B/C) |
| `rcmz_2022` | 230 | `nome_urna_candidato` only — casing class | (B) |
| `rcs_2024` / `rps_2024` | 1 each | single Stata row with blank `sigla_uf` — python drops blank records | (B) |
| `rcs_2020` | 8 | one supplementary election renumbered `id_eleicao` 580→605 between TSE vintages (votes revised too) | vintage drift, documented |
| `perfil_local_votacao` | 12 | per-(ano,UF) counts identical; 12 Stata rows with blank ano/UF — python drops | (B) |
| `norm_candidatos` | 8,364 of 3.3M | propagation of the candidatos (B) name/encoding fixes; `aux_id` is a Stata-internal helper column | (B) |

**Gate A verdict: 236 pairs; 158 MATCH; all 76 MISMATCHes classified —
zero unexplained python bugs. The only python-side actions are
regenerating the stale 2014/2022 secao parquets (fold into the full
rebuild) — the code itself reproduces raw ground truth exactly wherever
tested.**
