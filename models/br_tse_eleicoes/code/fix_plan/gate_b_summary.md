# Gate B — summary (CLOSED 2026-07-31)

Gate B validates that the fixed Python code parses **today's / the local
uniform vintage** correctly, complementing Gate A (which proved parity vs
the Stata `.dta`). Three legs, all green.

## 1. Freshness vs today's TSE — PASS

- Tier-2 layouts re-fetched from TSE via remote HTTP-Range (headers only,
  no data download). Tier-3 cross-check: **396 OK / 59 advisory WARN /
  0 FAIL** against today's official layouts (`diagnostics run --tier 3`
  exits 0 — the pre-build gate).
- Republication drift (`gate_b_drift.py`, local zip headers vs today's
  official layout): TSE renamed `ANO_ELEICAO`→`AA_ELEICAO` and appended
  columns (julgamento/cassação, raça/identidade-de-gênero/quilombola,
  federation, anulados) across votacao/perfil/finance families since our
  download vintage. Read-by-name parsing absorbs the additions; the ano
  rename needed the `aa_eleicao` dual key, now in every builder site
  (commit 7828c89f). No re-download required.

## 2. Phase-1 rebuild from the local zips — PASS

`gate_b_rebuild.py` rebuilt every phase-1 table×year from the local
input zips (one uniform vintage) and compared each to the March parquet.
Final deduplicated tally (latest result per stem, across three runs):

| Result | Count | Meaning |
|---|---|---|
| MATCH | 155 | byte-identical to March parquet |
| DIFF (expected) | 6 | documented corrections, not regressions |
| NO_MARCH_REF | 8 | `perfil_eleitorado_local_votacao` per-year (March ref is one all-years file; Gate A verified the 5.97M-row combined = byte-identical) |
| SKIP-BIG | 2 families | `resultados_secao`, `perfil_secao` — see below |

The 6 expected DIFFs:

- `bens_candidato` 2006/2010/2014/2018/2022: +176 to +315 rows each, all
  in the blank-`sigla_uf` bucket = the deliberate BR (national/exterior)
  file inclusion that postdates the March parquets. Verified: bens 2006
  delta is exactly 221 rows, all `sigla_uf == ''`.
- `partidos_2020`: same schema + row count, cells differ = the rebuild
  fills the `sequencial_coligacao`/`nome_coligacao`/`composicao_coligacao`
  columns the old positional bug (a Pisa FAIL cell) left empty. This is
  the fix visibly working.

## 3. Giant seção families — validated without in-RAM rebuild

`resultados_candidato_secao`, `resultados_partido_secao`, and
`perfil_eleitorado_secao` reach 60–75M rows and cannot be built in the
host's 16 GB RAM (the first pass OOM-crashed the machine). They are
validated by proof instead of rebuild:

- Their local files carry `ANO_ELEICAO`, so the added `aa_eleicao` key is
  **inert** — `select_named` matches `ano_eleicao` and ignores the absent
  alternative → output byte-identical to the March parquets.
- Gate A already cell-verified those March parquets against Stata.

**Production build of these three families (work order 05) needs a
streaming/partitioned path (write per-UF without concatenating all UFs in
RAM) or a bigger host. Do not build 60M-row tables in-RAM on 16 GB.**

## Verdict

Gate B PASS. The fixed code parses the current TSE vintage with zero
FAILs, reproduces every validatable table from the local zips, and every
non-MATCH is a documented, intended correction. Combined with Gate A
(parity vs Stata, all 76 mismatches classified), the Python refactor is
validated end-to-end. Remaining work is work order 05: the uniform-vintage
production rebuild (with the streaming path for the giants) → dev upload →
user approval → prod.
