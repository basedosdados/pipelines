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

## Execution (2026-07-31, in progress)

Persistent work dir (survives reboot; `/tmp` does not):
`WORK=/Users/rdahis/Downloads/dados_TSE_rebuild`. Run everything from
`code/python` with `TSE_WORK=$WORK` (also sets `TSE_DATA_DIR=$WORK` via
`gate_b_rebuild`). Pre-build gate re-confirmed: tier 3 = 361 OK / 51 WARN /
**0 FAIL**.

Stage scripts (all resumable, sequential — never run two builds at once, 16 GB):

1. **Non-giant phase-1** — `uv run python gate_b_rebuild.py`
   → `WORK/output_python/<table>_<ano>.parquet`. Skips a table-year whose
   parquet exists. Skips the two giant families (`resultados_secao`,
   `perfil_secao`). Compares each to the March refs; only documented DIFFs
   expected (bens BR-file inclusion 2006–2022; partidos_2020 coligação fill).
2. **Giants (stream)** — `uv run python wo05_stream_giants.py [--family F]`
   → `WORK/output_python/_stream/<table>/ano=/sigla_uf=/data.parquet`.
   Extract→stream→evict per `(ano,uf)`; `.done/<family>_<ano>` markers.
   Families: `resultados_secao` (1994–2024), `perfil_secao` (2008–2024).
3. **Phase-3 + aggregation** — `uv run python build.py normalize` then
   `uv run python build.py aggregate` → final Hive-partitioned CSVs under
   `WORK/output_python/<table>/`. `_partition_secao_table` reads the `_stream`
   intermediates automatically.
4. **Dev upload** — `uv run --with basedosdados python wo05_upload_dev.py`
   → `basedosdados-dev.br_tse_eleicoes_staging.<table>` (19 tables; markers in
   `WORK/upload_done/`). Uses the repo `.venv` bd 2.0.3 create() pattern.
5. **dbt** — `uv run dbt run --select br_tse_eleicoes` + `uv run dbt test ...`.
6. **Dev probes** — `diagnostics/prod_validation.sql` Q1–Q18, retargeted from
   `basedosdados.` → `basedosdados-dev.`, billed to `basedosdados-dev`.

The 19 pipeline-produced tables (upload/probe scope): candidatos, partidos,
vagas, bens_candidato, receitas_candidato, despesas_candidato,
perfil_eleitorado_{municipio_zona,secao,local_votacao},
detalhes_votacao_{municipio_zona,secao,municipio},
resultados_candidato_{municipio_zona,secao,municipio}, resultados_partido_
{municipio_zona,secao,municipio}, resultados_candidato. The 3 dbt models the
pipeline does not build (`receitas_comite`, `receitas_orgao_partidario`,
`dicionario`) are out of scope and keep their existing dev staging.

### Disk incident + giants-as-parquet fix (2026-08-01)

The first phase-3 run exhausted disk: the three seção **giants** as all-string
CSV are ~35× their parquet size (`resultados_candidato_secao` hit **50 GB** of
CSV alone before the kill, vs 1.4 GB parquet). The full CSV set does not fit on
this host. Fix: `normalization_partition._partition_secao_table` now writes the
giants as compact Hive **parquet** (`_save_secao_partition_parquet`), not CSV.
The giants do not feed aggregation, and the dbt models `safe_cast` every column,
so parquet-typed staging is equivalent to the all-string CSV convention.
`wo05_upload_dev.py` uploads the three giants with `source_format="parquet"`
(set `GIANT_PARQUET`); every other table stays CSV. Re-runs are disk-guarded
(kill the build if free < 6 GB). All non-giant CSVs are modest (rcmz 4.5 GB is
the largest; phase-1 parquets total 3.6 GB).

### dbt result (2026-08-01): 17/19 PASS; despesas+receitas schema gap

dbt env: dedicated venv `WORK/dbtenv` with dbt-bigquery==1.5.9 (matches the
project pin; the repo `.venv` pair 1.8.8/1.9.2 is mismatched). Run:
`dbt run --select br_tse_eleicoes --exclude receitas_comite
receitas_orgao_partidario dicionario prod_validation --profiles-dir . --target
dev` with `BD_SERVICE_ACCOUNT_DEV=~/.basedosdados/credentials/staging.json`.
Published to `basedosdados-dev.br_tse_eleicoes.<table>` (generate_schema_name =
custom schema alone). **PASS=17** (all corrupted-cell tables + all 3 parquet
giants), **ERROR=2**.

The 2 failures are `despesas_candidato` and `receitas_candidato` — a
**pre-existing refactor↔model schema mismatch**, not a corrupted cell and not
caused by this rebuild (matches the March refactor output). The campaign_finance
Python builder emits **non-uniform per-year columns** (despesas 2002=17, 2014=24,
2018+=38) and never emits `especie_recurso`/`fonte_recurso`, whereas PROD's
`despesas_candidato` has a **uniform 45-col** schema (all present) and the dbt
model reads that superset. Fixing it means making `build_despesas`/`build_receitas`
emit the uniform superset schema (older years padded), matching prod's 45/55
cols — a distinct campaign_finance task, outside the 62-FAIL / 11-corrupted-cell
scope. Verification of the corrupted cells proceeds on the 17 passing tables
(all probes except Q8-despesas).

**Status:** stages 1–4 DONE + dbt 17/19. Running Q1–Q18 dev probes on the 17
tables. Then checkpoint + PAUSE for approval; despesas/receitas flagged as a
separate decision.

### Post-checkpoint fixes (user chose "fix the new findings first")

- **FIX-A** (`sub/results_mun_zone.py`): 1994 rcmz `votos` now reads
  `QT_VOTOS_NOMINAIS_VALIDOS` (raw 1994 `QT_VOTOS_NOMINAIS` is 0 for all rows;
  real counts are in `_VALIDOS`). Verified dev: rcmz/rc_municipio/rc 1994 votos
  now real (rc_municipio max 3.0M, rc max 8.68M; was 100% zero).
- **FIX-B**: no code — candidatos 1996 demographics are `#NE`/`-1` at source
  (honest null), detalhes 1996 is source-sparse (matches prod). Documented.
- **FIX-C** (`normalization_partition._partition_finance`): reindex despesas/
  receitas to the uniform model schema (43/53 cols) so ragged per-year files
  become one staging schema; `especie_recurso`/`fonte_recurso` absent from raw
  despesas → always empty (matches prod). dbt despesas (32.4M) + receitas
  (15.3M) now PASS → **dbt 19/19**.
- **FIX-E** (`normalization_partition._partition_results_mun_zone`): rcmz/rpmz
  raw column ORDER differs across years (`id_municipio` pos 5 vs last); bd's
  position-based CSV staging then mis-read `votos` as `resultado`
  ('suplente'/'eleito') and safe_cast nulled it — silently zeroing **2018 &
  2022 votos**. Fixed by reindexing every year to `_RCMZ_ORDER`/`_RPMZ_ORDER`
  before save. Verified dev: rcmz/rpmz votos **0 nulls all years** (2018 8.67M,
  2022 9.38M rows). This was the highest-impact defect the checkpoint caught.

Dev now: 19/19 dbt PASS; all corrupted cells repaired; the finance schema and
the mun-zona column-order defects (which would have corrupted prod on
promotion) fixed. Code fixes UNCOMMITTED (await user). Then prod needs approval.

## Acceptance

- [ ] Dev probes Q1–Q18 all clean
- [ ] User approved prod promotion
- [ ] Prod probes clean; row counts verified
- [ ] Issues answered/closed; DIAGNOSIS.md updated with repair record
