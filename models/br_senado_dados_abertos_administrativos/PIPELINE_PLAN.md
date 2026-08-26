# Recurring pipeline — br_senado_dados_abertos_administrativos

Prefect 3 pipeline that refreshes the dataset on a schedule, reusing the
onboarding cleaning transform verbatim (`pipelines/datasets/.../utils.py::clean_all`).
This is step 12; the static onboarding (steps 1–11) is merged in PR #1908.

## Two table shapes, one mechanism

The dataset has two partition schemes (see [CLAUDE.md](CLAUDE.md)):

| Shape | Tables | Partition | Refresh semantics |
|---|---|---|---|
| Snapshot | 30 tables (staff, contracting, colaboradores, gestão, senador dim) | `data_extracao` (DATE) | Each run stacks a **new dated snapshot** — the source exposes only current state, so the time dimension is ours (CNPJ model, as `br_cgu_sancoes` / `au_ato_abr`) |
| Time series | `despesa_ceaps`, `servidor_remuneracao`, `servidor_hora_extra(_dia)`, six `suprido_*` (10 tables) | `ano` (INT64) | Each run **refreshes the last two years** (current + prior, to catch late-arriving data) in place; older years are stable |

Both are served by the **same** mechanism, from `au_ato_abr`:

1. The extract writes only the current window — for snapshots that is today's
   snapshot; for the series it is the last two years
   (`clean_all(years=[current-1, current])`). A table with no rows for the window
   writes no parquet and is skipped (with `insert_overwrite`, skipping simply
   leaves its existing partitions in prod). `dicionario` is likewise skipped per
   run — it is rebuilt from the run's supridos, so a windowed extract would
   shrink it; it is static and fully populated at onboarding.
2. `upload_to_gcs(dump_mode="overwrite")` replaces the **staging** external table
   with that window.
3. The dbt model is **incremental with `incremental_strategy="insert_overwrite"`**
   on its partition column, so it replaces exactly the window's partition in
   **prod** and leaves every other partition untouched. History therefore
   accumulates in prod, not in staging.

`insert_overwrite` makes re-runs idempotent: re-running the same day replaces the
same `data_extracao`/`ano` partition rather than duplicating it. `dicionario`
(no partition) stays `materialized="table"` — a full refresh each run.

This converts the 39 partitioned onboarding models from `table` to `incremental`.
Safe post-onboarding: the first incremental run replaces only the current
partition and keeps the full history the onboarding already materialized.

## Cadence — parents daily, contratação sub-resources weekly

The contratação children require a ~27k-request status fan-out; the parents and
everything else are cheap. `clean_all(sub_resources=...)` already gates the
fan-out, so one flow with two schedules covers both:

| Cron (America/Sao_Paulo) | `sub_resources` | Builds |
|---|---|---|
| daily, `17 6 * * 0,2-6` (minute chosen off the shared crontab) | `False` | senador dim, all snapshots, contratação **parents**, last-two-years series |
| weekly, `23 6 * * 1` | `True` | the above **plus** the contratação children (item, garantia, pagamento(+documento_fiscal,+empenho), aditivo, ata_acionamento) |

The weekly run is a superset; on Mondays it is the only run needed but running
both is harmless (`insert_overwrite` is idempotent per partition). Time-series
tables are re-extracted for the last two years on every run — cheap for CEAPS and
supridos, ~13 monthly requests for payroll.

## BD Pro — rolling paywall on every time-tracked table

The rolling paywall covers **every table with a month-or-finer time column** (36
of 40), each on its finest column; `register_table_materialization_task` rolls
the window on every run. The two year-only series stay `AllFree` (a sub-year
window is not expressible on a year column), and the two tables with no record
time dimension — `senador` (a full-refresh dimension) and `dicionario` — take no
coverage. All four stay free.

| Group | Tables | Date column | Format | Tier |
|---|---|---|---|---|
| Snapshots | the 28 `data_extracao` tables | `data_extracao` | `YEAR_MD` | PartBdpro, 6-month lag |
| Series with a `data` column | `despesa_ceaps`, `servidor_hora_extra_dia`, `suprido_ato_concessao`, `suprido_empenho`, `suprido_transacao`, `suprido_movimentacao` | `data` | `YEAR_MD` | PartBdpro, 6-month lag |
| Monthly series | `servidor_remuneracao`, `servidor_hora_extra` | `ano` + `mes` | `YEAR_MONTH` | PartBdpro, 6-month lag |
| Year-only series | `suprido_transacao_objeto`, `suprido_movimentacao_subtipo` | `ano` | `YEAR` | AllFree (no paywall) |

For the series with real history (CEAPS to 2008, payroll to 2013) the paywall
leaves most data free and covers only the recent tail; the snapshots have no
history yet, so their free tier fills in as snapshots age past the lag.

**Prerequisite before arming:** every `part_bdpro` table needs BOTH a free
(`is_closed=False`) and a pro (`is_closed=True`) Coverage to pre-exist, or the
first run hard-fails at `assert_coverage_topology`. The onboarding registered
only the free Coverage on each table, so the pro Coverage must be created on the
**36** paywalled tables before the schedule is armed. The two `AllFree` year-only
tables need only their existing free Coverage (and no pro Coverage).

## Update / Poll records

Snapshot-stacking has no "did the source update?" question — we snapshot on
schedule — so there is no source poll gate (as SICONFI). Each run records the
table Update (wall clock) and commits the source Update (snapshot date). The
Poll record is created at arm time.

## Files

- `constants.py` — `DATASET_ID`, table lists (reuse `utils.PARTITIONED/SNAPSHOTS`),
  first years, schedule minute.
- `tasks.py` — `@task` wrappers over `clean_all` (download+clean in one, the
  extract *is* the download here), split into daily/weekly table sets.
- `flows.py` — the flow + two `deploy_schedules`, `job_variables` sized to the
  payroll year (peak RAM), `PartBdpro` coverage map.

## Validation (the gate)

Local checks fail fast but prove little; the gate is a **dev-pool run** with
`{"materialize_to_prod": False, "update_metadata": False, "force_run": True}`
showing `dbt run OK` + `dbt test OK` for every table, clone path
`/app/pipelines-pipeline-br_senado_dados_abertos_administrativos/`. Needs the
`deploy-flow` label; the PR changes `flows.py`, so the deploy has something to
pick up.
