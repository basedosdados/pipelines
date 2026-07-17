# Prefect Pipeline Conventions

Reference for building **recurring** pipelines (Prefect 3) in the `pipelines`
repo — the automated refresh that keeps an already-onboarded dataset up to date.
Static onboarding (raw → clean → BigQuery → metadata, one shot) is covered by
`onboarding-workflow`; this rule covers the ongoing pipeline that re-runs it.

Reference implementation: `pipelines/datasets/us_bls_cpi/` (monthly). The closest
Brazilian analog is `pipelines/datasets/br_ibge_ipca/` +
`pipelines/crawler/ibge_inflacao/`.

## When to build one

Build a recurring pipeline when the source publishes on a cadence (monthly CPI,
daily prices, annual survey). Skip it for one-off/frozen datasets. A recurring
pipeline is added **after** the static onboarding is verified in dev — it reuses
the same dbt models, architecture, and cleaning transform.

## Directory layout

```
pipelines/datasets/<gcp_dataset_id>/
├── __init__.py      # empty
├── constants.py     # Enum `constants`: URLs, headers, table list, schema dir…
├── utils.py         # PURE functions: download + cleaning transform (no Prefect)
├── tasks.py         # @task wrappers over utils
└── flows.py         # @flow(s) + inline schedule; the deploy unit
```

Flows **must be defined at module level in `flows.py`** — the deploy script only
picks up `Flow` objects whose function is defined in that file
(`obj.fn.__code__.co_filename` check). A factory that returns an inner `@flow`
(see `br_ibge_ipca`) is fine because the inner fn is still defined in that file.

## DRY with the onboarding code

The cleaning transform lives in **one place** and is shared:

- Put pure functions (`download_*`, `build_*`, `write_partitioned`, `clean_all`)
  in `pipelines/datasets/<ds>/utils.py`. No Prefect imports there.
- The one-shot bootstrap under `models/<ds>/code/` **imports** those functions
  (`from pipelines.datasets.<ds>.utils import ...`) instead of duplicating them.
- Schema/column order comes from the architecture CSVs
  (`models/<ds>/code/architecture/*.csv`) — the single source of truth — read
  via a repo-relative path in `constants.py`.

Verify the port reproduces the exact same row counts as the validated bootstrap
before committing (reuse the already-downloaded `input/`).

## The canonical flow recipe

From `pipelines/crawler/ibge_inflacao/flows.py::_run_ibge_inflacao` and
`us_bls_cpi/flows.py`:

```
rename_flow_run_dataset_table(prefix="Dump: ", dataset_id, table_id)
download → clean                      # your @task wrappers over utils
max_date = <latest period in source>  # e.g. "YYYY-MM"
has_new = poll_source_for_update_task(dataset_id, table_id, max_date, env="prod", date_format)
if not has_new and not force_run: return
for table in tables:                  # dev first
    upload_to_gcs(data_path, dataset_id, table, bucket_name="basedosdados-dev", dump_mode, source_format)
    run_dbt(dataset_id, table, dbt_command="run/test", target="dev")
if not materialize_to_prod: return
for table in tables:                  # then prod
    upload_to_gcs(..., bucket_name="basedosdados", ...)
    run_dbt(..., target="prod")
if update_metadata:
    register_table_materialization_task(dataset_id, table, coverage=<CoverageSpec>, env="prod", bq_project="basedosdados")
    commit_source_update_task(dataset_id, table, max_date, env="prod", date_format)  # ONLY at the very end
```

`poll_source_for_update_task` records a `Poll` and returns whether the source is
newer than the registered `Update.latest`; `commit_source_update_task` writes the
new `Update.latest` and must run **last**, only after prod succeeded. The poll
guard makes a scheduled run a no-op until the source actually publishes.

## Shared building blocks (`pipelines/utils`)

| Function (module) | Signature (key args) | Purpose |
|---|---|---|
| `rename_flow_run_dataset_table` (`utils.tasks`) | `prefix, dataset_id, table_id` | Names the run in the Prefect UI |
| `upload_to_gcs` (`utils.tasks`) | `data_path, dataset_id, table_id, bucket_name, dump_mode="append", source_format="csv"` | Upload a file/partitioned dir to `<bucket>/staging/<ds>/<table>` + create staging BQ table |
| `run_dbt` (`utils.tasks`) | `dataset_id, table_id=None, dbt_alias=True, dbt_command="run/test", target="dev", flags=None, _vars=None` | dbt run and/or test, then upload artifacts |
| `poll_source_for_update_task` (`utils.metadata.tasks`) | `dataset_id, table_id, source_max_date, env="dev", date_format="%Y-%m-%d"` → `bool` | Record a Poll; is source newer than `Update.latest`? |
| `commit_source_update_task` (`utils.metadata.tasks`) | `dataset_id, table_id, source_max_date, env, date_format` | Write source `Update.latest` (call last) |
| `register_table_materialization_task` (`utils.metadata.tasks`) | `dataset_id, table_id, coverage: CoverageSpec, env="dev", bq_project="basedosdados"` | Update coverage `DateTimeRange` + table update from BQ |

`dump_mode`: **`"append"`** (create staging table once, then append) or
**`"overwrite"`** (drop + recreate — use for sources that ship the *full history*
each release, like BLS flat files). No `"replace"`. `source_format="parquet"` is
supported; `data_path` may be a hive-partitioned directory (`.../year=YYYY/data.parquet`).

### Staging parquet must be all-STRING (current limitation)

`upload_to_gcs` builds the staging table from a one-row header written by
`gcs.py::dump_header`, and the parquet branch does `.to_pandas().astype(str)` — so
BigQuery infers **every column as STRING**. Emitting *typed* parquet then fails on read:

```
Parquet column 'index_value' has type DOUBLE which does not match the
target cpp_type STRING_PIECE
```

Until `dump_header` is fixed, a pipeline writing parquet must cast to an all-string
schema. Two details are load-bearing:

- **Cast via arrow, never `astype(str)`** — the latter renders NULL as the literal
  `"nan"`, which `safe_cast` will not turn back into NULL.
- **Pass through the architecture's real types first**, then cast. Otherwise `year`
  serializes as `"1959.0"` and `safe_cast(year as int64)` yields NULL.

This costs nothing downstream: staging is all-STRING by house convention anyway and the
dbt model `safe_cast`s every column. `us_bls_cpi/utils.py::write_partitioned` is the
reference. Note `bigquery-conventions.md` tells you to pin an explicit `pa.Schema` —
correct for the one-shot onboarding upload, wrong here. → `project_dump_header_parquet_bug`

### One raw data source per table (current limitation)

`client._raw_source_id` resolves the table's raw source via `_query_id`, which **raises
when a table has 2+ raw sources**:

```
allRawdatasource: mais de um nó encontrado para {'tables_Id': …}
```

The poll and commit tasks both go through it, so a table linked to two sources cannot
run a recurring pipeline at all — it fails at the first poll. The data model permits the
M2M; the client is over-constrained. Until it is fixed, link each table to exactly **one**
raw source (the primary one), and note in the dataset's memory that the others should be
re-linked afterwards. → `project_metadata_multi_raw_source_bug`

### Coverage domain types (`utils.metadata.domain`)

Build a `CoverageSpec` per table; the `date_column.kind` must match `date_format`:

| Tier class | Use |
|---|---|
| `AllFree(date_column, date_format)` | Fully public data (no BD-pro gating) |
| `PartBdpro(date_column, date_format, free_lag=6 months)` | Recent data behind BD-pro |
| `AllBdpro(...)` / `NonHistorical()` | All-pro / single last-modified coverage |

Date columns: `YearMonth(year=, month=)`→`DateFormat.YEAR_MONTH`,
`YearOnly(col=)`→`DateFormat.YEAR`, `DateOnly(col=)`→`DateFormat.YEAR_MD`,
`YearQuarter(year=, quarter=)`→`DateFormat.YEAR_MONTH`.

## Update and Poll records — a recurring pipeline must have all three

A recurring dataset carries three records, and a pipeline is not finished until all three
exist. They answer different questions and are easy to conflate:

| Record | Anchor | `latest` | Written by |
|---|---|---|---|
| `Update` | table | when **we** last refreshed (wall clock) | `register_table_materialization_task` |
| `Update` | raw data source | what the **source** published — its **max coverage date** | `commit_source_update_task` |
| `Poll` | raw data source | when we last **looked** (wall clock) | `poll_source_for_update_task` |

**The source Update is a coverage date; the table Update and Poll are wall clocks.** "Did
the source release anything new?" is not a stored boolean — it is `Poll.latest` (when we
looked) versus `RawDataSource.Update.latest` (what they had).

The flow recipe already writes all three, but **only on a run with
`update_metadata=True`** — so a dataset tested with metadata off will have a Poll and no
source Update. Create the source Update at onboarding rather than waiting for the first
real run:

```
create_update_update(raw_data_source_id=…, entity_id=<month>, frequency=1,
                     latest="<source max coverage date>", env="prod")
```

`frequency` = units between releases (1 + `month` = monthly); `lag` = publication delay in
the same units (CPI month M lands in M+1 → `lag=1`), conventionally unset on the source
Update. `upsert_raw_source_update` hardcodes `entity=month`, so a non-monthly source still
gets a month-entity source Update — expected, not a bug to fix per dataset.

Reference (`br_ibge_ipca`, and `cpi`):

```
Table.Update          month  freq=1  lag=1     latest=2026-07-17  (wall clock)
RawDataSource.Update  month  freq=1  lag=None  latest=2026-06-01  (coverage date)
RawDataSource.Poll    day    freq=1            latest=2026-07-17  (wall clock)
```

## BD Pro rolling window (high-frequency data)

**Business rule: any table refreshed monthly or more often paywalls its most
recent window to BD Pro subscribers; everything older stays free.** Lower-frequency
tables in the same dataset (annual, semiannual) stay `AllFree`. A `dicionario` (or
any table with no date column) can take no coverage spec at all.

Pick the tier per table, not per dataset. Reference: `us_bls_cpi` — `monthly` is
`PartBdpro(free_lag=6 months)`; `annual`/`semiannual` are `AllFree`.

### It is already built — do not hand-roll it

`register_table_materialization_task` does the whole thing on every run:

1. `source_end = bq.read_max_date(...)`
2. `free_end = source_end - free_lag`
3. upserts **both** `DateTimeRange`s — free ends at `free_end`; pro spans
   `free_end + 1 period → source_end`. `free_end` is **inclusive** (the RAP grants
   `date <= free_end`), so the two ranges are mutually exclusive — pro starts the
   period *after* free ends, stepping by the spec's granularity.
4. re-issues the BigQuery **Row Access Policies** (`policy.needs_row_access_policy` is
   True only for `part_bdpro`):

```sql
CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON `<proj>.<ds>.<tbl>`
  GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")
  FILTER USING (TRUE);
CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON `<proj>.<ds>.<tbl>`
  GRANT TO ("allUsers") FILTER USING (<date_col> <= "<free_end>");
```

So the window **rolls on its own** — each run recomputes `free_end` and moves it
forward. There is nothing monthly to do by hand.

**The dbt model does NOT change.** The paywall is enforced by Row Access Policies in
BigQuery, not by SQL. (`br_bd_diretorios_brasil__empresa.sql` has a `max_bdpro_date`
CTE — that is a *different*, static pattern; do not copy it for a rolling window.)

### Prerequisite: two Coverages must exist first

`Coverage.is_closed` is the free/pro discriminator, and the polarity is
counterintuitive:

| Coverage | `is_closed` | Meaning |
|---|---|---|
| free | `False` | Open/public data |
| pro | `True` | BD Pro data — drives `Table.contains_closed_data` (the site's Pro badge) |

Set `is_closed` on the **`DateTimeRange` too**, matching its Coverage (pro range →
`is_closed=True`). The pipeline never writes it — `DateTimeRangeInput` has no such
field — so whatever you register at onboarding is what stays. Every existing
part_bdpro table has it set on both.

Reference shape (`us_bls_cpi.monthly`, `free_lag=6 months`, source `1913-01..2026-06`):

| Coverage | `is_closed` | DateTimeRange | range `is_closed` |
|---|---|---|---|
| free | `False` | `1913-01 .. 2025-12` | `False` |
| pro | `True` | `2026-01 .. 2026-06` | `True` |

`assert_coverage_topology` **hard-fails before any write** if the topology does not
match the tier — `part_bdpro exige Coverage free + pro`. A table onboarded as
`AllFree` has only the free Coverage, so **create the pro Coverage before switching
the spec**:

```
create_update_coverage(table_id=…, area_id=…, is_closed=True, env="prod")
```

Conversely `AllFree` requires free **and no pro**, and `AllBdpro` the reverse — so
you cannot silently flip tiers without fixing the coverages too.

### What you can verify locally

`assert_coverage_topology`, `compute_coverage_ranges`, and `needs_row_access_policy`
are pure — unit-test the window and its roll. `apply_row_access_policies` issues real
BigQuery DDL and needs the worker's rights; it is **not** exercisable locally. Say so
rather than implying the paywall was tested end-to-end.

## Environments & credentials — pipelines run on deployed workers, not locally

`upload_to_gcs` sets `billing_project_id = bucket_name`. `bucket_name="basedosdados-dev"`
writes the dev project; `"basedosdados"` writes prod. The prod write only works
on the **deployed Prefect worker** (its pod SA has access) — the local
`~/.basedosdados/config.toml` is provisioned for `basedosdados-dev` only, so you
**cannot exercise the prod half locally**. (dbt's own prod staging reads from
`basedosdados-staging`; see `reference_databasis_prod_promotion` in memory.)

## Scheduling & deploy

Schedule inline on the flow object (do NOT register storage/run-config by hand):

```python
my_flow.deploy_schedules = [{"cron": "0 16 10,11,12,13 * *", "timezone": "America/Sao_Paulo"}]
my_flow.job_variables = {"memory": "8Gi"}   # optional; size to the clean step's peak RAM
```

Deploy is CI, via `.github/scripts/deploy_flows.py`:
- **Dev pool** (`cd-prefect3-staging.yaml`, `--pool basedosdados-dev`, on PR):
  runs **only if the PR carries the `deploy-flow` label** — no label, no deploy, and
  the job reports `skipped`, not failed. A PR without it deploys **nothing** and
  looks fine. The workflow triggers on `labeled` and `synchronize`, so adding the
  label is itself enough. Schedules are **stripped** — manual runs only.
- **Prod pool** (`cd-prefect3.yaml`, `--pool basedosdados --all`, on merge to main):
  schedules become `Cron` objects; deployed **`paused=True`**.
- Cron in `America/Sao_Paulo`; see crontab.guru. For a monthly source, poll across
  a few release-window days — the source-poll guard no-ops until a new period lands.

### Arming is a manual step — merging does NOT start the schedule

Merging deploys to prod **paused**. CI then POSTs `admin-tools/sync-deployments/`,
which for an **unknown** deployment creates a `DisabledFlowSchedule` row with
`is_schedule_active=False` — so it **stays paused**. Sync only *enforces* the stored
state for deployments it already knows; it never arms a new one.

To arm: **https://backend.basedosdados.org/admin/admin_data_tools/disabledflowschedule/**
→ find `flow_name` → tick `is_schedule_active` → save. The admin's `save_model` calls
`Prefect3Client().set_paused(deployment_id, paused=False)`, so the tick un-pauses
Prefect directly; unticking re-pauses it (the kill switch).

Note a paused deployment still shows its schedule as `active=True` — deployment-level
`paused` wins. Read `paused`, not the schedule flag.

**Before arming, know what the first run does**: it is the first execution of the prod
upload (`bucket_name="basedosdados"`) and, for `part_bdpro`, of
`apply_row_access_policies` — i.e. the paywall goes live and the open-data export
starts excluding the pro window. Neither is exercisable locally. Watch that run.

## Verification — a dev run is part of the job, not a bonus

**Local checks do not tell you whether the pipeline works.** On `us_bls_cpi` they all
passed while three separate bugs waited in the parts they cannot reach: a raw-source
link that made the poll raise, typed parquet BigQuery refused, and an upload whose
staging schema contradicted the data. Each surfaced on the first real run. Do the local
checks to fail fast, then **run it**.

### 1. Local (fail fast, proves little)

1. **Imports**: `uv run python -c "from pipelines.datasets.<ds> import flows; print(flows.<flow>.name)"`.
2. **Transform parity**: run `clean_all` on the existing `input/` and assert the
   same row counts as the onboarding validation.
3. **Download**: fetch one small dimension file via `requests` + the real headers.
4. **Deploy discovery**: run `deploy_flows.load_flows_from_file("pipelines/datasets/<ds>/flows.py")`
   and confirm the flow name appears. `load_flows_from_file` **swallows import errors**
   and returns `{}`, so a broken import means a silent no-deploy.
5. Pure metadata policy (`compute_coverage_ranges`, `assert_coverage_topology`,
   `needs_row_access_policy`) is unit-testable — test the window and its roll.

### 2. A real dev run (the actual gate)

Add the **`deploy-flow`** label, wait for `cd-prefect3 (staging)` to report
`registrado`, then trigger the deployment manually **with prod off**:

```
parameters = {"materialize_to_prod": False, "update_metadata": False, "force_run": True}
```

**All three matter.** The flow defaults are `materialize_to_prod=True,
update_metadata=True` — a run triggered with `{}` writes **prod data, prod metadata,
and applies the paywall**, from the dev pool, because the metadata tasks are pinned
`env="prod"` regardless of pool. `force_run=True` bypasses the poll guard, which
otherwise returns before doing anything.

Done means: every task `COMPLETED`, and the logs show `dbt run OK` **and**
`dbt test OK` for each table.

### 3. Reading the result — green does not mean it ingested

The poll guard returns early and Prefect still reports **`COMPLETED`**. A dead pipeline
is indistinguishable from a working one by state alone; `br_ibge_ipca` sat at 4 ingests
in 60 completed runs unnoticed. To tell them apart, read the logs
(`Não há novas atualizações na fonte original` = polled, did nothing) or check whether
the coverage/Update records moved.

Use the `mcp__databasis__*` Prefect tools — `list_flow_runs(flow_name=…)`,
`get_flow_run_logs(<run_id>)`, `get_failed_flow_runs()`. They speak the Prefect 3 REST
API; anything pointing at `prefect.basedosdados.org` is the retired Prefect 2 host and
returns nothing.

### 4. What is NOT verifiable before arming

The prod upload (`bucket_name="basedosdados"`) and `apply_row_access_policies` need the
worker's rights and run for the first time on the first armed run. Say so plainly rather
than implying the pipeline is proven end-to-end.

## Commit discipline

`feat(<dataset_id>): add recurring Prefect pipeline`. Never commit `input/` or
`output/` (data). Pre-commit runs ruff/format — pre-format new `.py` with
`uv run pre-commit run --files ...` before committing to avoid the hook re-write loop.
