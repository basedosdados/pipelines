# Prefect Pipeline Conventions

Reference for building **recurring** pipelines (Prefect 3) in the `pipelines`
repo — the automated refresh that keeps an already-onboarded dataset up to date.
Static onboarding (raw → clean → BigQuery → metadata, one shot) is covered by
`onboarding-workflow`; this rule covers the ongoing pipeline that re-runs it.

New pipelines use the **staged (event-driven) structure**:
`check_update → extract_and_load → build_and_promote`. The old single-flow
("monolithic") shape is summarized at the end under
"Legacy: monolithic flow" — not every dataset has been
migrated yet.

Reference implementations:
- single-source, multi-table: `pipelines/datasets/br_ibge_ipca/` (4 monthly
  tables, `PartBdpro`, shared code in `pipelines/shared/ibge_inflacao/`);
- multi-table mixing coverage tiers and a per-table cron:
  `pipelines/datasets/br_rf_cnpj/` (`empresas`/`estabelecimentos`/`socios` are
  `PartBdpro`; only `simples`/`dicionario` are `NonHistorical`);
- overwrite-mode dataset: `pipelines/datasets/us_bls_cpi/`.

The API lives in `pipelines/utils/stage_dispatch.py`.

## When to build one

Build a recurring pipeline when the source publishes on a cadence (monthly CPI,
daily prices, annual survey). Skip it for one-off/frozen datasets. A recurring
pipeline is added **after** the static onboarding is verified in dev — it reuses
the same dbt models, architecture, and cleaning transform.

Before writing anything, open the dataset's real code
(`pipelines/datasets/<ds>/`, and `pipelines/shared/<name>/` if any) to learn how
many tables there are and whether the freshness check is really cheap.

## The staged pipeline

One dataset (or table) becomes **two scheduled-chain flows plus one shared flow**,
each its own Prefect deployment in its own pod:

| Stage | Does | Deployments |
|---|---|---|
| `check_update` | Asks the source for its latest date, compares to the backend; if newer, commits the source `Update` and dispatches the next stage | one per dataset (per table if multi-table); **carries the cron** |
| `extract_and_load` | Downloads/cleans the data, uploads to dev staging, dispatches `build_and_promote` | one per dataset/table; **no schedule** — only triggered by the check |
| `build_and_promote` | `dbt run/test` in dev → (if allowed) transfer to prod → `dbt run/test` in prod → register materialization/coverage | **one generic deployment shared by every dataset** (`pipelines/utils/metadata/flows.py`) — never defined in a dataset's `flows.py` |

- **Chaining** is `run_deployment(timeout=0, as_subflow=True)`, called by the
  pipeline class — the dataset code never dispatches by hand.
- **Handoff**: `check_update → extract_and_load` passes `download_params`
  (always `reference_date`, plus `SourceInspection.extra_download_params`).
  `extract_and_load → build_and_promote` passes `dataset_id, table_id, coverage,
  partition_folders, dump_mode, source_format, prefect_mode, …`.
- **`promote_to_prod` is not set by the dataset.** It is `not _dev_prefix()`: true
  only when the `extract_and_load` ran in a prod deployment. `deploy_flows.py` tags
  deployments `env:dev` / `env:prod` (plus `env:local` for `--local`), and dev
  deployments are named `dev-<flow>`.
- `build_and_promote` **always runs on the prod pool**, even when dispatched from
  dev. From a dev `extract_and_load` it runs with `promote_to_prod=False` (dev dbt
  only). Its job variables are fixed (`BUILD_AND_PROMOTE_JOB_VARIABLES`,
  `memory_limit: 2Gi`).

Only the standard variant exists in code. For a source whose date can only be found
by downloading (e.g. `us_bls_cpi`), still use it: `extract_load_data` downloads
again independently — note the duplicated download in a comment in `flows.py`.

Not supported yet: sources that detect change by **file size** instead of a date
(e.g. `br_bcb_sicor`) — `stage_dispatch` compares dates.

## Directory layout

```
pipelines/datasets/<gcp_dataset_id>/
├── __init__.py      # empty
├── constants.py     # DATASET_ID, one <TABLE>_TABLE_ID per table, COVERAGE specs, URLs, params
├── utils.py         # PURE functions: download + cleaning transform (no Prefect)
├── tasks.py         # get_latest_update / extract_load_data (factories) + make_pipeline
├── flows.py         # wiring only: pipeline + @flow(s) + schedule; the deploy unit
└── README.md        # source, file layout, known issues, what each flow does
```

Flows **must be defined at module level in `flows.py`** — the deploy script only
picks up `Flow` objects whose function is defined in that file
(`obj.fn.__code__.co_filename` check).

**Do not write a shared `@flow` factory** that returns inner flows for several
tables: `_pipeline.extract_load_deployment = <flow>.fn.__name__` would be the same
string for every instance and the deployments would collide. Repeat the block
explicitly once per table (see `br_ibge_ipca/flows.py`, `br_rf_cnpj/flows.py`).

### `@flow` comes from `pipelines.utils.flow`, not from `prefect`

```python
from pipelines.utils.flow import flow  # NOT `from prefect import flow`
```

The deploy script reads two attributes off the flow object — `deploy_schedules`
and `job_variables` — that `prefect.Flow` does not declare, so setting them on a
plain Prefect flow is a Pyrefly `missing-attribute` error. `pipelines/utils/flow.py`
declares both on a `prefect.Flow` subclass and exports a `flow` decorator that
builds it; it takes the same arguments as `prefect.flow` and the object stays a
`prefect.Flow` for every `isinstance` check. `deploy_tags` is set the same way.
Both attributes default to empty (no schedule, work-pool default infrastructure).
`deploy_schedules` is a list of `prefect.schedules.Cron` —
`Cron("0 16 10 * *", timezone="America/Sao_Paulo")`.

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

## Writing a dataset's flow — step by step

### 1. `constants.py`

`DATASET_ID`; one `<TABLE>_TABLE_ID` per table; the `CoverageSpec` per table
(`PartBdpro` / `AllFree` / `AllBdpro` / `NonHistorical`, see
"Coverage domain types" below); download
parameters. `br_ibge_ipca`:

```python
DATASET_ID = "br_ibge_ipca"
MES_BRASIL_TABLE_ID = "mes_brasil"
...
COVERAGE = PartBdpro(
    date_column=YearMonth(year="ano", month="mes"),
    date_format=DateFormat.YEAR_MONTH,
)
```

`br_rf_cnpj` has one coverage per table (`COVERAGE[table_id]`) and a
`CHECK_UPDATE_CRON[table_id]` dict so each table's cron is data, not code.

### 2. `get_latest_update` (in `tasks.py`)

Discovers the source's latest date and returns a `SourceInspection`. **It does not
decide whether the data is new** — `poll_source_for_update_task`, called inside the
pipeline, compares against the backend.

```python
@dataclass
class SourceInspection:
    reference_date: datetime.date
    extra_download_params: dict = {}  # whatever extract_load_data needs
    compare_against: str = "coverage"  # or "table_update"
```

- `compare_against="coverage"` (default) compares to `Coverage.DateTimeRange`.
  Use `"table_update"` (compares to `Table.Update.latest`) for **`NonHistorical`**
  tables, which have no trustworthy coverage baseline.
- Multi-table: pass a **factory** `make_get_latest_update(table_id)` returning the
  right callable. If one function serves all tables, pass `lambda _: fn`.
- `br_rf_cnpj`: empresas/estabelecimentos/socios return the folder competência
  (compared to coverage); simples/dicionario return `last_modified_date` with
  `compare_against="table_update"`; both put `folder_date`/`last_modified_date` in
  `extra_download_params`.

### 3. `extract_load_data` (in `tasks.py`)

Receives `download_params` and **actually downloads/cleans**, returning:

```python
@dataclass
class ExtractAndLoad:
    coverage: dict  # CoverageSpec.model_dump()
    data_path: str  # local file, or Hive-partitioned directory
    prefect_mode: str = "prod"
    dump_mode: str = "append"  # or "overwrite"
    source_format: str = "csv"  # "parquet" must be declared — see below
    partition_folders: list[str] | None = field(
        init=False
    )  # NOT yours to fill
```

- **Never call `upload_to_gcs` here.** `run_extract_and_load` uploads from
  `data_path` right after you return; calling it too uploads twice.
- Declare `source_format="parquet"` for parquet. The default is `"csv"`, and an
  undeclared parquet only fails later, at promotion (`FileNotFoundError`, it looks
  for `.csv`). `dump_mode`/`source_format` travel in `ExtractAndLoad` so the dev
  upload and the prod promotion use the same values.
- **Partitioned tables**: `data_path` must be a directory **already in the final
  Hive layout** (`data_path/ano=2026/mes=09/arquivo.csv`) holding **only the
  partitions that changed in this run**. `partition_folders` is discovered by
  `discover_partition_folders(data_path)` (leaf `key=value` dirs; `None` if no
  subdirs) — never set by the dataset. Only those folders are downloaded from dev
  and promoted to prod; `None` promotes the whole staging dir.

### 4. `make_pipeline` (in `tasks.py`, last definition in the file)

Multi-table: `make_pipeline = pipeline_factory(DATASET_ID, make_get_latest_update,
make_extract_load_data, **shared_kwargs)`. `shared_kwargs` (e.g.
`date_format="%Y-%m"`) go to `CheckThenExtractLoadPipeline` and can be overridden
per table: `make_pipeline(TABLE_ID, date_format=...)`.

Single table: skip the factory and instantiate
`CheckThenExtractLoadPipeline(dataset_id=, table_id=, get_latest_update=,
extract_load_data=, ...)` directly in `flows.py`.

`date_format` is the format of the date in the check (default `"%Y-%m-%d"`; use
`"%Y-%m"` for monthly data). `prefect_dataset_id` defaults to
`f"{dataset_id}__{table_id}"` — repo convention even for single-table datasets.

Mother table + derived tables with no download of their own (dbt-only, e.g.
aggregates from a `microdados`): use
`run_extract_and_load_with_derived_tables(...)` instead of `run_extract_and_load`
(see `br_ms_sim/flows.py`). It materializes and promotes mother + derived inline
in the `extract_and_load` run via `promote_primary_and_derived_tables`, and does
**not** yet support promoting a partitioned slice (mother table must be
unpartitioned).

### 5. `flows.py`

Only wiring. From `br_ibge_ipca` (one block per table):

```python
_mes_brasil_pipeline = make_pipeline(MES_BRASIL_TABLE_ID)


@flow(name=_mes_brasil_pipeline.check_update_flow_name, log_prints=True)
def br_ibge_ipca_mes_brasil_check_update() -> None:
    _mes_brasil_pipeline.run_check_update()


br_ibge_ipca_mes_brasil_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MES_BRASIL_TABLE_ID
)
br_ibge_ipca_mes_brasil_check_update.deploy_schedules = [
    Cron("40 14 8,9,10,11,12,13 * *", timezone="America/Sao_Paulo")
]


@flow(name=_mes_brasil_pipeline.extract_and_load_flow_name, log_prints=True)
def br_ibge_ipca_mes_brasil_download(download_params: dict) -> None:
    _mes_brasil_pipeline.run_extract_and_load(download_params)


br_ibge_ipca_mes_brasil_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MES_BRASIL_TABLE_ID
)
_mes_brasil_pipeline.extract_load_deployment = (
    br_ibge_ipca_mes_brasil_download.fn.__name__
)
```

The order is not arbitrary:

1. Create the pipeline (`make_pipeline(TABLE_ID)`).
2. `@flow(name=_pipeline.check_update_flow_name, ...)` — **never hand-build the
   name**: dispatch recomputes it by the same convention
   (`"<etapa>: <prefect_dataset_id>"`).
3. `deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, TABLE_ID)` — pass `table_id`
   whenever the dataset has more than one table (adds the
   `<dataset>__<table>` tag; `staged-pipeline` is always added).
4. `deploy_schedules` **on the `check_update` flow only**.
5. Same for the `extract_and_load` flow (tags, **no** schedule). It takes
   `download_params: dict`.
6. **Last**: `_pipeline.extract_load_deployment = <flow>.fn.__name__`, after the
   function exists. Never a hand-typed string — it must match the real deployment
   name (the flow variable is not literally `extract_and_load`).

Convention: `<ds>_<table>_check_update` and `<ds>_<table>_download` for the two
function names, with a comment banner per table listing both flow names.

Per-stage job variables go on the flow object (see
"`memory` alone is silently ignored" below):
size `extract_and_load` for the clean step's peak RAM. `check_update` stays small.

### 6. Replacing a monolithic flow

Delete the old flow code from the file **entirely, in the same PR**. Deploy
registers every `Flow` it finds in the file, so a forgotten old flow keeps being
registered next to the new ones — and deploying from your branch would repoint the
real deployment's source. Also keep the old cron on the new `check_update`
(`# Mesmo cron do flow monolítico antigo (main)` in `br_ibge_ipca`), so release
timing doesn't silently change. After merge, turn off the old monolithic
deployment manually once the new one is confirmed (it stays registered, active or
paused, until removed).

### Known gap: side effects on other datasets

A monolithic flow sometimes also materialized *another* dataset after a table
(`br_rf_cnpj.estabelecimentos` re-ran `br_bd_diretorios_brasil.empresa` and
registered its coverage). `build_and_promote` is generic and has no such hook, so
that tail has no home in the staged shape yet — it is a commented-out TODO in
`br_rf_cnpj/flows.py`. Don't drop such side effects silently when migrating;
flag them.

## Shared building blocks

| Function (module) | Signature (key args) | Purpose |
|---|---|---|
| `pipeline_factory` (`utils.stage_dispatch`) | `dataset_id, get_latest_update_factory, extract_load_data_factory, **shared_kwargs` → `make(table_id, **overrides)` | Per-table `CheckThenExtractLoadPipeline` |
| `CheckThenExtractLoadPipeline` (`utils.stage_dispatch`) | `dataset_id, table_id, get_latest_update, extract_load_data, prefect_dataset_id=None, env="prod", date_format, extract_load_deployment=None` | `run_check_update()`, `run_extract_and_load(download_params)`, `*_flow_name` properties |
| `check_update_and_dispatch` (`utils.stage_dispatch`) | `prefect_dataset_id, dataset_id, table_id, reference_date, …, compare_against` | poll → commit source `Update` → `run_deployment` next stage (internal to the pipeline class) |
| `dispatch_build_and_promote` (`utils.stage_dispatch`) | `dataset_id, table_id, result: ExtractAndLoad, env` | Triggers the generic `build_and_promote`; derives `promote_to_prod` |
| `deploy_tags` (`utils.stage_dispatch`) | `dataset_id, etapa, table_id=None` | Tags for finding deployments (`staged-pipeline`, stage, dataset, `<ds>__<table>`) |
| `build_and_promote` (`utils.metadata.flows`) | `dataset_id, table_id, coverage, env, bq_project, prefect_mode, promote_to_prod, partition_folders, dump_mode, source_format, …` | The shared flow; do not define one per dataset |
| `rename_flow_run_dataset_table` (`utils.tasks`) | `prefix, dataset_id, table_id` | Names the run in the UI (done for you by the pipeline class) |
| `upload_to_gcs` (`utils.tasks`) | `data_path, dataset_id, table_id, bucket_name, dump_mode="append", source_format="csv"` | Upload a file/partitioned dir to `<bucket>/staging/<ds>/<table>` + create staging BQ table (done for you; don't call from `extract_load_data`) |
| `run_dbt` (`utils.tasks`) | `dataset_id, table_id=None, dbt_alias=True, dbt_command="run", target="dev", flags=None, _vars=None` | dbt run and/or test, then upload artifacts (done for you in `build_and_promote`) |
| `poll_source_for_update_task` (`utils.metadata.tasks`) | `dataset_id, table_id, source_max_date, env, date_format, compare_against` → `bool` | Record a Poll; is source newer? |
| `commit_source_update_task` (`utils.metadata.tasks`) | `dataset_id, table_id, source_max_date, env, date_format` | Write source `Update.latest` |
| `register_table_materialization_task` (`utils.metadata.tasks`) | `dataset_id, table_id, coverage: CoverageSpec, env, bq_project, prefect_mode` | Update coverage `DateTimeRange` + table update from BQ |

A dataset's own code normally only touches the first three rows; the rest are
called by the pipeline class and `build_and_promote`.

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
reference.

**Write all-STRING staging for the one-shot onboarding upload too, not only the pipeline.**
Both paths share one staging dataset, so their external-table schemas must match. A dataset
onboarded with *typed* parquet gets a *typed* external table; add a pipeline later and its
all-STRING overwrite leaves dbt reading the stale typed table against string files
(`Parquet column ... has type BYTE_ARRAY which does not match the target cpp_type INT32`), or
`safe_cast`ing a still-typed column (`Invalid cast from INT64 to DATE`). Keeping both paths
all-STRING removes the divergence — so `bigquery-conventions.md` no longer pins a *typed*
`pa.Schema` for onboarding; it pins an all-STRING one with stable column order.
→ `project_au_ato_abr`, `project_dump_header_parquet_bug`

### One raw data source per table (current limitation)

`client._raw_source_id` resolves the table's raw source via `_query_id`, which **raises
when a table has 2+ raw sources**:

```
allRawdatasource: mais de um nó encontrado para {'tables_Id': …}
```

The poll and commit tasks both go through it, so a table linked to two sources cannot
run a recurring pipeline at all — it fails at the first poll. A table with **no**
`RawDataSource` is worse: `poll_source_for_update` fails *silently* (the filter is
ignored rather than matching null). The data model permits the M2M; the client is
over-constrained. Until it is fixed, link each table to exactly **one**
raw source (the primary one), and note in the dataset's memory that the others should be
re-linked afterwards. Register the `RawDataSource` **before** migrating.
→ `project_metadata_multi_raw_source_bug`

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
| `Update` | table | when **we** last refreshed (wall clock) | `register_table_materialization_task` (in `build_and_promote`) |
| `Update` | raw data source | what the **source** published — its **max coverage date** | `commit_source_update_task` (in `check_update`) |
| `Poll` | raw data source | when we last **looked** (wall clock) | `poll_source_for_update_task` (in `check_update`) |

**The source Update is a coverage date; the table Update and Poll are wall clocks.** "Did
the source release anything new?" is not a stored boolean — it is `Poll.latest` (when we
looked) versus `RawDataSource.Update.latest` (what they had).

In the staged shape the poll and source `Update` are written by every `check_update`
that finds new data, and the table `Update` by the prod `build_and_promote` — so there
is no "metadata off" mode that leaves a Poll without a source `Update`. A dev-triggered
run still writes the **source** `Update`/`Poll` (they are pinned `env="prod"`), which is
why a dev test on an up-to-date dataset is a no-op: the commit makes the next poll see
"nothing new". Still, create the source Update at onboarding rather than waiting for
the first real run:

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

`register_table_materialization_task` (reached through `build_and_promote` /
`transfer_files_to_prod_flow`) does the whole thing on every promoted run:

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

`upload_to_gcs` sets `billing_project_id = bucket_name`. `extract_and_load` always
uploads to `basedosdados-dev` staging; the **prod** write happens inside
`build_and_promote`'s `transfer_files_to_prod_flow`, and only works on the
**deployed prod-pool worker** (its pod SA has access) — the local
`~/.basedosdados/config.toml` is provisioned for `basedosdados-dev` only, so you
**cannot exercise the prod half locally**. (dbt's own prod staging reads from
`basedosdados-staging`; see `reference_databasis_prod_promotion` in memory.)

A local Prefect server is available through `scripts/dev_local.sh` and
`deploy_flows.py --local --files pipelines/datasets/<ds>/flows.py` (pool
`local-dev`, tags `env:dev` + `env:local`, code read from disk — no commit/push).
In that pool `stage_dispatch` calls the next stage's function directly
(`_run_flow_locally`) instead of `run_deployment()`, which 422s against the local
server.

## Scheduling & deploy

The cron lives on the **`check_update`** flow, inline on the flow object (do NOT
register storage/run-config by hand). `extract_and_load` has no schedule;
`build_and_promote` is shared and not yours:

```python
from prefect.schedules import Cron

from pipelines.utils.flow import flow


@flow(name=pipeline.check_update_flow_name, log_prints=True)
def my_check_update() -> None:
    pipeline.run_check_update()


my_check_update.deploy_schedules = [
    Cron("35 16 10,11,12,13 * *", timezone="America/Sao_Paulo")
]


@flow(name=pipeline.extract_and_load_flow_name, log_prints=True)
def my_download(download_params: dict) -> None:
    pipeline.run_extract_and_load(download_params)


my_download.job_variables = {
    "memory_limit": "8Gi",  # <- the one the pod actually gets
    "memory_request": "2Gi",
}  # optional; size to the clean step's peak RAM
```

For a monthly source, poll across a few release-window days (`10,11,12,13`) — the
poll guard no-ops until a new period lands. Multiple `Cron`s in the list are allowed
(`br_ibge_ppm` uses two for a Sept/Oct window).

### `memory` alone is silently ignored — set `memory_limit` too

**`memory` is not a variable of the work pool's job template.** `basedosdados-dev`
exposes `memory_limit` (default **`4Gi`**) and `memory_request` (default `1Gi`); a key
that is not in the template is dropped without complaint, so a flow that sets only
`memory` runs on **4Gi no matter what number it names**.

Nothing surfaces this. The deploy succeeds, the deployment shows the `job_variables` you
set, and the pod is simply killed later:

```
Container 'prefect-job' was killed due to out-of-memory (OOMKilled)
```

Two `cl_chilecompra_mercado_publico` dev runs died this way at a declared `"16Gi"`, both
after the same four months, while instrumentation showed RSS plateauing at 2.5 GB — far
under the limit that was asked for, and just over the 4Gi that was actually applied.

**As of 2026-09-07, 43 flows under `pipelines/datasets/*/flows.py` set only `memory`**
and were capped at 4Gi regardless of the value written (that count predates the staged
migration and is stale — re-check). Any flow whose workload grew past 4Gi is failing
for this reason and not for the reason its logs suggest. Find the offenders with:

```bash
grep -l '"memory"' pipelines/datasets/*/flows.py | xargs grep -L memory_limit
```

Verify rather than trusting the number you wrote:

```bash
uv run python -c "from databasis_mcp.tools.prefect import _prefect_get; \
print(list(_prefect_get('/work_pools/basedosdados-dev')['base_job_template']['variables']['properties']))"
```

**Pick a minute nobody else is using — never `0`.** The hour follows the source's
publication time, but the minute is free, and defaulting it to `0` piles every
pipeline onto the same instant. Twelve of the pipelines added in Aug 2026 all fired
at `0 16 * * *` before that was spread out. Same-instant runs compete for BigQuery
slots, and if the daily processing quota trips at that moment they **all** fail
together, which also makes it hard to tell which pipeline actually caused the spike.
(Multi-table datasets: give each table's `check_update` its own minute — compare the
`br_ibge_ipca` `:20/:30/:40/:50` spread.)

Before choosing, list what is already taken and pick a free slot:

```bash
grep -rhoE 'Cron\("[^"]*"' pipelines/datasets/*/flows.py | sort | uniq -c | sort -rn
```

Spacing of 5 minutes is plenty. Note this reduces contention, **not** bytes billed
per day — the daily quota is a byte ceiling, and only doing less work (scoped tests,
incremental models, no redundant dev materialization) moves that.

Deploy is CI, via `.github/workflows/scripts/deploy_flows.py`:
- **Dev pool** (`cd-prefect3-staging.yaml`, `--pool basedosdados-dev`, on PR):
  runs **only if the PR carries the `deploy-flow` label** — no label, no deploy, and
  the job reports `skipped`, not failed. A PR without it deploys **nothing** and
  looks fine. The workflow triggers on `labeled` and `synchronize`, so adding the
  label is itself enough. Deployments are named `dev-<flow_name>` (a separate
  registry entry from prod — a PR deploy can never steal the prod deployment),
  tagged `env:dev`, and **schedules are stripped** — manual runs only.
  **The label only covers `flows.py`** — see the subsection below.
- **Prod pool** (`cd-prefect3.yaml`, `--pool basedosdados --all`, on merge to main):
  `deploy_schedules` is passed straight to the deployment (name `<flow_name>`,
  tag `env:prod`); deployed **`paused=True`**.
- Cron in `America/Sao_Paulo`; see crontab.guru.

### `deploy-flow` only deploys a PR that changes `flows.py`

The workflow passes every changed `pipelines/**/*.py` to `deploy_flows.py --files`,
and the script keeps only the files that **define a `Flow` object**. A PR that fixes
`utils.py`, `tasks.py`, `constants.py` or a `*_clean.py` — the usual place a pipeline
bug lives — contributes **zero** deployments.

The job still reports **`pass`**. Nothing says it deployed nothing.

```
senado_api.py:   0 flow(s)     <- load_flows_from_file
senado_clean.py: 0 flow(s)
flows.py:        1 flow(s)
```

The consequence is that the dev run in "Verification" below **cannot validate that
PR**. The deployment keeps whatever git ref it already had, so a trigger silently
runs a *different branch* — whichever PR last touched that `flows.py`, or `main`.
This is not theoretical: `mx_sesnsp_incidencia_delictiva` (#1873) and
`br_senado_dados_abertos` (#1874) both fixed non-`flows.py` code, and three separate
trigger attempts ran another branch's code and reproduced the original error, which
reads exactly like "the fix does not work".

**Confirm which branch actually ran.** The clone path in the logs names it:

```
File "/app/pipelines-<branch-slug>/pipelines/datasets/<ds>/utils.py", line ...
```

`/app/pipelines-main/` means it ran `main`, not your PR. Check this before drawing
any conclusion from a validation run.

To validate a non-`flows.py` fix, pick one:
- **Merge it and watch the next scheduled run** — usually right when the flow is
  already failing every run, so the fix cannot make things worse. Then confirm with
  `uv run python -m pipelines.diagnostics health`.
- **Touch `flows.py` in the same PR** (a real change, not a whitespace nudge) so the
  deploy has something to pick up.

Related: `load_flows_from_file` also **swallows import errors** and returns `{}`, so a
broken import is a silent no-deploy with a green check for the same reason. A green
"deploy flows" is never evidence that anything deployed.

### Arming is a manual step — merging does NOT start the schedule

Merging deploys to prod **paused**. CI then POSTs `admin-tools/sync-deployments/`,
which for an **unknown** deployment creates a `DisabledFlowSchedule` row with
`is_schedule_active=False` — so it **stays paused**. Sync only *enforces* the stored
state for deployments it already knows; it never arms a new one. Only the
`check_update` deployments carry a schedule, so those are the ones to arm.

To arm: **https://backend.basedosdados.org/admin/admin_data_tools/disabledflowschedule/**
→ find `flow_name` → tick `is_schedule_active` → save, **or** use the MCP tool
`set_deployment_schedule_active` (`mcp__databasis__set_deployment_schedule_active`).
Either goes through the backend, whose `save_model` calls
`Prefect3Client().set_paused(deployment_id, paused=False)`; unticking re-pauses it
(the kill switch). **Never un-pause directly in the Prefect UI** — the next sync
re-imposes the stored state and silently pauses it again.

Note a paused deployment still shows its schedule as `active=True` — deployment-level
`paused` wins. Read `paused`, not the schedule flag.

**Before arming, know what the first run does**: it is the first execution of the prod
`build_and_promote` path (prod upload via `transfer_files_to_prod_flow`) and, for
`part_bdpro`, of `apply_row_access_policies` — i.e. the paywall goes live and the
open-data export starts excluding the pro window. Neither is exercisable locally.
Watch that run. Also arm the new `check_update` only **after** the old monolithic
deployment is off, or both will ingest.

## Verification — a dev run is part of the job, not a bonus

**Local checks do not tell you whether the pipeline works.** On `us_bls_cpi` they all
passed while three separate bugs waited in the parts they cannot reach: a raw-source
link that made the poll raise, typed parquet BigQuery refused, and an upload whose
staging schema contradicted the data. Each surfaced on the first real run. Do the local
checks to fail fast, then **run it**.

### 1. Local (fail fast, proves little)

1. **Static**: `uv run ruff check`, `uv run ruff format`, `uv run pyrefly check` on the
   changed files; confirm no `@flow` name or deployment name collides with another
   already-migrated dataset (flow names are `"<etapa>: <dataset>__<table>"`).
2. **Imports**: `uv run python -c "from pipelines.datasets.<ds> import flows; print(flows.<flow>.name)"`.
3. **Transform parity**: run `clean_all` on the existing `input/` and assert the
   same row counts as the onboarding validation.
4. **Download**: fetch one small dimension file via `requests` + the real headers.
5. **Deploy discovery**: run `deploy_flows.load_flows_from_file("pipelines/datasets/<ds>/flows.py")`
   and confirm **every** expected flow name appears (2 per table). `load_flows_from_file`
   **swallows import errors** and returns `{}`, so a broken import means a silent no-deploy.
6. Pure metadata policy (`compute_coverage_ranges`, `assert_coverage_topology`,
   `needs_row_access_policy`) is unit-testable — test the window and its roll.
7. Optionally, the whole chain against a local Prefect server (see
   "Environments & credentials").

### 2. A real dev run (the actual gate)

Add the **`deploy-flow`** label, wait for `cd-prefect3 (staging)` to report
`registrado`, then trigger the **`dev-…` `check_update`** deployment manually from
the Prefect UI. First confirm the PR actually changes `flows.py` — if it does not,
this gate does not apply to it and the run will execute another branch's code (see
"`deploy-flow` only deploys a PR that changes `flows.py`" above).

There are **no** `materialize_to_prod` / `update_metadata` / `force_run` parameters
any more: the dev deployment cannot promote (`promote_to_prod=False`, derived from
`env:dev`), so a dev trigger is safe by construction. Confirm:

1. **The chain forms.** If the source has new data, `check_update` dispatches
   `extract_and_load`, which dispatches `build_and_promote`; each appears as a
   subflow of the previous one in the run tree.
2. **Nothing is promoted from dev.** `build_and_promote` runs only `dbt run/test` on
   dev and ends; no `transfer_files_to_prod_flow`.
3. Every task `COMPLETED`, the logs show `dbt run OK` **and** `dbt test OK`, **and**
   the clone path is your branch (`/app/pipelines-<your-branch>/`), not
   `/app/pipelines-main/`.

**Up to date is not a bug.** If the backend coverage is already current, the
`check_update` ends without dispatching anything — expected. Forcing a full test
means rewinding the coverage/`Update` in the backend, which edits real data: get
explicit approval first. Remember that a dev trigger that *does* find new data
**commits the source `Update`** (prod backend), so the real first run will then see
"nothing new" until the next release.

### 3. Reading the result — green does not mean it ingested

The poll guard returns early and Prefect still reports **`COMPLETED`**. A dead pipeline
is indistinguishable from a working one by state alone; `br_ibge_ipca` sat at 4 ingests
in 60 completed runs unnoticed. To tell them apart, check whether `extract_and_load`
runs were ever dispatched, read the logs (`Não há novas atualizações na fonte original`
= polled, did nothing) or check whether the coverage/Update records moved.

Use the `mcp__databasis__*` Prefect tools — `list_flow_runs(flow_name=…)`,
`get_flow_run_logs(<run_id>)`, `get_failed_flow_runs()`. They speak the Prefect 3 REST
API; anything pointing at `prefect.basedosdados.org` is the retired Prefect 2 host and
returns nothing. Search by tag `staged-pipeline` / `<dataset>__<table>` to find a
dataset's three stages.

### 4. What is NOT verifiable before arming

The prod promotion (`transfer_files_to_prod_flow` inside `build_and_promote`) needs
the prod pool's rights and runs for the first time on the first armed run. So does
`apply_row_access_policies` — but **only if a table is `part_bdpro`**; an all-free
pipeline never issues them, so there is nothing unexercised on that front. Say which
of the two applies, plainly, rather than implying the pipeline is proven end-to-end.

### Common problems

| Symptom | Cause / fix |
|---|---|
| "`get_latest_update` triggers nothing" | It only reports a date; `poll_source_for_update_task` decides. Compare the returned date to the **backend** (`Coverage.DateTimeRange` / `Table.Update.latest`), not to your code |
| Data uploaded twice or never | `extract_load_data` calls `upload_to_gcs` — remove it |
| Wrong/whole partition promoted | `data_path` isn't the final Hive layout, or holds more than this run's partitions |
| `FileNotFoundError` promoting a parquet dataset | Missing `source_format="parquet"` in `ExtractAndLoad` |
| Old flow still runs beside the new one | Monolith still registered; remove manually after confirming the new one |
| `poll_source_for_update` silently no-ops | Table has no `RawDataSource` registered |
| New table missing in `basedosdados-dev` | `bd.Table.create()` only registers the staging pointer; create the dev table by hand before the first `dbt run` there |

## Commit discipline

`feat(<dataset_id>): add recurring Prefect pipeline` (or
`feat(<dataset_id>): migrate to staged pipeline` when replacing a monolith). Never
commit `input/` or `output/` (data). Pre-commit runs ruff/format — pre-format new
`.py` with `uv run pre-commit run --files ...` before committing to avoid the hook
re-write loop.

## Legacy: monolithic flow

**Not every dataset has been migrated** (as of 2026-10-21, roughly half of
`pipelines/datasets/*/flows.py` still lack `stage_dispatch`). Flows you find without
`CheckThenExtractLoadPipeline` are the old shape — keep them working, but build new
pipelines and migrate touched ones to the staged structure above.

The monolith (`pipelines/shared/ibge_inflacao/flows.py::_run_ibge_inflacao`, old
`us_bls_cpi`) did everything in **one flow, one pod**, with `materialize_to_prod`,
`update_metadata` and `force_run` parameters:

```
rename_flow_run_dataset_table(prefix="Dump: ", dataset_id, table_id)
download → clean
max_date = <latest period in source>
has_new = poll_source_for_update_task(...)
if not has_new and not force_run: return
for table in tables:  upload_to_gcs(bucket="basedosdados-dev") ; run_dbt(command="run", target="dev")
for table in tables:  run_dbt(command="test", target="dev")
if not materialize_to_prod: return
for table in tables:  upload_to_gcs(bucket="basedosdados") ; run_dbt(command="run", target="prod")
for table in tables:  run_dbt(command="test", target="prod")
if update_metadata:
    register_table_materialization_task(...)
    commit_source_update_task(...)   # ONLY at the very end
```

Rules that still apply to any monolith you touch:

- **Run every table, then test every table — never interleave `run`/`test` per
  table.** Cross-table tests (`relationships`, `dbt_utils` referential checks,
  `custom_dictionary_coverage` with `ref('..._dicionario')`) read *sibling* models;
  interleaved, the first table's test runs before its sibling is built and fails
  (`Not found: Table ... au_ato_abr.dicionario`). Silent in a re-run where a stale
  sibling survives (dev), it only bites in a clean environment (fresh prod). Don't use
  the combined `"run/test"` inside a per-table loop. (The generic `build_and_promote`
  is per-table and uses `"run/test"`; datasets whose tables test each other need
  that checked when migrating.) → `project_au_ato_abr`
- The commit of the source `Update` must run **last**, only after prod succeeded.
- The dev run must be triggered with
  `{"materialize_to_prod": False, "update_metadata": False, "force_run": True}` —
  the defaults (`True`/`True`) write prod data, prod metadata and the paywall from the
  dev pool, because the metadata tasks are pinned `env="prod"`; `force_run=True` bypasses
  the poll guard.
- Cron: single schedule on the monolithic flow; single `memory`/`memory_limit` for
  all stages (the reason the staged split exists).
