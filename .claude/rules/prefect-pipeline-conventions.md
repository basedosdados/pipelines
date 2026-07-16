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
- Dev pool (`--pool basedosdados-dev`, on PR): schedules **stripped** — manual runs only.
- Prod pool (`--pool basedosdados --all`, on merge to main): schedules become
  `Cron` objects; deployed **`paused=True`**, activated by backend sync.
- Cron in `America/Sao_Paulo`; see crontab.guru. For a monthly source, poll across
  a few release-window days — the source-poll guard no-ops until a new period lands.

## Local verification (what you CAN check without infra)

1. **Imports**: `uv run python -c "from pipelines.datasets.<ds> import flows; print(flows.<flow>.name)"`.
2. **Transform parity**: run `clean_all` on the existing `input/` and assert the
   same row counts as the onboarding validation.
3. **Download**: fetch one small dimension file via `requests` + the real headers.
4. **Deploy discovery**: run `deploy_flows.load_flows_from_file("pipelines/datasets/<ds>/flows.py")`
   and confirm the flow name appears.
5. The `upload_to_gcs`/`run_dbt`/metadata halves run on the worker — do not expect
   them to pass locally; state that plainly.

## Commit discipline

`feat(<dataset_id>): add recurring Prefect pipeline`. Never commit `input/` or
`output/` (data). Pre-commit runs ruff/format — pre-format new `.py` with
`uv run pre-commit run --files ...` before committing to avoid the hook re-write loop.
