"""Flows for br_mgi_pncp — Prefect 3.

Portal Nacional de Contratações Públicas: procurement across federal, state and
municipal government. Unlike a statistical release that republishes its full
history each period, PNCP is a continuously-appended register, so each run
harvests a trailing window from the API's *update-date* endpoints and appends it
to staging.

That makes the run idempotent without any bookkeeping: overlapping windows
re-deliver records the previous run already loaded, and the dbt models collapse
them on the PNCP control number, keeping the row with the latest
``data_atualizacao``. Re-running a window is therefore always safe.

Migrated to check_update -> extract_and_load, SEM `build_and_promote`: unlike the
standard shape of a `pipeline_factory`/`CheckThenExtractLoadPipeline` (see
`pipelines/utils/stage_dispatch.py`), this dataset keeps ONE check_update/
extract_and_load pair for the whole 4-table group, not a pair per table —
same class of exception as `br_bcb_ifdata`/`au_rba_statistical_tables`:

1. Mandatory dbt order: `custom_dictionary_coverage` on `contratacao`,
   `contrato` and `instrumento_cobranca` references
   `ref('br_mgi_pncp__dicionario')` (see `models/br_mgi_pncp/schema.yml`), and
   `dicionario` is itself derived from the fact tables (see
   `utils.DICIONARIO_DERIVED`). The four fact tables must be `run` before
   ANY table is `test`ed, and `dicionario` must be `run` last (see `_materialize`
   below). `dispatch_build_and_promote` dispatches one `build_and_promote` PER
   TABLE, each async and independent — exactly the race this ordering must
   prevent. The fix is the same as the two precedents: `extract_and_load` calls
   `_materialize` directly, uploading and `run`ning every table before `test`ing
   any of them.
2. A single freshness signal: PNCP has no release calendar, so "is there
   anything new" is answered once, across every fact table combined
   (`max_publication_date`), not per table — splitting the poll per table would
   require five independent judgements about one and the same harvest.

check_update harvests the FULL trailing window across all four fact tables —
the same work extract_and_load repeats, independently, to actually load it —
because PNCP exposes no freshness signal cheaper than the harvest itself.
Duplicated cost accepted, same trade-off as `au_rba_statistical_tables`.

One behavioral change from the old monolithic flow this replaces:
`commit_source_update_task` now fires inside `check_update_and_dispatch`,
immediately after the poll confirms new data and BEFORE extract_and_load even
starts (rather than after prod materialization succeeded, as the old single
flow did deliberately — "Last, and only after prod succeeded"). This matches
`commit_source_update_task`'s own documented contract (write the source Update
right after the poll, not conditional on materialization) and is exactly what
every other `check_update_and_dispatch` consumer in this repo already does,
`br_bcb_ifdata`/`au_rba_statistical_tables` included — it is the standard
staged-pipeline contract, not a regression: a mid-run failure now leaves
`RawDataSource.Update` already advanced, same as any other migrated dataset.

The old `force_run` flag is gone too, replaced the same way `au_rba_statistical_tables`
replaced it: trigger `br_mgi_pncp_extract_and_load` directly (with an explicit
`download_params`) to rematerialize on demand, bypassing the poll entirely.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers both flows below;
the dev pool ignores the check_update schedule, the prod pool activates it (deployed
paused until armed).
"""

import shutil
import tempfile
from datetime import date

from prefect.schedules import Cron

from pipelines.datasets.br_mgi_pncp.constants import constants
from pipelines.datasets.br_mgi_pncp.tasks import (
    clean_window,
    harvest_window,
    max_publication_date,
)
from pipelines.utils.flow import flow
from pipelines.utils.metadata.tasks import register_table_materialization_task
from pipelines.utils.stage_dispatch import (
    Etapa,
    check_update_and_dispatch,
    deploy_tags,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATASET_ID = constants.DATASET_ID.value
POLL_TABLE = "contratacao"

# Name of the `br_mgi_pncp_extract_and_load` flow below (`deploy_flows.py`
# registers a deployment under the Python variable name of the `@flow`). Passed
# explicitly to `check_update_and_dispatch` because it's literally
# `str(Etapa.EXTRACT_AND_LOAD)` already — kept explicit so it doesn't depend on
# that coincidence.
_EXTRACT_AND_LOAD_DEPLOYMENT = "br_mgi_pncp_extract_and_load"


def _harvest_all(work_dir: str, lookback_days: int) -> dict:
    """Harvest and clean the trailing window for every fact table.

    Shared between check_update (which only needs the summaries to judge
    freshness) and extract_and_load (which re-runs this same work
    independently to actually load it — duplicated cost accepted, see the
    module banner).
    """
    summaries = {}
    for table in constants.FACT_TABLES.value:
        input_dir = harvest_window(
            work_dir=work_dir, table=table, lookback_days=lookback_days
        )
        summaries[table] = clean_window(
            work_dir=work_dir, input_dir=input_dir, table=table
        )
    return summaries


def _materialize(
    target: str, bucket: str, summaries: dict, dbt_vars: dict
) -> None:
    """Upload every fact table, `run` all 5 models, then `test` all 5.

    The separation between `run` and `test` is structural, not stylistic:
    `custom_dictionary_coverage` on `contratacao`, `contrato` and
    `instrumento_cobranca` references `ref('br_mgi_pncp__dicionario')` (see
    `models/br_mgi_pncp/schema.yml`). Testing table-by-table would run the
    first sibling's test before `dicionario` exists in a clean environment and
    fail with `Not found: Table ... br_mgi_pncp.dicionario`. In an already
    populated environment this passes unnoticed because the sibling survives
    from a previous build; it only breaks in a clean one, so the order has to
    be structural.

    Only the fact tables have data to upload — `dicionario` is a dbt model
    over them (see `models/br_mgi_pncp/code/gen_dbt.py`), so it is built and
    tested but never uploaded, and, being last in `ALL_TABLES`, it is built
    after the models it reads.
    """
    for table in constants.FACT_TABLES.value:
        upload_to_gcs(
            data_path=summaries[table]["data_path"],
            dataset_id=DATASET_ID,
            table_id=table,
            bucket_name=bucket,
            dump_mode="append",
            source_format="parquet",
        )
    for table in constants.ALL_TABLES.value:
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=table,
            dbt_command="run",
            target=target,
            _vars=dbt_vars,
        )
    for table in constants.ALL_TABLES.value:
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=table,
            dbt_command="test",
            target=target,
            _vars=dbt_vars,
        )


@flow(name=f"{Etapa.CHECK_UPDATE}: {DATASET_ID}", log_prints=True)
def br_mgi_pncp_check_update(
    lookback_days: int = constants.LOOKBACK_DAYS.value,
) -> bool:
    """Harvest the trailing window across all fact tables, poll, and dispatch.

    Harvests the FULL window (see `_harvest_all`) just to find the freshest
    publication date seen in it — the same work `extract_and_load` repeats
    independently to actually load the data. PNCP has no release calendar and
    no freshness signal cheaper than the harvest itself (see the module
    banner).

    Args:
        lookback_days: How far back to re-harvest. Wider than the schedule
            interval on purpose, because PNCP backdates amendments. Widen this
            on a manual trigger to recover from an outage longer than the
            default window (see `models/br_mgi_pncp/CLAUDE.md`).

    Returns:
        `True` if new data was found (and extract_and_load was dispatched);
        `False` otherwise.
    """
    rename_flow_run_dataset_table(
        prefix="Check Update: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="br_mgi_pncp_check_")
    try:
        summaries = _harvest_all(work_dir, lookback_days)

        harvested = sum(s["written_rows"] for s in summaries.values())
        if harvested == 0:
            print(
                "Nenhum registro novo ou atualizado na fonte original",
                flush=True,
            )
            return False

        # PNCP has no release calendar — it is a register, not a periodical —
        # so the poll (inside check_update_and_dispatch) compares
        # publication-year coverage rather than a release period. It guards
        # against dispatching extract_and_load when a run harvested only
        # amendments to years already covered.
        source_max_date = max_publication_date(
            summaries=[summaries[t] for t in constants.FACT_TABLES.value]
        )
        print(f"max publication_date na fonte: {source_max_date}", flush=True)

        return check_update_and_dispatch(
            prefect_dataset_id=DATASET_ID,
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            reference_date=date.fromisoformat(source_max_date),
            next_deployment=_EXTRACT_AND_LOAD_DEPLOYMENT,
            date_format="%Y-%m-%d",
            extra_download_params={"lookback_days": lookback_days},
            compare_against="coverage",
        )
    finally:
        # Covers both early returns and any exception. The k8s work pool gives
        # each run a fresh pod, but a process worker reuses its filesystem.
        shutil.rmtree(work_dir, ignore_errors=True)


br_mgi_pncp_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)
# PNCP publishes continuously, so a daily run at a minute nobody else is using.
# 04:12 BRT clears the overnight backlog before the working day.
br_mgi_pncp_check_update.deploy_schedules = [
    Cron("12 4 * * *", timezone="America/Sao_Paulo")
]
# Holds a full lookback window of contratações in memory, same as
# extract_and_load below — `memory` alone is silently dropped: it is not a
# variable of the work pool's job template, so the pod would get the 4Gi
# default while the deployment record still showed the 8Gi we asked for.
# `memory_limit` is the one the container actually gets.
br_mgi_pncp_check_update.job_variables = {
    "memory": "8Gi",
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}


@flow(name=f"{Etapa.EXTRACT_AND_LOAD}: {DATASET_ID}", log_prints=True)
def br_mgi_pncp_extract_and_load(
    download_params: dict,
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
) -> None:
    """Re-harvest the trailing window and materialize all 5 tables in batch.

    Args:
        download_params: dict received from check_update via `run_deployment()`.
            Carries `reference_date` for traceability only — the harvest
            re-derives its own window from `lookback_days` against today's
            date (see `tasks.harvest_window`), it does not look at
            `reference_date`. Also carries `lookback_days`, falling back to
            `constants.LOOKBACK_DAYS` when this flow is triggered manually
            without it.
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production. Triggering this flow manually with
            `materialize_to_prod=False` replaces the old monolithic flow's
            `force_run` parameter: since extract_and_load is now directly
            triggerable (independent of check_update's poll finding anything
            new), rematerializing on demand no longer needs a separate flag.
        update_metadata: After a successful prod materialization, register table
            coverage. Has no effect when ``materialize_to_prod`` is False.
    """
    rename_flow_run_dataset_table(
        prefix="Extract and Load: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    lookback_days = download_params.get(
        "lookback_days", constants.LOOKBACK_DAYS.value
    )

    work_dir = tempfile.mkdtemp(prefix="br_mgi_pncp_")
    try:
        summaries = _harvest_all(work_dir, lookback_days)

        # The dbt models are incremental with insert_overwrite on `ano`. Passing
        # the touched years scopes each incremental run to those partitions
        # instead of rebuilding the whole table; the model reads the *complete*
        # staging partition for each of them, so the overwrite never drops rows
        # this run did not happen to harvest.
        touched_years = sorted(
            {
                int(y)
                for s in summaries.values()
                for y in (s.get("years") or [])
            }
        )
        dbt_vars = {"pncp_years": ",".join(str(y) for y in touched_years)}

        _materialize("dev", "basedosdados-dev", summaries, dbt_vars)

        if not materialize_to_prod:
            return

        _materialize("prod", "basedosdados", summaries, dbt_vars)

        if update_metadata:
            for table in constants.FACT_TABLES.value:
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers both the dev-only early return and any exception. The k8s
        # work pool gives each run a fresh pod, but a process worker reuses
        # its filesystem.
        shutil.rmtree(work_dir, ignore_errors=True)


br_mgi_pncp_extract_and_load.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD
)
# The clean step holds a full lookback window of contratações in memory.
br_mgi_pncp_extract_and_load.job_variables = {
    "memory": "8Gi",
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
