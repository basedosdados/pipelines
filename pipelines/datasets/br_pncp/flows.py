"""Flows for br_pncp — Prefect 3.

Portal Nacional de Contratações Públicas: procurement across federal, state and
municipal government. Unlike a statistical release that republishes its full
history each period, PNCP is a continuously-appended register, so each run
harvests a trailing window from the API's *update-date* endpoints and appends it
to staging.

That makes the run idempotent without any bookkeeping: overlapping windows
re-deliver records the previous run already loaded, and the dbt models collapse
them on the PNCP control number, keeping the row with the latest
``data_atualizacao``. Re-running a window is therefore always safe.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers ``br_pncp_flow``; the
dev pool ignores the schedule, the prod pool activates it (paused until armed).
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.br_pncp.constants import constants
from pipelines.datasets.br_pncp.tasks import (
    build_dicionario_task,
    clean_window,
    harvest_window,
    max_publication_date,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
)
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATASET_ID = constants.DATASET_ID.value

# Coverage spec per table.
#
# PNCP refreshes daily, so the house rule puts the recent window behind BD Pro:
# the most recent 6 months are pro-only, everything older is free. Each run
# recomputes free_end = source_end - free_lag, rewrites both DateTimeRanges and
# re-issues the BigQuery Row Access Policies, so the window slides on its own.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written. Those must be created before the flow is armed.
#
# The window keys on the publication date rather than the `ano` partition: a
# 6-month lag is meaningless against a year-granular column.
#
# plano_contratacao_anual is forward-looking planning data published a year
# ahead, so paywalling its recent window would paywall essentially all of it —
# it stays free. dicionario has no date column and takes no spec at all.
_PART_BDPRO = PartBdpro(
    date_column=DateOnly(col="data_publicacao"),
    date_format=DateFormat.YEAR_MD,
    free_lag=FreeLag(unit="months", value=6),
)

_COVERAGE = {
    "contratacao": _PART_BDPRO,
    "contrato": _PART_BDPRO,
    "ata_registro_preco": _PART_BDPRO,
    "instrumento_cobranca": _PART_BDPRO,
    "plano_contratacao_anual": AllFree(
        date_column=DateOnly(col="data_publicacao"),
        date_format=DateFormat.YEAR_MD,
    ),
}


@flow(name="br_pncp", log_prints=True)
def br_pncp_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    lookback_days: int = constants.LOOKBACK_DAYS.value,
) -> None:
    """Harvest recent PNCP updates, append them to staging, materialize the tables.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports nothing new.
        lookback_days: How far back to re-harvest. Wider than the schedule
            interval on purpose, because PNCP backdates amendments.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="contratacao"
    )

    work_dir = tempfile.mkdtemp(prefix="br_pncp_")
    try:
        summaries = {}
        for table in constants.FACT_TABLES.value:
            input_dir = harvest_window(
                work_dir=work_dir, table=table, lookback_days=lookback_days
            )
            summaries[table] = clean_window(
                work_dir=work_dir, input_dir=input_dir, table=table
            )

        harvested = sum(s["deduped_rows"] for s in summaries.values())
        if harvested == 0 and not force_run:
            print(
                "Nenhum registro novo ou atualizado na fonte original",
                flush=True,
            )
            return

        summaries["dicionario"] = build_dicionario_task(work_dir=work_dir)

        source_max_date = max_publication_date(
            summaries=[summaries[t] for t in constants.FACT_TABLES.value]
        )

        # PNCP has no release calendar — it is a register, not a periodical — so
        # the poll compares publication-year coverage rather than a release
        # period. It guards against re-materializing when a run harvested only
        # amendments to years already covered.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="contratacao",
            source_max_date=source_max_date,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

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
        tables = constants.ALL_TABLES.value

        if not materialize_to_prod:
            for table in tables:
                upload_to_gcs(
                    data_path=summaries[table]["data_path"],
                    dataset_id=DATASET_ID,
                    table_id=table,
                    bucket_name="basedosdados-dev",
                    dump_mode="append",
                    source_format="parquet",
                )
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="run",
                    target="dev",
                    _vars=dbt_vars,
                )
            # Test only after every table is built: the relationship and
            # dictionary-coverage tests read sibling models, so interleaving
            # run/test per table fails on a clean environment.
            for table in tables:
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target="dev",
                    _vars=dbt_vars,
                )
            return

        for table in tables:
            upload_to_gcs(
                data_path=summaries[table]["data_path"],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados",
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target="prod",
                _vars=dbt_vars,
            )
        for table in tables:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target="prod",
                _vars=dbt_vars,
            )

        if update_metadata:
            for table, coverage in _COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
            # Last, and only after prod succeeded.
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id="contratacao",
                source_max_date=source_max_date,
                env="prod",
                date_format="%Y-%m-%d",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )
    finally:
        # Covers both early returns and any exception. The k8s work pool gives
        # each run a fresh pod, but a process worker reuses its filesystem.
        shutil.rmtree(work_dir, ignore_errors=True)


# PNCP publishes continuously, so a daily run at a minute nobody else is using.
# 04:12 BRT clears the overnight backlog before the working day.
# pyrefly: ignore [missing-attribute]
br_pncp_flow.deploy_schedules = [
    {"cron": "12 4 * * *", "timezone": "America/Sao_Paulo"}
]
# The clean step holds a full lookback window of contratações in memory.
# pyrefly: ignore [missing-attribute]
br_pncp_flow.job_variables = {"memory": "8Gi"}
