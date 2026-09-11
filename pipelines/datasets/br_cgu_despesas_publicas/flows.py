"""Flows for br_cgu_despesas_publicas — Prefect 3.

Execução mensal da despesa do Governo Federal (Portal da Transparência, CGU):
empenhado, liquidado, pago e movimentação de restos a pagar, por órgão, unidade
gestora, unidade orçamentária, função, programa, ação, plano orçamentário,
localizador e natureza da despesa, um arquivo por mês desde 2014-01.

**Why this refreshes a trailing window rather than rebuilding everything.** The
source restates closed months, but each month is restated on its own schedule
and the full history is ~150 files and ~10 GB — a 25-minute download. So a
scheduled run re-pulls only the last ``months_back`` months and uploads with
``dump_mode="append"``, which replaces exactly the ``ano=/mes=`` partitions it
wrote and leaves the rest intact. ``dump_mode="overwrite"`` would be wrong here:
combined with a trailing window it would rebuild the table from the window alone
and destroy the history. Restatements older than the window are picked up by a
``full_refresh=True`` run.

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers
``br_cgu_despesas_publicas_flow``; the dev pool ignores the schedule, the prod
pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.br_cgu_despesas_publicas.constants import constants
from pipelines.datasets.br_cgu_despesas_publicas.tasks import (
    clean_despesas,
    download_despesas,
    probe_source,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    FreeLag,
    PartBdpro,
    YearMonth,
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
TABLE_ID = constants.TABLE_ID.value

# Monthly table, so the BD Pro rolling window applies: the most recent six
# months are pro-only and everything older is free. register_table_materialization_task
# recomputes free_end on every run and re-issues the BigQuery Row Access
# Policies, so the window slides on its own.
#
# This requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written.
_COVERAGE = PartBdpro(
    date_column=YearMonth(year="ano", month="mes"),
    date_format=DateFormat.YEAR_MONTH,
    free_lag=FreeLag(unit="months", value=6),
)


@flow(name="br_cgu_despesas_publicas", log_prints=True)
def br_cgu_despesas_publicas_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    months_back: int = 6,
    full_refresh: bool = False,
) -> None:
    """Refresh br_cgu_despesas_publicas.execucao from the Portal da Transparência.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the portal has not regenerated the
            current month since the last refresh.
        months_back: Size of the trailing window of months to re-pull.
        full_refresh: Re-pull every month from 2014-01. Slow (~25 min of paced
            downloading) — use when the source has restated older months.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=TABLE_ID
    )

    work_dir = tempfile.mkdtemp(prefix="br_cgu_despesas_publicas_")
    try:
        # One HEAD before spending the download budget. The CGU host CAPTCHA-
        # blocks bursts, so the run asks the cheapest question first.
        max_modified = probe_source()

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            source_max_date=max_modified,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="table_update",
        )
        if not has_new_data and not force_run:
            return

        downloaded = download_despesas(
            work_dir=work_dir,
            months_back=months_back,
            full_refresh=full_refresh,
        )
        result = clean_despesas(
            work_dir=work_dir,
            input_dir=downloaded["input_dir"],
            months=downloaded["months"],
        )
        print(
            f"cleaned {len(result['rows'])} months, {result['total']:,} rows "
            f"(through {result['max_period']})"
        )

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            source_max_date=max_modified,
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        if not materialize_to_prod:
            upload_to_gcs(
                data_path=result["path"],
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                bucket_name="basedosdados-dev",
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                dbt_command="run",
                target="dev",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                dbt_command="test",
                target="dev",
            )
            return

        upload_to_gcs(
            data_path=result["path"],
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            bucket_name="basedosdados",
            dump_mode="append",
            source_format="parquet",
        )
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            dbt_command="run",
            target="prod",
        )
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            dbt_command="test",
            target="prod",
        )

        if update_metadata:
            register_table_materialization_task(
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                coverage=_COVERAGE,
                env="prod",
                bq_project="basedosdados",
            )
    finally:
        # Covers both early returns and any exception. The scratch directory
        # holds several GB of raw CSV.
        shutil.rmtree(work_dir, ignore_errors=True)


# CGU refreshes the monthly execution files a few days into each month. Poll
# across a few early-month days at 06:47 BRT; the source-poll guard no-ops a run
# that finds nothing new.
# pyrefly: ignore [missing-attribute]
br_cgu_despesas_publicas_flow.deploy_schedules = [
    {"cron": "47 6 4,6,8,10 * *", "timezone": "America/Sao_Paulo"}
]
# The clean step holds one month (~60k rows x 46 string columns) at a time, but
# the raw CSVs for a six-month window are ~400 MB on disk. `memory` alone is
# silently dropped by the work pool template — the effective key is `memory_limit`.
# pyrefly: ignore [missing-attribute]
br_cgu_despesas_publicas_flow.job_variables = {
    "memory_limit": "6Gi",
    "memory_request": "2Gi",
}
