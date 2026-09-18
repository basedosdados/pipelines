"""Flows for us_epu_gpr — Prefect 3.

Economic Policy Uncertainty (EPU) and Geopolitical Risk (GPR) newspaper-based
indices. Both families revise their history whenever the underlying article
counts are recomputed, so each run is a **full replace** (dump_mode="overwrite"),
never an incremental append. A single flow downloads every source, rebuilds all
three tables, and materializes them. The source poll (on the monthly table)
short-circuits the run until a new month is published, so a scheduled run is a
cheap no-op between releases.

All three tables are AllFree: the underlying data is freely licensed (EPU is
CC BY 4.0; GPR is free to use with citation) and trivially re-downloadable from
the source, so no BD Pro rolling window is applied. To switch the monthly table
to a rolling paywall later, create a pro Coverage (is_closed=True) on it and
change its spec to PartBdpro(free_lag=...).

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_epu_gpr_flow`; the
dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_epu_gpr.constants import constants
from pipelines.datasets.us_epu_gpr.tasks import clean_epu_gpr, download_epu_gpr
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
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

# Coverage spec per table. Both data tables are fully free (see module docstring);
# `dicionario` has no date column and takes no spec.
_COVERAGE = {
    "index_monthly": AllFree(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
    ),
    "index_daily": AllFree(
        date_column=DateOnly(col="date"),
        date_format=DateFormat.YEAR_MD,
    ),
}


@flow(name="us_epu_gpr", log_prints=True)
def us_epu_gpr_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the EPU and GPR sources, rebuild all three tables, materialize them.

    Both families ship their full (revised) history on every release, so each run
    is a full replace (``dump_mode="overwrite"``) rather than an incremental
    append. The source poll short-circuits the run when no newer month has been
    published, which makes a scheduled run a cheap no-op between releases.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage. Has no effect when ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new month.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="index_monthly"
    )

    work_dir = tempfile.mkdtemp(prefix="us_epu_gpr_")
    try:
        input_dir = download_epu_gpr(work_dir=work_dir)
        result = clean_epu_gpr(work_dir=work_dir, input_dir=input_dir)
        max_ym = result["max_year_month"]

        # Skip the run when no newer month has been published (unless forced).
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="index_monthly",
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="index_monthly",
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        tables = constants.ALL_TABLES.value

        # Dev materialization is the pre-arm validation path only; nothing
        # downstream reads basedosdados-dev, and prod runs the same models and
        # tests seconds later.
        if not materialize_to_prod:
            for table in tables:
                upload_to_gcs(
                    data_path=result[table],
                    dataset_id=DATASET_ID,
                    table_id=table,
                    bucket_name="basedosdados-dev",
                    dump_mode="overwrite",
                    source_format="parquet",
                )
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="run/test",
                    target="dev",
                )
            return

        for table in tables:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados",
                dump_mode="overwrite",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run/test",
                target="prod",
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
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# EPU and GPR both publish monthly at the start of the month (GPR ~1st, EPU by
# ~8th). Poll across early-month days at 16:33 BRT; the source-poll guard no-ops
# until a new month appears.
# pyrefly: ignore [missing-attribute]
us_epu_gpr_flow.deploy_schedules = [
    {"cron": "33 16 4,5,6,7,8,9,10 * *", "timezone": "America/Sao_Paulo"}
]
