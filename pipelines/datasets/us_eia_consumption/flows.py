"""Flows for us_eia_consumption — Prefect 3.

U.S. Energy Information Administration Form EIA-861 (annual retail electricity
sales, utility frame and service territory) and Form EIA-861M (monthly sales and
revenue by state and sector).

**EIA republishes a report year rather than extending it** — an EIA-861 year
appears as an early release and then one or more final revisions, and the 861M
file restates recent months — so each run rebuilds every affected year partition
from the newest files. Rebuilding makes supersession double-counts structurally
impossible and keeps the dicionario computed over the whole record.

The annual form and the monthly form are polled separately because they move on
different clocks. The three annual tables are fully free; eia861m refreshes
monthly and carries the BD Pro rolling window.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_eia_consumption_flow`.
"""

import shutil
import tempfile

from prefect import flow
from prefect.utilities.asyncutils import run_coro_as_sync

from pipelines.datasets.us_eia_consumption.constants import constants
from pipelines.datasets.us_eia_consumption.tasks import (
    clean_corpus,
    download_corpus,
    probe_source,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    FreeLag,
    PartBdpro,
    YearMonth,
    YearOnly,
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

# The table each source clock is polled through.
POLL_TABLE = {
    "eia861": constants.RETAIL_SALES.value,
    "eia861m": constants.EIA861M.value,
}

# Coverage per table. The annual tables are AllFree; eia861m refreshes monthly,
# so it carries the BD Pro rolling window (free_end = source_end - 6 months),
# which requires a free AND a pro Coverage to already exist (registered at
# onboarding with --bdpro). Its (year, month) are guaranteed non-null by the
# builder, so no row is paywalled forever by a NULL date. dicionario has no date
# column and takes no coverage spec.
_FREE_LAG = FreeLag(unit="months", value=6)
_COVERAGE = {
    constants.UTILITY.value: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    constants.RETAIL_SALES.value: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    constants.SERVICE_TERRITORY.value: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    constants.EIA861M.value: PartBdpro(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=_FREE_LAG,
    ),
}


@flow(name="us_eia_consumption", log_prints=True)
def us_eia_consumption_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh EIA-861 (annual) and EIA-861M (monthly) from the published files.

    Args:
        materialize_to_prod: Continue past the dev materialization to write prod
            and run dbt against ``target="prod"``. False exercises only the dev
            half — required for a safe test run.
        update_metadata: After a successful prod materialization, register table
            coverage and commit source updates. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when both polls report nothing new — needed
            for a revision that restates without adding a period.
    """
    run_coro_as_sync(
        rename_flow_run_dataset_table(
            prefix="Dump: ",
            dataset_id=DATASET_ID,
            table_id=constants.RETAIL_SALES.value,
        )
    )

    work_dir = tempfile.mkdtemp(prefix="us_eia_consumption_")
    try:
        probe = probe_source(work_dir=work_dir)
        print(
            f"annual report years {probe['years'][0]}-{probe['years'][-1]}; "
            f"max dates {probe['max_date']}"
        )

        has_new_data = False
        for form in ("eia861", "eia861m"):
            if poll_source_for_update_task(
                dataset_id=DATASET_ID,
                table_id=POLL_TABLE[form],
                source_max_date=probe["max_date"][form],
                env="prod",
                date_format="%Y-%m-%d",
                compare_against="coverage",
            ):
                has_new_data = True
        if not has_new_data and not force_run:
            print("Não há novas atualizações na fonte original")
            return

        input_dir = download_corpus(work_dir=work_dir, probe=probe)
        result = clean_corpus(work_dir=work_dir, input_dir=input_dir)
        print(f"row counts: {result['row_counts']}")

        for form in ("eia861", "eia861m"):
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id=POLL_TABLE[form],
                source_max_date=probe["max_date"][form],
                env="prod",
                date_format="%Y-%m-%d",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )

        tables = constants.ALL_TABLES.value

        # Build every table before testing any (custom_dictionary_coverage reads
        # dicionario via ref()).
        if not materialize_to_prod:
            for table in tables:
                upload_to_gcs(
                    data_path=result[table],
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
                )
            for table in tables:
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target="dev",
                )
            return

        for table in tables:
            upload_to_gcs(
                data_path=result[table],
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
            )
        for table in tables:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
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


# EIA-861M gains a month near the end of each month; EIA-861 gains a year in the
# autumn. Poll daily and let the source-poll guard no-op. :31 keeps clear of
# other crons in this repo (and of us_eia_electricity at :17 and us_eia_seds at
# :23).
# pyrefly: ignore [missing-attribute]
us_eia_consumption_flow.deploy_schedules = [
    {"cron": "31 9 * * *", "timezone": "America/Sao_Paulo"}
]
# The clean step parses one EIA-861 workbook at a time; 8Gi is comfortable.
# `memory` alone is dropped by the work pool — the pod honours `memory_limit`.
# pyrefly: ignore [missing-attribute]
us_eia_consumption_flow.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
