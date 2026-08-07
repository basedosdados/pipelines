"""
Flows for us_bls_cpi — Prefect 3.

US Consumer Price Index (BLS), CPI-U + CPI-W. The BLS flat files carry the full
history every month, so each run is a **full replace** (dump_mode="overwrite"),
not an incremental append. A single flow downloads once and rebuilds all four
tables. Schedule targets the BLS monthly release window (~2nd week).

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_bls_cpi_flow`; the
dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_bls_cpi.constants import constants
from pipelines.datasets.us_bls_cpi.tasks import clean_cpi, download_cpi
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

# Coverage spec per table.
#
# `monthly` is the high-frequency table, so it carries the BD Pro rolling
# window: the most recent 6 months are pro-only, everything older is free.
# Each run recomputes free_end = source_end - free_lag, rewrites both
# DateTimeRanges, and re-issues the BigQuery Row Access Policies, so the
# window slides forward on its own.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro
# (is_closed=True) Coverage to already exist on the table, or
# assert_coverage_topology raises before anything is written.
#
# annual and semiannual are lower-frequency and stay fully free.
# `dicionario` has no date column, so it takes no coverage spec at all.
_COVERAGE = {
    "monthly": PartBdpro(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "annual": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "semiannual": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
}


@flow(name="us_bls_cpi", log_prints=True)
def us_bls_cpi_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the BLS CPI flat files, rebuild all four tables, materialize them.

    BLS ships the full history on every release, so each run is a full replace
    (``dump_mode="overwrite"``) rather than an incremental append. The source poll
    short-circuits the run when BLS has not published a new month, which makes a
    scheduled run a cheap no-op between releases.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new month.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="cpi"
    )

    work_dir = tempfile.mkdtemp(prefix="us_bls_cpi_")
    try:
        input_dir = download_cpi(work_dir=work_dir)
        result = clean_cpi(work_dir=work_dir, input_dir=input_dir)
        max_ym = result["max_year_month"]

        # Skip the run when BLS has not published a newer month (unless forced).
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="monthly",
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
        )
        if not has_new_data and not force_run:
            return

        tables = constants.ALL_TABLES.value

        # Dev: upload staging + materialize/test.
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

        if not materialize_to_prod:
            return

        # Prod: upload staging + materialize/test.
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
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id="monthly",
                source_max_date=max_ym,
                env="prod",
                date_format="%Y-%m",
            )
    finally:
        # Covers both early returns (no new data, dev-only) and any exception.
        # The k8s work pool gives each run a fresh pod, but a process/local
        # worker reuses its filesystem — the download is several hundred MB.
        shutil.rmtree(work_dir, ignore_errors=True)


# BLS releases CPI monthly, ~2nd week, on a US business day. Poll across a few
# mid-month days at 16:00 BRT; the source-poll guard no-ops until a new month
# actually appears.
# pyrefly: ignore [missing-attribute]
us_bls_cpi_flow.deploy_schedules = [
    {"cron": "0 16 10,11,12,13,14,15 * *", "timezone": "America/Sao_Paulo"}
]
# The clean step holds ~4M rows in pandas; give the worker headroom.
# pyrefly: ignore [missing-attribute]
us_bls_cpi_flow.job_variables = {"memory": "8Gi"}
