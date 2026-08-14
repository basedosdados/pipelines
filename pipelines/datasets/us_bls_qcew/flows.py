"""
Flows for us_bls_qcew — Prefect 3.

US Quarterly Census of Employment and Wages (BLS QCEW). The recurring pipeline
refreshes only the 8 **NAICS** tables ({quarterly, annual} x {national, state,
county, metro}); the 8 SIC tables are a frozen classification (1975-2000, never
republished) and the dicionario is static, so neither is in the pipeline's loop.

QCEW publishes a new quarter roughly one quarter after it ends and revises prior
quarters on every release. Each run therefore re-cleans the entire NAICS history
and does a **full replace** (dump_mode="overwrite"), not an incremental append.
The source poll (on the newest quarter) short-circuits a scheduled run until BLS
actually publishes a newer period, making the between-release runs cheap no-ops.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_bls_qcew_flow`; the
dev pool ignores the schedule, the prod pool activates it (paused until armed).
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_bls_qcew.constants import constants
from pipelines.datasets.us_bls_qcew.tasks import (
    clean_qcew,
    latest_source_period,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    FreeLag,
    PartBdpro,
    YearOnly,
    YearQuarter,
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
NAICS_TABLES = constants.NAICS_TABLES.value
POLL_TABLE = constants.POLL_TABLE.value

# Coverage spec per NAICS table.
#
# The four `naics_quarterly_*` tables are refreshed quarterly, so they carry the
# BD Pro rolling window: the most recent 6 months (the latest two closed
# quarters) are pro-only, everything older is free. Each run recomputes
# free_end = source_end - free_lag, rewrites both DateTimeRanges, and re-issues
# the BigQuery Row Access Policies, so the window slides forward on its own.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written — those were created at onboarding.
#
# The four `naics_annual_*` tables are lower-frequency and stay fully free.
_QUARTERLY = PartBdpro(
    date_column=YearQuarter(year="year", quarter="qtr"),
    date_format=DateFormat.YEAR_MONTH,
    free_lag=FreeLag(unit="months", value=6),
)
_ANNUAL = AllFree(
    date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
)
_COVERAGE = {
    table: (_QUARTERLY if "quarterly" in table else _ANNUAL)
    for table in NAICS_TABLES
}


@flow(name="us_bls_qcew", log_prints=True)
def us_bls_qcew_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh the 8 NAICS QCEW tables from the full published history.

    QCEW ships each year as a per-year singlefile and revises prior quarters on
    every release; ``upload_to_gcs`` overwrites the whole staging table, so each
    run re-cleans the entire NAICS history rather than a single partition. The
    source poll short-circuits the run when BLS has not published a newer
    quarter, which makes a scheduled run a cheap no-op between releases.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage (rolling the BD Pro window on the quarterly tables) and
            commit the source update. Has no effect when ``materialize_to_prod``
            is False.
        force_run: Materialize even when the source poll reports no new quarter.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="naics"
    )

    work_dir = tempfile.mkdtemp(prefix="us_bls_qcew_")
    try:
        # Cheap poll first: probe BLS and read only the newest quarterly file.
        max_ym = latest_source_period(work_dir=work_dir)

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
        # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
        # novo publicado, mesmo que a tabela não tenha sido atualizada.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        # Expensive: re-clean the full NAICS history into partitioned parquet.
        paths = clean_qcew(work_dir=work_dir)

        # Dev: upload staging + materialize/test.
        for table in NAICS_TABLES:
            upload_to_gcs(
                data_path=paths[table],
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
        for table in NAICS_TABLES:
            upload_to_gcs(
                data_path=paths[table],
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
            for table in NAICS_TABLES:
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=_COVERAGE[table],
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers both early returns (no new data, dev-only) and any exception.
        # The k8s work pool gives each run a fresh pod, but a process/local
        # worker reuses its filesystem — the full NAICS download is tens of GB.
        shutil.rmtree(work_dir, ignore_errors=True)


# QCEW releases a quarter roughly one quarter after it ends: County Employment
# and Wages news releases land in early March, June, September, and December.
# Poll across the first ~10 days of those months at 16:00 BRT; the source-poll
# guard no-ops until a new quarter actually appears in the singlefiles.
# pyrefly: ignore [missing-attribute]
us_bls_qcew_flow.deploy_schedules = [
    {"cron": "0 16 1-10 3,6,9,12 *", "timezone": "America/Sao_Paulo"}
]
# The clean step streams ~15M-row singlefiles one chunk at a time (peak ~1.75GB
# in pandas); give the worker headroom above that.
# pyrefly: ignore [missing-attribute]
us_bls_qcew_flow.job_variables = {"memory": "8Gi"}
