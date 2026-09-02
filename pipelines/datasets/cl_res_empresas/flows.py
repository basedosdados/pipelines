"""
Flows for cl_res_empresas — Prefect 3.

Chile's Registro de Empresas y Sociedades (Ley 20.659). datos.gob.cl publishes
one CSV per year of incorporation and republishes the current year's file with a
later cut-off date, so every run is a **full rebuild** of all years, not an
incremental append. The whole source is ~243 MB, which makes a full re-fetch both
simplest and most robust.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `cl_res_empresas_flow`;
the dev pool ignores the schedule, the prod pool activates it (paused).
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.cl_res_empresas.constants import constants
from pipelines.datasets.cl_res_empresas.tasks import clean_res, download_res
from pipelines.utils.metadata.domain import AllFree, DateFormat, YearMonth
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

# Build `dicionario` before `sociedad`: the sociedad model's
# custom_dictionary_coverage test reads the dicionario model, so the sibling has
# to exist first. Tests run in a second pass over the same list, after every
# table is built — never interleaved per table.
TABLES = ["dicionario", "sociedad"]

# The source data is CC BY and fully public, so the whole series stays free.
# `dicionario` has no date column and takes no coverage spec at all.
#
# If this table is ever moved behind the BD Pro rolling window, swapping this
# for PartBdpro is NOT sufficient on its own: a pro Coverage (is_closed=True)
# must already exist on the table in the backend, or assert_coverage_topology
# raises before anything is written.
_COVERAGE = {
    "sociedad": AllFree(
        date_column=YearMonth(year="ano", month="mes"),
        date_format=DateFormat.YEAR_MONTH,
    ),
}


@flow(name="cl_res_empresas", log_prints=True)
def cl_res_empresas_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Re-fetch the RES incorporations, rebuild both tables, materialize them.

    datos.gob.cl republishes the current year's file with a later cut-off date,
    so each run rebuilds every year from scratch. The source poll short-circuits
    the run when no newer month has been published, which makes a scheduled run a
    cheap no-op between releases.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new period.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="sociedad"
    )

    work_dir = tempfile.mkdtemp(prefix="cl_res_empresas_")
    try:
        input_dir = download_res(work_dir=work_dir)
        result = clean_res(work_dir=work_dir, input_dir=input_dir)
        max_ym = result["max_year_month"]

        # Skip the run when the source has published no newer period. Only
        # `sociedad` carries the raw data source link, so it is the poll anchor.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="sociedad",
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="sociedad",
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        # dump_mode is "append", never "overwrite": "overwrite" calls
        # tb.delete(mode="all"), which drops the materialized PROD table even
        # from the dev half of the run. "append" ends in an
        # st.upload(if_exists="replace") that replaces each blob wholesale —
        # identical semantics here, since every partition is rewritten each run.

        # The dev materialization is the pre-arm validation path, not part of a
        # production run: it rebuilds and re-tests both tables in
        # basedosdados-dev, which nothing downstream reads.
        if not materialize_to_prod:
            for table in TABLES:
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
            for table in TABLES:
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target="dev",
                )
            return

        for table in TABLES:
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
        for table in TABLES:
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
        # Covers both early returns (no new data, dev-only) and any exception.
        # The k8s work pool gives each run a fresh pod, but a process worker
        # reuses its filesystem — the download is ~243 MB.
        shutil.rmtree(work_dir, ignore_errors=True)


# The current year's file is refreshed periodically (the July 2026 cut appeared
# on 21 Aug 2026) and the previous year is closed off each January. Poll a few
# days a month at 17:17 BRT — a free slot; hour 16 is crowded. The source-poll
# guard no-ops until a newer period actually appears.
# pyrefly: ignore [missing-attribute]
cl_res_empresas_flow.deploy_schedules = [
    {"cron": "17 17 8,15,22 * *", "timezone": "America/Sao_Paulo"}
]
# The clean step holds ~1.6M rows in pandas one year at a time; 4Gi is ample.
# pyrefly: ignore [missing-attribute]
cl_res_empresas_flow.job_variables = {"memory": "4Gi"}
