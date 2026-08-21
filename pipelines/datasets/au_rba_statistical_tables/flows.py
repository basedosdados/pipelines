"""
Flows for au_rba_statistical_tables — Prefect 3.

Reserve Bank of Australia statistical tables. Every CSV carries the full history
of its series, so each run is a **full replace** (``dump_mode="overwrite"``),
not an incremental append. A single flow downloads all ~220 CSVs once and
rebuilds all four tables.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers
`au_rba_statistical_tables_flow`; the dev pool ignores the schedule, the prod
pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.au_rba_statistical_tables.constants import constants
from pipelines.datasets.au_rba_statistical_tables.tasks import (
    clean_rba,
    download_rba,
)
from pipelines.utils.metadata.domain import AllFree, DateFormat, DateOnly
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
POLL_TABLE = constants.POLL_TABLE.value

# Coverage spec per table.
#
# Every table is AllFree — no BD Pro rolling window — even though the dataset
# refreshes daily. The RBA copyright notice (Section 4) lists, as prohibited
# "improper commercial exploitation", charging for the Cash Rate "without
# informing customers that [it is] published on this website without a fee being
# charged by the RBA". An all-free dataset cannot engage that condition at all.
# See models/au_rba_statistical_tables/LICENCE.md.
#
# `dicionario` has no date column, so it takes no coverage spec.
_COVERAGE = {
    "data": AllFree(
        date_column=DateOnly(col="date"), date_format=DateFormat.YEAR_MD
    ),
    "series": AllFree(
        date_column=DateOnly(col="observation_end"),
        date_format=DateFormat.YEAR_MD,
    ),
    "series_break": AllFree(
        date_column=DateOnly(col="date"), date_format=DateFormat.YEAR_MD
    ),
}


@flow(name="au_rba_statistical_tables", log_prints=True)
def au_rba_statistical_tables_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the RBA statistical tables, rebuild all four tables, materialize them.

    The RBA republishes the full history of each table on every release, so each
    run is a full replace (``dump_mode="overwrite"``) rather than an incremental
    append. The source poll short-circuits the run when the RBA has published
    nothing since the last refresh, which makes a scheduled run a cheap no-op on
    quiet days.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports nothing new.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="statistical_tables"
    )

    work_dir = tempfile.mkdtemp(prefix="au_rba_statistical_tables_")
    try:
        input_dir = download_rba(work_dir=work_dir)
        result = clean_rba(work_dir=work_dir, input_dir=input_dir)
        max_pub = result["max_publication_date"]

        # The freshness signal is the RBA's own "Publication date" stamp, the
        # max across all series — a publication timestamp, not a coverage
        # competência, so it is compared against Table.Update.latest rather than
        # the coverage range. Comparing against coverage would stall the poll:
        # the newest coverage date belongs to a forward-dated quarterly
        # expectations series (G3), months ahead of the daily data.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_pub,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="table_update",
        )
        if not has_new_data and not force_run:
            print(f"Nada novo na fonte (max publication_date = {max_pub}).")
            return

        # Commit the source Update before materializing: if the flow dies
        # mid-run, the source metadata still records that new data had been
        # published, even though the tables were not refreshed.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_pub,
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        tables = constants.ALL_TABLES.value

        # Dev: upload + build EVERY table first, then test them all.
        #
        # Never interleave run/test per table. `data` carries a composite
        # foreign-key test against `series`, and `series_break` a
        # dictionary-coverage test against `dicionario` — both read sibling
        # models. Interleaved, the first table's tests run before its sibling
        # exists and fail with "Not found: Table ...". This is silent in a
        # re-run where a stale sibling survives, and only bites in a clean
        # environment, so it has to be structural.
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

        if not materialize_to_prod:
            return

        # Prod: same two-pass shape.
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
        # Covers both early returns (no new data, dev-only) and any exception.
        # The k8s work pool gives each run a fresh pod, but a process/local
        # worker reuses its filesystem.
        shutil.rmtree(work_dir, ignore_errors=True)


# The RBA publishes on Sydney business days, typically late morning AEST — which
# is the small hours in São Paulo. Run daily at 09:00 BRT, comfortably after;
# the source-poll guard makes weekends and quiet days a no-op.
# pyrefly: ignore [missing-attribute]
au_rba_statistical_tables_flow.deploy_schedules = [
    {"cron": "40 9 * * *", "timezone": "America/Sao_Paulo"}
]
# The clean step holds ~1.5M parsed observations in memory before writing.
# pyrefly: ignore [missing-attribute]
au_rba_statistical_tables_flow.job_variables = {"memory": "4Gi"}
