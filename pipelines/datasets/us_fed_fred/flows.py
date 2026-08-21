"""
Flows for us_fed_fred — Prefect 3.

FRED (Federal Reserve Bank of St. Louis) public-domain economic series. FRED
serves the full history of each series on every call (latest revision only), so
each run is a **full replace** (``dump_mode="overwrite"``), not an incremental
append. A single flow downloads the seed series, rebuilds both tables, and
materializes them. The source poll short-circuits the run when no series has a
newer observation, making a scheduled daily run a cheap no-op between releases.

``observation`` is high-frequency (daily/weekly series), so it carries the BD Pro
rolling window: the most recent 6 months are pro-only, everything older is free.
``series`` is a metadata catalog and stays fully free (``NonHistorical`` coverage
from the table's last-modified time).

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers ``us_fed_fred_flow``;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_fed_fred.constants import constants
from pipelines.datasets.us_fed_fred.tasks import clean_fred, download_fred
from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    FreeLag,
    NonHistorical,
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
# `observation` is the high-frequency table, so it carries the BD Pro rolling
# window: the most recent 6 months are pro-only, everything older is free. Each
# run recomputes free_end = source_end - free_lag, rewrites both DateTimeRanges,
# and re-issues the BigQuery Row Access Policies, so the window slides forward on
# its own. part_bdpro requires BOTH a free (is_closed=False) and a pro
# (is_closed=True) Coverage to already exist on the table, or
# assert_coverage_topology raises before anything is written — the pro Coverage is
# created once at onboarding (the table was first registered AllFree).
#
# `series` is a metadata catalog with no time dimension, so its coverage is a
# single NonHistorical range from the table's last-modified time — fully free.
_COVERAGE = {
    "observation": PartBdpro(
        date_column=DateOnly(col="date"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "series": NonHistorical(),
}


@flow(name="us_fed_fred", log_prints=True)
def us_fed_fred_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the FRED seed series, rebuild both tables, materialize them.

    FRED serves the full history of each series on every call, so each run is a
    full replace (``dump_mode="overwrite"``) rather than an incremental append.
    The source poll short-circuits the run when no series has a newer observation,
    which makes a scheduled daily run a cheap no-op between releases.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new data.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="observation"
    )

    work_dir = tempfile.mkdtemp(prefix="us_fed_fred_")
    try:
        input_dir = download_fred(work_dir=work_dir)
        result = clean_fred(work_dir=work_dir, input_dir=input_dir)
        max_date = result["max_date"]

        # Skip the run when no series has a newer observation than what the
        # observation coverage already spans (unless forced).
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="observation",
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        # Commit the source Update here, before materializing: if the flow fails
        # midway, the source metadata still reflects that new data was published.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="observation",
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        tables = constants.ALL_TABLES.value

        # Dev: upload + run ALL tables first, THEN test. The observation<->series
        # relationships test runs during each table's test phase and needs BOTH
        # tables present. Since the overwrite upload deletes each table before its
        # run, a per-table run/test would test one table while the other is still
        # absent — so run and test are split into separate passes.
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

        # Prod: upload + run ALL tables first, THEN test (see the dev-phase note).
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
        shutil.rmtree(work_dir, ignore_errors=True)


# FRED updates on US business days through the afternoon (ET). Poll once daily at
# 21:00 BRT (~19:00-20:00 ET); the source-poll guard no-ops on days with no new
# observation, so a plain daily cron is cheap.
# pyrefly: ignore [missing-attribute]
us_fed_fred_flow.deploy_schedules = [
    {"cron": "25 21 * * *", "timezone": "America/Sao_Paulo"}
]
