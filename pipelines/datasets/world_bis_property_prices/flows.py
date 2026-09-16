"""
Flows for world_bis_property_prices — Prefect 3.

BIS Selected residential property prices (dataset code WS_SPP). A single flow
downloads the one bulk flat CSV and rebuilds the one ``price_index`` table. The
BIS republishes the full history each quarter, so each run is a full replace;
the source poll short-circuits a run until the BIS publishes a newer quarter,
making a scheduled run a cheap no-op between releases.

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers ``world_bis_property_prices_flow``;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.world_bis_property_prices.constants import constants
from pipelines.datasets.world_bis_property_prices.tasks import (
    clean_bis,
    download_bis,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
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
TABLE_ID = "price_index"

# The BIS ships the full history in the bulk file every release, so each run is
# a full replace. Use `append`, not `overwrite`: `overwrite` calls
# `tb.delete(mode="all")`, which drops the MATERIALIZED prod table (bd.Table
# resolves its projects from config.toml, not `bucket_name`, so a dev-only run
# would delete the prod table and then return before rebuilding it). `append`
# ends in `st.upload(..., if_exists="replace")`, replacing each blob wholesale;
# since the single table writes one `data.parquet` per `year=YYYY/` and the
# whole history ships every release, the file names are identical run to run and
# the semantics match — without the delete. (cf. au_abs_prices_inflation.)
_DUMP_MODE = "append"

# Quarterly refresh -> AllFree: the BD Pro rolling window applies only to tables
# refreshed monthly or more often, so no Row Access Policies are issued.
#
# The poll compares a "%Y-%m" string against the registered coverage, which for
# a YearQuarter column is stored as MAX(DATE(year, quarter*3, 1)) formatted the
# same way — so the pollable form is the year-MONTH (`clean_all`'s
# max_year_month), not the year-quarter. Polling at a coarser granularity than
# the coverage is stored at compares mismatched clocks, and the guard then
# either never fires or fires on every run.
_COVERAGE = AllFree(
    date_column=YearQuarter(year="year", quarter="quarter"),
    date_format=DateFormat.YEAR_MONTH,
)


@flow(name="world_bis_property_prices", log_prints=True)
def world_bis_property_prices_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the BIS selected property prices, rebuild the table, materialize it.

    The BIS ships the full history on every release, so each run is a full
    replace. The source poll short-circuits the run when the BIS has not
    published a newer quarter, making a scheduled run a cheap no-op between
    releases.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new quarter.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=TABLE_ID
    )

    work_dir = tempfile.mkdtemp(prefix="world_bis_property_prices_")
    try:
        input_dir = download_bis(work_dir=work_dir)
        result = clean_bis(work_dir=work_dir, input_dir=input_dir)
        max_ym = result["max_year_month"]

        # Skip the run when the BIS has not published a newer quarter (unless
        # forced). The pollable form is the year-MONTH (month = quarter*3).
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        # Commit the source Update up front: if the flow fails mid-way, the
        # source metadata still reflects that a new quarter was published.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        # The dev materialization is the pre-arm validation path, not part of a
        # production run: it rebuilds and re-tests the table in basedosdados-dev,
        # which nothing downstream reads.
        if not materialize_to_prod:
            upload_to_gcs(
                data_path=result[TABLE_ID],
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                bucket_name="basedosdados-dev",
                dump_mode=_DUMP_MODE,
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                dbt_command="run/test",
                target="dev",
            )
            return

        # Prod: upload staging + materialize/test.
        upload_to_gcs(
            data_path=result[TABLE_ID],
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            bucket_name="basedosdados",
            dump_mode=_DUMP_MODE,
            source_format="parquet",
        )
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            dbt_command="run/test",
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
        # Covers early returns (no new data, dev-only) and any exception.
        shutil.rmtree(work_dir, ignore_errors=True)


# The BIS publishes residential property prices quarterly; the new quarter lands
# roughly a quarter after quarter-end and the exact day varies, while the bulk
# file also refreshes between releases as economies report. Poll a short window
# in the second half of every month at 15:47 America/Sao_Paulo (a free minute);
# the source-poll guard no-ops until a new quarter actually lands, so the extra
# polls cost only a download.
# pyrefly: ignore [missing-attribute]
world_bis_property_prices_flow.deploy_schedules = [
    {"cron": "47 15 19,20,21,22,23 * *", "timezone": "America/Sao_Paulo"}
]
