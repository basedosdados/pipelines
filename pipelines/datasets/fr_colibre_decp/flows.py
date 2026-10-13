"""Recurring pipeline for fr_colibre_decp (consolidated DECP, colibre.fr).

The publisher rebuilds decp.parquet from scratch every day around 05:20 UTC, and a
rebuild can revise any contract, not only recent ones: a buyer can publish an
amendment to a 2019 contract today. So the flow does not look for "a new period".
Each run downloads the whole file (about 250 MB), rebuilds the three tables and
replaces their staging files. The full rebuild takes seconds locally, so there is
nothing to gain from an incremental path.

The source Poll is still recorded, for metadata hygiene, but it does not gate the
run: the source's maximum coverage month rarely changes between weekly runs while
the content does.
"""

from __future__ import annotations

import shutil
import tempfile

from prefect import get_run_logger
from prefect.schedules import Cron

from pipelines.datasets.fr_colibre_decp.constants import constants
from pipelines.datasets.fr_colibre_decp.tasks import (
    download_and_clean_task,
    replace_staging_task,
    source_last_modified_task,
    source_max_date_task,
)
from pipelines.utils.flow import flow
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
from pipelines.utils.tasks import rename_flow_run_dataset_table, run_dbt

DATASET_ID = constants.DATASET_ID.value
TABLES = constants.TABLES.value
DATE_FORMAT = "%Y-%m"

# The source refreshes more often than monthly, so the house rule paywalls the most
# recent six months of every table and leaves everything older free. All three
# tables carry the contract's initial-notification month, so one window applies.
_PART_BDPRO = dict(
    date_column=YearMonth(year="ano", month="mes"),
    date_format=DateFormat.YEAR_MONTH,
    free_lag=FreeLag(unit="months", value=6),
)
COVERAGE = {table: PartBdpro(**_PART_BDPRO) for table in TABLES}


@flow(name="fr_colibre_decp")
def fr_colibre_decp_flow(
    force_run: bool = False,
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
):
    """Rebuild the three DECP tables from the publisher's latest daily file.

    Args:
        force_run: accepted for parity with the other flows. Every run already
            rebuilds the full history, so it changes nothing.
        materialize_to_prod: also upload and materialize in the production project.
        update_metadata: write coverage, Poll, table Update and source Update records.
    """
    logger = get_run_logger()
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="marche"
    )
    logger.info(
        "source last rebuilt %s; force_run=%s",
        source_last_modified_task(),
        force_run,
    )

    scratch_root = tempfile.mkdtemp(prefix="fr_colibre_decp_")
    try:
        output_dir = download_and_clean_task(scratch_root)
        max_date = source_max_date_task(output_dir)
        logger.info("latest initial-notification month: %s", max_date)

        # Gated on update_metadata because the task is pinned to env="prod" whichever
        # pool the run is on; a dev validation run must not write prod metadata.
        if update_metadata:
            poll_source_for_update_task(
                dataset_id=DATASET_ID,
                table_id="marche",
                source_max_date=max_date,
                env="prod",
                date_format=DATE_FORMAT,
            )

        # Run every table, then test every table: the modification and titulaire
        # tests reference marche, which must already be built.
        for table in TABLES:
            replace_staging_task(output_dir, table, "basedosdados-dev")
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

        if not materialize_to_prod:
            return

        for table in TABLES:
            replace_staging_task(output_dir, table, "basedosdados")
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

        if not update_metadata:
            return

        for table in TABLES:
            register_table_materialization_task(
                dataset_id=DATASET_ID,
                table_id=table,
                coverage=COVERAGE[table],
                env="prod",
                bq_project="basedosdados",
            )
        # Last, and only after production succeeded.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="marche",
            source_max_date=max_date,
            env="prod",
            date_format=DATE_FORMAT,
        )
    finally:
        shutil.rmtree(scratch_root, ignore_errors=True)


# Weekly, Tuesday 03:55 São Paulo: after the publisher's daily rebuild (about 02:20
# São Paulo) and on a minute no other flow uses.
fr_colibre_decp_flow.deploy_schedules = [
    Cron("55 3 * * 2", timezone="America/Sao_Paulo")
]

# `memory` alone is ignored by the work pool's job template; `memory_limit` is the
# value the pod gets. DuckDB is capped at 3 GB in clean_decp and spills to disk
# beyond that, so 8Gi leaves room for the Python process and the upload.
fr_colibre_decp_flow.job_variables = {
    "memory": "8Gi",
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
