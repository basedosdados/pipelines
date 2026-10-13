"""
Flows for us_ucla_voteview — Prefect 3.

Voteview (UCLA) congressional roll-call votes. Voteview rebuilds its
"all congresses" files on every release, and DW-NOMINATE re-estimates scores
over the whole history when new votes arrive, so past rows can change. Each run
is therefore a full replace (dump_mode="overwrite") of all five tables.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers
`us_ucla_voteview_flow`; the dev pool ignores the schedule, the prod pool
activates it (paused until armed).
"""

import shutil
import tempfile

from prefect.schedules import Cron

from pipelines.datasets.us_ucla_voteview.constants import constants
from pipelines.datasets.us_ucla_voteview.tasks import (
    clean_voteview,
    download_voteview,
)
from pipelines.utils.flow import flow
from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    FreeLag,
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

# Order matters for the tests, not the runs: members.party_code references
# parties, votes.icpsr references members, and the dictionary-coverage tests read
# dictionary. Every table is run before any is tested (see _materialize).
TABLES = ["dictionary", "parties", "members", "rollcalls", "votes"]

# BD Pro rolling window on the two dated, weekly-refreshed tables: the most
# recent 6 months of roll calls and votes are pro-only. Each run recomputes
# free_end = max(date) - 6 months, rewrites both DateTimeRanges and re-issues the
# Row Access Policies, so the window slides on its own. Both tables need a free
# (is_closed=False) and a pro (is_closed=True) Coverage before the first run.
#
# members, parties and dictionary have no date column (members and parties are
# keyed by Congress), so they take no coverage spec and stay free.
_PRO_WINDOW = PartBdpro(
    date_column=DateOnly(col="date"),
    date_format=DateFormat.YEAR_MD,
    free_lag=FreeLag(unit="months", value=6),
)
_COVERAGE = {"rollcalls": _PRO_WINDOW, "votes": _PRO_WINDOW}


def _materialize(result: dict, bucket_name: str, target: str) -> None:
    """Upload and run every table, then test every table."""
    for table in TABLES:
        upload_to_gcs(
            data_path=result[table],
            dataset_id=DATASET_ID,
            table_id=table,
            bucket_name=bucket_name,
            dump_mode="overwrite",
            source_format="parquet",
        )
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=table,
            dbt_command="run",
            target=target,
        )
    for table in TABLES:
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=table,
            dbt_command="test",
            target=target,
        )


@flow(name="us_ucla_voteview", log_prints=True)
def us_ucla_voteview_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download Voteview, rebuild all five tables and materialize them.

    Args:
        materialize_to_prod: Write the prod staging bucket and run dbt against
            ``target="prod"``. Set False to exercise only the dev half, which a
            safe test run requires: the default writes production.
        update_metadata: After the prod materialization, register coverage (and
            the BD Pro window) and commit the source update.
        force_run: Materialize even when the source poll finds no new roll call.
    """
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="voteview"
    )

    work_dir = tempfile.mkdtemp(prefix="us_ucla_voteview_")
    try:
        input_dir = download_voteview(work_dir=work_dir)
        result = clean_voteview(work_dir=work_dir, input_dir=input_dir)
        max_date = result["max_date"]

        # No new roll call since the registered coverage: nothing to do.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="rollcalls",
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="rollcalls",
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        # Dev is the pre-arm validation path only; an armed prod run skips it.
        if not materialize_to_prod:
            _materialize(result, bucket_name="basedosdados-dev", target="dev")
            return

        _materialize(result, bucket_name="basedosdados", target="prod")

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


# Voteview rebuilds its files most days while Congress sits (~06:00 UTC). A
# weekly refresh on Tuesday morning BRT catches each week's votes; the poll
# no-ops during recesses.
us_ucla_voteview_flow.deploy_schedules = [
    Cron("22 10 * * 2", timezone="America/Sao_Paulo")
]
# Measured peak RSS of the clean step: 3.8 GB (26.4M vote rows held in arrow).
# `memory` alone is ignored by the work pool, so set the limit explicitly.
us_ucla_voteview_flow.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "4Gi",
}
