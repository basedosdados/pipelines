"""
Flows for world_openalex — Prefect 3.

OpenAlex publishes its full database as a free CC0 snapshot on S3, once a
quarter (second Wednesday of January, April, July and October). Each release
rewrites most records, so every run is a **full rebuild** of all 38 tables,
not an incremental upsert.

The source poll compares the snapshot's release date with the last time the
tables were refreshed (``compare_against="table_update"``): the release date is
a publication timestamp, not a coverage period. Between releases a scheduled
run polls and returns.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers
`world_openalex_flow`; the dev pool ignores the schedule, the prod pool
activates it (paused until armed).
"""

from prefect.schedules import Cron

from pipelines.datasets.world_openalex.constants import constants
from pipelines.datasets.world_openalex.tasks import (
    get_release_date,
    load_snapshot_task,
)
from pipelines.utils.flow import flow
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    NonHistorical,
    YearOnly,
)
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import rename_flow_run_dataset_table, run_dbt

DATASET_ID = constants.DATASET_ID.value
TABLES = [t for ts in constants.ENTITY_TABLES.value.values() for t in ts] + [
    "dicionario"
]

# Every table is fully free: the source refreshes quarterly, below the monthly
# threshold for a BD Pro window. Works tables cover their publication years,
# the two author-by-year tables their year column; entity tables carry no
# time dimension and get a single last-modified coverage.
_COVERAGE: dict[str, AllFree | NonHistorical] = {
    t: AllFree(
        date_column=YearOnly(col="publication_year"),
        date_format=DateFormat.YEAR,
    )
    for t in constants.ENTITY_TABLES.value["works"]
}
for _t in ("author_affiliation", "author_counts_by_year"):
    _COVERAGE[_t] = AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    )
for _t in TABLES:
    _COVERAGE.setdefault(_t, NonHistorical())
del _COVERAGE["dicionario"]


@flow(name="world_openalex", log_prints=True)
def world_openalex_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    fresh_load: bool = False,
) -> None:
    """Rebuild every world_openalex table from the current OpenAlex snapshot.

    Args:
        materialize_to_prod: Load the prod staging bucket and run dbt against
            ``target="prod"``. False exercises only the dev half — required for
            a safe test run, since the default writes production.
        update_metadata: After a prod materialization, register coverage and
            commit the source update. No effect when ``materialize_to_prod``
            is False.
        force_run: Rebuild even when the poll finds no new release.
        fresh_load: Reload every file even when an interrupted load of the same
            release and code can be resumed.
    """
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="work"
    )
    release = get_release_date()

    # A forced run skips the poll: the poll writes a Poll record to the prod
    # backend, which a dev test run must not touch, and which fails outright
    # before the dataset exists there.
    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="work",
            source_max_date=release,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="table_update",
        )
        if not has_new_data:
            return

    commit_source_update_task(
        dataset_id=DATASET_ID,
        table_id="work",
        source_max_date=release,
        env="prod",
        date_format="%Y-%m-%d",
        update_metadata=update_metadata,
        materialize_after_dump=materialize_to_prod,
    )

    bucket = "basedosdados" if materialize_to_prod else "basedosdados-dev"
    target = "prod" if materialize_to_prod else "dev"
    load_snapshot_task(bucket_name=bucket, fresh=fresh_load)

    # Run every model, then test every model: the relationship and dictionary
    # tests read sibling models, which must all exist first.
    for table in TABLES:
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

    if materialize_to_prod and update_metadata:
        for table, coverage in _COVERAGE.items():
            register_table_materialization_task(
                dataset_id=DATASET_ID,
                table_id=table,
                coverage=coverage,
                env="prod",
                bq_project="basedosdados",
            )


# Releases land on the second Wednesday of Jan/Apr/Jul/Oct (days 8 to 14). Poll
# daily through the 21st of those months at 03:25 BRT, a free slot; the poll
# guard makes every run after the first ingest a no-op.
world_openalex_flow.deploy_schedules = [
    Cron("25 3 8-21 1,4,7,10 *", timezone="America/Sao_Paulo")
]
# Six worker processes, each downloading and uploading with parallel
# connections; the load is network-bound (a single stream ran at ~2 MB/s from
# the pods), so CPU and memory stay modest. The request stays at 4Gi so the
# pod fits the dev pool's nodes (8Gi was unschedulable); the limit is what the
# pod may grow to. `memory` alone is ignored by the work pool.
world_openalex_flow.job_variables = {
    "memory": "16Gi",
    "memory_limit": "16Gi",
    "memory_request": "4Gi",
    "cpu_limit": "4",
    "cpu_request": "1",
}
