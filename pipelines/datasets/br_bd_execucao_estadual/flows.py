"""
Flows for br_bd_execucao_estadual — Prefect 3.

State-government budget execution and procurement: Minas Gerais, Bahia, Pernambuco and
São Paulo, ten published tables over 110.8M rows.

TWO FLOWS, because the four sources refresh at very different speeds:

* `br_bd_execucao_estadual_flow` — daily. MG, BA and PE are bulk file downloads, and
  only the open exercise moves, so a daily pass re-fetches that year and rebuilds.
* `br_bd_execucao_estadual_sp_flow` — weekly. São Paulo has no bulk download at all;
  SIGEO is a WebForms consultation queried once per (exercise, órgão) at roughly 36 s
  each. One exercise is about twenty minutes and the full history took five hours, so
  SP must not gate the daily run.

WHY THIS PIPELINE IS LOAD-BEARING, not just a refresh convenience.

Almost every dataset here keeps one staging table per published table, named the same,
which is the assumption `table-approve` makes when it syncs
`staging/<dataset>/<published table>/` from the dev bucket to the prod one. This dataset
has 49 staging mirrors -- one per SOURCE table -- feeding 10 published models through
ephemeral per-state models, because harmonizing four states genuinely is a join across
each one's dimensional export.

So table-approve's sync matched nothing, merging the onboarding PR left
`basedosdados-staging` empty, and the prod dbt run failed on `mg_dm_acao` with
"Access Denied ... or perhaps it does not exist". This flow is what puts the mirrors in
prod, by uploading them to the prod bucket itself. The first prod run therefore has to
be `full_refresh=True`, which downloads every exercise and uploads all 49; after that
the daily incremental keeps them current.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers both flows; the dev pool
ignores the schedule, the prod pool activates it (paused).
"""

import datetime
import shutil
import tempfile

from prefect import flow

from pipelines.datasets.br_bd_execucao_estadual.constants import constants
from pipelines.datasets.br_bd_execucao_estadual.coverage import (
    refresh_state_coverage,
)
from pipelines.datasets.br_bd_execucao_estadual.tasks import (
    parquet_row_count,
    refresh_state,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATASET_ID = constants.DATASET_ID.value

# MG and BA publish daily; PE republishes the open exercise as it goes. Grouping them in
# one flow keeps a single dbt pass over the tables they share -- `licitacao`,
# `licitacao_item` and `relacionamentos` are each fed by two states, and rebuilding them
# once per state would run the same model twice for no gain.
DAILY_STATES = ["MG", "BA", "PE"]
WEEKLY_STATES = ["SP"]


def _run(
    states: list[str],
    materialize_to_prod: bool,
    update_metadata: bool,
    full_refresh: bool,
) -> None:
    """Refresh the given states, then rebuild every table they feed."""
    year = datetime.date.today().year
    work_dir = tempfile.mkdtemp(prefix="br_bd_execucao_estadual_")
    try:
        staging: dict[str, str] = {}
        for state in states:
            paths = refresh_state(
                state=state,
                work_dir=work_dir,
                year=year,
                full_refresh=full_refresh,
            )
            print(f"{state}: {parquet_row_count(paths):,} rows on disk")
            staging |= paths

        # The dev pass exists to exercise the flow before arming it; nothing downstream
        # reads basedosdados-dev. A production run writes the prod bucket, which is the
        # only way the 49 mirrors reach `basedosdados-staging`.
        bucket = "basedosdados" if materialize_to_prod else "basedosdados-dev"
        target = "prod" if materialize_to_prod else "dev"

        for table_id, path in sorted(staging.items()):
            # append, not overwrite: the parquet is one file per source file, named
            # deterministically, so re-uploading an exercise replaces that object
            # instead of adding a second copy. overwrite would drop the years this run
            # did not rebuild -- and, on a dev run, would delete the prod table.
            upload_to_gcs(
                data_path=path,
                dataset_id=DATASET_ID,
                table_id=table_id,
                bucket_name=bucket,
                dump_mode="append",
                source_format="parquet",
            )

        # Every table these states feed, deduplicated: `licitacao` is MG and BA, and
        # must be rebuilt once, after both have uploaded.
        tables = [
            t
            for t in constants.PUBLISHED_TABLES.value
            if any(t in constants.TABLES_BY_STATE.value[s] for s in states)
        ]

        # Run every model before testing any: relationships tests read sibling models,
        # and `dicionario` must exist before the tables that reference it are tested.
        for table_id in tables:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table_id,
                dbt_command="run",
                target=target,
            )
        for table_id in tables:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table_id,
                dbt_command="test",
                target=target,
            )

        if materialize_to_prod and update_metadata:
            from pipelines.utils.metadata.client import MetadataClient

            client = MetadataClient(env="prod")
            for table_id in tables:
                for state in states:
                    if table_id in constants.TABLES_BY_STATE.value[state]:
                        print(
                            refresh_state_coverage(
                                client,
                                table_id,
                                state,
                                bq_project="basedosdados",
                                billing_project="basedosdados",
                            )
                        )
    finally:
        # Covers the dev-only path and any exception. A process worker reuses its
        # filesystem and a full refresh is roughly 20 GB of raw input.
        shutil.rmtree(work_dir, ignore_errors=True)


@flow(name="br_bd_execucao_estadual", log_prints=True)
def br_bd_execucao_estadual_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    full_refresh: bool = False,
) -> None:
    """Refresh Minas Gerais, Bahia and Pernambuco, and rebuild the tables they feed.

    Args:
        materialize_to_prod: Continue past the dev pass to write the prod staging
            bucket and run dbt against ``target="prod"``. False exercises only the dev
            half — required for a safe test run, since the default writes production.
        update_metadata: After a successful prod materialization, refresh each
            (table, state) coverage range. No effect when materialize_to_prod is False.
        full_refresh: Re-download every exercise instead of just the open one, and
            upload all of that state's staging mirrors. Needed for the FIRST prod run,
            which is what populates `basedosdados-staging` — table-approve cannot do it
            for this dataset. Roughly 20 GB of input and several hours.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="despesa"
    )
    _run(DAILY_STATES, materialize_to_prod, update_metadata, full_refresh)


@flow(name="br_bd_execucao_estadual_sp", log_prints=True)
def br_bd_execucao_estadual_sp_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    full_refresh: bool = False,
) -> None:
    """Re-scrape São Paulo's open exercise and rebuild `despesa_anual`.

    Separate from the daily flow because SIGEO is a per-(exercise, órgão) WebForms
    scrape: about twenty minutes for the open year, five hours for all seventeen.

    Args:
        materialize_to_prod: As above.
        update_metadata: As above.
        full_refresh: Re-scrape every exercise from 2010. Five hours; needed once, for
            the first prod run.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="despesa_anual"
    )
    _run(WEEKLY_STATES, materialize_to_prod, update_metadata, full_refresh)


# MG publishes D+1 and BA D-1, so the data is a day old by 06:00 either way. 04:40 is
# unused elsewhere in the repo and lands before the working day in São Paulo.
# pyrefly: ignore [missing-attribute]
br_bd_execucao_estadual_flow.deploy_schedules = [
    {"cron": "40 4 * * *", "timezone": "America/Sao_Paulo"}
]
# `despesa` is 85M rows and the MG clean holds a duckdb working set; the raw MG input
# alone is ~2 GB before it is read.
# pyrefly: ignore [missing-attribute]
br_bd_execucao_estadual_flow.job_variables = {"memory": "12Gi"}

# Sunday, when the scrape's twenty minutes competes with nothing. SIGEO is annual, so a
# weekly pass is well inside the useful resolution of the data.
# pyrefly: ignore [missing-attribute]
br_bd_execucao_estadual_sp_flow.deploy_schedules = [
    {"cron": "20 5 * * 0", "timezone": "America/Sao_Paulo"}
]
# pyrefly: ignore [missing-attribute]
br_bd_execucao_estadual_sp_flow.job_variables = {"memory": "8Gi"}
