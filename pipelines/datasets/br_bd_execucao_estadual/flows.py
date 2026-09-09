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
# ES is per-year bulk CSV like MG, so it belongs in the daily group rather
# than the weekly one, which exists only for SP's per-(exercise, orgao) scrape.
# RS is absent from both schedules, but no longer because reachability is unknown.
#
# `dados.rs.gov.br` refused a residential Australian ISP outright while answering from a
# university range, so the reachability is path-dependent and a cluster test was the only
# way to settle it. That test has now run: a dev flow run on 2026-09-09 fetched 19
# archives (0.27 GB) and cleaned 6,744,072 rows from the GKE worker. The source IS
# reachable from the cluster.
#
# RS stays on its own flow only until it is in production, because the first prod run
# must be `full_refresh=True` and RS alone is ~36 GB expanded -- worth its own pod rather
# than added to a daily run that already carries four states. Move "RS" here once that
# run has completed.
DAILY_STATES = ["MG", "BA", "PE", "ES"]
WEEKLY_STATES = ["SP"]


def _run(
    states: list[str],
    materialize_to_prod: bool,
    update_metadata: bool,
    full_refresh: bool,
) -> None:
    """Refresh the given states, then rebuild every table they feed."""
    # Checked here, before anything is downloaded, because the failure it catches is
    # otherwise invisible until the worst possible moment. A state wired into
    # REFRESHERS and STAGING_BY_STATE but missing from TABLES_BY_STATE downloads
    # every archive, cleans it, and UPLOADS it to the bucket -- and only then raises
    # KeyError while working out what to rebuild. On a prod run that leaves the
    # staging mirrors written and no model built. RS did exactly this on its first
    # dev run; ES would have done it on the first prod run.
    unknown = [s for s in states if s not in constants.TABLES_BY_STATE.value]
    if unknown:
        raise KeyError(
            f"{unknown} refresh but feed no known table: add them to "
            "TABLES_BY_STATE, listing every published model their staging tables "
            "union into"
        )

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


@flow(name="br_bd_execucao_estadual_rs", log_prints=True)
def br_bd_execucao_estadual_rs_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    full_refresh: bool = False,
) -> None:
    """Refresh Rio Grande do Sul and rebuild `despesa`.

    Separate from the daily flow, and deployed WITHOUT a schedule, because RS's
    reachability is path-dependent: `dados.rs.gov.br` refused a residential Australian
    ISP outright while answering from a university range. A dev run from the cluster on
    2026-09-09 settled that question -- 19 archives, 6,744,072 rows -- so the source is
    reachable from GKE. The flow stays separate through the first prod run, which is
    ~36 GB expanded and does not belong bolted onto four other states.

    This flow exists so RS has a route to prod at all. `table-approve` cannot promote
    this dataset -- it syncs `staging/<dataset>/<published table>/`, and the 49 staging
    mirrors here are named after SOURCE tables -- so a flow run is the only way any
    state reaches production.

    The dev run that answered the reachability question is done. What remains is the
    first prod run at `full_refresh=True`; after that, move "RS" into DAILY_STATES and
    retire this flow.

    Args:
        materialize_to_prod: As in the daily flow.
        update_metadata: As in the daily flow. Leave False until `br_rs` coverages
            exist in prod -- the refresh updates existing coverages and silently finds
            nothing to do when there are none.
        full_refresh: Re-download all 175 monthly archives instead of the open years.
            Needed once, for the first prod run. ~2.3 GB compressed, ~36 GB expanded.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="despesa"
    )
    _run(["RS"], materialize_to_prod, update_metadata, full_refresh)


# MG publishes D+1 and BA D-1, so the data is a day old by 06:00 either way. 04:40 is
# unused elsewhere in the repo and lands before the working day in São Paulo.
# pyrefly: ignore [missing-attribute]
br_bd_execucao_estadual_flow.deploy_schedules = [
    {"cron": "40 4 * * *", "timezone": "America/Sao_Paulo"}
]
# `despesa` is 85M rows and the MG clean holds a duckdb working set; the raw MG input
# alone is ~2 GB before it is read.
# `memory` is NOT a variable of the work pool's job template, so a pod asking for it
# alone silently gets the 4Gi default -- the deploy succeeds, the deployment record shows
# what was asked for, and the OOM arrives later at an unrelated size. The pool reads
# `memory_limit` and `memory_request`. Set all three.
# pyrefly: ignore [missing-attribute]
br_bd_execucao_estadual_flow.job_variables = {
    "memory": "12Gi",
    "memory_limit": "12Gi",
    "memory_request": "4Gi",
}

# Sunday, when the scrape's twenty minutes competes with nothing. SIGEO is annual, so a
# weekly pass is well inside the useful resolution of the data.
# pyrefly: ignore [missing-attribute]
br_bd_execucao_estadual_sp_flow.deploy_schedules = [
    {"cron": "20 5 * * 0", "timezone": "America/Sao_Paulo"}
]
# pyrefly: ignore [missing-attribute]
br_bd_execucao_estadual_sp_flow.job_variables = {
    "memory": "8Gi",
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}

# No `deploy_schedules`: this flow is deployed unscheduled on purpose, and is triggered
# by hand. See the docstring.
#
# RS is the heaviest clean here -- 175 monthly archives expanding to ~36 GB, converted
# one at a time with duckdb capped at 2GB. 8Gi leaves room for the transient peak that
# killed a local run.
# pyrefly: ignore [missing-attribute]
br_bd_execucao_estadual_rs_flow.job_variables = {
    "memory": "8Gi",
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
