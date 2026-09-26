"""
Flows for br_bd_execucao_estadual — Prefect 3.

State-government budget execution and procurement: Minas Gerais, Bahia, Pernambuco,
São Paulo, Espírito Santo, Rio Grande do Sul, Santa Catarina, Paraíba and (frozen)
Ceará -- twelve published tables, and `despesa` alone carries 115.6M rows.

FOUR FLOWS -- three refreshers on different cadences, plus one manual seed:

* `br_bd_execucao_estadual_flow` — daily. MG, BA, PE, ES, RS, SC and PB are bulk file
  or API downloads, and only the open exercises move, so a daily pass re-fetches those
  and rebuilds every table they feed (now including `liquidacao` and `contrato`).
* `br_bd_execucao_estadual_sp_flow` — weekly. São Paulo has no bulk download at all;
  SIGEO is a WebForms consultation queried once per (exercise, órgão) at roughly 36 s
  each. One exercise is about twenty minutes and the full history took five hours, so
  SP must not gate the daily run.
* `br_bd_execucao_estadual_rs_flow` — no schedule. A manual backfill route for RS
  alone; see its docstring for when it earns its keep.
* `br_bd_execucao_estadual_seed_frozen_prod_flow` — no schedule. A ONE-TIME seed that
  copies the frozen mirrors (Ceará, and the SC/RS contract registries) forward from the
  dev staging bucket to prod, because they are produced by no refresher. Run it once,
  before the first daily prod run that carries CE / liquidacao / contrato.

ORDERING TRAP, if this dataset is ever bootstrapped into a fresh environment.

`despesa` is ONE model unioning MG, PE, ES and RS, and those states are split across
two flows. Whichever flow rebuilds `despesa` needs every one of those staging mirrors
to exist already -- including the ones it does not upload itself. The first prod run
learned this the expensive way: the daily flow downloaded ~20 GB over 7h50m, uploaded
all 57 mirrors, then failed with

    Not found: Table basedosdados-staging:...rs_despesa

because RS had never uploaded. The flow that owns the missing mirror has to go FIRST.
Recovery was cheap only because uploads precede the dbt phase, so the retry ran at
`full_refresh=False` in 1h49m instead of repeating the download.

WHY THIS PIPELINE IS LOAD-BEARING, not just a refresh convenience.

Almost every dataset here keeps one staging table per published table, named the same,
which is the assumption `table-approve` makes when it syncs
`staging/<dataset>/<published table>/` from the dev bucket to the prod one. This dataset
has 49 staging mirrors -- one per SOURCE table -- feeding 10 published models through
ephemeral per-state models, because harmonizing six states genuinely is a join across
each one's dimensional export.

So table-approve's sync matched nothing, merging the onboarding PR left
`basedosdados-staging` empty, and the prod dbt run failed on `mg_dm_acao` with
"Access Denied ... or perhaps it does not exist". This flow is what puts the mirrors in
prod, by uploading them to the prod bucket itself. The first prod run therefore has to
be `full_refresh=True`, which downloads every exercise and uploads all 49; after that
the daily incremental keeps them current.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers all four flows; the dev pool
ignores the schedule, the prod pool activates it (paused). The dev pool is only written
by a PR carrying the `deploy-flow` label -- without it the deploy job skips and the
staging deployments silently keep whatever they had.
"""

import datetime
import shutil
import tempfile

from prefect import flow
from prefect.utilities.asyncutils import run_coro_as_sync

from pipelines.datasets.br_bd_execucao_estadual.constants import constants
from pipelines.datasets.br_bd_execucao_estadual.coverage import (
    refresh_state_coverage,
)
from pipelines.datasets.br_bd_execucao_estadual.tasks import (
    download_frozen_mirror,
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
# RS joined the daily group on 2026-09-10, once both preconditions it was waiting on
# were met: a GKE worker CAN reach `dados.rs.gov.br` (a dev run fetched 19 archives and
# 6.7M rows on 2026-09-09), and RS is in production (a full_refresh prod run loaded
# 51,262,226 rows the same day). Its reachability was only ever path-dependent -- the
# source refused a residential Australian ISP while answering a university range -- and
# the cluster's path works.
#
# A scoped RS pass is two exercises, about 8 minutes, which the daily run absorbs.
DAILY_STATES = ["MG", "BA", "PE", "ES", "RS", "SC", "PB"]
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
    run_coro_as_sync(
        rename_flow_run_dataset_table(
            prefix="Dump: ", dataset_id=DATASET_ID, table_id="despesa"
        )
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
    run_coro_as_sync(
        rename_flow_run_dataset_table(
            prefix="Dump: ", dataset_id=DATASET_ID, table_id="despesa_anual"
        )
    )
    _run(WEEKLY_STATES, materialize_to_prod, update_metadata, full_refresh)


@flow(name="br_bd_execucao_estadual_rs", log_prints=True)
def br_bd_execucao_estadual_rs_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    full_refresh: bool = False,
) -> None:
    """Rebuild Rio Grande do Sul on its own. MANUAL BACKFILL ONLY -- no schedule.

    RS's routine refresh is the daily flow; this is not a second schedule and must
    never become one. It is kept, rather than retired, for the one job the daily flow
    does badly: a `full_refresh=True` pass over RS alone.

    A combined full refresh would hold every state's input and parquet in ONE pod's
    work_dir at once -- `_run` uses a single tempdir for the whole run and only clears
    it at the end -- and RS by itself is ~2.3 GB compressed / ~36 GB expanded on top of
    MG, BA, PE and ES. Rebuilding RS's whole series through the daily flow therefore
    risks the pod's ephemeral storage for no reason, when this flow does the same work
    against the same tables in a pod of its own.

    Reach for it when RS republishes history, or if the six months absent from its
    catalogue (2020-06, 2020-08, 2022-04, 2023-02, 2023-06, 2023-08) ever appear. For
    anything the open exercises cover, do nothing -- the daily flow has it.

    Note that `despesa` unions four states, so this flow cannot build it unless MG, PE
    and ES already have staging mirrors in the target environment. That is what made
    the first prod run order-dependent; see the module docstring.

    Args:
        materialize_to_prod: As in the daily flow.
        update_metadata: As in the daily flow.
        full_refresh: Re-download all 175 monthly archives instead of the open years.
            This is the reason the flow still exists.
    """
    run_coro_as_sync(
        rename_flow_run_dataset_table(
            prefix="Dump: ", dataset_id=DATASET_ID, table_id="despesa"
        )
    )
    _run(["RS"], materialize_to_prod, update_metadata, full_refresh)


@flow(name="br_bd_execucao_estadual_seed_frozen_prod", log_prints=True)
def br_bd_execucao_estadual_seed_frozen_prod_flow(
    materialize_to_prod: bool = True,
    download_billing_project: str = "basedosdados",
    mirrors: list[str] | None = None,
    rebuild_tables: list[str] | None = None,
) -> None:
    """Copy already-built staging mirrors from the dev bucket into prod. MANUAL.

    Two bootstrap jobs share this one mechanism -- copy a mirror's cleaned parquet from
    `basedosdados-dev` to `basedosdados` and create the `basedosdados-staging` external
    table through `upload_to_gcs`, exactly as a refresh upload would (table-approve
    cannot: it looks for per-published-table prefixes this dataset does not have):

    * The FROZEN mirrors (`mirrors=None`, the default -> constants.FROZEN_PROD_MIRRORS):
      `ce_*`, `sc_contrato`, `rs_contrato`. No refresher produces these -- Ceará's portal
      is WAF + geo-blocked, the SC/RS contract registries are one-shot -- so they live in
      prod only by being copied here, and stay frozen.
    * A re-scrapable state's mirrors already built in dev, when re-running the whole
      daily flow to reach one late state would cost hours. SC/PB's first prod load used
      `mirrors=[sc_empenho, sc_liquidacao, sc_pagamento, pb_empenho, pb_liquidacao,
      pb_pagamento]` because those mirrors already existed in dev; a fresh clean was not
      worth another ~20 GB download of the five states ahead of them. Going forward the
      daily flow keeps SC/PB current from source -- this is a bootstrap, not their
      refresh path.

    Pass `rebuild_tables` to run those published models against `target="prod"` after the
    upload, materialising them in one shot -- their other union mirrors must already be in
    prod staging. Omit it to upload mirrors only and let the daily flow rebuild.

    Re-running is safe (idempotent overwrite).

    PROD POOL ONLY. Like `transfer_files_to_prod_flow`, this reads the requester-pays
    `basedosdados-dev` bucket billed to `download_billing_project`, which must be a
    project where the worker's SA holds `serviceusage.services.use`. That is
    `basedosdados` on the prod pool. The dev pool's SA lacks it, so a dev dry run 403s
    on the very first read -- there is no dev exercise of this flow, and the read half
    is instead proven by the repo's `download_files_from_bucket_folders` utility, which
    reads the same bucket the same way.

    Args:
        materialize_to_prod: Write the prod bucket (`basedosdados`). False writes the
            dev bucket instead -- only meaningful from the prod pool, where the SA can
            still bill the requester-pays read.
        download_billing_project: Project billed for the requester-pays read of the dev
            staging bucket. Must grant the worker SA `serviceusage.services.use`;
            `basedosdados` on the prod pool.
        mirrors: Staging mirrors to copy. None means constants.FROZEN_PROD_MIRRORS.
        rebuild_tables: Published models to `dbt run` against prod after the copy. None
            means copy only.
    """
    mirrors = (
        mirrors if mirrors is not None else constants.FROZEN_PROD_MIRRORS.value
    )
    run_coro_as_sync(
        rename_flow_run_dataset_table(
            prefix="Seed prod staging: ",
            dataset_id=DATASET_ID,
            table_id="contrato",
        )
    )
    bucket = "basedosdados" if materialize_to_prod else "basedosdados-dev"
    target = "prod" if materialize_to_prod else "dev"
    billing = download_billing_project
    work_dir = tempfile.mkdtemp(prefix="br_bd_execucao_estadual_seed_")
    try:
        for mirror in mirrors:
            path = download_frozen_mirror(
                mirror=mirror, work_dir=work_dir, billing_project=billing
            )
            # overwrite, not append: this is a one-time full replacement of the prefix,
            # and bucket and billing are the same environment, so the dev-run hazard the
            # daily flow guards against (overwrite deleting the prod table) cannot arise.
            upload_to_gcs(
                data_path=path,
                dataset_id=DATASET_ID,
                table_id=mirror,
                bucket_name=bucket,
                dump_mode="overwrite",
                source_format="parquet",
            )
        # Run every model before testing any: relationships tests read sibling models.
        for table_id in rebuild_tables or []:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table_id,
                dbt_command="run",
                target=target,
            )
        for table_id in rebuild_tables or []:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table_id,
                dbt_command="test",
                target=target,
            )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# Manual utility, disparo manual -- never scheduled.
# pyrefly: ignore [missing-attribute]
br_bd_execucao_estadual_seed_frozen_prod_flow.deploy_schedules = []


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
