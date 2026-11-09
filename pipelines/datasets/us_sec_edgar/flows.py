"""
Flows for us_sec_edgar — Prefect 3.

SEC EDGAR Financial Statement Data Sets. The SEC publishes one ZIP per calendar
quarter, roughly five weeks after quarter end, and never rewrites an earlier
one — so each run **appends one partition** (``dump_mode="append"``) rather than
replacing the history the way us_bls_cpi does.

Migrated to the staged pipeline: check_update -> extract_and_load. Unlike the
usual one-pipeline-per-table_id shape (see
``pipelines.utils.stage_dispatch.pipeline_factory``) AND unlike the usual
one-check_update-plus-one-extract_and_load-that-dispatches-N-build_and_promotes
shape (``au_geoscape_gnaf``/``us_bls_oes``/``br_rf_cno`` before its tables were
found independent), this dataset keeps the whole materialization — ``run`` and
``test`` for every table, in both dev and prod — *inside* ``extract_and_load``,
never calling the generic ``build_and_promote``. Two reasons, both genuine,
confirmed by re-reading this dataset's tasks.py/utils.py before migrating:

1. Shared download. ``download_and_clean`` downloads ONE ~700 MB quarterly ZIP
   and derives all 4 published tables (submission, numeric_fact, tag,
   presentation) plus ``dicionario`` from it in one pass — the same shape as
   au_geoscape_gnaf/us_bls_oes, which is why ``extract_and_load`` is singular
   here too.

2. Cross-table dbt test ordering. The dbt tests here are genuinely cross-table:
   ``relationships`` on numeric_fact/presentation point at submission, and
   ``custom_dictionary_coverage`` on 3 tables checks against dicionario. The
   generic ``build_and_promote`` dispatches one independent deployment run per
   table (``dispatch_build_and_promote``, see ``stage_dispatch.py``) that runs
   that table's own ``run`` **and** ``test`` without waiting for its siblings —
   so a table's tests could run before a sibling it depends on even exists.
   br_rf_cno hit the same dispatch shape and concluded its tables *don't*
   really depend on each other, so it split into one pipeline per table
   instead; here they do, so the fix is the opposite: batch every table's
   ``run`` first, then every table's ``test``, within a single flow run — the
   exact sequencing the old monolithic flow already used. This is why
   ``extract_and_load`` below calls ``run_dbt``/``register_table_materialization_task``
   directly (same tasks the generic ``build_and_promote`` composes) instead of
   dispatching it.

Both or neither were needed to migrate this dataset at all: solving only (1)
with a generic ``build_and_promote`` dispatch per table would still trip over
(2), since a dispatched run has no way to wait for its siblings.

check_update resolves the newest quarter the SEC actually serves and polls
cheaply first (the resolved quarter's last month vs. the free ``Coverage``),
only dispatching extract_and_load — which downloads the ~700 MB ZIP — when a
newer quarter has actually been published, so a scheduled run is a cheap no-op
between releases.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers both flows
below; the dev pool ignores the check_update schedule, the prod pool activates
it (deployed paused).
"""

import shutil
import tempfile

from prefect import runtime
from prefect.schedules import Cron

from pipelines.datasets.us_sec_edgar.constants import constants
from pipelines.datasets.us_sec_edgar.tasks import (
    build_dicionario_task,
    download_and_clean,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.metadata.tasks import register_table_materialization_task
from pipelines.utils.stage_dispatch import (
    Etapa,
    check_update_and_dispatch,
    deploy_tags,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATASET_ID = constants.DATASET_ID.value
CORE_TABLE = constants.CORE_TABLE.value

# Deployment name of `us_sec_edgar_extract_and_load` below (`deploy_flows.py`
# registers a deployment under the Python variable name of the `@flow`). Passed
# explicitly to `check_update_and_dispatch` since it isn't literally
# `"extract_and_load"` (the `Etapa` value `deployment_name()` defaults to).
_EXTRACT_AND_LOAD_DEPLOYMENT = "us_sec_edgar_extract_and_load"

# `dicionario` has no date column, so it takes no coverage spec at all.
TABLES_WITH_COVERAGE = list(constants.SOURCE_FILES.value.values())

# Every table uploads with dump_mode="append", including dicionario.
#
# dicionario IS a full rebuild each run — it is the union of the published table
# and the new quarter — but it must NOT use dump_mode="overwrite" to get that.
# The overwrite branch of _upload_to_gcs calls `tb.delete(mode="all")`, and
# mode="all" drops the MATERIALIZED PRODUCTION table, not just the staging
# external table. bd.Table resolves its BigQuery projects from the pod config
# rather than from bucket_name, so the dev half of the loop deletes the prod
# table too; a run with materialize_to_prod=False then never rebuilds it. That
# is exactly how basedosdados.us_sec_edgar.dicionario was lost on 2026-08-19,
# and the same defect previously bit us_fed_fred.
#
# append gives identical semantics here without any delete: build_dicionario
# writes a single fixed dicionario/data.parquet, and the append branch ends in
# st.upload(..., if_exists="replace"), which replaces that one blob wholesale.


def _promote_to_prod() -> bool:
    """Should this extract_and_load run also materialize to prod?

    Mirrors `stage_dispatch._dev_prefix()`'s `env:dev` tag check — the same
    signal `dispatch_build_and_promote` reads on every other migrated dataset
    to decide `promote_to_prod` (`deploy_flows.py` sets the tag only on
    deployments registered to the `basedosdados-dev` pool). Reproduced here,
    rather than imported, because this flow materializes to prod inline
    instead of dispatching the generic `build_and_promote` — see the module
    banner. Outside a real flow run (e.g. a bare local call),
    `runtime.flow_run.tags` is empty and this resolves to `True` (assume
    prod), same as `_dev_prefix()` — pass `materialize_to_prod=False`
    explicitly for a local/manual dry run.
    """
    return "env:dev" not in runtime.flow_run.tags


@flow(name=f"{Etapa.CHECK_UPDATE}: {DATASET_ID}", log_prints=True)
def us_sec_edgar_check_update() -> bool:
    """Poll the SEC for a newer quarterly release, dispatch extract_and_load.

    Returns:
        `True` if a newer quarter was found (and `extract_and_load` was
        dispatched); `False` otherwise.
    """
    rename_flow_run_dataset_table(
        prefix="Check Update: ", dataset_id=DATASET_ID, table_id=CORE_TABLE
    )

    result = get_latest_update()

    return check_update_and_dispatch(
        prefect_dataset_id=DATASET_ID,
        dataset_id=DATASET_ID,
        table_id=CORE_TABLE,
        reference_date=result.reference_date,
        next_deployment=_EXTRACT_AND_LOAD_DEPLOYMENT,
        date_format="%Y-%m",
        extra_download_params=result.extra_download_params,
        compare_against=result.compare_against,
    )


us_sec_edgar_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)
# The SEC posts a quarter roughly five weeks after it ends: 2026Q1 landed
# 2026-04-09 and 2026Q2 on 2026-07-06. Poll across the first half of the month
# after each quarter's release window; the source-poll guard no-ops until a new
# quarter actually appears.
us_sec_edgar_check_update.deploy_schedules = [
    Cron("55 16 5,8,11,14,17,20 1,4,7,10 *", timezone="America/Sao_Paulo")
]


@flow(name=f"{Etapa.EXTRACT_AND_LOAD}: {DATASET_ID}", log_prints=True)
def us_sec_edgar_extract_and_load(
    download_params: dict,
    materialize_to_prod: bool | None = None,
    update_metadata: bool = True,
) -> None:
    """Download the newest quarter once, build every table, run then test all.

    Args:
        download_params: dict received from check_update via
            `run_deployment()` — carries `year`/`quarter` (from
            `get_latest_update`'s `extra_download_params`); `reference_date`
            is also present but unused here (`year`/`quarter` are the exact
            values `download_and_clean` needs).
        materialize_to_prod: `None` (default) auto-detects from the
            deployment's `env:dev`/`env:prod` tag via `_promote_to_prod()` —
            the behaviour every automatic dispatch from check_update gets.
            Pass `False` explicitly to exercise only the dev half, e.g. for a
            manual/local test run — the default otherwise writes production
            even outside a real deployment (see `_promote_to_prod`). Passing
            `True` explicitly has no effect beyond what auto-detection
            already gives inside a prod deployment.
        update_metadata: After a successful prod materialization, register
            table coverage, which also re-issues the BD Pro Row Access
            Policies. Has no effect when prod is not materialized.
    """
    rename_flow_run_dataset_table(
        prefix="Extract and Load: ", dataset_id=DATASET_ID, table_id=CORE_TABLE
    )

    year = int(download_params["year"])
    quarter = int(download_params["quarter"])

    promote_to_prod = (
        _promote_to_prod()
        if materialize_to_prod is None
        else materialize_to_prod
    )

    work_dir = tempfile.mkdtemp(prefix="us_sec_edgar_")
    try:
        result = download_and_clean(
            work_dir=work_dir, year=year, quarter=quarter
        )
        tables = constants.TABLES.value

        # (bucket, dbt target) per environment; dbt target happens to share
        # its name with the environment here, but the two are conceptually
        # distinct (bucket is a staging bucket, dbt_target a profile name).
        environments = [("basedosdados-dev", "dev")]
        if promote_to_prod:
            environments.append(("basedosdados", "prod"))

        for bucket, dbt_target in environments:
            # Rebuilt per environment so the union starts from that project's
            # own published table rather than the other one's copy of it.
            result["dicionario"] = build_dicionario_task(
                work_dir=work_dir,
                observed=result["observed"],
                billing_project_id=bucket,
            )

            # Build every table before testing any of them. The tests are
            # cross-table — numeric_fact and presentation have relationships
            # onto submission, and three tables check custom_dictionary_coverage
            # against dicionario — so testing inside this loop would run a
            # table's tests before its sibling exists.
            for table in tables:
                upload_to_gcs(
                    data_path=result[table],
                    dataset_id=DATASET_ID,
                    table_id=table,
                    bucket_name=bucket,
                    dump_mode="append",
                    source_format="parquet",
                )
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="run",
                    target=dbt_target,
                )
            for table in tables:
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target=dbt_target,
                )

        if promote_to_prod and update_metadata:
            for table in TABLES_WITH_COVERAGE:
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers both early returns and any exception. One quarter is ~700 MB
        # of TSV plus its parquet; a process worker reuses its filesystem.
        shutil.rmtree(work_dir, ignore_errors=True)


us_sec_edgar_extract_and_load.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD
)
# num.txt alone is ~600 MB of TSV, held as an arrow table while it is written.
us_sec_edgar_extract_and_load.job_variables = {"memory": "8Gi"}
