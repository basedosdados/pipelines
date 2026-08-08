"""Flows for br_me_siconfi — Prefect 3.

SICONFI (Tesouro Nacional): 19 annual budget/balance-sheet tables across
município / UF / Brasil. Tesouro revises already-published years retroactively,
so a delta pipeline would miss those — every run **rebuilds and fully overwrites**
all tables (``dump_mode="overwrite"``).

The API is one call per (entity, year) at ~1.1s, and the ~5,570 municípios
dominate. To keep a monthly run tractable, each run re-downloads only a trailing
window of years and reads the older years (incl. the frozen 1989-2012 Finbra
data) from a GCS parquet cache, then rebuilds the full tables from the union.

Unlike ``us_bls_cpi``, this flow does **not** early-return on the source poll: a
new *year* appearing is not the trigger — catching intra-year revisions is — so
every scheduled run does real work. The poll/commit still record the source
``Poll``/``Update`` for bookkeeping.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers ``br_me_siconfi_flow``;
the dev pool ignores the schedule, the prod pool activates it (paused).
"""

import shutil
import tempfile
from datetime import datetime

from prefect.schedules import Cron

from pipelines.datasets.br_me_siconfi import tasks, utils
from pipelines.datasets.br_me_siconfi.constants import constants
from pipelines.utils.flow import flow
from pipelines.utils.metadata.domain import AllFree, DateFormat, YearOnly
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

# All 19 tables are annual and fully public (the dataset is AllFree in prod), so
# every table takes the same free, year-granular coverage. No BD Pro window.
_COVERAGE = AllFree(
    date_column=YearOnly(col="ano"), date_format=DateFormat.YEAR
)

# One representative table anchors the source Poll/Update. Its rows span both of
# the dataset's raw sources (the updating API source + the frozen 1989-2012
# Finbra source), so poll/commit disambiguate by _SOURCE_URL — the API source's
# registered url — via the raw_source_url selector (multi-source fix in
# pipelines/utils/metadata). The API source is the one that actually publishes.
_SOURCE_TABLE = "municipio_receitas_orcamentarias"
_SOURCE_URL = "https://siconfi.tesouro.gov.br/siconfi/index.jsf"


@flow(name="br_me_siconfi", log_prints=True)
def br_me_siconfi_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    start_year: int | None = None,
    end_year: int | None = None,
    full_refresh: bool = False,
    levels: tuple[str, ...] = constants.ALL_LEVELS.value,
    use_cache: bool = True,
    cache_bucket: str = "basedosdados",
    download_workers: int = 1,
) -> None:
    """Refresh br_me_siconfi from the SICONFI API and materialize all tables.

    Every run rebuilds and fully overwrites the tables (``dump_mode="overwrite"``)
    to capture Tesouro's retroactive revisions; the source poll is recorded but
    is **not** a gate.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt ``target="prod"``. Set False to
            exercise only the dev half — required for a safe test run.
        update_metadata: After a successful prod materialization, register table
            coverage and record the source Poll/Update. No effect when
            ``materialize_to_prod`` is False. Set False for dev tests.
        start_year: First window year to re-download. Default = current year -
            ``WINDOW_YEARS`` + 1 (the trailing window).
        end_year: Last window year. Default = current year.
        full_refresh: Re-download every API year from ``API_FIRST_YEAR`` instead
            of just the trailing window (re-catches deeper revisions). Overrides
            ``start_year``.
        levels: Government levels to build. Restrict (e.g. ``("brasil","uf")``)
            for a fast dev smoke test that skips the município download.
        use_cache: Union out-of-window years from the GCS parquet cache so the
            overwrite is a full rebuild. Set False for a bounded dev run that
            uploads only the window years (dev data is disposable).
        cache_bucket: Bucket holding the parquet cache (defaults to prod, which
            the deployed prod worker can read/write). Only used when
            ``use_cache`` is True.
        download_workers: Parallel download threads for the município-heavy
            window. Default 1; raise with care against the .gov API.
    """
    now_year = datetime.now().year
    if full_refresh:
        start_year = constants.API_FIRST_YEAR.value
    elif start_year is None:
        start_year = now_year - constants.WINDOW_YEARS.value + 1
    if end_year is None:
        end_year = now_year

    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="siconfi"
    )

    work_dir = tempfile.mkdtemp(prefix="br_me_siconfi_")
    try:
        api_dir = tasks.download(
            work_dir=work_dir,
            start_year=start_year,
            end_year=end_year,
            levels=levels,
            workers=download_workers,
        )
        # Archive the raw source JSON (provenance) to the dev bucket's raw/ prefix.
        tasks.archive(work_dir=work_dir, bucket_name="basedosdados-dev")
        result = tasks.clean(
            work_dir=work_dir,
            api_dir=api_dir,
            start_year=start_year,
            end_year=end_year,
            levels=levels,
            use_cache=use_cache,
            cache_bucket=cache_bucket,
        )
        max_year = result.pop("max_year", None)
        tables = list(result)  # only non-empty tables were returned

        # Record that we looked at the source (Poll). NOT a gate — SICONFI must
        # rebuild every run regardless of whether a new year appeared.
        if update_metadata and max_year is not None:
            poll_source_for_update_task(
                dataset_id=DATASET_ID,
                table_id=_SOURCE_TABLE,
                source_max_date=str(max_year),
                env="prod",
                date_format="%Y",
                raw_source_url=_SOURCE_URL,
            )

        # Dev: upload staging + materialize/test.
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
                dbt_command="run/test",
                target="dev",
            )

        if not materialize_to_prod:
            return

        # Archive the raw source JSON (provenance) to the prod bucket's raw/ prefix.
        tasks.archive(work_dir=work_dir, bucket_name="basedosdados")

        # Prod: upload staging + materialize/test.
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
                dbt_command="run/test",
                target="prod",
            )

        if update_metadata:
            for table in tables:
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=_COVERAGE,
                    env="prod",
                    bq_project="basedosdados",
                )
            if max_year is not None:
                commit_source_update_task(
                    dataset_id=DATASET_ID,
                    table_id=_SOURCE_TABLE,
                    source_max_date=str(max_year),
                    env="prod",
                    date_format="%Y",
                    raw_source_url=_SOURCE_URL,
                )
    finally:
        # Covers early returns (dev-only) and exceptions. k8s gives each run a
        # fresh pod, but a process/local worker reuses its filesystem — the
        # município window download can be several GB.
        shutil.rmtree(work_dir, ignore_errors=True)


# SICONFI is annual but revised retroactively; rebuild once a month (1st at
# 16:00 BRT). Each run rebuilds fully — there is no source-poll no-op here.
br_me_siconfi_flow.deploy_schedules = [
    Cron("0 16 1 * *", timezone="America/Sao_Paulo")
]
# The município window build holds a full year of data in pandas at a time.
br_me_siconfi_flow.job_variables = {"memory": "16Gi"}


@flow(name="br_me_siconfi_seed", log_prints=True)
def br_me_siconfi_seed_flow(
    raw_bucket: str = "basedosdados",
    raw_prefix: str = constants.RAW_LEGACY_PREFIX.value,
    cache_bucket: str = "basedosdados",
    seed_legacy: bool = True,
    seed_bq: bool = True,
    bq_start_year: int | None = None,
    bq_end_year: int | None = None,
    bq_project: str = "basedosdados",
) -> None:
    """One-time seed of the whole out-of-window cache before the first run.

    The recurring flow is overwrite and serves pre-window years from the cache,
    so an unseeded run would rebuild the prod tables without them and wipe that
    history. Two complementary steps fill the cache for every year before the
    trailing window:

    1. ``seed_legacy`` — rebuild 1989-2012 (the four legacy município tables)
       from the raw Excel at ``gs://<raw_bucket>/<raw_prefix>/``.
    2. ``seed_bq`` — cache the API years ``API_FIRST_YEAR..(window_start-1)`` for
       ALL tables by reading the existing prod tables in ``<bq_project>`` (the
       same cleaned data, built by this code), avoiding a many-hour paginated API
       re-download on the first run.

    After this, the first recurring run only downloads the trailing window.
    Defaults target prod; for a dev rehearsal set ``cache_bucket`` to
    ``basedosdados-dev`` (``bq_project`` stays ``basedosdados`` — prod is the
    read source).

    Args:
        raw_bucket: Bucket holding the raw legacy Excel.
        raw_prefix: Prefix of the raw legacy Excel.
        cache_bucket: Bucket whose parquet cache is seeded.
        seed_legacy: Run the 1989-2012 legacy Excel seed.
        seed_bq: Run the API-year seed from prod BQ.
        bq_start_year: First BQ year (default ``API_FIRST_YEAR``).
        bq_end_year: Last BQ year (default ``window_start - 1`` =
            ``current_year - WINDOW_YEARS``).
        bq_project: Project holding the prod tables to read from.
    """
    if bq_start_year is None:
        bq_start_year = constants.API_FIRST_YEAR.value
    if bq_end_year is None:
        bq_end_year = datetime.now().year - constants.WINDOW_YEARS.value

    if seed_legacy:
        work_dir = tempfile.mkdtemp(prefix="br_me_siconfi_seed_")
        try:
            legacy = tasks.seed_legacy_cache(
                work_dir=work_dir,
                raw_bucket=raw_bucket,
                raw_prefix=raw_prefix,
                cache_bucket=cache_bucket,
            )
            print(
                f"seed_legacy_cache: seeded {len(legacy)} legacy tables into cache"
            )
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

    if seed_bq:
        tables = utils.tables_for_levels(constants.ALL_LEVELS.value)
        bq = tasks.seed_cache_from_bq(
            cache_bucket=cache_bucket,
            tables=tables,
            start_year=bq_start_year,
            end_year=bq_end_year,
            bq_project=bq_project,
        )
        print(
            f"seed_cache_from_bq: seeded {len(bq)} tables "
            f"({bq_start_year}-{bq_end_year}) from {bq_project} BQ into cache"
        )


# The legacy build holds one year of município Excel in pandas at a time.
br_me_siconfi_seed_flow.job_variables = {"memory": "8Gi"}
