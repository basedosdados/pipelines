"""Flow for br_sfb_sicar — Prefect 3.

Cadastro Ambiental Rural (SICAR). Nine spatial theme tables, snapshot-stacked:
each per-UF release is appended under its own ``data`` (release date) partition,
so the history accumulates (``dump_mode="append"``). One flow refreshes all nine
themes for all 27 UFs.

The source publishes per-UF, on no fixed calendar, so the run is guarded by a
source-update poll on the anchor table (``area_imovel``): the flow short-circuits
until a UF publishes a newer snapshot. The schedule polls across a few mid-month
days; the guard makes each scheduled run a cheap no-op between releases.

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers ``br_sfb_sicar_flow``
(the flow fn is defined in this file); the dev pool ignores the schedule, the
prod pool activates it.
"""

import shutil
import tempfile
from datetime import date

from dateutil.relativedelta import relativedelta
from prefect import flow

from pipelines.crawler.sfb_sicar.constants import Constants
from pipelines.crawler.sfb_sicar.tasks import (
    clean_uf_theme,
    download_uf_theme,
    get_release_dates_task,
)
from pipelines.crawler.sfb_sicar.utils import (
    container_memory_limit_gb,
    max_release_iso,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    CoverageSpec,
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

DATASET_ID = Constants.DATASET_ID.value
THEME_TABLES = Constants.THEME_TABLES.value
TABLE_TO_POLYGON = Constants.TABLE_TO_POLYGON.value
UF_SIGLAS = Constants.UF_SIGLAS.value
ANCHOR_TABLE = Constants.ANCHOR_TABLE.value
DOWNLOAD_TRIES = Constants.DOWNLOAD_TRIES.value
DOWNLOAD_MAX_RETRIES = Constants.DOWNLOAD_MAX_RETRIES.value


# ── coverage tier (per table, span-gated) ────────────────────────────────────
#
# Business rule (set by the dataset owner): every table starts AllFree and only
# switches to the BD Pro rolling window (PartBdpro, free_lag = 6 months) once the
# stacked snapshot history spans MORE than 6 months — i.e. max(data) - min(data)
# exceeds 6 months. Below that threshold there is no "recent 6 months" worth
# paywalling, so the whole series stays free.
#
# Today every table has span 0 (a single snapshot round), so coverage_for
# always returns AllFree and the pipeline is correct as-is. The gate future-
# proofs the switch: once the second snapshot round lands (a month later), the
# span crosses 6 months only after ~6 months of accumulated rounds.
#
# ⚠️ PartBdpro is NOT a drop-in switch. register_table_materialization_task calls
# assert_coverage_topology, which HARD-FAILS unless the table already has BOTH a
# free (is_closed=False) and a pro (is_closed=True) Coverage. AllFree tables have
# only the free Coverage. So BEFORE the first run in which coverage_for returns
# PartBdpro for a table, someone must create the pro Coverage by hand:
#
#     create_update_coverage(table_id=<uuid>, area_id=<br>, is_closed=True,
#                            env="prod")   # and set is_closed=True on its range
#
# See the "BD Pro rolling window" section of prefect-pipeline-conventions.md for
# the full mechanism (the rolling window, the two DateTimeRanges, and the Row
# Access Policies are all handled by register_table_materialization_task — the
# dbt model is never touched).
def coverage_for(
    table_id: str, min_data: date | None, max_data: date | None
) -> CoverageSpec:
    """Return the coverage spec for a table given its current ``data`` span.

    AllFree while the stacked history spans <= 6 months; PartBdpro(free_lag=6mo)
    once it exceeds 6 months.

    Args:
        table_id: Table slug (unused today; kept so per-table exceptions are easy
            to add later).
        min_data: Earliest ``data`` value in the prod table, or None if empty.
        max_data: Latest ``data`` value in the prod table, or None if empty.

    Returns:
        An ``AllFree`` or ``PartBdpro`` spec keyed on the ``data`` column.
    """
    date_col = DateOnly(col="data")
    span_over_6mo = (
        min_data is not None
        and max_data is not None
        and max_data > min_data + relativedelta(months=6)
    )
    if span_over_6mo:
        return PartBdpro(
            date_column=date_col,
            date_format=DateFormat.YEAR_MD,
            free_lag=FreeLag(unit="months", value=6),
        )
    return AllFree(date_column=date_col, date_format=DateFormat.YEAR_MD)


def _bq_min_max_data(
    table_id: str, billing_project: str
) -> tuple[date | None, date | None]:
    """Read ``(min(data), max(data))`` for a table from prod BigQuery.

    Returns ``(None, None)`` if the table does not exist yet or is empty (the
    first prod run, before the table is materialized) — coverage_for then falls
    back to AllFree.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=billing_project)
    sql = (
        f"SELECT MIN(data) AS mn, MAX(data) AS mx "
        f"FROM `basedosdados.{DATASET_ID}.{table_id}`"
    )
    try:
        row = next(iter(client.query(sql).result()))
    except Exception as exc:  # table missing on the very first prod run
        print(f"min/max(data) query failed for {table_id}: {exc}")
        return None, None
    return row["mn"], row["mx"]


def _staging_uf_done(
    bucket_name: str, table_id: str, snapshot_iso: str, sigla_uf: str
) -> bool:
    """True if this UF's partition already sits in GCS staging.

    The staging prefix is the pipeline's durable resume checkpoint: each UF is
    uploaded the moment it is cleaned, so a pod eviction mid-run loses only the
    in-flight UF and the resubmit skips everything already here.

    The staging bucket is requester-pays, and its ``user_project`` must be billed
    to a project the *storage_staging* credentials can use — which a raw
    ``storage.Client`` (pod ADC) cannot: without ``user_project`` it 400s
    ("requester pays … no user project"), and billing the pod's own project 403s
    on ``serviceusage.services.use``. So reuse ``bd.Storage``'s own bucket — the
    exact object ``_upload_to_gcs`` uploads through — which is built from the
    storage_staging credentials with ``user_project=billing_project_id``. On any
    transient error, return False (treat as not-staged) so the UF is re-cleaned
    and re-uploaded (idempotent) rather than crashing the run.
    """
    import basedosdados as bd

    st = bd.Storage(
        dataset_id=DATASET_ID,
        table_id=table_id,
        bucket_name=bucket_name,
        billing_project_id=bucket_name,
    )
    prefix = (
        f"staging/{DATASET_ID}/{table_id}/"
        f"data={snapshot_iso}/sigla_uf={sigla_uf}/"
    )
    try:
        return (
            next(
                iter(st.bucket.list_blobs(prefix=prefix, max_results=1)), None
            )
            is not None
        )
    except Exception as exc:
        print(f"staging check failed for {table_id}/{sigla_uf}: {exc}")
        return False


def _bq_table_exists(project: str, table_id: str) -> bool:
    """True if the materialized table exists in ``<project>.<dataset>``.

    Lets a resubmit skip re-running dbt for a theme already built in an earlier
    pod, while still building a theme whose UFs were staged but not yet
    materialized (staged in a pod that was evicted before its dbt run).
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    sql = (
        f"SELECT COUNT(1) AS c FROM `{project}.{DATASET_ID}.__TABLES__` "
        f"WHERE table_id = '{table_id}'"
    )
    try:
        return next(iter(client.query(sql).result()))["c"] > 0
    except Exception as exc:  # dataset absent on the very first run
        print(f"table-exists check failed for {project}.{table_id}: {exc}")
        return False


@flow(name="br_sfb_sicar", log_prints=True)
def br_sfb_sicar_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    only_themes: str = "",
    only_ufs: str = "",
    clean_only: bool = False,
    stage_only: bool = False,
) -> None:
    """Refresh all nine SICAR theme tables (snapshot-stacked, append).

    Downloads each UF x theme, cleans it to all-string partitioned parquet, and
    stages it to GCS under its per-UF release-date partition the instant it is
    cleaned. That per-UF staging is a durable checkpoint: the run is resumable
    across node evictions (Prefect resubmits the flow from scratch on SIGTERM,
    but staged UFs are skipped, so progress accumulates). The source-update poll
    on ``area_imovel`` short-circuits the run until a UF publishes a newer
    snapshot.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new snapshot.
        only_themes: Comma-separated table slugs to restrict the run to (empty =
            all nine). Scopes cleaning, upload, dbt, and metadata alike.
        only_ufs: Comma-separated UF codes to restrict the run to (empty = all
            27). Useful to re-run a single failed state.
        clean_only: Clean each UF but stage nothing — a memory smoke test for the
            vertex-dense themes (e.g. Amazonas ``app``) that touches neither dev
            nor prod. No GCS checkpoint is written, so it is not resumable.
        stage_only: Download → clean → stage every UF to GCS (the full, resumable
            checkpoint), but skip all dbt run/test and metadata. Use to complete
            the national backfill's staging without touching the BigQuery daily
            query quota (which the 9 large geo tables' full materializations
            exhaust); verify staging row counts afterwards, and let table-approve
            materialize prod on merge.
    """
    limit_gb = container_memory_limit_gb()
    print(
        f"container memory limit: {limit_gb:.1f} GiB"
        if limit_gb is not None
        else "container memory limit: unlimited/unknown"
    )

    themes = [
        t
        for t in THEME_TABLES
        if not only_themes or t in only_themes.split(",")
    ]
    ufs = [u for u in UF_SIGLAS if not only_ufs or u in only_ufs.split(",")]

    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=ANCHOR_TABLE
    )

    # Cheap: one page fetch. Do the poll BEFORE downloading gigabytes of zips.
    release_iso = get_release_dates_task()
    max_date = max_release_iso(release_iso)

    has_new_data = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id=ANCHOR_TABLE,
        source_max_date=max_date,
        env="prod",
        date_format="%Y-%m-%d",
        compare_against="coverage",
    )
    if not has_new_data and not force_run:
        return

    # Commit the source Update up front: if the flow fails mid-materialization,
    # the source metadata still reflects that a newer snapshot was published.
    commit_source_update_task(
        dataset_id=DATASET_ID,
        table_id=ANCHOR_TABLE,
        source_max_date=max_date,
        env="prod",
        date_format="%Y-%m-%d",
        update_metadata=update_metadata,
        materialize_after_dump=materialize_to_prod,
    )

    work_dir = tempfile.mkdtemp(prefix="br_sfb_sicar_")
    input_dir = f"{work_dir}/input"
    output_dir = f"{work_dir}/output"
    dev_bucket = "basedosdados-dev"
    prod_bucket = "basedosdados"
    try:
        # Resumable per-UF staging. The full national backfill is ~30 h of
        # download+clean across 9 themes x 27 UFs — far longer than the ~hourly
        # node eviction window of this pool. A single monolithic pod is therefore
        # SIGTERM'd mid-run and Prefect resubmits the flow from scratch (empty
        # /tmp, no task cache), so it can never finish.
        #
        # So each UF is staged to GCS the instant it is cleaned: GCS staging is a
        # durable checkpoint. An eviction loses only the in-flight UF; the
        # resubmit skips every already-staged UF (no re-download — the expensive
        # part) and continues. Progress is monotonic and the run completes across
        # however many resubmits. dbt runs per theme once its UFs are staged;
        # tests run after every theme is built (cross-table refs need siblings).
        #
        # A UF whose download fails after all retries is SKIPPED, not fatal
        # (SICAR's captcha download is flaky); a resubmit retries it, since a
        # skipped UF is never staged. Clean failures stay fatal — the clean is
        # deterministic, so a failure there is a real bug to surface.
        skipped: list[str] = []
        built_themes: list[str] = []
        # Temas que precisam de build em prod — executados só depois que dev
        # inteiro rodou e passou nos testes.
        prod_pending: list[str] = []
        for table in themes:
            polygon = TABLE_TO_POLYGON[table]
            theme_root = f"{output_dir}/{table}"
            fresh_dev = False
            fresh_prod = False
            any_staged = False
            for sigla_uf in ufs:
                snapshot_iso = release_iso.get(sigla_uf)
                if not snapshot_iso:
                    print(f"no release date for {sigla_uf}; skipping")
                    skipped.append(f"{table}/{sigla_uf} (no release date)")
                    continue
                dev_done = _staging_uf_done(
                    dev_bucket, table, snapshot_iso, sigla_uf
                )
                prod_done = not materialize_to_prod or _staging_uf_done(
                    prod_bucket, table, snapshot_iso, sigla_uf
                )
                if dev_done and prod_done:
                    any_staged = True  # already checkpointed in a prior pod
                    continue
                try:
                    zip_path = download_uf_theme(
                        input_dir=input_dir,
                        sigla_uf=sigla_uf,
                        polygon=polygon,
                        tries=DOWNLOAD_TRIES,
                        max_retries=DOWNLOAD_MAX_RETRIES,
                    )
                except Exception as exc:  # any download failure → skip this UF
                    print(
                        f"download failed for {sigla_uf} {table} after "
                        f"{DOWNLOAD_MAX_RETRIES} retries; SKIPPING this state "
                        f"and continuing: {exc}"
                    )
                    skipped.append(f"{table}/{sigla_uf} (download)")
                    continue
                rows = clean_uf_theme(
                    zip_path=zip_path,
                    output_dir=output_dir,
                    table=table,
                    snapshot_iso=snapshot_iso,
                    sigla_uf=sigla_uf,
                )
                if not rows:
                    continue
                if clean_only:
                    # Smoke test: cleaned locally, stage nothing. Free the disk.
                    shutil.rmtree(theme_root, ignore_errors=True)
                    any_staged = True
                    continue
                # Upload just this UF's hive partition. Storage.upload globs the
                # table root and preserves data=…/sigla_uf=…, so passing the root
                # (which holds only this UF now) appends one partition to staging
                # without touching the others. Then clear the root for the next UF
                # so peak disk stays near a single UF.
                if not dev_done:
                    upload_to_gcs(
                        data_path=theme_root,
                        dataset_id=DATASET_ID,
                        table_id=table,
                        bucket_name=dev_bucket,
                        dump_mode="append",
                        source_format="parquet",
                    )
                    fresh_dev = True
                if materialize_to_prod and not prod_done:
                    upload_to_gcs(
                        data_path=theme_root,
                        dataset_id=DATASET_ID,
                        table_id=table,
                        bucket_name=prod_bucket,
                        dump_mode="append",
                        source_format="parquet",
                    )
                    fresh_prod = True
                shutil.rmtree(theme_root, ignore_errors=True)
                any_staged = True

            if clean_only or not any_staged:
                continue
            built_themes.append(table)
            if stage_only:
                continue  # staging is the durable output; skip dbt entirely
            # Build (dbt run) this theme now that its UFs are staged. Skip if it
            # is already materialized and nothing new was staged this pod — but
            # always build when the table is missing (staged in a pod evicted
            # before its dbt run). Tests are deferred to the loop below.
            if fresh_dev or not _bq_table_exists(dev_bucket, table):
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="run",
                    target="dev",
                )
            # O build de prod NÃO acontece aqui: fica pendente até dev ter
            # rodado e passado nos testes. A staging de prod já está no bucket,
            # então adiar só reordena as invocações do dbt — a resumabilidade
            # por UF continua intacta.
            if materialize_to_prod and (
                fresh_prod or not _bq_table_exists(prod_bucket, table)
            ):
                prod_pending.append(table)

        if skipped:
            print(
                f"WARNING: {len(skipped)} UF x theme combo(s) skipped "
                f"(source unreachable): {', '.join(skipped)}. A resubmit / "
                f"only_ufs re-run retries them (staged UFs are fast-forwarded)."
            )

        if clean_only:
            print("clean_only: cleaned without staging; returning")
            return

        if not built_themes:
            print("no UF x theme produced output; nothing to build")
            return

        if stage_only:
            print(
                f"stage_only: {len(built_themes)} theme(s) fully staged to GCS "
                f"(dev bucket); skipped dbt run/test and metadata. Verify staging "
                f"row counts; prod materializes via table-approve on merge."
            )
            return

        # Test after every theme is built: cross-table tests (relationships,
        # dictionary coverage) read sibling models, so all must exist first.
        for table in built_themes:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target="dev",
            )

        # Só agora prod: dev inteiro construído e testado. Se qualquer teste de
        # dev acima falhar, run_dbt levanta e nenhuma tabela de prod é tocada.
        if not materialize_to_prod:
            return

        for table in prod_pending:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target="prod",
            )
        for table in built_themes:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target="prod",
            )

        if update_metadata and materialize_to_prod:
            for table in built_themes:
                min_data, max_data = _bq_min_max_data(table, "basedosdados")
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage_for(table, min_data, max_data),
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers the early returns (no new data, dev-only) and any exception.
        shutil.rmtree(work_dir, ignore_errors=True)


# SICAR publishes per-UF on no fixed calendar. Poll across a few mid-month days
# at 16:00 BRT; the source-poll guard no-ops until a UF publishes a newer
# snapshot.
# pyrefly: ignore [missing-attribute]
br_sfb_sicar_flow.deploy_schedules = [
    {"cron": "0 16 10,11,12,13,14,15 * *", "timezone": "America/Sao_Paulo"}
]
# Memory: the clean is bounded to one feature range per subprocess, so it does
# not need much — but the pod must actually get what we ask for. This work pool's
# job template may key memory as either ``memory`` or ``memory_limit`` /
# ``memory_request``. Set both conventions; ``container_memory_limit_gb`` logs
# the limit the pod actually got.
# MALLOC_ARENA_MAX (env, plus the utils.py mallopt/malloc_trim backstops) is a
# cheap second bound on any single range's glibc footprint. The work pool's
# `env` variable is a Kubernetes-style array of {name, value} objects, not a
# flat dict — a dict here fails server-side schema validation on every
# full-catalog deploy (basedosdados/pipelines#1893).
# pyrefly: ignore [missing-attribute]
br_sfb_sicar_flow.job_variables = {
    "memory": "12Gi",
    "memory_limit": "12Gi",
    "memory_request": "4Gi",
    "env": [
        {"name": "MALLOC_ARENA_MAX", "value": "2"},
        {"name": "MALLOC_TRIM_THRESHOLD_", "value": "131072"},
    ],
}
