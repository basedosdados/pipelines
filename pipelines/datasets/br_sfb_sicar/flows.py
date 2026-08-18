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


@flow(name="br_sfb_sicar", log_prints=True)
def br_sfb_sicar_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    only_themes: str = "",
    only_ufs: str = "",
    clean_only: bool = False,
) -> None:
    """Refresh all nine SICAR theme tables (snapshot-stacked, append).

    Downloads each UF x theme, cleans it to all-string partitioned parquet, and
    appends it under its per-UF release-date partition. The source-update poll on
    ``area_imovel`` short-circuits the run until a UF publishes a newer snapshot.

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
        clean_only: Stop after the download/clean phase without uploading — a
            memory smoke test for the vertex-dense themes (e.g. Amazonas ``app``)
            that touches neither dev nor prod.
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
    try:
        # Build all output. One UF x theme at a time: download → clean → delete
        # the zip, so peak disk stays near a single archive (APP is huge).
        for table in themes:
            polygon = TABLE_TO_POLYGON[table]
            for sigla_uf in ufs:
                snapshot_iso = release_iso.get(sigla_uf)
                if not snapshot_iso:
                    print(f"no release date for {sigla_uf}; skipping")
                    continue
                zip_path = download_uf_theme(
                    input_dir=input_dir,
                    sigla_uf=sigla_uf,
                    polygon=polygon,
                    tries=DOWNLOAD_TRIES,
                    max_retries=DOWNLOAD_MAX_RETRIES,
                )
                clean_uf_theme(
                    zip_path=zip_path,
                    output_dir=output_dir,
                    table=table,
                    snapshot_iso=snapshot_iso,
                    sigla_uf=sigla_uf,
                )

        if clean_only:
            print("clean_only: cleaned without uploading; returning")
            return

        # Dev: upload staging + materialize/test.
        for table in themes:
            upload_to_gcs(
                data_path=f"{output_dir}/{table}",
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados-dev",
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run/test",
                dbt_alias=True,
                target="dev",
            )

        if not materialize_to_prod:
            return

        # Prod: upload staging + materialize/test.
        for table in themes:
            upload_to_gcs(
                data_path=f"{output_dir}/{table}",
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados",
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run/test",
                dbt_alias=True,
                target="prod",
            )

        if update_metadata:
            for table in themes:
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
# ``memory_request``; a key it does not recognize is silently dropped (the ``env``
# key was), which can leave the pod on a small default limit. Set both
# conventions; ``container_memory_limit_gb`` logs the limit the pod actually got.
# MALLOC_ARENA_MAX (env, plus the utils.py mallopt/malloc_trim backstops) is a
# cheap second bound on any single range's glibc footprint.
# pyrefly: ignore [missing-attribute]
br_sfb_sicar_flow.job_variables = {
    "memory": "16Gi",
    "memory_limit": "16Gi",
    "memory_request": "4Gi",
    "env": {"MALLOC_ARENA_MAX": "2", "MALLOC_TRIM_THRESHOLD_": "131072"},
}
