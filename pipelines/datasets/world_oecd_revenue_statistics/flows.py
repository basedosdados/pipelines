"""Flow for world_oecd_revenue_statistics — Prefect 3, ON-DEMAND (no schedule).

OECD Global Revenue Statistics comparative cube (SDMX DF_RSGLOBAL). The OECD host
Cloudflare-challenges heavy automated pulls, so a scheduled worker cannot reliably
download the ~300 MB cube — this flow is therefore **on-demand**: trigger it after
OECD publishes a new edition. Two ways to feed it data:

1. Let it download (works from a fresh IP; may be blocked by Cloudflare), or
2. Pre-stage the CSVs in ``<work_dir>/input`` (e.g. a manual browser download of
   ``.../DF_RSGLOBAL,2.1/......?format=csvfile``) — the download task reuses any
   files already present.

The source ships the full history each edition, so every run is a full replace
(``dump_mode="overwrite"``). Deploy: ``.github/scripts/deploy_flows.py``
auto-discovers ``world_oecd_revenue_statistics_flow``; no schedule is registered.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.world_oecd_revenue_statistics.constants import (
    constants,
)
from pipelines.datasets.world_oecd_revenue_statistics.tasks import (
    clean_revenue,
    download_revenue,
)
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

# revenue is annual → fully free (no BD Pro rolling window; that is for tables
# refreshed monthly or more often). dicionario has no date column → no coverage.
_COVERAGE = {
    "revenue": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
}


@flow(name="world_oecd_revenue_statistics", log_prints=True)
def world_oecd_revenue_statistics_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh the OECD Global Revenue Statistics table (on-demand).

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod bucket and run dbt against ``target="prod"``. False exercises
            only the dev half — required for a safe test run.
        update_metadata: After a successful prod materialization, refresh table
            coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new year.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="revenue"
    )

    work_dir = tempfile.mkdtemp(prefix="world_oecd_revenue_statistics_")
    try:
        input_dir = download_revenue(work_dir=work_dir)
        result = clean_revenue(work_dir=work_dir, input_dir=input_dir)
        max_year = str(result["max_year"])

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="revenue",
            source_max_date=max_year,
            env="prod",
            date_format="%Y",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="revenue",
            source_max_date=max_year,
            env="prod",
            date_format="%Y",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        tables = constants.ALL_TABLES.value
        bucket = "basedosdados" if materialize_to_prod else "basedosdados-dev"
        target = "prod" if materialize_to_prod else "dev"
        for table in tables:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name=bucket,
                dump_mode="overwrite",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run/test",
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
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# On-demand: no schedule is registered. The clean step holds ~2.8M rows in
# pandas; `memory` alone is silently ignored by the work pool (defaults 4Gi), so
# set the limit the pod actually reads.
# pyrefly: ignore [missing-attribute]
world_oecd_revenue_statistics_flow.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
