"""
Flows for br_senado_dados_abertos — Prefect 3.

Senado Federal legislative open data. One flow refreshes all ten tables from the
public Legislative Open Data API each day: dimensions in full, the four
time-series tables (votacao, votacao_parlamentar, votacao_orientacao_bancada,
processo) for the recent window only — uploaded with ``dump_mode="append"``,
which replaces just those ``ano=`` partitions and leaves history in place. Like
the Câmara pipeline, there is no source-poll gate: legislative activity changes
continuously, so a daily run is always meaningful.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `br_senado_dados_abertos_flow`;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect.schedules import Cron

from pipelines.datasets.br_senado_dados_abertos.constants import constants
from pipelines.datasets.br_senado_dados_abertos.tasks import extract_clean
from pipelines.utils.flow import flow
from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
)
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATASET_ID = constants.DATASET_ID.value
ALL_TABLES = constants.ALL_TABLES.value

# BD Pro rolling window: the four time-series tables paywall their most recent
# 6 months (part_bdpro), everything older is free. register_table_materialization_task
# recomputes free_end = source_end - free_lag, rewrites both DateTimeRanges, and
# re-issues the BigQuery Row Access Policies each run, so the window slides on
# its own. It requires BOTH a free and a pro Coverage to already exist on the
# table, or assert_coverage_topology raises — the pro Coverage is created once
# before the pipeline is first armed. The dimension tables stay fully free and
# keep their onboarding coverage, so they are not re-registered here.
_COVERAGE = {
    "votacao": PartBdpro(
        date_column=DateOnly(col="data_sessao"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "votacao_parlamentar": PartBdpro(
        date_column=DateOnly(col="data_sessao"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "votacao_orientacao_bancada": PartBdpro(
        date_column=DateOnly(col="data_votacao"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "processo": PartBdpro(
        date_column=DateOnly(col="data_apresentacao"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
}


@flow(name="br_senado_dados_abertos", log_prints=True)
def br_senado_dados_abertos_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh all ten Senate tables and materialize them.

    Dimensions are rebuilt in full; the time-series tables refresh the recent
    window and append (replacing only those partitions). There is no source
    poll — the run always materializes.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, roll the BD Pro
            coverage window (re-issuing Row Access Policies) and commit the source
            update. No effect when ``materialize_to_prod`` is False.
        force_run: Accepted for parity with the other dataset flows; this flow has
            no source-poll gate, so it does not change behavior.
    """
    _ = force_run
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="votacao"
    )

    work_dir = tempfile.mkdtemp(prefix="br_senado_dados_abertos_")
    try:
        result = extract_clean(
            work_dir=work_dir, prior_years=constants.REFRESH_PRIOR_YEARS.value
        )
        max_ds = result["max_data_sessao"]

        # Dev: upload staging (append = replace refreshed partitions) + dbt.
        for table in ALL_TABLES:
            upload_to_gcs(
                data_path=result[table],
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
                target="dev",
            )

        if not materialize_to_prod:
            return

        # Prod: upload staging + dbt.
        for table in ALL_TABLES:
            upload_to_gcs(
                data_path=result[table],
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
                target="prod",
            )

        if update_metadata:
            for table, coverage in _COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id="votacao",
                source_max_date=max_ds,
                env="prod",
                date_format="%Y-%m-%d",
            )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# Legislative activity updates on business days; refresh every morning (BRT).
br_senado_dados_abertos_flow.deploy_schedules = [
    Cron("0 8 * * *", timezone="America/Sao_Paulo")
]
br_senado_dados_abertos_flow.job_variables = {"memory": "4Gi"}
