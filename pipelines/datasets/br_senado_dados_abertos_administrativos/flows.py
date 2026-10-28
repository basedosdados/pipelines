"""Flows de br_senado_dados_abertos_administrativos — Prefect 3."""

import datetime as dt
import shutil
import tempfile

from prefect.schedules import Cron

from pipelines.datasets.br_senado_dados_abertos_administrativos.constants import (
    TABLES_CONSTANTS,
    constants,
)
from pipelines.datasets.br_senado_dados_abertos_administrativos.tasks import (
    extract_and_clean,
)
from pipelines.utils.flow import flow
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


def _run(
    sub_resources: bool,
    materialize_to_prod: bool,
    update_metadata: bool,
) -> None:
    """Extrai a janela, sobe para a staging, roda o dbt e atualiza os metadados.

    Args:
        sub_resources: True inclui as tabelas filhas das contratações.
        materialize_to_prod: True grava em prod; False só em dev.
        update_metadata: True atualiza a cobertura das tabelas e o `Update` da
            fonte; só tem efeito com `materialize_to_prod`.
    """
    dataset_id = constants.DATASET_ID.value
    rename_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=constants.ANCHOR_TABLE.value,
    )

    bucket = "basedosdados" if materialize_to_prod else "basedosdados-dev"
    target = "prod" if materialize_to_prod else "dev"

    work_dir = tempfile.mkdtemp(prefix="br_senado_adm_")
    try:
        output_dir = f"{work_dir}/output"
        current_year = dt.date.today().year
        years = list(
            range(
                current_year - constants.REFRESH_PRIOR_YEARS.value,
                current_year + 1,
            )
        )
        result = extract_and_clean(
            output_dir=output_dir,
            sub_resources=sub_resources,
            years=years,
        )
        counts = result["counts"]

        tables = [
            table_id
            for table_id in result["tables"]
            if counts.get(table_id, 0) > 0
            and table_id not in constants.STATIC_TABLES.value
        ]
        empty = [
            table_id
            for table_id in result["tables"]
            if counts.get(table_id, 0) == 0
        ]
        if empty:
            print(
                f"Skipping {len(empty)} table(s) empty for this window: {empty}"
            )
        print("Skipping dicionario (static reference; not refreshed per run)")

        for table_id in tables:
            upload_to_gcs(
                data_path=f"{output_dir}/{table_id}",
                dataset_id=dataset_id,
                table_id=table_id,
                bucket_name=bucket,
                dump_mode="overwrite",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                dbt_command="run",
                target=target,
            )
        for table_id in tables:
            run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                dbt_command="test",
                target=target,
            )

        if not materialize_to_prod:
            return

        if update_metadata:
            for table_id in tables:
                coverage = TABLES_CONSTANTS[table_id].COVERAGE.value
                if coverage is None:
                    continue
                register_table_materialization_task(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
            commit_source_update_task(
                dataset_id=dataset_id,
                table_id=constants.ANCHOR_TABLE.value,
                source_max_date=result["extracted_at"],
                env="prod",
                date_format="%Y-%m-%d",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


@flow(name="br_senado_dados_abertos_administrativos_daily", log_prints=True)
def br_senado_dados_abertos_administrativos_daily_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Atualiza o conjunto sem as tabelas filhas das contratações."""
    _run(
        sub_resources=False,
        materialize_to_prod=materialize_to_prod,
        update_metadata=update_metadata,
    )


@flow(name="br_senado_dados_abertos_administrativos_weekly", log_prints=True)
def br_senado_dados_abertos_administrativos_weekly_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Atualiza o conjunto inteiro, com as tabelas filhas das contratações."""
    _run(
        sub_resources=True,
        materialize_to_prod=materialize_to_prod,
        update_metadata=update_metadata,
    )


br_senado_dados_abertos_administrativos_daily_flow.deploy_schedules = [
    Cron(constants.DAILY_CRON.value, timezone="America/Sao_Paulo")
]
br_senado_dados_abertos_administrativos_daily_flow.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
br_senado_dados_abertos_administrativos_weekly_flow.deploy_schedules = [
    Cron(constants.WEEKLY_CRON.value, timezone="America/Sao_Paulo")
]
br_senado_dados_abertos_administrativos_weekly_flow.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
