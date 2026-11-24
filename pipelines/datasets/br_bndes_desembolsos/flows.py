"""
Flows de br_bndes_desembolsos — Prefect 3.
"""

from collections.abc import Callable
from pathlib import Path

from prefect.schedules import Cron

from pipelines.datasets.br_bndes_desembolsos.constants import constants
from pipelines.datasets.br_bndes_desembolsos.tasks import (
    clean_mapeamento_bndes_cnae,
    clean_mensal,
    download_table,
    get_source_last_modified,
)
from pipelines.utils.flow import flow
from pipelines.utils.metadata.domain import DateFormat
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


def _run_br_bndes_desembolsos(
    dataset_id: str,
    table_id: str,
    resource_id: str,
    path: str,
    csv_filename: str,
    clean: Callable[[Path], Path],
    materialize_after_dump: bool,
    update_metadata: bool,
    force_run: bool,
) -> None:
    """Atualiza uma tabela do conjunto quando o BNDES republica o recurso.

    Args:
        dataset_id: Id do conjunto.
        table_id: Id da tabela.
        resource_id: Id do recurso da tabela no CKAN.
        path: Diretório de trabalho da tabela.
        csv_filename: Nome com que o CSV é gravado em `input/`.
        clean: Task que limpa o CSV e devolve o diretório para o upload.
        materialize_after_dump: Se sobe a tabela para prod depois de dev.
        update_metadata: Se registra a materialização no backend.
        force_run: Se carrega mesmo sem republicação na fonte.
    """
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_max_date = get_source_last_modified(resource_id=resource_id)

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_max_date,
            env="prod",
            date_format=DateFormat.YEAR_MD,
            compare_against="table_update",
        )
        if not has_new_data:
            print(f"Não há atualizações para a tabela {table_id}!")
            return

    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=DateFormat.YEAR_MD,
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    csv_path = download_table(
        resource_id=resource_id, path=path, filename=csv_filename
    )
    filepath = clean(csv_path)

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="overwrite",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        target="dev",
    )

    if not materialize_after_dump:
        return

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="overwrite",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        target="prod",
    )

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            env="prod",
            bq_project="basedosdados",
        )


@flow(
    name="br_bndes_desembolsos__mensal",
    log_prints=True,
)
def br_bndes_desembolsos__mensal(
    dataset_id: str = "br_bndes_desembolsos",
    table_id: str = "mensal",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Atualiza a mensal quando o BNDES republica o recurso."""
    _run_br_bndes_desembolsos(
        dataset_id=dataset_id,
        table_id=table_id,
        resource_id=constants.MENSAL_CKAN_RESOURCE_ID.value,
        path=constants.MENSAL_PATH.value,
        csv_filename=constants.MENSAL_CSV_FILENAME.value,
        clean=clean_mensal,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        force_run=force_run,
    )


br_bndes_desembolsos__mensal.deploy_schedules = [
    Cron("35 2 * * 1", timezone="America/Sao_Paulo")
]


@flow(
    name="br_bndes_desembolsos__mapeamento_bndes_cnae",
    log_prints=True,
)
def br_bndes_desembolsos__mapeamento_bndes_cnae(
    dataset_id: str = "br_bndes_desembolsos",
    table_id: str = "mapeamento_bndes_cnae",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Atualiza o mapeamento_bndes_cnae quando o BNDES republica o recurso."""
    _run_br_bndes_desembolsos(
        dataset_id=dataset_id,
        table_id=table_id,
        resource_id=constants.MAPEAMENTO_BNDES_CNAE_CKAN_RESOURCE_ID.value,
        path=constants.MAPEAMENTO_BNDES_CNAE_PATH.value,
        csv_filename=constants.MAPEAMENTO_BNDES_CNAE_CSV_FILENAME.value,
        clean=clean_mapeamento_bndes_cnae,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        force_run=force_run,
    )


br_bndes_desembolsos__mapeamento_bndes_cnae.deploy_schedules = [
    Cron("40 2 * * 1", timezone="America/Sao_Paulo")
]
