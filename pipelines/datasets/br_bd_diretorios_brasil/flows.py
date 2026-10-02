"""
Flows de br_bd_diretorios_brasil — Prefect 3.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_bd_diretorios_brasil.tasks import (
    build_municipio_lookup,
    clean_catalogo,
    download_catalogo,
    fetch_censo_escolar,
    fetch_diretorio_publicado,
    get_source_max_date,
)
from pipelines.utils.flow import flow
from pipelines.utils.metadata.domain import DateFormat, NonHistorical
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


@flow(
    name="br_bd_diretorios_brasil__escola",
    log_prints=True,
)
def br_bd_diretorios_brasil__escola(
    dataset_id: str = "br_bd_diretorios_brasil",
    table_id: str = "escola",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    """Atualiza o diretório de escolas a partir do Catálogo de Escolas do Inep."""
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_max_date = get_source_max_date()

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

    csv_path = download_catalogo()
    filepath = clean_catalogo(
        csv_path=csv_path,
        municipio_lookup=build_municipio_lookup(),
        diretorio_publicado=fetch_diretorio_publicado(),
        censo_escolar=fetch_censo_escolar(),
    )

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
        source_format="parquet",
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
        dump_mode="append",
        source_format="parquet",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        target=target,
    )

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=NonHistorical(),
            env="prod",
            bq_project="basedosdados",
        )


br_bd_diretorios_brasil__escola.deploy_schedules = [
    Cron("27 6 5 * *", timezone="America/Sao_Paulo")
]
