"""
Shared run logic for br_rf_cnpj — Prefect 3.
"""

from pipelines.crawler.rf_cnpj.constants import constants as constants_cnpj
from pipelines.crawler.rf_cnpj.tasks import get_data_source_max_date, main
from pipelines.utils.metadata.domain import (
    AllBdpro,
    DateFormat,
    DateOnly,
    NonHistorical,
    PartBdpro,
    YearMonth,
)
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    download_data_to_gcs,
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


def _run_rf_cnpj(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
    chunk_size: int = 100000,
    folder_date: str | None = None,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    tabelas = constants_cnpj.TABLES.value[table_id]

    folder_date, last_modified_date = get_data_source_max_date(folder_date)

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=folder_date,
            env="prod",
            date_format="%Y-%m",
        )
        if not has_new_data:
            print(f"Não há atualizações para a tabela {tabelas}!")
            return

    output_filepath = main(
        tables=tabelas,
        folder_date=folder_date,
        last_modified_date=last_modified_date,
        chunk_size=chunk_size,
    )

    upload_to_gcs(
        data_path=output_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target="dev",
    )

    if not materialize_after_dump:
        return

    upload_to_gcs(
        data_path=output_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="append",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target=target,
    )

    if update_metadata:
        if table_id == "simples" or table_id == "dicionario":
            # historical_database=False (sem coluna de data confiável) → NonHistorical
            register_table_materialization_task(
                dataset_id=dataset_id,
                table_id=table_id,
                coverage=NonHistorical(),
                env="prod",
                bq_project="basedosdados",
            )
        else:
            register_table_materialization_task(
                dataset_id=dataset_id,
                table_id=table_id,
                coverage=PartBdpro(
                    date_column=YearMonth(year="ano", month="mes"),
                    date_format=DateFormat.YEAR_MONTH,
                ),
                env="prod",
                bq_project="basedosdados",
            )

        if folder_date is not None:
            commit_source_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=folder_date,
                env="prod",
                date_format="%Y-%m",
            )

    # estabelecimentos: atualiza diretório de empresas + download p/ GCS
    if table_id == "estabelecimentos":
        run_dbt(
            dataset_id="br_bd_diretorios_brasil",
            table_id="empresa",
            dbt_command="run/test",
            dbt_alias=dbt_alias,
            target=target,
        )
        download_data_to_gcs(
            dataset_id=dataset_id,
            table_id=table_id,
        )
        if update_metadata:
            register_table_materialization_task(
                dataset_id="br_bd_diretorios_brasil",
                table_id="empresa",
                coverage=AllBdpro(
                    date_column=DateOnly(col="data"),
                    date_format=DateFormat.YEAR_MD,
                ),
                env="prod",
                bq_project="basedosdados",
            )
