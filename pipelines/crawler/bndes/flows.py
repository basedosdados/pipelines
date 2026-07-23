"""
Shared run logic for br_bndes_operacoes_contratadas — Prefect 3.

Poll DEFERIDO: poll_source_for_update_task detecta novidade (grava so o Poll);
o Update so e gravado por commit_source_update_task NO FIM, apos materializar.
Isso evita adiantar o Update e travar runs futuras se o flow falhar no meio.

Dois conceitos de data (nao confundir):
- RawDataSource Update (poll/commit) = data de publicacao (CKAN last_modified),
  granularidade %Y-%m-%d.
- Coverage da tabela (materializacao) = anual (coluna `ano`), dado publico ->
  AllFree + YearOnly + DateFormat.YEAR.
"""

from pipelines.crawler.bndes.tasks import (
    clean_and_partition,
    download_source_csv,
    get_source_max_date,
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

SOURCE_DATE_FORMAT = (
    "%Y-%m-%d"  # granularidade do last_modified (RawDataSource Update)
)


def _run_operacoes_indiretas_automaticas(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    """
    Orquestra o crawler: poll deferido -> download -> clean -> upload -> dbt.

    Args:
        dataset_id (str): ID do dataset no GCP/BigQuery.
        table_id (str): ID da tabela no GCP/BigQuery.
        materialize_after_dump (bool): se False, para apos o dbt em dev (nao toca prod).
        dbt_alias (bool): passa adiante para run_dbt.
        update_metadata (bool): se True, registra materializacao e commita o Update da fonte.
        target (str): target do dbt na etapa de prod.
        force_run (bool): ignora o early-return quando nao ha novidade.
    """

    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_max_date = get_source_max_date()

    has_new_data = poll_source_for_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=SOURCE_DATE_FORMAT,
    )

    if not has_new_data and not force_run:
        return

    csv_path = download_source_csv()

    output_dir = clean_and_partition(csv_path=csv_path)

    upload_to_gcs(
        data_path=output_dir,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="overwrite",
        source_format="parquet",
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
        data_path=output_dir,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="overwrite",
        source_format="parquet",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target=target,
    )

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=AllFree(
                date_column=YearOnly(col="ano"), date_format=DateFormat.YEAR
            ),
            env="prod",
            bq_project="basedosdados",
        )

        commit_source_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_max_date,
            env="prod",
            date_format=SOURCE_DATE_FORMAT,
        )
