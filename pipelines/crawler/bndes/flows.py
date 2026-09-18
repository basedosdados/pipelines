"""
Shared run logic for br_bndes_operacoes_contratadas — Prefect 3.

Poll DEFERIDO: poll_source_for_update_task detecta novidade (grava so o Poll);
o commit_source_update_task grava o Update logo apos essa confirmacao, antes
de baixar/materializar. O poll nunca le RawDataSource.Update para decidir
nada (so le Coverage/Table.Update), entao adiantar esse valor nao trava runs
futuras — e da visibilidade de que havia dado novo publicado mesmo que o
flow falhe no meio (download, upload, dbt).

Dois conceitos de data (nao confundir):
- RawDataSource Update (poll/commit) = data de publicacao (CKAN last_modified),
  granularidade %Y-%m-%d.
- Coverage da tabela (materializacao) = anual (coluna `ano`), dado publico ->
  AllFree + YearOnly + DateFormat.YEAR.
"""

from pipelines.crawler.bndes.tasks import (
    clean_and_partition,
    clean_and_partition_administracao_publica,
    clean_and_partition_exportacao_bens,
    clean_and_partition_exportacao_servicos,
    download_administracao_publica_csv,
    download_exportacao_bens_csv,
    download_exportacao_servicos_csv,
    download_source_csv,
    get_source_max_date,
    get_source_max_date_administracao_publica,
    get_source_max_date_exportacao_bens,
    get_source_max_date_exportacao_servicos,
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


def _run_operacoes(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
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
        update_metadata (bool): se True, registra materializacao e commita o Update da fonte.
        target (str): target do dbt na etapa de prod.
        force_run (bool): ignora o early-return quando nao ha novidade.
    """

    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_max_date = get_source_max_date(table_id=table_id)

    has_new_data = poll_source_for_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=SOURCE_DATE_FORMAT,
        compare_against="table_update",
    )

    if not has_new_data and not force_run:
        return

    # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
    # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
    # novo publicado, mesmo que a tabela não tenha sido atualizada.
    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=SOURCE_DATE_FORMAT,
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    csv_path = download_source_csv(table_id=table_id)

    output_dir = clean_and_partition(csv_path=csv_path, table_id=table_id)

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


def _run_operacoes_exportacao_bens(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    update_metadata: bool,
    force_run: bool,
) -> None:
    """
    Orquestra o crawler da 3a tabela: poll deferido -> download -> clean -> upload -> dbt.

    Mesma receita das irmas, trocando so as tasks especificas da tabela.

    Args:
        dataset_id (str): ID do dataset no GCP/BigQuery.
        table_id (str): ID da tabela no GCP/BigQuery.
        materialize_after_dump (bool): se False, para apos o dbt em dev (nao toca prod).
        update_metadata (bool): se True, registra materializacao e commita o Update da fonte.
        force_run (bool): ignora o early-return quando nao ha novidade.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_max_date = get_source_max_date_exportacao_bens()

    has_new_data = poll_source_for_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=SOURCE_DATE_FORMAT,
        compare_against="table_update",
    )

    if not has_new_data and not force_run:
        return

    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=SOURCE_DATE_FORMAT,
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    csv_path = download_exportacao_bens_csv()

    output_dir = clean_and_partition_exportacao_bens(csv_path=csv_path)

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
        target="prod",
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


def _run_operacoes_exportacao_servicos(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    """
    Orquestra o crawler da 4a tabela: poll deferido -> download -> clean -> upload -> dbt.

    Mesma receita das irmas, trocando so as tasks especificas da tabela. A
    tabela e carga unica — a serie da fonte termina em 2015 e o flow nao tem
    cron —, entao na pratica so roda por disparo manual, com force_run.

    Args:
        dataset_id (str): ID do dataset no GCP/BigQuery.
        table_id (str): ID da tabela no GCP/BigQuery.
        materialize_after_dump (bool): se False, para apos o dbt em dev (nao toca prod).
        update_metadata (bool): se True, registra materializacao e commita o Update da fonte.
        target (str): target do dbt na etapa de prod.
        force_run (bool): ignora o early-return quando nao ha novidade.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_max_date = get_source_max_date_exportacao_servicos()

    has_new_data = poll_source_for_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=SOURCE_DATE_FORMAT,
        compare_against="table_update",
    )

    if not has_new_data and not force_run:
        return

    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=SOURCE_DATE_FORMAT,
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    csv_path = download_exportacao_servicos_csv()

    output_dir = clean_and_partition_exportacao_servicos(csv_path=csv_path)

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


def _run_operacoes_administracao_publica(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    """
    Orquestra o crawler da 2a tabela: poll deferido -> download -> clean -> upload -> dbt.

    Mesma receita de _run_operacoes_indiretas_automaticas: poll deferido — o
    poll_source_for_update_task grava so o Poll ao detectar novidade; o
    commit_source_update_task grava o Update da fonte logo apos essa
    confirmacao, antes de baixar/materializar prod.

    Args:
        dataset_id (str): ID do dataset no GCP/BigQuery.
        table_id (str): ID da tabela no GCP/BigQuery.
        materialize_after_dump (bool): se False, para apos o dbt em dev (nao toca prod).
        update_metadata (bool): se True, registra materializacao e commita o Update da fonte.
        target (str): target do dbt na etapa de prod.
        force_run (bool): ignora o early-return quando nao ha novidade.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_max_date = get_source_max_date_administracao_publica()

    has_new_data = poll_source_for_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=SOURCE_DATE_FORMAT,
        compare_against="table_update",
    )

    if not has_new_data and not force_run:
        return

    # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
    # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
    # novo publicado, mesmo que a tabela não tenha sido atualizada.
    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=SOURCE_DATE_FORMAT,
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    csv_path = download_administracao_publica_csv()

    output_dir = clean_and_partition_administracao_publica(csv_path=csv_path)

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
