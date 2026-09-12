"""
Flow br_ans_beneficiario__informacao_consolidada — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.ans_beneficiario.tasks import (
    crawler_ans,
    extract_links_and_dates,
    files_to_download,
    get_file_max_date,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    PartBdpro,
    YearMonth,
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


@flow(
    name="br_ans_beneficiario__informacao_consolidada",
    log_prints=True,
)
def br_ans_beneficiario__informacao_consolidada(
    dataset_id: str = "br_ans_beneficiario",
    table_id: str = "informacao_consolidada",
    url: str = "https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios-024/",
    year: str | None = None,
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    links_and_dates = extract_links_and_dates(url=url)
    file_last_date = get_file_max_date(df=links_and_dates)

    if force_run:
        files = files_to_download(df=links_and_dates, year=year)
    else:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=file_last_date,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data:
            print(f"Não há atualizações para a tabela {table_id}!")
            return
        files = files_to_download(df=links_and_dates, year=None)

    # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
    # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
    # novo publicado, mesmo que a tabela não tenha sido atualizada.
    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=file_last_date,
        env="prod",
        date_format="%Y-%m",
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    if not files:
        print("Nenhum arquivo para baixar.")
        return

    output_filepath = crawler_ans(files=files)

    # crawler_ans -> parquet_partition grava .parquet. Sem declarar o
    # formato, o dump_header chamado por upload_to_gcs procura .csv (default)
    # e não encontra nada.
    upload_to_gcs(
        data_path=output_filepath,
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
        data_path=output_filepath,
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
        # R4-normalização: o legado usava date_format="%Y-%m-%d" com colunas
        # ano/mes (emitia um endDay/startDay=1 espúrio). O domínio refatorado
        # força YearMonth↔YEAR_MONTH, descartando o dia — granularidade correta
        # para dado mensal. Valores ano/mês idênticos ao legado; free_lag default
        # (months=6) reproduz time_delta={"months":6}.
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


# pyrefly: ignore [missing-attribute]
br_ans_beneficiario__informacao_consolidada.deploy_schedules = [
    {"cron": "0 21 * * *", "timezone": "America/Sao_Paulo"}
]
# Pico medido em produção após otimizar parquet_partition (category dtype +
# del/gc.collect() por estado): ~1.78Gi. ~1.7x de margem sobre esse valor.
# pyrefly: ignore [missing-attribute]
br_ans_beneficiario__informacao_consolidada.job_variables = {"memory": "3Gi"}
