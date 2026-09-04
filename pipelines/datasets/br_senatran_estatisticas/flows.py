"""
Flows para br_senatran_estatisticas — Prefect 3.
"""

from pathlib import Path

from prefect import flow

from pipelines.datasets.br_senatran_estatisticas.constants import (
    constants as senatran_constants,
)
from pipelines.datasets.br_senatran_estatisticas.tasks import (
    build_paths,
    crawl_task,
    get_breakdown_months_task,
    get_desired_file_task,
    get_latest_date_task,
    treat_breakdown_task,
    treat_municipio_tipo_task,
    treat_uf_tipo_task,
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


def _run_senatran(
    *,
    dataset_id: str,
    table_id: str,
    filetype: str,
    treat_task,
    materialize_after_dump: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
    backfill_start: str | None = None,
) -> None:
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    input_dir, output_dir = build_paths()

    (
        source_available_dates,
        _source_available_dates_str,
        _source_first_available_date,
        source_first_available_date_str,
    ) = get_latest_date_task(
        table_id=table_id,
        dataset_id=dataset_id,
        input_dir=input_dir,
        backfill_start=backfill_start,
    )

    print(f"First available date: {source_first_available_date_str}")

    if not force_run and not backfill_start:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_first_available_date_str,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data:
            print("No new data to be downloaded")
            return

    # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
    # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
    # novo publicado, mesmo que a tabela não tenha sido atualizada.
    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_first_available_date_str,
        env="prod",
        date_format="%Y-%m",
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    print("Updates found! The run will be started.")
    parquet_outputs = []
    for source_max_date in source_available_dates:
        crawl_task(
            source_max_date=source_max_date,
            table_id=table_id,
            temp_dir=input_dir,
        )
        # pyrefly: ignore [no-matching-overload]
        desired_file = get_desired_file_task(
            source_max_date=source_max_date,
            download_directory=input_dir,
            table_id=table_id,
            filetype=filetype,
        )
        parquet_outputs.append(
            treat_task(file=desired_file, output_dir=output_dir)
        )

    filepath = parquet_outputs[0]

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

    # pyrefly: ignore [no-matching-overload]
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
            coverage=PartBdpro(
                date_column=YearMonth(year="ano", month="mes"),
                date_format=DateFormat.YEAR_MONTH,
            ),
            env="prod",
            bq_project="basedosdados",
        )


@flow(
    name="br_senatran_estatisticas__uf_tipo",
    log_prints=True,
)
def br_senatran_estatisticas__uf_tipo(
    dataset_id: str = "br_senatran_estatisticas",
    table_id: str = "uf_tipo",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
    backfill_start: str | None = None,
) -> None:
    _run_senatran(
        dataset_id=dataset_id,
        table_id=table_id,
        filetype=senatran_constants.UF_TIPO_BASIC_FILENAME.value,
        treat_task=treat_uf_tipo_task,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
        backfill_start=backfill_start,
    )


@flow(
    name="br_senatran_estatisticas__municipio_tipo",
    log_prints=True,
)
def br_senatran_estatisticas__municipio_tipo(
    dataset_id: str = "br_senatran_estatisticas",
    table_id: str = "municipio_tipo",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
    backfill_start: str | None = None,
) -> None:
    _run_senatran(
        dataset_id=dataset_id,
        table_id=table_id,
        filetype=senatran_constants.MUNIC_TIPO_BASIC_FILENAME.value,
        treat_task=treat_municipio_tipo_task,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
        backfill_start=backfill_start,
    )


# pyrefly: ignore [missing-attribute]
br_senatran_estatisticas__uf_tipo.deploy_schedules = [
    {"cron": "0 21 10-30 * *", "timezone": "America/Sao_Paulo"}
]
# pyrefly: ignore [missing-attribute]
br_senatran_estatisticas__municipio_tipo.deploy_schedules = [
    {"cron": "20 21 10-30 * *", "timezone": "America/Sao_Paulo"}
]


def _run_breakdown(
    *,
    dataset_id: str,
    table_id: str,
    layout_key: str,
    materialize_after_dump: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
    backfill_start: str | None,
) -> None:
    """Executa um recorte da frota (combustível, cor, potência…).

    Difere de ``_run_senatran`` só no par baixar/limpar: os recortes vêm de um
    único XLSX por mês, já em formato longo, então não passam pelo
    ``crawl_task``/``get_desired_file_task`` do par município/UF x tipo.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    input_dir, output_dir = build_paths()

    datas, urls = get_breakdown_months_task(
        table_id=table_id,
        dataset_id=dataset_id,
        layout_key=layout_key,
        backfill_start=backfill_start,
    )

    if not datas:
        print("Não há novas atualizações na fonte original")
        return

    source_max_date_str = max(datas).strftime("%Y-%m")

    if not force_run and not backfill_start:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_max_date_str,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data:
            print("Não há novas atualizações na fonte original")
            return

    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date_str,
        env="prod",
        date_format="%Y-%m",
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    filepath: Path | None = None
    for data, url in zip(datas, urls, strict=True):
        filepath = treat_breakdown_task(
            url=url,
            year=data.year,
            month=data.month,
            layout_key=layout_key,
            input_dir=input_dir,
            output_dir=output_dir,
        )

    if filepath is None:
        raise RuntimeError("Nenhum mês foi processado — nada a subir")

    # pyrefly: ignore [no-matching-overload]
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

    # pyrefly: ignore [no-matching-overload]
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
            coverage=PartBdpro(
                date_column=YearMonth(year="ano", month="mes"),
                date_format=DateFormat.YEAR_MONTH,
            ),
            env="prod",
            bq_project="basedosdados",
        )


@flow(
    name="br_senatran_estatisticas__municipio_combustivel",
    log_prints=True,
)
def br_senatran_estatisticas__municipio_combustivel(
    dataset_id: str = "br_senatran_estatisticas",
    table_id: str = "municipio_combustivel",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
    backfill_start: str | None = None,
) -> None:
    _run_breakdown(
        dataset_id=dataset_id,
        table_id=table_id,
        layout_key="municipio_combustivel",
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
        backfill_start=backfill_start,
    )


# pyrefly: ignore [missing-attribute]
br_senatran_estatisticas__municipio_combustivel.deploy_schedules = [
    {"cron": "40 21 10-30 * *", "timezone": "America/Sao_Paulo"}
]


@flow(
    name="br_senatran_estatisticas__municipio_cor",
    log_prints=True,
)
def br_senatran_estatisticas__municipio_cor(
    dataset_id: str = "br_senatran_estatisticas",
    table_id: str = "municipio_cor",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
    backfill_start: str | None = None,
) -> None:
    _run_breakdown(
        dataset_id=dataset_id,
        table_id=table_id,
        layout_key="municipio_cor",
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
        backfill_start=backfill_start,
    )


# pyrefly: ignore [missing-attribute]
br_senatran_estatisticas__municipio_cor.deploy_schedules = [
    {"cron": "5 22 10-30 * *", "timezone": "America/Sao_Paulo"}
]


@flow(
    name="br_senatran_estatisticas__municipio_potencia",
    log_prints=True,
)
def br_senatran_estatisticas__municipio_potencia(
    dataset_id: str = "br_senatran_estatisticas",
    table_id: str = "municipio_potencia",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
    backfill_start: str | None = None,
) -> None:
    _run_breakdown(
        dataset_id=dataset_id,
        table_id=table_id,
        layout_key="municipio_potencia",
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
        backfill_start=backfill_start,
    )


# pyrefly: ignore [missing-attribute]
br_senatran_estatisticas__municipio_potencia.deploy_schedules = [
    {"cron": "15 22 10-30 * *", "timezone": "America/Sao_Paulo"}
]


@flow(
    name="br_senatran_estatisticas__municipio_restricao",
    log_prints=True,
)
def br_senatran_estatisticas__municipio_restricao(
    dataset_id: str = "br_senatran_estatisticas",
    table_id: str = "municipio_restricao",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
    backfill_start: str | None = None,
) -> None:
    _run_breakdown(
        dataset_id=dataset_id,
        table_id=table_id,
        layout_key="municipio_restricao",
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
        backfill_start=backfill_start,
    )


# pyrefly: ignore [missing-attribute]
br_senatran_estatisticas__municipio_restricao.deploy_schedules = [
    {"cron": "25 22 10-30 * *", "timezone": "America/Sao_Paulo"}
]


@flow(
    name="br_senatran_estatisticas__municipio_cep",
    log_prints=True,
)
def br_senatran_estatisticas__municipio_cep(
    dataset_id: str = "br_senatran_estatisticas",
    table_id: str = "municipio_cep",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
    backfill_start: str | None = None,
) -> None:
    _run_breakdown(
        dataset_id=dataset_id,
        table_id=table_id,
        layout_key="municipio_cep",
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
        backfill_start=backfill_start,
    )


# pyrefly: ignore [missing-attribute]
br_senatran_estatisticas__municipio_cep.deploy_schedules = [
    {"cron": "35 22 10-30 * *", "timezone": "America/Sao_Paulo"}
]


@flow(
    name="br_senatran_estatisticas__municipio_ano_fabricacao_modelo",
    log_prints=True,
)
def br_senatran_estatisticas__municipio_ano_fabricacao_modelo(
    dataset_id: str = "br_senatran_estatisticas",
    table_id: str = "municipio_ano_fabricacao_modelo",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
    backfill_start: str | None = None,
) -> None:
    _run_breakdown(
        dataset_id=dataset_id,
        table_id=table_id,
        layout_key="municipio_ano_fabricacao_modelo",
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
        backfill_start=backfill_start,
    )


# pyrefly: ignore [missing-attribute]
br_senatran_estatisticas__municipio_ano_fabricacao_modelo.deploy_schedules = [
    {"cron": "45 22 10-30 * *", "timezone": "America/Sao_Paulo"}
]


@flow(
    name="br_senatran_estatisticas__municipio_tipo_especie_eixos",
    log_prints=True,
)
def br_senatran_estatisticas__municipio_tipo_especie_eixos(
    dataset_id: str = "br_senatran_estatisticas",
    table_id: str = "municipio_tipo_especie_eixos",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
    backfill_start: str | None = None,
) -> None:
    _run_breakdown(
        dataset_id=dataset_id,
        table_id=table_id,
        layout_key="municipio_tipo_especie_eixos",
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
        backfill_start=backfill_start,
    )


# pyrefly: ignore [missing-attribute]
br_senatran_estatisticas__municipio_tipo_especie_eixos.deploy_schedules = [
    {"cron": "55 22 10-30 * *", "timezone": "America/Sao_Paulo"}
]
