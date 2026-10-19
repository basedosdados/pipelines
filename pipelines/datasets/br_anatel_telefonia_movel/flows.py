"""Flows for br_anatel_telefonia_movel — Prefect 3."""

from prefect.schedules import Cron

from pipelines.datasets.br_anatel_telefonia_movel.tasks import (
    get_max_date_in_table_microdados,
    get_semester,
    get_year_full,
    join_tables_in_function,
    unzip,
)
from pipelines.utils.flow import flow
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


def _run_anatel_telefonia_movel(
    dataset_id: str,
    table_id: str,
    ano: int | None,
    semestre: int | None,
    materialize_after_dump: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    unzip()
    new_year = get_year_full(ano)
    new_semester = get_semester(semestre)

    data_source_max_date = get_max_date_in_table_microdados(
        table_id=table_id, ano=new_year, semestre=new_semester
    )

    has_new_data = poll_source_for_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=data_source_max_date,
        env="prod",
        date_format="%Y-%m",
        compare_against="coverage",
    )

    if not has_new_data and not force_run:
        print(f"Não há atualizações para a tabela {table_id}!")
        return

    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=data_source_max_date,
        env="prod",
        date_format="%Y-%m",
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    filepath = join_tables_in_function(
        table_id=table_id, ano=new_year, semestre=new_semester
    )

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
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


def _anatel_tm_flow(table_id: str, cron: str):
    @flow(
        name=f"br_anatel_telefonia_movel__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_anatel_telefonia_movel",
        table_id: str = table_id,
        ano: int | None = None,
        semestre: int | None = None,
        materialize_after_dump: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        _run_anatel_telefonia_movel(
            dataset_id=dataset_id,
            table_id=table_id,
            ano=ano,
            semestre=semestre,
            materialize_after_dump=materialize_after_dump,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
        )

    _flow.deploy_schedules = [Cron(cron, timezone="America/Sao_Paulo")]
    _flow.job_variables = {"memory_limit": "8Gi", "memory_request": "2Gi"}
    return _flow


br_anatel_telefonia_movel__microdados = _anatel_tm_flow(
    table_id="microdados", cron="30 16 * * 1"
)
br_anatel_telefonia_movel__densidade_municipio = _anatel_tm_flow(
    table_id="densidade_municipio", cron="30 16 * * 2"
)
br_anatel_telefonia_movel__densidade_uf = _anatel_tm_flow(
    table_id="densidade_uf", cron="30 16 * * 3"
)
br_anatel_telefonia_movel__densidade_brasil = _anatel_tm_flow(
    table_id="densidade_brasil", cron="30 16 * * 4"
)
