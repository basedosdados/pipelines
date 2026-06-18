"""
Shared run logic for br_anatel_telefonia_movel — Prefect 3.
"""

from pipelines.crawler.anatel.telefonia_movel.tasks import (
    get_max_date_in_table_microdados,
    get_semester,
    get_year_full,
    join_tables_in_function,
    unzip,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    PartBdpro,
    YearMonth,
)
from pipelines.utils.metadata.tasks import (
    register_source_poll_task,
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
    dbt_alias: bool,
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

    if not force_run:
        is_outdated = register_source_poll_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=data_source_max_date,
            env="prod",
            date_format="%Y-%m",
        )
        if not is_outdated:
            print(f"Não há atualizações para a tabela {table_id}!")
            return

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
        dbt_alias=dbt_alias,
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
        dbt_alias=dbt_alias,
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
