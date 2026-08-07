"""
Flow br_mp_pep — Prefect 3.
"""

import datetime

from prefect import flow

from pipelines.crawler.mp_pep.tasks import (
    clean_data,
    download_xlsx,
    is_up_to_date,
    make_partitions,
    scraper,
    setup_web_driver,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    PartBdpro,
    YearMonth,
)
from pipelines.utils.metadata.tasks import (
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


@flow(
    name="br_mp_pep__cargos_funcoes",
    log_prints=True,
)
def br_mp_pep__cargos_funcoes(
    dataset_id: str = "br_mp_pep",
    table_id: str = "cargos_funcoes",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    setup_web_driver()

    if not force_run and is_up_to_date():
        print("Tabela já está atualizada")
        return

    now = datetime.datetime.now()
    current_date = datetime.datetime(year=now.year, month=now.month, day=1)
    year_end = (current_date - datetime.timedelta(days=1)).year

    scraper_result = scraper(
        year_start=year_end, year_end=year_end, headless=True
    )
    download_xlsx(scraper_result)
    df = clean_data()
    output_filepath = make_partitions(df)

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
br_mp_pep__cargos_funcoes.deploy_schedules = [
    {"cron": "0 14 * * 3", "timezone": "America/Sao_Paulo"}
]
