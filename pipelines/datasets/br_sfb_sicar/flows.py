"""
Flow br_sfb_sicar — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.sfb_sicar.constants import Constants
from pipelines.crawler.sfb_sicar.tasks import (
    download_car,
    get_each_uf_release_date,
    unzip_to_parquet,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    PartBdpro,
)
from pipelines.utils.metadata.tasks import (
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

INPUTPATH = Constants.INPUT_PATH.value
OUTPUTPATH = Constants.OUTPUT_PATH.value
SIGLAS_UF = Constants.UF_SIGLAS.value


@flow(
    name="br_sfb_sicar__area_imovel",
    log_prints=True,
)
def br_sfb_sicar__area_imovel(
    dataset_id: str = "br_sfb_sicar",
    table_id: str = "area_imovel",
    materialize_after_dump: bool = True,
    dbt_alias: bool = False,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    # Loop sequencial sobre UFs (substitui .map() do Prefect 0)
    for sigla_uf in SIGLAS_UF:
        download_car(
            inputpath=INPUTPATH,
            outputpath=OUTPUTPATH,
            sigla_uf=sigla_uf,
            polygon="AREA_IMOVEL",
        )

    uf_release_dates = get_each_uf_release_date()
    unzip_to_parquet(
        inputpath=INPUTPATH,
        outputpath=OUTPUTPATH,
        uf_relase_dates=uf_release_dates,
    )

    upload_to_gcs(
        data_path=OUTPUTPATH,
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
        dbt_alias=dbt_alias,
        target="dev",
    )

    if not materialize_after_dump:
        return

    upload_to_gcs(
        data_path=OUTPUTPATH,
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
        dbt_alias=dbt_alias,
        target=target,
    )

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=PartBdpro(
                date_column=DateOnly(col="data_extracao"),
                date_format=DateFormat.YEAR_MD,
            ),
            env="prod",
            bq_project="basedosdados",
        )


# pyrefly: ignore [missing-attribute]
br_sfb_sicar__area_imovel.deploy_schedules = [
    {"cron": "15 21 15 * *", "timezone": "America/Sao_Paulo"}
]
