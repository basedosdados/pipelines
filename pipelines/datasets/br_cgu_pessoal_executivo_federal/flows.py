"""
Flows para br_cgu_pessoal_executivo_federal — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.cgu_pessoal_executivo_federal.tasks import (
    clean_save_table,
    crawl,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

ROOT = "/tmp/data"
URL = "https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados"


@flow(
    name="br_cgu_pessoal_executivo_federal__terceirizados",
    log_prints=True,
)
def br_cgu_pessoal_executivo_federal__terceirizados(
    dataset_id: str = "br_cgu_pessoal_executivo_federal",
    table_id: str = "terceirizados",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    update_metadata: bool = False,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    crawl_urls, _ = crawl(URL)
    filepath = clean_save_table(ROOT, crawl_urls)

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="overwrite",
        source_format="csv",
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
        dump_mode="overwrite",
        source_format="csv",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target=target,
    )


br_cgu_pessoal_executivo_federal__terceirizados.deploy_schedules = [
    {"cron": "0 0 28 2/4 *", "timezone": "America/Sao_Paulo"}
]
