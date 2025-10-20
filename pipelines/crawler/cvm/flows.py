"""
Flows for br_cvm_fi

"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.crawler.cvm.tasks import (
    clean_cvm_data,
    download_unzip,
    extract_links_and_dates,
    generate_links_to_download,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    log_task,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(name="BD - Template CVM") as flow_cvm:
    # Parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )

    df, max_date = extract_links_and_dates(table_id, upstream_tasks=[table_id])

    log_task(f"Links e datas: {df}")

    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=max_date,
        date_format="%Y-%m-%d",
        date_type="last_update_date",
        upstream_tasks=[df],
    )

    with case(check_if_outdated, False):
        log_task(
            "A execução sera agendada para a próxima data definida na schedule"
        )

    with case(check_if_outdated, True):
        arquivos = generate_links_to_download(df=df, max_date=max_date)
        log_task(f"Arquivos: {arquivos}")

        input_filepath = download_unzip(
            table_id=table_id,
            files=arquivos,
            upstream_tasks=[arquivos],
        )
        output_filepath = clean_cvm_data(
            input_dir=input_filepath,
            table_id=table_id,
            config_key=table_id,
            upstream_tasks=[input_filepath],
        )

        rename_flow_run = rename_current_flow_run_dataset_table(
            prefix="Dump: ",
            dataset_id=dataset_id,
            table_id=table_id,
            wait=table_id,
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=output_filepath,
        )
        with case(materialize_after_dump, True):
            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                target=target,
                dbt_alias=dbt_alias,
                upstream_tasks=[wait_upload_table],
            )
            wait_for_dowload_data_to_gcs = download_data_to_gcs(
                dataset_id=dataset_id,
                table_id=table_id,
                upstream_tasks=[wait_for_materialization],
            )
            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"date": "data_competencia"},
                    date_format="%Y-%m-%d",
                    coverage_type="all_bdpro",
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )


flow_cvm.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_cvm.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
