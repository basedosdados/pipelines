"""
Flows for br_rf_cno
"""

from prefect import Parameter, case, unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_rf_cno.constants import (
    constants as br_rf_cno_constants,
)
from pipelines.datasets.br_rf_cno.schedules import schedule_br_rf_cno
from pipelines.datasets.br_rf_cno.tasks import (
    check_need_for_update,
    crawl_cno,
    create_parameters_list,
    list_files,
    process_file,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    download_data_to_gcs,
    log_task,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="br_rf_cno.tables", code_owners=["Gabriel Pisa"]
) as br_rf_cno_tables:
    dataset_id = Parameter("dataset_id", default="br_rf_cno", required=True)
    table_id = Parameter("table_id", default="microdados", required=True)
    table_ids = Parameter(
        "table_ids",
        default=["microdados", "areas", "cnaes", "vinculos"],
        required=False,
    )

    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )

    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    last_update_original_source = check_need_for_update(
        url=br_rf_cno_constants.URL_FTP.value
    )

    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=last_update_original_source,
        date_format="%Y-%m-%d",
        upstream_tasks=[last_update_original_source],
    )

    with case(check_if_outdated, False):
        log_task(f"Não há atualizações para a tabela de {table_id}!!")

    with case(check_if_outdated, True):
        log_task("Existem atualizações! A run será inciada")

        data = crawl_cno(
            root="input",
            url=br_rf_cno_constants.URL.value,
            upstream_tasks=[check_if_outdated, last_update_original_source],
        )

        files = list_files(
            input_dir="input",
            upstream_tasks=[data],
        )

        paths = process_file.map(
            files,
            input_dir=unmapped("input"),
            output_dir=unmapped("output"),
            partition_date=unmapped(last_update_original_source),
            chunksize=unmapped(100000),
            upstream_tasks=[unmapped(files)],
        )

        # 3. subir tabelas para o Storage e materilizar no BQ usando map
        wait_upload_table = create_table_dev_and_upload_to_gcs.map(
            data_path=paths,
            dataset_id=unmapped(dataset_id),
            table_id=table_ids,
            dump_mode=unmapped("append"),
            source_format=unmapped("parquet"),
            upstream_tasks=[
                unmapped(files)
            ],  # https://github.com/PrefectHQ/prefect/issues/2752
        )

        dbt_parameters = create_parameters_list(
            dataset_id=dataset_id,
            table_ids=table_ids,
            target=target,
            dbt_alias=dbt_alias,
            download_csv_file=True,
            dbt_command="run",
            disable_elementary=True,
            upstream_tasks=[unmapped(wait_upload_table)],
        )

        with case(materialize_after_dump, True):
            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                target=target,
                dbt_alias=dbt_alias,
                upstream_tasks=[dbt_parameters],
            )

            wait_for_dowload_data_to_gcs = download_data_to_gcs(
                dataset_id=dataset_id,
                table_id=table_id,
                upstream_tasks=[wait_for_materialization],
            )

            with case(update_metadata, True):
                update_django_metadata.map(
                    dataset_id=unmapped(dataset_id),
                    table_id=table_ids,
                    date_column_name=unmapped({"date": "data_extracao"}),
                    date_format=unmapped("%Y-%m-%d"),
                    coverage_type=unmapped("part_bdpro"),
                    time_delta=unmapped({"months": 6}),
                    prefect_mode=unmapped(target),
                    bq_project=unmapped("basedosdados"),
                    upstream_tasks=[unmapped(wait_for_dowload_data_to_gcs)],
                )


br_rf_cno_tables.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_rf_cno_tables.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    memory_limit="4Gi",
    memory_request="1Gi",
    cpu_limit=1,
)
br_rf_cno_tables.schedule = schedule_br_rf_cno
