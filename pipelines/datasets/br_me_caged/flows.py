"""
Flows for br_me_caged
"""

from prefect import Parameter, case, unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_me_caged.schedules import (
    every_month_movimentacao,
    every_month_movimentacao_excluida,
    every_month_movimentacao_fora_prazo,
)
from pipelines.datasets.br_me_caged.tasks import (
    build_partitions,
    build_table_paths,
    crawl_novo_caged_ftp,
    generate_yearmonth_range,
    get_source_last_date,
    get_table_last_date,
)

# pylint: disable=invalid-name
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

with Flow(
    "br_me_caged.microdados_movimentacao", code_owners=["Luiza"]
) as br_me_caged_microdados_movimentacao:
    dataset_id = Parameter("dataset_id", default="br_me_caged", required=True)
    table_id = Parameter(
        "table_id", default="microdados_movimentacao", required=True
    )
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    source_last_date = get_source_last_date(upstream_tasks=[table_id])

    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=source_last_date,
        date_format="%Y-%m",
        upstream_tasks=[source_last_date],
    )

    with case(check_if_outdated, False):
        log_task(f"No updates for table {table_id}!")

    with case(check_if_outdated, True):
        table_last_date = get_table_last_date(
            dataset_id, table_id, upstream_tasks=[check_if_outdated]
        )
        input_dir, output_dir = build_table_paths(
            table_id, upstream_tasks=[check_if_outdated]
        )
        yearmonths = generate_yearmonth_range(
            table_last_date,
            source_last_date,
            upstream_tasks=[table_last_date],
        )

        failed_crawls = crawl_novo_caged_ftp.map(
            yearmonths,
            unmapped(table_id),
            upstream_tasks=[unmapped(input_dir)],
        )

        log_download = log_task.map(
            failed_crawls,
            upstream_tasks=[failed_crawls],
        )

        filepath = build_partitions(
            table_id=table_id,
            table_output_dir=output_dir,
            upstream_tasks=[failed_crawls],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
            upstream_tasks=[filepath],
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
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

br_me_caged_microdados_movimentacao.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_me_caged_microdados_movimentacao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_me_caged_microdados_movimentacao.schedule = every_month_movimentacao

with Flow(
    "br_me_caged.microdados_movimentacao_excluida", code_owners=["Luiza"]
) as br_me_caged_microdados_movimentacao_excluida:
    dataset_id = Parameter("dataset_id", default="br_me_caged", required=True)
    table_id = Parameter(
        "table_id", default="microdados_movimentacao_excluida", required=True
    )
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    source_last_date = get_source_last_date(upstream_tasks=[table_id])

    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=source_last_date,
        date_format="%Y-%m",
        upstream_tasks=[source_last_date],
    )

    with case(check_if_outdated, False):
        log_task(f"No updates for table {table_id}!")

    with case(check_if_outdated, True):
        table_last_date = get_table_last_date(
            dataset_id, table_id, upstream_tasks=[check_if_outdated]
        )
        input_dir, output_dir = build_table_paths(
            table_id, upstream_tasks=[check_if_outdated]
        )
        yearmonths = generate_yearmonth_range(
            table_last_date,
            source_last_date,
            upstream_tasks=[table_last_date],
        )

        failed_crawls = crawl_novo_caged_ftp.map(
            yearmonths,
            unmapped(table_id),
            upstream_tasks=[unmapped(input_dir)],
        )

        log_download = log_task.map(
            failed_crawls,
            upstream_tasks=[failed_crawls],
        )

        filepath = build_partitions(
            table_id=table_id,
            table_output_dir=output_dir,
            upstream_tasks=[failed_crawls],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
            upstream_tasks=[filepath],
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
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

br_me_caged_microdados_movimentacao_excluida.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_me_caged_microdados_movimentacao_excluida.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_me_caged_microdados_movimentacao_excluida.schedule = (
    every_month_movimentacao_excluida
)

with Flow(
    "br_me_caged.microdados_movimentacao_fora_prazo", code_owners=["Luiza"]
) as br_me_caged_microdados_movimentacao_fora_prazo:
    dataset_id = Parameter("dataset_id", default="br_me_caged", required=True)
    table_id = Parameter(
        "table_id", default="microdados_movimentacao_fora_prazo", required=True
    )
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    target = Parameter("target", default="prod", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    source_last_date = get_source_last_date(upstream_tasks=[table_id])

    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=source_last_date,
        date_format="%Y-%m",
        upstream_tasks=[source_last_date],
    )

    with case(check_if_outdated, False):
        log_task(f"No updates for table {table_id}!")

    with case(check_if_outdated, True):
        table_last_date = get_table_last_date(
            dataset_id, table_id, upstream_tasks=[check_if_outdated]
        )
        input_dir, output_dir = build_table_paths(
            table_id, upstream_tasks=[check_if_outdated]
        )
        yearmonths = generate_yearmonth_range(
            table_last_date,
            source_last_date,
            upstream_tasks=[table_last_date],
        )

        failed_crawls = crawl_novo_caged_ftp.map(
            yearmonths,
            unmapped(table_id),
            upstream_tasks=[unmapped(input_dir)],
        )

        log_download = log_task.map(
            failed_crawls,
            upstream_tasks=[failed_crawls],
        )

        filepath = build_partitions(
            table_id=table_id,
            table_output_dir=output_dir,
            upstream_tasks=[failed_crawls],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
            upstream_tasks=[filepath],
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
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

br_me_caged_microdados_movimentacao_fora_prazo.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_me_caged_microdados_movimentacao_fora_prazo.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_me_caged_microdados_movimentacao_fora_prazo.schedule = (
    every_month_movimentacao_fora_prazo
)
