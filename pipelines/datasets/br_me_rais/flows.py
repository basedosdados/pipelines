"""
Flows for br_me_rais
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_me_rais.tasks import (
    build_partitions,
    build_table_paths,
    crawl_rais_ftp,
    generate_year_range,
    get_source_last_year,
    get_table_last_year,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    "br_me_rais.microdados_estabelecimentos", code_owners=["equipe_dados"]
) as br_me_rais_microdados_estabelecimentos:
    dataset_id = Parameter("dataset_id", default="br_me_rais", required=True)
    table_id = Parameter(
        "table_id", default="microdados_estabelecimentos", required=True
    )
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
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

    source_last_year = get_source_last_year(upstream_tasks=[table_id])
    table_last_year = get_table_last_year(
        dataset_id, table_id, upstream_tasks=[source_last_year]
    )
    input_dir, output_dir = build_table_paths(
        table_id, upstream_tasks=[table_last_year]
    )
    years = generate_year_range(
        table_last_year, source_last_year, upstream_tasks=[table_last_year]
    )

    failed_crawls = crawl_rais_ftp.map(
        year=years,
        table_id=table_id,
        input_dir=input_dir,
        upstream_tasks=[input_dir],
    )

    filepath = build_partitions.map(
        table_id=table_id,
        year=years,
        input_dir=input_dir,
        output_dir=output_dir,
        upstream_tasks=[failed_crawls],
    )

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=output_dir,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[filepath],
    )

    wait_for_materialization = run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        upstream_tasks=[wait_upload_table],
    )

    with case(materialize_after_dump, True):
        wait_upload_prod = create_table_prod_gcs_and_run_dbt(
            data_path=output_dir,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[wait_for_materialization],
        )

        with case(update_metadata, True):
            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"year": "ano"},
                date_format="%Y",
                coverage_type="part_bdpro",
                time_delta={"years": 1},
                bq_project="basedosdados",
                upstream_tasks=[wait_upload_prod],
            )

br_me_rais_microdados_estabelecimentos.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_me_rais_microdados_estabelecimentos.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


with Flow(
    "br_me_rais.microdados_vinculos", code_owners=["equipe_dados"]
) as br_me_rais_microdados_vinculos:
    dataset_id = Parameter("dataset_id", default="br_me_rais", required=True)
    table_id = Parameter(
        "table_id", default="microdados_vinculos", required=True
    )
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
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

    source_last_year = get_source_last_year(upstream_tasks=[table_id])
    table_last_year = get_table_last_year(
        dataset_id, table_id, upstream_tasks=[source_last_year]
    )
    input_dir, output_dir = build_table_paths(
        table_id, upstream_tasks=[table_last_year]
    )
    years = generate_year_range(
        table_last_year, source_last_year, upstream_tasks=[table_last_year]
    )

    failed_crawls = crawl_rais_ftp.map(
        year=years,
        table_id=table_id,
        input_dir=input_dir,
        upstream_tasks=[input_dir],
    )

    filepath = build_partitions.map(
        table_id=table_id,
        year=years,
        input_dir=input_dir,
        output_dir=output_dir,
        upstream_tasks=[failed_crawls],
    )

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=output_dir,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[filepath],
    )

    wait_for_materialization = run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        upstream_tasks=[wait_upload_table],
    )

    with case(materialize_after_dump, True):
        wait_upload_prod = create_table_prod_gcs_and_run_dbt(
            data_path=output_dir,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[wait_for_materialization],
        )

        with case(update_metadata, True):
            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"year": "ano"},
                date_format="%Y",
                coverage_type="part_bdpro",
                time_delta={"years": 1},
                bq_project="basedosdados",
                upstream_tasks=[wait_upload_prod],
            )

br_me_rais_microdados_vinculos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_me_rais_microdados_vinculos.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
