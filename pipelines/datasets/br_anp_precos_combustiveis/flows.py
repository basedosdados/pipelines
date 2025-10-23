"""
Flows for br_anp_precos_combustiveis
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_anp_precos_combustiveis.schedules import (
    every_week_anp_microdados,
)
from pipelines.datasets.br_anp_precos_combustiveis.tasks import (
    download_and_transform,
    get_data_source_anp_max_date,
    make_partitions,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="br_anp_precos_combustiveis.microdados", code_owners=["trick"]
) as anp_microdados:
    dataset_id = Parameter(
        "dataset_id", default="br_anp_precos_combustiveis", required=True
    )
    table_id = Parameter("table_id", default="microdados", required=True)

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
    update_metadata = Parameter(
        "update_metadata", default=True, required=False
    )

    data_source_max_date = get_data_source_anp_max_date()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=data_source_max_date,
        date_format="%Y-%m-%d",
        upstream_tasks=[data_source_max_date],
    )

    with case(dados_desatualizados, True):
        df = download_and_transform(upstream_tasks=[rename_flow_run])
        output_path = make_partitions(df=df, upstream_tasks=[df])

        wait_upload_table = create_table_dev_and_upload_to_gcs(
            data_path=output_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[output_path],
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
                ata_path=output_path,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                upstream_tasks=[wait_for_materialization],
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"date": "data_coleta"},
                    date_format="%Y-%m-%d",
                    coverage_type="part_bdpro",
                    time_delta={"weeks": 6},
                    bq_project="basedosdados",
                    upstream_tasks=[wait_upload_prod],
                )


anp_microdados.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
anp_microdados.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
anp_microdados.schedule = every_week_anp_microdados
