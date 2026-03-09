from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.crawler.bcb.tasks import (
    create_load_dictionary,
    download_table,
    search_sicor_links,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="BD template - BR_BCB_SICOR",
    code_owners=[
        "Gabriel Pisa",
    ],
) as br_bcb_sicor_template:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_bcb_sicor", required=True)
    table_id = Parameter(
        "table_id", default="microdados_operacao", required=True
    )
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    append_overwrite = Parameter("append_overwrite", default="append")

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    download_links = search_sicor_links()
    download_table = download_table(download_links, table_id=table_id)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=download_table,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=append_overwrite,
        upstream_tasks=[download_table],
    )

    wait_for_materialization = run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        disable_elementary=True,
        upstream_tasks=[wait_upload_table],
    )

    with case(materialize_after_dump, True):
        wait_upload_prod = create_table_prod_gcs_and_run_dbt(
            data_path=download_table,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode=append_overwrite,
            upstream_tasks=[wait_for_materialization],
        )


br_bcb_sicor_template.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_bcb_sicor_template.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


with Flow(
    name="br_bcb_sicor.dicionario",
    code_owners=[
        "Gabriel Pisa",
    ],
) as br_bcb_sicor_dicionario:
    dataset_id = Parameter("dataset_id", default="br_bcb_sicor", required=True)
    table_id = Parameter("table_id", default="dicionario", required=True)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    dicionario_filepath = create_load_dictionary()

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=dicionario_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        upstream_tasks=[dicionario_filepath],
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
            data_path=dicionario_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="overwrite",
            upstream_tasks=[wait_for_materialization],
        )


br_bcb_sicor_template.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_bcb_sicor_template.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
