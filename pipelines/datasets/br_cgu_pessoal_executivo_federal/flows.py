"""
Flows for br_cgu_terceirizados
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_cgu_pessoal_executivo_federal.schedules import (
    every_four_months,
)
from pipelines.datasets.br_cgu_pessoal_executivo_federal.tasks import (
    clean_save_table,
    crawl,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

ROOT = "/tmp/data"
URL = "https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados"


with Flow(
    name="br_cgu_pessoal_executivo_federal.terceirizados",
    code_owners=["ath67"],
) as br_cgu_pess_exec_fed_terc:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_cgu_pessoal_executivo_federal", required=True
    )
    table_id = Parameter("table_id", default="terceirizados", required=True)
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    crawl_urls, temporal_coverage = crawl(URL)
    filepath = clean_save_table(ROOT, crawl_urls)

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        upstream_tasks=[filepath],
    )

    wait_for_materialization = run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        target=target,
        dbt_alias=dbt_alias,
        upstream_tasks=[wait_upload_table],
    )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
    #         {"temporal_coverage": [temporal_coverage]},
    #     ],
    #     upstream_tasks=[temporal_coverage],
    # )

    with case(materialize_after_dump, True):
        create_table_prod_gcs_and_run_dbt(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="overwrite",
            upstream_tasks=[wait_for_materialization],
        )


br_cgu_pess_exec_fed_terc.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_pess_exec_fed_terc.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cgu_pess_exec_fed_terc.schedule = every_four_months
