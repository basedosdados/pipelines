"""
Flows for br_mp_pep_cargos_funcoes
"""

import datetime

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_mp_pep.schedules import every_month
from pipelines.datasets.br_mp_pep.tasks import (
    clean_data,
    download_xlsx,
    is_up_to_date,
    make_partitions,
    scraper,
    setup_web_driver,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    rename_current_flow_run_dataset_table,
    run_dbt,
)
from pipelines.utils.utils import log_task

with Flow(
    name="br_mp_pep.cargos_funcoes",
    code_owners=[
        "aspeddro",
    ],
) as datasets_br_mp_pep_cargos_funcoes_flow:
    dataset_id = Parameter("dataset_id", default="br_mp_pep", required=True)
    table_id = Parameter("table_id", default="cargos_funcoes", required=True)
    update_metadata = Parameter(
        "update_metadata", default=True, required=False
    )

    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    setup = setup_web_driver()

    data_is_up_to_date = is_up_to_date(upstream_tasks=[setup])

    with case(data_is_up_to_date, True):
        log_task("Tabela já esta atualizada")

    with case(data_is_up_to_date, False):
        current_date = datetime.datetime(
            year=datetime.datetime.now().year,
            month=datetime.datetime.now().month,
            day=1,
        )
        year_delta = current_date - datetime.timedelta(days=1)
        year_end = year_delta.year

        scraper_result = scraper(
            year_start=year_end,
            year_end=year_end,
            headless=True,
            upstream_tasks=[data_is_up_to_date],
        )

        download = download_xlsx(
            scraper_result, upstream_tasks=[scraper_result]
        )
        log_task("Download XLSX finished")

        df = clean_data(upstream_tasks=[download])

        output_filepath = make_partitions(df, upstream_tasks=[df])

        wait_upload_table = create_table_dev_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[output_filepath],
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
                data_path=output_filepath,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
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
                    bq_project="basedosdados",
                    upstream_tasks=[wait_upload_prod],
                )

datasets_br_mp_pep_cargos_funcoes_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
datasets_br_mp_pep_cargos_funcoes_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# datasets_br_mp_pep_cargos_funcoes_flow.schedule = every_month
