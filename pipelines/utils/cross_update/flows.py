# -*- coding: utf-8 -*-
"""
Flows for br_tse_eleicoes
"""
# pylint: disable=invalid-name,line-too-long

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect import Parameter, unmapped, case

from pipelines.constants import constants
from pipelines.utils.decorators import Flow
from pipelines.utils.cross_update.tasks import (
    crawler_datasets,
    last_updated_tables,
    update_nrows,
    tables_to_zip
)
from pipelines.utils.cross_update.schedules import schedule_nrows
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.tasks import get_current_flow_labels

with Flow(
    name="cross_update.update_nrows", code_owners=["lucas_cr"]
) as crossupdate_nrows:
    dump_to_gcs = Parameter("dump_to_gcs", default=False, required=False)
    days = Parameter("days", default=7, required=False)
    mode = Parameter("mode", default="prod", required=False)

    json_response = crawler_datasets(page_size=100,mode=mode)
    updated_tables = last_updated_tables(json_response, days=days)
    updated_tables.set_upstream(json_response)

    table_nrows = update_nrows.map(updated_tables, mode=unmapped(mode))
    table_nrows.set_upstream(updated_tables)

    with case(dump_to_gcs, True):
        json_response = crawler_datasets(page_size=100,mode=mode)
        current_flow_labels = get_current_flow_labels()
        tables_to_zip = tables_to_zip(json_response, days=days)
        tables_to_zip.set_upstream(json_response)
        dump_eventos_to_gcs_flow = create_flow_run.map(
            flow_name=unmapped(utils_constants.FLOW_DUMP_TO_GCS_NAME.value),
            project_name=unmapped(constants.PREFECT_DEFAULT_PROJECT.value),
            parameters=tables_to_zip,
            labels=unmapped(current_flow_labels),
            run_name=unmapped("Dump to GCS"),
        )

        wait_for_dump_to_gcs = wait_for_flow_run(
            dump_eventos_to_gcs_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

crossupdate_nrows.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
crossupdate_nrows.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
crossupdate_nrows.schedule = schedule_nrows
