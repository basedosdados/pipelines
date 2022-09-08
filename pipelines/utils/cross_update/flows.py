# -*- coding: utf-8 -*-
"""
Flows for br_tse_eleicoes
"""
# pylint: disable=invalid-name,line-too-long

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.decorators import Flow
from pipelines.utils.cross_update.tasks import (
    crawler_datasets,
    last_updated_tables,
    get_nrows,
    update_nrows,
)
from pipelines.utils.cross_update.schedules import (
    schedule_nrows
)

with Flow(
    name="cross_update.update_nrows", code_owners=["lucas_cr"]
) as crossupdate_nrows:
    json_response = crawler_datasets()
    updated_tables = last_updated_tables(json_response)
    updated_tables.set_upstream(json_response)

    table_nrows = get_nrows.map(updated_tables)
    table_nrows.set_upstream(updated_tables)

    update_nrows.map(table_nrows)
    update_nrows.set_upstream(table_nrows)

crossupdate_nrows.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
crossupdate_nrows.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
crossupdate_nrows.schedule = schedule_nrows
