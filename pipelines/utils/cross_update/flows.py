# -*- coding: utf-8 -*-
"""
Flows for br_tse_eleicoes
"""
# pylint: disable=invalid-name,line-too-long
from datetime import datetime, timedelta

from prefect import Parameter, case, unmapped
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.cross_update.tasks import (
    get_nrows,
)
from pipelines.datasets.br_tse_eleicoes.schedules import (
    schedule_bens,
    schedule_candidatos,
    schedule_despesa,
    schedule_receita,
)
from pipelines.utils.tasks import (
    update_metadata,
)

from pipelines.datasets.br_tse_eleicoes.constants import constants as tse_constants

with Flow(
    name="cross_update.update_nrows", code_owners=["lucas_cr"]
) as crossupdate_nrows:
    updated_tables = last_updated_tables()

    table_nrows = get_nrows.map(updated_tables)

    update_nrows.map(table_nrows)

crossupdate_nrows.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
crossupdate_nrows.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
crossupdate_nrows.schedule = schedule_candidatos
