# -*- coding: utf-8 -*-
"""
Flow definition for the delete_flows pipeline
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.datasets.delete_flows.schedules import daily_at_3am
from pipelines.datasets.delete_flows.tasks import (
    delete_flow_run,
    get_old_flow_runs,
    get_prefect_client,
)
from pipelines.constants import constants
from pipelines.utils.decorators import Flow

with Flow(
    name="Limpeza de histórico de runs", code_owners=["guialvesp1"]
) as database_cleanup_flow:

    # Parameters
    days_old = Parameter("days_old", default=60, required=False)

    # Get the Prefect client
    client = get_prefect_client()

    # Get the old flow runs
    old_flow_runs = get_old_flow_runs(days_old=days_old, client=client)

    # Delete the old flow runs
    delete_flow_run.map(old_flow_runs)

database_cleanup_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
database_cleanup_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.BASEDOSDADOS_DEV_AGENT_LABEL.value],
)
database_cleanup_flow.schedule = daily_at_3am
