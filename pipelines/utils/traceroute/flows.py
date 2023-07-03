# -*- coding: utf-8 -*-
"""
DBT-related flows.
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import rename_current_flow_run
from pipelines.utils.traceroute.tasks import log_traceroute

with Flow(name="BD utils: Traceroute") as traceroute_flow:
    # Parameters
    hostname = Parameter("hostname")

    # Rename flow run
    rename_current_flow_run(msg=f"Traceroute: {hostname}")

    # Log traceroute
    log_traceroute(hostname=hostname)

traceroute_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
traceroute_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
