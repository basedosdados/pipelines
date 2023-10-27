# -*- coding: utf-8 -*-

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import parse_coverage

with Flow(name="feat_parse_coverage", code_owners=["Gabriel Pisa"]) as feat:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_bcb_sicor", required=True)
    table_id = Parameter("table_id", default="microdados_operacao", required=True)

    dados = parse_coverage(
        dataset_id=dataset_id, table_id=table_id, date_format="yy-mm-dd"
    )
