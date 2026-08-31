"""
Script standalone (não deployado, roda uma vez manualmente) que cria (ou
atualiza, se já existir) a Automação 1 do piloto da issue #1867:
check_update.completed -> RunDeployment do flow_download_flow, propagando
um dict de parâmetros serializado (`download_params`, ver
pipelines/utils/automations.py::encode_params) via Jinja.

Pré-requisito: os flows de pipelines/datasets/test_event_pipeline/flows.py
já deployados no pool basedosdados-dev, via:
  uv run python .github/scripts/deploy_flows.py --pool basedosdados-dev \
    --branch feat/event-pipeline-automations-poc \
    --files pipelines/datasets/test_event_pipeline/flows.py

Uso:
  uv run python scripts/pilot_event_automations.py
"""

from prefect.automations import Automation
from prefect.client.orchestration import get_client

from pipelines.datasets.test_event_pipeline.constants import DATASET_ID
from pipelines.utils.automations import build_chained_automation

FLOW_DOWNLOAD_DEPLOYMENT_NAME = (
    "test_event_pipeline: flow_download/flow_download_flow"
)
AUTOMATION_NAME = "test-event-pipeline: check_update -> flow_download"


def main() -> None:
    with get_client(sync_client=True) as client:
        deployment = client.read_deployment_by_name(
            FLOW_DOWNLOAD_DEPLOYMENT_NAME
        )

    desired = build_chained_automation(
        name=AUTOMATION_NAME,
        dataset_id=DATASET_ID,
        upstream_etapa="check_update",
        downstream_deployment_id=str(deployment.id),
        payload_fields=["download_params"],
    )

    try:
        existing = Automation.read(name=AUTOMATION_NAME)
    except ValueError:
        existing = None

    if existing is None:
        created = desired.create()
        print(f"Automação criada: {created.name} (id={created.id})")
    else:
        existing.trigger = desired.trigger
        existing.actions = desired.actions
        existing.update()
        print(f"Automação atualizada: {existing.name} (id={existing.id})")


if __name__ == "__main__":
    main()
