"""
Script standalone (não deployado, roda uma vez manualmente) que cria (ou
atualiza, se já existir) as automações do piloto da issue #1867:
  Automação 1: check_update.completed  -> RunDeployment(flow_download_flow)
  Automação 2: flow_download.completed -> RunDeployment(mat_test_flow)
Cada uma propaga um dict de parâmetros serializado (ver
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

AUTOMATIONS = [
    {
        "name": "test-event-pipeline: check_update -> flow_download",
        "upstream_etapa": "check_update",
        "downstream_deployment_name": (
            "test_event_pipeline: flow_download/flow_download_flow"
        ),
        "payload_fields": ["download_params"],
    },
    {
        "name": "test-event-pipeline: flow_download -> mat_test",
        "upstream_etapa": "flow_download",
        "downstream_deployment_name": (
            "test_event_pipeline: mat_test/mat_test_flow"
        ),
        "payload_fields": ["mat_test_params"],
    },
]


def upsert_automation(client, spec: dict) -> None:
    deployment = client.read_deployment_by_name(spec["downstream_deployment_name"])

    desired = build_chained_automation(
        name=spec["name"],
        dataset_id=DATASET_ID,
        upstream_etapa=spec["upstream_etapa"],
        downstream_deployment_id=str(deployment.id),
        payload_fields=spec["payload_fields"],
    )

    try:
        existing = Automation.read(name=spec["name"])
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


def main() -> None:
    with get_client(sync_client=True) as client:
        for spec in AUTOMATIONS:
            upsert_automation(client, spec)


if __name__ == "__main__":
    main()
