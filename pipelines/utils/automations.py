"""
Building blocks compartilhados para criar Automations do Prefect 3 que
encadeiam flows via evento (issue #1867: basedosdados/pipelines#1867).

Centraliza a convenção de resource id, nome de evento e o template Jinja de
parâmetro, pra cada automação nova (uma por par de flows encadeados) não
duplicar essas strings — só monta e chama `build_chained_automation`.
"""

import datetime
import json

from prefect.automations import Automation
from prefect.events.actions import RunDeployment
from prefect.events.schemas.automations import EventTrigger, Posture
from prefect.events.schemas.events import ResourceSpecification
from prefect.events.utilities import emit_event

from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
)


def dataset_resource_id(dataset_id: str) -> str:
    """Resource id usado tanto no `emit_event` quanto no `match` da automação."""
    return f"dataset.{dataset_id}"


def event_name(etapa: str) -> str:
    """etapa: check_update | flow_download | mat_test"""
    return f"{etapa}.completed"


def etapa_tag(etapa: str) -> str:
    """etapa: check_update | flow_download | mat_test"""
    return f"etapa:{etapa}"


def deploy_tags(dataset_id: str, etapa: str) -> list[str]:
    """
    Tags de deploy que marcam um flow como parte de uma cadeia de
    automações — é assim que um script de criação de automação em massa
    (pros datasets reais, não só o piloto) encontra qual deployment
    corresponde a qual etapa de qual dataset, em vez de precisar de nomes
    de deployment hardcoded. Usar em `<flow>.deploy_tags = [...]` no
    `flows.py` do dataset. O Prefect anexa essas tags automaticamente como
    related resources em qualquer evento emitido durante a execução do
    deployment — é isso que `build_chained_automation` usa no
    `match_related`, não só o nome do evento.
    """
    return [etapa_tag(etapa), f"dataset:{dataset_id}"]


def encode_params(params: dict) -> str:
    """
    Serializa um dict de parâmetros pro payload de um `emit_event`. Não dá
    pra colocar o dict cru no payload e referenciar ele inteiro via
    `{{ event.payload.campo }}` num RunDeployment: o Jinja das automações do
    Prefect sempre renderiza como string (não existe modo "tipo nativo"),
    então um dict viraria a repr() em Python, não um dict de verdade no
    parâmetro do flow downstream. JSON, por já ser string, atravessa esse
    Jinja sem alteração — o flow downstream reconstrói com `decode_params`.
    Isso permite que upstream e downstream combinem em qualquer conjunto de
    campos sem precisar mudar a automação a cada campo novo.
    """
    return json.dumps(params)


def decode_params(raw: str) -> dict:
    """Reconstrói o dict serializado por `encode_params`."""
    return json.loads(raw)


def payload_parameters(*fields: str) -> dict[str, str]:
    """
    Monta o dict `parameters` de um RunDeployment referenciando cada campo
    do payload do evento via Jinja, ex.:
    payload_parameters("reference_date", "source_url") ->
        {"reference_date": "{{ event.payload.reference_date }}",
         "source_url": "{{ event.payload.source_url }}"}
    """
    return {field: f"{{{{ event.payload.{field} }}}}" for field in fields}


def build_chained_automation(
    name: str,
    dataset_id: str,
    upstream_etapa: str,
    downstream_deployment_id: str,
    payload_fields: list[str],
) -> Automation:
    """
    Uma automação reativa: quando `upstream_etapa` emite seu evento
    `<etapa>.completed` pro resource do dataset, dispara o deployment
    downstream passando `payload_fields` do payload do evento como
    parâmetros do flow.

    `match_related` exige que o evento tenha vindo de um flow run cujo
    deployment carrega a tag `etapa:<upstream_etapa>` (anexada
    automaticamente pelo Prefect como related resource, via
    `deploy_tags` — ver `deploy_tags()`/`.github/scripts/deploy_flows.py`).
    Isso evita que a automação dispare por engano se algum dia existir
    outro evento com o mesmo nome vindo de um deployment sem essa tag —
    o nome do evento sozinho não bastaria como garantia de que veio da
    etapa certa.
    """
    return Automation(
        name=name,
        trigger=EventTrigger(
            expect={event_name(upstream_etapa)},
            match=ResourceSpecification(
                {"prefect.resource.id": [dataset_resource_id(dataset_id)]}
            ),
            match_related=ResourceSpecification(
                {
                    "prefect.resource.role": ["tag"],
                    "prefect.resource.id": [
                        f"prefect.tag.{etapa_tag(upstream_etapa)}"
                    ],
                }
            ),
            posture=Posture.Reactive,
            threshold=1,
            within=0,
        ),
        actions=[
            RunDeployment(
                deployment_id=downstream_deployment_id,
                parameters=payload_parameters(*payload_fields),
            )
        ],
    )


def check_update_and_emit(
    resource_dataset_id: str,
    backend_dataset_id: str,
    backend_table_id: str,
    reference_date: datetime.date,
    upstream_etapa: str = "check_update",
    env: str = "prod",
    date_format: str = "%Y-%m-%d",
    extra_download_params: dict | None = None,
) -> bool:
    """
    Encapsula o padrão real de check_update, pra não repetir essa sequência
    em cada dataset: `poll_source_for_update_task` decide se há dado novo
    comparando `reference_date` contra a coverage registrada no backend; se
    houver, comita o Update (`commit_source_update_task`) e emite o evento
    `<upstream_etapa>.completed` com `download_params` (`reference_date` +
    `extra_download_params`). Devolve `has_new_data` — o flow só precisa
    decidir o que logar, não repetir poll+commit+emit.

    `poll_source_for_update_task`/`commit_source_update_task` vêm de
    `pipelines.utils.metadata.tasks` — o mesmo par usado pelos datasets
    reais (ver `br_bcb_estban/flows.py`).
    """
    has_new_data = poll_source_for_update_task(
        dataset_id=backend_dataset_id,
        table_id=backend_table_id,
        source_max_date=reference_date,
        env=env,
        date_format=date_format,
        compare_against="coverage",
    )
    if not has_new_data:
        return False

    commit_source_update_task(
        dataset_id=backend_dataset_id,
        table_id=backend_table_id,
        source_max_date=reference_date,
        env=env,
        date_format=date_format,
    )

    download_params = {
        "reference_date": reference_date.isoformat(),
        **(extra_download_params or {}),
    }
    emit_event(
        event=event_name(upstream_etapa),
        resource={"prefect.resource.id": dataset_resource_id(resource_dataset_id)},
        payload={"download_params": encode_params(download_params)},
    )
    return True
