"""
Building blocks compartilhados pra encadear flows via `run_deployment()`
(issue #1867: basedosdados/pipelines#1867).

Centraliza a convenção de nome de deployment, pra cada dataset novo não
duplicar essa string — só chama `deployment_name()` e `run_deployment()`
com parâmetros nativos (dict/tipo real), sem Jinja/JSON de payload.

Substituiu o mecanismo anterior baseado em `Automation`/`emit_event` — ver
`run-deployment-vs-automacao.md` (D:\\docs\\pipelines) pra comparação e
motivação da troca.
"""

import datetime

from prefect.deployments import run_deployment

from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
)


def etapa_tag(etapa: str) -> str:
    """etapa: check_update | flow_download | mat_test"""
    return f"etapa:{etapa}"


def deploy_tags(dataset_id: str, etapa: str) -> list[str]:
    """
    Tags de deploy — não têm mais função na cadeia de disparo em si
    (`run_deployment()` resolve o deployment por nome, não por tag), mas
    continuam úteis pra achar deployments relacionados no Prefect UI/CI
    sem precisar abrir cada `flows.py`. Usar em `<flow>.deploy_tags = [...]`.
    """
    return [etapa_tag(etapa), f"dataset:{dataset_id}"]


def deployment_name(dataset_id: str, etapa: str) -> str:
    """
    Resolve o identificador `"<flow name>/<deployment name>"` de um
    deployment por convenção — o mesmo formato aceito por
    `run_deployment(name=...)`, sem precisar consultar a API antes.

    `mat_test` é genérico (um deployment só, compartilhado por todos os
    datasets, ver `pipelines/utils/metadata/flows.py`) — nome fixo. As
    outras etapas seguem o padrão de `@flow(name="<dataset_id>: <etapa>")`
    numa função `<etapa>_flow` (mesma convenção usada em
    `pipelines/datasets/test_event_pipeline/flows.py`).
    """
    if etapa == "mat_test":
        return "mat_test/mat_test_flow"
    return f"{dataset_id}: {etapa}/{etapa}_flow"


def check_update_and_dispatch(
    resource_dataset_id: str,
    backend_dataset_id: str,
    backend_table_id: str,
    reference_date: datetime.date,
    upstream_etapa: str = "check_update",
    downstream_etapa: str = "flow_download",
    env: str = "prod",
    date_format: str = "%Y-%m-%d",
    extra_download_params: dict | None = None,
) -> bool:
    """
    Encapsula o padrão real de check_update, pra não repetir essa sequência
    em cada dataset: `poll_source_for_update_task` decide se há dado novo
    comparando `reference_date` contra a coverage registrada no backend; se
    houver, comita o Update (`commit_source_update_task`) e dispara o
    deployment de `downstream_etapa` do mesmo dataset via `run_deployment()`
    (`timeout=0` — não espera o flow downstream terminar, mesma garantia de
    isolamento de recurso entre etapas que a automação dava;
    `as_subflow=True` — aparece linkado como filho na árvore de execução do
    Prefect UI). Devolve `has_new_data` — o flow só precisa decidir o que
    logar, não repetir poll+commit+dispatch.

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
    run_deployment(
        name=deployment_name(resource_dataset_id, downstream_etapa),
        parameters={"download_params": download_params},
        timeout=0,
        as_subflow=True,
    )
    return True
