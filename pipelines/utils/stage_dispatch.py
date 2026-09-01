"""
Building blocks compartilhados pra encadear flows via `run_deployment()`
(issue #1867: basedosdados/pipelines#1867).

Centraliza a convenção de nome de deployment, pra cada dataset novo não
duplicar essa string — só chama `deployment_name()` e `run_deployment()`
com parâmetros nativos (dict/tipo real).
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
    Tags de deploy pra achar deployments relacionados no Prefect UI/CI
    (ex. "todo deployment de etapa:mat_test", "todo deployment do dataset
    X") sem precisar abrir cada `flows.py`. Usar em
    `<flow>.deploy_tags = [...]`.
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
    prefect_dataset_id: str,
    dataset_id: str,
    table_id: str,
    reference_date: datetime.date,
    next_etapa: str = "flow_download",
    env: str = "prod",
    date_format: str = "%Y-%m-%d",
    extra_download_params: dict | None = None,
) -> bool:
    """
    Encapsula o padrão real de check_update, pra não repetir essa sequência
    em cada dataset: `poll_source_for_update_task` decide se há dado novo
    comparando `reference_date` contra a coverage registrada no backend; se
    houver, comita o Update (`commit_source_update_task`) e dispara o
    deployment de `next_etapa` do mesmo dataset via `run_deployment()`
    (`timeout=0` — não espera o flow seguinte terminar, cada etapa continua
    isolada em seu próprio pod; `as_subflow=True` — aparece linkado como
    filho na árvore de execução do Prefect UI). Devolve `has_new_data` — o
    flow só precisa decidir o que logar, não repetir poll+commit+dispatch.

    `dataset_id`/`table_id` são a identidade real no backend/BigQuery.
    `prefect_dataset_id` é só a convenção de nome usada pelo `deployment_name()`
    pra resolver qual deployment chamar em seguida — as duas coisas podem
    divergir (ex. o piloto: `prefect_dataset_id="test_event_pipeline"`,
    `dataset_id="test_dataset"`), por isso não têm o mesmo nome de parâmetro.

    `poll_source_for_update_task`/`commit_source_update_task` vêm de
    `pipelines.utils.metadata.tasks` — o mesmo par usado pelos datasets
    reais (ver `br_bcb_estban/flows.py`).
    """
    has_new_data = poll_source_for_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=reference_date,
        env=env,
        date_format=date_format,
        compare_against="coverage",
    )
    if not has_new_data:
        return False

    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=reference_date,
        env=env,
        date_format=date_format,
    )

    download_params = {
        "reference_date": reference_date.isoformat(),
        **(extra_download_params or {}),
    }
    run_deployment(
        name=deployment_name(prefect_dataset_id, next_etapa),
        parameters={"download_params": download_params},
        timeout=0,
        as_subflow=True,
    )
    return True
