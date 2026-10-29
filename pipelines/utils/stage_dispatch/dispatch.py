"""Passagem de uma etapa do pipeline em estágios para a seguinte.

`check_update_and_dispatch` dispara o `extract_and_load` quando a fonte tem
dado novo, e `dispatch_build_and_promote` dispara o `build_and_promote`. Os
dois usam `run_deployment()`; no pool local, chamam a função do próximo
estágio direto, por `_run_flow_locally()`.
"""

import datetime
import importlib.util
from typing import Any

from prefect.client.orchestration import get_client
from prefect.deployments import run_deployment

from pipelines.utils.metadata.constants import constants as metadata_constants
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
)
from pipelines.utils.stage_dispatch.contracts import Etapa, ExtractAndLoad
from pipelines.utils.stage_dispatch.naming import (
    _dev_prefix,
    _is_local,
    deployment_name,
)


def _run_flow_locally(deployment_full_name: str, parameters: dict) -> Any:
    """Roda o próximo estágio chamando a função do flow direto (`.fn`),
    sem passar por `run_deployment()`.

    `run_deployment()` chamado de dentro de um flow já em execução quebra
    com `422 Unprocessable Entity` contra o servidor Prefect local
    (`scripts/dev_local.sh`) — não ocorre em produção. `validate_parameters()`
    aplica a mesma coerção de tipos que `run_deployment()` faria
    (`dict` -> `CoverageSpec`, etc.).

    Args:
        deployment_full_name: `"<flow name>/<deployment name>"`, mesmo
            formato de `run_deployment(name=...)`.
        parameters: parâmetros a passar pra função do próximo estágio.

    Returns:
        O que a função do próximo estágio devolver.
    """
    with get_client(sync_client=True) as client:
        deployment = client.read_deployment_by_name(deployment_full_name)

    if deployment.entrypoint is None:
        raise RuntimeError(
            f"Deployment '{deployment_full_name}' não tem entrypoint registrado "
            "— não dá pra resolver o flow pra chamar localmente."
        )
    file_path, func_name = deployment.entrypoint.split(":")
    spec = importlib.util.spec_from_file_location(func_name, file_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(
            f"Não foi possível carregar '{file_path}' pra rodar "
            f"'{deployment_full_name}' localmente."
        )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    target_flow = getattr(module, func_name)

    validated = target_flow.validate_parameters(parameters)
    return target_flow.fn(**validated)


def check_update_and_dispatch(
    prefect_dataset_id: str,
    dataset_id: str,
    table_id: str,
    reference_date: datetime.date,
    next_etapa: Etapa = Etapa.EXTRACT_AND_LOAD,
    next_deployment: str | None = None,
    env: str = "prod",
    date_format: str = "%Y-%m-%d",
    extra_download_params: dict | None = None,
    compare_against: str = "coverage",
) -> bool:
    """Encapsula o padrão real de check_update: poll, commit e dispatch.

    `poll_source_for_update_task` decide se há dado novo comparando
    `reference_date` contra o alvo de comparação no backend; se houver,
    comita o Update (`commit_source_update_task`) e dispara o deployment
    de `next_etapa` via `run_deployment()` (`timeout=0`, `as_subflow=True`)
    — ou, no pool local (`_is_local()`), chama a função do próximo estágio
    direto, sem passar por `run_deployment()` (ver `_run_flow_locally()`).

    Args:
        prefect_dataset_id: convenção de nome usada por `deployment_name()`
            pra resolver o próximo deployment — normalmente
            `f"{dataset_id}__{table_id}"`.
        dataset_id: ID do dataset no backend/BigQuery.
        table_id: ID da tabela no backend/BigQuery.
        reference_date: data de referência encontrada na fonte.
        next_etapa: etapa a disparar em seguida.
        next_deployment: sobrescreve o nome do deployment de `next_etapa`
            — só necessário quando a variável do flow não se chama
            literalmente `<etapa>`.
        env: backend de destino.
        date_format: formato de `reference_date` quando repassado como
            string.
        extra_download_params: campos extras a incluir no
            `download_params` do próximo estágio, além de `reference_date`.
        compare_against: `"coverage"` (padrão) lê `Coverage.DateTimeRange`.
            `"table_update"` lê `Table.Update.latest` — tabelas
            `NonHistorical` precisam dele em vez do padrão.

    Returns:
        `True` se havia dado novo e o próximo estágio foi disparado;
        `False` caso contrário.
    """
    has_new_data = poll_source_for_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=reference_date,
        env=env,
        date_format=date_format,
        compare_against=compare_against,
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
    name = deployment_name(prefect_dataset_id, next_etapa, next_deployment)
    if _is_local():
        _run_flow_locally(name, {"download_params": download_params})
    else:
        run_deployment(
            name=name,
            parameters={"download_params": download_params},
            timeout=0,
            as_subflow=True,
        )
    return True


def dispatch_build_and_promote(
    dataset_id: str,
    table_id: str,
    result: ExtractAndLoad,
    env: str = "prod",
) -> None:
    """Dispara o `build_and_promote` genérico via `run_deployment()` (ou,
    no pool local, chamando a função direto — ver `_run_flow_locally()`).

    `promote_to_prod` não vem de `result`: é derivado de `_dev_prefix()` —
    quem despachou (o `extract_and_load` chamando esta função) só promove
    pra prod se ele mesmo estiver rodando num deployment de prod. Um
    dataset real testado num deployment de dev nunca promove pra prod de
    verdade, não importa o que o dataset diga — é uma garantia estrutural,
    não uma configuração que o dataset possa contornar.

    Args:
        dataset_id: ID do dataset no backend/BigQuery.
        table_id: ID da tabela no backend/BigQuery.
        result: o que `extract_load_data` do dataset devolveu.
        env: backend de destino.
    """
    parameters = {
        "dataset_id": dataset_id,
        "table_id": table_id,
        "coverage": result.coverage,
        "env": env,
        "bq_project": metadata_constants.MODE_PROJECT.value[
            result.prefect_mode
        ],
        "prefect_mode": result.prefect_mode,
        "promote_to_prod": not _dev_prefix(),
        "partition_folders": result.partition_folders,
        "dump_mode": result.dump_mode,
        "source_format": result.source_format,
    }
    name = deployment_name(dataset_id, Etapa.BUILD_AND_PROMOTE)
    if _is_local():
        _run_flow_locally(name, parameters)
    else:
        run_deployment(
            name=name,
            parameters=parameters,
            timeout=0,
            as_subflow=True,
        )
