"""
Building blocks compartilhados pra encadear flows via `run_deployment()`
(issue #1867: basedosdados/pipelines#1867).

Centraliza a convenção de nome de deployment, pra cada dataset novo não
duplicar essa string — só chama `deployment_name()` e `run_deployment()`
com parâmetros nativos (dict/tipo real).

`CheckThenDownloadPipeline`, no fim do arquivo, é a interface recomendada
pra um `flows.py` de dataset novo na variante padrão (check_update e
flow_download separados): encapsula o boilerplate repetido entre os dois
estágios (rename do flow run, poll/commit/dispatch, dispatch pro
mat_test), recebendo só a lógica específica do dataset (`check_fn`,
`download_fn`). Ver `pipelines/datasets/test_event_pipeline/flows.py`
pra um exemplo de uso.
"""

import datetime
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum

from prefect.deployments import run_deployment
from prefect.utilities.asyncutils import run_coro_as_sync

from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
)
from pipelines.utils.tasks import rename_flow_run_dataset_table


class Etapa(StrEnum):
    """
    As três etapas da arquitetura orientada a eventos (issue #1867).
    `StrEnum` (não `Enum` puro) de propósito: os membros já são `str` de
    verdade, então continuam funcionando direto em f-string/chave de dict
    sem precisar de `.value` em lugar nenhum — só ganha a proteção contra
    erro de digitação (`Etapa.CHECK_UPDTE` vira `AttributeError` na hora,
    em vez de gerar silenciosamente um nome de deployment errado que só
    falha lá na frente, no `run_deployment()`). Mesmo padrão de
    `DateFormat` em `pipelines.utils.metadata.domain`.
    """

    CHECK_UPDATE = "check_update"
    FLOW_DOWNLOAD = "flow_download"
    MAT_TEST = "mat_test"


@dataclass
class CheckResult:
    """
    O que a lógica de check_update de um dataset (`check_fn`) precisa
    devolver: a data de referência encontrada na fonte (comparada contra a
    coverage registrada pra decidir se há dado novo — essa comparação
    continua genérica, em `check_update_and_dispatch`) e, opcionalmente,
    qualquer informação extra que o `download_fn` for precisar (ex. a URL
    exata descoberta durante o check, se não for previsível de antemão).
    """

    reference_date: datetime.date
    extra_download_params: dict = field(default_factory=dict)


@dataclass
class DownloadResult:
    """
    O que a lógica de download de um dataset (`download_fn`) precisa
    devolver pro `mat_test` genérico materializar e testar a tabela.
    `coverage` é um `CoverageSpec.model_dump()` (`AllFree`/`AllBdpro`/
    `PartBdpro`/`NonHistorical` — ver `pipelines.utils.metadata.domain`).
    """

    coverage: dict
    targets: list[str] = field(default_factory=lambda: ["dev", "prod"])
    bq_project: str = "basedosdados"
    prefect_mode: str = "prod"
    partition_folders: list[str] | None = None


def etapa_tag(etapa: Etapa) -> str:
    return f"etapa:{etapa}"


def deploy_tags(dataset_id: str, etapa: Etapa) -> list[str]:
    """
    Tags de deploy pra achar deployments relacionados no Prefect UI/CI
    (ex. "todo deployment de etapa:mat_test", "todo deployment do dataset
    X") sem precisar abrir cada `flows.py`. Usar em
    `<flow>.deploy_tags = [...]`.
    """
    return [etapa_tag(etapa), f"dataset:{dataset_id}"]


def deployment_name(dataset_id: str, etapa: Etapa) -> str:
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
    if etapa == Etapa.MAT_TEST:
        return "mat_test/mat_test_flow"
    return f"{dataset_id}: {etapa}/{etapa}_flow"


def check_update_and_dispatch(
    prefect_dataset_id: str,
    dataset_id: str,
    table_id: str,
    reference_date: datetime.date,
    next_etapa: Etapa = Etapa.FLOW_DOWNLOAD,
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


def dispatch_mat_test(
    dataset_id: str,
    table_id: str,
    result: DownloadResult,
    env: str = "prod",
) -> None:
    """
    Dispara o `mat_test_flow` genérico (`pipelines/utils/metadata/flows.py`)
    via `run_deployment()` (`timeout=0`/`as_subflow=True`, mesma convenção
    de `check_update_and_dispatch`). `result` vem do `download_fn` de cada
    dataset — os campos viram parâmetros nativos (dict/tipo real), o
    `mat_test_flow` os recebe tipados e o Pydantic valida sozinho.

    Generalizado a partir do que era `dispatch_mat_test` só dentro de
    `pipelines/datasets/test_event_pipeline/tasks.py` — qualquer dataset na
    variante padrão usa este, não precisa reescrever.
    """
    run_deployment(
        name=deployment_name(dataset_id, Etapa.MAT_TEST),
        parameters={
            "dataset_id": dataset_id,
            "table_id": table_id,
            "coverage": result.coverage,
            "env": env,
            "bq_project": result.bq_project,
            "prefect_mode": result.prefect_mode,
            "targets": result.targets,
            "partition_folders": result.partition_folders,
        },
        timeout=0,
        as_subflow=True,
    )


class CheckThenDownloadPipeline:
    """
    Interface recomendada pra um `flows.py` de dataset na variante padrão
    (check_update e flow_download como estágios separados — ~51% dos
    datasets reais, ver levantamento na issue #1867). Encapsula o
    boilerplate repetido entre os dois estágios (rename do flow run,
    poll/commit/dispatch, dispatch pro mat_test) — cada dataset só precisa
    fornecer `check_fn`/`download_fn` com a lógica específica dele.

    `check_fn` faz o que for necessário pra descobrir a data de referência
    na fonte (HEAD request, listagem FTP, scraping leve, API de metadata —
    ver exemplos reais no levantamento de crawlers da issue #1867) e
    devolve um `CheckResult`. A decisão de "isso é dado novo?" continua
    genérica (`check_update_and_dispatch`, comparando contra a coverage
    registrada) — `check_fn` não decide isso, só informa a data.

    `download_fn` recebe o `download_params` (dict repassado pelo
    `run_deployment()` do estágio anterior — sempre tem `reference_date`,
    mais o que `check_fn` tiver posto em `extra_download_params`), faz o
    download de verdade e o upload pro staging (`upload_to_gcs` ou
    equivalente — fica a critério da lógica do dataset, não é genérico
    porque dump_mode/formato variam), e devolve um `DownloadResult` com o
    que o `mat_test` precisa pra materializar e testar a tabela.

    O `@flow` em si continua tendo que ser definido no `flows.py` do
    próprio dataset (não dá pra gerar via fábrica genérica — `deploy_flows.py`
    só reconhece um `Flow` cuja função esteja definida no arquivo do
    dataset, via `obj.fn.__code__.co_filename`). Esta classe só reduz o
    corpo de cada `@flow` a uma chamada de método:

        _pipeline = CheckThenDownloadPipeline(
            dataset_id=DATASET_ID, table_id=TABLE_ID,
            check_fn=minha_logica_de_check, download_fn=minha_logica_de_download,
        )

        @flow(name=f"{PREFECT_DATASET_ID}: check_update", log_prints=True)
        def check_update_flow() -> None:
            _pipeline.run_check_update()
        check_update_flow.deploy_tags = deploy_tags(PREFECT_DATASET_ID, Etapa.CHECK_UPDATE)

        @flow(name=f"{PREFECT_DATASET_ID}: flow_download", log_prints=True)
        def flow_download_flow(download_params: dict) -> None:
            _pipeline.run_download(download_params)
        flow_download_flow.deploy_tags = deploy_tags(PREFECT_DATASET_ID, Etapa.FLOW_DOWNLOAD)
    """

    def __init__(
        self,
        *,
        dataset_id: str,
        table_id: str,
        check_fn: Callable[[], CheckResult],
        download_fn: Callable[[dict], DownloadResult],
        prefect_dataset_id: str | None = None,
        env: str = "prod",
        date_format: str = "%Y-%m-%d",
    ) -> None:
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.check_fn = check_fn
        self.download_fn = download_fn
        self.prefect_dataset_id = prefect_dataset_id or dataset_id
        self.env = env
        self.date_format = date_format

    def run_check_update(self) -> bool:
        """Corpo completo do estágio check_update. Devolve `has_new_data`."""
        run_coro_as_sync(
            rename_flow_run_dataset_table(
                prefix="Check Update: ",
                dataset_id=self.dataset_id,
                table_id=self.table_id,
            )
        )

        result = self.check_fn()

        return check_update_and_dispatch(
            prefect_dataset_id=self.prefect_dataset_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            reference_date=result.reference_date,
            env=self.env,
            date_format=self.date_format,
            extra_download_params=result.extra_download_params,
        )

    def run_download(self, download_params: dict) -> None:
        """Corpo completo do estágio flow_download."""
        run_coro_as_sync(
            rename_flow_run_dataset_table(
                prefix="Flow Download: ",
                dataset_id=self.dataset_id,
                table_id=self.table_id,
            )
        )

        result = self.download_fn(download_params)

        dispatch_mat_test(
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            result=result,
            env=self.env,
        )
