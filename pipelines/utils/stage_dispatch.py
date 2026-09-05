"""
Building blocks compartilhados pra encadear flows via `run_deployment()`
(issue #1867: basedosdados/pipelines#1867).

Centraliza a convenção de nome de deployment, pra cada dataset novo não
duplicar essa string — só chama `deployment_name()` e `run_deployment()`
com parâmetros nativos (dict/tipo real).

`CheckThenDownloadPipeline`, no fim do arquivo, é a interface recomendada
pra um `flows.py` de dataset novo na variante padrão (check_update e
download separados): encapsula o boilerplate repetido entre os dois
estágios (rename do flow run, poll/commit/dispatch, dispatch pro
mat_test), recebendo só a lógica específica do dataset (`check_for_update`,
`download_data`). Ver `pipelines/datasets/test_dataset/flows.py`
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
from pipelines.utils.tasks import rename_flow_run_dataset_table, upload_to_gcs


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
    DOWNLOAD = "download"
    MAT_TEST = "mat_test"


@dataclass
class CheckResult:
    """
    O que a lógica de check_update de um dataset (`check_for_update`) precisa
    devolver: a data de referência encontrada na fonte (comparada contra a
    coverage registrada pra decidir se há dado novo — essa comparação
    continua genérica, em `check_update_and_dispatch`) e, opcionalmente,
    qualquer informação extra que o `download_data` for precisar (ex. a URL
    exata descoberta durante o check, se não for previsível de antemão).
    """

    reference_date: datetime.date
    extra_download_params: dict = field(default_factory=dict)


@dataclass
class DownloadResult:
    """
    O que a lógica de download de um dataset (`download_data`) precisa
    devolver. `coverage` e `data_path` são usados pela própria cápsula
    (`CheckThenDownloadPipeline.run_download`, que faz o upload pro
    staging a partir de `data_path` antes de disparar o `mat_test`); os
    demais campos vão direto pro `mat_test` genérico materializar e testar
    a tabela. `coverage` é um `CoverageSpec.model_dump()` (`AllFree`/
    `AllBdpro`/`PartBdpro`/`NonHistorical` — ver
    `pipelines.utils.metadata.domain`).

    `data_path` é o caminho local (arquivo ou pasta, quando particionado —
    ver `partition_folders`) que o `download_data` escreveu; `dump_mode`/
    `source_format` variam de verdade entre datasets reais (incremental
    vs. snapshot completo, csv vs. parquet — não dá pra fixar um valor só
    pra todos, ver levantamento de crawlers na issue #1867), por isso são
    campos aqui em vez de constantes na cápsula.
    """

    coverage: dict
    data_path: str
    targets: list[str] = field(default_factory=lambda: ["dev", "prod"])
    bq_project: str = "basedosdados"
    prefect_mode: str = "prod"
    partition_folders: list[str] | None = None
    dump_mode: str = "append"
    source_format: str = "csv"


def deploy_tags(dataset_id: str, etapa: Etapa) -> list[str]:
    """
    Tags de deploy pra achar deployments relacionados no Prefect UI/CI
    (ex. "todo deployment de mat_test", "todo deployment do dataset X")
    sem precisar abrir cada `flows.py`. Usar em `<flow>.deploy_tags = [...]`.

    A tag da etapa é só o nome dela (`check_update`/`download`/
    `mat_test`), sem prefixo — `dataset:X` já deixa claro que a outra tag
    é a etapa, o `etapa:` na frente só poluía sem agregar.
    """
    return [str(etapa), f"dataset:{dataset_id}"]


def _flow_name(dataset_id: str, etapa: Etapa) -> str:
    """
    Nome do `@flow` por convenção: `"<etapa>: <dataset_id>"`. Um lugar só
    define esse formato — usado tanto por `deployment_name()` (resolve o
    `run_deployment(name=...)`) quanto pelas propriedades
    `check_update_flow_name`/`download_flow_name` de
    `CheckThenDownloadPipeline` (o que de fato vira o `@flow(name=...)`)
    — pra nunca divergir entre onde o flow é declarado e onde é
    referenciado no dispatch.
    """
    return f"{etapa}: {dataset_id}"


def deployment_name(
    dataset_id: str, etapa: Etapa, deployment: str | None = None
) -> str:
    """
    Resolve o identificador `"<flow name>/<deployment name>"` de um
    deployment por convenção — o mesmo formato aceito por
    `run_deployment(name=...)`, sem precisar consultar a API antes.

    `mat_test` é genérico (um deployment só, compartilhado por todos os
    datasets, ver `pipelines/utils/metadata/flows.py`) — nome fixo. As
    outras etapas seguem o padrão de `@flow(name="<etapa>: <dataset_id>")`
    (ver `_flow_name`) numa função `<etapa>_flow` — default de
    `deployment` quando não informado.

    `deployment`: sobrescreve a segunda metade (depois da barra) quando a
    variável do flow não se chama literalmente `<etapa>_flow`. Necessário
    quando vários datasets/pilotos compartilham o mesmo arquivo
    `flows.py` — `deploy_flows.py` descobre flows pelo nome da variável
    no módulo (`vars(module)`), então duas pipelines no mesmo arquivo não
    podem ter as duas uma variável `check_update_flow` (a segunda
    sobrescreveria a primeira no namespace do módulo, e o
    `deploy_flows.py` nunca veria a primeira). Cada uma precisa de um
    nome de variável próprio, e esse nome precisa bater com o que foi de
    fato implantado — ver `pipelines/datasets/test_dataset/flows.py`.
    """
    if etapa == Etapa.MAT_TEST:
        return "mat_test/mat_test_flow"
    return f"{_flow_name(dataset_id, etapa)}/{deployment or f'{etapa}_flow'}"


def check_update_and_dispatch(
    prefect_dataset_id: str,
    dataset_id: str,
    table_id: str,
    reference_date: datetime.date,
    next_etapa: Etapa = Etapa.DOWNLOAD,
    next_deployment: str | None = None,
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
    divergir. Isso não é um caso raro: é a convenção padrão do repo inteiro
    pra qualquer dataset com mais de uma tabela (`f"{dataset_id}__{table_id}"`,
    ver `br_bcb_agencia__agencia`/`br_denatran_frota__uf_tipo`/
    `br_rf_cno__{table_id}` em `pipelines/datasets/`), usada mesmo quando há
    só uma tabela. `CheckThenDownloadPipeline` deriva esse valor sozinho por
    padrão — ver seu construtor.

    `next_deployment` repassa pro `deployment_name()` — só precisa ser
    informado quando a variável do flow de `next_etapa` não se chama
    literalmente `<etapa>_flow` (ver docstring de `deployment_name`).

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
        name=deployment_name(prefect_dataset_id, next_etapa, next_deployment),
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
    de `check_update_and_dispatch`). `result` vem do `download_data` de cada
    dataset — os campos viram parâmetros nativos (dict/tipo real), o
    `mat_test_flow` os recebe tipados e o Pydantic valida sozinho.

    Generalizado a partir do que era `dispatch_mat_test` só dentro de
    `pipelines/datasets/test_dataset/tasks.py` — qualquer dataset na
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
    (check_update e download como estágios separados — ~51% dos
    datasets reais, ver levantamento na issue #1867). Encapsula o
    boilerplate repetido entre os dois estágios (rename do flow run,
    poll/commit/dispatch, dispatch pro mat_test) — cada dataset só precisa
    fornecer `check_for_update`/`download_data` com a lógica específica dele.

    `check_for_update` faz o que for necessário pra descobrir a data de referência
    na fonte (HEAD request, listagem FTP, scraping leve, API de metadata —
    ver exemplos reais no levantamento de crawlers da issue #1867) e
    devolve um `CheckResult`. A decisão de "isso é dado novo?" continua
    genérica (`check_update_and_dispatch`, comparando contra a coverage
    registrada) — `check_for_update` não decide isso, só informa a data.

    `download_data` recebe o `download_params` (dict repassado pelo
    `run_deployment()` do estágio anterior — sempre tem `reference_date`,
    mais o que `check_for_update` tiver posto em `extra_download_params`), faz o
    download de verdade e devolve um `DownloadResult` com o caminho local
    do arquivo/pasta (`data_path`) e o que o `mat_test` precisa pra
    materializar e testar a tabela. O upload pro staging (`upload_to_gcs`,
    sempre `bucket_name="basedosdados-dev"` nesta etapa) é feito pela
    própria cápsula em `run_download` — `download_data` não deveria chamar
    `upload_to_gcs` diretamente, só devolver onde escreveu o dado.

    O `@flow` em si continua tendo que ser definido no `flows.py` do
    próprio dataset (não dá pra gerar via fábrica genérica — `deploy_flows.py`
    só reconhece um `Flow` cuja função esteja definida no arquivo do
    dataset, via `obj.fn.__code__.co_filename`). Esta classe só reduz o
    corpo de cada `@flow` a uma chamada de método. `self.prefect_dataset_id`
    (derivado automaticamente como `f"{dataset_id}__{table_id}"` — mesma
    convenção usada em todo o repo, ver docstring de `check_update_and_dispatch`)
    e as propriedades `check_update_flow_name`/`download_flow_name`
    (que montam `f"<etapa>: {prefect_dataset_id}"`, o mesmo formato que
    `deployment_name()` espera do outro lado do dispatch — ver `_flow_name`)
    ficam disponíveis assim que a instância é construída, então dá pra usar
    direto no nome do `@flow`, sem precisar montar o f-string à mão nem de
    uma constante à parte:

        _pipeline = CheckThenDownloadPipeline(
            dataset_id=DATASET_ID, table_id=TABLE_ID,
            check_for_update=minha_logica_de_check, download_data=minha_logica_de_download,
        )

        @flow(name=_pipeline.check_update_flow_name, log_prints=True)
        def check_update_flow() -> None:
            _pipeline.run_check_update()
        check_update_flow.deploy_tags = deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE)

        @flow(name=_pipeline.download_flow_name, log_prints=True)
        def download_flow(download_params: dict) -> None:
            _pipeline.run_download(download_params)
        download_flow.deploy_tags = deploy_tags(DATASET_ID, Etapa.DOWNLOAD)

    Quando vários pilotos/datasets dividem o mesmo `flows.py` (`deploy_flows.py`
    descobre flows pelo nome da variável no módulo, então duas pipelines no
    mesmo arquivo não podem ter as duas uma variável `download_flow` —
    ver `pipelines/datasets/test_dataset/flows.py`), seta o
    `download_deployment` **depois** que o flow existir, a partir do
    `__name__` da própria função — não repita o nome como string solta no
    construtor (a função ainda não existe nesse ponto, e duas grafias do
    mesmo nome podem divergir silenciosamente, o mesmo risco que o
    `Etapa(StrEnum)` evita):

        _pipeline = CheckThenDownloadPipeline(...)

        @flow(name=_pipeline.download_flow_name, log_prints=True)
        def meu_download_flow(download_params: dict) -> None:
            _pipeline.run_download(download_params)
        meu_download_flow.deploy_tags = deploy_tags(...)
        _pipeline.download_deployment = meu_download_flow.fn.__name__
    """

    def __init__(
        self,
        *,
        dataset_id: str,
        table_id: str,
        check_for_update: Callable[[], CheckResult],
        download_data: Callable[[dict], DownloadResult],
        prefect_dataset_id: str | None = None,
        env: str = "prod",
        date_format: str = "%Y-%m-%d",
        download_deployment: str | None = None,
    ) -> None:
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.check_for_update = check_for_update
        self.download_data = download_data
        # Convenção padrão do repo pra nome de flow no Prefect, sempre
        # `dataset_id__table_id` (ver `br_bcb_agencia__agencia`,
        # `br_denatran_frota__uf_tipo`) — usada mesmo quando há só uma
        # tabela, então não precisa ser informada manualmente no caso
        # comum. `prefect_dataset_id` continua aceito como override pra
        # casos que fujam da convenção.
        self.prefect_dataset_id = (
            prefect_dataset_id or f"{dataset_id}__{table_id}"
        )
        self.env = env
        self.date_format = date_format
        self.download_deployment = download_deployment

    @property
    def check_update_flow_name(self) -> str:
        """Nome do `@flow` de check_update — passar direto em
        `@flow(name=...)`, ver docstring da classe."""
        return _flow_name(self.prefect_dataset_id, Etapa.CHECK_UPDATE)

    @property
    def download_flow_name(self) -> str:
        """Nome do `@flow` de download — passar direto em
        `@flow(name=...)`, ver docstring da classe."""
        return _flow_name(self.prefect_dataset_id, Etapa.DOWNLOAD)

    def run_check_update(self) -> bool:
        """Corpo completo do estágio check_update. Devolve `has_new_data`."""
        run_coro_as_sync(
            rename_flow_run_dataset_table(
                prefix="Check Update: ",
                dataset_id=self.dataset_id,
                table_id=self.table_id,
            )
        )

        result = self.check_for_update()

        return check_update_and_dispatch(
            prefect_dataset_id=self.prefect_dataset_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            reference_date=result.reference_date,
            next_deployment=self.download_deployment,
            env=self.env,
            date_format=self.date_format,
            extra_download_params=result.extra_download_params,
        )

    def run_download(self, download_params: dict) -> None:
        """Corpo completo do estágio download."""
        run_coro_as_sync(
            rename_flow_run_dataset_table(
                prefix="Download: ",
                dataset_id=self.dataset_id,
                table_id=self.table_id,
            )
        )

        result = self.download_data(download_params)

        upload_to_gcs(
            data_path=result.data_path,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            bucket_name="basedosdados-dev",
            dump_mode=result.dump_mode,
            source_format=result.source_format,
        )

        dispatch_mat_test(
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            result=result,
            env=self.env,
        )
