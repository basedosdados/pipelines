"""Building blocks compartilhados pra encadear flows via `run_deployment()`.

`Etapa`, `SourceInspection`, `ExtractAndLoad`: tipos compartilhados entre
as etapas. `deploy_tags`/`deployment_name`/`_flow_name`: convenção de nome
de deployment. `check_update_and_dispatch`/`dispatch_build_and_promote`:
dispatch entre etapas via `run_deployment()` (ou, no pool local, chamando
o próximo estágio direto — ver `_run_flow_locally()`). `pipeline_factory`/
`CheckThenExtractLoadPipeline`: interface recomendada pra um `flows.py` de
dataset na variante padrão (check_update e extract_and_load separados).
"""

import datetime
import importlib.util
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from prefect import runtime
from prefect.client.orchestration import get_client
from prefect.deployments import run_deployment

from pipelines.utils.metadata.constants import constants as metadata_constants
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
)
from pipelines.utils.tasks import rename_flow_run_dataset_table, upload_to_gcs


class Etapa(StrEnum):
    """As três etapas da arquitetura em estágios (staged pipeline).

    Attributes:
        CHECK_UPDATE: etapa que verifica se há dado novo na fonte.
        EXTRACT_AND_LOAD: etapa que baixa o dado e sobe pro staging.
        BUILD_AND_PROMOTE: etapa que materializa, testa e promove pra prod.
    """

    CHECK_UPDATE = "check_update"
    EXTRACT_AND_LOAD = "extract_and_load"
    BUILD_AND_PROMOTE = "build_and_promote"


@dataclass
class SourceInspection:
    """O que `get_latest_update` de um dataset devolve pro check_update.

    Attributes:
        reference_date: data de referência encontrada na fonte.
        extra_download_params: campos extra que `extract_load_data` vai
            precisar.
        compare_against: `"coverage"` (padrão, lê `Coverage.DateTimeRange`)
            ou `"table_update"` (lê `Table.Update.latest` — tabelas
            `NonHistorical`, sem baseline de coverage confiável).
    """

    reference_date: datetime.date
    extra_download_params: dict = field(default_factory=dict)
    compare_against: str = "coverage"


@dataclass
class ExtractAndLoad:
    """O que `extract_load_data` de um dataset devolve pro extract_and_load.

    Attributes:
        coverage: `CoverageSpec.model_dump()` (`AllFree`/`AllBdpro`/
            `PartBdpro`/`NonHistorical` — ver
            `pipelines.utils.metadata.domain`).
        data_path: caminho local que `extract_load_data` escreveu. Numa
            tabela particionada, precisa ser um diretório organizado
            exatamente na estrutura Hive que será promovida (ex.
            `data_path/ano=2026/mes=09/dados.csv`) — o upload deriva o
            prefixo de partição no GCS a partir dessa estrutura em disco.
        prefect_mode: modo do staging (`"dev"`/`"prod"`) — também resolve
            o projeto BigQuery de destino automaticamente (`MODE_PROJECT`).
        partition_folders: pastas Hive (`chave=valor`, ex.
            `ano=2026/mes=09`) atualizadas nesta execução — só elas são
            promovidas pra prod, não a tabela inteira. Preenchido
            automaticamente por `CheckThenExtractLoadPipeline.run_extract_and_load()`
            (`discover_partition_folders(data_path)`), nunca pelo
            dataset — não é parâmetro do construtor. `None` pra tabela
            sem partição (nenhuma pasta Hive achada em `data_path`).
        dump_mode: modo de escrita no BigQuery.
        source_format: formato do arquivo em `data_path`.
    """

    coverage: dict
    data_path: str
    prefect_mode: str = "prod"
    dump_mode: str = "append"
    source_format: str = "csv"
    partition_folders: list[str] | None = field(default=None, init=False)


def discover_partition_folders(data_path: str) -> list[str] | None:
    """Acha as pastas-folha Hive (`chave=valor`) escritas em `data_path`.

    Genérico pra qualquer profundidade de partição (`ano=/mes=`,
    `ano=/mes=/sigla_uf=`, etc.) — não precisa saber de antemão quais
    partições existem, só que `data_path` segue a convenção Hive (nenhuma
    subpasta que não seja partição).

    Args:
        data_path: caminho local que `extract_load_data` escreveu.

    Returns:
        Caminhos relativos a `data_path` das pastas-folha, ordenados;
        `None` se `data_path` não existe ou não tem nenhuma subpasta
        (tabela sem partição).
    """
    base = Path(data_path)
    if not base.exists():
        return None
    leaves = [
        str(p.relative_to(base))
        for p in base.rglob("*")
        if p.is_dir() and not any(c.is_dir() for c in p.iterdir())
    ]
    return sorted(leaves) or None


def deploy_tags(
    dataset_id: str, etapa: Etapa, table_id: str | None = None
) -> list[str]:
    """Tags de deploy pra achar deployments relacionados no Prefect UI/CI.

    Usar em `<flow>.deploy_tags = deploy_tags(...)`.

    Args:
        dataset_id: ID do dataset.
        etapa: etapa do flow.
        table_id: ID da tabela, quando o dataset tem mais de uma — adiciona
            a tag `<dataset_id>__<table_id>`, pra filtrar uma tabela
            específica sem misturar com as outras do mesmo dataset (a tag
            `<dataset_id>` sozinha não distingue).

    Returns:
        Lista com `"staged-pipeline"` (todo flow que passa por essa
        cápsula, pra achar todos os migrados de uma vez — issue #1867), a
        tag da etapa, `<dataset_id>`, e `<dataset_id>__<table_id>` quando
        `table_id` é passado — todas sem prefixo, mesmo padrão da tag de
        etapa.
    """
    tags = ["staged-pipeline", str(etapa), dataset_id]
    if table_id is not None:
        tags.append(f"{dataset_id}__{table_id}")
    return tags


def _flow_name(dataset_id: str, etapa: Etapa) -> str:
    """Nome do `@flow` por convenção: `"<etapa>: <dataset_id>"`.

    Args:
        dataset_id: ID do dataset (ou `prefect_dataset_id`, quando o
            dataset tem mais de uma tabela).
        etapa: etapa do flow.

    Returns:
        O nome formatado do flow.
    """
    return f"{etapa}: {dataset_id}"


def _dev_prefix() -> str:
    """Prefixo `"dev-"` do deployment atual, ou `""` fora do pool de dev.

    Lê `prefect.runtime.flow_run.tags` (já disponível no contexto do flow
    run, sem chamada de API) procurando a tag `env:dev`, que `deploy_flow()`
    (`.github/workflows/scripts/deploy_flows.py`) adiciona só quando o
    deploy é pro pool `basedosdados-dev`. Fora de um flow run real (ex.
    chamada local em teste), `runtime.flow_run.tags` vem vazio — resolve
    pra `""`, mesmo comportamento de hoje (assume prod).

    Returns:
        `"dev-"` quando a tag `env:dev` está presente; `""` caso contrário.
    """
    return "dev-" if "env:dev" in runtime.flow_run.tags else ""


def _is_local() -> bool:
    """`True` quando o flow run atual está no pool local do `scripts/dev_local.sh`.

    Lê a tag `env:local` (já disponível no contexto do flow run, sem
    chamada de API), que `deploy_flow()` (`deploy_flows.py`) adiciona só
    quando o deploy é feito com `--local` — mesmo padrão de `_dev_prefix()`.

    Returns:
        `True` se a tag `env:local` está presente nas tags do flow run.
    """
    return "env:local" in runtime.flow_run.tags


def _run_flow_locally(deployment_full_name: str, parameters: dict) -> Any:
    """Roda o próximo estágio localmente, sem passar por `run_deployment()`.

    `run_deployment()` (mesmo com `as_subflow=True`) e uma chamada direta
    de `@flow` de dentro de outro flow já em execução usam o mesmo
    mecanismo interno pra registrar o vínculo pai-filho — uma "tarefa
    fantasma" de rastreio criada via `client.create_task_run()` — que
    quebra com `422 Unprocessable Entity` contra o servidor Prefect local
    (`scripts/dev_local.sh`). Confirmado isoladamente: reproduz igual com
    um subflow puro, sem deployment nenhum envolvido; não reproduz fora de
    um flow run, nem contra o servidor via `curl` direto — parece uma
    interação específica do motor de flows do Prefect 3.5.0 com o cliente
    HTTP nesse cenário (servidor SQLite local), não um bug no dataset.

    Chamar a função crua por trás do `@flow` (`.fn`) evita essa tarefa
    fantasma por completo: as tasks de dentro rodam normalmente (viram
    task runs do flow que despachou), só não aparecem como um flow filho
    separado na UI local — sem importância pra teste local, onde o que
    importa é a lógica rodar de ponta a ponta. `validate_parameters()`
    garante a mesma coerção de tipos (`dict` -> `CoverageSpec`, etc.) que
    `run_deployment()` faria no caminho real.

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


def deployment_name(
    dataset_id: str, etapa: Etapa, deployment: str | None = None
) -> str:
    """Resolve o identificador de um deployment por convenção.

    Mesmo formato `"<flow name>/<deployment name>"` aceito por
    `run_deployment(name=...)`. Repete o prefixo `dev-` (ver `_dev_prefix`)
    quando o flow que está chamando também foi deployado em dev — sem
    isso, o dispatch dev sempre tentaria achar o nome de prod.

    Args:
        dataset_id: ID do dataset (ou `prefect_dataset_id`).
        etapa: etapa do deployment. `build_and_promote` é genérico — um
            deployment só, nome fixo, **sempre no pool de prod**, mesmo
            quando quem despacha rodou em dev (só a service account do
            pool prod tem permissão de escrita real pra promover dado) —
            por isso não leva o prefixo `dev-`, ao contrário das outras
            etapas. Exceção: no pool local, `deploy_flows.py` prefixa
            `dev-` em **tudo** (inclusive `build_and_promote`, que lá não
            existe separado por ambiente) — `_is_local()` cobre esse caso.
        deployment: sobrescreve a segunda metade (depois da barra), quando
            a variável do flow não se chama literalmente `<etapa>`.

    Returns:
        O identificador `"<flow name>/<deployment name>"`.
    """
    if etapa == Etapa.BUILD_AND_PROMOTE:
        prefix = "dev-" if _is_local() else ""
        return f"build_and_promote/{prefix}build_and_promote"
    prefix = _dev_prefix()
    return (
        f"{_flow_name(dataset_id, etapa)}/{prefix}{deployment or str(etapa)}"
    )


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


def pipeline_factory(
    dataset_id: str,
    get_latest_update_factory: Callable[[str], Callable[[], SourceInspection]],
    extract_load_data_factory: Callable[
        [str], Callable[[dict], ExtractAndLoad]
    ],
    **shared_kwargs,
) -> Callable[..., "CheckThenExtractLoadPipeline"]:
    """Fábrica de `CheckThenExtractLoadPipeline` pra datasets multi-tabela.

    Fixa `dataset_id` e qualquer kwarg comum entre as tabelas — cada
    chamada só precisa do `table_id`:

        _make_pipeline = pipeline_factory(
            DATASET_ID, make_get_latest_update, make_extract_load_data,
            date_format="%Y-%m",
        )
        _mes_brasil_pipeline = _make_pipeline(MES_BRASIL_TABLE_ID)

    Args:
        dataset_id: ID do dataset, fixado pra todas as tabelas.
        get_latest_update_factory: recebe `table_id`, devolve o callable
            de check_update correspondente. Quando a checagem é uma
            função só compartilhada por todas as tabelas, passe
            `lambda _: minha_funcao_unica` em vez de uma fábrica de
            verdade.
        extract_load_data_factory: idem, pro callable de extract_and_load.
        **shared_kwargs: repassados a `CheckThenExtractLoadPipeline`,
            sobrescritos por `**overrides` na chamada de cada tabela.

    Returns:
        Uma função `make(table_id, **overrides) -> CheckThenExtractLoadPipeline`.
    """

    def make(table_id: str, **overrides) -> CheckThenExtractLoadPipeline:
        return CheckThenExtractLoadPipeline(
            dataset_id=dataset_id,
            table_id=table_id,
            get_latest_update=get_latest_update_factory(table_id),
            extract_load_data=extract_load_data_factory(table_id),
            **{**shared_kwargs, **overrides},
        )

    return make


class CheckThenExtractLoadPipeline:
    """Interface recomendada pra um `flows.py` de dataset com check_update
    e extract_and_load como estágios separados.

    Encapsula o boilerplate repetido entre os dois estágios (rename do
    flow run, poll/commit/dispatch, dispatch pro build_and_promote) — cada
    dataset só fornece `get_latest_update`/`extract_load_data` com a
    lógica específica dele.

    Attributes:
        dataset_id: ID do dataset no backend/BigQuery.
        table_id: ID da tabela no backend/BigQuery.
        get_latest_update: descobre a data de referência na fonte e
            devolve um `SourceInspection`. Não decide se há dado novo —
            só informa a data.
        extract_load_data: recebe o `download_params` do estágio anterior
            (sempre tem `reference_date`, mais o que `get_latest_update`
            tiver posto em `extra_download_params`), baixa o dado de
            verdade e devolve um `ExtractAndLoad`. Não deve chamar
            `upload_to_gcs` diretamente — a própria cápsula faz isso em
            `run_extract_and_load`.
        prefect_dataset_id: convenção de nome pro Prefect, derivada como
            `f"{dataset_id}__{table_id}"` quando não informada.
        env: backend de destino.
        date_format: formato de data usado no check_update.
        extract_load_deployment: nome do deployment de extract_and_load a
            disparar, quando a variável do flow não se chama literalmente
            `extract_and_load`.

    Example:
        _pipeline = CheckThenExtractLoadPipeline(
            dataset_id=DATASET_ID, table_id=TABLE_ID,
            get_latest_update=minha_logica_de_check,
            extract_load_data=minha_logica_de_extract_load,
        )

        @flow(name=_pipeline.check_update_flow_name, log_prints=True)
        def check_update() -> None:
            _pipeline.run_check_update()
        check_update.deploy_tags = deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE)

        @flow(name=_pipeline.extract_and_load_flow_name, log_prints=True)
        def extract_and_load(download_params: dict) -> None:
            _pipeline.run_extract_and_load(download_params)
        extract_and_load.deploy_tags = deploy_tags(
            DATASET_ID, Etapa.EXTRACT_AND_LOAD
        )
        _pipeline.extract_load_deployment = extract_and_load.fn.__name__
    """

    def __init__(
        self,
        *,
        dataset_id: str,
        table_id: str,
        get_latest_update: Callable[[], SourceInspection],
        extract_load_data: Callable[[dict], ExtractAndLoad],
        prefect_dataset_id: str | None = None,
        env: str = "prod",
        date_format: str = "%Y-%m-%d",
        extract_load_deployment: str | None = None,
    ) -> None:
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.get_latest_update = get_latest_update
        self.extract_load_data = extract_load_data
        self.prefect_dataset_id = (
            prefect_dataset_id or f"{dataset_id}__{table_id}"
        )
        self.env = env
        self.date_format = date_format
        self.extract_load_deployment = extract_load_deployment

    @property
    def check_update_flow_name(self) -> str:
        """Nome do `@flow` de check_update.

        Returns:
            O nome formatado, pronto pra passar em `@flow(name=...)`.
        """
        return _flow_name(self.prefect_dataset_id, Etapa.CHECK_UPDATE)

    @property
    def extract_and_load_flow_name(self) -> str:
        """Nome do `@flow` de extract_and_load.

        Returns:
            O nome formatado, pronto pra passar em `@flow(name=...)`.
        """
        return _flow_name(self.prefect_dataset_id, Etapa.EXTRACT_AND_LOAD)

    def run_check_update(self) -> bool:
        """Corpo completo do estágio check_update.

        Returns:
            `True` se havia dado novo e o extract_and_load foi disparado;
            `False` caso contrário.
        """
        rename_flow_run_dataset_table(
            prefix="Check Update: ",
            dataset_id=self.dataset_id,
            table_id=self.table_id,
        )

        result = self.get_latest_update()

        return check_update_and_dispatch(
            prefect_dataset_id=self.prefect_dataset_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            reference_date=result.reference_date,
            next_deployment=self.extract_load_deployment,
            env=self.env,
            date_format=self.date_format,
            extra_download_params=result.extra_download_params,
            compare_against=result.compare_against,
        )

    def run_extract_and_load(self, download_params: dict) -> None:
        """Corpo completo do estágio extract_and_load.

        Args:
            download_params: dict recebido do estágio anterior via
                `run_deployment()` — sempre tem `reference_date`.
        """
        rename_flow_run_dataset_table(
            prefix="Extract and Load: ",
            dataset_id=self.dataset_id,
            table_id=self.table_id,
        )

        download_result = self.extract_load_data(download_params)
        download_result.partition_folders = discover_partition_folders(
            download_result.data_path
        )

        upload_to_gcs(
            data_path=download_result.data_path,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            bucket_name="basedosdados-dev",
            dump_mode=download_result.dump_mode,
            source_format=download_result.source_format,
        )

        dispatch_build_and_promote(
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            result=download_result,
            env=self.env,
        )
