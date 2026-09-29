"""Building blocks compartilhados pra encadear flows via `run_deployment()`.

`Etapa`, `SourceInspection`, `ExtractAndLoad`: tipos compartilhados entre
as etapas. `deploy_tags`/`deployment_name`/`_flow_name`: convenção de nome
de deployment. `check_update_and_dispatch`/`dispatch_build_and_promote`:
dispatch entre etapas via `run_deployment()`. `pipeline_factory`/
`CheckThenExtractLoadPipeline`: interface recomendada pra um `flows.py` de
dataset na variante padrão (check_update e extract_and_load separados).
"""

import datetime
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum

from prefect.deployments import run_deployment
from prefect.utilities.asyncutils import run_coro_as_sync

from pipelines.utils.metadata.constants import constants as metadata_constants
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
)
from pipelines.utils.tasks import rename_flow_run_dataset_table, upload_to_gcs


class Etapa(StrEnum):
    """As três etapas da arquitetura orientada a eventos.

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
            exatamente na estrutura Hive que `partition_folders` nomeia
            (ex. `data_path/ano=2026/mes=09/dados.csv`) — o upload deriva
            o prefixo de partição no GCS a partir dessa estrutura em
            disco. Se não bater com `partition_folders`, o arquivo sobe
            pro lugar errado e a promoção pra prod não encontra nada.
        targets: ambientes a promover.
        prefect_mode: modo do staging (`"dev"`/`"prod"`) — também resolve
            o projeto BigQuery de destino automaticamente (`MODE_PROJECT`).
        partition_folders: pastas Hive (`chave=valor`, ex.
            `ano=2026/mes=09`) atualizadas nesta execução — só elas são
            promovidas pra prod, não a tabela inteira. `None` (default)
            pra tabela sem partição.
        dump_mode: modo de escrita no BigQuery.
        source_format: formato do arquivo em `data_path`.
    """

    coverage: dict
    data_path: str
    targets: list[str] = field(default_factory=lambda: ["dev", "prod"])
    prefect_mode: str = "prod"
    partition_folders: list[str] | None = None
    dump_mode: str = "append"
    source_format: str = "csv"


def deploy_tags(dataset_id: str, etapa: Etapa) -> list[str]:
    """Tags de deploy pra achar deployments relacionados no Prefect UI/CI.

    Usar em `<flow>.deploy_tags = deploy_tags(...)`.

    Args:
        dataset_id: ID do dataset.
        etapa: etapa do flow.

    Returns:
        Lista com a tag da etapa (sem prefixo) e a tag `dataset:<dataset_id>`.
    """
    return [str(etapa), f"dataset:{dataset_id}"]


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


def deployment_name(
    dataset_id: str, etapa: Etapa, deployment: str | None = None
) -> str:
    """Resolve o identificador de um deployment por convenção.

    Mesmo formato `"<flow name>/<deployment name>"` aceito por
    `run_deployment(name=...)`.

    Args:
        dataset_id: ID do dataset (ou `prefect_dataset_id`).
        etapa: etapa do deployment. `build_and_promote` é genérico — um
            deployment só, nome fixo, compartilhado por todos os
            datasets.
        deployment: sobrescreve a segunda metade (depois da barra), quando
            a variável do flow não se chama literalmente `<etapa>`.

    Returns:
        O identificador `"<flow name>/<deployment name>"`.
    """
    if etapa == Etapa.BUILD_AND_PROMOTE:
        return "build_and_promote/build_and_promote"
    return f"{_flow_name(dataset_id, etapa)}/{deployment or str(etapa)}"


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
    de `next_etapa` via `run_deployment()` (`timeout=0`, `as_subflow=True`).

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
    run_deployment(
        name=deployment_name(prefect_dataset_id, next_etapa, next_deployment),
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
    """Dispara o `build_and_promote` genérico via `run_deployment()`.

    Args:
        dataset_id: ID do dataset no backend/BigQuery.
        table_id: ID da tabela no backend/BigQuery.
        result: o que `extract_load_data` do dataset devolveu.
        env: backend de destino.
    """
    run_deployment(
        name=deployment_name(dataset_id, Etapa.BUILD_AND_PROMOTE),
        parameters={
            "dataset_id": dataset_id,
            "table_id": table_id,
            "coverage": result.coverage,
            "env": env,
            "bq_project": metadata_constants.MODE_PROJECT.value[
                result.prefect_mode
            ],
            "prefect_mode": result.prefect_mode,
            "targets": result.targets,
            "partition_folders": result.partition_folders,
        },
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
        run_coro_as_sync(
            rename_flow_run_dataset_table(
                prefix="Check Update: ",
                dataset_id=self.dataset_id,
                table_id=self.table_id,
            )
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
        run_coro_as_sync(
            rename_flow_run_dataset_table(
                prefix="Extract and Load: ",
                dataset_id=self.dataset_id,
                table_id=self.table_id,
            )
        )

        download_result = self.extract_load_data(download_params)

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
