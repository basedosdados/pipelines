"""Interface que os `flows.py` usam para montar o pipeline em estágios.

`CheckThenExtractLoadPipeline` concentra o corpo do `check_update` e do
`extract_and_load` de uma tabela; cada conjunto fornece só a função que lê
a data de referência na fonte e a que baixa o dado. `pipeline_factory` cria
um pipeline por tabela nos conjuntos com várias tabelas, e
`discover_partition_folders` acha as partições que o `extract_and_load`
gravou.
"""

from collections.abc import Callable
from pathlib import Path

from pipelines.utils.metadata.domain import CoverageSpec
from pipelines.utils.stage_dispatch.contracts import (
    Etapa,
    ExtractAndLoad,
    SourceInspection,
)
from pipelines.utils.stage_dispatch.derived_tables import (
    promote_primary_and_derived_tables,
)
from pipelines.utils.stage_dispatch.dispatch import (
    check_update_and_dispatch,
    dispatch_build_and_promote,
)
from pipelines.utils.stage_dispatch.naming import _flow_name
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    upload_to_gcs,
)


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
        """Cria o pipeline de `table_id`, usando as fábricas fixadas em
        `pipeline_factory`.
        """
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
        """Inicializa o pipeline — ver atributos na docstring da classe."""
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

    def run_extract_and_load_with_derived_tables(
        self,
        download_params: dict,
        *,
        coverage: CoverageSpec,
        derived_table_ids: list[str],
        derived_coverage: CoverageSpec | dict[str, CoverageSpec],
    ) -> None:
        """Corpo completo do estágio extract_and_load — tabela mãe + derivadas.

        Mesmo papel de `run_extract_and_load`, pra quando a tabela mãe tem
        tabelas derivadas sem extração própria — só `dbt run/test` lendo
        dela (ex. agregados calculados a partir de um `microdados`).
        Termina em `promote_primary_and_derived_tables` em vez de
        `dispatch_build_and_promote` (que só sabe materializar uma tabela
        por vez, cada uma baixando seu próprio staging de dev — as
        derivadas não têm nenhum). Ver `promote_primary_and_derived_tables`
        pro motivo completo.

        Não preenche `partition_folders` (ao contrário de
        `run_extract_and_load`) — `promote_primary_and_derived_tables`
        ainda não tem suporte a promover só uma fatia particionada: hoje
        só serve pra tabela mãe sem partição.

        Args:
            download_params: dict recebido do estágio anterior via
                `run_deployment()` — sempre tem `reference_date`.
            coverage: `CoverageSpec` da tabela mãe.
            derived_table_ids: tabelas derivadas, sem download próprio —
                só `dbt run/test` lendo da mãe.
            derived_coverage: `CoverageSpec` das derivadas. Uma só,
                aplicada a todas, ou um dict `{table_id: CoverageSpec}`
                quando elas não compartilham a mesma cobertura.
        """
        rename_flow_run_dataset_table(
            prefix="Extract and Load: ",
            dataset_id=self.dataset_id,
            table_id=self.table_id,
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

        promote_primary_and_derived_tables(
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            data_path=download_result.data_path,
            coverage=coverage,
            derived_table_ids=derived_table_ids,
            derived_coverage=derived_coverage,
            env=self.env,
            prefect_mode=download_result.prefect_mode,
            dump_mode=download_result.dump_mode,
            source_format=download_result.source_format,
        )
