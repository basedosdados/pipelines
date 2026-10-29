"""Tipos que passam entre as etapas do pipeline em estágios.

`Etapa` nomeia as três etapas. `SourceInspection` e `ExtractAndLoad` são o
que as funções de cada conjunto devolvem ao `check_update` e ao
`extract_and_load`.
"""

import datetime
from dataclasses import dataclass, field
from enum import StrEnum


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
