"""
Tasks for br_bndes_operacoes_contratadas.

`get_latest_update`/`extract_load_data` reaproveitam as tasks Prefect e
funções puras já existentes em `pipelines/crawler/bndes/` (tasks.py/utils.py)
— só a fiação pro pipeline em estágios (staged pipeline) é nova.

Poll deferido (ver `pipelines/crawler/bndes/flows.py`): o antigo
`_run_operacoes*` já separava poll (`poll_source_for_update_task`, grava só o
Poll) de commit (`commit_source_update_task`, grava o Update logo após
confirmar novidade, antes de baixar) — exatamente o que
`check_update_and_dispatch`/`CheckThenExtractLoadPipeline.run_check_update`
(`pipelines/utils/stage_dispatch.py`) já fazem de forma genérica. Cada
`get_latest_update` abaixo só precisa devolver a data de referência — uma
única chamada síncrona ao `resource_show` do CKAN (`get_source_max_date*`,
já com retry via `@task`); quem decide se há dado novo e faz o poll/commit é
a cápsula.
"""

from collections.abc import Callable

from pipelines.crawler.bndes.tasks import (
    clean_and_partition,
    clean_and_partition_administracao_publica,
    clean_and_partition_exportacao_bens,
    clean_and_partition_exportacao_servicos,
    download_administracao_publica_csv,
    download_exportacao_bens_csv,
    download_exportacao_servicos_csv,
    download_source_csv,
    get_source_max_date,
    get_source_max_date_administracao_publica,
    get_source_max_date_exportacao_bens,
    get_source_max_date_exportacao_servicos,
)
from pipelines.datasets.br_bndes_operacoes_contratadas.constants import (
    COVERAGE,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection

# ──────────────────────────────────────────────────────────────────────────────
# operacoes_indiretas_automaticas / operacoes_nao_automaticas — mesma task
# genérica do crawler (table_id resolve RESOURCE_SHOW_URL/DOWNLOAD_URL/RENAME/
# ORDER_COLUMNS/SCHEMA em `constants.TABLES_CONFIGS`).
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        last_modified = get_source_max_date(table_id=table_id)
        return SourceInspection(
            reference_date=last_modified.date(),
            compare_against="table_update",
        )

    return get_latest_update


def make_extract_load_data(
    table_id: str,
) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        csv_path = download_source_csv(table_id=table_id)
        output_dir = clean_and_partition(csv_path=csv_path, table_id=table_id)

        return ExtractAndLoad(
            coverage=COVERAGE.model_dump(),
            data_path=output_dir,
            dump_mode="overwrite",
            source_format="parquet",
        )

    return extract_load_data


# ──────────────────────────────────────────────────────────────────────────────
# operacoes_administracao_publica — fonte/transform próprios (sem table_id).
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update_administracao_publica() -> SourceInspection:
    last_modified = get_source_max_date_administracao_publica()
    return SourceInspection(
        reference_date=last_modified.date(),
        compare_against="table_update",
    )


def extract_load_data_administracao_publica(
    download_params: dict,
) -> ExtractAndLoad:
    csv_path = download_administracao_publica_csv()
    output_dir = clean_and_partition_administracao_publica(csv_path=csv_path)

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=output_dir,
        dump_mode="overwrite",
        source_format="parquet",
    )


# ──────────────────────────────────────────────────────────────────────────────
# operacoes_exportacao_bens
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update_exportacao_bens() -> SourceInspection:
    last_modified = get_source_max_date_exportacao_bens()
    return SourceInspection(
        reference_date=last_modified.date(),
        compare_against="table_update",
    )


def extract_load_data_exportacao_bens(
    download_params: dict,
) -> ExtractAndLoad:
    csv_path = download_exportacao_bens_csv()
    output_dir = clean_and_partition_exportacao_bens(csv_path=csv_path)

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=output_dir,
        dump_mode="overwrite",
        source_format="parquet",
    )


# ──────────────────────────────────────────────────────────────────────────────
# operacoes_exportacao_servicos — série termina em 2015, sem cron (ver
# flows.py); mesma receita das irmãs, disparo manual.
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update_exportacao_servicos() -> SourceInspection:
    last_modified = get_source_max_date_exportacao_servicos()
    return SourceInspection(
        reference_date=last_modified.date(),
        compare_against="table_update",
    )


def extract_load_data_exportacao_servicos(
    download_params: dict,
) -> ExtractAndLoad:
    csv_path = download_exportacao_servicos_csv()
    output_dir = clean_and_partition_exportacao_servicos(csv_path=csv_path)

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=output_dir,
        dump_mode="overwrite",
        source_format="parquet",
    )
