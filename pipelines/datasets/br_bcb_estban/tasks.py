"""
Tasks for br_bcb_estban.
"""

from collections.abc import Callable
from datetime import date, datetime

from pipelines.crawler.bcb_estban.tasks import (
    cleaning_data,
    download_table,
    extract_urls_list,
    get_documents_metadata,
    get_id_municipio,
    get_latest_file,
)
from pipelines.datasets.br_bcb_estban.constants import COVERAGE, DATASET_ID
from pipelines.utils.metadata.tasks import task_get_api_most_recent_date
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

# ──────────────────────────────────────────────────────────────────────────────
# As 2 tabelas (agencia, municipio) — ver constants.py
#
# get_latest_update é leve de verdade: só busca o metadado de documentos da
# API do BCB (`get_documents_metadata`/`get_latest_file`), sem baixar nenhum
# arquivo — igual ao flow monolítico antigo (`_run_bcb_estban`), que também
# fazia essa checagem antes de decidir se baixava algo.
#
# `documents_metadata` é repassado pra `extract_load_data` via
# `SourceInspection.extra_download_params` — mesmo padrão de br_ms_cnes
# (`ftp_files`): informação descoberta no check que o download precisa,
# evitando uma segunda chamada à API do BCB.
#
# Igual ao flow antigo, `extract_load_data` pode baixar MAIS de um mês: a
# fonte não garante que o pipeline rode todo mês, então o range vai do que já
# está publicado no backend (`task_get_api_most_recent_date`) até a
# `reference_date` descoberta no check_update — `extract_urls_list` resolve
# esse range, e `cleaning_data` particiona tudo junto em
# ano=/mes=/sigla_uf= (Hive), que `discover_partition_folders` promove
# automaticamente.
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        documents_metadata = get_documents_metadata(table_id)
        if documents_metadata is None:
            raise RuntimeError(
                "BCB metadata was not loaded! It was not possible to "
                "determine if the dataset is up to date."
            )

        _, data_source_max_date = get_latest_file(documents_metadata)
        if data_source_max_date is None:
            # Mesmo fallback de br_ms_cnes: sem documento novo publicado,
            # usa a própria cobertura atual como reference_date, pra
            # poll_source_for_update_task comparar data com data e concluir
            # corretamente "sem dado novo" (SourceInspection.reference_date
            # não aceita None).
            reference_date = task_get_api_most_recent_date(
                dataset_id=DATASET_ID,
                table_id=table_id,
                date_format="%Y-%m",
            )
        else:
            reference_date = datetime.strptime(
                data_source_max_date, "%Y-%m"
            ).date()

        return SourceInspection(
            reference_date=reference_date,
            extra_download_params={"documents_metadata": documents_metadata},
        )

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        documents_metadata = download_params["documents_metadata"]
        data_source_max_date = date.fromisoformat(
            download_params["reference_date"]
        ).strftime("%Y-%m")

        api_max_date = task_get_api_most_recent_date(
            dataset_id=DATASET_ID,
            table_id=table_id,
            date_format="%Y-%m",
        )

        # pyrefly: ignore [no-matching-overload]
        urls_list = extract_urls_list(
            documents_metadata,
            data_source_max_date,
            api_max_date,
            date_format="%Y-%m",
        )

        for url in urls_list:
            download_table(url=url, table_id=table_id)

        df_diretorios = get_id_municipio()
        filepath = cleaning_data(table_id, df_diretorios)

        return ExtractAndLoad(
            coverage=COVERAGE.model_dump(),
            data_path=filepath,
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
    # Mesma granularidade do flow monolítico antigo (`_run_bcb_estban`, que
    # já compara coverage com date_format="%Y-%m" — ESTBAN é mensal).
    date_format="%Y-%m",
)
