"""
Tasks for br_rf_cnpj
"""

import asyncio
import datetime
from collections.abc import Callable
from pathlib import Path

from prefect import task

from pipelines.datasets.br_rf_cnpj.constants import (
    COMPONENTS_SPECS,
    COVERAGE,
    CSV_CHUNK_SIZE,
    DATASET_ID,
    DOWNLOAD_CHUNK_SIZE,
    DOWNLOAD_MAX_PARALLEL,
    DOWNLOAD_MAX_RETRIES,
    DOWNLOAD_TIMEOUT,
    FOLDER_DATE_FORMAT,
    NON_HISTORICAL_TABLES,
    TABLE_COMPONENTS,
    URL,
)
from pipelines.datasets.br_rf_cnpj.utils import (
    build_paths,
    data_url,
    download_unzip_csv,
    get_table_files,
    process_csv_dicionario,
    process_csv_empresas,
    process_csv_estabelecimentos,
    process_csv_simples,
    process_csv_socios,
    process_manual_dictionaries,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)
from pipelines.utils.utils import log


@task(retries=3, retry_delay_seconds=30)
def get_data_source_max_date(
    folder_date: str | None = None,
) -> tuple[str, datetime.date]:
    """
    Looks up the latest release published by the Receita Federal.

    Returns:
        tuple: the latest folder date on the source ("%Y-%m", the competência the
        data refers to) and the max last-modified date of the source files.
    """
    return data_url(url=URL, folder_date=folder_date)


"""
São 5 tabelas
1. `check_update`
    CHECK LEVE - PROPFIND na listagem WebDAV da Receita Federal)
        - empresas/estabelecimentos/socios: compara-se a competência (`folder_date`) 
        contra `Coverage`;
        - simples/dicionario (NonHistorical): `last_modified_date` contra
        `Table.Update`.
2. `extract_and_load`: `folder_date` e `last_modified_date` (em `extra_download_params`) usados em 
`main`
"""


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        folder_date, last_modified_date = get_data_source_max_date()
        extra_download_params = {
            "folder_date": folder_date,
            "last_modified_date": last_modified_date.isoformat(),
        }

        if table_id in NON_HISTORICAL_TABLES:
            return SourceInspection(
                reference_date=last_modified_date,
                extra_download_params=extra_download_params,
                compare_against="table_update",
            )

        return SourceInspection(
            reference_date=datetime.datetime.strptime(
                folder_date, FOLDER_DATE_FORMAT
            ).date(),
            extra_download_params=extra_download_params,
        )

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        output_path = main(
            tables=TABLE_COMPONENTS[table_id],
            folder_date=download_params["folder_date"],
            last_modified_date=datetime.date.fromisoformat(
                download_params["last_modified_date"]
            ),
        )
        return ExtractAndLoad(
            coverage=COVERAGE[table_id].model_dump(),
            data_path=str(output_path),
        )

    return extract_load_data


@task(retries=3, retry_delay_seconds=30)
def main(
    tables: list[str],
    folder_date: str,
    last_modified_date: datetime.date,
    chunk_size: int = CSV_CHUNK_SIZE,
    download_chunk_size: int = DOWNLOAD_CHUNK_SIZE,
    download_max_retries: int = DOWNLOAD_MAX_RETRIES,
    download_max_parallel: int = DOWNLOAD_MAX_PARALLEL,
    download_timeout: int = DOWNLOAD_TIMEOUT,
) -> Path:
    """
    Performs the download, processing, and organization of CNPJ data.

    Args:
        tables (list): A list of tables to be processed.
        folder_date (datetime | str): CNPJs max folder date
        last_modified_date (datetime | str): CNPJs max last modified date
        chunk_size (int): size of csv chunks

    Returns:
        str: The path to the output folder where the data has been organized.
    """
    arquivos_baixados = []  # List to track already downloaded files
    for table in tables:
        table_configs = COMPONENTS_SPECS[table]

        # Creates dataset table paths (input and output)

        if table_configs["dicionario"]:
            if table_configs["manual"] is False:
                input_path, _ = build_paths(table_id=table, build_output=False)
            _, output_path = build_paths(
                table_id="dicionario", build_input=False
            )
        else:
            input_path, output_path = build_paths(table_id=table)

        if table_configs["segmentada"]:
            files = get_table_files(
                table_configs["table_name"],
                f"{URL}{folder_date}",
            )
            for i, item in enumerate(files):
                nome_arquivo = item[0]
                url_download = item[1]

                if nome_arquivo not in arquivos_baixados:
                    arquivos_baixados.append(nome_arquivo)
                    asyncio.run(
                        download_unzip_csv(
                            url_download,
                            # pyrefly: ignore [bad-argument-type]
                            # pyrefly: ignore [unbound-name]
                            input_path,
                            chunk_size=download_chunk_size,
                            max_retries=download_max_retries,
                            max_parallel=download_max_parallel,
                            timeout=download_timeout,
                        )
                    )

                    if table_configs["table_name"] == "Estabelecimentos":
                        process_csv_estabelecimentos(
                            # pyrefly: ignore [bad-argument-type]
                            input_path,
                            # pyrefly: ignore [bad-argument-type]
                            output_path,
                            folder_date,
                            last_modified_date,
                            i,
                            chunk_size,
                        )

                    elif table_configs["table_name"] == "Socios":
                        process_csv_socios(
                            # pyrefly: ignore [bad-argument-type]
                            input_path,
                            # pyrefly: ignore [bad-argument-type]
                            output_path,
                            folder_date,
                            last_modified_date,
                            i,
                            chunk_size,
                        )
                    elif table_configs["table_name"] == "Empresas":
                        process_csv_empresas(
                            # pyrefly: ignore [bad-argument-type]
                            input_path,
                            # pyrefly: ignore [bad-argument-type]
                            output_path,
                            folder_date,
                            last_modified_date,
                            i,
                            chunk_size,
                        )
        else:
            nome_arquivo = f"{table_configs['table_name']}"
            url_download = (
                f"{URL}{folder_date}/{table_configs['table_name']}.zip"
            )

            if (nome_arquivo not in arquivos_baixados) and not table_configs[
                "manual"
            ]:
                arquivos_baixados.append(nome_arquivo)
                # pyrefly: ignore [bad-argument-type]
                # pyrefly: ignore [unbound-name]
                asyncio.run(download_unzip_csv(url_download, input_path))
                log(f"Nome Arquivo: {nome_arquivo}")

            if table_configs["dicionario"]:
                if table_configs["manual"]:
                    # pyrefly: ignore [bad-argument-type]
                    process_manual_dictionaries(output_path, table)
                else:
                    # pyrefly: ignore [bad-argument-type]
                    process_csv_dicionario(input_path, output_path, table)
            elif table_configs["table_name"] == "Simples":
                process_csv_simples(
                    # pyrefly: ignore [bad-argument-type]
                    input_path,
                    # pyrefly: ignore [bad-argument-type]
                    output_path,
                    folder_date,
                    last_modified_date,
                    table,
                    chunk_size,
                )
    # pyrefly: ignore[bad-return]
    # pyrefly: ignore [unbound-name]
    return output_path


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
)
