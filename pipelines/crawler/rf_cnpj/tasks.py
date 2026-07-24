"""
Tasks for br_rf_cnpj
"""

import asyncio
import datetime

from prefect import task

from pipelines.crawler.rf_cnpj.constants import constants as constants_cnpj
from pipelines.crawler.rf_cnpj.utils import (
    build_paths,
    data_url,
    download_unzip_csv,
    process_csv_dicionario,
    process_csv_empresas,
    process_csv_estabelecimentos,
    process_csv_simples,
    process_csv_socios,
    process_manual_dictionaries,
)
from pipelines.utils.utils import log

ufs = constants_cnpj.UFS.value
url = constants_cnpj.URL.value
headers = constants_cnpj.HEADERS.value


@task(retries=3, retry_delay_seconds=30)
def get_data_source_max_date(
    folder_date: str | None = None,
) -> tuple[datetime.datetime, datetime.date]:
    """
    Checks if there are available updates for a specific dataset and table.

    Returns:
        tuple: Returns a tuple with the date extracted from the CNPJs API folder and today date
        to be used as partition
    """

    folder_date, last_modified_date = data_url(
        url=url, folder_date=folder_date
    )
    return folder_date, last_modified_date


@task(retries=3, retry_delay_seconds=30)
def main(
    tables: list[str],
    folder_date: datetime.datetime,
    last_modified_date: datetime.date,
    chunk_size: int = 100000,
) -> str:
    """
    Performs the download, processing, and organization of CNPJ data.

    Args:
        tables (list): A list of tables to be processed.
        folder_date (datetime): Most recent database release extracted from API
        folder_date (datetime): CNPJs max folder date
        last_modified_date (datetime): CNPJs max last modified date

    Returns:
        str: The path to the output folder where the data has been organized.
    """
    arquivos_baixados = []  # List to track already downloaded files
    for table in tables:
        table_configs = constants_cnpj.TABLE_CONFIGS.value[table]

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
            for i in range(0, 10):  # Segmented tables have 10 files by default
                nome_arquivo = f"{table_configs['table_name']}{i}"
                url_download = f"{constants_cnpj.URL.value}{folder_date}/{table_configs['table_name']}{i}.zip"

                if nome_arquivo not in arquivos_baixados:
                    arquivos_baixados.append(nome_arquivo)
                    asyncio.run(download_unzip_csv(url_download, input_path))

                    if table_configs["table_name"] == "Estabelecimentos":
                        process_csv_estabelecimentos(
                            input_path,
                            output_path,
                            last_modified_date,
                            i,
                            chunk_size,
                        )
                    elif table_configs["table_name"] == "Socios":
                        process_csv_socios(
                            input_path,
                            output_path,
                            last_modified_date,
                            i,
                            chunk_size,
                        )
                    elif table_configs["table_name"] == "Empresas":
                        process_csv_empresas(
                            input_path,
                            output_path,
                            last_modified_date,
                            i,
                            chunk_size,
                        )
        else:
            nome_arquivo = f"{table_configs['table_name']}"
            url_download = f"{constants_cnpj.URL.value}{folder_date}/{table_configs['table_name']}.zip"

            if (nome_arquivo not in arquivos_baixados) and not table_configs[
                "manual"
            ]:
                arquivos_baixados.append(nome_arquivo)
                asyncio.run(download_unzip_csv(url_download, input_path))
                log(f"Nome Arquivo: {nome_arquivo}")

            if table_configs["dicionario"]:
                if table_configs["manual"]:
                    process_manual_dictionaries(output_path, table)
                else:
                    process_csv_dicionario(input_path, output_path, table)
            elif table_configs["table_name"] == "Simples":
                process_csv_simples(
                    input_path,
                    output_path,
                    last_modified_date,
                    table,
                    chunk_size,
                )
    return output_path
