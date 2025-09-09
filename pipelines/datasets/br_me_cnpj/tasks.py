"""
Tasks for br_me_cnpj
"""

import asyncio
import os
from datetime import datetime, timedelta

from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_me_cnpj.constants import constants as constants_cnpj
from pipelines.datasets.br_me_cnpj.utils import (
    data_url,
    destino_output,
    download_unzip_csv,
    process_csv_empresas,
    process_csv_estabelecimentos,
    process_csv_simples,
    process_csv_socios,
)
from pipelines.utils.utils import log

ufs = constants_cnpj.UFS.value
url = constants_cnpj.URL.value
headers = constants_cnpj.HEADERS.value


@task(
    max_retries=3,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_data_source_max_date() -> tuple[datetime, datetime.date]:
    """
    Checks if there are available updates for a specific dataset and table.

    Returns:
        tuple: Returns a tuple with the date extracted from the CNPJs API folder and today date
        to be used as partition
    """

    max_folder_date, max_table_date = data_url(url=url, headers=headers)
    return max_folder_date, max_table_date


@task(
    max_retries=3,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def main(
    tabelas: [str],
    max_folder_date: datetime,
    max_last_modified_date: datetime.date,
) -> str:
    """
    Performs the download, processing, and organization of CNPJ data.

    Args:
        tabelas (list): A list of tables to be processed.
        folder_date (datetime): Most recent database release extracted from API
        max_folder_date (datetime): CNPJs max folder date
        max_last_modified_date (datetime): CNPJs max last modified date

    Returns:
        str: The path to the output folder where the data has been organized.
    """
    arquivos_baixados = []  # Lista para rastrear os arquivos baixados
    for tabela in tabelas:
        sufixo = tabela.lower()

        # Define o caminho para a pasta de entrada (input)
        input_path = (
            f"/tmp/data/br_me_cnpj/input/data={max_last_modified_date}/"
        )
        os.makedirs(input_path, exist_ok=True)
        log("Pasta destino input construído")

        # Define o caminho para a pasta de saída (output)
        output_path = destino_output(sufixo, max_last_modified_date)

        # Loop para baixar e processar os arquivos
        for i in range(0, 10):
            if tabela != "Simples":
                nome_arquivo = f"{tabela}{i}"
                url_download = f"https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/{max_folder_date}/{tabela}{i}.zip"
                if nome_arquivo not in arquivos_baixados:
                    arquivos_baixados.append(nome_arquivo)
                    asyncio.run(download_unzip_csv(url_download, input_path))
                    if tabela == "Estabelecimentos":
                        process_csv_estabelecimentos(
                            input_path, output_path, max_last_modified_date, i
                        )
                    elif tabela == "Socios":
                        process_csv_socios(
                            input_path, output_path, max_last_modified_date, i
                        )
                    elif tabela == "Empresas":
                        process_csv_empresas(
                            input_path, output_path, max_last_modified_date, i
                        )
            else:
                nome_arquivo = f"{tabela}"
                url_download = f"https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/{max_folder_date}/{tabela}.zip"
                if nome_arquivo not in arquivos_baixados:
                    arquivos_baixados.append(nome_arquivo)
                    asyncio.run(download_unzip_csv(url_download, input_path))
                    process_csv_simples(
                        input_path, output_path, max_last_modified_date, sufixo
                    )

    return output_path
