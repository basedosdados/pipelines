# -*- coding: utf-8 -*-
"""
Tasks for br_me_cnpj
"""
import asyncio
import os
from typing import Union, List
from datetime import datetime
import basedosdados as bd
from prefect import task

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


@task
def get_data_source_max_date() -> tuple[datetime,datetime]:
    """
    Checks if there are available updates for a specific dataset and table.

    Returns:
        bool: Returns True if updates are available, otherwise returns False.
    """

    folder_date, today_date = data_url(url=url, headers=headers)
    return folder_date, today_date

@task
def main(tabelas:[str], data_atualizacao:datetime)-> str:
    """
    Performs the download, processing, and organization of CNPJ data.

    Args:
        tabelas (list): A list of tables to be processed.
        data_atualizaão: Most recent database release extracted from API

    Returns:
        str: The path to the output folder where the data has been organized.
    """
    arquivos_baixados = []  # Lista para rastrear os arquivos baixados
    for tabela in tabelas:
        sufixo = tabela.lower()

        # Define o caminho para a pasta de entrada (input)
        input_path = f"/tmp/data/br_me_cnpj/input/data={data_atualizacao}/"
        os.makedirs(input_path, exist_ok=True)
        log("Pasta destino input construído")

        # Define o caminho para a pasta de saída (output)
        output_path = destino_output(sufixo, data_atualizacao)

        # Loop para baixar e processar os arquivos
        for i in range(0, 10):
            if tabela != "Simples":
                nome_arquivo = f"{tabela}{i}"
                url_download = f"https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/{data_atualizacao.strftime('%Y-%m')}/{tabela}{i}.zip"
                if nome_arquivo not in arquivos_baixados:
                    arquivos_baixados.append(nome_arquivo)
                    asyncio.run((download_unzip_csv(url_download, input_path)))
                    if tabela == "Estabelecimentos":
                        process_csv_estabelecimentos(
                            input_path, output_path, data_atualizacao, i
                        )
                    elif tabela == "Socios":
                        process_csv_socios(input_path, output_path, data_atualizacao, i)
                    elif tabela == "Empresas":
                        process_csv_empresas(input_path, output_path, data_atualizacao, i)
            else:
                nome_arquivo = f"{tabela}"
                url_download = f"https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/{data_atualizacao.strftime('%Y-%m')}/{tabela}.zip"
                if nome_arquivo not in arquivos_baixados:
                    arquivos_baixados.append(nome_arquivo)
                    asyncio.run((download_unzip_csv(url_download, input_path)))
                    process_csv_simples(input_path, output_path, data_atualizacao, sufixo)

    return output_path
