# -*- coding: utf-8 -*-
"""
Utils for br_sfb_sicar
"""
import geopandas as gpd
import zipfile
import os
import pandas as pd
import tempfile
from datetime import datetime
from pipelines.utils.utils import log
import httpx
import time as tm
import shutil

def unpack_zip(zip_file_path: str) -> str:
    """
    Descompacta um arquivo ZIP em um diretório temporário.

    Args:
        zip_file_path (str): Caminho para o arquivo ZIP.

    Retorna:
        str: Caminho do diretório temporário onde os arquivos foram extraídos.

    Exceções:
        zipfile.BadZipFile: Se o arquivo ZIP estiver corrompido ou inválido.
    """
    temp_dir = tempfile.mkdtemp()

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        try:
            zip_ref.extractall(temp_dir)
        except zipfile.BadZipFile as e:
            log(f"O arquivo zip {zip_file_path.split('/')[-1]} está provavelmente corrompido. {e}")
            raise e
    return temp_dir

def convert_shp_to_parquet(shp_file_path: str, output_parquet_path: str, uf_relase_dates: dict, sigla_uf: str) -> None:
    """
    Converte um arquivo .shp para o formato Parquet, incluindo a geometria como WKT.

    Args:
        shp_file_path (str): Caminho para o arquivo .shp.
        output_parquet_path (str): Caminho onde o arquivo Parquet será salvo.
        uf_relase_dates (dict): Dicionário com as datas de lançamento por estado.
        sigla_uf (str): Sigla do estado sendo processado.

    Exceções:
        FileNotFoundError: Se o arquivo .shp não for encontrado.
        ValueError: Se houver erro na conversão de data ou formatação incorreta.
        RuntimeError: Se houver problemas na conversão da geometria para WKT.
    """
    try:
        gdf = gpd.read_file(shp_file_path)
    except FileNotFoundError as e:
        log(f"Arquivo .shp {shp_file_path} não encontrado. {e}")
        raise e

    try:
        # Convertendo geometria para WKT
        gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt)
    except Exception as e:
        log(f"Erro ao converter a geometria para WKT no arquivo {shp_file_path}. {e}")
        raise RuntimeError(f"Erro na conversão de geometria para WKT: {e}")

    # Convertendo para DataFrame do pandas
    df = pd.DataFrame(gdf)

    try:
        # Formatando a data de atualização a partir das informações do estado
        date = datetime.strptime(uf_relase_dates[sigla_uf], '%d/%m/%Y').strftime('%Y-%m-%d')
    except ValueError as e:
        log(f"Erro ao converter a data de lançamento para {sigla_uf}. Verifique o formato no dicionário. {e}")
        raise e

    log(f'A data de atualização do CAR do Estado {sigla_uf} foi {date}')
    df['data_atualizacao_car'] = date

    # Salvando em Parquet
    df.to_parquet(output_parquet_path, index=False)

    del df

def process_all_files(zip_folder: str, output_folder: str, uf_relase_dates: dict) -> None:
    """
    Processa todos os arquivos ZIP em um diretório, descompactando-os e convertendo os arquivos .shp para Parquet.

    Args:
        zip_folder (str): Caminho para o diretório contendo os arquivos ZIP.
        output_folder (str): Caminho para o diretório onde os arquivos Parquet serão salvos.
        uf_relase_dates (dict): Dicionário com as datas de lançamento dos dados por estado.
    """
    for zip_filename in os.listdir(zip_folder):
        if zip_filename.endswith('.zip'):
            zip_file_path = os.path.join(zip_folder, zip_filename)
            sigla_uf = zip_filename.split('_')[0]
            data_particao = datetime.today().strftime('%Y-%m-%d')

            # Descompactar o arquivo em diretório temporário
            unpacked_dir = unpack_zip(zip_file_path)

            try:
                # Encontrar o arquivo .shp
                shp_file = next((f for f in os.listdir(unpacked_dir) if f.endswith('.shp')), None)

                if shp_file:
                    shp_file_path = os.path.join(unpacked_dir, shp_file)
                    sigla_uf_dir = os.path.join(output_folder, f"data_extracao={data_particao}/sigla_uf={sigla_uf}")
                    os.makedirs(sigla_uf_dir, exist_ok=True)

                    output_parquet_path = os.path.join(sigla_uf_dir, f"{os.path.splitext(shp_file)[0]}.parquet")
                    log(f"Salvando {output_parquet_path}")

                    # Converte shapefile para Parquet com geometria em formato WKT
                    convert_shp_to_parquet(shp_file_path, output_parquet_path, uf_relase_dates, sigla_uf)

            finally:
                # Remove o diretório temporário após o processamento
                shutil.rmtree(unpacked_dir)
                log(f"Diretório temporário {unpacked_dir} removido.")

            # Remover o arquivo ZIP após o processamento
            os.remove(zip_file_path)
            log(f"Arquivo ZIP {zip_file_path} removido.")





def retry_download_car(car, state: str, polygon: bool, folder: str, max_retries: int = 8) -> None:
    """
    Faz o download do CAR com tentativas em caso de falhas.
    *NOTE* : A API do CAR é muito instável e timeouts ocorrem com frequência. Por isso a necessidade de tentativas sucessivas de estabelecer conexão e
    finalizar o download.

    Args:
        car: Objeto responsável pelo download.
        state (str): Sigla do estado.
        polygon (bool): Se o download deve incluir ou não o polígono.
        folder (str): Diretório onde o arquivo será salvo.
        max_retries (int): Número máximo de tentativas de download (padrão: 8).

    Exceções:
        httpx.ReadTimeout: Se ocorrer um timeout ao tentar o download.
        Exception: Qualquer outro erro inesperado durante o processo de download.
    """
    retries = 0
    success = False

    while retries < max_retries and not success:
        try:
            car.download_state(state=state, polygon=polygon, folder=folder)
            success = True
            log(f'Download do estado {state} concluído com sucesso.')
        except httpx.ReadTimeout as e:
            retries += 1
            log(f'Erro de timeout ao baixar {state}. Tentativa {retries} de {max_retries}. Exceção: {e}')
            log(f'Tentando novamente em 8 segundos.')
            tm.sleep(8)
            if retries >= max_retries:
                log(f'Falha ao baixar o estado {state} após {max_retries} tentativas.')
                raise e
        except Exception as e:
            log(f'Erro inesperado ao baixar {state}: {e}')
            raise e