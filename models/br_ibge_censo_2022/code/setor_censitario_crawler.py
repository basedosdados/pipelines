# -*- coding: utf-8 -*-
from io import BytesIO
import tempfile
import zipfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fiona
import geopandas as gpd
import requests
from tqdm import tqdm
import basedosdados as bd

def download_files_from_ftp(url: str) -> BytesIO:
    """
    Baixa um arquivo do FTP e retorna seu conteúdo em memória.

    Parâmetros:
    url (str): URL do arquivo a ser baixado.

    Retorna:
    BytesIO: Conteúdo do arquivo baixado em memória.
    """
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024  # 1 Kibibyte
    t = tqdm(total=total_size, unit='iB', unit_scale=True)
    file_data = BytesIO()
    for data in response.iter_content(block_size):
        t.update(len(data))
        file_data.write(data)
    t.close()
    return file_data

def convert_shp_to_parquet(
    shp_file_path: str,
    output_parquet_path: str,
    chunk_size: int = 100000
) -> None:
    """
    Converte um arquivo .shp para o formato Parquet, incluindo a geometria como WKT e processando em chunks.

    Args:
        shp_file_path (str): Caminho para o arquivo .shp.
        output_parquet_path (str): Caminho onde o arquivo Parquet será salvo.
        uf_relase_dates (dict): Dicionário com as datas de lançamento por estado.
        sigla_uf (str): Sigla do estado sendo processado.
        chunk_size (int): Número de registros a serem processados por chunk.

    Exceções:
        FileNotFoundError: Se o arquivo .shp não for encontrado.
        ValueError: Se houver erro na conversão de data ou formatação incorreta.
        RuntimeError: Se houver problemas na conversão da geometria para WKT.
    """
    # Abrir o shapefile usando fiona
    try:
        print(f"Lendo arquivo {shp_file_path}")
        with fiona.open(shp_file_path, 'r') as src:
            total_features = len(src)
            print(f"Total de registros no shapefile: {total_features}")

            features = []
            writer = None  # Inicializa o writer como None

            bol = 1
            for i, feature in enumerate(src, 1):
                features.append(feature)
                # Quando atingir o tamanho do chunk ou último registro
                if len(features) == chunk_size or i == total_features:
                    # Converter lista de features para GeoDataFrame
                    gdf_chunk = gpd.GeoDataFrame.from_features(features, crs=src.crs)
                    try:
                        # Convertendo geometria para WKT
                        gdf_chunk['geometry'] = gdf_chunk['geometry'].apply(lambda geom: geom.wkt)
                    except Exception as e:
                        print(f"Erro ao converter a geometria para WKT no chunk que termina no registro {i}. {e}")
                        raise RuntimeError(f"Erro na conversão de geometria para WKT: {e}")

                    # Converter para pandas DataFrame
                    df_chunk = pd.DataFrame(gdf_chunk)
                    if bol:
                        print(df_chunk.columns)
                        bol = 0
                    # Converter para PyArrow Table
                    table_chunk = pa.Table.from_pandas(df_chunk)

                    # Inicializar o ParquetWriter com o esquema na primeira iteração
                    if writer is None:
                        schema = table_chunk.schema
                        writer = pq.ParquetWriter(output_parquet_path, schema)

                    # Escrever o chunk no arquivo Parquet
                    writer.write_table(table_chunk)
                    print(f"Chunk até registro {i} escrito no arquivo Parquet.")

                    # Limpar lista de features
                    features = []
                    del gdf_chunk, df_chunk, table_chunk



            # Fechar o writer após escrever todos os chunks
            if writer is not None:
                writer.close()

    except FileNotFoundError as e:
        print(f"Arquivo .shp {shp_file_path} não encontrado. {e}")
        raise e
    except Exception as e:
        print(f"Erro inesperado ao processar o arquivo {shp_file_path}: {e}")
        raise e

    print(f"Arquivo Parquet {output_parquet_path} salvo com sucesso.")

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
            print(f"O arquivo zip {zip_file_path.split('/')[-1]} está provavelmente corrompido. {e}")
            raise e
    return temp_dir

def convert_gpkg_to_parquet(gpkg_file_path, output_parquet_path):
    # Read the GPKG file using geopandas
    gdf = gpd.read_file(gpkg_file_path)

    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt)

    gdf.to_parquet(output_parquet_path, index=False)
    print(f"Data saved to {output_parquet_path}")



if __name__== '__main__':

    # NOTE: Escolhemos usar os arquivos shp porque os arquivos gpkg estão com alguns setores duplicados (com poligonos diferentes para o mesmo setor)
    # NOTE: Iniciamos usando a malha preliminar, posteriormente foi substituida pela malha oficial
    # NOTE: Na malha oficial os arquivs gpkg continuam com setores duplicados
    # FTP_SETOR_CENSITARIO = "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/malha_com_atributos/"
    # FTP_SETOR_CENSITARIO_PRELIMINAR = "http://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios_preliminares/agregados_por_setores_csv/"

    SHP_FILE_PATH = '/home/laura/Documents/conjuntos/br_ibge_censo_2022/setor_censitario/shp_files/BR_setores_CD2022.shp'
    GPKG_FILE_PATH = '/home/laura/Documents/conjuntos/br_ibge_censo_2022/setor_censitario/BR_setores_CD2022.gpkg'
    OUTPUT_PARQUET_PATH ="/home/laura/Documents/conjuntos/br_ibge_censo_2022/BR_setores_CD2022.parquet"
    OUTPUT_PARQUET_PATH_V2 ="/home/laura/Documents/conjuntos/br_ibge_censo_2022/BR_setores_CD2022_v2.parquet"

    # convert_gpkg_to_parquet(gpkg_file_path=GPKG_FILE_PATH,
    #                        output_parquet_path=OUTPUT_PARQUET_PATH)
    convert_shp_to_parquet(shp_file_path=SHP_FILE_PATH,
                           output_parquet_path=OUTPUT_PARQUET_PATH_V2)

    tb = bd.Table(dataset_id='br_ibge_censo_2022', table_id='setor_censitario')
    tb.create(path=OUTPUT_PARQUET_PATH_V2,
              source_format='parquet',
              if_table_exists='replace',
              if_storage_data_exists='pass')
