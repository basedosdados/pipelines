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

def unpack_zip(zip_file_path):
    temp_dir = tempfile.mkdtemp()
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)
    return temp_dir

def convert_shp_to_parquet(shp_file_path, output_parquet_path):

    gdf = gpd.read_file(shp_file_path)
    # Convertendo geometria para WKT
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt)
    # Convertendo para DataFrame do pandas
    df = pd.DataFrame(gdf)
    # Salvando em Parquet


    df.to_parquet(output_parquet_path, index=False)
    #Todo: inserir data de extração como partição

    return output_parquet_path

def process_all_files(zip_folder, output_folder):

    for zip_filename in os.listdir(zip_folder):

        if zip_filename.endswith('.zip'):

            zip_file_path = os.path.join(zip_folder, zip_filename)


            zip_filename = os.path.basename(zip_file_path)
            sigla_uf = zip_filename.split('_')[0]
            data_particao = datetime.today().strftime('%Y-%m-%d')
            # Descompactar o arquivo em diretório temporário
            unpacked_dir = unpack_zip(zip_file_path)

            # Encontrar o arquivo .shp
            shp_file = next((f for f in os.listdir(unpacked_dir) if f.endswith('.shp')), None)

            if shp_file:
                shp_file_path = os.path.join(unpacked_dir, shp_file)


                sigla_uf_dir = os.path.join(output_folder, f"data_extracao={data_particao}/sigla_uf={sigla_uf}")
                os.makedirs(sigla_uf_dir, exist_ok=True)


                output_parquet_path = os.path.join(sigla_uf_dir, f"{os.path.splitext(shp_file)[0]}.parquet")
                log(f"Salvando {output_parquet_path}")

                # Converte shapefile para parquet com coluna WKT para representar geometria
                convert_shp_to_parquet(shp_file_path, output_parquet_path)
