import time

import geopandas as gpd
import numpy as np
import pandas as pd
from selenium import webdriver
from shapely.geometry import Point

# ---- download to data
driver = webdriver.Chrome()
time.sleep(3)
driver.get("https://ide.sefin.fortaleza.ce.gov.br/downloads")
time.sleep(2)
driver.maximize_window()
time.sleep(2)
driver.find_element("xpath", '//*[@id="menu"]/li[5]/ul/li/span').click()
time.sleep(10)
driver.find_element(
    "xpath",
    '//*[@id="menu"]/li[5]/ul/li/ul/li[2]/li/label/div[2]/table/tbody/tr/td[1]/a',
).click()
time.sleep(15)

# ---- read data
df = pd.read_csv(
    "/d/download/br_ce_fortaleza_sefin_iptu/Faces de quadra.csv", sep=","
)
df = df.rename(
    columns={
        "quadra_face_id": "id_face_quadra",
        "the_geom": "geometria",
        "x": "latitude",
        "y": "longitude",
        "agua": "indicador_agua",
        "esgoto": "indicador_esgoto",
        "galeria_pluvial": "indicador_galeria_pluvial",
        "sarjeta": "indicador_sarjeta",
        "iluminacao_publica": "indicador_iluminacao_publica",
        "arborizacao": "indicador_arborizacao",
    }
)

lista = [
    "indicador_agua",
    "indicador_esgoto",
    "indicador_galeria_pluvial",
    "indicador_sarjeta",
    "indicador_iluminacao_publica",
    "indicador_arborizacao",
]

for variaveis_bool in lista:
    df[variaveis_bool] = df[variaveis_bool].apply(
        lambda x: str(x).replace("Com", "True").replace("Sem", "False")
    )

df["pavimentacao"] = (
    df["pavimentacao"]
    .str.replace("Paralelep.", "Paralelepípedo")
    .replace("Sem", "Sem Pavimentação")
)

df = df.replace("nan", "")
df["logradouro"] = df["logradouro"].apply(lambda x: " ".join(str(x).split()))
geometry = [
    Point(xy) for xy in zip(df["longitude"], df["latitude"], strict=False)
]
gdf = gpd.GeoDataFrame(df, geometry=geometry)
crs = {"init": "epsg:4326"}  # sistema de coordenadas
# Convertendo Point
df["longitude"] = df["longitude"].replace(",", ".")
df["latitude"] = df["latitude"].replace(",", ".")

# Change to Float64
df["longitude"] = df["longitude"].astype(np.float64)
df["latitude"] = df["latitude"].astype(np.float64)

# Assigning the geometry variables
from shapely.geometry.polygon import Point  # noqa: E402

geometry = [
    Point(xy) for xy in zip(df["longitude"], df["latitude"], strict=False)
]

# For being a point, it became point, if polygon, assign polygon and so on
geo_df = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

# para projeções geográficas
gdf = gdf.set_crs(epsg=32724)
gdf = gdf.to_crs(epsg=4326)

gdf = gdf.drop(["latitude", "longitude", "geometria"], axis=1)
gdf = gdf.rename(columns={"geometry": "geometria"})
gdf = gdf[
    [
        "ano",
        "id_face_quadra",
        "logradouro",
        "metrica",
        "pavimentacao",
        "indicador_agua",
        "indicador_esgoto",
        "indicador_galeria_pluvial",
        "indicador_sarjeta",
        "indicador_iluminacao_publica",
        "indicador_arborizacao",
        "geometria",
        "valor",
    ]
]

gdf.to_csv(
    "/d/download/br_ce_fortaleza_sefin_iptu/microdados.csv'",
    index=False,
    encoding="utf-8",
    sep=",",
    na_rep="",
)
