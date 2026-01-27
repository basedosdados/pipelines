"""
Script para atualizar a tabela br_inep_indicadores_educacionais.brasil_remuneracao_docentes
"""

import zipfile
from pathlib import Path

import basedosdados as bd
import pandas as pd
import requests

input = (
    Path("input")
    / "br_inep_indicadores_educacionais"
    / "brasil_remuneracao_docentes"
)
output = (
    Path("output")
    / "br_inep_indicadores_educacionais"
    / "brasil_remuneracao_docentes"
)

urls = [
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2018/remuneracao_media_docentes/remuneracao_docentes_brasil_2018.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2019/remuneracao_media_docentes/remuneracao_docentes_brasil_2019.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2020/remuneracao_media_docentes/remuneracao_docentes_brasil_2020.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2021/remuneracao_docentes_brasil_regioes_ufs_2021.zip",
]

input.mkdir(exist_ok=True, parents=True)

for url in urls:
    response = requests.get(
        url, headers={"User-Agent": "Mozilla/5.0"}, verify=False
    )
    content_type = response.headers["Content-Type"]

    if content_type != "application/zip":
        raise Exception(f"Content type of {url} is not zip file")

    (input / url.split("/")[-1]).write_bytes(response.content)

for file in input.iterdir():
    if file.suffix == ".zip":
        with zipfile.ZipFile(file) as z:
            z.extractall(input)
            file.unlink()

br_2018 = pd.read_excel(
    input
    / "Remuneracao_docentes_Brasil_2018"
    / "Remuneracao_docentes_Brasil_2018.xlsx",
    skiprows=8,
)
br_2019 = pd.read_excel(
    input
    / "Remuneracao_docentes_Brasil_2019"
    / "Remuneracao_docentes_Brasil_2019.xlsx",
    skiprows=8,
)
br_2020 = pd.read_excel(
    input
    / "Remuneracao_docentes_Brasil_2020"
    / "Remuneracao_docentes_Brasil_2020.xlsx",
    skiprows=8,
)
br_2021 = pd.read_excel(
    input
    / "Remuneracao_docentes_Brasil_Regioes_UFs_2021"
    / "Remuneracao_docentes_Brasil_2021.xlsx",
    skiprows=8,
)

renames = {
    "NU_ANO_CENSO": "ano",
    "DEPENDENCIA": "rede",
    "Escolaridade": "escolaridade",
    "n_censo": "numero_docentes",
    "localizados": "prop_docentes_rais",
    "quartil_1": "rem_bruta_rais_1_quartil",
    "mediana": "rem_bruta_rais_mediana",
    "media": "rem_bruta_rais_media",
    "quartil3": "rem_bruta_rais_3_quartil",
    "desvio_padrao": "rem_bruta_rais_desvio_padrao",
    "carga_media": "carga_horaria_media_semanal",
    "rem_40_horas": "rem_media_40_horas_semanais",
}

df_updated = pd.concat([br_2018, br_2019, br_2020, br_2021]).rename(
    columns=renames, errors="raise"
)[renames.values()]

df_updated = df_updated.loc[df_updated.ano.isin(range(2018, 2022))]

df_updated.rede = df_updated.rede.str.lower().replace({"pública": "publica"})

df_updated.escolaridade.unique()

df_updated.escolaridade = df_updated.escolaridade.str.lower().replace(
    {"com superior": "superior"}
)

df_bd = bd.read_sql(
    "select * from `basedosdados-dev.br_inep_indicadores_educacionais_staging.brasil_remuneracao_docentes`",
    billing_project_id="basedosdados-dev",
)

df_final = pd.concat([df_bd, df_updated[df_bd.columns]])

output.mkdir(exist_ok=True, parents=True)

df_final.to_csv(output / "data.csv", index=False)

tb = bd.Table(
    "br_inep_indicadores_educacionais", "brasil_remuneracao_docentes"
)
tb.create(output / "data.csv", if_table_exists="replace")
