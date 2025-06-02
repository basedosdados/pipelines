"""
Esse script atualiza as seguintes tabelas do dataset br_inep_educacao_especial
para 2024.

- etapa_ensino
- faixa_etaria
- localizacao
- tempo_ensino
- sexo_raca_cor
- tipo_deficiencia
"""

import os
import zipfile
from pathlib import Path

import basedosdados as bd
import pandas as pd
import requests

ROOT = Path("models") / "br_inep_educacao_especial"
INPUT = ROOT / "input"
OUTPUT = ROOT / "output"

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

URL = "https://download.inep.gov.br/dados_abertos/sinopses_estatisticas/sinopses_estatisticas_censo_escolar_2024.zip"

r = requests.get(
    URL, headers={"User-Agent": "Mozilla/5.0"}, verify=False, stream=True
)

with open(INPUT / "2024.zip", "wb") as fd:
    for chunk in r.iter_content(chunk_size=128):
        fd.write(chunk)

with zipfile.ZipFile(INPUT / "2024.zip") as z:
    z.extractall(INPUT)


def read_sheet(sheet_name: str, skiprows: int = 9) -> pd.DataFrame:
    return pd.read_excel(
        INPUT
        / "sinopse_estatistica_censo_escolar_2024"
        / "Sinopse_Estatistica_da_Educação_Basica_2024.xlsx",
        skiprows=skiprows,
        sheet_name=sheet_name,
    )


bd_dir = bd.read_sql(
    "SELECT nome, sigla FROM `basedosdados.br_bd_diretorios_brasil.uf`",
    billing_project_id="basedosdados-dev",
)

# Etapa ensino

df_etapa_ensino_classes_comuns = read_sheet("Classes Comuns 1.40")
df_etapa_ensino_classes_exclusisas = read_sheet("Classes Exclusivas 1.46")

RENAMES_ETAPA_ENSINO_CLASSES_COMUNS = {
    # "Unnamed: 0": "",
    "Unnamed: 1": "uf",
    # "Unnamed: 2": "",
    "Unnamed: 3": "id_municipio",
    # "Unnamed: 4": "",
    # "Total5": "",
    "Creche": "Educação Infantil - Creche",
    "Pré-Escola": "Educação Infantil - Pré-Escola",
    # "Total6": "",
    "Anos Iniciais7": "Ensino Fundamental - Anos Iniciais",
    "Anos Finais8": "Ensino Fundamental - Anos Finais",
    # "Total9": "",
    "Ensino Médio Propedêutico ": "Ensino Médio - Ensino Médio Propedêutico",
    "Ensino Médio Normal/Magistério": "Ensino Médio Normal - Magistério",
    "Curso Técnico Integrado (Ensino Médio Integrado)": "Ensino Médio - Curso Técnico Integrado (Ensino Médio Integrado)",
    # "Unnamed: 15": "",
    # "Total11": "",
    "Associada ao Ensino Médio12": "Educação Profissional - Associada ao Ensino Médio",
    "Curso Técnico Concomitante": "Educação Profissional - Curso Técnico Concomitante",
    "Curso Técnico Subsequente": "Educação Profissional - Curso Técnico Subsequente",
    # "Total13": "",
    "Curso FIC Concomitante": "Educação Profissional - Curso FIC Concomitante",
    "Curso FIC Integrado na Modalidade EJA14": "Educação Profissional - Curso FIC Integrado na Modalidade EJA",
    # "Total15": "",
    "Ensino Fundamental16": "EJA - Ensino Fundamental",
    "Ensino Médio17": "EJA - Ensino Médio",
    # "Total18": "",
    # "Classes Comuns19": "",
    # "Classes Exclusivas20": "",
}

df_etapa_ensino_classes_comuns = (
    df_etapa_ensino_classes_comuns.rename(
        columns=RENAMES_ETAPA_ENSINO_CLASSES_COMUNS, errors="raise"
    )[RENAMES_ETAPA_ENSINO_CLASSES_COMUNS.values()]
    .pipe(
        lambda d: d.loc[
            (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
        ]
    )
    .pipe(
        lambda d: pd.melt(
            d,
            id_vars=["id_municipio", "uf"],
            value_vars=[
                c for c in d.columns if c not in ["id_municipio", "uf"]
            ],
            value_name="quantidade_matricula",
        )
    )
    .assign(tipo_classe="Classes Comuns")
)

df_etapa_ensino_classes_exclusisas = (
    df_etapa_ensino_classes_exclusisas.rename(
        columns=RENAMES_ETAPA_ENSINO_CLASSES_COMUNS, errors="raise"
    )[RENAMES_ETAPA_ENSINO_CLASSES_COMUNS.values()]
    .pipe(
        lambda d: d.loc[
            (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
        ]
    )
    .pipe(
        lambda d: pd.melt(
            d,
            id_vars=["id_municipio", "uf"],
            value_vars=[
                c for c in d.columns if c not in ["id_municipio", "uf"]
            ],
            value_name="quantidade_matricula",
        )
    )
    .assign(tipo_classe="Classes Exclusivas")
)

pd.concat(
    [df_etapa_ensino_classes_exclusisas, df_etapa_ensino_classes_comuns]
).assign(
    sigla_uf=lambda d: d["uf"]
    .apply(lambda uf: uf.strip())
    .replace({i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")})
).rename(columns={"variable": "etapa_ensino"}).drop(columns=["uf"]).columns
