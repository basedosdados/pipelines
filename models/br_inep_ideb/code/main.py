# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
import zipfile
import basedosdados as bd


INPUT = os.path.join(os.getcwd(), "input")
OUTPUT = os.path.join(os.getcwd(), "output")

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

URLS = {
    "municipios_ef_anos_iniciais": "https://download.inep.gov.br/educacao_basica/portal_ideb/planilhas_para_download/2019/divulgacao_anos_iniciais_municipios_2019.zip",
    "municipios_ef_anos_finais": "https://download.inep.gov.br/educacao_basica/portal_ideb/planilhas_para_download/2019/divulgacao_anos_finais_municipios_2019.zip",
    "escolas_ef_anos_iniciais": "https://download.inep.gov.br/educacao_basica/portal_ideb/planilhas_para_download/2019/divulgacao_anos_iniciais_escolas_2019.zip",
    "escolas_ef_anos_finais": "https://download.inep.gov.br/educacao_basica/portal_ideb/planilhas_para_download/2019/divulgacao_anos_finais_escolas_2019.zip",
}


for name, url in URLS.items():
    os.system(f"cd {INPUT}; curl -O -k {url}")

for file in os.listdir(INPUT):
    with zipfile.ZipFile(os.path.join(INPUT, file), "r") as z:
        z.extractall(INPUT)


cols = ["VL_NOTA_MATEMATICA_2015", "VL_NOTA_PORTUGUES_2015"]

cols_municipio = ["CO_MUNICIPIO", "REDE", *cols]

cols_escolas = ["CO_MUNICIPIO", "ID_ESCOLA", "REDE", *cols]


def to_float(value) -> float:
    return (
        float(value.replace(",", ".").replace("*", ""))
        if value not in ["-", "ND", "ND*", "ND**"]
        else np.nan
    )


def sanitize_dataframe(df: pd.DataFrame, table_escolas: bool) -> pd.DataFrame:
    df = df[cols_escolas if table_escolas else cols_municipio]  # type: ignore

    df = df.loc[df["CO_MUNICIPIO"].notna(),]

    df["CO_MUNICIPIO"] = df["CO_MUNICIPIO"].astype("Int64").astype("string")

    if table_escolas:
        df["ID_ESCOLA"] = df["ID_ESCOLA"].astype("Int64").astype("string")

    df["REDE"] = df["REDE"].str.lower()

    df["VL_NOTA_MATEMATICA_2015"] = df["VL_NOTA_MATEMATICA_2015"].apply(to_float)

    df["VL_NOTA_PORTUGUES_2015"] = df["VL_NOTA_PORTUGUES_2015"].apply(to_float)

    return df


escolas_inicial = pd.read_excel(
    os.path.join(
        INPUT,
        "divulgacao_anos_iniciais_escolas_2019",
        "divulgacao_anos_iniciais_escolas_2019.xlsx",
    ),
    skiprows=9,
)

escolas_inicial = sanitize_dataframe(escolas_inicial, table_escolas=True)

escolas_final = pd.read_excel(
    os.path.join(
        INPUT,
        "divulgacao_anos_finais_escolas_2019",
        "divulgacao_anos_finais_escolas_2019.xlsx",
    ),
    skiprows=9,
)

escolas_final = sanitize_dataframe(escolas_final, table_escolas=True)

bd_escolas_inicial = bd.read_sql(
    """
SELECT
  *
FROM
  `basedosdados.br_inep_ideb.escola`
WHERE
  ano = 2015
  and anos_escolares = "iniciais (1-5)"
""",
    billing_project_id="basedosdados-dev",
)

bd_escolas_final = bd.read_sql(
    """
SELECT
  *
FROM
  `basedosdados.br_inep_ideb.escola`
WHERE
  ano = 2015
  and anos_escolares = "finais (6-9)"
""",
    billing_project_id="basedosdados-dev",
)

fixed_escolas_inicial = (
    bd_escolas_inicial.merge(  # type: ignore
        escolas_inicial,
        left_on=["id_municipio", "id_escola", "rede"],
        right_on=["CO_MUNICIPIO", "ID_ESCOLA", "REDE"],
    )
    .drop(
        columns=[
            "nota_saeb_matematica",
            "nota_saeb_lingua_portuguesa",
            "REDE",
            "CO_MUNICIPIO",
            "ID_ESCOLA",
        ]
    )
    .rename(
        columns={
            "VL_NOTA_MATEMATICA_2015": "nota_saeb_matematica",
            "VL_NOTA_PORTUGUES_2015": "nota_saeb_lingua_portuguesa",
        },
        errors="raise",
    )[bd_escolas_inicial.columns]  # type: ignore
)

fixed_escolas_final = (
    bd_escolas_final.merge(  # type: ignore
        escolas_final,
        left_on=["id_municipio", "id_escola", "rede"],
        right_on=["CO_MUNICIPIO", "ID_ESCOLA", "REDE"],
    )
    .drop(
        columns=[
            "nota_saeb_matematica",
            "nota_saeb_lingua_portuguesa",
            "REDE",
            "CO_MUNICIPIO",
            "ID_ESCOLA",
        ]
    )
    .rename(
        columns={
            "VL_NOTA_MATEMATICA_2015": "nota_saeb_matematica",
            "VL_NOTA_PORTUGUES_2015": "nota_saeb_lingua_portuguesa",
        },
        errors="raise",
    )[bd_escolas_inicial.columns]  # type: ignore
)

escolas_without_2015 = bd.read_sql(
    """
SELECT
  *
FROM
  `basedosdados.br_inep_ideb.escola`
WHERE ano <> 2015
""",
    billing_project_id="basedosdados-dev",
)

pd.concat(
    [escolas_without_2015, fixed_escolas_inicial, fixed_escolas_final] # type: ignore
).sort_values(["ano", "sigla_uf"]).to_csv(
    os.path.join(OUTPUT, "escola.csv"), index=False
)

del escolas_without_2015
del fixed_escolas_inicial
del fixed_escolas_final
del bd_escolas_inicial
del bd_escolas_final

# Municipios

municipios_inicial = pd.read_excel(
    os.path.join(INPUT, "divulgacao_anos_iniciais_municipios_2019.xlsx"), skiprows=9
)
municipios_final = pd.read_excel(
    os.path.join(INPUT, "divulgacao_anos_finais_municipios_2019.xlsx"), skiprows=9
)

municipios_inicial = sanitize_dataframe(municipios_inicial, table_escolas=False)

municipios_final = sanitize_dataframe(municipios_final, table_escolas=False)


bd_municipios_inicial = bd.read_sql(
    """
SELECT
  *
FROM
  `basedosdados.br_inep_ideb.municipio`
WHERE
  ano = 2015
  and anos_escolares = "iniciais (1-5)"
""",
    billing_project_id="basedosdados-dev",
)

bd_municipios_final = bd.read_sql(
    """
SELECT
  *
FROM
  `basedosdados.br_inep_ideb.municipio`
WHERE
  ano = 2015
  and anos_escolares = "finais (6-9)"
""",
    billing_project_id="basedosdados-dev",
)

fixed_municipios_inicial = (
    bd_municipios_inicial.merge(  # type: ignore
        municipios_inicial,
        left_on=["id_municipio", "rede"],
        right_on=["CO_MUNICIPIO", "REDE"],
    )
    .drop(
        columns=[
            "nota_saeb_matematica",
            "nota_saeb_lingua_portuguesa",
            "REDE",
            "CO_MUNICIPIO",
        ]
    )
    .rename(
        columns={
            "VL_NOTA_MATEMATICA_2015": "nota_saeb_matematica",
            "VL_NOTA_PORTUGUES_2015": "nota_saeb_lingua_portuguesa",
        },
        errors="raise",
    )[bd_municipios_inicial.columns]  # type: ignore
)

fixed_municipios_final = (
    bd_municipios_final.merge(  # type: ignore
        municipios_final,
        left_on=["id_municipio", "rede"],
        right_on=["CO_MUNICIPIO", "REDE"],
    )
    .drop(
        columns=[
            "nota_saeb_matematica",
            "nota_saeb_lingua_portuguesa",
            "REDE",
            "CO_MUNICIPIO",
        ]
    )
    .rename(
        columns={
            "VL_NOTA_MATEMATICA_2015": "nota_saeb_matematica",
            "VL_NOTA_PORTUGUES_2015": "nota_saeb_lingua_portuguesa",
        },
        errors="raise",
    )[bd_municipios_final.columns]  # type: ignore
)

municipios_without_2015 = bd.read_sql(
    """
SELECT
  *
FROM
  `basedosdados.br_inep_ideb.municipio`
WHERE ano <> 2015
""",
    billing_project_id="basedosdados-dev",
)

pd.concat(
    [municipios_without_2015, fixed_municipios_inicial, fixed_municipios_final] # type: ignore
).sort_values(["ano", "sigla_uf"]).to_csv(
    os.path.join(OUTPUT, "municipio.csv"), index=False
)

tb_escola = bd.Table(dataset_id="br_inep_ideb", table_id="escola")

tb_escola.create(
    os.path.join(OUTPUT, "escola.csv"),
    if_table_exists="replace",
    if_storage_data_exists="replace",
)

tb_municipio = bd.Table(dataset_id="br_inep_ideb", table_id="municipio")

tb_municipio.create(
    os.path.join(OUTPUT, "municipio.csv"),
    if_table_exists="replace",
    if_storage_data_exists="replace",
)
