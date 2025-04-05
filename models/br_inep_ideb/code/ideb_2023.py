# -*- coding: utf-8 -*-
import os
import zipfile

import basedosdados as bd
import numpy as np
import pandas as pd

ROOT = os.path.join("models", "br_inep_ideb")
INPUT = os.path.join(ROOT, "input")
TMP = os.path.join(ROOT, "tmp")
OUTPUT = os.path.join(ROOT, "output")

os.makedirs(INPUT, exist_ok=True)
os.makedirs(TMP, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

URLS = {
    "brasil": "https://download.inep.gov.br/ideb/resultados/divulgacao_brasil_ideb_2023.zip",
    "regioes_estados": "https://download.inep.gov.br/ideb/resultados/divulgacao_regioes_ufs_ideb_2023.zip",
    "municipio_anos_iniciais": "https://download.inep.gov.br/ideb/resultados/divulgacao_anos_iniciais_municipios_2023.zip",
    "municipio_anos_finais": "https://download.inep.gov.br/ideb/resultados/divulgacao_anos_finais_municipios_2023.zip",
    "municipio_em": "https://download.inep.gov.br/ideb/resultados/divulgacao_ensino_medio_municipios_2023.zip",
    "escola_anos_iniciais": "https://download.inep.gov.br/ideb/resultados/divulgacao_anos_iniciais_escolas_2023.zip",
    "escola_anos_finais": "https://download.inep.gov.br/ideb/resultados/divulgacao_anos_finais_escolas_2023.zip",
    "escola_em": "https://download.inep.gov.br/ideb/resultados/divulgacao_ensino_medio_escolas_2023.zip",
}

for _, url in URLS.items():
    os.system(f"cd {INPUT}; curl -O -k {url}")


for key, url in URLS.items():
    file = url.split("/")[-1]
    file_path = os.path.join(INPUT, file)
    output_dir = os.path.join(TMP, key)

    os.makedirs(output_dir, exist_ok=True)

    with zipfile.ZipFile(file_path, "r") as z:
        z.extractall(output_dir)

XLSX_BR = os.path.join(
    TMP,
    "brasil",
    "divulgacao_brasil_ideb_2023",
    "divulgacao_brasil_ideb_2023.xlsx",
)

sheet_names_br: list[str] = pd.ExcelFile(XLSX_BR).sheet_names

COMMON_RENAMES = {
    "rede": "rede",
    "VL_NOTA_MATEMATICA_2023": "nota_saeb_matematica",
    "VL_NOTA_PORTUGUES_2023": "nota_saeb_lingua_portuguesa",
    "VL_NOTA_MEDIA_2023": "nota_saeb_media_padronizada",
    "VL_INDICADOR_REND_2023": "indicador_rendimento",
    "VL_APROVACAO_2023_SI_4": "taxa_aprovacao",
    "VL_OBSERVADO_2023": "ideb",
}

df_brasil = pd.concat(
    [
        pd.read_excel(XLSX_BR, sheet_name=sheet_name, skiprows=9)[
            list(COMMON_RENAMES.keys())
        ]
        .rename(columns=COMMON_RENAMES, errors="raise")  # type: ignore
        .assign(anos_escolares=sheet_name)
        for sheet_name in sheet_names_br
    ]
)

df_brasil = df_brasil.pipe(lambda d: d.loc[d["rede"].notna()]).assign(ano=2023)

df_brasil["rede"].unique()

df_brasil["rede"] = (
    df_brasil["rede"]
    .str.lower()
    .replace({"privada (1)": "privada", "pública": "publica"})
)

df_brasil["anos_escolares"] = df_brasil["anos_escolares"].replace(
    {
        "Brasil (Anos Iniciais)": "iniciais (1-5)",
        "Brasil (Anos Finais)": "finais (6-9)",
        "Brasil (EM)": "todos (1-4)",
    }
)

df_brasil["projecao"] = np.nan

df_brasil["ensino"] = df_brasil["anos_escolares"].apply(
    lambda v: "medio" if v == "todos (1-4)" else "fundamental"
)

df_brasil_upstream = bd.read_sql(
    "select * from `basedosdados.br_inep_ideb.brasil`",
    billing_project_id="basedosdados-dev",
)

tb_brasil = bd.Table(dataset_id="br_inep_ideb", table_id="brasil")

tb_brasil_cols_from_bq = tb_brasil._get_columns_from_bq()

assert len(tb_brasil_cols_from_bq["partition_columns"]) == 0

tb_brasil_order_cols: list[str] = [
    i["name"] for i in tb_brasil_cols_from_bq["columns"]
]


df_brasil_updated = pd.concat(
    [df_brasil[tb_brasil_order_cols], df_brasil_upstream]
)  # type: ignore

OUTPUT_BR = os.path.join(OUTPUT, "brasil.csv")

df_brasil_updated.to_csv(OUTPUT_BR, index=False)

tb_brasil.create(
    OUTPUT_BR, if_table_exists="replace", if_storage_data_exists="replace"
)

# Regioes, UFs

XLSX_REGIOES_UFS = os.path.join(
    TMP,
    "regioes_estados",
    "divulgacao_regioes_ufs_ideb_2023",
    "divulgacao_regioes_ufs_ideb_2023.xlsx",
)

sheet_names_regioes_ufs: list[str] = pd.ExcelFile(XLSX_REGIOES_UFS).sheet_names


df_regioes_ufs_latest = pd.concat(
    [
        pd.read_excel(XLSX_REGIOES_UFS, sheet_name=sheet_name, skiprows=9)[
            [
                *["Unnamed: 0", "Unnamed: 1"],
                *[i for i in COMMON_RENAMES.keys() if i != "rede"],
            ]
        ]  # type: ignore
        .rename(columns={"Unnamed: 0": "uf_regiao", "Unnamed: 1": "rede"})
        .rename(columns=COMMON_RENAMES, errors="raise")
        .assign(anos_escolares=sheet_name)
        for sheet_name in sheet_names_regioes_ufs
    ]
)

df_regioes_ufs_latest = df_regioes_ufs_latest.pipe(
    lambda d: d.loc[d["rede"].notna()]
).assign(ano=2023)

df_regioes_ufs_latest["rede"].unique()

df_regioes_ufs_latest["rede"] = (
    df_regioes_ufs_latest["rede"]
    .str.lower()
    .replace(
        {
            "privada (1)": "privada",
            "privada (2)": "privada",
            "total (3)(4)": "total",
            "total (4)": "total",
            "pública": "publica",
            "pública (4)": "publica",
        }
    )
)

df_regioes_ufs_latest["anos_escolares"].unique()

df_regioes_ufs_latest["anos_escolares"] = df_regioes_ufs_latest[
    "anos_escolares"
].replace(
    {
        "UF e Regiões (AI)": "iniciais (1-5)",
        "UF e Regiões (AF)": "finais (6-9)",
        "UF e Regiões (EM)": "todos (1-4)",
    }
)

df_regioes_ufs_latest["projecao"] = np.nan

df_regioes_ufs_latest["ensino"] = df_regioes_ufs_latest[
    "anos_escolares"
].apply(lambda v: "medio" if v == "todos (1-4)" else "fundamental")

df_regioes_ufs_latest["uf_regiao"].unique()

SIGLA_UFS_REPLACES = {
    "R. G. do Norte": "Rio Grande do Norte",
    "R. G. do Sul": "Rio Grande do Sul",
    "M. G. do Sul": "Mato Grosso do Sul",
}

df_regioes_ufs_latest["uf_regiao"] = df_regioes_ufs_latest[
    "uf_regiao"
].replace(SIGLA_UFS_REPLACES)

br_dirs = bd.read_sql(
    "SELECT * from `basedosdados.br_bd_diretorios_brasil.uf`",
    billing_project_id="basedosdados-dev",
)

assert isinstance(br_dirs, pd.DataFrame)

## Região

df_regioes_latest = df_regioes_ufs_latest.loc[
    df_regioes_ufs_latest["uf_regiao"].isin(
        br_dirs["regiao"].unique().tolist()
    )
]

assert len(df_regioes_latest["uf_regiao"].unique()) == 5

df_regioes_latest = df_regioes_latest.rename(
    columns={"uf_regiao": "regiao"}, errors="raise"
)

tb_regiao = bd.Table(dataset_id="br_inep_ideb", table_id="regiao")

tb_regiao_cols_from_bq = tb_regiao._get_columns_from_bq()

assert len(tb_regiao_cols_from_bq["partition_columns"]) == 0

tb_regiao_order_cols: list[str] = [
    i["name"] for i in tb_regiao_cols_from_bq["columns"]
]

df_regiao_upstream = bd.read_sql(
    "select * from `basedosdados.br_inep_ideb.regiao`",
    billing_project_id="basedosdados-dev",
)

df_regiao_updated = pd.concat(
    [df_regioes_latest[tb_regiao_order_cols], df_regiao_upstream]  # type: ignore
)

OUTPUT_REGIAO = os.path.join(OUTPUT, "regiao.csv")

df_regiao_updated.to_csv(OUTPUT_REGIAO, index=False)

tb_regiao.create(
    OUTPUT_REGIAO, if_table_exists="replace", if_storage_data_exists="replace"
)

## UFs

df_ufs_latest = df_regioes_ufs_latest.loc[
    df_regioes_ufs_latest["uf_regiao"].isin(br_dirs["nome"].unique().tolist())
]

assert len(df_ufs_latest["uf_regiao"].unique()) == 27

df_ufs_latest["uf_regiao"].unique()


df_ufs_latest = df_ufs_latest.rename(
    columns={"uf_regiao": "sigla_uf"}, errors="raise"
)

df_ufs_latest["sigla_uf"] = df_ufs_latest["sigla_uf"].replace(
    {i["nome"]: i["sigla"] for i in br_dirs.to_dict("records")}
)

tb_uf = bd.Table(dataset_id="br_inep_ideb", table_id="uf")

tb_uf_cols_from_bq = tb_uf._get_columns_from_bq()

assert len(tb_uf_cols_from_bq["partition_columns"]) == 0

tb_uf_order_cols: list[str] = [
    i["name"] for i in tb_uf_cols_from_bq["columns"]
]

df_uf_upstream = bd.read_sql(
    "select * from `basedosdados.br_inep_ideb.uf`",
    billing_project_id="basedosdados-dev",
)

df_uf_updated = pd.concat(
    [df_ufs_latest[tb_uf_order_cols], df_uf_upstream]  # type: ignore
)

OUTPUT_UF = os.path.join(OUTPUT, "uf.csv")

df_uf_updated.to_csv(OUTPUT_UF, index=False)

tb_uf.create(
    OUTPUT_UF, if_table_exists="replace", if_storage_data_exists="replace"
)

# Municipios

XLSX_MUN_ANOS_INICIAIS = os.path.join(
    TMP,
    "municipio_anos_iniciais",
    "divulgacao_anos_iniciais_municipios_2023",
    "divulgacao_anos_iniciais_municipios_2023.xlsx",
)

XLSX_MUN_ANOS_FINAIS = os.path.join(
    TMP,
    "municipio_anos_finais",
    "divulgacao_anos_finais_municipios_2023",
    "divulgacao_anos_finais_municipios_2023.xlsx",
)

XLSX_MUN_EM = os.path.join(
    TMP,
    "municipio_em",
    "divulgacao_ensino_medio_municipios_2023",
    "divulgacao_ensino_medio_municipios_2023.xlsx",
)

df_municipio_latest = (
    pd.concat(
        [
            pd.read_excel(path, skiprows=9)[
                [
                    *[i for i in COMMON_RENAMES.keys() if i != "rede"],
                    *["SG_UF", "CO_MUNICIPIO", "REDE"],
                ]
            ]
            .rename(
                columns={
                    "SG_UF": "sigla_uf",
                    "REDE": "rede",
                    "CO_MUNICIPIO": "id_municipio",
                },
                errors="raise",
            )  # type: ignore
            .rename(columns=COMMON_RENAMES, errors="raise")
            .assign(anos_escolares=table_name)
            for table_name, path in {
                "anos_iniciais": XLSX_MUN_ANOS_INICIAIS,
                "anos_finais": XLSX_MUN_ANOS_FINAIS,
                "em": XLSX_MUN_EM,
            }.items()
        ]
    )
    .pipe(lambda d: d.loc[d["rede"].notna()])
    .assign(ano=2023, projecao=np.nan)
)

df_municipio_latest.head()

df_municipio_latest["rede"].unique()

df_municipio_latest["rede"] = (
    df_municipio_latest["rede"]
    .str.lower()
    .replace(
        {
            "pública": "publica",
        }
    )
)

df_municipio_latest["anos_escolares"].unique()

df_municipio_latest["anos_escolares"] = df_municipio_latest[
    "anos_escolares"
].replace(
    {
        "anos_iniciais": "iniciais (1-5)",
        "anos_finais": "finais (6-9)",
        "em": "todos (1-4)",
    }
)

df_municipio_latest["ensino"] = df_municipio_latest["anos_escolares"].apply(
    lambda v: "medio" if v == "todos (1-4)" else "fundamental"
)

df_municipio_latest["ensino"].unique()

assert len(df_municipio_latest["sigla_uf"].unique()) == 27

df_municipio_latest["id_municipio"] = (
    df_municipio_latest["id_municipio"].astype(int).astype(str)
)

assert (
    df_municipio_latest[["id_municipio", "rede", "anos_escolares"]]
    .value_counts(dropna=False)
    .reset_index()["count"]
    .unique()[0]
    == 1
)

df_municipio_latest.head()

tb_municipio = bd.Table(dataset_id="br_inep_ideb", table_id="municipio")

tb_municipio_cols_from_bq = tb_municipio._get_columns_from_bq(mode="prod")

assert len(tb_municipio_cols_from_bq["partition_columns"]) == 0

tb_municipio_order_cols: list[str] = [
    i["name"] for i in tb_municipio_cols_from_bq["columns"]
]

df_municipio_upstream = bd.read_sql(
    "select * from `basedosdados.br_inep_ideb.municipio`",
    billing_project_id="basedosdados-dev",
)

df_municipio_updated = pd.concat(
    [df_municipio_latest[tb_municipio_order_cols], df_municipio_upstream]  # type: ignore
)

OUTPUT_MUNICIPIO = os.path.join(OUTPUT, "municipio.csv")

df_municipio_updated.to_csv(OUTPUT_MUNICIPIO, index=False)

tb_municipio.create(
    OUTPUT_MUNICIPIO,
    if_table_exists="replace",
    if_storage_data_exists="replace",
)

# Escolas

XLSX_ESCOLAS_ANOS_INICIAIS = os.path.join(
    TMP,
    "escola_anos_iniciais",
    "divulgacao_anos_iniciais_escolas_2023",
    "divulgacao_anos_iniciais_escolas_2023.xlsx",
)

XLSX_ESCOLAS_ANOS_FINAIS = os.path.join(
    TMP,
    "escola_anos_finais",
    "divulgacao_anos_finais_escolas_2023",
    "divulgacao_anos_finais_escolas_2023.xlsx",
)

XLSX_ESCOLAS_EM = os.path.join(
    TMP,
    "escola_em",
    "divulgacao_ensino_medio_escolas_2023",
    "divulgacao_ensino_medio_escolas_2023.xlsx",
)

df_escolas_latest = (
    pd.concat(
        [
            pd.read_excel(path, skiprows=9)[
                [
                    *[i for i in COMMON_RENAMES.keys() if i != "rede"],
                    *["SG_UF", "CO_MUNICIPIO", "REDE", "ID_ESCOLA"],
                ]
            ]
            .rename(
                columns={
                    "SG_UF": "sigla_uf",
                    "REDE": "rede",
                    "CO_MUNICIPIO": "id_municipio",
                    "ID_ESCOLA": "id_escola",
                },
                errors="raise",
            )  # type: ignore
            .rename(columns=COMMON_RENAMES, errors="raise")
            .assign(anos_escolares=table_name)
            for table_name, path in {
                "anos_iniciais": XLSX_ESCOLAS_ANOS_INICIAIS,
                "anos_finais": XLSX_ESCOLAS_ANOS_FINAIS,
                "em": XLSX_ESCOLAS_EM,
            }.items()
        ]
    )
    .pipe(lambda d: d.loc[d["rede"].notna()])
    .assign(ano=2023, projecao=np.nan)
)

df_escolas_latest.head()

df_escolas_latest["rede"].unique()

df_escolas_latest["rede"] = df_escolas_latest["rede"].str.lower()

df_escolas_latest["anos_escolares"].unique()

df_escolas_latest["anos_escolares"] = df_escolas_latest[
    "anos_escolares"
].replace(
    {
        "anos_iniciais": "iniciais (1-5)",
        "anos_finais": "finais (6-9)",
        "em": "todos (1-4)",
    }
)

df_escolas_latest["ensino"] = df_escolas_latest["anos_escolares"].apply(
    lambda v: "medio" if v == "todos (1-4)" else "fundamental"
)

df_escolas_latest["ensino"].unique()

assert len(df_escolas_latest["sigla_uf"].unique()) == 27

df_escolas_latest["id_municipio"]
df_escolas_latest["id_escola"]

df_escolas_latest["id_municipio"] = (
    df_escolas_latest["id_municipio"].astype(int).astype(str)
)

df_escolas_latest["id_escola"] = (
    df_escolas_latest["id_escola"].astype(int).astype(str)
)

assert (
    df_escolas_latest[["rede", "anos_escolares", "id_escola"]]
    .value_counts(dropna=False)
    .reset_index()["count"]
    .unique()[0]
    == 1
)

df_escolas_latest.head()

tb_escola = bd.Table(dataset_id="br_inep_ideb", table_id="escola")

tb_escola_cols_from_bq = tb_escola._get_columns_from_bq(mode="prod")

assert len(tb_escola_cols_from_bq["partition_columns"]) == 0

tb_escola_order_cols: list[str] = [
    i["name"] for i in tb_escola_cols_from_bq["columns"]
]

df_escola_upstream = bd.read_sql(
    "select * from `basedosdados.br_inep_ideb.escola`",
    billing_project_id="basedosdados-dev",
)

df_escolas_updated = pd.concat(
    [df_escolas_latest[tb_escola_order_cols], df_escola_upstream]  # type: ignore
)

OUTPUT_ESCOLA = os.path.join(OUTPUT, "escola.csv")

df_escolas_updated.to_csv(OUTPUT_ESCOLA, index=False)

tb_escola.create(
    OUTPUT_ESCOLA, if_table_exists="replace", if_storage_data_exists="replace"
)
