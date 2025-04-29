# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
import zipfile
import basedosdados as bd

ROOT = os.path.join("models", "br_inep_ideb")
INPUT = os.path.join(ROOT, "input")
TMP = os.path.join(ROOT, "tmp")
OUTPUT = os.path.join(ROOT, "output")

os.makedirs(INPUT, exist_ok=True)
os.makedirs(TMP, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

exit_code = os.system(
    f"cd {INPUT}; curl -O -k https://download.inep.gov.br/educacao_basica/portal_ideb/planilhas_para_download/2021/divulgacao_regioes_ufs_ideb_2021.zip"
)

assert exit_code == 0

with zipfile.ZipFile(
    os.path.join(INPUT, "divulgacao_regioes_ufs_ideb_2021.zip"), "r"
) as zip_ref:
    zip_ref.extractall(TMP)

XLSX_PATH = os.path.join(TMP, "divulgacao_regioes_ufs_ideb_2021.xlsx")

sheet_names: list[str] = pd.ExcelFile(XLSX_PATH).sheet_names


RENAMES = {
    "Unnamed: 0": "sigla_uf",
    "Unnamed: 1": "rede",
    "VL_APROVACAO_2021_SI_4": "taxa_aprovacao",
    "VL_INDICADOR_REND_2021": "indicador_rendimento",
    "VL_NOTA_MATEMATICA_2021": "nota_saeb_matematica",
    "VL_NOTA_PORTUGUES_2021": "nota_saeb_lingua_portuguesa",
    "VL_NOTA_MEDIA_2021": "nota_saeb_media_padronizada",
    "VL_OBSERVADO_2021": "ideb",
}

df = pd.concat(
    [
        pd.read_excel(XLSX_PATH, sheet_name=sheet_name, skiprows=9)[
            list(RENAMES.keys())
        ]
        .rename(columns=RENAMES, errors="raise")  # type: ignore
        .assign(anos_escolares=sheet_name)
        for sheet_name in sheet_names
    ]
)

df["sigla_uf"].unique()

SIGLA_UFS_REPLACES = {
    "R. G. do Norte": "Rio Grande do Norte",
    "R. G. do Sul": "Rio Grande do Sul",
    "M. G. do Sul": "Mato Grosso do Sul",
}

df["sigla_uf"] = df["sigla_uf"].replace(SIGLA_UFS_REPLACES)

df["sigla_uf"].unique()

br_dirs = bd.read_sql(
    "SELECT * from `basedosdados.br_bd_diretorios_brasil.uf`",
    billing_project_id="basedosdados-dev",
)

assert isinstance(br_dirs, pd.DataFrame)

df = df.loc[df["sigla_uf"].isin(br_dirs["nome"].tolist())]

assert len(df["sigla_uf"].unique()) == 27

replaces_name_sigla_uf = {i["nome"]: i["sigla"] for i in br_dirs.to_dict("records")}

df["sigla_uf"] = df["sigla_uf"].replace(replaces_name_sigla_uf)

df["anos_escolares"].unique()

ANOS_ESCOLARES = {
    "UF e Regiões (AI)": "iniciais (1-5)",
    "UF e Regiões (AF)": "finais (6-9)",
    "UF e Regiões (EM)": "todos (1-4)",
}

df["anos_escolares"] = df["anos_escolares"].replace(ANOS_ESCOLARES)

df["anos_escolares"].unique()

# add col ensino
df["ensino"] = df["anos_escolares"].apply(
    lambda v: "medio" if v == "todos (1-4)" else "fundamental"
)

df["rede"] = df["rede"].str.lower().replace({"pública": "publica"})

df["rede"].unique()

for col in df.columns:
    print(col, " -- ", df[col].unique())

# replace `-` with nan
df = df.replace({"-": np.nan})

tb = bd.Table(dataset_id="br_inep_ideb", table_id="uf")

cols_from_bq = tb._get_columns_from_bq()

order_cols = [i["name"] for i in cols_from_bq["columns"]]

# add `ano` and `projecao` column

df["ano"] = 2021
df["projecao"] = None

df[order_cols]

df_upstream = bd.read_sql(
    "select * from `basedosdados.br_inep_ideb.uf` where ano <> 2021",
    billing_project_id="basedosdados-dev",
)

OUTPUT_PATH = os.path.join(OUTPUT, "uf.csv")

pd.concat([df[order_cols], df_upstream]).to_csv(  # type: ignore
    OUTPUT_PATH, index=False
)

tb.create(
    OUTPUT_PATH,
    if_table_exists="replace",
    if_storage_data_exists="replace",
)
