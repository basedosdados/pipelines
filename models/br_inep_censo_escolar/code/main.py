# -*- coding: utf-8 -*-
import io
import os

import basedosdados as bd
import numpy as np
import pandas as pd
import requests

INPUT = os.path.join(os.getcwd(), "input")
OUTPUT = os.path.join(os.getcwd(), "output")

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

st = bd.Storage(dataset_id="br_inep_censo_escolar", table_id="turma")

blobs = list(st.bucket.list_blobs(prefix="raw/br_inep_censo_escolar/turma/"))

for blob in blobs:
    filename = blob.name.split("/")[-1]
    if filename.endswith(".CSV"):
        blob.download_to_filename(filename=os.path.join(INPUT, filename))


dfs = {
    str(year): pd.read_csv(os.path.join(INPUT, f"TURMAS_{year}.CSV"), sep=";")
    for year in range(2021, 2023 + 1)
}


arch = pd.read_csv(
    io.StringIO(
        requests.get(
            "https://docs.google.com/spreadsheets/d/1qRf25hLSPYX-bSSyffk0DJP_C_mpCHngDY2x_kIohVo/export?format=csv",
            timeout=10,
        ).content.decode("utf-8")
    ),
    dtype=str,
    na_values="",
)

renames = {
    i["original_name_2020"]: i["name"]
    for i in arch.loc[
        (arch["name"] != "(deletado)") & (arch["original_name_2020"].notna()),
    ][["original_name_2020", "name"]].to_dict("records")
}

arch_cols = arch.loc[
    (arch["name"] != "(deletado)") & (arch["original_name_2020"].notna()),
]["name"].to_list()


dfs = {
    year: df.rename(
        columns={k: v for k, v in renames.items() if k in df.columns},
        errors="raise",
    )
    for year, df in dfs.items()
}

dfs = {
    year: df[[i for i in arch_cols if i in df.columns]]
    for year, df in dfs.items()
}

df = pd.concat([i for _, i in dfs.items()])

del dfs  # need memory

all_cols = arch.loc[(arch["name"] != "(deletado)"),]["name"].to_list()

cols_missing = list(set(all_cols) - set(df.columns))

for i in arch.loc[arch["bigquery_type"] == "STRING"]["name"]:
    if i in df.columns:
        # NOTE: fillna("") porque a coerção astype("Int64").astype("String")
        # cria <NA> e ao salvar o csv, <NA> não é salvo como um valor
        # vazio i.e "", ele salva como <NA> e isso é intepretado como uma string no BQ
        df[i] = df[i].astype("Int64").astype("string").fillna("")  # type: ignore

for i in arch.loc[arch["bigquery_type"] == "INT64"]["name"]:
    if i in df.columns:
        df[i] = df[i].astype("Int64")

for i in cols_missing:
    df[i] = np.nan

tb = bd.Table(dataset_id="br_inep_censo_escolar", table_id="turma")

bq_cols = tb._get_columns_from_bq()

partitions = [i["name"] for i in bq_cols["partition_columns"]]

bd_dir = bd.read_sql(
    "SELECT id_uf, sigla FROM `basedosdados.br_bd_diretorios_brasil.uf`",
    billing_project_id="basedosdados-dev",
)

df["sigla_uf"].unique()  # type: ignore

df["sigla_uf"] = df["sigla_uf"].replace(  # type: ignore
    {i["id_uf"]: i["sigla"] for i in bd_dir.to_dict("records")}  # type: ignore
)

df["sigla_uf"].unique()  # type: ignore

bq_storage_cols_order = [i["name"] for i in bq_cols["columns"]]

df["rede"].unique()  # type: ignore

df["rede"] = df["rede"].replace(  # type: ignore
    {"1": "federal", "2": "estadual", "3": "municipal", "4": "privada"}
)

df["rede"].unique()  # type: ignore

for keys, df_split in df.groupby(partitions):
    ano, sigla_uf = keys  # type: ignore
    path = os.path.join(OUTPUT, f"ano={ano}", f"sigla_uf={sigla_uf}")
    os.makedirs(path, exist_ok=True)
    df_split.drop(columns=["ano", "sigla_uf"])[bq_storage_cols_order].to_csv(  # type: ignore
        os.path.join(path, f"{ano}_{sigla_uf}.csv"), index=False
    )


tb.create(OUTPUT, if_table_exists="replace", if_storage_data_exists="replace")
