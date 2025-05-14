"""
Script para subir dados do censo escolar de 2024
"""
import os
import io
import requests
import basedosdados as bd
import pandas as pd
from pathlib import Path

ROOT = Path("models") / "br_inep_censo_escolar" / "code"

INPUT = ROOT / "input"
OUTPUT = ROOT / "output"

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

# Download raw file from storage

st = bd.Storage(dataset_id="br_inep_censo_escolar", table_id="turma")

st.download(filename="TURMAS_2024.CSV", mode="raw", savepath=INPUT)

df_turma_2024 = pd.read_csv(
    INPUT / "raw" / "br_inep_censo_escolar" / "turma" / "TURMAS_2024.CSV", sep=";"
)

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
    i["original_name_2023"]: i["name"]
    for i in arch.loc[
        (arch["name"] != "(deletado)") & (arch["original_name_2023"].notna()),
    ][["original_name_2023", "name"]].to_dict("records")  # type: ignore
}

missing_cols = [i for i in renames.keys() if i not in df_turma_2024.columns]

df_turma_2024[missing_cols] = None

arch_cols = arch.loc[
    (arch["name"] != "(deletado)") & (arch["original_name_2023"].notna()),
]["name"].to_list()  # type: ignore

df_turma_2024 = df_turma_2024[renames.keys()].rename(columns=renames)

cols_missing = list(set(arch_cols) - set(df_turma_2024.columns))

assert len(cols_missing) == 0

for i in arch.loc[arch["bigquery_type"] == "STRING"]["name"]:
    if i in df_turma_2024.columns:
        # NOTE: fillna("") porque a coerção astype("Int64").astype("String")
        # cria <NA> e ao salvar o csv, <NA> não é salvo como um valor
        # vazio i.e "", ele salva como <NA> e isso é intepretado como uma string no BQ
        df_turma_2024[i] = df_turma_2024[i].astype("Int64").astype("string").fillna("")  # type: ignore

for i in arch.loc[arch["bigquery_type"] == "INT64"]["name"]:
    if i in df_turma_2024.columns:
        df_turma_2024[i] = df_turma_2024[i].astype("Int64")

tb = bd.Table(dataset_id="br_inep_censo_escolar", table_id="turma")

bq_cols = tb._get_columns_from_bq()

partitions = [i["name"] for i in bq_cols["partition_columns"]]

bd_dir = bd.read_sql(
    "SELECT id_uf, sigla FROM `basedosdados.br_bd_diretorios_brasil.uf`",
    billing_project_id="basedosdados-dev",
)

df_turma_2024["sigla_uf"].unique()  # type: ignore

df_turma_2024["sigla_uf"] = df_turma_2024["sigla_uf"].replace(  # type: ignore
    {i["id_uf"]: i["sigla"] for i in bd_dir.to_dict("records")}  # type: ignore
)

df_turma_2024["sigla_uf"].unique()  # type: ignore

bq_storage_cols_order = [i["name"] for i in bq_cols["columns"]]

missing_cols_from_bq = [
    i for i in bq_storage_cols_order if i not in df_turma_2024.columns
]

df_turma_2024[missing_cols_from_bq] = None

df_turma_2024["rede"].unique()  # type: ignore

df_turma_2024["rede"] = df_turma_2024["rede"].replace(  # type: ignore
    {"1": "federal", "2": "estadual", "3": "municipal", "4": "privada"}
)

df_turma_2024["rede"].unique()  # type: ignore

for keys, df_split in df_turma_2024.groupby(partitions):
    ano, sigla_uf = keys  # type: ignore
    save_path = OUTPUT / f"ano={ano}" / f"sigla_uf={sigla_uf}"
    os.makedirs(save_path, exist_ok=True)
    df_split.drop(columns=partitions)[bq_storage_cols_order].to_csv(  # type: ignore
        os.path.join(save_path, f"{ano}_{sigla_uf}.csv"), index=False
    )


tb.create(OUTPUT, if_table_exists="replace", if_storage_data_exists="replace")
