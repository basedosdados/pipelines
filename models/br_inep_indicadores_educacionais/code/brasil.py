# -*- coding: utf-8 -*-
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(
    current_dir
)  # Isso vai para /home/laribrito/BD/pipelines/models/br_inep_indicadores_educacionais/
sys.path.append(parent_dir)

# Importando o módulo constants
from functools import reduce

import basedosdados as bd
import pandas as pd
from constants import (
    rename_afd,
    rename_atu,
    rename_dsu,
    rename_had,
    rename_icg,
    rename_ied,
    rename_ird,
    rename_tdi,
    rename_tnr,
    rename_tx,
)

# URLS = [
#     "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/ATU_2024_BRASIL_REGIOES_UF.zip",
#     "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/tx_rend_brasil_regioes_ufs_2024.zip",
#     "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/HAD_2024_BRASIL_REGIOES_UFS.zip",
#     "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/TDI_2024_BRASIL_REGIOES_UFS.zip",
#     "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/tnr_brasil_regioes_ufs_2024.zip",
#     "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/DSU_2024_BRASIL_REGIOES_UFS.zip",
#     "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/AFD_2024_BRASIL_REGIOES_UFS.zip",
#     "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/IRD_2024_BRASIL_REGIOES_UFS.zip",
#     "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/IED_2024_BRASIL_REGIOES_UFS.zip",
#     "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/ICG_2024_BRASIL_REGIOES_UFS.zip",
# ]

INPUT = os.path.join(
    parent_dir, "tmp"
)  # Agora será: /home/laribrito/BD/pipelines/models/br_inep_indicadores_educacionais/tmp

os.makedirs(INPUT, exist_ok=True)

INPUT_BR = os.path.join(INPUT, "br_regioes_uf")

OUTPUT = os.path.join(parent_dir, "output")

os.makedirs(OUTPUT, exist_ok=True)
os.makedirs(INPUT_BR, exist_ok=True)

# import requests

# # Baixar os arquivos
# for url in URLS:
#     filename = url.split("/")[-1]  # pega só o nome do arquivo
#     filepath = os.path.join(INPUT_BR, filename)

#     try:
#         r = requests.get(url, stream=True, timeout=60)
#         r.raise_for_status()  # erro se não for 200
#         with open(filepath, "wb") as f:
#             for chunk in r.iter_content(chunk_size=8192):
#                 f.write(chunk)
#         print(f"Baixado: {filename}")
#     except Exception as e:
#         print(f"Erro ao baixar {url}: {e}")

# # Extrair os arquivos
# for file in os.listdir(INPUT_BR):
#     path = os.path.join(INPUT_BR, file)
#     if os.path.isfile(path) and file.lower().endswith(".zip"):
#         try:
#             with zipfile.ZipFile(path) as z:
#                 z.extractall(INPUT_BR)
#             print(f"Extraído: {file}")
#         except zipfile.BadZipFile:
#             print(f"Arquivo inválido (não é zip de verdade): {file}")

afd = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "AFD_2024_BRASIL_REGIOES_UF",
        "AFD_BRASIL_REGIOES_UFS_2024.xlsx",
    ),
    skiprows=10,
)


afd = afd.rename(columns=rename_afd, errors="raise")

afd = afd.loc[afd["ano"] == 2024,]
afd["localizacao"] = afd["localizacao"].str.lower()
afd["rede"] = afd["rede"].str.lower().replace("pública", "publica")

## Média de alunos por turma (ATU)

atu = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "ATU_2024_BRASIL_REGIOES_UFS",
        "ATU_BRASIL_REGIOES_UFS_2024.xlsx",
    ),
    skiprows=8,
)

atu = atu.rename(columns=rename_atu, errors="raise")

atu = atu.loc[atu["ano"] == 2024,]
atu["localizacao"] = atu["localizacao"].str.lower()
atu["rede"] = atu["rede"].str.lower().replace("pública", "publica")

# Percentual de Docentes com Curso Superior
dsu = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "DSU_2024_BRASIL_REGIOES_UFS",
        "DSU_BRASIL_REGIOES_UFS_2024.xlsx",
    ),
    skiprows=9,
)


dsu = dsu.rename(columns=rename_dsu, errors="raise")

dsu = dsu.loc[dsu["ano"] == 2024,]
dsu["localizacao"] = dsu["localizacao"].str.lower()
dsu["rede"] = dsu["rede"].str.lower().replace("pública", "publica")


# Média de Horas-aula diária HAD -> 2024

had = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "HAD_2024_BRASIL_REGIOES_UFS",
        "HAD_BRASIL_REGIOES_UFS_2024.xlsx",
    ),
    skiprows=8,
)


had = had.rename(columns=rename_had, errors="raise")

had = had.loc[had["ano"] == 2024,]
had["localizacao"] = had["localizacao"].str.lower()
had["rede"] = had["rede"].str.lower().replace("pública", "publica")

# Complexidade de Gestão da Escola (ICG) -> 2024

icg = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "ICG_2024_BRASIL_REGIOES_UFS",
        "ICG_BRASIL_REGIOES_UFS_2024.xlsx",
    ),
    skiprows=8,
)


icg = icg.rename(columns=rename_icg, errors="raise")

icg = icg.loc[icg["ano"] == 2024,]
icg["localizacao"] = icg["localizacao"].str.lower()
icg["rede"] = icg["rede"].str.lower().replace("pública", "publica")

# Esforço Docente (IED) -> 2024

ied = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "IED_2024_BRASIL_REGIOES_UFS",
        "IED_BRASIL_REGIOES_UFS_2024.xlsx",
    ),
    skiprows=10,
)

ied = ied.rename(columns=rename_ied, errors="raise")

ied = ied.loc[ied["ano"] == 2024,]
ied["localizacao"] = ied["localizacao"].str.lower()
ied["rede"] = ied["rede"].str.lower().replace("pública", "publica")

# Regularidade do Corpo Docente (IRD) -> 2024

ird = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "IRD_2024_BRASIL_REGIOES_UFS",
        "IRD_BRASIL_REGIOES_UFS_2024.xlsx",
    ),
    skiprows=9,
)


ird = ird.rename(columns=rename_ird, errors="raise")

ird = ird.loc[ird["ano"] == 2024,]
ird["localizacao"] = ird["localizacao"].str.lower()
ird["rede"] = ird["rede"].str.lower().replace("pública", "publica")

# Taxas de Distorção Idade-série (TDI) -> 2024

tdi = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "TDI_2024_BRASIL_REGIOES_UFS",
        "TDI_BRASIL_REGIOES_UFS_2024.xlsx",
    ),
    skiprows=8,
)


tdi = tdi.rename(columns=rename_tdi, errors="raise")

tdi = tdi.loc[tdi["ano"] == 2024,]
tdi["localizacao"] = tdi["localizacao"].str.lower()
tdi["rede"] = tdi["rede"].str.lower().replace("pública", "publica")

# Taxa de Não Resposta (tnr) -> 2024

tnr = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "tnr_brasil_regioes_ufs_2024",
        "tnr_brasil_regioes_ufs_2024.xlsx",
    ),
    skiprows=8,
)


tnr = tnr.rename(columns=rename_tnr, errors="raise")

tnr = tnr.loc[tnr["ano"] == 2024,]
tnr["localizacao"] = tnr["localizacao"].str.lower()
tnr["rede"] = tnr["rede"].str.lower().replace("pública", "publica")

# Taxa de aprovação, reprovação, abandono -> 2024

tx = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "tx_rend_brasil_regioes_ufs_2024",
        "tx_rend_brasil_regioes_ufs_2024.xlsx",
    ),
    skiprows=8,
)

tx = tx.rename(columns=rename_tx, errors="raise")

tx = tx.loc[tx["ano"] == 2024,]
tx["localizacao"] = tx["localizacao"].str.lower()
tx["rede"] = tx["rede"].str.lower().replace("pública", "publica")


brasil_2024 = bd.read_sql(
    """
SELECT
  *
FROM
  `basedosdados.br_inep_indicadores_educacionais.brasil`
WHERE
  ano = 2024
""",
    billing_project_id="basedosdados-dev",
)

uf_2024 = bd.read_sql(
    """
SELECT
  *
FROM
  `basedosdados.br_inep_indicadores_educacionais.uf`
WHERE
  ano = 2024
""",
    billing_project_id="basedosdados-dev",
)

regiao_2024 = bd.read_sql(
    """
SELECT
  *
FROM
  `basedosdados-dev.br_inep_indicadores_educacionais.regiao`
WHERE
  ano = 2024
""",
    billing_project_id="basedosdados-dev",
)

bd_dir = bd.read_sql(
    "SELECT * FROM `basedosdados.br_bd_diretorios_brasil.uf`",
    billing_project_id="basedosdados-dev",
)

regioes = bd_dir["regiao"].unique()  # type: ignore
estados = bd_dir["nome"].unique()  # type: ignore

tnr_brasil_2024 = tnr.loc[tnr["UNIDGEO"] == "Brasil",].drop(
    columns=["UNIDGEO"]
)

tnr_ufs_2024 = (
    tnr.loc[tnr["UNIDGEO"].isin(estados),]
    .merge(bd_dir[["sigla", "nome"]], left_on="UNIDGEO", right_on="nome")  # type: ignore
    .drop(columns=["UNIDGEO", "nome"])
    .rename(columns={"sigla": "sigla_uf"})
)

assert tnr_ufs_2024["sigla_uf"].unique().size == 27

tnr_regioes_2024 = tnr.loc[tnr["UNIDGEO"].isin(regioes),].rename(
    columns={"UNIDGEO": "regiao"}
)

tnr_columns = [
    i
    for i in tnr_brasil_2024.columns
    if i not in ["ano", "localizacao", "rede"]
]

# TNR Brasil 2024
brasil_2024_updated = brasil_2024.drop(columns=tnr_columns).merge(  # type: ignore
    tnr_brasil_2024,
    left_on=["ano", "localizacao", "rede"],
    right_on=["ano", "localizacao", "rede"],
)[brasil_2024.columns]  # type: ignore

# TNR UFs
uf_2024_updated = uf_2024.drop(columns=tnr_columns).merge(  # type: ignore
    tnr_ufs_2024,
    left_on=["ano", "localizacao", "rede", "sigla_uf"],
    right_on=["ano", "localizacao", "rede", "sigla_uf"],
)[uf_2024.columns]  # type: ignore

# TNR regioes
regiao_2024_updated = regiao_2024.drop(columns=tnr_columns).merge(  # type: ignore
    tnr_regioes_2024,
    left_on=["ano", "localizacao", "rede", "regiao"],
    right_on=["ano", "localizacao", "rede", "regiao"],
)[regiao_2024.columns]  # type: ignore


# Taxa de aprovação, reprovação, abandono -> 2024

tx_brasil_2024 = tx.loc[tx["UNIDGEO"] == "Brasil",].drop(columns=["UNIDGEO"])

tx_ufs_2024 = (
    tx.loc[tnr["UNIDGEO"].isin(estados),]
    .merge(bd_dir[["sigla", "nome"]], left_on="UNIDGEO", right_on="nome")  # type: ignore
    .drop(columns=["UNIDGEO", "nome"])
    .rename(columns={"sigla": "sigla_uf"})
)

assert tx_ufs_2024["sigla_uf"].unique().size == 27

tx_regioes_2024 = tx.loc[tx["UNIDGEO"].isin(regioes),].rename(
    columns={"UNIDGEO": "regiao"}
)

tx_columns = [
    i
    for i in tx_brasil_2024.columns
    if i not in ["ano", "localizacao", "rede"]
]

# TX Brasil 2024
brasil_2024_updated = brasil_2024_updated.drop(columns=tx_columns).merge(
    tx_brasil_2024,
    left_on=["ano", "localizacao", "rede"],
    right_on=["ano", "localizacao", "rede"],
)[brasil_2024.columns]  # type: ignore

# TX UFs 2024
uf_2024_updated = uf_2024_updated.drop(columns=tx_columns).merge(
    tx_ufs_2024,
    left_on=["ano", "localizacao", "rede", "sigla_uf"],
    right_on=["ano", "localizacao", "rede", "sigla_uf"],
)[uf_2024.columns]  # type: ignore

# TX regioes 2024
regiao_2024_updated = regiao_2024_updated.drop(columns=tx_columns).merge(  # type: ignore
    tx_regioes_2024,
    left_on=["ano", "localizacao", "rede", "regiao"],
    right_on=["ano", "localizacao", "rede", "regiao"],
)[regiao_2024.columns]  # type: ignore

# Some sanitize
brasil_2024_updated = brasil_2024_updated.replace("--", None)

uf_2024_updated = uf_2024_updated.replace("--", None)

regiao_2024_updated = regiao_2024_updated.replace("--", None)

brasil_output_path = os.path.join(OUTPUT, "brasil")

for key, df in brasil_2024_updated.groupby("ano"):
    path = os.path.join(brasil_output_path, f"ano={key}")
    os.makedirs(path, exist_ok=True)
    df.drop(columns="ano").to_csv(
        os.path.join(path, "brasil.csv"), index=False
    )


uf_output_path = os.path.join(OUTPUT, "uf")

for keys, df in uf_2024_updated.groupby(["ano", "sigla_uf"]):
    year, sigla_uf = keys  # type: ignore
    path = os.path.join(uf_output_path, f"ano={year}", f"sigla_uf={sigla_uf}")
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["ano", "sigla_uf"]).to_csv(
        os.path.join(path, "uf.csv"), index=False
    )

regioes_output_path = os.path.join(OUTPUT, "regiao")

for year, df in regiao_2024_updated.groupby("ano"):
    path = os.path.join(regioes_output_path, f"ano={year}")
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["ano"]).to_csv(
        os.path.join(path, "data.csv"), index=False
    )


# Atualizacoes disponivel para 2024

keys_col_merge = ["ano", "UNIDGEO", "localizacao", "rede"]

df_2024 = reduce(
    lambda left, right: left.merge(
        right, left_on=keys_col_merge, right_on=keys_col_merge
    ),
    [afd, atu, dsu, had, icg, ied, ird, tdi],
)

df_2024 = df_2024.replace("--", None)

# Vamos adicionar colunas de dois indicadores
# Nao temos dados de 2022 para eles
for empty_col in [*tnr_columns, *tx_columns]:
    df_2024[empty_col] = None

brasil_2024 = df_2024.loc[df_2024["UNIDGEO"] == "Brasil",].drop(
    columns=["UNIDGEO"]
)[
    brasil_2024.columns  # type: ignore
]

assert brasil_2024.shape[1] == brasil_2024.shape[1]  # type: ignore

for key, df in brasil_2024.groupby("ano"):
    path = os.path.join(brasil_output_path, f"ano={key}")
    os.makedirs(path, exist_ok=True)
    df.drop(columns="ano").to_csv(
        os.path.join(path, "brasil.csv"), index=False
    )

uf_2024 = (
    df_2024.loc[df_2024["UNIDGEO"].isin(estados),]
    .merge(bd_dir[["sigla", "nome"]], left_on="UNIDGEO", right_on="nome")  # type: ignore
    .drop(columns=["UNIDGEO", "nome"])
    .rename(columns={"sigla": "sigla_uf"})[uf_2024.columns]  # type: ignore
)

assert uf_2024.shape[1] == uf_2024.shape[1]  # type: ignore

for keys, df in uf_2024.groupby(["ano", "sigla_uf"]):
    year, sigla_uf = keys  # type: ignore
    path = os.path.join(uf_output_path, f"ano={year}", f"sigla_uf={sigla_uf}")
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["ano", "sigla_uf"]).to_csv(
        os.path.join(path, "uf.csv"), index=False
    )

regioes_2024 = (
    df_2024.loc[df_2024["UNIDGEO"].isin(regioes),].rename(
        columns={"UNIDGEO": "regiao"}
    )[regiao_2024.columns]  # type: ignore
)

for year, df in regioes_2024.groupby("ano"):
    path = os.path.join(regioes_output_path, f"ano={year}")
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["ano"]).to_csv(
        os.path.join(path, "data.csv"), index=False
    )


# tb_brasil = bd.Table(
#     dataset_id="br_inep_indicadores_educacionais",
#     table_id="brasil"
# )

# tb_brasil.create(
#     path=brasil_output_path, # Caminho para o arquivo csv ou parquet
#     if_storage_data_exists='raise',
#     if_table_exists='replace',
#     source_format='csv'
# )

# tb_regiao = bd.Table(
#     dataset_id="br_inep_indicadores_educacionais",
#     table_id="regiao"
# )

# tb_regiao.create(
#     path=regioes_output_path, # Caminho para o arquivo csv ou parquet
#     if_storage_data_exists='raise',
#     if_table_exists='replace',
#     source_format='csv'
# )

# tb_uf = bd.Table(
#     dataset_id="br_inep_indicadores_educacionais",
#     table_id="uf"
# )

# tb_uf.create(
#     path=uf_output_path, # Caminho para o arquivo csv ou parquet
#     if_storage_data_exists='raise',
#     if_table_exists='replace',
#     source_format='csv'
# )
