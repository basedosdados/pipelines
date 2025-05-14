# -*- coding: utf-8 -*-
import os
import zipfile
from code.constants import (  # type: ignore
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
from functools import reduce

import basedosdados as bd
import pandas as pd

URLS = [
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/ATU_2023_BRASIL_REGIOES_UFS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2022/tx_rend_brasil_regioes_ufs_2022.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/HAD_2023_BRASIL_REGIOES_UFS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/TDI_2023_BRASIL_REGIOES_UFS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2022/tnr_brasil_regioes_ufs_2022.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/DSU_2023_BRASIL_REGIOES_UFS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/AFD_2023_BRASIL_REGIOES_UF.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/IRD_2023_BRASIL_REGIOES_UFS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/IED_2023_BRASIL_REGIOES_UFS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/ICG_2023_BRASIL_REGIOES_UFS.zip",
]

INPUT = os.path.join(os.getcwd(), "tmp")

if not os.path.exists(INPUT):
    os.mkdir(INPUT)

INPUT_BR = os.path.join(INPUT, "br_regioes_uf")

OUTPUT = os.path.join(os.getcwd(), "output")

if not os.path.exists(OUTPUT):
    os.mkdir(OUTPUT)

os.mkdir(INPUT_BR)

for url in URLS:
    os.system(f"cd {INPUT_BR}; curl -O -k {url}")

for file in os.listdir(INPUT_BR):
    with zipfile.ZipFile(os.path.join(INPUT_BR, file)) as z:
        z.extractall(INPUT_BR)


afd = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "AFD_2023_BRASIL_REGIOES_UF",
        "AFD_BRASIL_REGIOES_UFS_2023.xlsx",
    ),
    skiprows=10,
)


afd = afd.rename(columns=rename_afd, errors="raise")

afd = afd.loc[afd["ano"] == 2023,]
afd["localizacao"] = afd["localizacao"].str.lower()
afd["rede"] = afd["rede"].str.lower().replace("pública", "publica")

## Média de alunos por turma (ATU)

atu = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "ATU_2023_BRASIL_REGIOES_UFS",
        "ATU_BRASIL_REGIOES_UFS_2023.xlsx",
    ),
    skiprows=8,
)

atu = atu.rename(columns=rename_atu, errors="raise")

atu = atu.loc[atu["ano"] == 2023,]
atu["localizacao"] = atu["localizacao"].str.lower()
atu["rede"] = atu["rede"].str.lower().replace("pública", "publica")

# Percentual de Docentes com Curso Superior
dsu = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "DSU_2023_BRASIL_REGIOES_UFS",
        "DSU_BRASIL_REGIOES_UFS_2023.xlsx",
    ),
    skiprows=9,
)


dsu = dsu.rename(columns=rename_dsu, errors="raise")

dsu = dsu.loc[dsu["ano"] == 2023,]
dsu["localizacao"] = dsu["localizacao"].str.lower()
dsu["rede"] = dsu["rede"].str.lower().replace("pública", "publica")


# Média de Horas-aula diária HAD -> 2023

had = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "HAD_2023_BRASIL_REGIOES_UFS",
        "HAD_BRASIL_REGIOES_UFS_2023.xlsx",
    ),
    skiprows=8,
)


had = had.rename(columns=rename_had, errors="raise")

had = had.loc[had["ano"] == 2023,]
had["localizacao"] = had["localizacao"].str.lower()
had["rede"] = had["rede"].str.lower().replace("pública", "publica")

# Complexidade de Gestão da Escola (ICG) -> 2023

icg = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "ICG_2023_BRASIL_REGIOES_UFS",
        "ICG_BRASIL_REGIOES_UFS_2023.xlsx",
    ),
    skiprows=8,
)


icg = icg.rename(columns=rename_icg, errors="raise")

icg = icg.loc[icg["ano"] == 2023,]
icg["localizacao"] = icg["localizacao"].str.lower()
icg["rede"] = icg["rede"].str.lower().replace("pública", "publica")

# Esforço Docente (IED) -> 2023

ied = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "IED_2023_BRASIL_REGIOES_UFS",
        "IED_BRASIL_REGIOES_UFS_2023.xlsx",
    ),
    skiprows=10,
)

ied = ied.rename(columns=rename_ied, errors="raise")

ied = ied.loc[ied["ano"] == 2023,]
ied["localizacao"] = ied["localizacao"].str.lower()
ied["rede"] = ied["rede"].str.lower().replace("pública", "publica")

# Regularidade do Corpo Docente (IRD) -> 2023

ird = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "IRD_2023_BRASIL_REGIOES_UFS",
        "IRD_BRASIL_REGIOES_UFS_2023.xlsx",
    ),
    skiprows=9,
)


ird = ird.rename(columns=rename_ird, errors="raise")

ird = ird.loc[ird["ano"] == 2023,]
ird["localizacao"] = ird["localizacao"].str.lower()
ird["rede"] = ird["rede"].str.lower().replace("pública", "publica")

# Taxas de Distorção Idade-série (TDI) -> 2023

tdi = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "TDI_2023_BRASIL_REGIOES_UFS",
        "TDI_BRASIL_REGIOES_UFS_2023.xlsx",
    ),
    skiprows=8,
)


tdi = tdi.rename(columns=rename_tdi, errors="raise")

tdi = tdi.loc[tdi["ano"] == 2023,]
tdi["localizacao"] = tdi["localizacao"].str.lower()
tdi["rede"] = tdi["rede"].str.lower().replace("pública", "publica")

# Taxa de Não Resposta (tnr) -> 2022

tnr = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "tnr_brasil_regioes_ufs_2022",
        "tnr_brasil_regioes_ufs_2022.xlsx",
    ),
    skiprows=8,
)


tnr = tnr.rename(columns=rename_tnr, errors="raise")

tnr = tnr.loc[tnr["ano"] == 2022,]
tnr["localizacao"] = tnr["localizacao"].str.lower()
tnr["rede"] = tnr["rede"].str.lower().replace("pública", "publica")

# Taxa de aprovação, reprovação, abandono -> 2022

tx = pd.read_excel(
    os.path.join(
        INPUT_BR,
        "tx_rend_brasil_regioes_ufs_2022",
        "tx_rend_brasil_regioes_ufs_2022.xlsx",
    ),
    skiprows=8,
)

tx = tx.rename(columns=rename_tx, errors="raise")

tx = tx.loc[tx["ano"] == 2022,]
tx["localizacao"] = tx["localizacao"].str.lower()
tx["rede"] = tx["rede"].str.lower().replace("pública", "publica")


brasil_2022 = bd.read_sql(
    """
SELECT
  *
FROM
  `basedosdados.br_inep_indicadores_educacionais.brasil`
WHERE
  ano = 2022
""",
    billing_project_id="basedosdados-dev",
)

uf_2022 = bd.read_sql(
    """
SELECT
  *
FROM
  `basedosdados.br_inep_indicadores_educacionais.uf`
WHERE
  ano = 2022
""",
    billing_project_id="basedosdados-dev",
)

regiao_2022 = bd.read_sql(
    """
SELECT
  *
FROM
  `basedosdados-dev.br_inep_indicadores_educacionais.regiao`
WHERE
  ano = 2022
""",
    billing_project_id="basedosdados-dev",
)

bd_dir = bd.read_sql(
    "SELECT * FROM `basedosdados.br_bd_diretorios_brasil.uf`",
    billing_project_id="basedosdados-dev",
)

regioes = bd_dir["regiao"].unique()  # type: ignore
estados = bd_dir["nome"].unique()  # type: ignore

tnr_brasil_2022 = tnr.loc[tnr["UNIDGEO"] == "Brasil",].drop(
    columns=["UNIDGEO"]
)

tnr_ufs_2022 = (
    tnr.loc[tnr["UNIDGEO"].isin(estados),]
    .merge(bd_dir[["sigla", "nome"]], left_on="UNIDGEO", right_on="nome")  # type: ignore
    .drop(columns=["UNIDGEO", "nome"])
    .rename(columns={"sigla": "sigla_uf"})
)

assert tnr_ufs_2022["sigla_uf"].unique().size == 27

tnr_regioes_2022 = tnr.loc[tnr["UNIDGEO"].isin(regioes),].rename(
    columns={"UNIDGEO": "regiao"}
)

tnr_columns = [
    i
    for i in tnr_brasil_2022.columns
    if i not in ["ano", "localizacao", "rede"]
]

# TNR Brasil 2022
brasil_2022_updated = brasil_2022.drop(columns=tnr_columns).merge(  # type: ignore
    tnr_brasil_2022,
    left_on=["ano", "localizacao", "rede"],
    right_on=["ano", "localizacao", "rede"],
)[brasil_2022.columns]  # type: ignore

# TNR UFs
uf_2022_updated = uf_2022.drop(columns=tnr_columns).merge(  # type: ignore
    tnr_ufs_2022,
    left_on=["ano", "localizacao", "rede", "sigla_uf"],
    right_on=["ano", "localizacao", "rede", "sigla_uf"],
)[uf_2022.columns]  # type: ignore

# TNR regioes
regiao_2022_updated = regiao_2022.drop(columns=tnr_columns).merge(  # type: ignore
    tnr_regioes_2022,
    left_on=["ano", "localizacao", "rede", "regiao"],
    right_on=["ano", "localizacao", "rede", "regiao"],
)[regiao_2022.columns]  # type: ignore


# Taxa de aprovação, reprovação, abandono -> 2022

tx_brasil_2022 = tx.loc[tx["UNIDGEO"] == "Brasil",].drop(columns=["UNIDGEO"])

tx_ufs_2022 = (
    tx.loc[tnr["UNIDGEO"].isin(estados),]
    .merge(bd_dir[["sigla", "nome"]], left_on="UNIDGEO", right_on="nome")  # type: ignore
    .drop(columns=["UNIDGEO", "nome"])
    .rename(columns={"sigla": "sigla_uf"})
)

assert tx_ufs_2022["sigla_uf"].unique().size == 27

tx_regioes_2022 = tx.loc[tx["UNIDGEO"].isin(regioes),].rename(
    columns={"UNIDGEO": "regiao"}
)

tx_columns = [
    i
    for i in tx_brasil_2022.columns
    if i not in ["ano", "localizacao", "rede"]
]

# TX Brasil 2022
brasil_2022_updated = brasil_2022_updated.drop(columns=tx_columns).merge(
    tx_brasil_2022,
    left_on=["ano", "localizacao", "rede"],
    right_on=["ano", "localizacao", "rede"],
)[brasil_2022.columns]  # type: ignore

# TX UFs 2022
uf_2022_updated = uf_2022_updated.drop(columns=tx_columns).merge(
    tx_ufs_2022,
    left_on=["ano", "localizacao", "rede", "sigla_uf"],
    right_on=["ano", "localizacao", "rede", "sigla_uf"],
)[uf_2022.columns]  # type: ignore

# TX regioes 2022
regiao_2022_updated = regiao_2022_updated.drop(columns=tx_columns).merge(  # type: ignore
    tx_regioes_2022,
    left_on=["ano", "localizacao", "rede", "regiao"],
    right_on=["ano", "localizacao", "rede", "regiao"],
)[regiao_2022.columns]  # type: ignore

# Some sanitize
brasil_2022_updated = brasil_2022_updated.replace("--", None)

uf_2022_updated = uf_2022_updated.replace("--", None)

regiao_2022_updated = regiao_2022_updated.replace("--", None)

brasil_output_path = os.path.join(OUTPUT, "brasil")

for key, df in brasil_2022_updated.groupby("ano"):
    path = os.path.join(brasil_output_path, f"ano={key}")
    os.makedirs(path, exist_ok=True)
    df.drop(columns="ano").to_csv(
        os.path.join(path, "brasil.csv"), index=False
    )


uf_output_path = os.path.join(OUTPUT, "uf")

for keys, df in uf_2022_updated.groupby(["ano", "sigla_uf"]):
    year, sigla_uf = keys  # type: ignore
    path = os.path.join(uf_output_path, f"ano={year}", f"sigla_uf={sigla_uf}")
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["ano", "sigla_uf"]).to_csv(
        os.path.join(path, "uf.csv"), index=False
    )

regioes_output_path = os.path.join(OUTPUT, "regiao")

for year, df in regiao_2022_updated.groupby("ano"):
    path = os.path.join(regioes_output_path, f"ano={year}")
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["ano"]).to_csv(
        os.path.join(path, "data.csv"), index=False
    )


# Atualizacoes disponivel para 2023

keys_col_merge = ["ano", "UNIDGEO", "localizacao", "rede"]

df_2023 = reduce(
    lambda left, right: left.merge(
        right, left_on=keys_col_merge, right_on=keys_col_merge
    ),
    [afd, atu, dsu, had, icg, ied, ird, tdi],
)

df_2023 = df_2023.replace("--", None)

# Vamos adicionar colunas de dois indicadores
# Nao temos dados de 2023 para eles
for empty_col in [*tnr_columns, *tx_columns]:
    df_2023[empty_col] = None

brasil_2023 = df_2023.loc[df_2023["UNIDGEO"] == "Brasil",].drop(
    columns=["UNIDGEO"]
)[
    brasil_2022.columns  # type: ignore
]

assert brasil_2022.shape[1] == brasil_2023.shape[1]  # type: ignore

for key, df in brasil_2023.groupby("ano"):
    path = os.path.join(brasil_output_path, f"ano={key}")
    os.makedirs(path, exist_ok=True)
    df.drop(columns="ano").to_csv(
        os.path.join(path, "brasil.csv"), index=False
    )

uf_2023 = (
    df_2023.loc[df_2023["UNIDGEO"].isin(estados),]
    .merge(bd_dir[["sigla", "nome"]], left_on="UNIDGEO", right_on="nome")  # type: ignore
    .drop(columns=["UNIDGEO", "nome"])
    .rename(columns={"sigla": "sigla_uf"})[uf_2022.columns]  # type: ignore
)

assert uf_2023.shape[1] == uf_2022.shape[1]  # type: ignore

for keys, df in uf_2023.groupby(["ano", "sigla_uf"]):
    year, sigla_uf = keys  # type: ignore
    path = os.path.join(uf_output_path, f"ano={year}", f"sigla_uf={sigla_uf}")
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["ano", "sigla_uf"]).to_csv(
        os.path.join(path, "uf.csv"), index=False
    )

regioes_2023 = (
    df_2023.loc[df_2023["UNIDGEO"].isin(regioes),].rename(
        columns={"UNIDGEO": "regiao"}
    )[regiao_2022.columns]  # type: ignore
)

for year, df in regioes_2023.groupby("ano"):
    path = os.path.join(regioes_output_path, f"ano={year}")
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["ano"]).to_csv(
        os.path.join(path, "data.csv"), index=False
    )
