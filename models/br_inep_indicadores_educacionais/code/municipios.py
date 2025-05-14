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

URLS_MUNICIPIOS = [
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/AFD_2023_MUNICIPIOS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/ATU_2023_MUNICIPIOS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/DSU_2023_MUNICIPIOS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/HAD_2023_MUNICIPIOS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/ICG_2023_MUNICIPIOS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/IED_2023_MUNICIPIOS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/IRD_2023_MUNICIPIOS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/TDI_2023_MUNICIPIOS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2022/tnr_municipios_2022.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2022/tx_rend_municipios_2022.zip",
]

INPUT = os.path.join(os.getcwd(), "tmp")

if not os.path.exists(INPUT):
    os.mkdir(INPUT)

INPUT_MUN = os.path.join(INPUT, "municipios")

OUTPUT = os.path.join(os.getcwd(), "output")

if not os.path.exists(OUTPUT):
    os.mkdir(OUTPUT)

os.mkdir(INPUT_MUN)

for url in URLS_MUNICIPIOS:
    os.system(f"cd {INPUT_MUN}; curl -O -k {url}")

for file in os.listdir(INPUT_MUN):
    with zipfile.ZipFile(os.path.join(INPUT_MUN, file)) as z:
        z.extractall(INPUT_MUN)

COL_ID_MUNICIPIO_RENAME = {"CO_MUNICIPIO": "id_municipio"}
UNSUED_COLS = ["NO_REGIAO", "SG_UF", "NO_MUNICIPIO"]

afd = pd.read_excel(
    os.path.join(INPUT_MUN, "AFD_2023_MUNICIPIOS", "AFD_MUNICIPIOS_2023.xlsx"),
    skiprows=10,
).drop(columns=UNSUED_COLS)

afd = afd.rename(
    columns={**COL_ID_MUNICIPIO_RENAME, **rename_afd}, errors="raise"
)

afd = afd.loc[afd["ano"] == 2023,]
afd["localizacao"] = afd["localizacao"].str.lower()
afd["rede"] = afd["rede"].str.lower().replace("pública", "publica")


atu = pd.read_excel(
    os.path.join(INPUT_MUN, "ATU_2023_MUNICIPIOS", "ATU_MUNICIPIOS_2023.xlsx"),
    skiprows=8,
).drop(columns=UNSUED_COLS)

atu = atu.rename(
    columns={**COL_ID_MUNICIPIO_RENAME, **rename_atu}, errors="raise"
)

atu = atu.loc[atu["ano"] == 2023,]
atu["localizacao"] = atu["localizacao"].str.lower()
atu["rede"] = atu["rede"].str.lower().replace("pública", "publica")


dsu = pd.read_excel(
    os.path.join(INPUT_MUN, "DSU_2023_MUNICIPIOS", "DSU_MUNICIPIOS_2023.xlsx"),
    skiprows=9,
).drop(columns=UNSUED_COLS)

dsu = dsu.rename(
    columns={**COL_ID_MUNICIPIO_RENAME, **rename_dsu}, errors="raise"
)

dsu = dsu.loc[dsu["ano"] == 2023,]
dsu["localizacao"] = dsu["localizacao"].str.lower()
dsu["rede"] = dsu["rede"].str.lower().replace("pública", "publica")

had = pd.read_excel(
    os.path.join(INPUT_MUN, "HAD_2023_MUNICIPIOS", "HAD_MUNICIPIOS_2023.xlsx"),
    skiprows=8,
).drop(columns=UNSUED_COLS)

rename_had_adapted = {
    k: v
    for k, v in {
        **{"MED_NS_CAT_01": "had_em_nao_seriado"},
        **rename_had,
    }.items()
    if k != "MED_NS_CAT_0"
}

had = had.rename(
    columns={**COL_ID_MUNICIPIO_RENAME, **rename_had_adapted}, errors="raise"
)

had = had.loc[had["ano"] == 2023,]
had["localizacao"] = had["localizacao"].str.lower()
had["rede"] = had["rede"].str.lower().replace("pública", "publica")


icg = pd.read_excel(
    os.path.join(INPUT_MUN, "ICG_2023_MUNICIPIOS", "ICG_MUNICIPIOS_2023.xlsx"),
    skiprows=8,
).drop(columns=UNSUED_COLS)

icg = icg.rename(
    columns={**COL_ID_MUNICIPIO_RENAME, **rename_icg}, errors="raise"
)

icg = icg.loc[icg["ano"] == 2023,]
icg["localizacao"] = icg["localizacao"].str.lower()
icg["rede"] = icg["rede"].str.lower().replace("pública", "publica")


ied = pd.read_excel(
    os.path.join(INPUT_MUN, "IED_2023_MUNICIPIOS", "IED_MUNICIPIOS_2023.xlsx"),
    skiprows=10,
).drop(columns=UNSUED_COLS)

ied = ied.rename(
    columns={**COL_ID_MUNICIPIO_RENAME, **rename_ied}, errors="raise"
)

ied = ied.loc[ied["ano"] == 2023,]
ied["localizacao"] = ied["localizacao"].str.lower()
ied["rede"] = ied["rede"].str.lower().replace("pública", "publica")


ird = pd.read_excel(
    os.path.join(INPUT_MUN, "IRD_2023_MUNICIPIOS", "IRD_MUNICIPIOS_2023.xlsx"),
    skiprows=9,
).drop(columns=UNSUED_COLS)

ird = ird.rename(
    columns={**COL_ID_MUNICIPIO_RENAME, **rename_ird}, errors="raise"
)

ird = ird.loc[ird["ano"] == 2023,]
ird["localizacao"] = ird["localizacao"].str.lower()
ird["rede"] = ird["rede"].str.lower().replace("pública", "publica")


tdi = pd.read_excel(
    os.path.join(INPUT_MUN, "TDI_2023_MUNICIPIOS", "TDI_MUNICIPIOS_2023.xlsx"),
    skiprows=8,
).drop(columns=UNSUED_COLS)

tdi = tdi.rename(
    columns={**COL_ID_MUNICIPIO_RENAME, **rename_tdi}, errors="raise"
)

tdi = tdi.loc[tdi["ano"] == 2023,]
tdi["localizacao"] = tdi["localizacao"].str.lower()
tdi["rede"] = tdi["rede"].str.lower().replace("pública", "publica")


tnr = pd.read_excel(
    os.path.join(INPUT_MUN, "tnr_municipios_2022", "tnr_municipios_2022.xlsx"),
    skiprows=8,
).drop(columns=UNSUED_COLS)

tnr = tnr.rename(
    columns={**COL_ID_MUNICIPIO_RENAME, **rename_tnr}, errors="raise"
)

tnr = tnr.loc[tnr["ano"] == 2022,]
tnr["localizacao"] = tnr["localizacao"].str.lower()
tnr["rede"] = tnr["rede"].str.lower().replace("pública", "publica")


tx = pd.read_excel(
    os.path.join(
        INPUT_MUN,
        "tx_rend_municipios_2022",
        "tx_rend_municipios_2022.xlsx",
    ),
    skiprows=8,
).drop(columns=UNSUED_COLS)

tx = tx.rename(
    columns={**COL_ID_MUNICIPIO_RENAME, **rename_tx}, errors="raise"
)

tx = tx.loc[tx["ano"] == 2022,]
tx["localizacao"] = tx["localizacao"].str.lower()
tx["rede"] = tx["rede"].str.lower().replace("pública", "publica")

municipio_2022 = bd.read_sql(
    """
SELECT
  *
FROM
  `basedosdados.br_inep_indicadores_educacionais.municipio`
WHERE
  ano = 2022
""",
    billing_project_id="basedosdados-dev",
)

tnr_columns = [
    i
    for i in tnr.columns
    if i not in ["ano", "id_municipio", "localizacao", "rede"]
]

tnr["id_municipio"] = tnr["id_municipio"].astype("Int64").astype("str")

municipio_2022_updated = municipio_2022.drop(columns=tnr_columns).merge(  # type: ignore
    tnr,
    how="left",
    left_on=["ano", "localizacao", "rede", "id_municipio"],
    right_on=["ano", "localizacao", "rede", "id_municipio"],
)

tx_columns = [
    i
    for i in tx.columns
    if i not in ["ano", "id_municipio", "localizacao", "rede"]
]

tx["id_municipio"] = tx["id_municipio"].astype("Int64").astype("str")

municipio_2022_updated = municipio_2022_updated.drop(columns=tx_columns).merge(
    tx,
    how="left",
    left_on=["ano", "localizacao", "rede", "id_municipio"],
    right_on=["ano", "localizacao", "rede", "id_municipio"],
)[municipio_2022.columns]  # type: ignore

municipio_2022_updated = municipio_2022_updated.replace("--", None)

assert municipio_2022_updated.shape == municipio_2022.shape  # type: ignore

municipio_output_path = os.path.join(OUTPUT, "municipio")

municipio_2022_output_path = os.path.join(municipio_output_path, "ano=2022")

os.makedirs(municipio_2022_output_path, exist_ok=True)

municipio_2022_updated.drop(columns=["ano"]).to_csv(
    os.path.join(municipio_2022_output_path, "municipio.csv"), index=False
)

# Municipio 2023

keys_col_merge = ["ano", "id_municipio", "localizacao", "rede"]

df_2023 = reduce(
    lambda left, right: left.merge(
        right, left_on=keys_col_merge, right_on=keys_col_merge
    ),
    [afd, atu, dsu, had, icg, ied, ird, tdi],
)


# Vamos adicionar colunas de dois indicadores
# Nao temos dados de 2023 para eles
for empty_col in [*tnr_columns, *tx_columns]:
    df_2023[empty_col] = None

assert df_2023.shape[1] == municipio_2022.shape[1]  # type: ignore

df_2023["id_municipio"] = df_2023["id_municipio"].astype("Int64").astype("str")
df_2023 = df_2023.replace("--", None)

municipio_2023_output_path = os.path.join(municipio_output_path, "ano=2023")

os.makedirs(municipio_2023_output_path, exist_ok=True)

df_2023[municipio_2022.columns].drop(columns="ano").to_csv(  # type: ignore
    os.path.join(municipio_2023_output_path, "municipio.csv"), index=False
)
