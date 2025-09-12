import os
import zipfile
from code.constants import (  # type: ignore
    rename_afd,
    rename_atu,
    rename_dsu,
    rename_had,
    # rename_icg,
    # rename_ied,
    # rename_ird,
    rename_tdi,
    rename_tnr,
    rename_tx,
)
from functools import reduce

import basedosdados as bd
import pandas as pd

URLS_ESCOLAS = [
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/AFD_2023_ESCOLAS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/ATU_2023_ESCOLAS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/DSU_2023_ESCOLAS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/HAD_2023_ESCOLAS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/ICG_2023_ESCOLAS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/IED_2023_ESCOLAS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/IRD_2023_ESCOLAS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/TDI_2023_ESCOLAS.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2022/tnr_escolas_2022.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2022/tx_rend_escolas_2022.zip",
]


INPUT = os.path.join(os.getcwd(), "tmp")

if not os.path.exists(INPUT):
    os.mkdir(INPUT)

INPUT_ESC = os.path.join(INPUT, "escolas")
OUTPUT = os.path.join(os.getcwd(), "output")
OUTPUT_ESC = os.path.join(OUTPUT, "escola")

if not os.path.exists(OUTPUT):
    os.mkdir(OUTPUT)

os.makedirs(INPUT_ESC)

for url in URLS_ESCOLAS:
    os.system(f"cd {INPUT_ESC}; curl -O -k {url}")


for file in os.listdir(INPUT_ESC):
    with zipfile.ZipFile(os.path.join(INPUT_ESC, file)) as z:
        z.extractall(INPUT_ESC)


COL_EXTEND_RENAME = {
    "CO_MUNICIPIO": "id_municipio",
    "CO_ENTIDADE": "id_escola",
}
UNSUED_COLS = ["NO_REGIAO", "SG_UF", "NO_MUNICIPIO", "NO_ENTIDADE"]

afd = pd.read_excel(
    os.path.join(INPUT_ESC, "AFD_2023_ESCOLAS", "AFD_ESCOLAS_2023.xlsx"),
    skiprows=10,
).drop(columns=UNSUED_COLS)

afd = afd.rename(columns={**COL_EXTEND_RENAME, **rename_afd}, errors="raise")

afd = afd.loc[afd["ano"] == 2023,]
afd["localizacao"] = afd["localizacao"].str.lower()
afd["rede"] = afd["rede"].str.lower().replace("pública", "publica")


atu = pd.read_excel(
    os.path.join(INPUT_ESC, "ATU_2023_ESCOLAS", "ATU_ESCOLAS_2023.xlsx"),
    skiprows=8,
).drop(columns=UNSUED_COLS)

atu = atu.rename(columns={**COL_EXTEND_RENAME, **rename_atu}, errors="raise")

atu = atu.loc[atu["ano"] == 2023,]
atu["localizacao"] = atu["localizacao"].str.lower()
atu["rede"] = atu["rede"].str.lower().replace("pública", "publica")

dsu = pd.read_excel(
    os.path.join(INPUT_ESC, "DSU_2023_ESCOLAS", "DSU_ESCOLAS_2023.xlsx"),
    skiprows=9,
).drop(columns=UNSUED_COLS)

dsu = dsu.rename(columns={**COL_EXTEND_RENAME, **rename_dsu}, errors="raise")

dsu = dsu.loc[dsu["ano"] == 2023,]
dsu["localizacao"] = dsu["localizacao"].str.lower()
dsu["rede"] = dsu["rede"].str.lower().replace("pública", "publica")


had = pd.read_excel(
    os.path.join(INPUT_ESC, "HAD_2023_ESCOLAS", "HAD_ESCOLAS_2023.xlsx"),
    skiprows=8,
).drop(columns=UNSUED_COLS)

had = had.rename(columns={**COL_EXTEND_RENAME, **rename_had}, errors="raise")

had = had.loc[had["ano"] == 2023,]
had["localizacao"] = had["localizacao"].str.lower()
had["rede"] = had["rede"].str.lower().replace("pública", "publica")


icg = pd.read_excel(
    os.path.join(INPUT_ESC, "ICG_2023_ESCOLAS", "ICG_ESCOLAS_2023.xlsx"),
    skiprows=10,
).drop(columns=UNSUED_COLS)

icg = icg.rename(
    columns={
        **COL_EXTEND_RENAME,
        **{
            "NU_ANO_CENSO": "ano",
            "NO_CATEGORIA": "localizacao",
            "NO_DEPENDENCIA": "rede",
            "COMPLEX": "icg_nivel_complexidade_gestao_escola",
        },
    },
    errors="raise",
)

icg = icg.loc[icg["ano"] == 2023,]
icg["localizacao"] = icg["localizacao"].str.lower()
icg["rede"] = icg["rede"].str.lower().replace("pública", "publica")


ied = pd.read_excel(
    os.path.join(INPUT_ESC, "IED_2023_ESCOLAS", "IED_ESCOLAS_2023.xlsx"),
    skiprows=10,
).drop(columns=UNSUED_COLS)

ied = ied.rename(
    columns={
        **COL_EXTEND_RENAME,
        **{
            "NU_ANO_CENSO": "ano",
            "CO_MUNICIPIO": "id_municipio",
            "CO_ENTIDADE": "id_escola",
            "NO_CATEGORIA": "localizacao",
            "NO_DEPENDENCIA": "rede",
            "EDU_BAS_CAT_0": "ird_media_regularidade_docente",
        },
    },
    errors="raise",
)

ied = ied.loc[ied["ano"] == 2023,]
ied["localizacao"] = ied["localizacao"].str.lower()
ied["rede"] = ied["rede"].str.lower().replace("pública", "publica")

ird = pd.read_excel(
    os.path.join(INPUT_ESC, "IRD_2023_ESCOLAS", "IRD_ESCOLAS_2023.xlsx"),
    skiprows=10,
).drop(columns=UNSUED_COLS)

ird = ird.rename(
    columns={
        **COL_EXTEND_RENAME,
        **{
            "NU_ANO_CENSO": "ano",
            "NO_CATEGORIA": "localizacao",
            "NO_DEPENDENCIA": "rede",
            "EDU_BAS_CAT_0": "ird_media_regularidade_docente",
        },
    },
    errors="raise",
)

ird = ird.loc[ird["ano"] == 2023,]
ird["localizacao"] = ird["localizacao"].str.lower()
ird["rede"] = ird["rede"].str.lower().replace("pública", "publica")

tdi = pd.read_excel(
    os.path.join(INPUT_ESC, "TDI_2023_ESCOLAS", "TDI_ESCOLAS_2023.xlsx"),
    skiprows=8,
).drop(columns=UNSUED_COLS)

tdi = tdi.rename(columns={**COL_EXTEND_RENAME, **rename_tdi}, errors="raise")

tdi = tdi.loc[tdi["ano"] == 2023,]
tdi["localizacao"] = tdi["localizacao"].str.lower()
tdi["rede"] = tdi["rede"].str.lower().replace("pública", "publica")


tnr = pd.read_excel(
    os.path.join(INPUT_ESC, "tnr_escolas_2022", "tnr_escolas_2022.xlsx"),
    skiprows=8,
).drop(columns=UNSUED_COLS)

tnr = tnr.rename(columns={**COL_EXTEND_RENAME, **rename_tnr}, errors="raise")

tnr = tnr.loc[tnr["ano"] == 2022,]
tnr["localizacao"] = tnr["localizacao"].str.lower()
tnr["rede"] = tnr["rede"].str.lower().replace("pública", "publica")

tx = pd.read_excel(
    os.path.join(
        INPUT_ESC,
        "tx_rend_escolas_2022",
        "tx_rend_escolas_2022.xlsx",
    ),
    skiprows=8,
).drop(columns=UNSUED_COLS)

tx = tx.rename(columns={**COL_EXTEND_RENAME, **rename_tx}, errors="raise")

tx = tx.loc[tx["ano"] == 2022,]
tx["localizacao"] = tx["localizacao"].str.lower()
tx["rede"] = tx["rede"].str.lower().replace("pública", "publica")


escola_2022 = bd.read_sql(
    """
SELECT
  *
FROM
  `basedosdados.br_inep_indicadores_educacionais.escola`
WHERE
  ano = 2022
""",
    billing_project_id="basedosdados-dev",
)

tnr_columns = [
    i
    for i in tnr.columns
    if i not in ["ano", "id_municipio", "id_escola", "localizacao", "rede"]
]

tnr["id_municipio"] = tnr["id_municipio"].astype("Int64").astype("str")
tnr["id_escola"] = tnr["id_escola"].astype("Int64").astype("str")

escola_2022_updated = escola_2022.drop(columns=tnr_columns).merge(  # type: ignore
    tnr,
    how="left",
    left_on=["ano", "localizacao", "rede", "id_municipio", "id_escola"],
    right_on=["ano", "localizacao", "rede", "id_municipio", "id_escola"],
)[escola_2022.columns]  # type: ignore


tx_columns = [
    i
    for i in tx.columns
    if i not in ["ano", "id_municipio", "id_escola", "localizacao", "rede"]
]

tx["id_municipio"] = tx["id_municipio"].astype("Int64").astype("str")
tx["id_escola"] = tx["id_escola"].astype("Int64").astype("str")

escola_2022_updated = escola_2022_updated.drop(columns=tx_columns).merge(  # type: ignore
    tx,
    how="left",
    left_on=["ano", "localizacao", "rede", "id_municipio", "id_escola"],
    right_on=["ano", "localizacao", "rede", "id_municipio", "id_escola"],
)[escola_2022.columns]  # type: ignore


assert escola_2022_updated.shape == escola_2022.shape  # type: ignore

escola_2022_updated = escola_2022_updated.replace("--", None)

escola_2022_output_path = os.path.join(OUTPUT_ESC, "ano=2022")
os.makedirs(escola_2022_output_path, exist_ok=True)

escola_2022_updated.drop(columns=["ano"]).to_csv(
    os.path.join(escola_2022_output_path, "escola.csv"), index=False
)

# Atualizacao para 2023

keys_col_merge = ["ano", "id_municipio", "id_escola", "localizacao", "rede"]

df_2023 = reduce(
    lambda left, right: left.merge(
        right, left_on=keys_col_merge, right_on=keys_col_merge
    ),
    [afd, atu, dsu, had, icg, ied, ird, tdi],
)

# Vamos adicionar colunas de dois indicadores
# Nao temos dados de 2023 para eles
for empty_col in [*tx_columns, *tnr_columns]:
    df_2023[empty_col] = None

df_2023["id_municipio"] = df_2023["id_municipio"].astype("Int64").astype("str")
df_2023["id_escola"] = df_2023["id_escola"].astype("Int64").astype("str")
df_2023.replace("--", None)

assert df_2023.shape[1] == escola_2022.shape[1]  # type: ignore

escola_2023_output_path = os.path.join(OUTPUT_ESC, "ano=2023")

os.makedirs(escola_2023_output_path)

df_2023[escola_2022.columns].drop(columns="ano").to_csv(  # type: ignore
    os.path.join(escola_2023_output_path, "escola.csv"), index=False
)
