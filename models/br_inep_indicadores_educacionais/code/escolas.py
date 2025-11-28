import os
from functools import reduce

import pandas as pd

from models.br_inep_indicadores_educacionais.code.constants import (  # type: ignore
    rename_afd,
    rename_atu,
    rename_dsu,
    rename_had,
    # rename_icg,
    rename_ied,
    # rename_ird,
    rename_tdi,
    rename_tnr,
    rename_tx,
)

URLS_ESCOLAS = [
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/AFD_2024_ESCOLAS.zip",  # Adequação da Formação Docente
    # "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/ATU_2024_ESCOLAS.zip",     # Média de Alunos por Turma
    # "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/DSU_2024_ESCOLAS.zip",     # Percentual de Docentes com Curso Superior
    # "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/HAD_2024_ESCOLAS.zip",     # Média de Horas-aula diária
    # "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/ICG_2024_ESCOLAS.zip",     # Complexidade de Gestão da Escola
    # "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/IED_2024_ESCOLAS.zip",     # Esforço Docente
    # "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/IRD_2024_ESCOLAS.zip",     # Regularidade do Corpo Docente
    # "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/TDI_2024_ESCOLAS.zip",     # Taxas de Distorção Idade-série
    # "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/tnr_escolas_2024.zip",     # Taxas de Não-resposta
    # "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2024/tx_rend_escolas_2024.zip", # Taxas de Rendimento Escolar
]

INPUT = os.path.join(os.getcwd(), "tmp/escolas/input")
OUTPUT = os.path.join(os.getcwd(), "tmp/escolas/output")
os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)


# for url in URLS_ESCOLAS:
#     os.system(f"cd {INPUT}; curl -O -k {url}")


# for file in os.listdir(INPUT):
#     print(f"Extracting {file}...")
#     if file.endswith(".zip"):
#       with zipfile.ZipFile(os.path.join(INPUT, file)) as z:
#           breakpoint()
#           z.extractall(INPUT)
#           os.remove(os.path.join(INPUT, file))


COL_EXTEND_RENAME = {
    "CO_MUNICIPIO": "id_municipio",
    "CO_ENTIDADE": "id_escola",
}
UNSUED_COLS = ["NO_REGIAO", "SG_UF", "NO_MUNICIPIO", "NO_ENTIDADE"]

afd = pd.read_excel(
    os.path.join(INPUT, "AFD_2024_ESCOLAS", "AFD_ESCOLAS_2024.xlsx"),
    skiprows=10,
).drop(columns=UNSUED_COLS)

afd = afd.rename(columns={**COL_EXTEND_RENAME, **rename_afd}, errors="raise")

afd = afd.loc[afd["ano"] == 2024,]
afd["localizacao"] = afd["localizacao"].str.lower()
afd["rede"] = afd["rede"].str.lower().replace("pública", "publica")


atu = pd.read_excel(
    os.path.join(INPUT, "ATU_2024_ESCOLAS", "ATU_ESCOLAS_2024.xlsx"),
    skiprows=8,
).drop(columns=UNSUED_COLS)

atu = atu.rename(columns={**COL_EXTEND_RENAME, **rename_atu}, errors="raise")

atu = atu.loc[atu["ano"] == 2024,]
atu["localizacao"] = atu["localizacao"].str.lower()
atu["rede"] = atu["rede"].str.lower().replace("pública", "publica")

dsu = pd.read_excel(
    os.path.join(INPUT, "DSU_2024_ESCOLAS", "DSU_ESCOLAS_2024.xlsx"),
    skiprows=9,
).drop(columns=UNSUED_COLS)

dsu = dsu.rename(columns={**COL_EXTEND_RENAME, **rename_dsu}, errors="raise")

dsu = dsu.loc[dsu["ano"] == 2024,]
dsu["localizacao"] = dsu["localizacao"].str.lower()
dsu["rede"] = dsu["rede"].str.lower().replace("pública", "publica")


had = pd.read_excel(
    os.path.join(INPUT, "HAD_2024_ESCOLAS", "HAD_ESCOLAS_2024.xlsx"),
    skiprows=8,
).drop(columns=UNSUED_COLS)

had = had.rename(columns={**COL_EXTEND_RENAME, **rename_had}, errors="raise")

had = had.loc[had["ano"] == 2024,]
had["localizacao"] = had["localizacao"].str.lower()
had["rede"] = had["rede"].str.lower().replace("pública", "publica")


icg = pd.read_excel(
    os.path.join(INPUT, "ICG_2024_ESCOLAS", "ICG_ESCOLAS_2024.xlsx"),
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

icg = icg.loc[icg["ano"] == 2024,]
icg["localizacao"] = icg["localizacao"].str.lower()
icg["rede"] = icg["rede"].str.lower().replace("pública", "publica")


ied = pd.read_excel(
    os.path.join(INPUT, "IED_2024_ESCOLAS", "IED_ESCOLAS_2024.xlsx"),
    skiprows=10,
).drop(columns=UNSUED_COLS)

ied = ied.rename(
    columns={
        **COL_EXTEND_RENAME,
        **rename_ied,
    },
    errors="raise",
)

ied = ied.loc[ied["ano"] == 2024,]
ied["localizacao"] = ied["localizacao"].str.lower()
ied["rede"] = ied["rede"].str.lower().replace("pública", "publica")

ird = pd.read_excel(
    os.path.join(INPUT, "IRD_2024_ESCOLAS", "IRD_ESCOLAS_2024.xlsx"),
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

ird = ird.loc[ird["ano"] == 2024,]
ird["localizacao"] = ird["localizacao"].str.lower()
ird["rede"] = ird["rede"].str.lower().replace("pública", "publica")

tdi = pd.read_excel(
    os.path.join(INPUT, "TDI_2024_ESCOLAS", "TDI_ESCOLAS_2024.xlsx"),
    skiprows=8,
).drop(columns=UNSUED_COLS)

tdi = tdi.rename(columns={**COL_EXTEND_RENAME, **rename_tdi}, errors="raise")

tdi = tdi.loc[tdi["ano"] == 2024,]
tdi["localizacao"] = tdi["localizacao"].str.lower()
tdi["rede"] = tdi["rede"].str.lower().replace("pública", "publica")


tnr = pd.read_excel(
    os.path.join(INPUT, "tnr_escolas_2024", "tnr_escolas_2024.xlsx"),
    skiprows=8,
).drop(columns=UNSUED_COLS)

tnr = tnr.rename(columns={**COL_EXTEND_RENAME, **rename_tnr}, errors="raise")

tnr = tnr.loc[tnr["ano"] == 2024,]
tnr["localizacao"] = tnr["localizacao"].str.lower()
tnr["rede"] = tnr["rede"].str.lower().replace("pública", "publica")

tx = pd.read_excel(
    os.path.join(
        INPUT,
        "tx_rend_escolas_2024",
        "tx_rend_escolas_2024.xlsx",
    ),
    skiprows=8,
).drop(columns=UNSUED_COLS)

tx = tx.rename(columns={**COL_EXTEND_RENAME, **rename_tx}, errors="raise")

tx = tx.loc[tx["ano"] == 2024,]
tx["localizacao"] = tx["localizacao"].str.lower()
tx["rede"] = tx["rede"].str.lower().replace("pública", "publica")


# escola_2024 = bd.read_sql(
#     """
# SELECT
#   *
# FROM
#   `basedosdados.br_inep_indicadores_educacionais.escola`
# WHERE
#   ano = 2024
# """,
#     billing_project_id="basedosdados-dev",
# )

tnr_columns = [
    i
    for i in tnr.columns
    if i not in ["ano", "id_municipio", "id_escola", "localizacao", "rede"]
]

tnr["id_municipio"] = tnr["id_municipio"].astype("Int64").astype("str")
tnr["id_escola"] = tnr["id_escola"].astype("Int64").astype("str")

# escola_2024_updated = escola_2024.drop(columns=tnr_columns).merge(  # type: ignore
#     tnr,
#     how="left",
#     left_on=["ano", "localizacao", "rede", "id_municipio", "id_escola"],
#     right_on=["ano", "localizacao", "rede", "id_municipio", "id_escola"],
# )[escola_2024.columns]  # type: ignore


tx_columns = [
    i
    for i in tx.columns
    if i not in ["ano", "id_municipio", "id_escola", "localizacao", "rede"]
]

tx["id_municipio"] = tx["id_municipio"].astype("Int64").astype("str")
tx["id_escola"] = tx["id_escola"].astype("Int64").astype("str")

# escola_2024_updated = escola_2024_updated.drop(columns=tx_columns).merge(  # type: ignore
#     tx,
#     how="left",
#     left_on=["ano", "localizacao", "rede", "id_municipio", "id_escola"],
#     right_on=["ano", "localizacao", "rede", "id_municipio", "id_escola"],
# )[escola_2024.columns]  # type: ignore


# assert escola_2024_updated.shape == escola_2024.shape  # type: ignore

# escola_2024_updated = escola_2024_updated.replace("--", None)

# escola_2024_output_path = os.path.join(OUTPUT, "ano=2024")
# os.makedirs(escola_2024_output_path, exist_ok=True)

# escola_2024_updated.drop(columns=["ano"]).to_csv(
#     os.path.join(escola_2024_output_path, "escola.csv"), index=False
# )

# Atualizacao para 2024

keys_col_merge = ["ano", "id_municipio", "id_escola", "localizacao", "rede"]

for df in [afd, atu, dsu, had, icg, ied, ird, tdi, tx, tnr]:
    print(f"O Valor de {df} -> {df.shape}")

    df["id_municipio"] = df["id_municipio"].astype("Int64").astype("str")
    df["id_escola"] = df["id_escola"].astype("Int64").astype("str")
breakpoint()
df_2024 = reduce(
    lambda left, right: left.merge(
        right, left_on=keys_col_merge, right_on=keys_col_merge
    ),
    [afd, atu, dsu, had, icg, ied, ird, tdi, tx, tnr],
)

# Vamos adicionar colunas de dois indicadores
# Nao temos dados de 2024 para eles
for empty_col in [*tx_columns, *tnr_columns]:
    df_2024[empty_col] = None

df_2024 = df_2024.apply(lambda x: x.replace("--", None))


escola_2024_output_path = os.path.join(OUTPUT, "ano=2024")

os.makedirs(escola_2024_output_path)

# df_2024.drop(columns="ano").to_csv(  # type: ignore
#     os.path.join(escola_2024_output_path, "escola.csv"), index=False
# )
