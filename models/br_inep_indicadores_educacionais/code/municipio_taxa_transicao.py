import os
import zipfile

import basedosdados as bd
import pandas as pd

URLS = [
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/taxa_transicao/tx_transicao_municipios_2018_2019.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/taxa_transicao/tx_transicao_municipios_2019_2020.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/taxa_transicao/tx_transicao_municipios_2020_2021.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/taxa_transicao/tx_transicao_municipios_2021_2022.zip",
]


INPUT = os.path.join(os.getcwd(), "tmp")

if not os.path.exists(INPUT):
    os.mkdir(INPUT)

INPUT_TX_MUN = os.path.join(INPUT, "taxa_transicao_municipio")

OUTPUT = os.path.join(os.getcwd(), "output")

if not os.path.exists(OUTPUT):
    os.mkdir(OUTPUT)

os.makedirs(INPUT_TX_MUN, exist_ok=True)

for url in URLS:
    os.system(f"cd {INPUT_TX_MUN}; curl -O -k {url}")


for file in os.listdir(INPUT_TX_MUN):
    with zipfile.ZipFile(os.path.join(INPUT_TX_MUN, file)) as z:
        z.extractall(INPUT_TX_MUN)

folders = [
    os.path.join(INPUT_TX_MUN, i)
    for i in os.listdir(INPUT_TX_MUN)
    if not i.endswith(".zip")
]

files = [
    os.path.join(f, i)
    for f in folders
    for i in os.listdir(f)
    if i.endswith(".xlsx")
]

municipio_updated = pd.concat([pd.read_excel(f, skiprows=8) for f in files])

renames = {
    "CO_MUNICIPIO": "id_municipio",
    "NO_LOCALIZACAO": "localizacao",
    "NO_DEPENDENCIA": "rede",
    "1_CAT1_CATFUN": "taxa_promocao_ef",
    "1_CAT1_CATFUN_AI": "taxa_promocao_ef_anos_iniciais",
    "1_CAT1_CATFUN_AF": "taxa_promocao_ef_anos_finais",
    "1_CAT1_CATFUN_01": "taxa_promocao_ef_1_ano",
    "1_CAT1_CATFUN_02": "taxa_promocao_ef_2_ano",
    "1_CAT1_CATFUN_03": "taxa_promocao_ef_3_ano",
    "1_CAT1_CATFUN_04": "taxa_promocao_ef_4_ano",
    "1_CAT1_CATFUN_05": "taxa_promocao_ef_5_ano",
    "1_CAT1_CATFUN_06": "taxa_promocao_ef_6_ano",
    "1_CAT1_CATFUN_07": "taxa_promocao_ef_7_ano",
    "1_CAT1_CATFUN_08": "taxa_promocao_ef_8_ano",
    "1_CAT1_CATFUN_09": "taxa_promocao_ef_9_ano",
    "1_CAT1_CATMED": "taxa_promocao_em",
    "1_CAT1_CATMED_01": "taxa_promocao_em_1_ano",
    "1_CAT1_CATMED_02": "taxa_promocao_em_2_ano",
    "1_CAT1_CATMED_03": "taxa_promocao_em_3_ano",
    "1_CAT2_CATFUN": "taxa_repetencia_ef",
    "1_CAT2_CATFUN_AI": "taxa_repetencia_ef_anos_iniciais",
    "1_CAT2_CATFUN_AF": "taxa_repetencia_ef_anos_finais",
    "1_CAT2_CATFUN_01": "taxa_repetencia_ef_1_ano",
    "1_CAT2_CATFUN_02": "taxa_repetencia_ef_2_ano",
    "1_CAT2_CATFUN_03": "taxa_repetencia_ef_3_ano",
    "1_CAT2_CATFUN_04": "taxa_repetencia_ef_4_ano",
    "1_CAT2_CATFUN_05": "taxa_repetencia_ef_5_ano",
    "1_CAT2_CATFUN_06": "taxa_repetencia_ef_6_ano",
    "1_CAT2_CATFUN_07": "taxa_repetencia_ef_7_ano",
    "1_CAT2_CATFUN_08": "taxa_repetencia_ef_8_ano",
    "1_CAT2_CATFUN_09": "taxa_repetencia_ef_9_ano",
    "1_CAT2_CATMED": "taxa_repetencia_em",
    "1_CAT2_CATMED_01": "taxa_repetencia_em_1_ano",
    "1_CAT2_CATMED_02": "taxa_repetencia_em_2_ano",
    "1_CAT2_CATMED_03": "taxa_repetencia_em_3_ano",
    "1_CAT3_CATFUN": "taxa_evasao_ef",
    "1_CAT3_CATFUN_AI": "taxa_evasao_ef_anos_iniciais",
    "1_CAT3_CATFUN_AF": "taxa_evasao_ef_anos_finais",
    "1_CAT3_CATFUN_01": "taxa_evasao_ef_1_ano",
    "1_CAT3_CATFUN_02": "taxa_evasao_ef_2_ano",
    "1_CAT3_CATFUN_03": "taxa_evasao_ef_3_ano",
    "1_CAT3_CATFUN_04": "taxa_evasao_ef_4_ano",
    "1_CAT3_CATFUN_05": "taxa_evasao_ef_5_ano",
    "1_CAT3_CATFUN_06": "taxa_evasao_ef_6_ano",
    "1_CAT3_CATFUN_07": "taxa_evasao_ef_7_ano",
    "1_CAT3_CATFUN_08": "taxa_evasao_ef_8_ano",
    "1_CAT3_CATFUN_09": "taxa_evasao_ef_9_ano",
    "1_CAT3_CATMED": "taxa_evasao_em",
    "1_CAT3_CATMED_01": "taxa_evasao_em_1_ano",
    "1_CAT3_CATMED_02": "taxa_evasao_em_2_ano",
    "1_CAT3_CATMED_03": "taxa_evasao_em_3_ano",
    "1_CAT4_CATFUN": "taxa_migracao_eja_ef",
    "1_CAT4_CATFUN_AI": "taxa_migracao_eja_ef_anos_iniciais",
    "1_CAT4_CATFUN_AF": "taxa_migracao_eja_ef_anos_finais",
    "1_CAT4_CATFUN_01": "taxa_migracao_eja_ef_1_ano",
    "1_CAT4_CATFUN_02": "taxa_migracao_eja_ef_2_ano",
    "1_CAT4_CATFUN_03": "taxa_migracao_eja_ef_3_ano",
    "1_CAT4_CATFUN_04": "taxa_migracao_eja_ef_4_ano",
    "1_CAT4_CATFUN_05": "taxa_migracao_eja_ef_5_ano",
    "1_CAT4_CATFUN_06": "taxa_migracao_eja_ef_6_ano",
    "1_CAT4_CATFUN_07": "taxa_migracao_eja_ef_7_ano",
    "1_CAT4_CATFUN_08": "taxa_migracao_eja_ef_8_ano",
    "1_CAT4_CATFUN_09": "taxa_migracao_eja_ef_9_ano",
    "1_CAT4_CATMED": "taxa_migracao_eja_em",
    "1_CAT4_CATMED_01": "taxa_migracao_eja_em_1_ano",
    "1_CAT4_CATMED_02": "taxa_migracao_eja_em_2_ano",
    "1_CAT4_CATMED_03": "taxa_migracao_eja_em_3_ano",
}

municipio_updated = municipio_updated.drop(
    columns=["NO_REGIAO", "NO_UF", "NO_MUNICIPIO"]
).rename(columns=renames)

municipio_updated = municipio_updated.loc[
    municipio_updated["id_municipio"].notna(),
]

municipio_updated["NU_ANO_CENSO"] = municipio_updated[
    "NU_ANO_CENSO"
].str.split("/")

municipio_updated["ano_de"] = municipio_updated["NU_ANO_CENSO"].apply(
    lambda l: int(l[0])  # noqa: E741
)
municipio_updated["ano_para"] = municipio_updated["NU_ANO_CENSO"].apply(
    lambda l: int(l[1])  # noqa: E741
)

municipio_updated = municipio_updated.drop(columns=["NU_ANO_CENSO"])

municipio_updated["id_municipio"] = (
    municipio_updated["id_municipio"].astype("Int64").astype("string")
)

municipio_updated = municipio_updated.replace("--", None)

bq_municipio_tran = bd.read_sql(
    """
SELECT
  *
FROM
  `basedosdados.br_inep_indicadores_educacionais.municipio_taxas_transicao`
""",
    billing_project_id="basedosdados-dev",
)

assert municipio_updated.shape[1] == bq_municipio_tran.shape[1]  # type: ignore

municipio_output_path = os.path.join(OUTPUT, "municipio_taxa_transicao")

for ano_para, df in pd.concat(
    [municipio_updated[bq_municipio_tran.columns], bq_municipio_tran]  # type: ignore
).groupby("ano_para"):
    path = os.path.join(municipio_output_path, f"ano={ano_para}")
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["ano_para"]).to_csv(
        os.path.join(path, "data.csv"), index=False
    )
