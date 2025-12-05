import os
import zipfile

# import basedosdados as bd
import pandas as pd
import requests

URLS = [
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/taxa_transicao/tx_transicao_municipios_2018_2019.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/taxa_transicao/tx_transicao_municipios_2019_2020.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/taxa_transicao/tx_transicao_municipios_2020_2021.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/taxa_transicao/tx_transicao_municipios_2021_2022.zip",
]


INPUT = os.path.join(
    os.getcwd(),
    "tmp",
    "taxa_transicao_municipio",
    "input",
)
OUTPUT = os.path.join(os.getcwd(), "tmp", "taxa_transicao_municipio", "output")

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

for url in URLS:
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    with open(os.path.join(INPUT, url.split("/")[-1]), "wb") as f:
        f.write(response.content)

for file in os.listdir(INPUT):
    if file.endswith(".zip"):
        with zipfile.ZipFile(os.path.join(INPUT, file)) as z:
            z.extractall(INPUT)
            os.remove(os.path.join(INPUT, file))

df_2018_2019 = pd.read_excel(
    os.path.join(
        INPUT,
        "TX_TRANSICAO_MUNICIPIOS_2018_2019",
        "TX_TRANSICAO_MUNICIPIOS_2018_2019.xlsx",
    ),
    skiprows=8,
)

df_2019_2020 = pd.read_excel(
    os.path.join(
        INPUT,
        "TX_TRANSICAO_MUNICIPIOS_2019_2020",
        "TX_TRANSICAO_MUNICIPIOS_2019_2020.xlsx",
    ),
    skiprows=8,
)

df_2020_2021 = pd.read_excel(
    os.path.join(
        INPUT,
        "TX_TRANSICAO_MUNICIPIOS_2020_2021",
        "TX_TRANSICAO_MUNICIPIOS_2020_2021.xlsx",
    ),
    skiprows=8,
)

df_2021_2022 = pd.read_excel(
    os.path.join(
        INPUT,
        "TX_TRANSICAO_MUNICIPIOS_2021_2022",
        "TX_TRANSICAO_MUNICIPIOS_2021_2022.xlsx",
    ),
    skiprows=8,
)

df_2021_2022 = df_2021_2022.rename(
    columns={"NU_ANO_C+A11+A+A6:A9": "NU_ANO_CENSO"}
)


df = pd.concat(
    [
        df_2018_2019,
        df_2019_2020,
        df_2020_2021,
        df_2021_2022,
    ]
)

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

df = df.drop(columns=["NO_REGIAO", "NO_UF", "NO_MUNICIPIO"]).rename(
    columns=renames
)

df = df.loc[df["id_municipio"].notna(),]
breakpoint()
df["ano_de"] = df["NU_ANO_CENSO"].str.split("/").str[0]
df["ano_para"] = df["NU_ANO_CENSO"].str.split("/").str[1]


df = df.drop(columns=["NU_ANO_CENSO"])
breakpoint()
df["id_municipio"] = df["id_municipio"].astype("Int64").astype("string")

df = df.replace("--", None)

municipio_output_path = os.path.join(OUTPUT, "municipio_taxa_transicao")

df = df.rename(columns={"ano_para": "ano"})
for ano in df["ano"].unique():
    path = os.path.join(municipio_output_path, f"ano={ano}")
    os.makedirs(path, exist_ok=True)
    df[df["ano"] == ano].drop(columns=["ano"]).to_csv(
        os.path.join(path, "data.csv"), index=False
    )
