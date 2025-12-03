# -*- coding: utf-8 -*-
import os
import zipfile

import basedosdados as bd
import pandas as pd
import requests

INPUT = os.path.join(os.getcwd(), "tmp", "tx_brasil_uf_regiao", "input")
OUTPUT = os.path.join(os.getcwd(), "tmp", "tx_brasil_uf_regiao", "output")

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)


# Ler diretório de estados e regiões (para mapear UFs e regiões)
bd_dir = bd.read_sql(
    "SELECT sigla, nome, regiao FROM `basedosdados.br_bd_diretorios_brasil.uf`",
    billing_project_id="basedosdados-dev",
)
regioes = bd_dir["regiao"].unique()
estados = bd_dir["nome"].unique()
estados_sigla = bd_dir["sigla"].unique()


def download() -> None:
    URLS = [
        "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/taxa_transicao/tx_transicao_brasil_regioes_ufs_2021_2022.zip"
    ]

    for url in URLS:
        print(url)
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        with open(os.path.join(INPUT, url.split("/")[-1]), "wb") as f:
            f.write(response.content)

    for file in os.listdir(INPUT):
        if file.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(INPUT, file)) as z:
                z.extractall(INPUT)
                os.remove(os.path.join(INPUT, file))


def brasil() -> None:
    df = pd.read_excel(
        "tmp/taxa_transicao_br_regioes_uf/input/TX_TRANSICAO_BRASIL_REGIOES_UFS_2021_2022/TX_TRANSICAO_BRASIL_REGIOES_UFS_2021_2022.xlsx",
        skiprows=8,
    )

    renames = {
        "NU_ANO_CENSO": "ano",
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
    df["NU_ANO_CENSO"] = df["NU_ANO_CENSO"].str.split("/").str[1]
    df = df.rename(columns=renames, errors="raise")

    df = df.replace("--", None)

    # ! --------> Brasil
    df[df["CODIGO"] == "Brasil"].drop(columns=["CODIGO"]).to_csv(
        os.path.join(OUTPUT, "tx_transicao_brasil_2022.csv"), index=False
    )

    # ! --------> UFs

    df_uf = df[df["CODIGO"].isin(estados)]

    df_uf = df_uf.rename(columns={"CODIGO": "uf"})

    df_uf = df_uf.replace("--", None)

    df_uf = df_uf.merge(
        bd_dir[["nome", "sigla"]], left_on="uf", right_on="nome"
    ).drop(columns=["uf", "nome"])
    df_uf = df_uf.rename(columns={"sigla": "sigla_uf"})

    for uf in df_uf["sigla_uf"].unique():
        output_path = os.path.join(OUTPUT, "uf", f"sigla_uf={uf}")
        os.makedirs(output_path, exist_ok=True)
        df_uf_year = df_uf[(df_uf["sigla_uf"] == uf)].drop(columns="sigla_uf")

        df_uf_year.to_csv(
            os.path.join(output_path, "uf_2022.csv"), index=False
        )

    # ! --------> Regiões

    df_regiao = df[df["CODIGO"].isin(regioes)]
    df_regiao = df_regiao.replace("--", None)
    df_regiao = df_regiao.rename(columns={"CODIGO": "regiao"})
    output_path = os.path.join(OUTPUT, "regiao")
    os.makedirs(output_path, exist_ok=True)
    df_regiao.to_csv(os.path.join(output_path, "regiao_2022.csv"), index=False)


brasil()
