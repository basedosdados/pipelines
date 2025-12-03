# -*- coding: utf-8 -*-
import os
import zipfile
from functools import reduce

import basedosdados as bd
import pandas as pd
import requests
from constants import (  # type: ignore
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

INPUT = os.path.join(os.getcwd(), "tmp", "brasil_uf_regiao", "input")
OUTPUT = os.path.join(os.getcwd(), "tmp", "brasil_uf_regiao", "output")

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


def download_data(ano: int) -> None:
    URLS = [
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/ATU_{ano}_BRASIL_REGIOES_UFS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/tx_rend_brasil_regioes_ufs_{ano}.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/HAD_{ano}_BRASIL_REGIOES_UFS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/TDI_{ano}_BRASIL_REGIOES_UFS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/tnr_brasil_regioes_ufs_{ano}.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/DSU_{ano}_BRASIL_REGIOES_UFS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/AFD_{ano}_BRASIL_REGIOES_UF.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/IRD_{ano}_BRASIL_REGIOES_UFS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/IED_{ano}_BRASIL_REGIOES_UFS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/ICG_{ano}_BRASIL_REGIOES_UFS.zip",
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


# ! ================================================ Brasil =================================================


def brasil(ano: int, tabela: str) -> None:
    afd = pd.read_excel(
        os.path.join(
            INPUT,
            f"AFD_{ano}_BRASIL_REGIOES_UF",
            f"AFD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=10,
    )

    afd = afd.rename(columns=rename_afd, errors="raise")

    afd = afd.loc[afd["ano"] == ano,]
    afd["localizacao"] = afd["localizacao"].str.lower()
    afd["rede"] = afd["rede"].str.lower().replace("pública", "publica")
    afd = afd[afd["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])
    ## Média de alunos por turma (ATU)

    atu = pd.read_excel(
        os.path.join(
            INPUT,
            f"ATU_{ano}_BRASIL_REGIOES_UFS",
            f"ATU_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    atu = atu.rename(columns=rename_atu, errors="raise")

    atu = atu.loc[atu["ano"] == ano,]
    atu["localizacao"] = atu["localizacao"].str.lower()
    atu["rede"] = atu["rede"].str.lower().replace("pública", "publica")
    atu = atu[atu["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    # Percentual de Docentes com Curso Superior
    dsu = pd.read_excel(
        os.path.join(
            INPUT,
            f"DSU_{ano}_BRASIL_REGIOES_UFS",
            f"DSU_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=9,
    )

    dsu = dsu.rename(columns=rename_dsu, errors="raise")

    dsu = dsu.loc[dsu["ano"] == ano,]
    dsu["localizacao"] = dsu["localizacao"].str.lower()
    dsu["rede"] = dsu["rede"].str.lower().replace("pública", "publica")
    dsu = dsu[dsu["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    breakpoint()
    # Média de Horas-aula diária HAD -> 2023

    had = pd.read_excel(
        os.path.join(
            INPUT,
            f"HAD_{ano}_BRASIL_REGIOES_UFS",
            f"HAD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    had = had.rename(columns=rename_had, errors="raise")

    had = had.loc[had["ano"] == ano,]
    had["localizacao"] = had["localizacao"].str.lower()
    had["rede"] = had["rede"].str.lower().replace("pública", "publica")
    had = had[had["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    # Complexidade de Gestão da Escola (ICG) -> 2023

    icg = pd.read_excel(
        os.path.join(
            INPUT,
            f"ICG_{ano}_BRASIL_REGIOES_UFS",
            f"ICG_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    icg = icg.rename(columns=rename_icg, errors="raise")

    icg = icg.loc[icg["ano"] == ano,]
    icg["localizacao"] = icg["localizacao"].str.lower()
    icg["rede"] = icg["rede"].str.lower().replace("pública", "publica")
    icg = icg[icg["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    # Esforço Docente (IED) -> 2023

    ied = pd.read_excel(
        os.path.join(
            INPUT,
            f"IED_{ano}_BRASIL_REGIOES_UFS",
            f"IED_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=10,
    )

    ied = ied.rename(columns=rename_ied, errors="raise")

    ied = ied.loc[ied["ano"] == ano,]
    ied["localizacao"] = ied["localizacao"].str.lower()
    ied["rede"] = ied["rede"].str.lower().replace("pública", "publica")
    ied = ied[ied["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    # Regularidade do Corpo Docente (IRD) -> 2023

    ird = pd.read_excel(
        os.path.join(
            INPUT,
            f"IRD_{ano}_BRASIL_REGIOES_UFS",
            f"IRD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=9,
    )

    ird = ird.rename(columns=rename_ird, errors="raise")

    ird = ird.loc[ird["ano"] == ano,]
    ird["localizacao"] = ird["localizacao"].str.lower()
    ird["rede"] = ird["rede"].str.lower().replace("pública", "publica")
    ird = ird[ird["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    # Taxas de Distorção Idade-série (TDI) -> 2023

    tdi = pd.read_excel(
        os.path.join(
            INPUT,
            f"TDI_{ano}_BRASIL_REGIOES_UFS",
            f"TDI_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tdi = tdi.rename(columns=rename_tdi, errors="raise")

    tdi = tdi.loc[tdi["ano"] == ano,]
    tdi["localizacao"] = tdi["localizacao"].str.lower()
    tdi["rede"] = tdi["rede"].str.lower().replace("pública", "publica")
    tdi = tdi[tdi["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    # Taxa de Não Resposta (tnr) -> 2023

    tnr = pd.read_excel(
        os.path.join(
            INPUT,
            f"tnr_brasil_regioes_ufs_{ano}",
            f"tnr_brasil_regioes_ufs_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tnr = tnr.rename(columns=rename_tnr, errors="raise")

    tnr = tnr.loc[tnr["ano"] == ano,]
    tnr["localizacao"] = tnr["localizacao"].str.lower()
    tnr["rede"] = tnr["rede"].str.lower().replace("pública", "publica")
    tnr = tnr[tnr["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    tx = pd.read_excel(
        os.path.join(
            INPUT,
            f"tx_rend_brasil_regioes_ufs_{ano}",
            f"tx_rend_brasil_regioes_ufs_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tx = tx.rename(columns=rename_tx, errors="raise")

    tx = tx.loc[tx["ano"] == ano,]
    tx["localizacao"] = tx["localizacao"].str.lower()
    tx["rede"] = tx["rede"].str.lower().replace("pública", "publica")
    tx = tx[tx["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    keys_col_merge = ["ano", "localizacao", "rede"]

    escola = [afd, atu, dsu, had, icg, ied, ird, tdi, tx, tnr]
    df = reduce(
        lambda left, right: pd.merge(
            left, right, on=keys_col_merge, how="outer"
        ),
        escola,
    )

    df.replace("--", None, inplace=True)

    for year in df["ano"].unique():
        output_path = os.path.join(OUTPUT, tabela, f"ano={year}")
        os.makedirs(output_path, exist_ok=True)
        df_year = df[df["ano"] == year].drop(columns="ano")
        df_year.to_csv(os.path.join(output_path, "brasil.csv"), index=False)


# ! ================================================ UF =================================================


def uf(ano: int, tabela: str) -> None:
    afd = pd.read_excel(
        os.path.join(
            INPUT,
            f"AFD_{ano}_BRASIL_REGIOES_UF",
            f"AFD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=10,
    )

    afd = afd.rename(columns=rename_afd, errors="raise")

    afd = afd.loc[afd["ano"] == ano,]
    afd["localizacao"] = afd["localizacao"].str.lower()
    afd["rede"] = afd["rede"].str.lower().replace("pública", "publica")
    afd = afd.loc[afd["UNIDGEO"].isin(estados)]
    ## Média de alunos por turma (ATU)

    atu = pd.read_excel(
        os.path.join(
            INPUT,
            f"ATU_{ano}_BRASIL_REGIOES_UFS",
            f"ATU_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    atu = atu.rename(columns=rename_atu, errors="raise")

    atu = atu.loc[atu["ano"] == ano,]
    atu["localizacao"] = atu["localizacao"].str.lower()
    atu["rede"] = atu["rede"].str.lower().replace("pública", "publica")
    atu = atu.loc[atu["UNIDGEO"].isin(estados)]

    # Percentual de Docentes com Curso Superior
    dsu = pd.read_excel(
        os.path.join(
            INPUT,
            f"DSU_{ano}_BRASIL_REGIOES_UFS",
            f"DSU_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=9,
    )

    dsu = dsu.rename(columns=rename_dsu, errors="raise")

    dsu = dsu.loc[dsu["ano"] == ano,]
    dsu["localizacao"] = dsu["localizacao"].str.lower()
    dsu["rede"] = dsu["rede"].str.lower().replace("pública", "publica")
    dsu = dsu.loc[dsu["UNIDGEO"].isin(estados)]

    # Média de Horas-aula diária HAD -> 2023

    had = pd.read_excel(
        os.path.join(
            INPUT,
            f"HAD_{ano}_BRASIL_REGIOES_UFS",
            f"HAD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    had = had.rename(columns=rename_had, errors="raise")

    had = had.loc[had["ano"] == ano,]
    had["localizacao"] = had["localizacao"].str.lower()
    had["rede"] = had["rede"].str.lower().replace("pública", "publica")
    had = had.loc[had["UNIDGEO"].isin(estados)]

    # Complexidade de Gestão da Escola (ICG) -> 2023

    icg = pd.read_excel(
        os.path.join(
            INPUT,
            f"ICG_{ano}_BRASIL_REGIOES_UFS",
            f"ICG_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    icg = icg.rename(columns=rename_icg, errors="raise")

    icg = icg.loc[icg["ano"] == ano,]
    icg["localizacao"] = icg["localizacao"].str.lower()
    icg["rede"] = icg["rede"].str.lower().replace("pública", "publica")
    icg = icg.loc[icg["UNIDGEO"].isin(estados)]

    # Esforço Docente (IED) -> 2023

    ied = pd.read_excel(
        os.path.join(
            INPUT,
            f"IED_{ano}_BRASIL_REGIOES_UFS",
            f"IED_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=10,
    )

    ied = ied.rename(columns=rename_ied, errors="raise")

    ied = ied.loc[ied["ano"] == ano,]
    ied["localizacao"] = ied["localizacao"].str.lower()
    ied["rede"] = ied["rede"].str.lower().replace("pública", "publica")
    ied = ied.loc[ied["UNIDGEO"].isin(estados)]

    # Regularidade do Corpo Docente (IRD) -> 2023

    ird = pd.read_excel(
        os.path.join(
            INPUT,
            f"IRD_{ano}_BRASIL_REGIOES_UFS",
            f"IRD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=9,
    )

    ird = ird.rename(columns=rename_ird, errors="raise")

    ird = ird.loc[ird["ano"] == ano,]
    ird["localizacao"] = ird["localizacao"].str.lower()
    ird["rede"] = ird["rede"].str.lower().replace("pública", "publica")
    ird = ird.loc[ird["UNIDGEO"].isin(estados)]

    # Taxas de Distorção Idade-série (TDI) -> 2023

    tdi = pd.read_excel(
        os.path.join(
            INPUT,
            f"TDI_{ano}_BRASIL_REGIOES_UFS",
            f"TDI_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tdi = tdi.rename(columns=rename_tdi, errors="raise")

    tdi = tdi.loc[tdi["ano"] == ano,]
    tdi["localizacao"] = tdi["localizacao"].str.lower()
    tdi["rede"] = tdi["rede"].str.lower().replace("pública", "publica")
    tdi = tdi.loc[tdi["UNIDGEO"].isin(estados)]

    # Taxa de Não Resposta (tnr) -> 2023

    tnr = pd.read_excel(
        os.path.join(
            INPUT,
            f"tnr_brasil_regioes_ufs_{ano}",
            f"tnr_brasil_regioes_ufs_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tnr = tnr.rename(columns=rename_tnr, errors="raise")

    tnr = tnr.loc[tnr["ano"] == ano,]
    tnr["localizacao"] = tnr["localizacao"].str.lower()
    tnr["rede"] = tnr["rede"].str.lower().replace("pública", "publica")
    tnr = tnr.loc[tnr["UNIDGEO"].isin(estados)]

    tx = pd.read_excel(
        os.path.join(
            INPUT,
            f"tx_rend_brasil_regioes_ufs_{ano}",
            f"tx_rend_brasil_regioes_ufs_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tx = tx.rename(columns=rename_tx, errors="raise")

    tx = tx.loc[tx["ano"] == ano,]
    tx["localizacao"] = tx["localizacao"].str.lower()
    tx["rede"] = tx["rede"].str.lower().replace("pública", "publica")
    tx = tx.loc[tx["UNIDGEO"].isin(estados)]

    keys_col_merge = ["ano", "localizacao", "rede", "UNIDGEO"]

    escola = [afd, atu, dsu, had, icg, ied, ird, tdi, tx, tnr]
    df = reduce(
        lambda left, right: pd.merge(
            left, right, on=keys_col_merge, how="outer"
        ),
        escola,
    )

    df = df.rename(columns={"UNIDGEO": "uf"})

    df.replace("--", None, inplace=True)
    df = df.merge(
        bd_dir[["nome", "sigla"]], left_on="uf", right_on="nome"
    ).drop(columns=["uf", "nome"])
    df = df.rename(columns={"sigla": "sigla_uf"})

    for year in df["ano"].unique():
        for uf in df["sigla_uf"].unique():
            output_path = os.path.join(
                OUTPUT, tabela, f"ano={year}", f"sigla_uf={uf}"
            )
            os.makedirs(output_path, exist_ok=True)
            df_year = df[(df["ano"] == year) & (df["sigla_uf"] == uf)].drop(
                columns=["ano", "sigla_uf"]
            )

            df_year.to_csv(os.path.join(output_path, "uf.csv"), index=False)


# ! ================================================ Região =================================================


def regiao(ano: int, tabela: str) -> None:
    afd = pd.read_excel(
        os.path.join(
            INPUT,
            f"AFD_{ano}_BRASIL_REGIOES_UF",
            f"AFD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=10,
    )

    afd = afd.rename(columns=rename_afd, errors="raise")

    afd = afd.loc[afd["ano"] == ano,]
    afd["localizacao"] = afd["localizacao"].str.lower()
    afd["rede"] = afd["rede"].str.lower().replace("pública", "publica")
    afd = afd.loc[afd["UNIDGEO"].isin(regioes)]
    ## Média de alunos por turma (ATU)

    atu = pd.read_excel(
        os.path.join(
            INPUT,
            f"ATU_{ano}_BRASIL_REGIOES_UFS",
            f"ATU_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    atu = atu.rename(columns=rename_atu, errors="raise")

    atu = atu.loc[atu["ano"] == ano,]
    atu["localizacao"] = atu["localizacao"].str.lower()
    atu["rede"] = atu["rede"].str.lower().replace("pública", "publica")
    atu = atu.loc[atu["UNIDGEO"].isin(regioes)]

    # Percentual de Docentes com Curso Superior
    dsu = pd.read_excel(
        os.path.join(
            INPUT,
            f"DSU_{ano}_BRASIL_REGIOES_UFS",
            f"DSU_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=9,
    )

    dsu = dsu.rename(columns=rename_dsu, errors="raise")

    dsu = dsu.loc[dsu["ano"] == ano,]
    dsu["localizacao"] = dsu["localizacao"].str.lower()
    dsu["rede"] = dsu["rede"].str.lower().replace("pública", "publica")
    dsu = dsu.loc[dsu["UNIDGEO"].isin(regioes)]

    # Média de Horas-aula diária HAD -> 2023

    had = pd.read_excel(
        os.path.join(
            INPUT,
            f"HAD_{ano}_BRASIL_REGIOES_UFS",
            f"HAD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    had = had.rename(columns=rename_had, errors="raise")

    had = had.loc[had["ano"] == ano,]
    had["localizacao"] = had["localizacao"].str.lower()
    had["rede"] = had["rede"].str.lower().replace("pública", "publica")
    had = had.loc[had["UNIDGEO"].isin(regioes)]

    # Complexidade de Gestão da Escola (ICG) -> 2023

    icg = pd.read_excel(
        os.path.join(
            INPUT,
            f"ICG_{ano}_BRASIL_REGIOES_UFS",
            f"ICG_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    icg = icg.rename(columns=rename_icg, errors="raise")

    icg = icg.loc[icg["ano"] == ano,]
    icg["localizacao"] = icg["localizacao"].str.lower()
    icg["rede"] = icg["rede"].str.lower().replace("pública", "publica")
    icg = icg.loc[icg["UNIDGEO"].isin(regioes)]

    # Esforço Docente (IED) -> 2023

    ied = pd.read_excel(
        os.path.join(
            INPUT,
            f"IED_{ano}_BRASIL_REGIOES_UFS",
            f"IED_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=10,
    )

    ied = ied.rename(columns=rename_ied, errors="raise")

    ied = ied.loc[ied["ano"] == ano,]
    ied["localizacao"] = ied["localizacao"].str.lower()
    ied["rede"] = ied["rede"].str.lower().replace("pública", "publica")
    ied = ied.loc[ied["UNIDGEO"].isin(regioes)]

    # Regularidade do Corpo Docente (IRD) -> 2023

    ird = pd.read_excel(
        os.path.join(
            INPUT,
            f"IRD_{ano}_BRASIL_REGIOES_UFS",
            f"IRD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=9,
    )

    ird = ird.rename(columns=rename_ird, errors="raise")

    ird = ird.loc[ird["ano"] == ano,]
    ird["localizacao"] = ird["localizacao"].str.lower()
    ird["rede"] = ird["rede"].str.lower().replace("pública", "publica")
    ird = ird.loc[ird["UNIDGEO"].isin(regioes)]

    # Taxas de Distorção Idade-série (TDI) -> 2023

    tdi = pd.read_excel(
        os.path.join(
            INPUT,
            f"TDI_{ano}_BRASIL_REGIOES_UFS",
            f"TDI_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tdi = tdi.rename(columns=rename_tdi, errors="raise")

    tdi = tdi.loc[tdi["ano"] == ano,]
    tdi["localizacao"] = tdi["localizacao"].str.lower()
    tdi["rede"] = tdi["rede"].str.lower().replace("pública", "publica")
    tdi = tdi.loc[tdi["UNIDGEO"].isin(regioes)]

    # Taxa de Não Resposta (tnr) -> 2023

    tnr = pd.read_excel(
        os.path.join(
            INPUT,
            f"tnr_brasil_regioes_ufs_{ano}",
            f"tnr_brasil_regioes_ufs_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tnr = tnr.rename(columns=rename_tnr, errors="raise")

    tnr = tnr.loc[tnr["ano"] == ano,]
    tnr["localizacao"] = tnr["localizacao"].str.lower()
    tnr["rede"] = tnr["rede"].str.lower().replace("pública", "publica")
    tnr = tnr.loc[tnr["UNIDGEO"].isin(regioes)]

    tx = pd.read_excel(
        os.path.join(
            INPUT,
            f"tx_rend_brasil_regioes_ufs_{ano}",
            f"tx_rend_brasil_regioes_ufs_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tx = tx.rename(columns=rename_tx, errors="raise")

    tx = tx.loc[tx["ano"] == ano,]
    tx["localizacao"] = tx["localizacao"].str.lower()
    tx["rede"] = tx["rede"].str.lower().replace("pública", "publica")
    tx = tx.loc[tx["UNIDGEO"].isin(regioes)]

    keys_col_merge = ["ano", "localizacao", "rede", "UNIDGEO"]

    escola = [afd, atu, dsu, had, icg, ied, ird, tdi, tx, tnr]
    df = reduce(
        lambda left, right: pd.merge(
            left, right, on=keys_col_merge, how="outer"
        ),
        escola,
    )

    df = df.replace("--", None)
    df = df.rename(columns={"UNIDGEO": "regiao"})
    for year in df["ano"].unique():
        output_path = os.path.join(OUTPUT, tabela, f"ano={year}")
        os.makedirs(output_path, exist_ok=True)
        df_year = df[(df["ano"] == year)].drop(columns=["ano"])
        df_year.to_csv(os.path.join(output_path, "uf.csv"), index=False)


if __name__ == "__main__":
    # download_data(ano=2024)
    # brasil(ano=2024, tabela="brasil")

    # uf(ano=2024, tabela="uf")

    regiao(ano=2024, tabela="regiao")
