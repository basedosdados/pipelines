# -*- coding: utf-8 -*-
import os
import zipfile
from functools import reduce

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


def municipios(ano: int) -> None:
    URLS_MUNICIPIOS = [
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/AFD_{ano}_MUNICIPIOS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/ATU_{ano}_MUNICIPIOS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/DSU_{ano}_MUNICIPIOS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/HAD_{ano}_MUNICIPIOS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/ICG_{ano}_MUNICIPIOS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/IED_{ano}_MUNICIPIOS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/IRD_{ano}_MUNICIPIOS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/TDI_{ano}_MUNICIPIOS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/tnr_municipios_{ano}.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/tx_rend_municipios_{ano}.zip",
    ]

    INPUT = os.path.join(os.getcwd(), "tmp", "municipios", "input")
    OUTPUT = os.path.join(os.getcwd(), "tmp", "municipios", "output")
    os.makedirs(INPUT, exist_ok=True)
    os.makedirs(OUTPUT, exist_ok=True)

    for url in URLS_MUNICIPIOS:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        with open(os.path.join(INPUT, url.split("/")[-1]), "wb") as f:
            f.write(response.content)

    for file in os.listdir(INPUT):
        if file.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(INPUT, file)) as z:
                z.extractall(INPUT)
                os.remove(os.path.join(INPUT, file))

    COL_ID_MUNICIPIO_RENAME = {"CO_MUNICIPIO": "id_municipio"}
    UNSUED_COLS = ["NO_REGIAO", "SG_UF", "NO_MUNICIPIO"]

    afd = pd.read_excel(
        os.path.join(
            INPUT, f"AFD_{ano}_MUNICIPIOS", f"AFD_MUNICIPIOS_{ano}.xlsx"
        ),
        skiprows=10,
    ).drop(columns=UNSUED_COLS)

    afd = afd.rename(
        columns={**COL_ID_MUNICIPIO_RENAME, **rename_afd}, errors="raise"
    )

    afd = afd.loc[afd["ano"] == ano,]
    afd["localizacao"] = afd["localizacao"].str.capitalize()
    afd["rede"] = (
        afd["rede"].str.replace("pública", "publica").str.capitalize()
    )

    atu = pd.read_excel(
        os.path.join(
            INPUT, f"ATU_{ano}_MUNICIPIOS", f"ATU_MUNICIPIOS_{ano}.xlsx"
        ),
        skiprows=8,
    ).drop(columns=UNSUED_COLS)

    atu = atu.rename(
        columns={**COL_ID_MUNICIPIO_RENAME, **rename_atu}, errors="raise"
    )

    atu = atu.loc[atu["ano"] == ano,]
    atu["localizacao"] = atu["localizacao"].str.capitalize()
    atu["rede"] = (
        atu["rede"].str.replace("pública", "publica").str.capitalize()
    )

    dsu = pd.read_excel(
        os.path.join(
            INPUT, f"DSU_{ano}_MUNICIPIOS", f"DSU_MUNICIPIOS_{ano}.xlsx"
        ),
        skiprows=9,
    ).drop(columns=UNSUED_COLS)

    dsu = dsu.rename(
        columns={**COL_ID_MUNICIPIO_RENAME, **rename_dsu}, errors="raise"
    )

    dsu = dsu.loc[dsu["ano"] == ano,]
    dsu["localizacao"] = dsu["localizacao"].str.capitalize()
    dsu["rede"] = (
        dsu["rede"].str.replace("pública", "publica").str.capitalize()
    )

    had = pd.read_excel(
        os.path.join(
            INPUT, f"HAD_{ano}_MUNICIPIOS", f"HAD_MUNICIPIOS_{ano}.xlsx"
        ),
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
        columns={**COL_ID_MUNICIPIO_RENAME, **rename_had_adapted},
        errors="raise",
    )

    had = had.loc[had["ano"] == ano,]
    had["localizacao"] = had["localizacao"].str.capitalize()
    had["rede"] = (
        had["rede"].str.replace("pública", "publica").str.capitalize()
    )

    icg = pd.read_excel(
        os.path.join(
            INPUT, f"ICG_{ano}_MUNICIPIOS", f"ICG_MUNICIPIOS_{ano}.xlsx"
        ),
        skiprows=8,
    ).drop(columns=UNSUED_COLS)

    icg = icg.rename(
        columns={**COL_ID_MUNICIPIO_RENAME, **rename_icg}, errors="raise"
    )

    icg = icg.loc[icg["ano"] == ano,]
    icg["localizacao"] = icg["localizacao"].str.capitalize()
    icg["rede"] = (
        icg["rede"].str.replace("pública", "publica").str.capitalize()
    )

    ied = pd.read_excel(
        os.path.join(
            INPUT, f"IED_{ano}_MUNICIPIOS", f"IED_MUNICIPIOS_{ano}.xlsx"
        ),
        skiprows=10,
    ).drop(columns=UNSUED_COLS)

    ied = ied.rename(
        columns={**COL_ID_MUNICIPIO_RENAME, **rename_ied}, errors="raise"
    )

    ied = ied.loc[ied["ano"] == ano,]
    ied["localizacao"] = ied["localizacao"].str.capitalize()
    ied["rede"] = (
        ied["rede"].str.replace("pública", "publica").str.capitalize()
    )

    ird = pd.read_excel(
        os.path.join(
            INPUT, f"IRD_{ano}_MUNICIPIOS", f"IRD_MUNICIPIOS_{ano}.xlsx"
        ),
        skiprows=9,
    ).drop(columns=UNSUED_COLS)

    ird = ird.rename(
        columns={**COL_ID_MUNICIPIO_RENAME, **rename_ird}, errors="raise"
    )

    ird = ird.loc[ird["ano"] == ano,]
    ird["localizacao"] = ird["localizacao"].str.capitalize()
    ird["rede"] = (
        ird["rede"].str.replace("pública", "publica").str.capitalize()
    )

    tdi = pd.read_excel(
        os.path.join(
            INPUT, f"TDI_{ano}_MUNICIPIOS", f"TDI_MUNICIPIOS_{ano}.xlsx"
        ),
        skiprows=8,
    ).drop(columns=UNSUED_COLS)

    tdi = tdi.rename(
        columns={**COL_ID_MUNICIPIO_RENAME, **rename_tdi}, errors="raise"
    )

    tdi = tdi.loc[tdi["ano"] == ano,]
    tdi["localizacao"] = tdi["localizacao"].str.capitalize()
    tdi["rede"] = (
        tdi["rede"].str.replace("pública", "publica").str.capitalize()
    )

    tnr = pd.read_excel(
        os.path.join(
            INPUT, f"tnr_municipios_{ano}", f"tnr_municipios_{ano}.xlsx"
        ),
        skiprows=8,
    ).drop(columns=UNSUED_COLS)

    tnr = tnr.rename(
        columns={**COL_ID_MUNICIPIO_RENAME, **rename_tnr}, errors="raise"
    )

    tnr = tnr.loc[tnr["ano"] == ano,]
    tnr["localizacao"] = tnr["localizacao"].str.capitalize()
    tnr["rede"] = (
        tnr["rede"].str.replace("pública", "publica").str.capitalize()
    )

    tx = pd.read_excel(
        os.path.join(
            INPUT,
            f"tx_rend_municipios_{ano}",
            f"tx_rend_municipios_{ano}.xlsx",
        ),
        skiprows=8,
    ).drop(columns=UNSUED_COLS)

    tx = tx.rename(
        columns={**COL_ID_MUNICIPIO_RENAME, **rename_tx}, errors="raise"
    )

    tx = tx.loc[tx["ano"] == ano]
    tx["localizacao"] = tx["localizacao"].str.capitalize()
    tx["rede"] = tx["rede"].str.replace("pública", "publica").str.capitalize()

    keys_col_merge = ["ano", "id_municipio", "localizacao", "rede"]

    escola = [afd, atu, dsu, had, icg, ied, ird, tdi, tx, tnr]
    for columns in escola:
        columns["id_municipio"] = (
            columns["id_municipio"].astype("Int64").astype("str")
        )

    df = reduce(
        lambda left, right: pd.merge(
            left, right, on=keys_col_merge, how="outer"
        ),
        escola,
    )
    df = df.apply(lambda x: x.replace("--", None))

    df = df.drop(columns="ano")

    escola_output_path = os.path.join(OUTPUT, f"ano={ano}")
    os.makedirs(escola_output_path, exist_ok=True)
    df.to_csv(os.path.join(escola_output_path, "municipio.csv"), index=False)


municipios(ano=2018)
