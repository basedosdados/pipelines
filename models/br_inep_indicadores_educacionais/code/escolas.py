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
    # rename_icg,
    rename_ied,
    # rename_ird,
    rename_tdi,
    rename_tnr,
    rename_tx,
)


def escola(ano: int) -> None:
    URLS_ESCOLAS = [  # noqa: N806
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/AFD_{ano}_ESCOLAS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/ATU_{ano}_ESCOLAS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/DSU_{ano}_ESCOLAS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/HAD_{ano}_ESCOLAS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/ICG_{ano}_ESCOLAS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/IED_{ano}_ESCOLAS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/IRD_{ano}_ESCOLAS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/TDI_{ano}_ESCOLAS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/tnr_escolas_{ano}.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/tx_rend_escolas_{ano}.zip",
    ]

    INPUT = os.path.join(os.getcwd(), "tmp", "escolas", "input")  # noqa: N806
    OUTPUT = os.path.join(os.getcwd(), "tmp", "escolas", "output")  # noqa: N806
    os.makedirs(INPUT, exist_ok=True)
    os.makedirs(OUTPUT, exist_ok=True)

    for url in URLS_ESCOLAS:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        with open(os.path.join(INPUT, url.split("/")[-1]), "wb") as f:
            f.write(response.content)

    for file in os.listdir(INPUT):
        if file.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(INPUT, file)) as z:
                z.extractall(INPUT)
                os.remove(os.path.join(INPUT, file))

    COL_EXTEND_RENAME = {  # noqa: N806
        "CO_MUNICIPIO": "id_municipio",
        "CO_ENTIDADE": "id_escola",
    }
    UNSUED_COLS = ["NO_REGIAO", "SG_UF", "NO_MUNICIPIO", "NO_ENTIDADE"]  # noqa: N806

    afd = pd.read_excel(
        os.path.join(INPUT, f"AFD_{ano}_ESCOLAS", f"AFD_ESCOLAS_{ano}.xlsx"),
        skiprows=10,
    ).drop(columns=UNSUED_COLS)

    afd = afd.rename(
        columns={**COL_EXTEND_RENAME, **rename_afd}, errors="raise"
    )

    afd = afd.loc[afd["ano"] == ano,]
    afd["localizacao"] = afd["localizacao"].str.lower()

    atu = pd.read_excel(
        os.path.join(INPUT, f"ATU_{ano}_ESCOLAS", f"ATU_ESCOLAS_{ano}.xlsx"),
        skiprows=8,
    ).drop(columns=UNSUED_COLS)

    atu = atu.rename(
        columns={**COL_EXTEND_RENAME, **rename_atu}, errors="raise"
    )

    atu = atu.loc[atu["ano"] == ano,]
    atu["localizacao"] = atu["localizacao"].str.lower()

    dsu = pd.read_excel(
        os.path.join(INPUT, f"DSU_{ano}_ESCOLAS", f"DSU_ESCOLAS_{ano}.xlsx"),
        skiprows=9,
    ).drop(columns=UNSUED_COLS)

    dsu = dsu.rename(
        columns={**COL_EXTEND_RENAME, **rename_dsu}, errors="raise"
    )

    dsu = dsu.loc[dsu["ano"] == ano,]
    dsu["localizacao"] = dsu["localizacao"].str.lower()

    had = pd.read_excel(
        os.path.join(INPUT, f"HAD_{ano}_ESCOLAS", f"HAD_ESCOLAS_{ano}.xlsx"),
        skiprows=8,
    ).drop(columns=UNSUED_COLS)

    had = had.rename(
        columns={**COL_EXTEND_RENAME, **rename_had}, errors="raise"
    )

    had = had.loc[had["ano"] == ano,]
    had["localizacao"] = had["localizacao"].str.lower()

    icg = pd.read_excel(
        os.path.join(INPUT, f"ICG_{ano}_ESCOLAS", f"ICG_ESCOLAS_{ano}.xlsx"),
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

    icg = icg.loc[icg["ano"] == ano,]
    icg["localizacao"] = icg["localizacao"].str.lower()

    ied = pd.read_excel(
        os.path.join(INPUT, f"IED_{ano}_ESCOLAS", f"IED_ESCOLAS_{ano}.xlsx"),
        skiprows=10,
    ).drop(columns=UNSUED_COLS)

    ied = ied.rename(
        columns={
            **COL_EXTEND_RENAME,
            **rename_ied,
        },
        errors="raise",
    )

    ied = ied.loc[ied["ano"] == ano,]
    ied["localizacao"] = ied["localizacao"].str.lower()

    ird = pd.read_excel(
        os.path.join(INPUT, f"IRD_{ano}_ESCOLAS", f"IRD_ESCOLAS_{ano}.xlsx"),
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

    ird = ird.loc[ird["ano"] == ano,]
    ird["localizacao"] = ird["localizacao"].str.lower()

    tdi = pd.read_excel(
        os.path.join(INPUT, f"TDI_{ano}_ESCOLAS", f"TDI_ESCOLAS_{ano}.xlsx"),
        skiprows=8,
    ).drop(columns=UNSUED_COLS)

    tdi = tdi.rename(
        columns={**COL_EXTEND_RENAME, **rename_tdi}, errors="raise"
    )

    tdi = tdi.loc[tdi["ano"] == ano,]
    tdi["localizacao"] = tdi["localizacao"].str.lower()

    tnr = pd.read_excel(
        os.path.join(INPUT, f"tnr_escolas_{ano}", f"tnr_escolas_{ano}.xlsx"),
        skiprows=8,
    ).drop(columns=UNSUED_COLS)

    tnr = tnr.rename(
        columns={**COL_EXTEND_RENAME, **rename_tnr}, errors="raise"
    )

    tnr = tnr.loc[tnr["ano"] == ano,]
    tnr["localizacao"] = tnr["localizacao"].str.lower()

    tx = pd.read_excel(
        os.path.join(
            INPUT,
            f"tx_rend_escolas_{ano}",
            f"tx_rend_escolas_{ano}.xlsx",
        ),
        skiprows=8,
    ).drop(columns=UNSUED_COLS)

    tx = tx.rename(columns={**COL_EXTEND_RENAME, **rename_tx}, errors="raise")

    tx = tx.loc[tx["ano"] == ano,]
    tx["localizacao"] = tx["localizacao"].str.lower()

    keys_col_merge = [
        "ano",
        "id_municipio",
        "id_escola",
        "localizacao",
        "rede",
    ]

    escola = [afd, atu, dsu, had, icg, ied, ird, tdi, tx, tnr]

    for columns in escola:
        columns["id_municipio"] = (
            columns["id_municipio"].astype("Int64").astype("str")
        )
        columns["id_escola"] = (
            columns["id_escola"].astype("Int64").astype("str")
        )

    df = reduce(
        lambda left, right: pd.merge(  # noqa: PD015
            left, right, on=keys_col_merge, how="outer"
        ),
        escola,
    )

    df = df.apply(lambda x: x.replace("--", None))
    breakpoint()
    df["rede"] = (
        df["rede"].str.lower().str.replace("pública", "publica", regex=False)
    )

    df = df.drop(columns="ano")

    escola_output_path = os.path.join(OUTPUT, f"ano={ano}")
    os.makedirs(escola_output_path, exist_ok=True)
    df.to_csv(os.path.join(escola_output_path, "escola.csv"), index=False)


escola(ano=2023)
