import os
import sys
import zipfile
from functools import reduce

import basedosdados as bd
import pandas as pd
import requests

# Allow running from any CWD: locate project root relative to this file
_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(_FILE_DIR, "..", "..", ".."))

sys.path.insert(0, _FILE_DIR)
from constants import (  # type: ignore  # noqa: E402
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

# bd_dir is loaded lazily inside uf() / regiao() to avoid BQ calls when only brasil() is used


def _input_dir(ano: int) -> str:
    """Return the input directory for a given year's raw files."""
    return os.path.join(
        PROJECT_ROOT, "input", "br_inep_indicadores_educacionais", str(ano)
    )


def _output_dir() -> str:
    return os.path.join(
        PROJECT_ROOT, "output", "br_inep_indicadores_educacionais"
    )


def download_data(ano: int) -> None:
    input_dir = _input_dir(ano)
    os.makedirs(input_dir, exist_ok=True)
    URLS = [  # noqa: N806
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
        with open(os.path.join(input_dir, url.split("/")[-1]), "wb") as f:
            f.write(response.content)

    for file in os.listdir(input_dir):
        if file.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(input_dir, file)) as z:
                z.extractall(input_dir)
                os.remove(os.path.join(input_dir, file))


# ! ================================================ Brasil =================================================


def brasil(ano: int, tabela: str) -> None:
    input_dir = _input_dir(ano)
    output_dir = _output_dir()

    def read_xl(
        subdir: str, filename: str, skiprows: int, rename_map: dict
    ) -> pd.DataFrame:
        path = os.path.join(input_dir, subdir, subdir, filename)
        df = pd.read_excel(path, skiprows=skiprows)
        df = df.rename(columns=rename_map, errors="ignore")
        df = df.loc[df["ano"] == ano]
        df["localizacao"] = df["localizacao"].str.lower()
        df["rede"] = (
            df["rede"]
            .str.lower()
            .str.replace("pública", "publica", regex=False)
        )
        return df

    afd = read_xl(
        f"AFD_{ano}_BRASIL_REGIOES_UF",
        f"AFD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        10,
        rename_afd,
    )
    afd = afd[afd["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    atu = read_xl(
        f"ATU_{ano}_BRASIL_REGIOES_UFS",
        f"ATU_BRASIL_REGIOES_UFS_{ano}.xlsx",
        8,
        rename_atu,
    )
    atu = atu[atu["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    dsu = read_xl(
        f"DSU_{ano}_BRASIL_REGIOES_UFS",
        f"DSU_BRASIL_REGIOES_UFS_{ano}.xlsx",
        9,
        rename_dsu,
    )
    dsu = dsu[dsu["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])
    # dsu_ep / dsu_ee absent from some years' DSU files
    for col in ("dsu_ep", "dsu_ee"):
        if col not in dsu.columns:
            dsu[col] = None

    had = read_xl(
        f"HAD_{ano}_BRASIL_REGIOES_UFS",
        f"HAD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        8,
        rename_had,
    )
    had = had[had["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    icg = read_xl(
        f"ICG_{ano}_BRASIL_REGIOES_UFS",
        f"ICG_BRASIL_REGIOES_UFS_{ano}.xlsx",
        8,
        rename_icg,
    )
    icg = icg[icg["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    ied = read_xl(
        f"IED_{ano}_BRASIL_REGIOES_UFS",
        f"IED_BRASIL_REGIOES_UFS_{ano}.xlsx",
        10,
        rename_ied,
    )
    ied = ied[ied["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    ird = read_xl(
        f"IRD_{ano}_BRASIL_REGIOES_UFS",
        f"IRD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        9,
        rename_ird,
    )
    ird = ird[ird["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    tdi = read_xl(
        f"TDI_{ano}_BRASIL_REGIOES_UFS",
        f"TDI_BRASIL_REGIOES_UFS_{ano}.xlsx",
        8,
        rename_tdi,
    )
    tdi = tdi[tdi["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    keys_col_merge = ["ano", "localizacao", "rede"]
    dfs = [afd, atu, dsu, had, icg, ied, ird, tdi]

    # tnr and tx_rend not published for all years
    tnr_path = os.path.join(
        input_dir,
        f"tnr_brasil_regioes_ufs_{ano}",
        f"tnr_brasil_regioes_ufs_{ano}.xlsx",
    )
    if os.path.exists(tnr_path):
        tnr = pd.read_excel(tnr_path, skiprows=8)
        tnr = tnr.rename(columns=rename_tnr, errors="ignore")
        tnr = tnr.loc[tnr["ano"] == ano]
        tnr["localizacao"] = tnr["localizacao"].str.lower()
        tnr["rede"] = (
            tnr["rede"]
            .str.lower()
            .str.replace("pública", "publica", regex=False)
        )
        tnr = tnr[tnr["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])
        dfs.append(tnr)

    tx_path = os.path.join(
        input_dir,
        f"tx_rend_brasil_regioes_ufs_{ano}",
        f"tx_rend_brasil_regioes_ufs_{ano}.xlsx",
    )
    if os.path.exists(tx_path):
        tx = pd.read_excel(tx_path, skiprows=8)
        tx = tx.rename(columns=rename_tx, errors="ignore")
        tx = tx.loc[tx["ano"] == ano]
        tx["localizacao"] = tx["localizacao"].str.lower()
        tx["rede"] = (
            tx["rede"]
            .str.lower()
            .str.replace("pública", "publica", regex=False)
        )
        tx = tx[tx["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])
        dfs.append(tx)

    df = reduce(
        lambda left, right: pd.merge(  # noqa: PD015
            left, right, on=keys_col_merge, how="outer"
        ),
        dfs,
    )
    df = df.replace("--", None)

    # Ensure NULL columns for any tx/tnr cols absent when files missing
    for col in list(rename_tx.values()) + list(rename_tnr.values()):
        if col not in keys_col_merge and col not in df.columns:
            df[col] = None

    # Column order: matches DBT model (ano + localizacao + rede + indicators)
    col_order = [
        "ano",
        "localizacao",
        "rede",
        "atu_ei",
        "atu_ei_creche",
        "atu_ei_pre_escola",
        "atu_ef",
        "atu_ef_anos_iniciais",
        "atu_ef_anos_finais",
        "atu_ef_1_ano",
        "atu_ef_2_ano",
        "atu_ef_3_ano",
        "atu_ef_4_ano",
        "atu_ef_5_ano",
        "atu_ef_6_ano",
        "atu_ef_7_ano",
        "atu_ef_8_ano",
        "atu_ef_9_ano",
        "atu_ef_turmas_unif_multi_fluxo",
        "atu_em",
        "atu_em_1_ano",
        "atu_em_2_ano",
        "atu_em_3_ano",
        "atu_em_4_ano",
        "atu_em_nao_seriado",
        "had_ei",
        "had_ei_creche",
        "had_ei_pre_escola",
        "had_ef",
        "had_ef_anos_iniciais",
        "had_ef_anos_finais",
        "had_ef_1_ano",
        "had_ef_2_ano",
        "had_ef_3_ano",
        "had_ef_4_ano",
        "had_ef_5_ano",
        "had_ef_6_ano",
        "had_ef_7_ano",
        "had_ef_8_ano",
        "had_ef_9_ano",
        "had_em",
        "had_em_1_ano",
        "had_em_2_ano",
        "had_em_3_ano",
        "had_em_4_ano",
        "had_em_nao_seriado",
        "tdi_ef",
        "tdi_ef_anos_iniciais",
        "tdi_ef_anos_finais",
        "tdi_ef_1_ano",
        "tdi_ef_2_ano",
        "tdi_ef_3_ano",
        "tdi_ef_4_ano",
        "tdi_ef_5_ano",
        "tdi_ef_6_ano",
        "tdi_ef_7_ano",
        "tdi_ef_8_ano",
        "tdi_ef_9_ano",
        "tdi_em",
        "tdi_em_1_ano",
        "tdi_em_2_ano",
        "tdi_em_3_ano",
        "tdi_em_4_ano",
        "taxa_aprovacao_ef",
        "taxa_aprovacao_ef_anos_iniciais",
        "taxa_aprovacao_ef_anos_finais",
        "taxa_aprovacao_ef_1_ano",
        "taxa_aprovacao_ef_2_ano",
        "taxa_aprovacao_ef_3_ano",
        "taxa_aprovacao_ef_4_ano",
        "taxa_aprovacao_ef_5_ano",
        "taxa_aprovacao_ef_6_ano",
        "taxa_aprovacao_ef_7_ano",
        "taxa_aprovacao_ef_8_ano",
        "taxa_aprovacao_ef_9_ano",
        "taxa_aprovacao_em",
        "taxa_aprovacao_em_1_ano",
        "taxa_aprovacao_em_2_ano",
        "taxa_aprovacao_em_3_ano",
        "taxa_aprovacao_em_4_ano",
        "taxa_aprovacao_em_nao_seriado",
        "taxa_reprovacao_ef",
        "taxa_reprovacao_ef_anos_iniciais",
        "taxa_reprovacao_ef_anos_finais",
        "taxa_reprovacao_ef_1_ano",
        "taxa_reprovacao_ef_2_ano",
        "taxa_reprovacao_ef_3_ano",
        "taxa_reprovacao_ef_4_ano",
        "taxa_reprovacao_ef_5_ano",
        "taxa_reprovacao_ef_6_ano",
        "taxa_reprovacao_ef_7_ano",
        "taxa_reprovacao_ef_8_ano",
        "taxa_reprovacao_ef_9_ano",
        "taxa_reprovacao_em",
        "taxa_reprovacao_em_1_ano",
        "taxa_reprovacao_em_2_ano",
        "taxa_reprovacao_em_3_ano",
        "taxa_reprovacao_em_4_ano",
        "taxa_reprovacao_em_nao_seriado",
        "taxa_abandono_ef",
        "taxa_abandono_ef_anos_iniciais",
        "taxa_abandono_ef_anos_finais",
        "taxa_abandono_ef_1_ano",
        "taxa_abandono_ef_2_ano",
        "taxa_abandono_ef_3_ano",
        "taxa_abandono_ef_4_ano",
        "taxa_abandono_ef_5_ano",
        "taxa_abandono_ef_6_ano",
        "taxa_abandono_ef_7_ano",
        "taxa_abandono_ef_8_ano",
        "taxa_abandono_ef_9_ano",
        "taxa_abandono_em",
        "taxa_abandono_em_1_ano",
        "taxa_abandono_em_2_ano",
        "taxa_abandono_em_3_ano",
        "taxa_abandono_em_4_ano",
        "taxa_abandono_em_nao_seriado",
        "tnr_ef",
        "tnr_ef_anos_iniciais",
        "tnr_ef_anos_finais",
        "tnr_ef_1_ano",
        "tnr_ef_2_ano",
        "tnr_ef_3_ano",
        "tnr_ef_4_ano",
        "tnr_ef_5_ano",
        "tnr_ef_6_ano",
        "tnr_ef_7_ano",
        "tnr_ef_8_ano",
        "tnr_ef_9_ano",
        "tnr_em",
        "tnr_em_1_ano",
        "tnr_em_2_ano",
        "tnr_em_3_ano",
        "tnr_em_4_ano",
        "tnr_em_nao_seriado",
        "dsu_ei",
        "dsu_ei_creche",
        "dsu_ei_pre_escola",
        "dsu_ef",
        "dsu_ef_anos_iniciais",
        "dsu_ef_anos_finais",
        "dsu_em",
        "dsu_ep",
        "dsu_eja",
        "dsu_ee",
        "afd_ei_grupo_1",
        "afd_ei_grupo_2",
        "afd_ei_grupo_3",
        "afd_ei_grupo_4",
        "afd_ei_grupo_5",
        "afd_ef_grupo_1",
        "afd_ef_grupo_2",
        "afd_ef_grupo_3",
        "afd_ef_grupo_4",
        "afd_ef_grupo_5",
        "afd_ef_anos_iniciais_grupo_1",
        "afd_ef_anos_iniciais_grupo_2",
        "afd_ef_anos_iniciais_grupo_3",
        "afd_ef_anos_iniciais_grupo_4",
        "afd_ef_anos_iniciais_grupo_5",
        "afd_ef_anos_finais_grupo_1",
        "afd_ef_anos_finais_grupo_2",
        "afd_ef_anos_finais_grupo_3",
        "afd_ef_anos_finais_grupo_4",
        "afd_ef_anos_finais_grupo_5",
        "afd_em_grupo_1",
        "afd_em_grupo_2",
        "afd_em_grupo_3",
        "afd_em_grupo_4",
        "afd_em_grupo_5",
        "afd_eja_fundamental_grupo_1",
        "afd_eja_fundamental_grupo_2",
        "afd_eja_fundamental_grupo_3",
        "afd_eja_fundamental_grupo_4",
        "afd_eja_fundamental_grupo_5",
        "afd_eja_medio_grupo_1",
        "afd_eja_medio_grupo_2",
        "afd_eja_medio_grupo_3",
        "afd_eja_medio_grupo_4",
        "afd_eja_medio_grupo_5",
        "ird_baixa_regularidade",
        "ird_media_baixa",
        "ird_media_alta",
        "ird_alta",
        "ied_ef_nivel_1",
        "ied_ef_nivel_2",
        "ied_ef_nivel_3",
        "ied_ef_nivel_4",
        "ied_ef_nivel_5",
        "ied_ef_nivel_6",
        "ied_ef_anos_iniciais_nivel_1",
        "ied_ef_anos_iniciais_nivel_2",
        "ied_ef_anos_iniciais_nivel_3",
        "ied_ef_anos_iniciais_nivel_4",
        "ied_ef_anos_iniciais_nivel_5",
        "ied_ef_anos_iniciais_nivel_6",
        "ied_ef_anos_finais_nivel_1",
        "ied_ef_anos_finais_nivel_2",
        "ied_ef_anos_finais_nivel_3",
        "ied_ef_anos_finais_nivel_4",
        "ied_ef_anos_finais_nivel_5",
        "ied_ef_anos_finais_nivel_6",
        "ied_em_nivel_1",
        "ied_em_nivel_2",
        "ied_em_nivel_3",
        "ied_em_nivel_4",
        "ied_em_nivel_5",
        "ied_em_nivel_6",
        "icg_nivel_1",
        "icg_nivel_2",
        "icg_nivel_3",
        "icg_nivel_4",
        "icg_nivel_5",
        "icg_nivel_6",
    ]

    missing = [c for c in col_order if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    df = df[col_order]

    partition_output = os.path.join(output_dir, tabela, f"ano={ano}")
    os.makedirs(partition_output, exist_ok=True)
    df_out = df.drop(columns=["ano"])
    csv_path = os.path.join(partition_output, "brasil.csv")
    df_out.to_csv(csv_path, index=False)

    print(f"Rows written: {len(df_out)}")
    print(f"Output: {csv_path}")
    print(
        f"Unique localizacao: {sorted(df_out['localizacao'].unique().tolist())}"
    )
    print(f"Unique rede:        {sorted(df_out['rede'].unique().tolist())}")


# ! ================================================ UF =================================================


def uf(ano: int, tabela: str) -> None:
    input_dir = _input_dir(ano)
    output_dir = _output_dir()
    bd_dir = bd.read_sql(
        "SELECT nome, sigla FROM `basedosdados.br_bd_diretorios_brasil.uf`",
        billing_project_id="basedosdados-dev",
    )
    estados = set(bd_dir["nome"].tolist())

    afd = pd.read_excel(
        os.path.join(
            input_dir,
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
            input_dir,
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
            input_dir,
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
            input_dir,
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
            input_dir,
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
            input_dir,
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
            input_dir,
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
            input_dir,
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
            input_dir,
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
            input_dir,
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
        lambda left, right: pd.merge(  # noqa: PD015
            left, right, on=keys_col_merge, how="outer"
        ),
        escola,
    )

    df = df.rename(columns={"UNIDGEO": "uf"})

    df = df.replace("--", None)
    df = df.merge(
        bd_dir[["nome", "sigla"]], left_on="uf", right_on="nome"
    ).drop(columns=["uf", "nome"])
    df = df.rename(columns={"sigla": "sigla_uf"})

    for year in df["ano"].unique():
        for uf in df["sigla_uf"].unique():
            output_path = os.path.join(
                output_dir, tabela, f"ano={year}", f"sigla_uf={uf}"
            )
            os.makedirs(output_path, exist_ok=True)
            df_year = df[(df["ano"] == year) & (df["sigla_uf"] == uf)].drop(
                columns=["ano", "sigla_uf"]
            )

            df_year.to_csv(os.path.join(output_path, "uf.csv"), index=False)


# ! ================================================ Região =================================================


def regiao(ano: int, tabela: str) -> None:
    input_dir = _input_dir(ano)
    output_dir = _output_dir()
    regioes = ["Norte", "Nordeste", "Sudeste", "Sul", "Centro-Oeste"]

    afd = pd.read_excel(
        os.path.join(
            input_dir,
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
            input_dir,
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
            input_dir,
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
            input_dir,
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
            input_dir,
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
            input_dir,
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
            input_dir,
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
            input_dir,
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
            input_dir,
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
            input_dir,
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
        lambda left, right: pd.merge(  # noqa: PD015
            left, right, on=keys_col_merge, how="outer"
        ),
        escola,
    )

    df = df.replace("--", None)
    df = df.rename(columns={"UNIDGEO": "regiao"})
    for year in df["ano"].unique():
        output_path = os.path.join(output_dir, tabela, f"ano={year}")
        os.makedirs(output_path, exist_ok=True)
        df_year = df[(df["ano"] == year)].drop(columns=["ano"])
        df_year.to_csv(os.path.join(output_path, "uf.csv"), index=False)


if __name__ == "__main__":
    brasil(ano=2025, tabela="brasil")
