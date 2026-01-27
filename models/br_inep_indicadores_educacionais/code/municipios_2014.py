"""
Script para atualizar e corrigir a tabela br_inep_indicadores_educacionais.municipio de 2014 até 2021.
"""

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
    rename_tdi,
    rename_tnr,
    rename_tx,
)

ano = 2014

URLS_MUNICIPIOS = [
    f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/AFD_{ano}_MUNICIPIOS.zip"
    if ano > 2015
    else f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/adequacao_formacao_docente/adequacao_formacao_docente_municipios_{ano}.zip",
    f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/ATU_{ano}_MUNICIPIOS.zip"
    if ano > 2015
    else f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/alunos_turma/media_alunos_turma_municipios_{ano}.zip",
    f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/DSU_{ano}_MUNICIPIOS.zip"
    if ano > 2015
    else f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/proporcao_docentes_formacao_superior/dsu_municipios_{ano}.zip",
    f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/HAD_{ano}_MUNICIPIOS.zip"
    if ano > 2015
    else f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/horas_aula_diaria/horas_aula_municipios_{ano}.zip",
    f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/ICG_{ano}_MUNICIPIOS.zip"
    if ano > 2015
    else f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/complexidade_gestao_escola/complexidade_gestao_escola_municipios_{ano}.zip",
    f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/IED_{ano}_MUNICIPIOS.zip"
    if ano > 2015
    else f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/esforco_docente/esforco_docente_municipios_{ano}.zip",
    f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/IRD_{ano}_MUNICIPIOS.zip"
    if ano > 2015
    else f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/regularidade_corpo_docente/regularidade_corpo_docente_municipios_{ano}.zip",
    f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/TDI_{ano}_MUNICIPIOS.zip"
    if ano > 2015
    else f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/distorcao_idade_serie/tdi_municipios_{ano}.zip",
    # f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/tnr_municipios_{ano}.zip"
    f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/TNR_MUNICIPIOS_{ano}.zip"
    if ano > 2015
    else f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/taxa_nao_resposta/tx_nao_resposta_municipios_{ano}.zip",
    # f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/tx_rend_municipios_{ano}.zip",
]

if ano >= 2019:
    URLS_MUNICIPIOS.append(
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/tx_rend_municipios_{ano}.zip"
    )
elif ano in [2018, 2017, 2016]:
    # URLS_MUNICIPIOS.append(
    #     f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/TAXA_REND_{ano}_MUNICIPIOS.zip"
    # )
    URLS_MUNICIPIOS.append(
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/TX_REND_MUNICIPIOS_{ano}.zip"
    )
else:
    URLS_MUNICIPIOS.append(
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/taxa_rendimento/tx_rendimento_municipios_{ano}.zip"
    )

INPUT = os.path.join(os.getcwd(), "tmp", "municipios", "input")
OUTPUT = os.path.join(os.getcwd(), "tmp", "municipios", "output")

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

for url in URLS_MUNICIPIOS:
    response = requests.get(
        url, headers={"User-Agent": "Mozilla/5.0"}, verify=False
    )
    content_type = response.headers["Content-Type"]
    if content_type != "application/zip":
        raise Exception(f"Content type of {url} is not zip file")
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
    os.path.join(INPUT, f"AFD_MUNICIPIOS_{ano}.xlsx"),
    skiprows=10,
)

renames_afd = {
    "ano": "ano",
    "pk_cod_municipio": "id_municipio",
    "TIPOLOCA": "localizacao",
    "Dependad": "rede",
    "AFD_INF1": "afd_ei_grupo_1",
    "AFD_INF2": "afd_ei_grupo_2",
    "AFD_INF3": "afd_ei_grupo_3",
    "AFD_INF4": "afd_ei_grupo_4",
    "AFD_INF5": "afd_ei_grupo_5",
    "AFD_FUN1": "afd_ef_grupo_1",
    "AFD_FUN2": "afd_ef_grupo_2",
    "AFD_FUN3": "afd_ef_grupo_3",
    "AFD_FUN4": "afd_ef_grupo_4",
    "AFD_FUN5": "afd_ef_grupo_5",
    "AFD_F141": "afd_ef_anos_iniciais_grupo_1",
    "AFD_F142": "afd_ef_anos_iniciais_grupo_2",
    "AFD_F143": "afd_ef_anos_iniciais_grupo_3",
    "AFD_F144": "afd_ef_anos_iniciais_grupo_4",
    "AFD_F145": "afd_ef_anos_iniciais_grupo_5",
    "AFD_F581": "afd_ef_anos_finais_grupo_1",
    "AFD_F582": "afd_ef_anos_finais_grupo_2",
    "AFD_F583": "afd_ef_anos_finais_grupo_3",
    "AFD_F584": "afd_ef_anos_finais_grupo_4",
    "AFD_F585": "afd_ef_anos_finais_grupo_5",
    "AFD_MED1": "afd_em_grupo_1",
    "AFD_MED2": "afd_em_grupo_2",
    "AFD_MED3": "afd_em_grupo_3",
    "AFD_MED4": "afd_em_grupo_4",
    "AFD_MED5": "afd_em_grupo_5",
    "AFD_EJAF1": "afd_eja_fundamental_grupo_1",
    "AFD_EJAF2": "afd_eja_fundamental_grupo_2",
    "AFD_EJAF3": "afd_eja_fundamental_grupo_3",
    "AFD_EJAF4": "afd_eja_fundamental_grupo_4",
    "AFD_EJAF5": "afd_eja_fundamental_grupo_5",
    "AFD_EJAM1": "afd_eja_medio_grupo_1",
    "AFD_EJAM2": "afd_eja_medio_grupo_2",
    "AFD_EJAM3": "afd_eja_medio_grupo_3",
    "AFD_EJAM4": "afd_eja_medio_grupo_4",
    "AFD_EJAM5": "afd_eja_medio_grupo_5",
}

renames_afd = {**rename_afd, **{"CO_MUNICIPIO": "id_municipio"}}

afd = afd.rename(columns=renames_afd, errors="raise")[renames_afd.values()]
afd: pd.DataFrame = afd.loc[afd["ano"] == ano,]  # type: ignore

atu = pd.read_excel(
    os.path.join(INPUT, "MEDIA ALUNOS TURMA MUNICIPIOS 2014.xlsx"),
    skiprows=9,
)

renames_atu = {
    "ano": "ano",
    "PK_COD_MUNICIPIO": "id_municipio",
    "TIPOLOCA": "localizacao",
    "Dependad": "rede",
    "ATU_INF": "atu_ei",
    "ATU_CRE": "atu_ei_creche",
    "ATU_PRE": "atu_ei_pre_escola",
    "ATU_FUN": "atu_ef",
    "ATU_F14": "atu_ef_anos_iniciais",
    "ATU_F58": "atu_ef_anos_finais",
    "ATU_F00": "atu_ef_1_ano",
    "ATU_F01": "atu_ef_2_ano",
    "ATU_F02": "atu_ef_3_ano",
    "ATU_F03": "atu_ef_4_ano",
    "ATU_F04": "atu_ef_5_ano",
    "ATU_F05": "atu_ef_6_ano",
    "ATU_F06": "atu_ef_7_ano",
    "ATU_F07": "atu_ef_8_ano",
    "ATU_F08": "atu_ef_9_ano",
    "ATU_UNIFICADA": "atu_ef_turmas_unif_multi_fluxo",
    "ATU_MED": "atu_em",
    "ATU_M01": "atu_em_1_ano",
    "ATU_M02": "atu_em_2_ano",
    "ATU_M03": "atu_em_3_ano",
    "ATU_M04": "atu_em_4_ano",
    "ATU_MNS": "atu_em_nao_seriado",
}

renames_atu = {**rename_atu, "CO_MUNICIPIO": "id_municipio"}

atu = atu.rename(columns=renames_atu, errors="raise")[renames_atu.values()]

atu: pd.DataFrame = atu.loc[atu["ano"] == ano,]  # type: ignore

dsu = pd.read_excel(
    os.path.join(INPUT, "DSU - MUNICIPIOS 2014.xlsx"),
    skiprows=9,
)

renames_dsu = {
    "ano": "ano",
    "PK_COD_MUNICIPIO": "id_municipio",
    "TIPOLOCA": "localizacao",
    "Dependad": "rede",
    "DSU_INF": "dsu_ei",
    "DSU_CRE": "dsu_ei_creche",
    "DSU_PRE": "dsu_ei_pre_escola",
    "DSU_FUN": "dsu_ef",
    "DSU_F14": "dsu_ef_anos_iniciais",
    "DSU_F58": "dsu_ef_anos_finais",
    "DSU_MED": "dsu_em",
    "DSU_PROF": "dsu_ep",
    "DSU_EJA": "dsu_eja",
    "DSU_ESP": "dsu_ee",
}

renames_dsu = {
    "NU_ANO_CENSO": "ano",
    "CO_MUNICIPIO": "id_municipio",
    "NO_CATEGORIA": "localizacao",
    "NO_DEPENDENCIA": "rede",
    "ED_INF_CAT_0": "dsu_ei",
    "CRE_CAT_0": "dsu_ei_creche",
    "PRE_CAT_0": "dsu_ei_pre_escola",
    "FUN_CAT_0": "dsu_ef",
    "FUN_AI_CAT_0": "dsu_ef_anos_iniciais",
    "FUN_AF_CAT_0": "dsu_ef_anos_finais",
    "MED_CAT_0": "dsu_em",
    "PROF_CAT_0": "dsu_ep",
    "EJA_CAT_0": "dsu_eja",
    "EDU_BAS_CAT_0": "dsu_ee",
}

renames_dsu = {**rename_dsu, "CO_MUNICIPIO": "id_municipio"}

dsu = dsu.rename(columns=renames_dsu, errors="raise")[renames_dsu.values()]

dsu: pd.DataFrame = dsu.loc[dsu["ano"] == ano,]  # type: ignore

had = pd.read_excel(
    os.path.join(INPUT, "MÉDIA HORAS-AULA MUNIC╓PIOS  2014.xlsX"),
    skiprows=8,
)

renames_had = {
    "Ano": "ano",
    "PK_COD_MUNICIPIO": "id_municipio",
    "TIPOLOCA": "localizacao",
    "Dependad": "rede",
    "HAD_INF": "had_ei",
    "HAD_CRE": "had_ei_creche",
    "HAD_PRE": "had_ei_pre_escola",
    "HAD_FUN": "had_ef",
    "HAD_F14": "had_ef_anos_iniciais",
    "HAD_F58": "had_ef_anos_finais",
    "HAD_F00": "had_ef_1_ano",
    "HAD_F01": "had_ef_2_ano",
    "HAD_F02": "had_ef_3_ano",
    "HAD_F03": "had_ef_4_ano",
    "HAD_F04": "had_ef_5_ano",
    "HAD_F05": "had_ef_6_ano",
    "HAD_F06": "had_ef_7_ano",
    "HAD_F07": "had_ef_8_ano",
    "HAD_F08": "had_ef_9_ano",
    "HAD_MED": "had_em",
    "HAD_M01": "had_em_1_ano",
    "HAD_M02": "had_em_2_ano",
    "HAD_M03": "had_em_3_ano",
    "HAD_M04": "had_em_4_ano",
    "HAD_MNS": "had_em_nao_seriado",
}

renames_had = {**rename_had, "CO_MUNICIPIO": "id_municipio"}

had = had.rename(columns=renames_had, errors="raise")[renames_had.values()]

had: pd.DataFrame = had.loc[had["ano"] == ano,]  # type: ignore

icg = pd.read_excel(
    os.path.join(INPUT, "ICG_MUNICIPIOS_2014.xlsx"),
    skiprows=8,
)


renames_icg = {
    "Ano": "ano",
    "PK_COD_MUNICIPIO": "id_municipio",
    "Dependad": "rede",
    "TIPOLOCA": "localizacao",
    "ICG_1": "icg_nivel_1",
    "ICG_2": "icg_nivel_2",
    "ICG_3": "icg_nivel_3",
    "ICG_4": "icg_nivel_4",
    "ICG_5": "icg_nivel_5",
    "ICG_6": "icg_nivel_6",
}

renames_icg = {**rename_icg, "CO_MUNICIPIO": "id_municipio"}

icg = icg.rename(columns=renames_icg, errors="raise")[renames_icg.values()]

icg: pd.DataFrame = icg.loc[icg["ano"] == ano,]  # type: ignore

ied = pd.read_excel(
    os.path.join(INPUT, "IED_MUNICIPIOS_2014.xlsx"),
    skiprows=10,
)

renames_ied = {
    "ano": "ano",
    "PK_COD_MUNICIPIO": "id_municipio",
    "TIPOLOCA": "localizacao",
    "Dependad": "rede",
    "IED_FUN1": "ied_ef_nivel_1",
    "IED_FUN2": "ied_ef_nivel_2",
    "IED_FUN3": "ied_ef_nivel_3",
    "IED_FUN4": "ied_ef_nivel_4",
    "IED_FUN5": "ied_ef_nivel_5",
    "IED_FUN6": "ied_ef_nivel_6",
    "IED_F141": "ied_ef_anos_iniciais_nivel_1",
    "IED_F142": "ied_ef_anos_iniciais_nivel_2",
    "IED_F143": "ied_ef_anos_iniciais_nivel_3",
    "IED_F144": "ied_ef_anos_iniciais_nivel_4",
    "IED_F145": "ied_ef_anos_iniciais_nivel_5",
    "IED_F146": "ied_ef_anos_iniciais_nivel_6",
    "IED_F581": "ied_ef_anos_finais_nivel_1",
    "IED_F582": "ied_ef_anos_finais_nivel_2",
    "IED_F583": "ied_ef_anos_finais_nivel_3",
    "IED_F584": "ied_ef_anos_finais_nivel_4",
    "IED_F585": "ied_ef_anos_finais_nivel_5",
    "IED_F586": "ied_ef_anos_finais_nivel_6",
    "IED_MED1": "ied_em_nivel_1",
    "IED_MED2": "ied_em_nivel_2",
    "IED_MED3": "ied_em_nivel_3",
    "IED_MED4": "ied_em_nivel_4",
    "IED_MED5": "ied_em_nivel_5",
    "IED_MED6": "ied_em_nivel_6",
}

renames_ied = {**rename_ied, "CO_MUNICIPIO": "id_municipio"}

ied = ied.rename(columns=renames_ied, errors="raise")[renames_ied.values()]

ied: pd.DataFrame = ied.loc[ied["ano"] == ano,]  # type: ignore

ird = pd.read_excel(
    os.path.join(INPUT, "IRD_MUNICIPIOS_2014.xlsx"),
    skiprows=8,
)

renames_ird = {
    "Unnamed: 0": "ano",
    "Unnamed: 3": "id_municipio",
    "Unnamed: 5": "localizacao",
    "Unnamed: 6": "rede",
    "Baixa regularidade \n(0-|2) ": "ird_baixa_regularidade",
    "Média-baixa \n(2-|3)": "ird_media_baixa",
    "Média-alta \n(3-|4)": "ird_media_alta",
    "Alta\n (4-|5)": "ird_alta",
}

ird = ird.rename(columns=renames_ird, errors="raise")[renames_ird.values()]


ird: pd.DataFrame = ird.loc[ird["ano"] == ano,]  # type: ignore

tdi = pd.read_excel(
    os.path.join(INPUT, "TDI_MUNICIPIOS_2014.xlsx"),
    skiprows=8,
)


renames_tdi = {
    "ano": "ano",
    "PK_COD_MUNICIPIO": "id_municipio",
    "TIPOLOCA": "localizacao",
    "Dependad": "rede",
    "TDI_FUN": "tdi_ef",
    "TDI_F14": "tdi_ef_anos_iniciais",
    "TDI_F58": "tdi_ef_anos_finais",
    "TDI_F00": "tdi_ef_1_ano",
    "TDI_F01": "tdi_ef_2_ano",
    "TDI_F02": "tdi_ef_3_ano",
    "TDI_F03": "tdi_ef_4_ano",
    "TDI_F04": "tdi_ef_5_ano",
    "TDI_F05": "tdi_ef_6_ano",
    "TDI_F06": "tdi_ef_7_ano",
    "TDI_F07": "tdi_ef_8_ano",
    "TDI_F08": "tdi_ef_9_ano",
    "TDI_MED": "tdi_em",
    "TDI_M01": "tdi_em_1_ano",
    "TDI_M02": "tdi_em_2_ano",
    "TDI_M03": "tdi_em_3_ano",
    "TDI_M04": "tdi_em_4_ano",
}

renames_tdi = {**rename_tdi, "CO_MUNICIPIO": "id_municipio"}

tdi = tdi.rename(columns=renames_tdi, errors="raise")[renames_tdi.values()]
tdi: pd.DataFrame = tdi.loc[tdi["ano"] == ano,]  # type: ignore

tnr = pd.read_excel(
    os.path.join(INPUT, "TAXA N╟O-RESPOSTA MUNICIPIOS  2014.xlsx"),
    skiprows=7,
)

renames_tnr = {
    "Unnamed: 0": "ano",
    "Unnamed: 3": "id_municipio",
    "Unnamed: 5": "localizacao",
    "Unnamed: 6": "rede",
    "Total Ens. Fundamental": "tnr_ef",
    "Anos Iniciais (1º ao 5º Ano)": "tnr_ef_anos_iniciais",
    "Anos Finais (6º ao 9º Ano)": "tnr_ef_anos_finais",
    "1º Ano ": "tnr_ef_1_ano",
    "2º Ano": "tnr_ef_2_ano",
    "3º Ano": "tnr_ef_3_ano",
    "4º Ano": "tnr_ef_4_ano",
    " 5º Ano": "tnr_ef_5_ano",
    "6º Ano": "tnr_ef_6_ano",
    "7º Ano": "tnr_ef_7_ano",
    "8º Ano": "tnr_ef_8_ano",
    "9º Ano": "tnr_ef_9_ano",
    "Total  Médio": "tnr_em",
    "1ª série": "tnr_em_1_ano",
    "2ª série": "tnr_em_2_ano",
    "3ª série": "tnr_em_3_ano",
    "4ª série": "tnr_em_4_ano",
    "Total Médio Não-Seriado": "tnr_em_nao_seriado",
}

renames_tnr = {
    "ano": "ano",
    "CO_MUNICIPIO": "id_municipio",
    "TIPOLOCA": "localizacao",
    "DEPENDAD": "rede",
    "TNR_FUN": "tnr_ef",
    "TNR_F14": "tnr_ef_anos_iniciais",
    "TNR_F58": "tnr_ef_anos_finais",
    "TNR_F00": "tnr_ef_1_ano",
    "TNR_F01": "tnr_ef_2_ano",
    "TNR_F02": "tnr_ef_3_ano",
    "TNR_F03": "tnr_ef_4_ano",
    "TNR_F04": "tnr_ef_5_ano",
    "TNR_F05": "tnr_ef_6_ano",
    "TNR_F06": "tnr_ef_7_ano",
    "TNR_F07": "tnr_ef_8_ano",
    "TNR_F08": "tnr_ef_9_ano",
    "TNR_MED": "tnr_em",
    "TNR_M01": "tnr_em_1_ano",
    "TNR_M02": "tnr_em_2_ano",
    "TNR_M03": "tnr_em_3_ano",
    "TNR_M04": "tnr_em_4_ano",
    "TNR_MNS": "tnr_em_nao_seriado",
}

renames_tnr = {**rename_tnr, "CO_MUNICIPIO": "id_municipio"}

tnr = tnr.rename(columns=renames_tnr, errors="raise")[renames_tnr.values()]

tnr: pd.DataFrame = tnr.loc[tnr["ano"] == ano,]  # type: ignore

tx = pd.read_excel(
    os.path.join(
        INPUT,
        "TAXAS RENDIMENTOS MUNICIPIOS 2014.xlsx",
    ),
    skiprows=7,
)

renames_tx = {
    "Unnamed: 0": "ano",
    "Unnamed: 3": "id_municipio",
    "Unnamed: 5": "localizacao",
    "Unnamed: 6": "rede",
    "Total Aprovação no Ens. Fundamental": "taxa_aprovacao_ef",
    # "Total": "taxa_aprovacao_ef",
    "Aprovação - Anos Iniciais (1º ao 5º Ano)": "taxa_aprovacao_ef_anos_iniciais",
    # "Anos Iniciais": "taxa_aprovacao_ef_anos_iniciais",
    "Aprovação - Anos Finais (6º ao 9º Ano)": "taxa_aprovacao_ef_anos_finais",
    # "Anos Finais": "taxa_aprovacao_ef_anos_finais",
    "Aprovação no 1º Ano ": "taxa_aprovacao_ef_1_ano",
    "Aprovação no 2º Ano": "taxa_aprovacao_ef_2_ano",
    "Aprovação no 3º Ano": "taxa_aprovacao_ef_3_ano",
    "Aprovação no 4º Ano": "taxa_aprovacao_ef_4_ano",
    "Aprovação no 5º Ano": "taxa_aprovacao_ef_5_ano",
    "Aprovação no 6º Ano": "taxa_aprovacao_ef_6_ano",
    "Aprovação no 7º Ano": "taxa_aprovacao_ef_7_ano",
    "Aprovação no 8º Ano": "taxa_aprovacao_ef_8_ano",
    "Aprovação no 9º Ano": "taxa_aprovacao_ef_9_ano",
    "Total Aprovação no Ens. Médio": "taxa_aprovacao_em",
    # "Total  ": "taxa_aprovacao_em",
    "Aprovação na 1ª série": "taxa_aprovacao_em_1_ano",
    "Aprovação na 2ª série": "taxa_aprovacao_em_2_ano",
    "Aprovação na 3ª série": "taxa_aprovacao_em_3_ano",
    "Aprovação na 4ª série": "taxa_aprovacao_em_4_ano",
    "Total  Aprovação Médio Não-Seriado": "taxa_aprovacao_em_nao_seriado",
    "Total Reprovação no Ens. Fundamental": "taxa_reprovacao_ef",
    "Reprovação - Anos Iniciais (1º ao 5º Ano)": "taxa_reprovacao_ef_anos_iniciais",
    "Reprovação - Anos Finais (6º ao 9º Ano)": "taxa_reprovacao_ef_anos_finais",
    "Reprovação no 1º Ano ": "taxa_reprovacao_ef_1_ano",
    "Reprovação no 2º Ano": "taxa_reprovacao_ef_2_ano",
    "Reprovação no 3º Ano": "taxa_reprovacao_ef_3_ano",
    "Reprovação no 4º Ano": "taxa_reprovacao_ef_4_ano",
    "Reprovação no 5º Ano": "taxa_reprovacao_ef_5_ano",
    "Reprovação no 6º Ano": "taxa_reprovacao_ef_6_ano",
    "Reprovação no 7º Ano": "taxa_reprovacao_ef_7_ano",
    "Reprovação no 8º Ano": "taxa_reprovacao_ef_8_ano",
    "Reprovação no 9º Ano": "taxa_reprovacao_ef_9_ano",
    "Total Reprovação no Ens. Médio": "taxa_reprovacao_em",
    "Reprovação na 1ª série": "taxa_reprovacao_em_1_ano",
    "Reprovação na 2ª série": "taxa_reprovacao_em_2_ano",
    "Reprovação na 3ª série": "taxa_reprovacao_em_3_ano",
    "Reprovação na 4ª série": "taxa_reprovacao_em_4_ano",
    "Total  Reprovação Médio Não-Seriado": "taxa_reprovacao_em_nao_seriado",
    "Total Abandono Ens. Fundamental": "taxa_abandono_ef",
    "Abandono - Anos Iniciais (1º ao 5º Ano)": "taxa_abandono_ef_anos_iniciais",
    "Abandono - Anos Finais (6º ao 9º Ano)": "taxa_abandono_ef_anos_finais",
    "Abandono no 1º Ano ": "taxa_abandono_ef_1_ano",
    "Abandono no 2º Ano": "taxa_abandono_ef_2_ano",
    "Abandono no 3º Ano": "taxa_abandono_ef_3_ano",
    "Abandono no 4º Ano": "taxa_abandono_ef_4_ano",
    "Abandono no 5º Ano": "taxa_abandono_ef_5_ano",
    "Abandono no 6º Ano": "taxa_abandono_ef_6_ano",
    "Abandono no 7º Ano": "taxa_abandono_ef_7_ano",
    "Abandono no 8º Ano": "taxa_abandono_ef_8_ano",
    "Abandono no 9º Ano": "taxa_abandono_ef_9_ano",
    "Total Abandono no Ens. Médio": "taxa_abandono_em",
    "Abandono na 1ª série": "taxa_abandono_em_1_ano",
    "Abandono na 2ª série": "taxa_abandono_em_2_ano",
    "Abandono na 3ª série": "taxa_abandono_em_3_ano",
    "Abandono na 4ª série": "taxa_abandono_em_4_ano",
    "Total  Abandono Médio Não-Seriado": "taxa_abandono_em_nao_seriado",
}

renames_tx = {**rename_tx, "CO_MUNICIPIO": "id_municipio"}

tx = tx.rename(columns=renames_tx, errors="raise")[renames_tx.values()]

tx = tx.loc[tx["ano"] == ano]

keys_col_merge = ["ano", "id_municipio", "localizacao", "rede"]

escola = [afd, atu, dsu, had, icg, ied, ird, tdi, tx, tnr]

for columns in escola:
    columns["rede"] = (
        columns["rede"]
        .str.lower()
        .replace(
            {
                "pública": "publica",
                "publico": "publica",
                "particular": "privada",
            }
        )
    )
    columns["localizacao"] = (
        columns["localizacao"].str.lower().replace({"pública": "publica"})
    )
    columns["id_municipio"] = (
        columns["id_municipio"].astype("Int64").astype("str")
    )

df: pd.DataFrame = reduce(
    lambda left, right: pd.merge(  # noqa: PD015
        left, right, on=keys_col_merge, how="outer"
    ),
    escola,
)

df.rede.unique()
df.localizacao.unique()


df = df.apply(lambda x: x.replace("--", None))

df = df.drop(columns="ano")

columns_order = bd.read_sql(
    "select * FROM `basedosdados-dev.br_inep_indicadores_educacionais.municipio` limit 0",
    billing_project_id="basedosdados-dev",
).columns.drop("ano")

escola_output_path = os.path.join(OUTPUT, f"ano={ano}")
os.makedirs(escola_output_path, exist_ok=True)
df[columns_order].to_csv(
    os.path.join(escola_output_path, "municipio.csv"), index=False
)

tb = bd.Table("br_inep_indicadores_educacionais", "municipio")

for folder in os.listdir(OUTPUT):
    pd.read_csv(f"{OUTPUT}/{folder}/municipio.csv")[columns_order].to_csv(
        f"{OUTPUT}/{folder}/municipio.csv", index=False
    )

tb.create(OUTPUT, if_table_exists="replace", if_storage_data_exists="replace")
