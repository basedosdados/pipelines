# -*- coding: utf-8 -*-
"""
Tasks for br_fgv_igp
"""

from prefect import task
import ipeadatapy as idpy
import pandas as pd
from functools import reduce


@task
# IGP-DI anual
def IGP_DI_anual():
    drop = ["DATE", "DAY", "MONTH", "CODE", "RAW DATE"]

    indice_anual = idpy.timeseries("IGP_IGP").reset_index()
    indice_anual.drop(drop, axis=1, inplace=True)
    rename = {"YEAR": "ano", "VALUE (-)": "indice"}
    indice_anual.rename(columns=rename, inplace=True)

    var_anual = idpy.timeseries("IGP_IGPDIG").reset_index()
    var_anual.drop(drop, axis=1, inplace=True)
    rename = {"YEAR": "ano", "VALUE ((% a.a.))": "variacao_anual"}
    var_anual.rename(columns=rename, inplace=True)

    indice_fechamento_anual = idpy.timeseries("IGP_IGPF").reset_index()
    indice_fechamento_anual.drop(drop, axis=1, inplace=True)
    rename = {"YEAR": "ano", "VALUE (-)": "indice_fechamento_anual"}
    indice_fechamento_anual.rename(columns=rename, inplace=True)

    var_fechamento_anual = idpy.timeseries("IGP_IGPF").reset_index()
    var_fechamento_anual.drop(drop, axis=1, inplace=True)
    rename = {"YEAR": "ano", "VALUE (-)": "variacao_fechamento_anual"}
    var_fechamento_anual.rename(columns=rename, inplace=True)

    dfs = [indice_anual, var_anual, indice_fechamento_anual, var_fechamento_anual]

    igpdi_anual = reduce(
        lambda left, right: pd.merge(left, right, on=["ano"], how="outer"), dfs
    )
    igpdi_anual.to_csv(
        "/br_fgv_igp/igpdi_ano.csv", encoding="utf-8", na_rep="", index=False
    )

    return "br_fgv_igp/igpdi_ano.csv"


def IGP_DI_mensal():
    drop = ["DATE", "DAY", "CODE", "RAW DATE"]

    # IGP-DI mensal
    indice = idpy.timeseries("IGP12_IGPDI12").reset_index()
    indice.drop(drop, axis=1, inplace=True)
    rename = {"YEAR": "ano", "MONTH": "mes", "VALUE (-)": "indice"}
    indice.rename(columns=rename, inplace=True)

    var_mensal = idpy.timeseries("IGP12_IGPDIG12").reset_index()
    var_mensal.drop(drop, axis=1, inplace=True)
    rename = {"YEAR": "ano", "MONTH": "mes", "VALUE ((% a.m.))": "variacao_mensal"}
    var_mensal.rename(columns=rename, inplace=True)

    indice_fechamento_mensal = idpy.timeseries("IGP12_IGPF12").reset_index()
    indice_fechamento_mensal.drop(drop, axis=1, inplace=True)
    rename = {"YEAR": "ano", "MONTH": "mes", "VALUE (-)": "indice_fechamento_mensal"}
    indice_fechamento_mensal.rename(columns=rename, inplace=True)

    dfs = [indice, var_mensal, indice_fechamento_mensal]

    igpdi_mensal = reduce(
        lambda left, right: pd.merge(left, right, on=["ano", "mes"], how="outer"), dfs
    )
    igpdi_mensal.to_csv(
        "/br_fgv_igp/igpdi_mes.csv", encoding="utf-8", na_rep="", index=False
    )

    return "br_fgv_igp/igpdi_mes.csv"


def IGP_M_anual():

    # IGP-M anual
    drop = ["DATE", "DAY", "MONTH", "CODE", "RAW DATE"]
    igpm_anual = idpy.timeseries("IGP_IGPMG").reset_index()
    igpm_anual.drop(drop, axis=1, inplace=True)
    rename = {"YEAR": "ano", "VALUE ((% a.a.))": "variacao_anual"}
    igpm_anual.rename(columns=rename, inplace=True)
    igpm_anual.to_csv("/br_fgv_igp/igpm_ano.csv", na_rep="", index=False)

    return "br_fgv_igp/igpm_ano.csv"


def IGP_M_mensal():
    # IGP-M mensal
    drop = ["DATE", "DAY", "CODE", "RAW DATE"]

    indice = idpy.timeseries("IGP12_IGPM12").reset_index()
    indice.drop(drop, axis=1, inplace=True)
    rename = {"YEAR": "ano", "MONTH": "mes", "VALUE (-)": "indice"}
    indice.rename(columns=rename, inplace=True)

    var_mensal = idpy.timeseries("IGP12_IGPMG12").reset_index()
    var_mensal.drop(drop, axis=1, inplace=True)
    rename = {"YEAR": "ano", "MONTH": "mes", "VALUE ((% a.m.))": "variacao_mensal"}
    var_mensal.rename(columns=rename, inplace=True)

    var_primeiro_decendio = idpy.timeseries("IGP12_IGPMG1D12").reset_index()
    var_primeiro_decendio.drop(drop, axis=1, inplace=True)
    rename = {
        "YEAR": "ano",
        "MONTH": "mes",
        "VALUE ((% a.m.))": "variacao_primeiro_decendio",
    }
    var_primeiro_decendio.rename(columns=rename, inplace=True)

    var_segundo_decendio = idpy.timeseries("IGP12_IGPMG2D12").reset_index()
    var_segundo_decendio.drop(drop, axis=1, inplace=True)
    rename = {
        "YEAR": "ano",
        "MONTH": "mes",
        "VALUE ((% a.m.))": "variacao_segundo_decendio",
    }
    var_segundo_decendio.rename(columns=rename, inplace=True)

    dfs = [indice, var_mensal, var_primeiro_decendio, var_segundo_decendio]

    igpm_mensal = reduce(
        lambda left, right: pd.merge(left, right, on=["ano", "mes"], how="outer"), dfs
    )
    igpm_mensal.to_csv(
        "/br_fgv_igp/igpm_mes.csv", encoding="utf-8", na_rep="", index=False
    )

    return "br_fgv_igp/igpm_mes.csv"


def IGP_OG_anual():
    # IGP-M anual
    drop = ["DATE", "DAY", "MONTH", "CODE", "RAW DATE"]
    igpm_anual = idpy.timeseries("IGP_IGPMG").reset_index()
    igpm_anual.drop(drop, axis=1, inplace=True)
    rename = {"YEAR": "ano", "VALUE ((% a.a.))": "variacao_anual"}
    igpm_anual.rename(columns=rename, inplace=True)
    igpm_anual.to_csv("/br_fgv_igp/igpm_ano.csv", na_rep="", index=False)

    return "br_fgv_igp/igpog_ano.csv"


def IGP_OG_mensal():
    # IGP-OG mensal
    drop = ["DATE", "DAY", "CODE", "RAW DATE"]

    indice = idpy.timeseries("IGP12_IGPOG12").reset_index()
    indice.drop(drop, axis=1, inplace=True)
    rename = {"YEAR": "ano", "MONTH": "mes", "VALUE (-)": "indice"}
    indice.rename(columns=rename, inplace=True)

    var_mensal = idpy.timeseries("IGP12_IGPOGG12").reset_index()
    var_mensal.drop(drop, axis=1, inplace=True)
    rename = {"YEAR": "ano", "MONTH": "mes", "VALUE ((% a.m.))": "variacao_mensal"}
    var_mensal.rename(columns=rename, inplace=True)

    dfs = [indice, var_mensal]

    igpog_mensal = reduce(
        lambda left, right: pd.merge(left, right, on=["ano", "mes"], how="outer"), dfs
    )
    igpog_mensal.to_csv(
        "/br_fgv_igp/igpog_mes.csv", encoding="utf-8", na_rep="", index=False
    )

    return "br_fgv_igp/igpog_mes.csv"


def IGP_10_mensal():
    # IGP-10 mensal
    drop = ["DATE", "DAY", "CODE", "RAW DATE"]
    indice = idpy.timeseries("IGP12_IGP1012").reset_index()
    indice.drop(drop, axis=1, inplace=True)
    rename = {"YEAR": "ano", "MONTH": "mes", "VALUE (-)": "indice"}
    indice.rename(columns=rename, inplace=True)
    indice.to_csv("/content/br_fgv_igp/igp10_mes.csv", na_rep="", index=False)

    return "br_fgv_igp/igp10_mes.csv"
