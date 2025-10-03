"""
Tasks for br_sp_saopaulo_dieese_icv
"""

import os

import ipeadatapy as idpy
from prefect import task


@task
def clean_dieese_icv():
    """
    Download and process 'Dieese' data

    """
    os.makedirs("/tmp/data/br_sp_saopaulo_dieese_icv", exist_ok=True)

    codes = ["DIEESE12_ICVSPD12", "DIEESE12_ICVSPDG12"]
    drop_m = ["DATE", "DAY", "CODE", "RAW DATE"]

    rename_m = {
        "YEAR": "ano",
        "MONTH": "mes",
        "VALUE (-)": "indice",
        "VALUE ((% a.m.))": "variacao_mensal",
    }

    indice = idpy.timeseries(codes[0]).reset_index()
    indice = indice.drop(drop_m, axis=1)
    indice = indice.rename(columns=rename_m)

    variacao_mensal = idpy.timeseries(codes[1]).reset_index()
    variacao_mensal = variacao_mensal.drop(drop_m, axis=1)
    variacao_mensal = variacao_mensal.rename(columns=rename_m)

    icv_mes = indice.merge(
        variacao_mensal,
        how="left",
        left_on=["ano", "mes"],
        right_on=["ano", "mes"],
    )

    filepath = "/tmp/data/br_sp_saopaulo_dieese_icv/mes.csv"
    icv_mes.to_csv(filepath, index=False)

    return filepath
