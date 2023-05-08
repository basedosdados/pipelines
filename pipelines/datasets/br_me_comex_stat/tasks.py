# -*- coding: utf-8 -*-

"""
Tasks for br_me_comex_stat
"""
# pylint: disable=invalid-name,too-many-nested-blocks
from glob import glob
from zipfile import ZipFile
import time as tm
from datetime import timedelta
import pandas as pd
import numpy as np
from prefect import task

from pipelines.datasets.br_me_comex_stat.utils import create_paths, download_data
from pipelines.datasets.br_me_comex_stat.constants import constants as comex_constants

from pipelines.constants import constants

from pipelines.utils.utils import (
    log,
    to_partitions,
)


@task
def download_br_me_comex_stat(
    table_type: str,
    table_name: str,
) -> None:
    create_paths(
        path=comex_constants.PATH.value,
        table_name=table_name,
    )

    log("paths created!")

    download_data(
        path=comex_constants.PATH.value,
        table_type=table_type,
        table_name=table_name,
    )
    log("data downloaded!")

    tm.sleep(10)


@task
def clean_br_me_comex_stat(
    table_name: str,
) -> pd.DataFrame:
    """
    clean and partition table
    """

    rename_ncm = {
        "CO_ANO": "ano",
        "CO_MES": "mes",
        "CO_NCM": "id_ncm",
        "CO_UNID": "id_unidade",
        "CO_PAIS": "id_pais",
        "SG_UF_NCM": "sigla_uf_ncm",
        "CO_VIA": "id_via",
        "CO_URF": "id_urf",
        "QT_ESTAT": "quantidade_estatistica",
        "KG_LIQUIDO": "peso_liquido_kg",
        "VL_FOB": "valor_fob_dolar",
        "VL_FRETE": "valor_frete",
        "VL_SEGURO": "valor_seguro",
    }

    rename_mun = {
        "CO_ANO": "ano",
        "CO_MES": "mes",
        "SH4": "id_sh4",
        "CO_PAIS": "id_pais",
        "SG_UF_MUN": "sigla_uf",
        "CO_MUN": "id_municipio",
        "KG_LIQUIDO": "peso_liquido_kg",
        "VL_FOB": "valor_fob_dolar",
    }

    filezip = glob(comex_constants.PATH.value + table_name + "/input/" + "*.zip")

    filezip = filezip[0]

    with ZipFile(filezip) as z:

        with z.open(filezip.split("/")[-1][:-4] + ".csv") as f:
            df = pd.read_csv(f, sep=";")

            if "MUN" in filezip:
                log(f"cleaning {filezip} file")

                df.rename(columns=rename_mun, inplace=True)

                log("df renamed")

                condicao = [
                    ((df["sigla_uf"] == "SP") & (df["id_municipio"] < 3500000)),
                    ((df["sigla_uf"] == "MS") & (df["id_municipio"] > 5000000)),
                    ((df["sigla_uf"] == "GO") & (df["id_municipio"] > 5200000)),
                    ((df["sigla_uf"] == "DF") & (df["id_municipio"] > 5300000)),
                ]

                valores = [
                    df["id_municipio"] + 100000,
                    df["id_municipio"] - 200000,
                    df["id_municipio"] - 100000,
                    df["id_municipio"] - 100000,
                ]

                df["id_municipio"] = np.select(
                    condicao, valores, default=df["id_municipio"]
                )
                log("Id_municipio column updated")
                log(df.columns)

                to_partitions(
                    data=df,
                    partition_columns=["ano", "mes", "sigla_uf"],
                    savepath=comex_constants.PATH.value + table_name + "/output/",
                )

            else:

                log(f"cleaning {filezip} file")
                df.rename(columns=rename_ncm, inplace=True)
                log("df renamed")

                to_partitions(
                    data=df,
                    partition_columns=["ano", "mes"],
                    savepath=comex_constants.PATH.value + table_name + "/output/",
                )

            del df

    return f"/tmp/br_me_comex_stat/{table_name}/output"
