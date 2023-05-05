# -*- coding: utf-8 -*-

"""
Tasks for br_me_comex_stat
"""
# pylint: disable=invalid-name,too-many-nested-blocks
from glob import glob
from zipfile import ZipFile

import pandas as pd
import numpy as np
from prefect import task

from pipelines.datasets.br_me_comex_stat.utils import create_paths, download_data
from pipelines.datasets.br_me_comex_stat.constants import constants
from pipelines.utils.utils import (
    log,
    to_partitions,
)


@task
# todo: insert nmax retry params
def clean_br_me_comex_stat():
    """
    clean and partition table
    """

    create_paths(
        constants.PATH.value,
    )

    log("paths created!")

    download_data(constants.PATH.value)
    log("data downloaded!")

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

    list_zip = glob(constants.PATH.value + "input/" + "*.zip")
    log(f"The files {list_zip} are in the given folder")

    for filezip in list_zip:

        with ZipFile(filezip) as z:

            with z.open(filezip.split("/")[-1][:-4] + ".csv") as f:
                df = pd.read_csv(f, sep=";")

                if "MUN" in filezip:
                    log(f"cleaning {filezip} file")

                    df.rename(columns=rename_mun, inplace=True)
                    # ! just for testing purposes
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

                    to_partitions(
                        data=df,
                        partition_columns=["ano", "mes"],
                        savepath=constants.PATH.value + "output",
                    )

                else:

                    log(f"cleaning {filezip} file")
                    df.rename(columns=rename_ncm, inplace=True)
                    log("df renamed")

                    to_partitions(
                        data=df,
                        partition_columns=["ano", "mes"],
                        savepath=constants.PATH.value + "output",
                    )

                del df

    return "/tmp/br_me_comex_stat/output/"
