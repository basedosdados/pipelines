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
from datetime import datetime

import dask.dataframe as dd


@task
def clean_br_me_comex_stat(table_name):

    """
    clean and partition table
    """
    current_year = datetime.now().year + 1

    filename = "IMP_COMPLETA_MUN"

    create_paths(table_name, constants.PATH.value, constants.UFS.value, current_year)
    download_data(constants.PATH.value, filename, "mun")

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

    file_name = constants.PATH.value + "input/" + filename + ".csv"

    df = dd.read_csv(file_name, sep=";", blocksize="16MB")

    if "MUN" in file_name:

        print("ENTROU NO MUN")
        print(df.shape)

        df = df.rename(columns=rename_mun)

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

        df["id_municipio"] = np.select(condicao, valores, default=df["id_municipio"])

        for table in constants.TABLE.value:

            for ano in [*range(1997, current_year)]:

                for mes in [*range(1, 13)]:

                    for uf in constants.UFS.value:

                        if "municipio" in table_name:

                            print(f"Particionando {table_name}_{ano}_{mes}_{uf}")

                            df_partition = df[df["ano"] == ano].copy()

                            df_partition = df_partition[df_partition["mes"] == mes]
                            df_partition = df_partition[df_partition["sigla_uf"] == uf]

                            df_partition = df_partition.drop(
                                ["ano", "mes", "sigla_uf"], axis=1
                            )

                            print("Iniciando save")
                            df_partition = df_partition.compute()

                            df_partition.to_csv(
                                constants.PATH.value
                                + "output/"
                                + f"{table_name}/ano={ano}/mes={mes}/sigla_uf={uf}/{table_name}.csv",
                                index=False,
                                encoding="utf-8",
                                na_rep="",
                                # single_file=True
                            )

                            del df_partition

    else:

        df.rename(columns=rename_ncm, inplace=True)

        for ano in [*range(1997, current_year)]:
            for mes in [*range(1, 13)]:

                if "ncm" in table_name:
                    print(f"Particionando {table_name}_{ano}_{mes}")
                    df_partition = df[df["ano"] == ano].copy()

                    df_partition = df_partition[df_partition["mes"] == mes]

                    df_partition.drop(["ano", "mes"], axis=1, inplace=True)

                    df_partition.to_csv(
                        constants.PATH.value
                        + "output/"
                        + f"{table_name}/ano={ano}/mes={mes}/{table_name}.csv",
                        index=False,
                        encoding="utf-8",
                        na_rep="",
                    )

                    del df_partition
    del df

    return "/tmp/br_me_comex_stat/output/"


clean_br_me_comex_stat("municipio_importacao")

## TODO:
## 1. Achar uma maneira de alinhar o select numpy com dask (linhas 71-87)
## 2. Ajustar flows com nome da tabela.
## 3. Ver uma maneira de passar o group e item para funcao de download
## 4. Excluir arquivo CSV ao final da funcao
