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
from pathlib import Path
from typing import Union, List
import basedosdados as bd


from pipelines.datasets.br_me_comex_stat.utils import create_paths, download_data
from pipelines.datasets.br_me_comex_stat.constants import constants as comex_constants

from pipelines.constants import constants

from pipelines.utils.utils import (
    log,
    to_partitions,
    dump_header_to_csv,
)


@task
def download_br_me_comex_stat(
    table_type: str,
    table_name: str,
) -> ZipFile:
    """This task creates directories to temporary input and output files
    and downloads the data from the source

    Args:
        table_type (str): the table type is either ncm or mun. ncm stands for 'nomenclatura comum do mercosul' and
        mun for 'município'.
        table_name (str): the table name is the original name of the zip file with raw data from comex stat website

    Output:
        ZipFile: the zip file downloaded from comex stat website.
    """

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
    """this task reads a zip file donwload by the upstream task download_br_me_comex_stat and
    unpacks it into a csv file. Then, it cleans the data and returns a pandas dataframe partitioned by year and month
    or by year, month and sigla_uf.

    Args:
        table_name (str): the table name is the original name of the zip file with raw data from comex stat website


    Returns:
        pd.DataFrame: a partitioned standardized pandas dataframe
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

                # for some reason partionate the hole dataset
                # crashed prefect, so I had to do it by year
                for year in df.ano.unique():
                    # filter rows of each year then make partitions

                    df_year = df[df["ano"] == year]
                    log(f"doing partitions year, month and uf for year {year}")

                    to_partitions(
                        data=df_year,
                        partition_columns=["ano"],
                        savepath=comex_constants.PATH.value + table_name + "/output/",
                    )
                    del df_year

            else:
                log(f"cleaning {filezip} file")
                df.rename(columns=rename_ncm, inplace=True)
                log("df renamed")

                for year in df.ano.unique():
                    # filter rows of each year then make partitions

                    df_year = df[df["ano"] == year]
                    log(f"doing partitions year, month and uf for year {year}")
                    to_partitions(
                        data=df,
                        partition_columns=["ano"],
                        savepath=comex_constants.PATH.value + table_name + "/output/",
                    )
                    del df_year

            del df

    return f"/tmp/br_me_comex_stat/{table_name}/output"


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def create_table_and_upload_to_gcs(
    data_path: Union[str, Path],
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    force_columns: bool = False,
    wait=None,  # pylint: disable=unused-argument
) -> None:
    """
    Create table using BD+ and upload to GCS.
    """
    # pylint: disable=C0103
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    table_staging = f"{tb.table_full_name['staging']}"
    # pylint: disable=C0103
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
    storage_path_link = f"https://console.cloud.google.com/storage/browser/{st.bucket_name}/staging/{dataset_id}/{table_id}"

    #####################################
    #
    # MANAGEMENT OF TABLE CREATION
    #
    #####################################
    log("STARTING TABLE CREATION MANAGEMENT")
    if dump_mode == "append":
        if tb.table_exists(mode="staging"):
            log(
                f"MODE APPEND: Table ALREADY EXISTS:"
                f"\n{table_staging}"
                f"\n{storage_path_link}"
            )
        else:
            # the header is needed to create a table when dosen't exist
            log("MODE APPEND: Table DOSEN'T EXISTS\n" + "Start to CREATE HEADER file")
            header_path = dump_header_to_csv(data_path=data_path)
            log("MODE APPEND: Created HEADER file:\n" f"{header_path}")

            tb.create(
                path=header_path,
                if_storage_data_exists="replace",
                if_table_config_exists="replace",
                if_table_exists="replace",
                force_columns=force_columns,
            )

            log(
                "MODE APPEND: Sucessfully CREATED A NEW TABLE:\n"
                f"{table_staging}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301

            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
            log(
                "MODE APPEND: Sucessfully REMOVED HEADER DATA from Storage:\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301
    elif dump_mode == "overwrite":
        if tb.table_exists(mode="staging"):
            log(
                "MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301
            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
            log(
                "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301
            tb.delete(mode="all")
            log(
                "MODE OVERWRITE: Sucessfully DELETED TABLE:\n"
                f"{table_staging}\n"
                f"{tb.table_full_name['prod']}"
            )  # pylint: disable=C0301

        # the header is needed to create a table when dosen't exist
        # in overwrite mode the header is always created
        log("MODE OVERWRITE: Table DOSEN'T EXISTS\n" + "Start to CREATE HEADER file")
        header_path = dump_header_to_csv(data_path=data_path)
        log("MODE OVERWRITE: Created HEADER file:\n" f"{header_path}")

        tb.create(
            path=header_path,
            if_storage_data_exists="replace",
            if_table_config_exists="replace",
            if_table_exists="replace",
        )

        log(
            "MODE OVERWRITE: Sucessfully CREATED TABLE\n"
            f"{table_staging}\n"
            f"{storage_path_link}"
        )

        st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
        log(
            f"MODE OVERWRITE: Sucessfully REMOVED HEADER DATA from Storage\n:"
            f"{storage_path}\n"
            f"{storage_path_link}"
        )  # pylint: disable=C0301

    #####################################
    #
    # Uploads a bunch of CSVs using BD+
    #
    #####################################

    log("STARTING UPLOAD TO GCS")
    if tb.table_exists(mode="staging"):
        # the name of the files need to be the same or the data doesn't get overwritten
        tb.append(filepath=data_path, if_exists="replace")

        log(
            f"STEP UPLOAD: Successfully uploaded {data_path} to Storage:\n"
            f"{storage_path}\n"
            f"{storage_path_link}"
        )
    else:
        # pylint: disable=C0301
        log("STEP UPLOAD: Table does not exist in STAGING, need to create first")
