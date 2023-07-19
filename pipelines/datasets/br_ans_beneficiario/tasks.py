# -*- coding: utf-8 -*-
"""
Tasks for br_ans_beneficiario
"""
import pandas as pd
from multiprocessing import Pool
from datetime import datetime
from loguru import logger
from pathlib import Path
from ftputil import FTPHost
from functools import reduce
from prefect import task
import os
from glob import glob
from pipelines.datasets.br_ans_beneficiario.utils import (
    host_months_path,
    host_list,
    host,
    read_csv_zip_to_dataframe,
    process,
    RAW_COLLUNS_TYPE,
)


@task  # noqa
def ans(data_inicio: str, data_fim: str):
    logger.info(f"Data início: {data_inicio} ---->  Data fim: {data_fim}.")
    for month_path, month_date in host_months_path(data_inicio, data_fim):
        # print(month_path, month_date)
        for state_path in host_list(month_path):
            state = state_path.split("_")[-1].split(".")[0]
            # print(state, month_path)
            output_path = (
                Path("/tmp/data/output")
                / f'ano={month_date.strftime("%Y")}'
                / f'mes={month_date.strftime("%m")}'
                / f"sigla_uf={state}"
                / f'ben{month_date.strftime("%Y%m")}-{state}.csv'
            )

            if output_path.exists():
                logger.info(f"Jumping path {output_path}. Already download")
                continue
            input_path = (
                Path("/tmp/data/input")
                / f'ano={month_date.strftime("%Y")}'
                / f'mes={month_date.strftime("%m")}'
                / f'ben{month_date.strftime("%Y%m")}_{state}.csv'
            )
            # print(input_path)
            if input_path.exists():
                logger.debug(f"reading input in {input_path}")
                df = pd.read_csv(
                    input_path, encoding="utf-8", dtype=RAW_COLLUNS_TYPE, index_col=0
                )
            else:
                # Wtf, pq tem um repositorio aqui?
                # https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios/201602/201607/
                if not host.path.isfile(state_path):
                    continue
                df = read_csv_zip_to_dataframe(state_path)
                # input_path.parent.mkdir(parents=True, exist_ok=True)
                # df.to_csv(input_path, encoding="utf-8")

                logger.info("Cleaning dataset")
                df = process(df)
                output_path.parent.mkdir(parents=True, exist_ok=True)

                # delete partition columns
                del df["ano"]
                del df["mes"]
                del df["sigla_uf"]

                logger.info(f"Writing to output {output_path.as_posix()}")
                df.to_csv(output_path, index=False)

    return "/tmp/data/output/"
