import os
import zipfile
from datetime import datetime
from pathlib import Path

import basedosdados as bd
import pandas as pd
import requests
from prefect import task

from pipelines.datasets.br_cgu_emendas_parlamentares.constants import constants
from pipelines.utils.utils import log

INPUT = Path("/tmp/input/")
OUTPUT = Path("/tmp/output/")


def download_unzip_file(input_dir: Path | str = INPUT):
    input_dir = Path(input_dir) if isinstance(input_dir, str) else input_dir
    if not input_dir.exists():
        input_dir.mkdir(exist_ok=True, parents=True)

    try:
        r = requests.get(
            constants.URL.value,
            headers={"User-Agent": "Mozilla/5.0"},
            verify=False,
        )
        with open(  # nosec
            input_dir / "emendas_parlamentares.zip", "wb"
        ) as code:
            code.write(r.content)

        with zipfile.ZipFile(
            input_dir / "emendas_parlamentares.zip", "r"
        ) as zp:
            zp.extractall(input_dir)

        os.remove(input_dir / "emendas_parlamentares.zip")

        log("Arquivo baixado e descompactado com sucesso")
    except Exception as e:
        print(e)
        log("Erro ao baixar e descompactar arquivo")


@task
def convert_str_to_float(
    input_dir: Path | str = INPUT, output_dir: Path | str = OUTPUT
):
    input_dir = Path(input_dir) if isinstance(input_dir, str) else input_dir
    df = pd.read_csv(
        input_dir / "EmendasParlamentares.csv",
        sep=";",
        encoding="latin1",
    )
    log("Convertendo valores para float")

    for _ in constants.VALUES_FLOAT.value:
        df[_] = df[_].str.replace(",", ".").astype(float)

    output_dir = (
        Path(output_dir) if isinstance(output_dir, str) else output_dir
    )
    output = output_dir / "microdados.csv"

    if not output_dir.exists():
        output_dir.mkdir(exist_ok=True, parents=True)

    df.to_csv(output, sep=",", encoding="utf-8", index=False)
    log("---------------- Tabela salva -------------------")
    return output_dir


@task
def get_last_modified_time(input_dir: Path = INPUT):
    download_unzip_file()
    emendas = pd.read_csv(
        input_dir / "EmendasParlamentares.csv",
        sep=";",
        encoding="latin1",
    )

    data = bd.read_sql(
        constants.QUERY.value,
        billing_project_id="basedosdados",
        from_file=True,
    )

    date = data.iloc[0].to_numpy()
    log("Data da última atualização: " + str(date[1]))
    log("Quantidade de linhas na tabela: " + str(date[0]))
    log("Quantidade de linhas no arquivo: " + str(emendas.shape[0]))
    if emendas.shape[0] > date[0]:
        return datetime.today()

    return date[1]
