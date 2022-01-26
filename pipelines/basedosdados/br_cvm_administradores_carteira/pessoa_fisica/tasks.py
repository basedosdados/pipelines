"""
Tasks for basedosdados
"""

###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################

import requests
import pandas as pd
import numpy as np
from typing import Union
from pathlib import Path
import zipfile
import os
import basedosdados as bd
from prefect import task


@task
def download_raw_files(url):

    # url = "http://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DADOS/oferta_distribuicao.csv"

    res = requests.get(url)

    return res


@task
def download_url(url, save_path, chunk_size=128):
    r = requests.get(url, stream=True)
    with open(save_path, "wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

    return save_path


@task
def treat(data_path, table_name):

    df = pd.read_csv(
        data_path + "/" + table_name,
        sep=";",
        keep_default_na=False,
        na_values="",
        encoding="latin1",
        dtype=object,
    )

    df.columns = [
        "nome",
        "data_registro",
        "data_cancelamento",
        "motivo_cancelamento",
        "situacao",
        "data_inicio_situacao",
        "categoria_registro",
    ]

    return df


@task
def unzip(zip_path, out_path):

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(out_path)

    return out_path


@task
def dataframe_to_csv(
    df: pd.DataFrame, path: Union[str, Path], filename
) -> Union[str, Path]:
    """
    Writes a dataframe to a CSV file.
    """
    # Remove filename from path
    path = Path(path)
    # Create directory if it doesn't exist
    os.makedirs(path, exist_ok=True)
    # Write dataframe to CSV
    # log(f"Writing dataframe to CSV: {path}")
    df.to_csv(path / f"{filename}.csv", index=False)
    # log(f"Wrote dataframe to CSV: {path}")

    return path


@task
def upload_to_gcs(path: Union[str, Path], dataset_id: str, table_id: str) -> None:
    """
    Uploads a bunch of CSVs using BD+
    """
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)

    st = bd.Storage(
        dataset_id="br_cvm_administradores_carteira", table_id="pessoa_fisica"
    )
    st.upload("output", if_exists="replace")
