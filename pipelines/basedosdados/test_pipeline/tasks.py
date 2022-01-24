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

from prefect import task


import os
from uuid import uuid4
import datetime
from pathlib import Path
from typing import Union
import requests

import pandas as pd
import numpy as np
import basedosdados as bd

# from pipelines.utils import log
@task
def get_data() -> pd.DataFrame:
    """
    Get random data
    """
    URL = "https://x-math.herokuapp.com/api/random"
    r = requests.get(URL)

    cols = ["date", "first", "second", "operation", "expression", "answer"]
    ds = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    try:
        df = pd.json_normalize(r.json())
        df["date"] = ds
    except:
        data = [datetime.datetime.now(), np.nan, np.nan, np.nan, np.nan, np.nan]
        df = pd.DataFrame(ds, columns=cols)
    return df[cols]


@task
def dataframe_to_csv(df: pd.DataFrame, path: Union[str, Path]) -> Union[str, Path]:
    """
    Writes a dataframe to a CSV file.
    """
    # Remove filename from path
    path = Path(path)
    # Create directory if it doesn't exist
    os.makedirs(path, exist_ok=True)
    # Write dataframe to CSV
    # log(f"Writing dataframe to CSV: {path}")
    ds = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M")
    df.to_csv(path / f"{ds}.csv", index=False)
    # log(f"Wrote dataframe to CSV: {path}")

    return path


@task
def upload_to_gcs(path: Union[str, Path], dataset_id: str, table_id: str) -> None:
    """
    Uploads a bunch of CSVs using BD+
    """
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)

    if tb.table_exists(mode="staging"):
        # Delete old data
        # st.delete_table(
        #     mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
        # log(
        #     f"Successfully deleted OLD DATA {st.bucket_name}.staging.{dataset_id}.{table_id}"
        # )

        # the name of the files need to be the same or the data doesn't get overwritten
        tb.append(
            filepath=path,
            if_exists="replace",
        )


#         log(
#             f"Successfully uploaded {path} to {tb.bucket_name}.staging.{dataset_id}.{table_id}"
#         )

#     else:
#         log(
#             "Table does not exist in STAGING, need to create it in local first.\nCreate and publish the table in BigQuery first."
#         )
