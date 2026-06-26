"""
Utilitários gerais — logging, detecção de ambiente, helpers de string.
"""

import logging
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd


def log(msg: Any, level: str = "info") -> None:
    """Loga uma mensagem. Usa o logger do Prefect dentro de tasks/flows; logging padrão fora."""
    levels = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    if level not in levels:
        raise ValueError(f"Nível de log inválido: {level}")
    try:
        from prefect.logging import get_run_logger

        get_run_logger().log(levels[level], str(msg))
    except Exception:
        logging.getLogger("pipelines").log(levels[level], str(msg))


def is_running_in_prod() -> bool:
    """Retorna True se o flow está rodando no work pool de prod (basedosdados)."""
    try:
        from prefect.runtime import flow_run

        return flow_run.work_pool_name == "basedosdados"
    except Exception:
        return False


def query_to_line(query: str) -> str:
    return " ".join(line.strip() for line in query.split("\n"))


def to_partitions(
    data: pd.DataFrame,
    partition_columns: list[str],
    savepath: str,
    file_type: str = "csv",
) -> None:
    """Salva um DataFrame em partições Hive (pasta/coluna=valor/data.csv)."""
    if not isinstance(data, pd.DataFrame):
        raise TypeError("data deve ser um pandas DataFrame")

    savepath = Path(savepath)
    unique_combinations = (
        data[partition_columns].drop_duplicates().to_dict(orient="records")
    )

    for combo in unique_combinations:
        partition_path = savepath / "/".join(
            f"{k}={v}" for k, v in combo.items()
        )
        partition_path.mkdir(parents=True, exist_ok=True)

        df_slice = data.loc[
            data[list(combo.keys())].isin(list(combo.values())).all(axis=1)
        ].drop(columns=partition_columns)

        if file_type == "csv":
            out = partition_path / "data.csv"
            df_slice.to_csv(
                out,
                sep=",",
                encoding="utf-8",
                na_rep="",
                index=False,
                mode="a",
                header=not out.exists(),
            )
        elif file_type == "parquet":
            out = partition_path / "data.parquet"
            df_slice.to_parquet(out, index=False, compression="gzip")
        else:
            raise ValueError(f"file_type inválido: {file_type!r}")


def clean_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Remove null bytes e normaliza strings em colunas object de um DataFrame."""
    for col in dataframe.columns.tolist():
        if dataframe[col].dtype == object:
            try:
                dataframe[col] = (
                    dataframe[col]
                    .astype(str)
                    .str.replace("\x00", "", regex=True)
                    .replace("None", np.nan, regex=True)
                )
            except Exception as exc:
                print(
                    "Column: ",
                    col,
                    "\nData: ",
                    dataframe[col].tolist(),
                    "\n",
                    exc,
                )
                raise
    return dataframe


def download_and_unzip_file(
    url: str, path: str, headers: dict | None = None
) -> None:
    """Baixa um arquivo ZIP da URL e extrai no caminho indicado.

    Parameters
    ----------
    url : str
        URL do arquivo ZIP.
    path : str
        Diretório onde o conteúdo será extraído.
    headers : dict | None
        Cabeçalhos HTTP (ex.: User-Agent de browser). Quando None, o download
        ocorre exatamente como antes (sem cabeçalhos customizados).
    """
    log(f"Baixando {url} → {path}")
    try:
        if headers:
            request_with_headers = Request(url=url, headers=headers)
            r = urlopen(request_with_headers)
        else:
            r = urlopen(url=url)
        zf = zipfile.ZipFile(BytesIO(r.read()))
        zf.extractall(path=path)
        log(f"Extração concluída: {url} → {path}")
    except Exception as e:
        log(e, level="error")
        raise
