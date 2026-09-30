"""
Download, limpeza e particionamento da tabela operacoes_pre_embarque.

O módulo não importa Prefect. As funções são chamadas pelas tasks de `tasks.py`
e também podem ser executadas diretamente, o que permite conferir a contagem de
linhas antes do upload.
"""

import hashlib
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.br_bndes_operacoes_contratadas.constants import (
    constants,
)
from pipelines.utils.metadata.domain import DateFormat
from pipelines.utils.utils import log


def build_paths() -> tuple[Path, Path]:
    """Cria os diretórios de trabalho da tabela, apagando o `output/`.

    Returns:
        Os caminhos de `input/` e de `output/`, nessa ordem.
    """
    base = Path(constants.PATH.value)
    input_dir = base / "input"
    output_dir = base / "output"
    shutil.rmtree(output_dir, ignore_errors=True)
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return input_dir, output_dir


def get_resource() -> dict[str, Any]:
    """Lê os metadados do recurso no CKAN.

    Returns:
        O campo `result` do `resource_show`, com `last_modified`, `url` e
        `hash`, entre outros.
    """
    response = httpx.get(
        constants.RESOURCE_SHOW_URL.value,
        params={"id": constants.CKAN_RESOURCE_ID.value},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()["result"]


def get_source_last_modified() -> str:
    """Lê a data da última republicação do recurso.

    Returns:
        A data do `last_modified` do recurso, no formato `%Y-%m-%d`.
    """
    last_modified = datetime.fromisoformat(get_resource()["last_modified"])
    log(f"last_modified do recurso: {last_modified}")
    return last_modified.strftime(DateFormat.YEAR_MD)


def download_table() -> Path:
    """Baixa o CSV do recurso e confere o MD5 contra o que o CKAN publica.

    Returns:
        O caminho do CSV baixado.

    Raises:
        ValueError: Quando o CKAN não publica o MD5, ou quando o arquivo
            baixado não bate com ele.
    """
    input_dir, _ = build_paths()
    csv_path = input_dir / constants.CSV_FILENAME.value

    resource = get_resource()
    expected_md5 = resource["hash"]
    if not expected_md5:
        raise ValueError(
            "O recurso não publica o MD5 no campo `hash`; sem ele o download "
            "não tem como ser conferido."
        )

    md5 = hashlib.md5(usedforsecurity=False)
    with httpx.stream(
        method="GET",
        url=resource["url"],
        timeout=5 * 60,
        follow_redirects=True,
    ) as response:
        response.raise_for_status()
        with open(csv_path, "wb") as file:
            for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                file.write(chunk)
                md5.update(chunk)

    if md5.hexdigest() != expected_md5:
        raise ValueError(
            f"O MD5 do CSV baixado ({md5.hexdigest()}) não bate com o que o "
            f"CKAN publica ({expected_md5})."
        )

    log(f"Download conferido pelo MD5: {csv_path.stat().st_size} bytes")
    return csv_path


def parse_valor(values: pd.Series) -> pd.Series:
    """Converte valor em reais do formato brasileiro para o que o `safe_cast` lê.

    `7.028.400` vira `7028400`, e `1.234,56` viraria `1234.56`.

    Args:
        values: Valores como a fonte publica, com nulo onde não há valor.

    Returns:
        Os mesmos valores como texto, sem ponto de milhar e com ponto decimal.

    Raises:
        ValueError: Quando algum valor não segue o formato brasileiro.
    """
    is_ptbr = values.str.fullmatch(r"\d{1,3}(\.\d{3})*(,\d+)?", na=True)
    if not is_ptbr.all():
        raise ValueError(
            "Valores fora do formato brasileiro: "
            f"{values[~is_ptbr].unique()[:5].tolist()}"
        )

    return values.str.replace(".", "", regex=False).str.replace(
        ",", ".", regex=False
    )


def transform(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Renomeia, padroniza e deriva o `ano`.

    Args:
        dataframe: CSV da fonte, com todas as colunas como texto.

    Returns:
        `ano` seguido das colunas de `constants.COLUMNS`.

    Raises:
        ValueError: Quando alguma `data_da_contratacao` não segue `dd/mm/aaaa`.
    """
    dataframe = dataframe.drop(columns=constants.DROP_COLUMNS.value)
    dataframe = dataframe.rename(columns=constants.RENAME.value)
    dataframe = dataframe.apply(lambda column: column.str.strip())
    dataframe = dataframe.replace("", pd.NA)

    is_municipio = dataframe["id_municipio"].str.fullmatch(r"\d{7}", na=False)
    dataframe["id_municipio"] = dataframe["id_municipio"].where(is_municipio)

    date = pd.to_datetime(
        dataframe["data_contratacao"],
        format=constants.SOURCE_DATE_FORMAT.value,
        errors="coerce",
    )
    invalid_dates = dataframe.loc[date.isna(), "data_contratacao"]
    if not invalid_dates.empty:
        raise ValueError(
            f"{len(invalid_dates)} linha(s) com data de contratação fora de "
            f"dd/mm/aaaa: {invalid_dates.unique()[:5].tolist()}"
        )
    dataframe["data_contratacao"] = date.dt.strftime(DateFormat.YEAR_MD.value)
    dataframe["ano"] = date.dt.year.astype(str)

    dataframe["valor_operacao"] = parse_valor(dataframe["valor_operacao"])
    dataframe["valor_desembolsado"] = parse_valor(
        dataframe["valor_desembolsado"]
    )

    return dataframe[
        constants.PARTITION_COLUMNS.value + constants.COLUMNS.value
    ]


def write_partitions(dataframe: pd.DataFrame, output_dir: Path) -> None:
    """Grava em partições Hive por `ano`, com todas as colunas como texto.

    Args:
        dataframe: Saída de `transform`.
        output_dir: Raiz do particionado.
    """
    columns = constants.COLUMNS.value
    schema = pa.schema([(column, pa.string()) for column in columns])

    for ano, group in dataframe.groupby("ano"):
        partition = output_dir / f"ano={ano}"
        partition.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(
            group[columns], schema=schema, preserve_index=False
        )
        pq.write_table(table, partition / "data.parquet", compression="snappy")


def clean_table(csv_path: Path) -> Path:
    """Limpa o CSV e grava o particionado por `ano`.

    Args:
        csv_path: CSV baixado por `download_table`.

    Returns:
        O diretório particionado, no formato esperado por `upload_to_gcs`.
    """
    _, output_dir = build_paths()

    dataframe = pd.read_csv(
        csv_path,
        sep=";",
        encoding="cp1252",
        dtype=str,
        keep_default_na=False,
        na_values=[""],
    )
    log(f"{len(dataframe)} linhas lidas de {csv_path}")

    dataframe = transform(dataframe)
    write_partitions(dataframe, output_dir)
    log(
        f"{len(dataframe)} linhas gravadas em {dataframe['ano'].nunique()} "
        f"partições de ano em {output_dir}"
    )

    return output_dir
