"""
Download, limpeza e particionamento de br_bndes_desembolsos.

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

from pipelines.datasets.br_bndes_desembolsos.constants import constants
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


def parse_sigla_uf(values: pd.Series) -> pd.Series:
    """Converte o nome da UF, como a fonte publica, na sigla.

    Args:
        values: Nomes das UFs em caixa alta e sem acento, com nulo onde não há
            UF.

    Returns:
        As siglas, com nulo onde não havia UF.

    Raises:
        ValueError: Quando algum nome não está em `constants.SIGLA_UF`.
    """
    siglas = values.map(constants.SIGLA_UF.value)
    unknown = values[siglas.isna() & values.notna()]
    if not unknown.empty:
        raise ValueError(
            f"UF fora de constants.SIGLA_UF: {unknown.unique()[:5].tolist()}"
        )
    return siglas


def parse_id_municipio(values: pd.Series) -> pd.Series:
    """Mantém só os códigos de município válidos.

    Fica nulo o que não tem 7 dígitos e o `9999998`, que a fonte usa com o
    município "DIVERSOS".

    Args:
        values: Códigos como a fonte publica.

    Returns:
        Os códigos de 7 dígitos, com nulo no lugar dos demais.
    """
    is_municipio = values.str.fullmatch(r"\d{7}", na=False) & (
        values != constants.ID_MUNICIPIO_DIVERSOS.value
    )
    return values.where(is_municipio)


def parse_valor(values: pd.Series) -> pd.Series:
    """Troca a vírgula decimal pelo ponto que o `safe_cast` lê.

    `24753538073,6` vira `24753538073.6`. A fonte não usa ponto de milhar.

    Args:
        values: Valores como a fonte publica.

    Returns:
        Os mesmos valores como texto, com ponto decimal.

    Raises:
        ValueError: Quando algum valor não segue o formato `1234,56`.
    """
    is_valid = values.str.fullmatch(r"-?\d+(,\d+)?", na=True)
    if not is_valid.all():
        raise ValueError(
            "Valores fora do formato 1234,56: "
            f"{values[~is_valid].unique()[:5].tolist()}"
        )
    return values.str.replace(",", ".", regex=False)


def transform(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Renomeia e padroniza um bloco do CSV.

    Args:
        dataframe: Bloco do CSV da fonte, com todas as colunas como texto.

    Returns:
        `ano` seguido das colunas de `constants.COLUMNS`.
    """
    dataframe = dataframe.rename(columns=constants.RENAME.value)
    dataframe = dataframe.apply(lambda column: column.str.strip())
    dataframe = dataframe.replace("", pd.NA)

    dataframe["sigla_uf"] = parse_sigla_uf(dataframe["sigla_uf"])
    dataframe["id_municipio"] = parse_id_municipio(dataframe["id_municipio"])
    dataframe["valor_desembolsado"] = parse_valor(
        dataframe["valor_desembolsado"]
    )

    return dataframe[
        constants.PARTITION_COLUMNS.value + constants.COLUMNS.value
    ]


def clean_table(csv_path: Path) -> Path:
    """Limpa o CSV em blocos e grava um parquet por `ano`.

    O CSV tem o histórico inteiro e passa de 700 MB, então é lido em blocos de
    `constants.CHUNKSIZE` linhas. Cada `ano` mantém um escritor aberto até o
    fim, de modo que a partição recebe um `data.parquet` só, qualquer que seja
    a ordem das linhas no arquivo.

    Args:
        csv_path: CSV baixado por `download_table`.

    Returns:
        O diretório particionado, no formato esperado por `upload_to_gcs`.
    """
    _, output_dir = build_paths()

    columns = constants.COLUMNS.value
    schema = pa.schema([(column, pa.string()) for column in columns])
    writers: dict[Path, pq.ParquetWriter] = {}
    rows_read = 0
    rows_written = 0

    chunks = pd.read_csv(
        csv_path,
        sep=constants.SEPARATOR.value,
        encoding=constants.ENCODING.value,
        dtype=str,
        keep_default_na=False,
        na_values=[""],
        chunksize=constants.CHUNKSIZE.value,
    )
    try:
        for chunk in chunks:
            dataframe = transform(chunk)
            for ano, group in dataframe.groupby("ano"):
                partition = output_dir / f"ano={ano}"
                if partition not in writers:
                    partition.mkdir(parents=True, exist_ok=True)
                    writers[partition] = pq.ParquetWriter(
                        partition / "data.parquet",
                        schema=schema,
                        compression="snappy",
                    )
                writers[partition].write_table(
                    pa.Table.from_pandas(
                        group[columns], schema=schema, preserve_index=False
                    )
                )
                rows_written += len(group)
            rows_read += len(chunk)
    finally:
        for writer in writers.values():
            writer.close()

    if rows_written != rows_read:
        raise ValueError(
            f"{rows_read - rows_written} linhas sem `ano` ficaram fora das "
            "partições; a tabela subiria incompleta."
        )

    log(
        f"{rows_read} linhas lidas, {rows_written} gravadas em "
        f"{len(writers)} partições de ano em {output_dir}"
    )
    return output_dir
