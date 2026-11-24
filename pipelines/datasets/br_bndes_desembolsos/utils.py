"""
Download, limpeza e particionamento de br_bndes_desembolsos.

O módulo não importa Prefect. As funções são chamadas pelas tasks de `tasks.py`
e também podem ser executadas diretamente, o que permite conferir a contagem de
linhas antes do upload.
"""

import hashlib
import re
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


def build_paths(path: str) -> tuple[Path, Path]:
    """Cria os diretórios de trabalho da tabela, apagando o `output/`.

    Args:
        path: Diretório de trabalho da tabela.

    Returns:
        Os caminhos de `input/` e de `output/`, nessa ordem.
    """
    base = Path(path)
    input_dir = base / "input"
    output_dir = base / "output"
    shutil.rmtree(output_dir, ignore_errors=True)
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return input_dir, output_dir


def get_resource(resource_id: str) -> dict[str, Any]:
    """Lê os metadados do recurso no CKAN.

    Args:
        resource_id: Id do recurso no CKAN.

    Returns:
        O campo `result` do `resource_show`, com `last_modified`, `url` e
        `hash`, entre outros.
    """
    response = httpx.get(
        constants.RESOURCE_SHOW_URL.value,
        params={"id": resource_id},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()["result"]


def get_source_last_modified(resource_id: str) -> str:
    """Lê a data da última republicação do recurso.

    Args:
        resource_id: Id do recurso no CKAN.

    Returns:
        A data do `last_modified` do recurso, no formato `%Y-%m-%d`.
    """
    last_modified = datetime.fromisoformat(
        get_resource(resource_id=resource_id)["last_modified"]
    )
    log(f"last_modified do recurso: {last_modified}")
    return last_modified.strftime(DateFormat.YEAR_MD)


def download_table(resource_id: str, path: str, filename: str) -> Path:
    """Baixa o CSV do recurso e confere o MD5 contra o que o CKAN publica.

    Args:
        resource_id: Id do recurso no CKAN.
        path: Diretório de trabalho da tabela.
        filename: Nome com que o CSV é gravado em `input/`.

    Returns:
        O caminho do CSV baixado.

    Raises:
        ValueError: Quando o CKAN não publica o MD5, ou quando o arquivo
            baixado não bate com ele.
    """
    input_dir, _ = build_paths(path=path)
    csv_path = input_dir / filename

    resource = get_resource(resource_id=resource_id)
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

    for column, labels in constants.MENSAL_SETOR_LABELS.value.items():
        dataframe[column] = dataframe[column].str.capitalize().replace(labels)

    dataframe["sigla_uf"] = parse_sigla_uf(dataframe["sigla_uf"])
    dataframe["id_municipio"] = parse_id_municipio(dataframe["id_municipio"])
    dataframe["valor_desembolsado"] = parse_valor(
        dataframe["valor_desembolsado"]
    )

    return dataframe[
        constants.PARTITION_COLUMNS.value + constants.COLUMNS.value
    ]


def clean_mensal(csv_path: Path) -> Path:
    """Limpa o CSV da mensal em blocos e grava um parquet por `ano`.

    O CSV tem o histórico inteiro e passa de 700 MB, então é lido em blocos de
    `constants.CHUNKSIZE` linhas. Cada `ano` mantém um escritor aberto até o
    fim, de modo que a partição recebe um `data.parquet` só, qualquer que seja
    a ordem das linhas no arquivo.

    Args:
        csv_path: CSV baixado por `download_table`.

    Returns:
        O diretório particionado, no formato esperado por `upload_to_gcs`.
    """
    _, output_dir = build_paths(path=constants.MENSAL_PATH.value)

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


def parse_cnae_codes(text: str) -> list[str]:
    """Separa a célula de códigos CNAE do mapeamento em um código por item.

    Listas (`D352 e D353`, `K64, K65 e K66`) viram um código por item, e
    faixas de divisões (`A01 a A03`) viram uma divisão por número. A marca
    `(restante)` de `H49 (restante)` sai, e a linha fica com a divisão 49.

    Args:
        text: Célula como a fonte publica, com os espaços já normalizados.

    Returns:
        Os códigos, cada um com a letra da seção seguida dos dígitos.

    Raises:
        ValueError: Quando um item não é código CNAE nem faixa de divisões
            de uma mesma seção.
    """
    text = text.replace(
        constants.MAPEAMENTO_BNDES_CNAE_REMAINDER_MARKER.value, ""
    ).strip()

    codes = []
    for item in re.split(r", | e ", text):
        range_match = re.fullmatch(r"([A-Z])(\d{2}) a ([A-Z])(\d{2})", item)
        if range_match:
            secao_start, divisao_start, secao_end, divisao_end = (
                range_match.groups()
            )
            if secao_start != secao_end:
                raise ValueError(
                    f"Faixa de divisões CNAE entre seções diferentes: {item!r}"
                )
            for divisao in range(int(divisao_start), int(divisao_end) + 1):
                codes.append(f"{secao_start}{divisao:02d}")
        elif re.fullmatch(r"[A-Z](\d{2}|\d{3}|\d{4}|\d{5}|\d{7})", item):
            codes.append(item)
        else:
            raise ValueError(f"Código CNAE fora do padrão: {item!r}")
    return codes


def clean_mapeamento_bndes_cnae(csv_path: Path) -> Path:
    """Limpa o mapeamento BNDES → CNAE e grava uma linha por código CNAE.

    Cada linha da fonte vira tantas linhas quantos forem os códigos da célula
    `codigo_cnae_ibge`. O código é decomposto em seção, divisão, grupo, classe
    e subclasse, nos formatos do diretório `br_bd_diretorios_brasil.cnae_2`;
    os níveis abaixo do código ficam nulos. A classe que a fonte publica sem
    dígito verificador (4 dígitos) segue assim e é completada no dbt.

    Args:
        csv_path: CSV baixado por `download_table`.

    Returns:
        O diretório com o parquet, no formato esperado por `upload_to_gcs`.
    """
    _, output_dir = build_paths(
        path=constants.MAPEAMENTO_BNDES_CNAE_PATH.value
    )

    dataframe = pd.read_csv(
        csv_path,
        sep=constants.SEPARATOR.value,
        encoding=constants.ENCODING.value,
        dtype=str,
        keep_default_na=False,
    )
    rows_read = len(dataframe)
    dataframe = dataframe.rename(
        columns=constants.MAPEAMENTO_BNDES_CNAE_RENAME.value
    )
    dataframe = dataframe.apply(
        lambda column: column.str.split().str.join(" ")
    )
    accents = constants.MAPEAMENTO_BNDES_CNAE_ACCENTS.value
    for column in constants.MAPEAMENTO_BNDES_CNAE_CATEGORICAL_COLUMNS.value:
        dataframe[column] = dataframe[column].str.capitalize()
        for word, accented in accents.items():
            dataframe[column] = dataframe[column].str.replace(
                rf"\b{word}\b", accented, regex=True
            )

    dataframe["codigo_cnae"] = dataframe["codigo_cnae"].map(parse_cnae_codes)
    dataframe = dataframe.explode("codigo_cnae", ignore_index=True)

    digits = dataframe["codigo_cnae"].str[1:]
    length = digits.str.len()
    dataframe["cnae_2_secao"] = dataframe["codigo_cnae"].str[0]
    dataframe["cnae_2_divisao"] = digits.str[:2]
    dataframe["cnae_2_grupo"] = digits.str[:3].where(length >= 3)
    dataframe["cnae_2_classe"] = digits.str[:5].where(length >= 4)
    dataframe["cnae_2_subclasse"] = digits.where(length == 7)

    columns = constants.MAPEAMENTO_BNDES_CNAE_COLUMNS.value
    schema = pa.schema([(column, pa.string()) for column in columns])
    pq.write_table(
        pa.Table.from_pandas(
            dataframe[columns], schema=schema, preserve_index=False
        ),
        output_dir / "data.parquet",
        compression="snappy",
    )

    log(
        f"{rows_read} linhas lidas, {len(dataframe)} gravadas (uma por código "
        f"CNAE) em {output_dir}"
    )
    return output_dir
