"""
Download, limpeza e particionamento de br_bndes_renda_variavel.

O módulo não importa Prefect. As funções são chamadas pelas tasks de `tasks.py`
e também podem ser executadas diretamente, o que permite conferir a contagem de
linhas antes do upload.
"""

import hashlib
import re
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

from pipelines.datasets.br_bndes_renda_variavel.constants import constants
from pipelines.utils.utils import brasil_proxy_url, log


def build_paths(table_id: str) -> tuple[Path, Path]:
    """Cria os diretórios de trabalho da tabela.

    O `output/` é apagado a cada chamada: só as partições desta execução podem
    estar nele, porque o pipeline promove para prod toda pasta que achar ali.

    Args:
        table_id: Slug da tabela.

    Returns:
        Os caminhos de `input/` e de `output/`, nessa ordem.
    """
    base = Path(constants.PATH.value) / table_id
    input_dir, output_dir = base / "input", base / "output"
    shutil.rmtree(output_dir, ignore_errors=True)
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return input_dir, output_dir


def get_resource(resource_id: str) -> dict[str, Any]:
    """Lê os metadados do recurso no CKAN do BNDES.

    Args:
        resource_id: ID do recurso no CKAN.

    Returns:
        O campo `result` do `resource_show`, com `url` e `hash`, entre outros.
    """
    response = httpx.get(
        constants.RESOURCE_SHOW_URL.value,
        params={"id": resource_id},
        timeout=60,
        proxy=brasil_proxy_url(),
    )
    response.raise_for_status()
    return response.json()["result"]


def assert_md5(path: Path, expected_md5: str) -> None:
    """Confere o MD5 do arquivo baixado com o que o CKAN publica.

    Args:
        path: Arquivo baixado.
        expected_md5: MD5 do campo `hash` do recurso.

    Raises:
        ValueError: Quando os dois MD5 não batem.
    """
    md5 = hashlib.md5(usedforsecurity=False)
    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            md5.update(chunk)
    if md5.hexdigest() != expected_md5:
        raise ValueError(
            f"O MD5 de {path} ({md5.hexdigest()}) não bate com o publicado "
            f"pelo BNDES ({expected_md5})."
        )


def download_table(table_id: str) -> Path:
    """Baixa o CSV da tabela e confere o MD5.

    Args:
        table_id: Slug da tabela.

    Returns:
        O caminho do CSV baixado.

    Raises:
        ValueError: Quando o CKAN não publica o MD5 do recurso.
    """
    input_dir, _ = build_paths(table_id)
    resource = get_resource(constants.TABLES.value[table_id].resource_id)
    expected_md5 = resource.get("hash")
    if not expected_md5:
        raise ValueError(
            f"O CKAN não publica o MD5 do recurso de {table_id}; sem ele não "
            "dá para conferir o download."
        )

    csv_path = input_dir / f"{table_id}.csv"
    with httpx.stream(
        "GET",
        resource["url"],
        timeout=60,
        follow_redirects=True,
        proxy=brasil_proxy_url(),
    ) as response:
        response.raise_for_status()
        with open(csv_path, "wb") as file:
            for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                file.write(chunk)

    assert_md5(csv_path, expected_md5)
    log(f"{table_id}: {csv_path.stat().st_size} bytes em {csv_path}")
    return csv_path


def get_source_last_modified(table_id: str) -> date:
    """Lê o dia em que o BNDES substituiu o CSV da tabela pela última vez.

    Args:
        table_id: Slug da tabela.

    Returns:
        O dia do `last_modified` do recurso no CKAN.
    """
    resource = get_resource(constants.TABLES.value[table_id].resource_id)
    last_modified = datetime.fromisoformat(resource["last_modified"])
    log(f"{table_id}: last_modified {last_modified}")
    return last_modified.date()


def read_table(csv_path: Path) -> pd.DataFrame:
    """Lê o CSV da fonte com todas as colunas como texto.

    Args:
        csv_path: CSV baixado por `download_table`.

    Returns:
        O CSV como está na fonte.
    """
    return pd.read_csv(
        csv_path,
        sep=constants.SEPARATOR.value,
        encoding=constants.ENCODING.value,
        dtype=str,
        keep_default_na=False,
    )


def clean_cnpj(text: str) -> str | None:
    """Deixa só os dígitos do CNPJ.

    Args:
        text: CNPJ da fonte, no formato `00.000.000/0000-00` ou `N/A`.

    Returns:
        Os 14 dígitos do CNPJ, ou `None` quando a fonte não traz número.
    """
    return re.sub(r"\D", "", text) or None


def clean_category(text: str) -> str | None:
    """Põe a categoria com inicial maiúscula e o resto minúsculo.

    Args:
        text: Categoria da fonte, em caixa alta.

    Returns:
        A categoria reescrita, ou `None` para `N/A`, `-` e célula vazia.
    """
    if text in constants.NULL_VALUES.value:
        return None
    return text.capitalize()


def clean_decimal(text: str) -> str | None:
    """Troca a vírgula decimal por ponto, sem arredondar.

    Args:
        text: Número da fonte, com vírgula decimal.

    Returns:
        O número com ponto decimal, ou `None` quando a célula está vazia.
    """
    if text == "":
        return None
    return text.replace(",", ".")


def clean_indicator(column: str, values: pd.Series) -> pd.Series:
    """Converte `SIM` em 1 e `NÃO` em 0.

    Args:
        column: Nome da coluna na tabela.
        values: Valores da coluna na fonte.

    Returns:
        A coluna com 1 e 0, como texto.

    Raises:
        ValueError: Quando a coluna tem valor diferente de `SIM` e `NÃO`.
    """
    mapping = constants.INDICATOR_VALUES.value
    unknown = sorted(set(values) - set(mapping))
    if unknown:
        raise ValueError(f"{column}: valores fora de SIM/NÃO: {unknown}")
    return values.map(mapping)


def clean_column(column: str, values: pd.Series) -> pd.Series:
    """Aplica à coluna o tratamento do tipo dela.

    Args:
        column: Nome da coluna na tabela.
        values: Valores da coluna, sem espaços nas pontas.

    Returns:
        A coluna tratada, com `None` no lugar dos nulos.
    """
    if column == constants.CNPJ_COLUMN.value:
        return values.map(clean_cnpj)
    if column in constants.CATEGORY_COLUMNS.value:
        return values.map(clean_category)
    if column in constants.INDICATOR_COLUMNS.value:
        return clean_indicator(column, values)
    if column in constants.DECIMAL_COLUMNS.value:
        return values.map(clean_decimal)
    return values.map(lambda text: text or None)


def clean_table(table_id: str, csv_path: Path) -> Path:
    """Renomeia, trata e particiona o CSV da tabela.

    Args:
        table_id: Slug da tabela.
        csv_path: CSV baixado por `download_table`.

    Returns:
        O diretório particionado em formato Hive (`ano=2026/`).

    Raises:
        ValueError: Quando as colunas da fonte não são as esperadas.
    """
    _, output_dir = build_paths(table_id)
    table = constants.TABLES.value[table_id]

    dataframe = read_table(csv_path)
    missing = sorted(set(table.rename) - set(dataframe.columns))
    unexpected = sorted(set(dataframe.columns) - set(table.rename))
    if missing or unexpected:
        raise ValueError(
            f"{table_id}: colunas da fonte mudaram; faltam {missing}, "
            f"sobram {unexpected}"
        )

    dataframe = dataframe.rename(columns=table.rename)[table.columns]
    dataframe = dataframe.map(str.strip)
    for column in table.columns:
        dataframe[column] = clean_column(column, dataframe[column])

    log(f"{table_id}: {len(dataframe)} linhas")
    write_partitions(dataframe, constants.PARTITION_COLUMNS.value, output_dir)
    return output_dir


def write_partitions(
    dataframe: pd.DataFrame,
    partition_columns: list[str],
    output_dir: Path,
) -> None:
    """Grava em partições Hive com todas as colunas como texto.

    A staging é toda STRING por convenção da casa, e o `.sql` faz `safe_cast`
    de cada coluna.

    Args:
        dataframe: Dados a gravar.
        partition_columns: Colunas que compõem o caminho da partição.
        output_dir: Raiz do particionado.
    """
    for keys, group in dataframe.groupby(partition_columns, dropna=False):
        keys = keys if isinstance(keys, tuple) else (keys,)
        partition = output_dir.joinpath(
            *(
                f"{col}={key}"
                for col, key in zip(partition_columns, keys, strict=True)
            )
        )
        partition.mkdir(parents=True, exist_ok=True)
        group.drop(columns=partition_columns).map(
            lambda value: None if pd.isna(value) else str(value)
        ).to_parquet(
            partition / "data.parquet", compression="snappy", index=False
        )
