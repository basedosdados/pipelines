"""
Download, limpeza e particionamento de br_bndes_estatisticas_operacoes.

O módulo não importa Prefect. As funções são chamadas pelas tasks de `tasks.py`
e também podem ser executadas diretamente, o que permite conferir a contagem de
linhas antes do upload.
"""

import hashlib
import shutil
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

from pipelines.datasets.br_bndes_estatisticas_operacoes.constants import (
    constants,
)
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


def read_table(csv_path: Path) -> pd.DataFrame:
    """Lê o CSV da fonte com todas as colunas como texto.

    Args:
        csv_path: CSV baixado por `download_table`.

    Returns:
        O CSV como está na fonte, no formato largo.
    """
    return pd.read_csv(
        csv_path,
        sep=constants.SEPARATOR.value,
        encoding=constants.ENCODING.value,
        dtype=str,
        keep_default_na=False,
    )


def get_source_last_modified(table_id: str) -> date:
    """Lê o dia em que o BNDES substituiu o CSV da tabela pela última vez.

    O `last_modified` do recurso muda a cada substituição do arquivo,
    inclusive numa correção de meses passados sem mês novo.

    Args:
        table_id: Slug da tabela.

    Returns:
        O dia do `last_modified` do recurso no CKAN.
    """
    resource = get_resource(constants.TABLES.value[table_id].resource_id)
    last_modified = datetime.fromisoformat(resource["last_modified"])
    log(f"{table_id}: last_modified {last_modified}")
    return last_modified.date()


def convert_value(text: str, factor: int) -> str | None:
    """Converte o valor para a unidade base 1, sem arredondar.

    A conta é feita em `Decimal` sobre o texto da fonte, para não somar erro
    de ponto flutuante.

    Args:
        text: Valor da fonte, com vírgula decimal.
        factor: Multiplicador da unidade da fonte (R$ milhões, US$ mil).

    Returns:
        O valor convertido, com ponto decimal, ou `None` quando vazio.
    """
    if text.strip() == "":
        return None
    value = Decimal(text.strip().replace(",", ".")) * factor
    return format(value.normalize(), "f")


def clean_table(table_id: str, csv_path: Path) -> Path:
    """Passa o CSV para o formato longo e grava o particionado.

    Cada coluna de categoria da fonte vira uma linha por mês, com os rótulos
    de `constants.TABLES` e o valor convertido.

    Args:
        table_id: Slug da tabela.
        csv_path: CSV baixado por `download_table`.

    Returns:
        O diretório particionado em formato Hive (`ano=2026/`).

    Raises:
        ValueError: Quando a fonte traz uma coluna sem rótulo em
            `constants.TABLES`.
    """
    _, output_dir = build_paths(table_id)
    table = constants.TABLES.value[table_id]
    period_columns = constants.PERIOD_COLUMNS.value
    category_columns = table.category_columns
    value_column = table.value_column
    labels = table.labels

    dataframe = read_table(csv_path)
    source_columns = [c for c in dataframe.columns if c not in period_columns]
    unknown = sorted(set(source_columns) - set(labels))
    if unknown:
        raise ValueError(
            f"{table_id}: colunas da fonte sem rótulo em constants: {unknown}"
        )

    long = dataframe.melt(
        id_vars=period_columns,
        value_vars=source_columns,
        var_name="coluna_fonte",
        value_name=value_column,
    )
    for column in category_columns:
        long[column] = long["coluna_fonte"].map(
            {source: values[column] for source, values in labels.items()}
        )
    long[value_column] = long[value_column].map(
        lambda text: convert_value(text, table.factor)
    )
    long = long[[*period_columns, *category_columns, value_column]]

    log(
        f"{table_id}: {len(dataframe)} meses e {len(source_columns)} "
        f"categorias, {len(long)} linhas"
    )
    write_partitions(long, constants.PARTITION_COLUMNS.value, output_dir)
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
