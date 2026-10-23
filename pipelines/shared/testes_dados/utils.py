"""
Checagens de dado antes do upload, compartilhadas entre os flows.

Cada função levanta `ValueError` quando o dado não passa.
"""

import hashlib
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from pipelines.shared.testes_dados.constants import constants
from pipelines.utils.utils import log


def _examples(values: pd.Series) -> list:
    """Devolve os primeiros valores distintos da série, para a mensagem de erro.

    Args:
        values: Valores que não passaram na checagem.

    Returns:
        Até `constants.MAX_EXAMPLES` valores distintos.
    """
    return values.unique()[: constants.MAX_EXAMPLES.value].tolist()


def check_md5(path: str | Path, expected_md5: str) -> None:
    """Confere o MD5 de um arquivo baixado contra o publicado pela fonte.

    Args:
        path: Arquivo baixado.
        expected_md5: MD5 publicado pela fonte.

    Raises:
        ValueError: Quando a fonte não publica o MD5, ou quando ele não bate
            com o do arquivo.
    """
    path = Path(path)

    if not expected_md5:
        raise ValueError(
            f"A fonte não publica o MD5 de {path}; o download não tem como "
            "ser conferido."
        )

    md5 = hashlib.md5(usedforsecurity=False)
    with open(path, "rb") as file:
        chunk = file.read(constants.HASH_CHUNK_BYTES.value)
        while chunk:
            md5.update(chunk)
            chunk = file.read(constants.HASH_CHUNK_BYTES.value)

    if md5.hexdigest() != expected_md5.lower():
        raise ValueError(
            f"O MD5 de {path} ({md5.hexdigest()}) não bate com o publicado "
            f"pela fonte ({expected_md5})."
        )

    log(f"Download conferido pelo MD5: {path}")


def check_file_size(path: str | Path, expected_size: int) -> None:
    """Confere o tamanho de um arquivo baixado contra o declarado pela fonte.

    Args:
        path: Arquivo baixado.
        expected_size: Tamanho em bytes declarado pela fonte, como o
            `Content-Length` da resposta.

    Raises:
        ValueError: Quando os tamanhos divergem.
    """
    path = Path(path)
    size = path.stat().st_size

    if size != expected_size:
        raise ValueError(
            f"{path} tem {size} bytes, e a fonte declara {expected_size}."
        )

    log(f"Tamanho conferido: {size} bytes em {path}")


def check_row_count(rows_read: int, rows_expected: int, source: str) -> None:
    """Confere as linhas lidas contra a contagem declarada pela fonte.

    Args:
        rows_read: Linhas lidas do arquivo baixado.
        rows_expected: Linhas declaradas pela fonte.
        source: Onde a fonte declara a contagem, para a mensagem de erro.

    Raises:
        ValueError: Quando as contagens divergem.
    """
    if rows_read != rows_expected:
        raise ValueError(
            f"{rows_read} linhas lidas, {rows_expected} declaradas em "
            f"{source}."
        )

    log(f"Contagem de linhas confere com {source}: {rows_read}")


def count_written_rows(
    output_dir: str | Path, csv_separator: str = ","
) -> int:
    """Conta as linhas gravadas nos `.csv` e `.parquet` de um diretório.

    Do parquet, usa a contagem dos metadados, sem ler o dado.

    Args:
        output_dir: Raiz das partições.
        csv_separator: Separador dos `.csv`.

    Returns:
        O total de linhas, sem os cabeçalhos dos `.csv`.
    """
    output_dir = Path(output_dir)
    rows = 0

    for path in sorted(output_dir.rglob("*.parquet")):
        rows += pq.ParquetFile(path).metadata.num_rows

    for path in sorted(output_dir.rglob("*.csv")):
        chunks = pd.read_csv(
            path,
            sep=csv_separator,
            usecols=[0],
            dtype=str,
            chunksize=constants.CSV_CHUNK_ROWS.value,
        )
        for chunk in chunks:
            rows += len(chunk)

    return rows


def check_rows_written(
    rows_read: int, output_dir: str | Path, csv_separator: str = ","
) -> None:
    """Confere se as partições gravadas têm as mesmas linhas que as lidas.

    Args:
        rows_read: Linhas lidas da fonte.
        output_dir: Raiz das partições.
        csv_separator: Separador dos `.csv`.

    Raises:
        ValueError: Quando as partições têm mais ou menos linhas que as lidas.
    """
    rows_written = count_written_rows(output_dir, csv_separator)

    if rows_written != rows_read:
        raise ValueError(
            f"{rows_read} linhas lidas e {rows_written} gravadas em "
            f"{output_dir}."
        )

    log(f"{rows_read} linhas lidas e gravadas em {output_dir}")


def check_states_present(
    partition_dir: str | Path, partition_column: str = "sigla_uf"
) -> None:
    """Confere se a partição tem uma subpartição para cada uma das 27 UFs.

    Olha só os nomes das pastas, sem ler os arquivos. Pastas de valores fora
    das 27 UFs não contam.

    Args:
        partition_dir: Partição cujas subpastas são as UFs, como
            `ano=2026/mes=8`.
        partition_column: Coluna de partição da UF.

    Raises:
        ValueError: Quando falta a pasta de alguma UF.
    """
    partition_dir = Path(partition_dir)
    states = {
        path.name.split("=", 1)[1]
        for path in partition_dir.glob(f"{partition_column}=*")
    }
    missing = sorted(set(constants.STATES.value) - states)

    if missing:
        raise ValueError(
            f"{len(missing)} UF(s) sem partição em {partition_dir}: {missing}"
        )

    log(f"As 27 UFs têm partição em {partition_dir}")


def check_not_null(values: pd.Series) -> None:
    """Confere se a coluna não tem nulos.

    Args:
        values: Coluna a conferir.

    Raises:
        ValueError: Quando alguma linha é nula.
    """
    nulls = values.isna().sum()

    if nulls:
        raise ValueError(f"{nulls} linha(s) com `{values.name}` nulo.")


def check_pattern(values: pd.Series, pattern: str) -> None:
    """Confere se os valores da coluna casam inteiros com uma expressão regular.

    Nulos passam.

    Args:
        values: Coluna de texto a conferir.
        pattern: Expressão regular que o valor inteiro tem de casar.

    Raises:
        ValueError: Quando algum valor não nulo não casa com `pattern`.
    """
    is_valid = values.str.fullmatch(pattern, na=True)
    invalid = values[~is_valid]

    if not invalid.empty:
        raise ValueError(
            f"{len(invalid)} linha(s) com `{values.name}` fora do padrão "
            f"{pattern}: {_examples(invalid)}"
        )


def check_date_format(values: pd.Series, date_format: str) -> None:
    """Confere se os valores da coluna são datas no formato dado.

    Nulos passam.

    Args:
        values: Coluna de texto a conferir.
        date_format: Formato esperado, como `%Y-%m-%d`.

    Raises:
        ValueError: Quando algum valor não nulo não é data nesse formato.
    """
    dates = pd.to_datetime(values, format=date_format, errors="coerce")
    invalid = values[dates.isna() & values.notna()]

    if not invalid.empty:
        raise ValueError(
            f"{len(invalid)} linha(s) com `{values.name}` fora do formato "
            f"{date_format}: {_examples(invalid)}"
        )
