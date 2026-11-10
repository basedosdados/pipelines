"""Checagens das linhas gravadas nas partições, antes do upload."""

from collections.abc import Mapping
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from pipelines.utils.data_tests.constants import constants
from pipelines.utils.utils import log


def count_written_rows(output_dir: str | Path) -> int:
    """Conta as linhas gravadas nos `.csv` e `.parquet` de um diretório.

    Do parquet, usa a contagem dos metadados, sem ler o dado.

    Args:
        output_dir: Raiz das partições, ou uma partição dentro dela.

    Returns:
        O total de linhas, sem os cabeçalhos dos `.csv`. Zero quando o
        diretório não existe ou não tem arquivos.
    """

    output_dir_as_path = Path(output_dir)

    sum_of_rows = 0

    for parquet_file in output_dir_as_path.rglob("*.parquet"):
        sum_of_rows += pq.ParquetFile(parquet_file).metadata.num_rows

    for csv_file in output_dir_as_path.rglob("*.csv"):
        for df in pd.read_csv(
            csv_file,
            chunksize=constants.CSV_CHUNK_ROWS.value,
            usecols=[0],
            dtype=str,
        ):
            sum_of_rows += len(df)

    return sum_of_rows


def check_rows_written(
    rows_read: int | Mapping[str, int], output_dir: str | Path
) -> None:
    """Confere se as partições gravadas têm as mesmas linhas que as lidas.

    Com um inteiro, compara o total de `output_dir`. Com um mapa, compara cada
    partição do mapa com as linhas gravadas dentro dela; partições gravadas
    que não estão no mapa não entram na conta.

    Args:
        rows_read: Linhas lidas da fonte, no total ou por partição. No mapa, a
            chave é o caminho da partição relativo a `output_dir`, como
            `ano=2026/mes=9`.
        output_dir: Raiz das partições.

    Raises:
        ValueError: Quando o total, ou alguma partição do mapa, tem mais ou
            menos linhas que as lidas. Com um mapa, a mensagem lista cada
            partição divergente com as duas contagens.
    """

    if isinstance(rows_read, int):
        rows_written = count_written_rows(output_dir)

        if rows_read != rows_written:
            raise ValueError(
                f"{rows_read} linhas lidas e {rows_written} gravadas em {output_dir}."
            )

        log(f"{rows_written} linhas gravadas em {output_dir}")
    else:
        divergences = []

        for partition, read_rows in rows_read.items():
            path = Path(output_dir) / partition

            rows_written = count_written_rows(path)

            if rows_written != read_rows:
                divergences.append(
                    f"{partition}: {read_rows} linhas lidas e {rows_written} gravadas"
                )

        if divergences:
            raise ValueError("\n".join(divergences))

        log(f"{sum(rows_read.values())} linhas gravadas em {output_dir}")
