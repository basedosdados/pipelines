"""Checagem de que as linhas de um arquivo estão contidas em outro."""

import pandas as pd

from pipelines.utils.utils import log


def check_rows_contained(rows: pd.DataFrame, container: pd.DataFrame) -> None:
    """Confere se todas as linhas de `rows` aparecem em `container`.

    Compara linha a linha, por todas as colunas de `rows`. Linhas de
    `container` que não estão em `rows` não entram na conta.

    Args:
        rows: Linhas que precisam estar em `container`.
        container: Dados que devem conter `rows`.

    Raises:
        ValueError: Quando `container` não tem alguma coluna de `rows`, ou
            quando alguma linha de `rows` não está em `container`. A mensagem
            traz as colunas ausentes ou o número de linhas faltantes.
    """

    missing_columns = set(rows.columns) - set(container.columns)

    if missing_columns:
        raise ValueError(f"Colunas ausentes: {sorted(missing_columns)}.")

    merged = rows.merge(
        container[list(rows.columns)].drop_duplicates(),
        how="left",
        indicator=True,
    )

    missing_rows = (merged["_merge"] == "left_only").sum()

    if missing_rows:
        raise ValueError(
            f"{missing_rows} de {len(rows)} linhas não estão contidas."
        )

    log(f"As {len(rows)} linhas estão contidas.")
