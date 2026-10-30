"""Testes de `check_year_matches_filename`, sem rede nem Prefect."""

import pandas as pd
import pytest

from pipelines.datasets.br_me_comex_stat.utils import (
    check_year_matches_filename,
)


def test_same_year_passes() -> None:
    """Com todas as linhas no ano do arquivo, não levanta nada."""
    dataframe = pd.DataFrame({"ano": [2026, 2026], "mes": [1, 2]})

    check_year_matches_filename(dataframe=dataframe, filename="EXP_2026.csv")


def test_municipality_filename_passes() -> None:
    """O ano sai do nome do arquivo de município, que termina em `_MUN`."""
    dataframe = pd.DataFrame({"ano": [2026]})

    check_year_matches_filename(
        dataframe=dataframe, filename="IMP_2026_MUN.csv"
    )


def test_other_year_raises() -> None:
    """Com uma linha de outro ano, levanta ValueError citando esse ano."""
    dataframe = pd.DataFrame({"ano": [2026, 2025]})

    with pytest.raises(ValueError, match="2025"):
        check_year_matches_filename(
            dataframe=dataframe, filename="EXP_2026.csv"
        )


def test_null_year_raises() -> None:
    """Linha sem `ano` também conta como fora do ano do arquivo."""
    dataframe = pd.DataFrame({"ano": [2026, None]})

    with pytest.raises(ValueError, match="1 linha"):
        check_year_matches_filename(
            dataframe=dataframe, filename="EXP_2026.csv"
        )
