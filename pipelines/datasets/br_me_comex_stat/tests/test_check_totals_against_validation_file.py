"""Testes de `check_totals_against_validation_file`, sem rede nem Prefect."""

import pandas as pd
import pytest

from pipelines.datasets.br_me_comex_stat.utils import (
    check_totals_against_validation_file,
)


def test_empty_file_raises(tmp_path) -> None:
    """Arquivo só com cabeçalho reprova antes de baixar o arquivo do MDIC."""
    dataframe = pd.DataFrame({"ano": [], "valor_fob_dolar": []})

    with pytest.raises(ValueError, match="no data rows"):
        check_totals_against_validation_file(
            filename="IMP_2026.csv",
            dataframe=dataframe,
            table_type="ncm",
            path=f"{tmp_path}/",
        )
