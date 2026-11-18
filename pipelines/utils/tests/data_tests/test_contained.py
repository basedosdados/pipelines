"""Testes de `check_rows_contained`, sem rede nem Prefect."""

import pandas as pd
import pytest

from pipelines.utils.data_tests.contained import check_rows_contained


def test_passes_when_all_rows_are_contained() -> None:
    """Passa quando todas as linhas estão no container, que pode ter mais."""

    rows = pd.DataFrame({"id": ["1", "2"], "valor": ["a", "b"]})
    container = pd.DataFrame({"id": ["1", "2", "3"], "valor": ["a", "b", "c"]})

    check_rows_contained(rows=rows, container=container)


def test_fails_when_a_row_is_missing() -> None:
    """Falha quando alguma linha não está no container."""

    rows = pd.DataFrame({"id": ["1", "4"], "valor": ["a", "d"]})
    container = pd.DataFrame({"id": ["1", "2"], "valor": ["a", "b"]})

    with pytest.raises(ValueError, match="1 de 2 linhas"):
        check_rows_contained(rows=rows, container=container)


def test_fails_when_a_value_differs() -> None:
    """Falha quando a chave existe, mas outra coluna da linha mudou."""

    rows = pd.DataFrame({"id": ["1"], "valor": ["novo"]})
    container = pd.DataFrame({"id": ["1"], "valor": ["antigo"]})

    with pytest.raises(ValueError, match="1 de 1 linhas"):
        check_rows_contained(rows=rows, container=container)


def test_duplicates_in_container_do_not_count_twice() -> None:
    """Linhas repetidas no container não duplicam o resultado."""

    rows = pd.DataFrame({"id": ["1"]})
    container = pd.DataFrame({"id": ["1", "1"]})

    check_rows_contained(rows=rows, container=container)


def test_empty_values_match() -> None:
    """Valores vazios dos dois lados contam como iguais."""

    rows = pd.DataFrame({"id": ["1"], "valor": [None]})
    container = pd.DataFrame({"id": ["1"], "valor": [None]})

    check_rows_contained(rows=rows, container=container)


def test_fails_when_container_lacks_a_column() -> None:
    """Falha quando o container não tem alguma coluna das linhas."""

    rows = pd.DataFrame({"id": ["1"], "valor": ["a"]})
    container = pd.DataFrame({"id": ["1"]})

    with pytest.raises(ValueError, match="Colunas ausentes"):
        check_rows_contained(rows=rows, container=container)
