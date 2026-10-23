"""Testes de `check_rows_written` e `count_written_rows`.

Sem rede nem Prefect.
"""

from pathlib import Path

import pandas as pd
import pytest

from pipelines.shared.testes_dados.utils import (
    check_rows_written,
    count_written_rows,
)


def write_csv(path: Path, rows: int) -> None:
    """Grava um `.csv` com cabeçalho e `rows` linhas, criando as pastas."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"ano": ["2026"] * rows, "valor": ["1"] * rows}).to_csv(
        path, index=False
    )


def write_parquet(path: Path, rows: int) -> None:
    """Grava um `.parquet` com `rows` linhas, criando as pastas."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"ano": ["2026"] * rows}).to_parquet(path, index=False)


def test_counts_csv_and_parquet_partitions(tmp_path: Path) -> None:
    """Soma as linhas dos `.csv` e `.parquet` de todas as partições.

    O cabeçalho dos `.csv` não conta.
    """
    write_csv(tmp_path / "ano=2026" / "mes=7" / "data.csv", rows=2)
    write_csv(tmp_path / "ano=2026" / "mes=8" / "data.csv", rows=3)
    write_parquet(tmp_path / "ano=2025" / "mes=12" / "data.parquet", rows=4)

    assert count_written_rows(tmp_path) == 9


def test_complete_partitions_pass(tmp_path: Path) -> None:
    """Com todas as linhas lidas gravadas, não levanta nada."""
    write_csv(tmp_path / "ano=2026" / "mes=8" / "sigla_uf=SP" / "data.csv", 2)
    write_csv(tmp_path / "ano=2026" / "mes=8" / "sigla_uf=RJ" / "data.csv", 3)

    check_rows_written(rows_read=5, output_dir=tmp_path)


def test_missing_partition_raises(tmp_path: Path) -> None:
    """Sem a partição de uma UF, levanta ValueError com as duas contagens."""
    write_csv(tmp_path / "ano=2026" / "mes=8" / "sigla_uf=SP" / "data.csv", 2)

    with pytest.raises(ValueError, match="5 linhas lidas e 2 gravadas"):
        check_rows_written(rows_read=5, output_dir=tmp_path)


def test_extra_rows_raise(tmp_path: Path) -> None:
    """Com mais linhas gravadas que lidas, também levanta ValueError."""
    write_csv(tmp_path / "ano=2026" / "mes=8" / "sigla_uf=SP" / "data.csv", 5)

    with pytest.raises(ValueError, match="2 linhas lidas e 5 gravadas"):
        check_rows_written(rows_read=2, output_dir=tmp_path)
