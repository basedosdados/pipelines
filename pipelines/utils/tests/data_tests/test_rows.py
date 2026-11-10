"""Testes de `count_written_rows` e `check_rows_written`, sem rede nem Prefect."""

from pathlib import Path

import pandas as pd
import pytest

from pipelines.utils.data_tests.rows import (
    check_rows_written,
    count_written_rows,
)


def write_csv(path: Path, rows: int) -> None:
    """Grava um `.csv` com cabeçalho e `rows` linhas, criando as pastas."""
    path.parent.mkdir(parents=True, exist_ok=True)

    df_rows = ["2026"] * rows

    df = pd.DataFrame(df_rows)

    df.to_csv(path, index=False)


def write_parquet(path: Path, rows: int) -> None:
    """Grava um `.parquet` com `rows` linhas, criando as pastas."""

    path.parent.mkdir(parents=True, exist_ok=True)

    df_rows = ["2026"] * rows

    df = pd.DataFrame(df_rows)

    df.to_parquet(path, index=False)


def test_counts_csv_partitions(tmp_path: Path) -> None:
    """Soma as linhas dos `.csv` de todas as partições, sem os cabeçalhos."""

    write_csv(path=tmp_path / "ano=2026" / "mes=7" / "data.csv", rows=2)
    write_csv(path=tmp_path / "ano=2026" / "mes=8" / "data.csv", rows=3)

    assert count_written_rows(tmp_path) == 5


def test_counts_parquet_partitions(tmp_path: Path) -> None:
    """Soma as linhas dos `.parquet` de todas as partições."""

    write_parquet(
        path=tmp_path / "ano=2026" / "mes=7" / "data.parquet", rows=2
    )
    write_parquet(
        path=tmp_path / "ano=2026" / "mes=8" / "data.parquet", rows=3
    )

    assert count_written_rows(tmp_path) == 5


def test_value_with_line_break_counts_once(tmp_path: Path) -> None:
    """Um valor com quebra de linha dentro conta como uma linha só."""
    Path.mkdir(tmp_path / "ano=2026", exist_ok=True)

    df_rows = [["linha1\nlinha2"], ["linha3"]]

    df = pd.DataFrame(df_rows)

    df.to_csv(tmp_path / "ano=2026" / "data.csv", index=False)

    assert count_written_rows(tmp_path) == 2


def test_missing_dir_counts_zero(tmp_path: Path) -> None:
    """Uma pasta que não existe conta zero linhas."""

    assert count_written_rows(tmp_path / "ano=2026") == 0


def test_total_matches_passes(tmp_path: Path) -> None:
    """Com todas as linhas lidas gravadas, não levanta nada."""

    path_one = tmp_path / "ano=2026" / "mes=8" / "sigla_uf=SP" / "data.csv"
    path_two = tmp_path / "ano=2026" / "mes=8" / "sigla_uf=RJ" / "data.csv"

    write_csv(path_one, rows=2)
    write_csv(path_two, rows=3)

    check_rows_written(rows_read=5, output_dir=tmp_path)


def test_missing_rows_raise(tmp_path: Path) -> None:
    """Com menos linhas gravadas que lidas, levanta ValueError com as duas contagens."""

    path = tmp_path / "ano=2026" / "mes=8" / "sigla_uf=SP" / "data.csv"

    write_csv(path, rows=2)

    with pytest.raises(ValueError, match="5 linhas lidas e 2 gravadas"):
        check_rows_written(rows_read=5, output_dir=tmp_path)


def test_extra_rows_raise(tmp_path: Path) -> None:
    """Com mais linhas gravadas que lidas, também levanta ValueError."""
    path = tmp_path / "ano=2026" / "mes=8" / "sigla_uf=SP" / "data.csv"

    write_csv(path, rows=5)

    with pytest.raises(ValueError, match="2 linhas lidas e 5 gravadas"):
        check_rows_written(rows_read=2, output_dir=tmp_path)


def test_partitions_match_pass(tmp_path: Path) -> None:
    """Com um mapa e todas as partições completas, não levanta nada."""

    path_one = tmp_path / "ano=2026" / "mes=7" / "sigla_uf=SP" / "data.csv"
    path_two = tmp_path / "ano=2026" / "mes=8" / "sigla_uf=RJ" / "data.csv"

    write_csv(path_one, rows=2)
    write_csv(path_two, rows=3)

    check_rows_written(
        output_dir=tmp_path,
        rows_read={"ano=2026/mes=7": 2, "ano=2026/mes=8": 3},
    )


def test_only_divergent_partition_is_reported(tmp_path: Path) -> None:
    """Com duas partições e só uma com linhas perdidas, o erro cita só essa."""

    path_one = tmp_path / "ano=2026" / "mes=7" / "sigla_uf=SP" / "data.csv"
    path_two = tmp_path / "ano=2026" / "mes=8" / "sigla_uf=RJ" / "data.csv"

    write_csv(path_one, rows=2)
    write_csv(path_two, rows=2)

    with pytest.raises(ValueError) as error:
        check_rows_written(
            output_dir=tmp_path,
            rows_read={"ano=2026/mes=7": 2, "ano=2026/mes=8": 3},
        )

    assert "ano=2026/mes=8" in str(error.value)
    assert "ano=2026/mes=7" not in str(error.value)


def test_missing_partition_raises(tmp_path: Path) -> None:
    """Uma partição do mapa sem pasta gravada conta zero e entra no erro."""

    path = tmp_path / "ano=2026" / "mes=7" / "sigla_uf=RJ" / "data.csv"

    write_csv(path, rows=2)

    with pytest.raises(
        ValueError, match="ano=2026/mes=8: 3 linhas lidas e 0 gravadas"
    ):
        check_rows_written(
            output_dir=tmp_path,
            rows_read={"ano=2026/mes=7": 2, "ano=2026/mes=8": 3},
        )


def test_partition_outside_map_is_ignored(tmp_path: Path) -> None:
    """Uma partição gravada que não está no mapa não entra na conta."""

    path_one = tmp_path / "ano=2026" / "mes=7" / "sigla_uf=SP" / "data.csv"
    path_two = tmp_path / "ano=2026" / "mes=8" / "sigla_uf=RJ" / "data.csv"

    write_csv(path_one, rows=2)
    write_csv(path_two, rows=3)

    check_rows_written(rows_read={"ano=2026/mes=8": 3}, output_dir=tmp_path)
