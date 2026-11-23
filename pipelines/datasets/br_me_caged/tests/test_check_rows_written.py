"""Testes da checagem de linhas gravadas do caged, sem rede nem Prefect."""

from pathlib import Path

import pytest

from pipelines.datasets.br_me_caged.utils import count_rows_read_by_month
from pipelines.utils.data_tests.rows import check_rows_written


def write_lines(path: Path, lines: list[str]) -> None:
    """Grava as linhas num arquivo, criando as pastas que faltarem."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def write_txt(input_dir: Path, year_month: str, rows: int) -> None:
    """Grava o `.txt` de um mês com cabeçalho, linhas de dado e uma linha em branco."""
    lines = ["competenciamov;uf;saldomovimentacao"]
    lines.extend(f"{year_month};35;1" for _ in range(rows))
    lines.append("")
    write_lines(input_dir / f"CAGEDMOV{year_month}.txt", lines)


def write_partition(
    output_dir: Path, year_month: str, state: str, rows: int
) -> None:
    """Grava o `data.csv` da partição de uma UF com cabeçalho e linhas de dado."""
    year = year_month[:4]
    month = int(year_month[-2:])
    lines = ["saldo_movimentacao"]
    lines.extend("1" for _ in range(rows))
    write_lines(
        output_dir
        / f"ano={year}"
        / f"mes={month}"
        / f"sigla_uf={state}"
        / "data.csv",
        lines,
    )


def test_rows_read_keyed_by_month_partition(tmp_path: Path) -> None:
    """Cada `.txt` vira a partição do mês, sem zero à esquerda no `mes`.

    A linha em branco no fim do `.txt` não conta.
    """
    write_txt(tmp_path, "202607", rows=2)
    write_txt(tmp_path, "202612", rows=3)

    assert count_rows_read_by_month(tmp_path) == {
        "ano=2026/mes=7": 2,
        "ano=2026/mes=12": 3,
    }


def test_complete_months_pass(tmp_path: Path) -> None:
    """Com todas as linhas gravadas em dois meses, não levanta nada.

    A contagem de um mês não se soma à do outro.
    """
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    write_txt(input_dir, "202607", rows=2)
    write_partition(output_dir, "202607", "SP", rows=2)
    write_txt(input_dir, "202608", rows=3)
    write_partition(output_dir, "202608", "SP", rows=2)
    write_partition(output_dir, "202608", "RJ", rows=1)

    check_rows_written(
        rows_read=count_rows_read_by_month(input_dir), output_dir=output_dir
    )


def test_missing_state_raises(tmp_path: Path) -> None:
    """Sem a partição de uma UF, levanta ValueError com o mês."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    write_txt(input_dir, "202608", rows=3)
    write_partition(output_dir, "202608", "SP", rows=2)
    rows_read = count_rows_read_by_month(input_dir)

    with pytest.raises(
        ValueError, match="ano=2026/mes=8: 3 linhas lidas e 2 gravadas"
    ):
        check_rows_written(rows_read=rows_read, output_dir=output_dir)


def test_only_divergent_month_is_reported(tmp_path: Path) -> None:
    """Com dois meses e só um com linhas perdidas, o erro cita só esse."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    write_txt(input_dir, "202607", rows=2)
    write_partition(output_dir, "202607", "SP", rows=2)
    write_txt(input_dir, "202608", rows=3)
    write_partition(output_dir, "202608", "SP", rows=2)
    rows_read = count_rows_read_by_month(input_dir)

    with pytest.raises(ValueError) as error:
        check_rows_written(rows_read=rows_read, output_dir=output_dir)

    assert "ano=2026/mes=8" in str(error.value)
    assert "ano=2026/mes=7" not in str(error.value)
