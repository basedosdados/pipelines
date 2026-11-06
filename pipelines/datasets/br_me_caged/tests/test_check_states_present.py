"""Testes de `check_states_present`, sem rede nem Prefect."""

from pathlib import Path

import pytest

from pipelines.datasets.br_me_caged.constants import (
    constants as caged_constants,
)
from pipelines.datasets.br_me_caged.tasks import check_states_present

STATES = [
    state
    for state in caged_constants.UF_DICT.value.values()
    if state != "UF não identificada"
]


def write_month(
    input_dir: Path, output_dir: Path, year_month: str, missing: list[str]
) -> None:
    """Grava o `.txt` de um mês e uma partição por UF, vazia nas que faltam.

    Como o `build_partitions`, grava a pasta de toda UF do `UF_DICT`; a UF que
    falta fica só com o cabeçalho.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / f"CAGEDMOV{year_month}.txt").write_text("uf\n")
    month = int(year_month[-2:])
    for state in caged_constants.UF_DICT.value.values():
        partition_dir = (
            output_dir
            / f"ano={year_month[:4]}"
            / f"mes={month}"
            / f"sigla_uf={state}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)
        rows = "" if state in missing else "1\n"
        (partition_dir / "data.csv").write_text("saldo_movimentacao\n" + rows)


def test_complete_month_passes(tmp_path: Path) -> None:
    """Com as 27 UFs com linhas, não levanta nada.

    A `UF não identificada` vazia não conta como falta.
    """
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    write_month(
        input_dir, output_dir, "202608", missing=["UF não identificada"]
    )

    check_states_present.fn(
        table_input_dir=input_dir, table_output_dir=output_dir
    )


def test_empty_state_raises(tmp_path: Path) -> None:
    """Com a partição de RR só com o cabeçalho, levanta citando RR."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    write_month(input_dir, output_dir, "202608", missing=["RR"])

    with pytest.raises(ValueError, match="RR"):
        check_states_present.fn(
            table_input_dir=input_dir, table_output_dir=output_dir
        )


def test_only_incomplete_month_is_reported(tmp_path: Path) -> None:
    """Com julho completo e agosto sem RR, o erro cita só agosto.

    As UFs de julho não contam como presentes em agosto.
    """
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    write_month(input_dir, output_dir, "202607", missing=[])
    write_month(input_dir, output_dir, "202608", missing=["RR"])

    with pytest.raises(ValueError) as error:
        check_states_present.fn(
            table_input_dir=input_dir, table_output_dir=output_dir
        )

    assert "202608" in str(error.value)
    assert "202607" not in str(error.value)


def test_state_list_has_27_states() -> None:
    """O `UF_DICT`, sem a `UF não identificada`, tem as 27 UFs."""
    assert len(STATES) == 27
