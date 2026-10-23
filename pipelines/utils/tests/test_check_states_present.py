"""Testes de `check_states_present`, sem rede nem Prefect."""

from pathlib import Path

import pytest

from pipelines.shared.testes_dados.constants import constants
from pipelines.shared.testes_dados.utils import check_states_present

STATES = constants.STATES.value


def write_partitions(partition_dir: Path, states: list[str]) -> None:
    """Cria a pasta `sigla_uf=` de cada UF dentro da partição."""
    for state in states:
        (partition_dir / f"sigla_uf={state}").mkdir(parents=True)


def without(*missing: str) -> list[str]:
    """Devolve as 27 UFs menos as indicadas."""
    return [state for state in STATES if state not in missing]


def test_state_list_has_27_states() -> None:
    """A lista de UFs tem 27 siglas distintas."""
    assert len(set(STATES)) == 27


def test_all_partitions_pass(tmp_path: Path) -> None:
    """Com as 27 pastas, não levanta nada, mesmo com uma pasta a mais.

    A `UF não identificada` não é UF e não atrapalha.
    """
    states = list(STATES)
    states.append("UF não identificada")
    write_partitions(tmp_path, states)

    check_states_present(tmp_path)


def test_missing_partition_raises(tmp_path: Path) -> None:
    """Sem a pasta de RR, levanta ValueError citando RR."""
    write_partitions(tmp_path, without("RR"))

    with pytest.raises(ValueError, match=r"\['RR'\]"):
        check_states_present(tmp_path)


def test_lists_every_missing_state_in_order(tmp_path: Path) -> None:
    """Com várias UFs faltando, cita todas, em ordem alfabética."""
    write_partitions(tmp_path, without("SP", "AC", "RR"))

    with pytest.raises(ValueError, match=r"3 UF\(s\) .*\['AC', 'RR', 'SP'\]"):
        check_states_present(tmp_path)


def test_only_given_partition_is_checked(tmp_path: Path) -> None:
    """Com julho completo e agosto sem RR, só agosto levanta.

    As UFs de julho não contam como presentes em agosto.
    """
    july = tmp_path / "ano=2026" / "mes=7"
    august = tmp_path / "ano=2026" / "mes=8"
    write_partitions(july, STATES)
    write_partitions(august, without("RR"))

    check_states_present(july)
    with pytest.raises(ValueError, match="mes=8"):
        check_states_present(august)


def test_other_partition_column(tmp_path: Path) -> None:
    """Com `partition_column="uf"`, lê as pastas `uf=`."""
    for state in STATES:
        (tmp_path / f"uf={state}").mkdir()

    check_states_present(tmp_path, partition_column="uf")
