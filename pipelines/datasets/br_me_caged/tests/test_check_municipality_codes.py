"""Testes da checagem de municípios do teste 5, sem rede nem Prefect."""

from pathlib import Path

import pandas as pd
import pytest

from pipelines.datasets.br_me_caged.tasks import check_municipality_codes
from pipelines.datasets.br_me_caged.utils import count_unknown_municipalities

DIRECTORY_CODES = {"355030", "330455"}


def test_counts_unknown_and_null_codes() -> None:
    """Conta o código fora do diretório e o nulo, e ignora o `999999`."""
    codes = pd.Series(["355030", "330455", "999999", "123456", None])

    unknown = count_unknown_municipalities(codes, DIRECTORY_CODES)

    assert unknown == 2


def test_code_written_as_float_is_compared_by_code() -> None:
    """Código gravado como `355030.0` vale como `355030`."""
    codes = pd.Series(["355030.0"])

    unknown = count_unknown_municipalities(codes, DIRECTORY_CODES)

    assert unknown == 0


def write_month(input_dir: Path, output_dir: Path, codes: list[int]) -> None:
    """Grava o `.txt` de 2026-08 e a partição de SP com os códigos."""
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "CAGEDMOV202608.txt").write_text("municipio\n")
    partition_dir = output_dir / "ano=2026" / "mes=8" / "sigla_uf=SP"
    partition_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"id_municipio": codes, "saldo_movimentacao": 1}).to_csv(
        partition_dir / "data.csv", index=False
    )


def test_task_passes_at_the_limit(tmp_path: Path) -> None:
    """Com 1 código fora do diretório em 1.000 linhas (0,1%), não levanta."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    write_month(input_dir, output_dir, [355030] * 999 + [123456])

    check_municipality_codes.fn(
        table_input_dir=input_dir,
        table_output_dir=output_dir,
        directory_codes=DIRECTORY_CODES,
    )


def test_task_raises_above_the_limit(tmp_path: Path) -> None:
    """Com 2 códigos fora do diretório em 1.000 linhas, levanta com o mês."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    write_month(input_dir, output_dir, [355030] * 998 + [123456, 654321])

    with pytest.raises(ValueError, match="202608, com 2 de 1000"):
        check_municipality_codes.fn(
            table_input_dir=input_dir,
            table_output_dir=output_dir,
            directory_codes=DIRECTORY_CODES,
        )


def test_task_ignores_the_ignored_municipality_code(tmp_path: Path) -> None:
    """O `999999` não conta para o limite, mesmo sendo muitas linhas."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    write_month(input_dir, output_dir, [355030] * 10 + [999999] * 10)

    check_municipality_codes.fn(
        table_input_dir=input_dir,
        table_output_dir=output_dir,
        directory_codes=DIRECTORY_CODES,
    )
