"""Testes da regra de competência do teste 3, sem rede nem Prefect."""

from pathlib import Path

import pandas as pd
import pytest

from pipelines.datasets.br_me_caged.tasks import check_competence_month
from pipelines.datasets.br_me_caged.utils import count_competence_violations


def test_movimentacao_counts_other_months_and_nulls() -> None:
    """Na `movimentacao`, conta a competência de outro mês e a nula."""
    competences = pd.DataFrame(
        {"competenciamov": ["202608", "202607", "202609", None]}
    )

    violations = count_competence_violations(
        competences, "microdados_movimentacao", "202608"
    )

    assert violations == 3


def test_fora_prazo_counts_same_and_later_months() -> None:
    """Na `_fora_prazo`, conta a competência igual ou posterior ao arquivo."""
    competences = pd.DataFrame(
        {"competenciamov": ["202607", "202001", "202608", "202609"]}
    )

    violations = count_competence_violations(
        competences, "microdados_movimentacao_fora_prazo", "202608"
    )

    assert violations == 2


def test_excluida_counts_later_competence_or_declaration() -> None:
    """Na `_excluida`, conta competência ou declaração posterior ao arquivo."""
    competences = pd.DataFrame(
        {
            "competenciamov": ["202608", "202607", "202609", "202607"],
            "competenciadec": ["202608", "202608", "202608", "202609"],
        }
    )

    violations = count_competence_violations(
        competences, "microdados_movimentacao_excluida", "202608"
    )

    assert violations == 2


def test_competence_written_as_float_is_compared_by_month() -> None:
    """Competência gravada como `202608.0` vale como `202608`."""
    competences = pd.DataFrame({"competenciamov": ["202608.0"]})

    violations = count_competence_violations(
        competences, "microdados_movimentacao", "202608"
    )

    assert violations == 0


def test_unknown_table_raises() -> None:
    """Tabela sem regra de competência levanta ValueError."""
    competences = pd.DataFrame({"competenciamov": ["202608"]})

    with pytest.raises(ValueError, match="microdados_antigos"):
        count_competence_violations(
            competences, "microdados_antigos", "202608"
        )


def write_month(
    input_dir: Path,
    output_dir: Path,
    competences_by_state: dict[str, list[int]],
) -> None:
    """Grava o `.txt` de 2026-08 e uma partição por UF com as competências."""
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "CAGEDMOV202608.txt").write_text("competenciamov\n")
    for state, competences in competences_by_state.items():
        partition_dir = output_dir / "ano=2026" / "mes=8" / f"sigla_uf={state}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {"competenciamov": competences, "saldo_movimentacao": 1}
        ).to_csv(partition_dir / "data.csv", index=False)


def test_task_passes_when_every_partition_follows_the_rule(
    tmp_path: Path,
) -> None:
    """Com a competência certa em todas as partições, não levanta nada."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    write_month(
        input_dir,
        output_dir,
        {"SP": [202608, 202608], "RJ": [202608]},
    )

    check_competence_month.fn(
        table_id="microdados_movimentacao",
        table_input_dir=input_dir,
        table_output_dir=output_dir,
    )


def test_task_raises_with_month_from_another_file(tmp_path: Path) -> None:
    """Com uma linha de julho no arquivo de agosto, levanta com o mês."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    write_month(
        input_dir,
        output_dir,
        {"SP": [202608, 202608], "RJ": [202607]},
    )

    with pytest.raises(ValueError, match="202608, com 1 linhas"):
        check_competence_month.fn(
            table_id="microdados_movimentacao",
            table_input_dir=input_dir,
            table_output_dir=output_dir,
        )
