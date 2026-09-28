"""Testes de `resolve_years`, sem rede nem backend."""

import pytest

from pipelines.datasets.br_ibge_ppm.utils import resolve_years


def test_no_coverage_starts_at_first_year() -> None:
    """Sem cobertura registrada, carrega do primeiro ano da tabela até a fonte.

    `producao_aquicultura` começa em 2013, o que deixa a faixa curta.
    """
    anos = resolve_years(
        table_id="producao_aquicultura",
        backfill_years=None,
        source_max_date="2024",
        coverage_max_year=None,
    )

    assert anos[0] == "2013"
    assert anos[-1] == "2024"
    assert len(anos) == 12


def test_lagging_coverage_loads_missing_years() -> None:
    """Com a cobertura em 2022 e a fonte em 2024, carrega 2023 e 2024."""
    anos = resolve_years(
        table_id="efetivo_rebanhos",
        backfill_years=None,
        source_max_date="2024",
        coverage_max_year="2022",
    )

    assert anos == ["2023", "2024"]


def test_current_coverage_loads_last_year() -> None:
    """Com a cobertura igual à fonte, carrega o último ano.

    Só chega aqui com `force_run`, porque o poll barraria antes.
    """
    anos = resolve_years(
        table_id="efetivo_rebanhos",
        backfill_years=None,
        source_max_date="2024",
        coverage_max_year="2024",
    )

    assert anos == ["2024"]


def test_coverage_ahead_of_source_loads_last_year() -> None:
    """Com o registro à frente da fonte, carrega o último ano publicado."""
    anos = resolve_years(
        table_id="efetivo_rebanhos",
        backfill_years=None,
        source_max_date="2024",
        coverage_max_year="2025",
    )

    assert anos == ["2024"]


def test_backfill_ignores_coverage() -> None:
    """Com backfill, os anos saem em ordem e a cobertura não entra na conta.

    Pela cobertura, a execução carregaria só 2024.
    """
    anos = resolve_years(
        table_id="efetivo_rebanhos",
        backfill_years=["2022", "2020", "2022"],
        source_max_date="2024",
        coverage_max_year="2023",
    )

    assert anos == ["2020", "2022"]


@pytest.mark.parametrize("ano", ["1973", "2025"])
def test_backfill_out_of_range_raises(ano: str) -> None:
    """Um ano de backfill fora do que a fonte publica levanta ValueError.

    `efetivo_rebanhos` vai de 1974 ao último ano publicado.
    """
    with pytest.raises(ValueError, match="fora"):
        resolve_years(
            table_id="efetivo_rebanhos",
            backfill_years=[ano],
            source_max_date="2024",
            coverage_max_year=None,
        )
