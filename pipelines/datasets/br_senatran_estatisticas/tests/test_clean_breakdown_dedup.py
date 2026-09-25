"""A fonte repete a chave com variantes de espaço; `clean_breakdown` tem que somar.

Sem isso `dbt_utils.unique_combination_of_columns` reprova a tabela inteira —
foi o que aconteceria com `municipio_cep` (38 chaves) e `municipio_combustivel`
(7 chaves).
"""

import polars as pl
import pytest

from pipelines.datasets.br_senatran_estatisticas.breakdowns import (
    LAYOUTS,
    clean_breakdown,
)

IBGE = pl.DataFrame(
    {
        "nome": ["Goiânia", "Anápolis"],
        "id_municipio": ["5208707", "5201108"],
        "sigla_uf": ["GO", "GO"],
    }
)


def _bruto(linhas: list[tuple[str, str, str, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "nome_uf": [x[0] for x in linhas],
            "nome_denatran": [x[1] for x in linhas],
            "combustivel": [x[2] for x in linhas],
            "quantidade": [x[3] for x in linhas],
        }
    )


LAYOUT = LAYOUTS["municipio_combustivel"]


def test_soma_variantes_de_espaco_da_mesma_dimensao():
    """O caso real: 'GASOLINA' e 'GASOLINA ' viram a mesma chave após o strip."""
    final, _ = clean_breakdown(
        _bruto(
            [
                ("GOIAS", "GOIANIA", "GASOLINA", "9377"),
                ("GOIAS", "GOIANIA", "GASOLINA ", "1"),
                ("GOIAS", "GOIANIA", "ALCOOL", "42"),
            ]
        ),
        LAYOUT,
        2026,
        7,
        IBGE,
    )

    chave = ["ano", "mes", "id_municipio", "combustivel"]
    assert final.group_by(chave).len().filter(pl.col("len") > 1).height == 0

    gasolina = final.filter(pl.col("combustivel") == "GASOLINA")
    assert gasolina.height == 1
    assert gasolina.get_column("quantidade").item() == 9378  # 9377 + 1
    # o total do município tem que sobreviver à soma
    assert final.get_column("quantidade").sum() == 9420


def test_nao_junta_municipios_diferentes():
    final, _ = clean_breakdown(
        _bruto(
            [
                ("GOIAS", "GOIANIA", "GASOLINA", "10"),
                ("GOIAS", "ANAPOLIS", "GASOLINA", "20"),
            ]
        ),
        LAYOUT,
        2026,
        7,
        IBGE,
    )
    assert final.height == 2
    assert set(final.get_column("id_municipio").to_list()) == {
        "5208707",
        "5201108",
    }


def test_quantidade_ausente_continua_ausente():
    """`sum` devolveria 0 para um grupo todo nulo; NULL não é zero veículos."""
    final, _ = clean_breakdown(
        _bruto([("GOIAS", "GOIANIA", "GASOLINA", "sem informação")]),
        LAYOUT,
        2026,
        7,
        IBGE,
    )
    assert final.height == 1
    assert final.get_column("quantidade").item() is None


@pytest.mark.parametrize("dim", ["0", "0    ", "  0"])
def test_dimensao_e_aparada(dim):
    final, _ = clean_breakdown(
        _bruto([("GOIAS", "GOIANIA", dim, "5")]), LAYOUT, 2026, 7, IBGE
    )
    assert final.get_column("combustivel").item() == "0"
