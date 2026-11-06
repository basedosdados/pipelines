"""A fonte grava UTF-8 lido como Latin-1 nas planilhas de 2014 a 2019.

`clean_breakdown` tem que reparar antes de somar, senão `NÃ£o Identificado` e
`Não Identificado` no mesmo mês viram duas categorias.
"""

import polars as pl
import pytest

from pipelines.datasets.br_senatran_estatisticas.breakdowns import (
    LAYOUTS,
    clean_breakdown,
    fix_mojibake,
)

IBGE = pl.DataFrame(
    {"nome": ["Goiânia"], "id_municipio": ["5208707"], "sigla_uf": ["GO"]}
)


@pytest.mark.parametrize(
    ("bruto", "esperado"),
    [
        ("NÃ£o Identificado", "Não Identificado"),
        ("AlienaÃ§Ã£o FiduciÃ¡ria", "Alienação Fiduciária"),
        ("Reserva de DomÃ\xadnio", "Reserva de Domínio"),
        ("Sem Informação", "Sem Informação"),
        ("BRANCA", "BRANCA"),
        (None, None),
    ],
)
def test_fix_mojibake(bruto, esperado):
    assert fix_mojibake(bruto) == esperado


def test_texto_que_nao_reverte_fica_intacto():
    assert fix_mojibake("Ã sozinho") == "Ã sozinho"


def test_grafias_do_mesmo_rotulo_somam_numa_chave():
    bruto = pl.DataFrame(
        {
            "nome_uf": ["GOIAS", "GOIAS"],
            "nome_denatran": ["GOIANIA", "GOIANIA"],
            "cor": ["NÃ£o Identificado", "Não Identificado"],
            "quantidade": ["3", "4"],
        }
    )
    final, _ = clean_breakdown(bruto, LAYOUTS["municipio_cor"], 2016, 3, IBGE)
    assert final["cor"].to_list() == ["Não Identificado"]
    assert final["quantidade"].to_list() == [7]
