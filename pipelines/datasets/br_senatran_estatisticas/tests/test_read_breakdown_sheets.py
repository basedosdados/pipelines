"""`read_breakdown` lê todas as abas, mas tem que ignorar as abas vazias.

Os arquivos de 2014 trazem `Plan2`, `Plan3` e `Plan4` vazias ao lado da aba de
dados; sem o filtro, todo mês de 2014 falhava na checagem de colunas.
"""

import pandas as pd
import pytest

from pipelines.datasets.br_senatran_estatisticas.breakdowns import (
    LAYOUTS,
    read_breakdown,
)

LAYOUT = LAYOUTS["municipio_cor"]


def _dados() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "UF": ["GOIAS", "GOIAS"],
            "Município": ["GOIANIA", "ANAPOLIS"],
            "Cor": ["BRANCA", "PRATA"],
            "Qtd": ["10", "20"],
        }
    )


def test_ignora_abas_vazias(tmp_path):
    path = tmp_path / "frota_cor.xlsx"
    with pd.ExcelWriter(path) as writer:
        _dados().to_excel(writer, sheet_name="Plan1", index=False)
        pd.DataFrame().to_excel(writer, sheet_name="Plan2", index=False)
        pd.DataFrame().to_excel(writer, sheet_name="Plan3", index=False)

    frame = read_breakdown(path, LAYOUT)

    assert frame.height == 2
    assert frame.columns == ["nome_uf", "nome_denatran", "cor", "quantidade"]


def test_aba_com_dado_e_colunas_de_menos_continua_erro(tmp_path):
    path = tmp_path / "frota_cor.xlsx"
    with pd.ExcelWriter(path) as writer:
        _dados().iloc[:, :2].to_excel(writer, sheet_name="Plan1", index=False)

    with pytest.raises(ValueError, match="esperava >= 4 colunas"):
        read_breakdown(path, LAYOUT)


def test_arquivo_so_com_abas_vazias_e_erro(tmp_path):
    path = tmp_path / "frota_cor.xlsx"
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame().to_excel(writer, sheet_name="Plan1", index=False)

    with pytest.raises(ValueError, match="Nenhuma aba com dados"):
        read_breakdown(path, LAYOUT)
