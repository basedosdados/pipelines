"""Constantes do pipeline br_bcb_ifdata."""

from enum import Enum


class constants(Enum):
    DATASET_ID = "br_bcb_ifdata"

    # A ordem importa: `coluna` e `instituicao` são referenciadas pelos testes
    # de `relatorio`, e `dicionario` pelo custom_dictionary_coverage das duas
    # primeiras. Todas são construídas antes de qualquer teste rodar.
    ALL_TABLES = ["dicionario", "instituicao", "coluna", "relatorio"]

    # Tabela usada para o poll da fonte. `relatorio` é a que carrega a
    # cobertura temporal completa.
    POLL_TABLE = "relatorio"
