"""
Constant values for br_anatel_banda_larga_fixa.
"""

from pipelines.utils.metadata.domain import DateFormat, PartBdpro, YearMonth

DATASET_ID = "br_anatel_banda_larga_fixa"

# As 4 tabelas do dataset — ver README.md pro histórico da migração e as
# particularidades de cada uma (revisão histórica do microdados, apagão de
# 2023-2025 nas densidades).
MICRODADOS_TABLE_ID = "microdados"
DENSIDADE_BRASIL_TABLE_ID = "densidade_brasil"
DENSIDADE_UF_TABLE_ID = "densidade_uf"
DENSIDADE_MUNICIPIO_TABLE_ID = "densidade_municipio"

# Mesma coverage pras 4 tabelas — todas mensais (ano/mes), sem diferença de
# tier.
COVERAGE = PartBdpro(
    date_column=YearMonth(year="ano", month="mes"),
    date_format=DateFormat.YEAR_MONTH,
)
