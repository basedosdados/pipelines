"""
Constant values for br_anatel_telefonia_movel.
"""

from pipelines.utils.metadata.domain import DateFormat, PartBdpro, YearMonth

DATASET_ID = "br_anatel_telefonia_movel"

# As 4 tabelas do dataset — ver README.md pro histórico de particularidades
# da fonte (revisão retroativa, granularidade UF/municipal só a partir de
# 2010, bug do cast de `acessos` em 2025, poll por geografia).
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
