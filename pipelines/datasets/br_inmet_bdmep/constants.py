"""
Constant values for br_inmet_bdmep.
"""

from pipelines.utils.metadata.domain import DateFormat, DateOnly, PartBdpro

DATASET_ID = "br_inmet_bdmep"

# Única tabela com flow próprio — `estacao` é um model dbt derivado de
# `microdados`, não tem crawler/flow separado.
MICRODADOS_TABLE_ID = "microdados"

COVERAGE = PartBdpro(
    date_column=DateOnly(col="data"),
    date_format=DateFormat.YEAR_MD,
)
