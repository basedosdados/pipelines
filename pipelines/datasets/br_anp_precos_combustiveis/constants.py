"""
Constant values for br_anp_precos_combustiveis.
"""

from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
)

DATASET_ID = "br_anp_precos_combustiveis"

# Única tabela do dataset.
MICRODADOS_TABLE_ID = "microdados"

# Tier part_bdpro com defasagem de 6 semanas pra liberação free — mesma
# coverage que o flow monolítico antigo já registrava em
# `register_table_materialization_task`.
COVERAGE = PartBdpro(
    date_column=DateOnly(col="data_coleta"),
    date_format=DateFormat.YEAR_MD,
    free_lag=FreeLag(unit="weeks", value=6),
)
