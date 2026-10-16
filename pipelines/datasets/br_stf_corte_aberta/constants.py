"""
Constant values for br_stf_corte_aberta.
"""

from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
)

DATASET_ID = "br_stf_corte_aberta"
DECISOES_TABLE_ID = "decisoes"

# Mesma coverage do flow monolítico antigo (`register_table_materialization_task`
# em flows.py): diária, com defasagem de 6 semanas pra parte recente virar BD pro.
COVERAGE = PartBdpro(
    date_column=DateOnly(col="data_decisao"),
    date_format=DateFormat.YEAR_MD,
    free_lag=FreeLag(unit="weeks", value=6),
)
