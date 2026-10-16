"""
Constant values for br_bcb_agencia.
"""

from pipelines.utils.metadata.domain import DateFormat, PartBdpro, YearMonth

DATASET_ID = "br_bcb_agencia"

AGENCIA_TABLE_ID = "agencia"

# Mesma coverage do flow monolítico antigo (`register_table_materialization_task`
# em `flows.py`): mensal, com a parte recente BD pro.
COVERAGE = PartBdpro(
    date_column=YearMonth(year="ano", month="mes"),
    date_format=DateFormat.YEAR_MONTH,
)
