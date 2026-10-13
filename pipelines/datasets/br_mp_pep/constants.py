"""
Constant values for br_mp_pep.
"""

from pipelines.utils.metadata.domain import DateFormat, PartBdpro, YearMonth

DATASET_ID = "br_mp_pep"

CARGOS_FUNCOES_TABLE_ID = "cargos_funcoes"

# Mesma coverage do flow monolítico antigo (`register_table_materialization_task`
# em `flows.py`): mensal, com a parte recente BD pro.
COVERAGE = PartBdpro(
    date_column=YearMonth(year="ano", month="mes"),
    date_format=DateFormat.YEAR_MONTH,
)
