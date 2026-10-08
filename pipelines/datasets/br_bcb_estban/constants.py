"""
Constant values for br_bcb_estban.
"""

from pipelines.utils.metadata.domain import DateFormat, PartBdpro, YearMonth

DATASET_ID = "br_bcb_estban"

# As 2 tabelas do dataset — migradas pro pipeline em estágios (staged
# pipeline), substituindo o antigo `_run_bcb_estban`/`_estban_flow`
# monolítico (que segue existindo em `crawler/bcb_estban/`, só como fonte
# das tasks/utils reaproveitadas aqui).
AGENCIA_TABLE_ID = "agencia"
MUNICIPIO_TABLE_ID = "municipio"

# Mesma coverage pras 2 tabelas — mesmo PartBdpro(YearMonth) que o flow
# monolítico antigo passava pra `register_table_materialization_task`.
COVERAGE = PartBdpro(
    date_column=YearMonth(year="ano", month="mes"),
    date_format=DateFormat.YEAR_MONTH,
)
