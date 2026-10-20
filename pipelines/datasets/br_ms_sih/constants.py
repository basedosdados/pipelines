"""
Constant values for br_ms_sih.
"""

from pipelines.utils.metadata.domain import DateFormat, PartBdpro, YearMonth

DATASET_ID = "br_ms_sih"

# As 2 tabelas do dataset — migradas pro pipeline em estágios (staged pipeline),
# substituindo o antigo `_run_sihsus`/`_run_dbf_to_parquet` monolítico (que
# segue existindo em `crawler/datasus/flows.py`, ainda usado por
# br_ms_sia/br_ms_sinan).
SERVICOS_PROFISSIONAIS_TABLE_ID = "servicos_profissionais"
AIHS_REDUZIDAS_TABLE_ID = "aihs_reduzidas"

# Mesma coverage das 2 tabelas (idêntica à usada por `_run_dbf_to_parquet` em
# `register_table_materialization_task` e à de br_ms_cnes).
COVERAGE = PartBdpro(
    date_column=YearMonth(year="ano", month="mes"),
    date_format=DateFormat.YEAR_MONTH,
)
