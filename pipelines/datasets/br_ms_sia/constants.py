"""
Constant values for br_ms_sia.
"""

from pipelines.utils.metadata.domain import DateFormat, PartBdpro, YearMonth

DATASET_ID = "br_ms_sia"

# As 2 tabelas do dataset — migradas pro pipeline em estágios (staged
# pipeline), substituindo o antigo `_sia_flow`/`_run_siasus` monolítico (que
# segue existindo como `_run_dbf_to_parquet` em `crawler/datasus/flows.py`,
# ainda usado por br_ms_sih/br_ms_sinan).
PRODUCAO_AMBULATORIAL_TABLE_ID = "producao_ambulatorial"
PSICOSSOCIAL_TABLE_ID = "psicossocial"

# Mesma coverage pras 2 tabelas (mesma usada por br_ms_cnes e definida em
# `_run_dbf_to_parquet`/`register_table_materialization_task` no flow antigo).
COVERAGE = PartBdpro(
    date_column=YearMonth(year="ano", month="mes"),
    date_format=DateFormat.YEAR_MONTH,
)
