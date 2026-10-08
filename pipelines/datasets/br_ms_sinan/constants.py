"""
Constant values for br_ms_sinan.
"""

from pipelines.utils.metadata.domain import AllFree, DateFormat, DateOnly

DATASET_ID = "br_ms_sinan"

# Única tabela do dataset hoje (ver `pipelines/crawler/datasus/constants.py`
# -> `DATASUS_DATABASE_TABLE`, que só mapeia "microdados_dengue").
MICRODADOS_DENGUE_TABLE_ID = "microdados_dengue"

# Mesma coverage usada por `_run_sinan`/`register_table_materialization_task`
# no flow antigo.
COVERAGE = AllFree(
    date_column=DateOnly(col="data_notificacao"),
    date_format=DateFormat.YEAR_MD,
)
