"""
Constants for br_rf_cno.
"""

from pipelines.utils.metadata.domain import DateFormat, DateOnly, PartBdpro

DATASET_ID = "br_rf_cno"

MICRODADOS_TABLE_ID = "microdados"
VINCULOS_TABLE_ID = "vinculos"
AREAS_TABLE_ID = "areas"
CNAES_TABLE_ID = "cnaes"

# Mesma coverage das 4 tabelas (flow antigo, `_run_rf` ->
# `register_table_materialization_task`).
COVERAGE = PartBdpro(
    date_column=DateOnly(col="data_extracao"),
    date_format=DateFormat.YEAR_MD,
)
