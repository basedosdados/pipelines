"""
Constant values for br_inmet_bdmep.
"""

DATASET_ID = "br_inmet_bdmep"

# Única tabela com flow próprio — `estacao` é um model dbt derivado de
# `microdados`, não tem crawler/flow separado.
MICRODADOS_TABLE_ID = "microdados"
