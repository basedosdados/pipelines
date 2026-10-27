"""
Constant values for br_ms_sih.
"""

DATASET_ID = "br_ms_sih"

# As 2 tabelas do dataset — migradas pro pipeline em estágios (staged pipeline),
# substituindo o antigo `_run_sihsus`/`_run_dbf_to_parquet` monolítico (que
# segue existindo em `crawler/datasus/flows.py`, ainda usado por
# br_ms_sia/br_ms_sinan).
SERVICOS_PROFISSIONAIS_TABLE_ID = "servicos_profissionais"
AIHS_REDUZIDAS_TABLE_ID = "aihs_reduzidas"
