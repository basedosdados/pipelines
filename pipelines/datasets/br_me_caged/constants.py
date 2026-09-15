"""
Constant values for br_me_caged.
"""

DATASET_ID = "br_me_caged"

# As 3 tabelas do dataset — migradas pro pipeline orientado a eventos
# (issue #1867), substituindo o antigo `_caged_flow`/`_run_me_caged`
# monolítico em `pipelines/crawler/me_caged/` (lógica de baixo nível
# reaproveitada, não removida — ver tasks.py).
MICRODADOS_MOVIMENTACAO_TABLE_ID = "microdados_movimentacao"
MICRODADOS_MOVIMENTACAO_FORA_PRAZO_TABLE_ID = (
    "microdados_movimentacao_fora_prazo"
)
MICRODADOS_MOVIMENTACAO_EXCLUIDA_TABLE_ID = "microdados_movimentacao_excluida"
