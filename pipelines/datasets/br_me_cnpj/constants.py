"""
Constant values for br_me_cnpj.
"""

DATASET_ID = "br_me_cnpj"

# As 4 tabelas do dataset — migradas pro pipeline orientado a eventos
# (issue #1867), substituindo o antigo `_me_cnpj_flow`/`_run_me_cnpj`
# monolítico (que segue existindo em `crawler/me_cnpj/flows.py`, só usado
# por este dataset — sem outros consumidores).
EMPRESAS_TABLE_ID = "empresas"
SOCIOS_TABLE_ID = "socios"
ESTABELECIMENTOS_TABLE_ID = "estabelecimentos"
SIMPLES_TABLE_ID = "simples"

# `simples` não tem coluna de data confiável (`NonHistorical`) — sem
# baseline de `Coverage.DateTimeRange`, precisa comparar contra o último
# `Table.Update` em vez da coverage (ver `compare_against` em
# `CheckThenDownloadPipeline`).
SIMPLES_COMPARE_AGAINST = "table_update"

# Tiers de memória usados pelo flow antigo pro download (unzip + processa
# CSVs grandes) — carregados pro estágio `download` da tabela
# correspondente. `simples` nunca teve tier customizado (usa o default do
# work pool).
HEAVY_DOWNLOAD_JOB_VARIABLES = {
    "memory_limit": "5Gi",
    "memory_request": "2Gi",
}
