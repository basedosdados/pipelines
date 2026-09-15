"""
Constant values for br_ibge_ipca.
"""

DATASET_ID = "br_ibge_ipca"

# As 4 tabelas do dataset — migradas pro pipeline orientado a eventos
# (issue #1867), substituindo o antigo `_ipca_flow`/`_run_ibge_inflacao`
# monolítico (que segue existindo em `crawler/ibge_inflacao/flows.py`,
# ainda usado por br_ibge_ipca15/br_ibge_inpc).
MES_BRASIL_TABLE_ID = "mes_brasil"
MES_CATEGORIA_BRASIL_TABLE_ID = "mes_categoria_brasil"
MES_CATEGORIA_RM_TABLE_ID = "mes_categoria_rm"
MES_CATEGORIA_MUNICIPIO_TABLE_ID = "mes_categoria_municipio"
