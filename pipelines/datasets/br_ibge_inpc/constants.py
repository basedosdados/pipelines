"""
Constant values for br_ibge_inpc.
"""

DATASET_ID = "br_ibge_inpc"

# As 4 tabelas do dataset — migradas pro pipeline em estágios (staged pipeline),
# substituindo o antigo `_inpc_flow`/`_run_ibge_inflacao` monolítico (removido;
# lógica compartilhada agora mora em `pipelines/shared/ibge_inflacao/`).
MES_BRASIL_TABLE_ID = "mes_brasil"
MES_CATEGORIA_BRASIL_TABLE_ID = "mes_categoria_brasil"
MES_CATEGORIA_RM_TABLE_ID = "mes_categoria_rm"
MES_CATEGORIA_MUNICIPIO_TABLE_ID = "mes_categoria_municipio"
