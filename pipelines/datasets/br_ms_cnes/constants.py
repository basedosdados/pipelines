"""
Constant values for br_ms_cnes.
"""

DATASET_ID = "br_ms_cnes"

# As 13 tabelas do dataset — migradas pro pipeline orientado a eventos
# (issue #1867), substituindo o antigo `_cnes_flow`/`_run_cnes` monolítico
# (que segue existindo em `crawler/datasus/flows.py`, ainda usado por
# br_ms_sia/br_ms_sih/br_ms_sinan).
PROFISSIONAL_TABLE_ID = "profissional"
ESTABELECIMENTO_TABLE_ID = "estabelecimento"
EQUIPE_TABLE_ID = "equipe"
LEITO_TABLE_ID = "leito"
EQUIPAMENTO_TABLE_ID = "equipamento"
ESTABELECIMENTO_ENSINO_TABLE_ID = "estabelecimento_ensino"
DADOS_COMPLEMENTARES_TABLE_ID = "dados_complementares"
ESTABELECIMENTO_FILANTROPICO_TABLE_ID = "estabelecimento_filantropico"
GESTAO_METAS_TABLE_ID = "gestao_metas"
HABILITACAO_TABLE_ID = "habilitacao"
INCENTIVOS_TABLE_ID = "incentivos"
REGRA_CONTRATUAL_TABLE_ID = "regra_contratual"
SERVICO_ESPECIALIZADO_TABLE_ID = "servico_especializado"
