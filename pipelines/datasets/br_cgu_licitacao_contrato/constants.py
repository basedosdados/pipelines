"""
Constant values for br_cgu_licitacao_contrato.
"""

DATASET_ID = "br_cgu_licitacao_contrato"

# As 7 tabelas do dataset — duas fontes no Portal da Transparência:
# "licitacoes" (licitacao, licitacao_participante, licitacao_item,
# licitacao_empenho) e "compras" (contrato_compra, contrato_item,
# contrato_termo_aditivo). O ZIP mensal de cada fonte contém os CSVs de
# várias tabelas do mesmo grupo, mas cada tabela baixa de forma
# independente (ver tasks.py) — não há dependência real entre elas no dbt
# (nenhum `ref()` cruzado em schema.yml/*.sql, só `relationships` contra
# diretórios externos como br_bd_diretorios_data_tempo).
LICITACAO_TABLE_ID = "licitacao"
LICITACAO_PARTICIPANTE_TABLE_ID = "licitacao_participante"
LICITACAO_ITEM_TABLE_ID = "licitacao_item"
LICITACAO_EMPENHO_TABLE_ID = "licitacao_empenho"
CONTRATO_COMPRA_TABLE_ID = "contrato_compra"
CONTRATO_ITEM_TABLE_ID = "contrato_item"
CONTRATO_TERMO_ADITIVO_TABLE_ID = "contrato_termo_aditivo"

# Mesmo valor do parâmetro `relative_month` do flow monolítico antigo
# (default em `_run_cgu_licitacao_contrato`,
# `pipelines/shared/cgu/flows.py`) — quantos meses somar à última data
# registrada nos metadados pra chegar no período a baixar.
RELATIVE_MONTH = 1
