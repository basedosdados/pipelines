"""
Constant values for br_bndes_operacoes_contratadas.
"""

from pipelines.utils.metadata.domain import AllFree, DateFormat, YearOnly

DATASET_ID = "br_bndes_operacoes_contratadas"

# As 5 tabelas do dataset — migradas pro pipeline em estágios (staged
# pipeline). A lógica de download/clean de cada uma segue existindo em
# `pipelines/crawler/bndes/` (tasks.py/utils.py/constants.py), reaproveitada
# por `tasks.py` deste pacote — só a fiação de check_update/extract_and_load
# é nova.
OPERACOES_INDIRETAS_AUTOMATICAS_TABLE_ID = "operacoes_indiretas_automaticas"
OPERACOES_NAO_AUTOMATICAS_TABLE_ID = "operacoes_nao_automaticas"
OPERACOES_ADMINISTRACAO_PUBLICA_TABLE_ID = "operacoes_administracao_publica"
OPERACOES_EXPORTACAO_BENS_TABLE_ID = "operacoes_exportacao_bens"
OPERACOES_EXPORTACAO_SERVICOS_TABLE_ID = "operacoes_exportacao_servicos"

# Mesma coverage pras 5 tabelas (dado público, anual, coluna `ano`) — igual
# ao que `_run_operacoes*` já registrava em
# `register_table_materialization_task` (pipelines/crawler/bndes/flows.py).
COVERAGE = AllFree(
    date_column=YearOnly(col="ano"), date_format=DateFormat.YEAR
)

# Granularidade do `last_modified` do recurso CKAN (RawDataSource Update) que
# o poll compara — não confundir com a coverage anual da tabela (`ano`). Ver
# a nota de duas datas em pipelines/crawler/bndes/flows.py.
SOURCE_DATE_FORMAT = "%Y-%m-%d"
