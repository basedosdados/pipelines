"""
Constant values for br_cvm_fi.
"""

DATASET_ID = "br_cvm_fi"

# As 6 tabelas do dataset — migradas pro pipeline em estágios (staged pipeline),
# substituindo o antigo `_cvm_fi_flow`/`_run_cvm_fi` monolítico (que segue
# existindo em `crawler/cvm/flows.py`, de onde as tasks de scraping/parsing
# continuam sendo reaproveitadas).
DOCUMENTOS_INFORME_DIARIO_TABLE_ID = "documentos_informe_diario"
DOCUMENTOS_CARTEIRAS_FUNDOS_INVESTIMENTO_TABLE_ID = (
    "documentos_carteiras_fundos_investimento"
)
DOCUMENTOS_EXTRATOS_INFORMACOES_TABLE_ID = "documentos_extratos_informacoes"
DOCUMENTOS_BALANCETE_TABLE_ID = "documentos_balancete"
DOCUMENTOS_INFORMACAO_CADASTRAL_TABLE_ID = "documentos_informacao_cadastral"
DOCUMENTOS_PERFIL_MENSAL_TABLE_ID = "documentos_perfil_mensal"

# Coluna de data usada como baseline de coverage (`AllBdpro`/`DateOnly`) de
# cada tabela — mesmo mapeamento que o `date_column_name` do flow monolítico
# antigo (`_cvm_fi_flow`, em `pipelines/datasets/br_cvm_fi/flows.py` antes da
# migração). Todas usam `data_competencia`, exceto o cadastro de fundos, cuja
# referência temporal é o início da situação atual do fundo.
DATE_COLUMN_BY_TABLE = {
    DOCUMENTOS_INFORME_DIARIO_TABLE_ID: "data_competencia",
    DOCUMENTOS_CARTEIRAS_FUNDOS_INVESTIMENTO_TABLE_ID: "data_competencia",
    DOCUMENTOS_EXTRATOS_INFORMACOES_TABLE_ID: "data_competencia",
    DOCUMENTOS_BALANCETE_TABLE_ID: "data_competencia",
    DOCUMENTOS_INFORMACAO_CADASTRAL_TABLE_ID: "data_inicio_situacao",
    DOCUMENTOS_PERFIL_MENSAL_TABLE_ID: "data_competencia",
}
