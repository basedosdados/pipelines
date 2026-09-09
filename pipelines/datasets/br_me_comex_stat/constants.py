"""
Constant values for br_me_comex_stat.
"""

DATASET_ID = "br_me_comex_stat"

MUNICIPIO_EXPORTACAO_TABLE_ID = "municipio_exportacao"
MUNICIPIO_IMPORTACAO_TABLE_ID = "municipio_importacao"
NCM_EXPORTACAO_TABLE_ID = "ncm_exportacao"
NCM_IMPORTACAO_TABLE_ID = "ncm_importacao"

# table_name/table_type por tabela -- mapeamento herdado de
# crawler/me_comex_stat/constants.py (TABLE_NAME/TABLE_TYPE, por posição
# de lista ali) -- preservado aqui de forma explícita, por nome, pra não
# depender de índice de lista em flows.py/tasks.py.
TABLE_SPECS = {
    MUNICIPIO_EXPORTACAO_TABLE_ID: {
        "table_name": "mun_exp",
        "table_type": "mun",
    },
    MUNICIPIO_IMPORTACAO_TABLE_ID: {
        "table_name": "mun_imp",
        "table_type": "mun",
    },
    NCM_EXPORTACAO_TABLE_ID: {
        "table_name": "ncm_exp",
        "table_type": "ncm",
    },
    NCM_IMPORTACAO_TABLE_ID: {
        "table_name": "ncm_imp",
        "table_type": "ncm",
    },
}
