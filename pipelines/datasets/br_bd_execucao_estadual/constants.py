"""Constants for the br_bd_execucao_estadual pipeline."""

from enum import Enum
from pathlib import Path


class constants(Enum):
    DATASET_ID = "br_bd_execucao_estadual"

    # The ten published tables, in the order the dataset presents them.
    PUBLISHED_TABLES = [
        "despesa",
        "pagamento",
        "despesa_mensal",
        "despesa_anual",
        "empenho_credor",
        "licitacao",
        "licitacao_item",
        "licitacao_participante",
        "relacionamentos",
        "dicionario",
    ]

    # Which published tables each state feeds. A state's refresh rebuilds exactly
    # these, and every one of them is a union, so rebuilding `despesa` for MG also
    # re-reads PE. That is intended: dbt materializes the union from staging, and
    # the other state's staging tables have not moved.
    TABLES_BY_STATE = {
        "MG": [
            "despesa",
            "licitacao",
            "licitacao_item",
            "relacionamentos",
            "dicionario",
        ],
        "BA": [
            "despesa_mensal",
            "empenho_credor",
            "licitacao",
            "licitacao_item",
            "licitacao_participante",
            "relacionamentos",
        ],
        "PE": ["despesa", "pagamento"],
        "SP": ["despesa_anual"],
    }

    # THE THING THAT MAKES THIS DATASET DIFFERENT FROM EVERY OTHER ONE HERE.
    #
    # Almost every BD dataset has one staging table per published table, named the
    # same, which is what `table-approve` assumes when it syncs
    # `staging/<dataset>/<published table>/` from dev to prod. This dataset has 49
    # staging mirrors -- one per SOURCE table -- feeding 10 published models through
    # ephemeral per-state models, because the harmonization is genuinely a join
    # across each state's dimensional export.
    #
    # The consequence is not cosmetic: table-approve's sync matches nothing here, so
    # merging the onboarding PR left `basedosdados-staging` empty and the prod dbt
    # run failed with "Access Denied ... or perhaps it does not exist" on
    # mg_dm_acao. This pipeline is what populates prod staging, by uploading every
    # mirror below to the prod bucket itself.
    STAGING_BY_STATE = {
        "MG": [
            "mg_ft_despesa",
            "mg_dm_empenho",
            "mg_dm_favorecido",
            "mg_dm_funcao",
            "mg_dm_subfuncao",
            "mg_dm_programa",
            "mg_dm_acao",
            "mg_dm_categoria",
            "mg_dm_grupo",
            "mg_dm_modalidade_aplic",
            "mg_dm_elemento",
            "mg_dm_item",
            "mg_dm_fonte",
            "mg_dm_unidade_orc",
            "mg_dm_procedencia",
            "mg_dm_tipo_documento",
            "mg_dm_situacao_op",
            "mg_fl_despesa_pgto",
            "mg_ft_compras",
            "mg_ft_compras_contrato",
            "mg_dm_processo",
            "mg_dm_contratado",
            "mg_dm_contrato",
            "mg_dm_item_matserv",
            "mg_dm_material_servico",
            "mg_dm_grupo_matserv",
            "mg_dm_classe_matserv",
            "mg_dm_unidade_medida",
            "mg_dm_linha_fornec",
            "mg_dm_tipo_licitacao",
            "mg_dm_procedimento",
            "mg_dm_situacao_proc",
            "mg_dm_situacao_cont",
            "mg_dm_orgao_demanda",
            "mg_dm_orgao_contrato",
            "mg_dm_municipio",
            "mg_dm_tempo",
            "mg_fl_compras_empenho",
            "mg_dm_empenho_compras",
        ],
        "BA": [
            "ba_despesa",
            "ba_empenho_sei",
            "ba_licitacao",
            "ba_licitacao_item",
            "ba_licitacao_participante",
            "ba_licitacao_empenho",
        ],
        "PE": ["pe_despesa", "pe_despesa_legado", "pe_pagamento"],
        "SP": ["sp_despesa"],
    }

    # Only Minas Gerais and Pernambuco publish per-exercise files, so only they can
    # be refreshed for the current year alone. Bahia ships whole-dataset ZIPs and
    # São Paulo is queried per (exercise, órgão), so BA re-downloads everything and
    # SP re-scrapes just the open exercise.
    YEARLY_SOURCES = {"MG", "PE"}

    # São Paulo's SIGEO is a WebForms scrape at roughly 36 s per (exercise, órgão).
    # One exercise is ~32 queries, about twenty minutes; all seventeen took five
    # hours. That is why SP has its own weekly flow instead of riding the daily one.
    SP_QUERIES_PER_YEAR = 32

    CODE_DIR = str(
        Path(__file__).resolve().parents[3]
        / "models/br_bd_execucao_estadual/code"
    )
