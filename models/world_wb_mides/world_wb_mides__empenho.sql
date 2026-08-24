{{
    config(
        alias="empenho",
        schema="world_wb_mides",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1989, "end": 2029, "interval": 1},
        },
        cluster_by=["mes", "sigla_uf"],
        labels={"tema": "economia"},
    )
}}
select
    ano,
    mes,
    data,
    sigla_uf,
    id_municipio,
    orgao,
    id_unidade_gestora,
    id_licitacao_bd,
    id_licitacao,
    modalidade_licitacao,
    id_empenho_bd,
    id_empenho,
    numero,
    descricao,
    modalidade,
    funcao,
    subfuncao,
    programa,
    acao,
    elemento_despesa,
    valor_inicial,
    valor_reforco,
    valor_anulacao,
    valor_ajuste,
    valor_final
from
    (
        select *
        from {{ ref("world_wb_mides__empenho_mg") }}
        union all
        select *
        from {{ ref("world_wb_mides__empenho_sp") }}
        union all
        select *
        from {{ ref("world_wb_mides__empenho_pe") }}
        union all
        select *
        from {{ ref("world_wb_mides__empenho_pr") }}
        union all
        select *
        from {{ ref("world_wb_mides__empenho_rs") }}
        union all
        select *
        from {{ ref("world_wb_mides__empenho_pb") }}
        union all
        select *
        from {{ ref("world_wb_mides__empenho_ce") }}
        union all
        select *
        from {{ ref("world_wb_mides__empenho_rj") }}
        union all
        select *
        from {{ ref("world_wb_mides__empenho_df") }}
        union all
        select *
        from {{ ref("world_wb_mides__empenho_sc") }}
    )
