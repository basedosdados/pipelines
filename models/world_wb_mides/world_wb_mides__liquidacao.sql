{{
    config(
        alias="liquidacao",
        schema="world_wb_mides",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1992, "end": 2029, "interval": 1},
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
    id_empenho_bd,
    id_empenho,
    numero_empenho,
    id_liquidacao_bd,
    id_liquidacao,
    numero,
    nome_responsavel,
    documento_responsavel,
    indicador_restos_pagar,
    valor_inicial,
    valor_anulacao,
    valor_ajuste,
    valor_final
from
    (
        select *
        from {{ ref("world_wb_mides__liquidacao_mg") }}
        union all
        select *
        from {{ ref("world_wb_mides__liquidacao_sp") }}
        union all
        select *
        from {{ ref("world_wb_mides__liquidacao_pe") }}
        union all
        select *
        from {{ ref("world_wb_mides__liquidacao_pr") }}
        union all
        select *
        from {{ ref("world_wb_mides__liquidacao_rs") }}
        union all
        select *
        from {{ ref("world_wb_mides__liquidacao_pb") }}
        union all
        select *
        from {{ ref("world_wb_mides__liquidacao_ce") }}
        union all
        select *
        from {{ ref("world_wb_mides__liquidacao_rj") }}
        union all
        select *
        from {{ ref("world_wb_mides__liquidacao_df") }}
        union all
        select *
        from {{ ref("world_wb_mides__liquidacao_sc") }}
    )
