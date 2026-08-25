{{
    config(
        alias="pagamento",
        schema="world_wb_mides",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1994, "end": 2029, "interval": 1},
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
    numero_liquidacao,
    id_pagamento_bd,
    id_pagamento,
    numero,
    nome_credor,
    documento_credor,
    indicador_restos_pagar,
    fonte,
    valor_inicial,
    valor_anulacao,
    valor_ajuste,
    valor_final,
    valor_liquido_recebido
from
    (
        select *
        from {{ ref("world_wb_mides__pagamento_mg") }}
        union all
        select *
        from {{ ref("world_wb_mides__pagamento_sp") }}
        union all
        select *
        from {{ ref("world_wb_mides__pagamento_pe") }}
        union all
        select *
        from {{ ref("world_wb_mides__pagamento_pr") }}
        union all
        select *
        from {{ ref("world_wb_mides__pagamento_rs") }}
        union all
        select *
        from {{ ref("world_wb_mides__pagamento_pb") }}
        union all
        select *
        from {{ ref("world_wb_mides__pagamento_ce") }}
        union all
        select *
        from {{ ref("world_wb_mides__pagamento_rj") }}
        union all
        select *
        from {{ ref("world_wb_mides__pagamento_df") }}
        union all
        select *
        from {{ ref("world_wb_mides__pagamento_sc") }}
        union all
        select *
        from {{ ref("world_wb_mides__pagamento_to") }}
    )
