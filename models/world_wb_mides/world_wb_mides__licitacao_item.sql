{{
    config(
        alias="licitacao_item",
        schema="world_wb_mides",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2009, "end": 2022, "interval": 1},
        },
        cluster_by=["sigla_uf"],
        labels={"tema": "economia"},
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(orgao as string) orgao,
    safe_cast(id_unidade_gestora as string) id_unidade_gestora,
    safe_cast(id_licitacao_bd as string) id_licitacao_bd,
    safe_cast(id_licitacao as string) id_licitacao,
    safe_cast(id_dispensa as string) id_dispensa,
    safe_cast(id_item_bd as string) id_item_bd,
    safe_cast(id_item as string) id_item,
    safe_cast(descricao as string) descricao,
    safe_cast(numero as int64) numero,
    safe_cast(numero_lote as int64) numero_lote,
    safe_cast(unidade_medida as string) unidade_medida,
    safe_cast(quantidade_cotada as int64) quantidade_cotada,
    safe_cast(valor_unitario_cotacao as float64) valor_unitario_cotacao,
    safe_cast(quantidade as int64) quantidade,
    safe_cast(valor_unitario as float64) valor_unitario,
    safe_cast(valor_total as float64) valor_total,
    safe_cast(quantidade_proposta as int64) quantidade_proposta,
    safe_cast(valor_proposta as float64) valor_proposta,
    safe_cast(valor_vencedor as float64) valor_vencedor,
    safe_cast(nome_vencedor as string) nome_vencedor,
    safe_cast(documento as string) documento
from {{ set_datalake_project("world_wb_mides_staging.licitacao_item") }} as t
