{{
    config(
        schema="br_mgi_compras_publicas",
        alias="licitacao_item_pregao",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1990, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(id_compra as string) id_compra,
    safe_cast(id_compra_item as string) id_compra_item,
    safe_cast(situacao_item as string) situacao_item,
    safe_cast(descricao_item as string) descricao_item,
    safe_cast(descricao_detalhada_item as string) descricao_detalhada_item,
    safe_cast(unidade_fornecimento as string) unidade_fornecimento,
    safe_cast(quantidade_item as float64) quantidade_item,
    safe_cast(valor_estimado_item as float64) valor_estimado_item,
    safe_cast(menor_lance as float64) menor_lance,
    safe_cast(valor_negociado as float64) valor_negociado,
    safe_cast(valor_homologado_item as float64) valor_homologado_item,
    safe_cast(fornecedor_vencedor as string) fornecedor_vencedor,
    safe_cast(nome_responsavel_adjudicacao as string) nome_responsavel_adjudicacao,
    safe_cast(nome_responsavel_homologacao as string) nome_responsavel_homologacao,
    safe_cast(indicador_decreto_7174 as string) indicador_decreto_7174,
    safe_cast(indicador_margem_preferencial as string) indicador_margem_preferencial,
    safe_cast(tratamento_diferenciado as string) tratamento_diferenciado,
    safe_cast(data_encerramento as date) data_encerramento,
    safe_cast(data_adjudicacao as date) data_adjudicacao,
    safe_cast(data_homologacao as date) data_homologacao,
    safe_cast(data_alteracao as datetime) data_alteracao
from
    {{ set_datalake_project("br_mgi_compras_publicas_staging.licitacao_item_pregao") }}
    as t
qualify
    row_number() over (
        partition by
            ano,
            id_compra,
            id_compra_item,
            situacao_item,
            descricao_item,
            descricao_detalhada_item,
            unidade_fornecimento,
            quantidade_item,
            valor_estimado_item,
            menor_lance,
            valor_negociado,
            valor_homologado_item,
            fornecedor_vencedor,
            nome_responsavel_adjudicacao,
            nome_responsavel_homologacao,
            indicador_decreto_7174,
            indicador_margem_preferencial,
            tratamento_diferenciado,
            data_encerramento,
            data_adjudicacao,
            data_homologacao,
            data_alteracao
        order by data_alteracao desc
    )
    = 1
