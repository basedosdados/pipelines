{{
    config(
        schema="br_mgi_compras_publicas",
        alias="compra_sem_licitacao",
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
    safe_cast(codigo_uasg as string) codigo_uasg,
    safe_cast(codigo_orgao as string) codigo_orgao,
    safe_cast(numero_aviso as string) numero_aviso,
    safe_cast(numero_processo as string) numero_processo,
    safe_cast(codigo_modalidade as string) codigo_modalidade,
    safe_cast(quantidade_total_item as int64) quantidade_total_item,
    safe_cast(objeto_licitacao as string) objeto_licitacao,
    safe_cast(fundamento_legal as string) fundamento_legal,
    safe_cast(justificativa as string) justificativa,
    safe_cast(
        nome_responsavel_declaracao_dispensa as string
    ) nome_responsavel_declaracao_dispensa,
    safe_cast(
        cargo_responsavel_declaracao_dispensa as string
    ) cargo_responsavel_declaracao_dispensa,
    safe_cast(nome_responsavel_ratificacao as string) nome_responsavel_ratificacao,
    safe_cast(cargo_responsavel_ratificacao as string) cargo_responsavel_ratificacao,
    safe_cast(nome_uasg as string) nome_uasg,
    safe_cast(codigo_orgao_superior as string) codigo_orgao_superior,
    safe_cast(lei as string) lei,
    safe_cast(valor_estimado as float64) valor_estimado,
    safe_cast(indicador_pertence_14133 as boolean) indicador_pertence_14133,
    safe_cast(data_declaracao_dispensa as date) data_declaracao_dispensa,
    safe_cast(data_ratificacao as date) data_ratificacao,
    safe_cast(data_publicacao as date) data_publicacao,
    safe_cast(data_alteracao as datetime) data_alteracao
from
    {{ set_datalake_project("br_mgi_compras_publicas_staging.compra_sem_licitacao") }}
    as t
qualify
    row_number() over (
        partition by
            ano,
            id_compra,
            codigo_uasg,
            codigo_orgao,
            numero_aviso,
            numero_processo,
            codigo_modalidade,
            quantidade_total_item,
            objeto_licitacao,
            fundamento_legal,
            justificativa,
            nome_responsavel_declaracao_dispensa,
            cargo_responsavel_declaracao_dispensa,
            nome_responsavel_ratificacao,
            cargo_responsavel_ratificacao,
            nome_uasg,
            codigo_orgao_superior,
            lei,
            valor_estimado,
            indicador_pertence_14133,
            data_declaracao_dispensa,
            data_ratificacao,
            data_publicacao,
            data_alteracao
        order by data_alteracao desc
    )
    = 1
