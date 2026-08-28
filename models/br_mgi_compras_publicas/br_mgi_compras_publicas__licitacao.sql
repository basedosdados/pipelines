{{
    config(
        schema="br_mgi_compras_publicas",
        alias="licitacao",
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
    safe_cast(identificador as string) identificador,
    safe_cast(codigo_uasg as string) codigo_uasg,
    safe_cast(numero_aviso as string) numero_aviso,
    safe_cast(numero_processo as string) numero_processo,
    safe_cast(codigo_modalidade as string) codigo_modalidade,
    safe_cast(modalidade as string) modalidade,
    safe_cast(tipo_pregao as string) tipo_pregao,
    safe_cast(tipo_recurso as string) tipo_recurso,
    safe_cast(situacao_aviso as string) situacao_aviso,
    safe_cast(codigo_municipio_uasg as string) codigo_municipio_uasg,
    safe_cast(objeto as string) objeto,
    safe_cast(informacoes_gerais as string) informacoes_gerais,
    safe_cast(endereco_entrega_edital as string) endereco_entrega_edital,
    safe_cast(nome_responsavel as string) nome_responsavel,
    safe_cast(funcao_responsavel as string) funcao_responsavel,
    safe_cast(numero_itens as int64) numero_itens,
    safe_cast(valor_estimado_total as float64) valor_estimado_total,
    safe_cast(valor_homologado_total as float64) valor_homologado_total,
    safe_cast(indicador_pertence_14133 as boolean) indicador_pertence_14133,
    safe_cast(data_publicacao as date) data_publicacao,
    safe_cast(data_entrega_edital as date) data_entrega_edital,
    safe_cast(data_entrega_proposta as date) data_entrega_proposta,
    safe_cast(data_abertura_proposta as date) data_abertura_proposta,
    safe_cast(data_alteracao as datetime) data_alteracao
from {{ set_datalake_project("br_mgi_compras_publicas_staging.licitacao") }} as t
qualify row_number() over (partition by id_compra order by data_alteracao desc) = 1
