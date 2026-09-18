{{
    config(
        schema="br_mgi_pncp",
        alias="ata_registro_preco",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2021, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(id_ata_pncp as string) id_ata_pncp,
    safe_cast(id_contratacao_pncp as string) id_contratacao_pncp,
    safe_cast(numero_ata as string) numero_ata,
    safe_cast(ano_ata as int64) ano_ata,
    safe_cast(cnpj_orgao as string) cnpj_orgao,
    safe_cast(nome_orgao as string) nome_orgao,
    safe_cast(codigo_unidade as string) codigo_unidade,
    safe_cast(nome_unidade as string) nome_unidade,
    safe_cast(cnpj_orgao_subrogado as string) cnpj_orgao_subrogado,
    safe_cast(nome_orgao_subrogado as string) nome_orgao_subrogado,
    safe_cast(codigo_unidade_subrogada as string) codigo_unidade_subrogada,
    safe_cast(nome_unidade_subrogada as string) nome_unidade_subrogada,
    safe_cast(objeto_contratacao as string) objeto_contratacao,
    safe_cast(indicador_cancelado as boolean) indicador_cancelado,
    safe_cast(indicador_possibilidade_adesao as boolean) indicador_possibilidade_adesao,
    safe_cast(data_assinatura as date) data_assinatura,
    safe_cast(data_vigencia_inicio as date) data_vigencia_inicio,
    safe_cast(data_vigencia_fim as date) data_vigencia_fim,
    safe_cast(data_cancelamento as date) data_cancelamento,
    safe_cast(data_publicacao as date) data_publicacao,
    safe_cast(data_atualizacao as date) data_atualizacao
from {{ set_datalake_project("br_mgi_pncp_staging.ata_registro_preco") }} as t
{% if is_incremental() and var("pncp_years", "") %}
    where safe_cast(ano as int64) in ({{ var("pncp_years") }})
{% endif %}
qualify
    row_number() over (
        partition by id_ata_pncp order by safe_cast(data_atualizacao as date) desc
    )
    = 1
