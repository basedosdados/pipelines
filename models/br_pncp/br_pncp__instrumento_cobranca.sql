{{
    config(
        schema="br_pncp",
        alias="instrumento_cobranca",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2023, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(cnpj_orgao as string) cnpj_orgao,
    safe_cast(ano_contrato as int64) ano_contrato,
    safe_cast(sequencial_contrato as string) sequencial_contrato,
    safe_cast(sequencial_instrumento_cobranca as string) sequencial_instrumento_cobranca,
    safe_cast(id_contrato_pncp as string) id_contrato_pncp,
    safe_cast(id_tipo_instrumento_cobranca as string) id_tipo_instrumento_cobranca,
    safe_cast(tipo_instrumento_cobranca as string) tipo_instrumento_cobranca,
    safe_cast(numero_instrumento_cobranca as string) numero_instrumento_cobranca,
    safe_cast(chave_nfe as string) chave_nfe,
    safe_cast(numero_nfe as string) numero_nfe,
    safe_cast(serie_nfe as string) serie_nfe,
    safe_cast(id_emitente_nfe as string) id_emitente_nfe,
    safe_cast(nome_emitente_nfe as string) nome_emitente_nfe,
    safe_cast(nome_municipio_emitente_nfe as string) nome_municipio_emitente_nfe,
    safe_cast(codigo_orgao_destinatario_nfe as string) codigo_orgao_destinatario_nfe,
    safe_cast(nome_orgao_destinatario_nfe as string) nome_orgao_destinatario_nfe,
    safe_cast(tipo_evento_recente_nfe as string) tipo_evento_recente_nfe,
    safe_cast(observacao as string) observacao,
    safe_cast(data_emissao as date) data_emissao,
    safe_cast(data_inclusao as date) data_inclusao,
    safe_cast(data_atualizacao as date) data_atualizacao,
    safe_cast(valor_nota_fiscal as float64) valor_nota_fiscal
from
    {{ set_datalake_project("br_pncp_staging.instrumento_cobranca") }}
    as t
{% if is_incremental() and var('pncp_years', '') %}
    where
        safe_cast(ano as int64) in (
            {{ var('pncp_years') }}
        )
{% endif %}
qualify
    row_number() over (
        partition by cnpj_orgao, ano_contrato, sequencial_contrato, sequencial_instrumento_cobranca
        order by safe_cast(data_atualizacao as date) desc
    )
    = 1
