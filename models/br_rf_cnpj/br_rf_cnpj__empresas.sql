{{
    config(
        schema="br_rf_cnpj",
        alias="empresas",
        materialized="incremental",
        partition_by={
            "field": "data_referencia",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

-- Atualizado em 2026-08-11
with
    cnpj_empresas as (
        select
            safe.parse_date('%Y-%m', data_referencia) data_referencia,
            safe_cast(lpad(cnpj_basico, 8, '0') as string) cnpj_basico,
            safe_cast(razao_social as string) razao_social,
            safe_cast(natureza_juridica as string) natureza_juridica,
            safe_cast(
                regexp_replace(qualificacao_responsavel, '^0', '') as string
            ) qualificacao_responsavel,
            safe_cast(capital_social as float64) capital_social,
            safe_cast(regexp_replace(porte, '^0', '') as string) porte,
            safe_cast(ente_federativo as string) ente_federativo,
            safe_cast(data_modificacao as date) data_modificacao
        from {{ set_datalake_project("br_rf_cnpj_staging.empresas") }} as t
        where
            porte != "porte"
            {% if is_incremental() %}
                and data_referencia
                > format_date('%Y-%m', (select max(data_referencia) from {{ this }}))
        {% else %}
            -- Dados históricos até 2023-04-30 foram migrados do modelo
            -- br_me_cnpj.estabelecimentos
            union all
            select
                safe_cast(data as date) data_referencia,
                safe_cast(lpad(cnpj_basico, 8, '0') as string) cnpj_basico,
                safe_cast(razao_social as string) razao_social,
                safe_cast(natureza_juridica as string) natureza_juridica,
                safe_cast(
                    regexp_replace(qualificacao_responsavel, '^0', '') as string
                ) qualificacao_responsavel,
                safe_cast(capital_social as float64) capital_social,
                safe_cast(regexp_replace(porte, '^0', '') as string) porte,
                safe_cast(ente_federativo as string) ente_federativo,
                safe_cast(null as date) data_modificacao
            from {{ set_datalake_project("br_rf_cnpj_staging.empresas_legado") }}
            where porte != "porte" and safe_cast(data as date) <= date("2023-04-30")
        {% endif %}
    )
select *
from cnpj_empresas
