{{
    config(
        schema="br_rf_cnpj",
        alias="socios",
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
    cnpj_socios as (
        select
            safe.parse_date('%Y-%m', data_referencia) data_referencia,
            lpad(safe_cast(cnpj_basico as string), 8, '0') cnpj_basico,
            safe_cast(tipo as string) tipo,
            safe_cast(nome as string) nome,
            safe_cast(documento as string) documento,
            safe_cast(cast(qualificacao as int64) as string) qualificacao,
            safe_cast(data_entrada_sociedade as date) data_entrada_sociedade,
            safe_cast(cast(id_pais as int64) as string) id_pais,
            safe_cast(cpf_representante_legal as string) cpf_representante_legal,
            safe_cast(nome_representante_legal as string) nome_representante_legal,
            safe_cast(
                cast(qualificacao_representante_legal as int64) as string
            ) qualificacao_representante_legal,
            safe_cast(faixa_etaria as string) faixa_etaria,
            safe_cast(data_modificacao as date) data_modificacao
        from {{ set_datalake_project("br_rf_cnpj_staging.socios") }} as t
        where
            safe_cast(qualificacao as string) != "qualificacao"
            {% if is_incremental() %}
                and data_referencia
                > format_date('%Y-%m', (select max(data_referencia) from {{ this }}))
        {% else %}
            -- Dados históricos até 2023-04-30 foram migrados do modelo
            -- br_me_cnpj.socios
            union all
            select
                safe_cast(data as date) data_referencia,
                lpad(safe_cast(cnpj_basico as string), 8, '0') cnpj_basico,
                safe_cast(tipo as string) tipo,
                safe_cast(nome as string) nome,
                safe_cast(documento as string) documento,
                safe_cast(cast(qualificacao as int64) as string) qualificacao,
                safe_cast(data_entrada_sociedade as date) data_entrada_sociedade,
                safe_cast(cast(id_pais as int64) as string) id_pais,
                safe_cast(cpf_representante_legal as string) cpf_representante_legal,
                safe_cast(nome_representante_legal as string) nome_representante_legal,
                safe_cast(
                    cast(qualificacao_representante_legal as int64) as string
                ) qualificacao_representante_legal,
                safe_cast(faixa_etaria as string) faixa_etaria,
                safe_cast(null as date) data_modificacao
            from {{ set_datalake_project("br_rf_cnpj_staging.socios_legado") }}
            where
                safe_cast(qualificacao as string) != "qualificacao"
                and safe_cast(data as date) <= date("2023-04-30")
        {% endif %}
    )
select *
from cnpj_socios
