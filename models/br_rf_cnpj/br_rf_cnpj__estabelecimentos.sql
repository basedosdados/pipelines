{{
    config(
        schema="br_rf_cnpj",
        alias="estabelecimentos",
        materialized="incremental",
        partition_by={
            "field": "data_referencia",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["sigla_uf"],
    )
}}
-- Atualizado em 2026-08-11
with
    cnpj_estabelecimentos as (
        select
            safe.parse_date('%Y-%m', data_referencia) data_referencia,
            safe_cast(lpad(cnpj, 14, "0") as string) cnpj,
            safe_cast(lpad(cnpj_basico, 8, '0') as string) cnpj_basico,
            safe_cast(lpad(cnpj_ordem, 4, '0') as string) cnpj_ordem,
            safe_cast(lpad(cnpj_dv, 2, '0') as string) cnpj_dv,
            safe_cast(
                identificador_matriz_filial as string
            ) identificador_matriz_filial,
            safe_cast(nome_fantasia as string) nome_fantasia,
            safe_cast(cast(situacao_cadastral as int64) as string) situacao_cadastral,
            safe_cast(data_situacao_cadastral as date) data_situacao_cadastral,
            safe_cast(
                regexp_replace(motivo_situacao_cadastral, '^0', '') as string
            ) motivo_situacao_cadastral,
            safe_cast(nome_cidade_exterior as string) nome_cidade_exterior,
            safe_cast(cast(id_pais as int64) as string) id_pais,
            safe_cast(data_inicio_atividade as date) data_inicio_atividade,
            safe_cast(cnae_fiscal_principal as string) cnae_fiscal_principal,
            safe_cast(cnae_fiscal_secundaria as string) cnae_fiscal_secundaria,
            safe_cast(sigla_uf as string) sigla_uf,
            safe_cast(safe_cast(id_municipio_rf as numeric) as string) id_municipio_rf,
            safe_cast(tipo_logradouro as string) tipo_logradouro,
            safe_cast(logradouro as string) logradouro,
            safe_cast(numero as string) numero,
            safe_cast(complemento as string) complemento,
            safe_cast(bairro as string) bairro,
            safe_cast(replace (cep, ".0", "") as string) cep,
            safe_cast(ddd_1 as string) ddd_1,
            safe_cast(telefone_1 as string) telefone_1,
            safe_cast(ddd_2 as string) ddd_2,
            safe_cast(telefone_2 as string) telefone_2,
            safe_cast(ddd_fax as string) ddd_fax,
            safe_cast(fax as string) fax,
            safe_cast(lower(email) as string) email,
            safe_cast(situacao_especial as string) situacao_especial,
            safe_cast(data_modificacao as date) data_modificacao,
            safe_cast(data_situacao_especial as date) data_situacao_especial
        from {{ set_datalake_project("br_rf_cnpj_staging.estabelecimentos") }}
        {% if is_incremental() %}
            where
                data_referencia
                > format_date('%Y-%m', (select max(data_referencia) from {{ this }}))
        -- Dados históricos até 2023-04-30 foram migrados do modelo
        -- br_me_cnpj.estabelecimentos
        {% else %}
            union all
            select
                safe_cast(data as date) data_referencia,
                safe_cast(lpad(cnpj, 14, "0") as string) cnpj,
                safe_cast(lpad(cnpj_basico, 8, '0') as string) cnpj_basico,
                safe_cast(lpad(cnpj_ordem, 4, '0') as string) cnpj_ordem,
                safe_cast(lpad(cnpj_dv, 2, '0') as string) cnpj_dv,
                safe_cast(
                    identificador_matriz_filial as string
                ) identificador_matriz_filial,
                safe_cast(nome_fantasia as string) nome_fantasia,
                safe_cast(
                    cast(situacao_cadastral as int64) as string
                ) situacao_cadastral,
                safe_cast(data_situacao_cadastral as date) data_situacao_cadastral,
                safe_cast(
                    regexp_replace(motivo_situacao_cadastral, '^0', '') as string
                ) motivo_situacao_cadastral,
                safe_cast(nome_cidade_exterior as string) nome_cidade_exterior,
                safe_cast(cast(id_pais as int64) as string) id_pais,
                safe_cast(data_inicio_atividade as date) data_inicio_atividade,
                safe_cast(cnae_fiscal_principal as string) cnae_fiscal_principal,
                safe_cast(cnae_fiscal_secundaria as string) cnae_fiscal_secundaria,
                safe_cast(sigla_uf as string) sigla_uf,
                safe_cast(
                    safe_cast(id_municipio_rf as numeric) as string
                ) id_municipio_rf,
                safe_cast(tipo_logradouro as string) tipo_logradouro,
                safe_cast(logradouro as string) logradouro,
                safe_cast(numero as string) numero,
                safe_cast(complemento as string) complemento,
                safe_cast(bairro as string) bairro,
                safe_cast(replace (cep, ".0", "") as string) cep,
                safe_cast(ddd_1 as string) ddd_1,
                safe_cast(telefone_1 as string) telefone_1,
                safe_cast(ddd_2 as string) ddd_2,
                safe_cast(telefone_2 as string) telefone_2,
                safe_cast(ddd_fax as string) ddd_fax,
                safe_cast(fax as string) fax,
                safe_cast(lower(email) as string) email,
                safe_cast(situacao_especial as string) situacao_especial,
                safe_cast(null as date) data_modificacao,
                safe_cast(data_situacao_especial as date) data_situacao_especial
            from
                {{ set_datalake_project("br_rf_cnpj_staging.estabelecimentos_legado") }}
            where safe_cast(data as date) <= date("2023-04-30")
        {% endif %}
    )
select
    a.data_referencia,
    a.cnpj,
    a.cnpj_basico,
    a.cnpj_ordem,
    a.cnpj_dv,
    a.identificador_matriz_filial,
    a.nome_fantasia,
    a.situacao_cadastral,
    a.data_situacao_cadastral,
    a.motivo_situacao_cadastral,
    a.nome_cidade_exterior,
    a.id_pais,
    a.data_inicio_atividade,
    a.cnae_fiscal_principal,
    a.cnae_fiscal_secundaria,
    a.sigla_uf,
    safe_cast(b.id_municipio as string) id_municipio,
    a.id_municipio_rf,
    a.tipo_logradouro,
    a.logradouro,
    a.numero,
    a.complemento,
    a.bairro,
    a.cep,
    a.ddd_1,
    a.telefone_1,
    a.ddd_2,
    a.telefone_2,
    a.ddd_fax,
    a.fax,
    a.email,
    a.situacao_especial,
    a.data_modificacao,
    a.data_situacao_especial
from cnpj_estabelecimentos a
left join
    basedosdados.br_bd_diretorios_brasil.municipio b
    on safe_cast(safe_cast(a.id_municipio_rf as numeric) as string) = b.id_municipio_rf
