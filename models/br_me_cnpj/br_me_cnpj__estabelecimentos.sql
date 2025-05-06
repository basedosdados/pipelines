{{
    config(
        schema="br_me_cnpj",
        alias="estabelecimentos",
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
        },
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    )
}}
with
    cnpj_estabelecimentos as (
        select
            safe_cast(data as date) data,
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
            safe_cast(motivo_situacao_cadastral as string) motivo_situacao_cadastral,
            safe_cast(nome_cidade_exterior as string) nome_cidade_exterior,
            safe_cast(cast(id_pais as int64) as string) id_pais,
            safe_cast(data_inicio_atividade as date) data_inicio_atividade,
            safe_cast(cnae_fiscal_principal as string) cnae_fiscal_principal,
            safe_cast(cnae_fiscal_secundaria as string) cnae_fiscal_secundaria,
            safe_cast(a.sigla_uf as string) sigla_uf,
            safe_cast(b.id_municipio as string) id_municipio,
            safe_cast(
                safe_cast(a.id_municipio_rf as numeric) as string
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
            safe_cast(data_situacao_especial as date) data_situacao_especial
        from `basedosdados-staging.br_me_cnpj_staging.estabelecimentos` a
        left join
            basedosdados.br_bd_diretorios_brasil.municipio b
            on safe_cast(safe_cast(a.id_municipio_rf as numeric) as string)
            = b.id_municipio_rf
    )
select *
from cnpj_estabelecimentos
{% if is_incremental() %} where data > (select max(data) from {{ this }}) {% endif %}
