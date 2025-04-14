{{
    config(
        alias="microdados",
        schema="br_rf_cno",
        materialized="incremental",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
        },
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    )
}}

select
    safe_cast(data as date) data_extracao,
    safe_cast(data_situacao as date) data_situacao,
    safe_cast(data_registro as date) data_registro,
    safe_cast(data_inicio as date) data_inicio,
    safe_cast(data_inicio_responsabilidade as date) data_inicio_responsabilidade,
    safe_cast(id_pais as string) id_pais,
    safe_cast(nome_pais as string) nome_pais,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(b.id_municipio as string) id_municipio,
    safe_cast(id_cno as string) id_cno,
    safe_cast(id_cno_vinculado as string) id_cno_vinculado,
    safe_cast(ltrim(situacao, '0') as string) situacao,
    safe_cast(id_responsavel as string) id_responsavel,
    safe_cast(nome_responsavel as string) nome_responsavel,
    safe_cast(ltrim(qualificacao_responsavel, '0') as string) qualificacao_responsavel,
    safe_cast(nome_empresarial as string) nome_empresarial,
    safe_cast(area as float64) area,
    safe_cast(unidade_medida as string) unidade_medida,
    safe_cast(bairro as string) bairro,
    safe_cast(cep as string) cep,
    safe_cast(logradouro as string) logradouro,
    safe_cast(tipo_logradouro as string) tipo_logradouro,
    safe_cast(numero_logradouro as string) numero_logradouro,
    safe_cast(complemento as string) complemento,
    safe_cast(caixa_postal as string) caixa_postal,
from {{ set_datalake_project("br_rf_cno_staging.microdados") }} microdados
left join
    (
        select id_municipio, id_municipio_rf
        from `basedosdados.br_bd_diretorios_brasil.municipio`
    ) b
    on ltrim(microdados.id_municipio_rf, '0') = b.id_municipio_rf

{% if is_incremental() %}
    where safe_cast(data as date) > (select max(data_extracao) from {{ this }})
{% endif %}
