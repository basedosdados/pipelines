{{
    config(
        alias="microdados",
        schema="br_rf_cno",
        materialized="incremental",
        unique_key="id_cno",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
        },
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    )
}}
with
    safe_select as (
        select
            safe_cast(data as date) as data_extracao,
            safe_cast(data_situacao as date) as data_situacao,
            safe_cast(data_registro as date) as data_registro,
            safe_cast(data_inicio as date) as data_inicio,
            safe_cast(
                data_inicio_responsabilidade as date
            ) as data_inicio_responsabilidade,
            safe_cast(id_pais as string) as id_pais,
            safe_cast(nome_pais as string) as nome_pais,
            safe_cast(sigla_uf as string) as sigla_uf,
            safe_cast(b.id_municipio as string) as id_municipio,
            safe_cast(id_cno as string) as id_cno,
            safe_cast(id_cno_vinculado as string) as id_cno_vinculado,
            safe_cast(ltrim(situacao, '0') as string) as situacao,
            safe_cast(id_responsavel as string) as id_responsavel,
            safe_cast(nome_responsavel as string) as nome_responsavel,
            safe_cast(
                ltrim(qualificacao_responsavel, '0') as string
            ) as qualificacao_responsavel,
            safe_cast(nome_empresarial as string) as nome_empresarial,
            safe_cast(area as float64) as area,
            safe_cast(unidade_medida as string) as unidade_medida,
            safe_cast(bairro as string) as bairro,
            safe_cast(cep as string) as cep,
            safe_cast(logradouro as string) as logradouro,
            safe_cast(tipo_logradouro as string) as tipo_logradouro,
            safe_cast(numero_logradouro as string) as numero_logradouro,
            safe_cast(complemento as string) as complemento,
            safe_cast(caixa_postal as string) as caixa_postal
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
    )
select
    data_extracao,
    data_situacao,
    data_registro,
    data_inicio,
    data_inicio_responsabilidade,
    {% set cols = [
        "id_pais",
        "nome_pais",
        "sigla_uf",
        "id_municipio",
        "id_cno",
        "id_cno_vinculado",
        "situacao",
        "id_responsavel",
        "nome_responsavel",
        "qualificacao_responsavel",
        "nome_empresarial",
    ] %}
    {% for col in cols %}
        {{ validate_null_cols(col) }} as {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %},
    area,
    {% set cols = [
        "unidade_medida",
        "bairro",
        "cep",
        "logradouro",
        "tipo_logradouro",
        "numero_logradouro",
        "complemento",
        "caixa_postal",
    ] %}
    {% for col in cols %}
        {{ validate_null_cols(col) }} as {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %}
from safe_select
