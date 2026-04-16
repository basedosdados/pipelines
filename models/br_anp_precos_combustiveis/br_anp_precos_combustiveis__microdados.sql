{{
    config(
        alias="microdados",
        schema="br_anp_precos_combustiveis",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2004, "end": 2023, "interval": 1},
        },
        cluster_by=["id_municipio", "sigla_uf"],
        labels={"project_id": "basedosdados-dev"},
        pre_hook="             BEGIN                 DROP ALL ROW ACCESS POLICIES ON {{ this }};             EXCEPTION WHEN ERROR THEN                 SELECT 1;              END;         ",
    )
}}
with
    anp as (
        select
            safe_cast(ano as int64) ano,
            safe_cast(sigla_uf as string) sigla_uf,
            safe_cast(id_municipio as string) id_municipio,
            initcap(bairro_revenda) as bairro_revenda,
            safe_cast(cep_revenda as string) cep_revenda,
            initcap(endereco_revenda) as endereco_revenda,
            replace(
                replace(replace(cnpj_revenda, "/", ""), "-", ""), ".", ""
            ) as cnpj_revenda,
            initcap(nome_estabelecimento) as nome_estabelecimento,
            initcap(bandeira_revenda) as bandeira_revenda,
            safe_cast(data_coleta as date) data_coleta,
            initcap(produto) as produto,
            safe_cast(unidade_medida as string) unidade_medida,
            safe_cast(preco_compra as float64) preco_compra,
            safe_cast(preco_venda as float64) preco_venda
        from
            {{ set_datalake_project("br_anp_precos_combustiveis_staging.microdados") }}
            as t
    )
select *
from anp
{% if is_incremental() %}
    where data_coleta > (select max(data_coleta) from {{ this }})
{% endif %}
