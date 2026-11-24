-- register flow
{{
    config(
        alias="microdados",
        schema="br_anp_precos_combustiveis",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2004, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf"],
        labels={"project_id": "basedosdados-dev"},
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
        {% if is_incremental() %}
                -- regrava os anos da janela de 4 semanas do arquivo da anp
                {% set query_inicio %}
                select format_date("%Y-01-01", date_sub(max(data_coleta), interval 35 day))
                from {{ this }}
                {% endset %}
                {% set inicio = (
                    run_query(query_inicio).columns[0].values()[0]
                    if execute
                    else "2004-01-01"
                ) %}
            where t.data_coleta >= "{{ inicio }}"
        {% endif %}
    )
select *
from anp
