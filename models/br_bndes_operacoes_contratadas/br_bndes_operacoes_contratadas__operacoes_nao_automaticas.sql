{{
    config(
        alias="operacoes_nao_automaticas",
        schema="br_bndes_operacoes_contratadas",
        materialized="table",
    )
}}


{% set source_table = set_datalake_project(
    "br_bndes_operacoes_contratadas_staging.operacoes_nao_automaticas"
) %}

{% set path_clean = source_table | replace("`", "") %}
{% set path_parts = path_clean.split(".") %}

{% set source_relation = api.Relation.create(
    database=path_parts[0] | trim,
    schema=path_parts[1] | trim,
    identifier=path_parts[2] | trim,
) %}

{% set source_table_cols = adapter.get_columns_in_relation(source_relation) %}


{% set date_cols = [
    "data_apuracao",
    "data_contratacao",
] %}

{% set float_cols = ["taxa_juros", "valor_contratado", "valor_desembolsado"] %}

{% set int_cols = ["prazo_carencia", "prazo_amortizacao", "indicador_inovacao"] %}

{% set string_cols = [
    "id_municipio",
    "cnpj_cliente",
    "id_contrato",
    "sigla_uf",
    "nome_municipio",
    "razao_social_cliente",
    "situacao_contrato",
    "porte_cliente",
    "natureza_cliente",
    "descricao_projeto",
    "modalidade_apoio",
    "forma_apoio",
    "produto",
    "tipo_instrumento_financeiro",
    "tipo_garantia",
    "tipo_excepcionalidade",
    "tipo_fonte_recursos",
    "area_operacional_bndes",
    "setor_cnae_bndes",
    "subsetor_agrupado_cnae_bndes",
    "secao_cnae",
    "divisao_cnae",
    "grupo_cnae",
    "classe_cnae",
    "subclasse_cnae",
    "descricao_subclasse",
    "setor_bndes",
    "subsetor_bndes",
    "nome_instituicao_financeira_credenciada",
    "cnpj_instituicao_financeira_credenciada",
    "custo_financeiro",
] %}


select
    {% for col in source_table_cols %}
        {% if col.name in string_cols %}
            upper(
                safe_cast({{ validate_null_cols(col.name) }} as string)
            ) as {{ col.name }}
        {% elif col.name in date_cols %}
            safe_cast({{ col.name }} as date) as {{ col.name }}
        {% elif col.name in float_cols %}
            safe_cast({{ col.name }} as float64) as {{ col.name }}
        {% elif col.name in int_cols %}
            safe_cast({{ col.name }} as int64) as {{ col.name }}
        {% else %} {{ col.name }}
        {% endif %}
        {% if not loop.last %}, {% endif %}
    {% endfor %}
from {{ source_table }}
