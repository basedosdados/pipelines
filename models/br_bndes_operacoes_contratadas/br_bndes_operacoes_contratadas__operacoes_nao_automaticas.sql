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
    "data_contratacao",
] %}

{% set float_cols = ["valor_operacao", "valor_desembolsado", "taxa_juros"] %}

{% set int_cols = [
    "ano",
    "prazo_carencia",
    "prazo_amortizacao",
    "indicador_inovacao",
] %}

{% set string_cols = [
    "sigla_uf",
    "id_municipio",
    "id_contrato",
    "cnpj_cliente",
    "nome_cliente",
    "porte_cliente",
    "natureza_cliente",
    "descricao_projeto",
    "fonte_recurso",
    "custo_financeiro",
    "modalidade_apoio",
    "forma_apoio",
    "produto",
    "instrumento_financeiro",
    "tipo_garantia",
    "tipo_excepcionalidade",
    "area_operacional",
    "setor_cnae",
    "subsetor_cnae_agrupado",
    "secao_cnae",
    "divisao_cnae",
    "grupo_cnae",
    "classe_cnae",
    "subclasse_cnae",
    "descricao_cnae",
    "setor_bndes",
    "subsetor_bndes",
    "nome_instituicao_financeira_credenciada",
    "cnpj_instituicao_financeira_credenciada",
    "situacao_operacao",
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
        {% endif %}
        {% if not loop.last %}, {% endif %}
    {% endfor %}
from {{ source_table }}
