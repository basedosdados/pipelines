-- https://github.com/basedosdados/pipelines/wiki/Incluindo-testes-no-seu-modelo#dicionários
{% test custom_dictionary_coverage(
    model, dictionary_model, columns_covered_by_dictionary
) %}
    {{ config(severity="error") }}

    {%- set combined_query_parts = [] -%}
    {%- set union_parts = [] -%}

    {% set model_sql = model | string %}
    {% set start = model_sql.find("`") + 1 %}
    {% set end = model_sql.rfind("`") %}
    {% set full_path = model_sql[start:end] %}
    {% set path_parts = full_path.split(".") %}
    {% set table_id = path_parts[2].replace("`", "").strip() %}

    {%- for column_name in columns_covered_by_dictionary %}
        {% set subquery_name = "exceptions_" ~ loop.index %}
        {% set left_table_name = "data_table_" ~ loop.index %}
        {% set right_table_name = "dict_table_" ~ loop.index %}

        {% set subquery %}
            {{ left_table_name }} as (
                select {{ column_name }} as id
                from {{ model }}
                where {{ column_name }} is not null
            ),
            {{ right_table_name }} as (
                select chave
                from {{ dictionary_model}}
                where valor is not null
                and id_tabela = '{{ table_id }}'
                and nome_coluna = '{{ column_name }}'
            ),
            {{ subquery_name }} as (
                select '{{ column_name }}' as failed_column, id as missing_value
                from {{ left_table_name }}
                left join {{ right_table_name }} on {{ left_table_name }}.id = {{ right_table_name }}.chave
                where {{ right_table_name }}.chave is null
            )
        {% endset %}

        {%- do combined_query_parts.append(subquery) -%}
        {%- do union_parts.append(subquery_name) -%}
    {%- endfor %}

    {# Combine all CTEs into a single WITH clause and then union all results #}
    {% set final_query %}
        with
        {{ combined_query_parts | join(', ') }}

        select distinct failed_column, missing_value from {{ union_parts | join(' union all select distinct failed_column, missing_value from ') }}
    {% endset %}

    {{ return(final_query) }}

{% endtest %}
