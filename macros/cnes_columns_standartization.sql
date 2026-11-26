{%- macro clean_cols(df_col) -%}
    {# pass a column that will be standardized #}
    {%- set cleaned_col = (
        df_col
        | replace(",", "")
        | replace("¿", "")
        | replace("ª", "")
        | replace("º", "")
    ) -%}
    {{ cleaned_col }}
{% endmacro %}

{%- macro cnes_staging_nan_as_null(
    table_path, partition_columns=["ano", "mes", "sigla_uf"]
) -%}
    {%- set path = set_datalake_project(table_path) | trim | replace("`", "") -%}
    {%- set parts = path.split(".") -%}
    {%- set relation = api.Relation.create(
        database=parts[0], schema=parts[1], identifier=parts[2]
    ) -%}
    {%- if execute -%}
        (
            select
                {%- for column in adapter.get_columns_in_relation(relation) %}
                    {% if column.dtype == "STRING" and column.name not in partition_columns -%}
                        nullif({{ adapter.quote(column.name) }}, 'nan')
                    {%- else -%} {{ adapter.quote(column.name) }}
                    {%- endif %} as {{ adapter.quote(column.name) }}
                    {%- if not loop.last %},{% endif %}
                {%- endfor %}
            from {{ relation }}
        )
    {%- else -%} {{ relation }}
    {%- endif -%}
{%- endmacro %}
