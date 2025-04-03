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
