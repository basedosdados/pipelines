{% test regex_or_null(model, column_name, pattern) %}

    {#- Marca como falha, linhas cujos valores não correspondem ao padrão.
        A severidade é definida através de `config`, por exemplo, `severity: warn`. -#}
    select {{ column_name }} as invalid_value
    from {{ model }}
    where
        {{ column_name }} is not null
        and not regexp_contains(cast({{ column_name }} as string), r'{{ pattern }}')
{% endtest %}
