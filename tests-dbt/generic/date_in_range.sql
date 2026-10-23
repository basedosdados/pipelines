{% test date_in_range(model, column_name, min_date="1900-01-01") %}

    {#- Mesma lógica de validate_date_range (min_date <= value <= today),
        mas ao invés de anular linhas incompatíveis, reporta as linhas que não cumprem o critério. -#}
    select {{ column_name }} as invalid_date
    from {{ model }}
    where
        {{ column_name }} is not null
        and (
            date({{ column_name }}) < date('{{ min_date }}')
            or date({{ column_name }}) > current_date()
        )
{% endtest %}
