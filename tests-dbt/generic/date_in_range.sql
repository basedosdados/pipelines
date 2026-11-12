{% test date_in_range(
    model, column_name, min_date="1900-01-01", max_date="current_date()"
) %}

    {#- Reporta as linhas fora do intervalo min_date <= valor < max_date (max_date é exclusivo),
        ao invés de anular linhas incompatíveis como validate_date_range.
        `max_date` é uma expressão SQL (padrão: current_date()), por exemplo
        `date('2027-01-01')` para aceitar datas até o último dia de 2026. -#}
    select {{ column_name }} as invalid_date
    from {{ model }}
    where
        {{ column_name }} is not null
        and (
            date({{ column_name }}) < date('{{ min_date }}')
            or date({{ column_name }}) > {{ max_date }}
        )
{% endtest %}
