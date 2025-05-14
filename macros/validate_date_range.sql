{% macro validate_date_range(column_name, start_date, end_date=None) %}

    {% if end_date is none %}
        case
            when {{ column_name }} is null
            then null
            when
                date({{ column_name }}) >= date('{{ start_date }}')
                and date({{ column_name }}) <= current_date()
            then safe_cast({{ column_name }} as date)
            else null
        end
    {% else %}
        case
            when {{ column_name }} is null
            then null
            when
                date({{ column_name }}) >= date('{{ start_date }}')
                and date({{ column_name }}) <= date('{{ end_date }}')
            then safe_cast({{ column_name }} as date)
            else null
        end
    {% endif %}

{% endmacro %}
