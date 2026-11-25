{% test custom_matching_rows_limit(model, condition, max_share=none, max_rows=none) %}

    {% if max_share is none and max_rows is none %}
        {{
            exceptions.raise_compiler_error(
                "custom_matching_rows_limit precisa de max_share ou max_rows"
            )
        }}
    {% endif %}

    with
        counts as (
            select countif({{ condition }}) as matching_rows, count(*) as total_rows
            from {{ model }}
        )

    select
        '{{ condition }}' as checked_condition,
        matching_rows,
        total_rows,
        safe_divide(matching_rows, total_rows) as share
    from counts
    where
        false
        {% if max_share is not none %}
            or safe_divide(matching_rows, total_rows) > {{ max_share }}
        {% endif %}
        {% if max_rows is not none %} or matching_rows > {{ max_rows }} {% endif %}

{% endtest %}
