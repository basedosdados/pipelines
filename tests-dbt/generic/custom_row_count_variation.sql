{% test custom_row_count_variation(
    model,
    history,
    min_monthly_change,
    max_monthly_change,
    min_annual_change,
    max_annual_change
) %}

    {% set tested_year_month = 0 %}
    {% if execute %}
        {% set tested_query %}
            select max(ano * 100 + mes) from {{ model }}
        {% endset %}
        {% set tested_year_month = run_query(tested_query).rows[0][0] | int %}
    {% endif %}

    {% set tested_year = tested_year_month // 100 %}
    {% set tested_month = tested_year_month % 100 %}

    {% if tested_month == 1 %}
        {% set previous_year = tested_year - 1 %} {% set previous_month = 12 %}
    {% else %}
        {% set previous_year = tested_year %}
        {% set previous_month = tested_month - 1 %}
    {% endif %}

    {% set last_year = tested_year - 1 %}

    {% if execute %}
        {% do log(
            "----- Volume de "
            ~ tested_year_month
            ~ " comparado com "
            ~ previous_year
            ~ "-"
            ~ previous_month
            ~ " e "
            ~ last_year
            ~ "-"
            ~ tested_month
            ~ " em "
            ~ history,
            info=True,
        ) %}
    {% endif %}

    with
        tested as (
            select count(*) as row_count
            from {{ model }}
            where ano = {{ tested_year }} and mes = {{ tested_month }}
        ),

        reference as (
            select
                'mensal' as comparison,
                {{ previous_year }} as reference_ano,
                {{ previous_month }} as reference_mes,
                {{ min_monthly_change }} as min_change,
                {{ max_monthly_change }} as max_change,
                (
                    select count(*)
                    from `{{ history }}`
                    where ano = {{ previous_year }} and mes = {{ previous_month }}
                ) as row_count
            union all
            select
                'anual',
                {{ last_year }},
                {{ tested_month }},
                {{ min_annual_change }},
                {{ max_annual_change }},
                (
                    select count(*)
                    from `{{ history }}`
                    where ano = {{ last_year }} and mes = {{ tested_month }}
                )
        ),

        compared as (
            select
                reference.comparison,
                {{ tested_year }} as ano,
                {{ tested_month }} as mes,
                tested.row_count,
                reference.reference_ano,
                reference.reference_mes,
                reference.row_count as reference_row_count,
                safe_divide(tested.row_count, reference.row_count) - 1 as change,
                reference.min_change,
                reference.max_change
            from reference
            cross join tested
        )

    select
        *,
        if(
            reference_row_count = 0, 'referência sem linhas', 'variação fora da faixa'
        ) as problem
    from compared
    where reference_row_count = 0 or change not between min_change and max_change

{% endtest %}
