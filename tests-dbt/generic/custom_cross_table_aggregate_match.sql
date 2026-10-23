{% test custom_cross_table_aggregate_match(
    model,
    compare_model,
    expression,
    compare_expression,
    group_by,
    year_column="ano",
    proportion_allowed_difference=0.0
) %}

    {#- Compara, por grupo, uma agregação do modelo com a de outro modelo, no
        ano mais recente presente nos dois. -#}
    {% set tested_year = 0 %}
    {% if execute %}
        {% set year_query %}
            select least(
                (select max({{ year_column }}) from {{ model }}),
                (select max({{ year_column }}) from {{ compare_model }})
            )
        {% endset %}
        {% set tested_year = run_query(year_query).rows[0][0] | int %}
        {% do log(
            "----- Comparação entre "
            ~ model
            ~ " e "
            ~ compare_model
            ~ " em "
            ~ year_column
            ~ " = "
            ~ tested_year,
            info=True,
        ) %}
    {% endif %}

    with
        tested as (
            select {{ group_by }} as grupo, {{ expression }} as valor
            from {{ model }}
            where {{ year_column }} = {{ tested_year }}
            group by 1
        ),

        compared as (
            select {{ group_by }} as grupo, {{ compare_expression }} as valor_comparado
            from {{ compare_model }}
            where {{ year_column }} = {{ tested_year }}
            group by 1
        )

    select
        {{ tested_year }} as {{ year_column }},
        grupo,
        valor,
        valor_comparado,
        safe_divide(valor, valor_comparado) - 1 as diferenca
    from tested
    full outer join compared using (grupo)
    where
        valor is null
        or valor_comparado is null
        or (valor_comparado = 0 and valor != 0)
        or abs(safe_divide(valor, valor_comparado) - 1)
        > {{ proportion_allowed_difference }}

{% endtest %}
