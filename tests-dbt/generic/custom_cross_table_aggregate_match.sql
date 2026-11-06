{% test custom_cross_table_aggregate_match(
    model,
    compare_model,
    expression,
    compare_expression,
    group_by,
    proportion_allowed_difference=0.0
) %}

    {#- Compara, por grupo, uma agregação do modelo com a de outro modelo, no
        ano mais recente presente nos dois. O ano é buscado antes para que a
        consulta leia só a partição dele. -#}
    {% set ano = 0 %}
    {% if execute %}
        {% set year_query %}
            select least(
                (select max(ano) from {{ model }}),
                (select max(ano) from {{ compare_model }})
            )
        {% endset %}
        {% set ano = run_query(year_query).rows[0][0] | int %}
    {% endif %}

    with
        tested as (
            select {{ group_by }} as grupo, {{ expression }} as valor
            from {{ model }}
            where ano = {{ ano }}
            group by 1
        ),

        compared as (
            select {{ group_by }} as grupo, {{ compare_expression }} as valor_comparado
            from {{ compare_model }}
            where ano = {{ ano }}
            group by 1
        )

    select
        {{ ano }} as ano,
        grupo,
        coalesce(valor, 0) as valor,
        coalesce(valor_comparado, 0) as valor_comparado
    from tested
    full outer join compared using (grupo)
    where
        abs(coalesce(valor, 0) - coalesce(valor_comparado, 0))
        > {{ proportion_allowed_difference }} * abs(coalesce(valor_comparado, 0))

{% endtest %}
