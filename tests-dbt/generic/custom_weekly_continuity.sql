{% test custom_weekly_continuity(model, column_name, min_rows_per_week=1) %}

    -- falha para cada semana, entre a primeira e a última, sem dados ou com menos
    -- de min_rows_per_week linhas; a primeira e a última semana só precisam
    -- existir, pois podem estar cortadas na virada do ano
    with
        semanas as (
            select date_trunc({{ column_name }}, isoweek) as semana, count(*) as linhas
            from {{ model }}
            where {{ column_name }} is not null
            group by 1
        ),
        limites as (select min(semana) as inicio, max(semana) as fim from semanas),
        esperadas as (
            select semana, inicio, fim
            from
                limites,
                unnest(generate_date_array(inicio, fim, interval 1 week)) as semana
        )
    select esperadas.semana, coalesce(semanas.linhas, 0) as linhas
    from esperadas
    left join semanas using (semana)
    where
        semanas.semana is null
        or (
            esperadas.semana > esperadas.inicio
            and esperadas.semana < esperadas.fim
            and semanas.linhas < {{ min_rows_per_week }}
        )

{% endtest %}
