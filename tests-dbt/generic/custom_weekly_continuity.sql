{% test custom_weekly_continuity(model, column_name) %}

    -- falha para cada semana, entre a primeira e a última, sem nenhuma data
    with
        semanas as (
            select distinct date_trunc({{ column_name }}, isoweek) as semana
            from {{ model }}
            where {{ column_name }} is not null
        ),
        limites as (select min(semana) as inicio, max(semana) as fim from semanas),
        esperadas as (
            select semana
            from
                limites,
                unnest(generate_date_array(inicio, fim, interval 1 week)) as semana
        )
    select semana as semana_sem_dados
    from esperadas
    where semana not in (select semana from semanas)

{% endtest %}
