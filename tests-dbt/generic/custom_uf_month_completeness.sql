{#
    Falha quando uma UF some ou encolhe num mês recente.

    Para cada UF e cada mês dos últimos `window_months` meses (contados a
    partir do mês mais recente da tabela), compara o número de linhas com a
    mediana dos `baseline_months` meses anteriores da mesma UF. Retorna as
    UF-mês com zero linhas ou com menos de `min_share` dessa mediana.

    As `grace_months` competências mais recentes ficam de fora: o DATASUS
    publica o mês primeiro com UFs faltando ou parciais e o completa depois, e
    o pipeline recarrega esses meses quando isso acontece. Um buraco que
    sobrevive à carência é falha de carga, e o teste falha.

    Uma UF que não aparece num mês entra com zero linhas, porque a grade de
    meses × UFs é montada à parte e não a partir dos dados.

    O filtro por `current_date()` limita a leitura às partições recentes.
#}
{% test custom_uf_month_completeness(
    model,
    year_column="ano",
    month_column="mes",
    uf_column="sigla_uf",
    window_months=24,
    baseline_months=12,
    min_share=0.5,
    grace_months=2
) %}

    {%- set ufs = [
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
    ] -%}
    {%- set lookback_years = ((window_months + baseline_months) // 12) + 2 -%}

    with
        counts as (
            select
                date({{ year_column }}, {{ month_column }}, 1) as competencia,
                {{ uf_column }} as uf,
                count(*) as n
            from {{ model }}
            where
                {{ year_column }}
                >= extract(year from current_date()) - {{ lookback_years }}
            group by 1, 2
        ),

        max_month as (select max(competencia) as competencia from counts),

        grid as (
            select competencia, uf
            from
                max_month,
                unnest(
                    generate_date_array(
                        date_sub(
                            max_month.competencia,
                            interval {{ window_months + baseline_months }} month
                        ),
                        max_month.competencia,
                        interval 1 month
                    )
                ) as competencia,
                unnest({{ ufs | tojson }}) as uf
        ),

        filled as (
            select grid.competencia, grid.uf, coalesce(counts.n, 0) as n
            from grid
            left join counts using (competencia, uf)
        ),

        scored as (
            select
                atual.competencia,
                atual.uf,
                atual.n,
                approx_quantiles(anterior.n, 2)[offset(1)] as mediana_anterior
            from filled as atual
            join
                filled as anterior
                on anterior.uf = atual.uf
                and anterior.competencia between date_sub(
                    atual.competencia, interval {{ baseline_months }} month
                ) and date_sub(atual.competencia, interval 1 month)
            group by 1, 2, 3
        )

    select scored.*
    from scored, max_month
    where
        scored.competencia
        > date_sub(max_month.competencia, interval {{ window_months }} month)
        and scored.competencia
        <= date_sub(max_month.competencia, interval {{ grace_months }} month)
        and (scored.n = 0 or scored.n < {{ min_share }} * scored.mediana_anterior)

{% endtest %}
