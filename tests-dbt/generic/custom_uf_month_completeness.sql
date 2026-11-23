{#
    Falha quando uma UF some ou encolhe num mês recente.

    Para cada UF e cada mês da janela conferida, compara o número de linhas
    com a mediana dos `baseline_months` meses anteriores da mesma UF. Retorna
    as UF-mês com zero linhas ou com menos de `min_share` dessa mediana, com
    as colunas `competencia`, `uf`, `n` e `mediana_anterior`.

    A janela conferida são os `window_months` meses que terminam no mês mais
    recente da tabela, menos as `grace_months` competências mais recentes.
    Com `window_months=1` e `grace_months=0`, só o mês mais recente é
    conferido; com `grace_months` igual a `window_months`, nenhum mês é.

    A carência serve a fontes que publicam o mês com UFs faltando ou parciais
    e o completam depois, quando o pipeline recarrega os meses recentes. Um
    buraco que sobrevive à carência é falha de carga.

    Requisitos da tabela:
    - as 27 UFs aparecem todo mês; uma tabela com menos UFs reprova sempre;
    - `uf_column` traz a sigla em maiúscula. A grade de meses × UFs é montada
      à parte, com as 27 siglas, e não a partir dos dados: uma UF que não
      aparece num mês entra com zero linhas, e `pa` conta como `PA` ausente.

    A leitura é limitada por `current_date()` às partições recentes, os
    últimos `(window_months + baseline_months) // 12 + 2` anos. Uma tabela
    sem dado nesse período passa sem conferir nada. Com `date_column`, a
    competência e o filtro saem dessa coluna, para a leitura ser podada numa
    tabela particionada por data.

    Parâmetros: `year_column` ("ano"), `month_column` ("mes") e `uf_column`
    ("sigla_uf") nomeiam as colunas; `date_column` (nenhuma) substitui ano e
    mês; `window_months` (24), `baseline_months` (12), `min_share` (0.5) e
    `grace_months` (2) seguem a descrição acima.

    Exemplo, conferindo só o mês mais recente:

        - custom_uf_month_completeness:
            min_share: 0.7
            window_months: 1
            grace_months: 0
#}
{% test custom_uf_month_completeness(
    model,
    year_column="ano",
    month_column="mes",
    uf_column="sigla_uf",
    window_months=24,
    baseline_months=12,
    min_share=0.5,
    grace_months=2,
    date_column=none
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
                {% if date_column %} date_trunc({{ date_column }}, month)
                {% else %} date({{ year_column }}, {{ month_column }}, 1)
                {% endif %} as competencia,
                {{ uf_column }} as uf,
                count(*) as n
            from {{ model }}
            where
                {% if date_column %}
                    {{ date_column }}
                    >= date_sub(current_date(), interval {{ lookback_years }} year)
                {% else %}
                    {{ year_column }}
                    >= extract(year from current_date()) - {{ lookback_years }}
                {% endif %}
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
