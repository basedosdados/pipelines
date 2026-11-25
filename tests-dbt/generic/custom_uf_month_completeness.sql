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

    O teste lê só os `window_months + baseline_months` meses (24 com os
    padrões) que terminam no mês mais recente da tabela. Antes da consulta,
    ele busca a partição mais recente com linhas em
    `INFORMATION_SCHEMA.PARTITIONS` e põe o início da leitura no `where` como
    valor fixo, para o BigQuery podar as partições. Com `date_column` e
    partição por mês ou dia, a leitura começa no primeiro mês necessário. Com
    partição por ano, as partições só dizem o ano mais recente, e o teste
    conta os meses a partir de janeiro desse ano: lê no máximo um ano a mais
    do que precisa.

    Requisitos da tabela:
    - partição pela coluna que o teste lê: `date_column` (por mês ou dia) ou
      `year_column` (por ano). Uma tabela sem partição de ano, mês ou dia com
      linhas faz o teste falhar com erro, em vez de passar sem conferir nada;
    - nenhum `where` na configuração do teste, que consulta as partições da
      própria tabela;
    - as 27 UFs aparecem todo mês; uma tabela com menos UFs reprova sempre;
    - `uf_column` traz a sigla em maiúscula. A grade de meses × UFs é montada
      à parte, com as 27 siglas, e não a partir dos dados: uma UF que não
      aparece num mês entra com zero linhas, e `pa` conta como `PA` ausente.

    Parâmetros: `year_column` ("ano"), `month_column` ("mes") e `uf_column`
    ("sigla_uf") nomeiam as colunas; `date_column` (nenhuma) substitui ano e
    mês; `window_months` (12), `baseline_months` (12), `min_share` (0.5) e
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
    window_months=12,
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
    {%- set months_read = window_months + baseline_months -%}
    {%- set read_start = "1900-01-01" -%}

    {%- if execute -%}
        {%- if model.identifier is not defined -%}
            {{
                exceptions.raise_compiler_error(
                    "custom_uf_month_completeness não aceita where: "
                    ~ "o teste consulta as partições da própria tabela"
                )
            }}
        {%- endif -%}
        {%- set read_start_query -%}
            select
                format_date(
                    '%Y-%m-%d',
                    date_sub(
                        case
                            length(max(partition_id))
                            when 4
                            then parse_date('%Y', max(partition_id))
                            when 6
                            then parse_date('%Y%m', max(partition_id))
                            else
                                date_trunc(
                                    parse_date('%Y%m%d', max(partition_id)),
                                    month
                                )
                        end,
                        interval {{ months_read - 1 }} month
                    )
                )
            from
                `{{ model.database }}`.`{{ model.schema }}`.INFORMATION_SCHEMA.PARTITIONS
            where
                table_name = '{{ model.identifier }}'
                and total_rows > 0
                and regexp_contains(partition_id, r'^[0-9]{4}([0-9]{2}){0,2}$')
        {%- endset -%}
        {%- set read_start = run_query(read_start_query).rows[0][0] -%}
        {%- if read_start is none -%}
            {{
                exceptions.raise_compiler_error(
                    "custom_uf_month_completeness: "
                    ~ model
                    ~ " não tem partição de ano, mês ou dia com linhas"
                )
            }}
        {%- endif -%}
    {%- endif -%}

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
                {% if date_column %} {{ date_column }} >= '{{ read_start }}'
                {% else %} {{ year_column }} >= {{ read_start[:4] | int }}
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
                            max_month.competencia, interval {{ months_read - 1 }} month
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
