-- https://github.com/basedosdados/pipelines/wiki/Incluindo-testes-no-seu-modelo#where--__most_recent_year_month__--__most_recent_date__--__most_recent_year__
{% macro get_where_subquery(relation) %}
    {% set where = config.get("where", "") %}

    {% if where %}
        {% set max_year_query = "" %}
        {% set max_date_query = "" %}
        {% set max_year = "" %}
        {% set max_date = "" %}

        {# This block looks for __most_recent_year__  placeholder #}
        {% if "__most_recent_year__" in where %}
            {% set max_year_query = (
                "select max(cast(ano as int64)) as max_year from " ~ relation
            ) %}
            {% set max_year_result = run_query(max_year_query) %}
            {% if execute and max_year_result.rows[0][0] %}
                {% set max_year = max_year_result.rows[0][0] %}
                {# ano is INT64 (partition column), so compare to an unquoted
                   integer literal — matching __most_recent_year_en__ and
                   __most_recent_year_month__. Quoting it ('2026') raised
                   "No matching signature for operator = (INT64, STRING)". #}
                {% set where = where | replace(
                    "__most_recent_year__", "ano = " ~ max_year
                ) %}
                {% do log(
                    "The test will filter by the most recent year: "
                    ~ max_year,
                    info=True,
                ) %}
            {% endif %}
        {% endif %}

        {# Same as __most_recent_year__, for datasets whose partition column is the
           English `year` instead of `ano` (us_*, world_*). #}
        {% if "__most_recent_year_en__" in where %}
            {% set max_year_query = (
                "select max(cast(year as int64)) as max_year from " ~ relation
            ) %}
            {% set max_year_result = run_query(max_year_query) %}
            {% if execute and max_year_result.rows[0][0] %}
                {% set max_year = max_year_result.rows[0][0] %}
                {% set where = where | replace(
                    "__most_recent_year_en__", "year = " ~ max_year
                ) %}
                {% do log(
                    "The test will filter by the most recent year: "
                    ~ max_year,
                    info=True,
                ) %}
            {% endif %}
        {% endif %}

        {# This block looks for __most_recent_date__  placeholder #}
        {% if "__most_recent_date__" in where %}
            {% set max_date_query = "select max(data) as max_date from " ~ relation %}
            {% set max_date_result = run_query(max_date_query) %}
            {% if execute and max_date_result.rows[0][0] %}
                {% set max_date = max_date_result.rows[0][0] %}
                {% set where = where | replace(
                    "__most_recent_date__", "data = '" ~ max_date ~ "'"
                ) %}
                {% do log(
                    "The test will filter by the most recent date: "
                    ~ max_date,
                    info=True,
                ) %}
            {% endif %}
        {% endif %}

        {# This block looks for __most_recent_date_cnpj__  placeholder #}
        {% if "__most_recent_date_cnpj__" in where %}
            {% set max_date_query = (
                "select max(data_referencia) as max_date from " ~ relation
            ) %}
            {% set max_date_result = run_query(max_date_query) %}
            {% if execute and max_date_result.rows[0][0] %}
                {% set max_date = max_date_result.rows[0][0] %}
                {% set where = where | replace(
                    "__most_recent_date_cnpj__",
                    "data_referencia = '" ~ max_date ~ "'",
                ) %}
                {% do log(
                    "The test will filter by the most recent date: "
                    ~ max_date,
                    info=True,
                ) %}
            {% endif %}
        {% endif %}

        {% if "__most_recent_date_cno__" in where %}
            {% set max_date_query = (
                "select max(data_extracao) as max_date from " ~ relation
            ) %}
            {% set max_date_result = run_query(max_date_query) %}
            {% if execute and max_date_result.rows[0][0] %}
                {% set max_date = max_date_result.rows[0][0] %}
                {% set where = where | replace(
                    "__most_recent_date_cno__",
                    "data_extracao = '" ~ max_date ~ "'",
                ) %}
                {% do log(
                    "The test will filter by the most recent date: "
                    ~ max_date,
                    info=True,
                ) %}
            {% endif %}
        {% endif %}

        {# This block looks for __most_recent_year_month__  placeholder #}
        {% if "__most_recent_year_month__" in where %}
            {% set max_date_query = (
                "select format_date('%Y-%m', max(date(cast(ano as int64), cast(mes as int64), 1))) as max_date from "
                ~ relation
            ) %}
            {% set max_date_result = run_query(max_date_query) %}

            {% if execute %}
                {% set max_date = max_date_result.rows[0][0] %}
                {% set max_year = max_date[:4] %}
                {% set max_month = max_date[5:7] %}

                {# Replace placeholder in the where config with actual maximum year and month #}
                {% set where = where | replace(
                    "__most_recent_year_month__",
                    "ano = " ~ max_year ~ " and mes = " ~ max_month,
                ) %}
                {% do log(
                    "----- The test will be performed for: " ~ where, info=True
                ) %}

            {% endif %}
        {% endif %}

        {# This block looks for __most_recent_year_month_sicor__  placeholder #}
        {% if "__most_recent_year_month_sicor__" in where %}
            {% set max_date_query = (
                "select format_date('%Y-%m', max(date(cast(ano_emissao as int64), cast(mes_emissao as int64), 1))) as max_date from "
                ~ relation
            ) %}
            {% set max_date_result = run_query(max_date_query) %}

            {% if execute %}
                {% set max_date = max_date_result.rows[0][0] %}
                {% set max_year = max_date[:4] %}
                {% set max_month = max_date[5:7] %}

                {# Replace placeholder in the where config with actual maximum year and month #}
                {% set where = where | replace(
                    "__most_recent_year_month_sicor__",
                    "ano_emissao = "
                    ~ max_year
                    ~ " and mes_emissao = "
                    ~ max_month,
                ) %}
                {% do log(
                    "----- The test will be performed for: " ~ where, info=True
                ) %}

            {% endif %}
        {% endif %}

        {# Generic, column-parameterized placeholder: `__most_recent__(col)`.

           The blocks above each hardcode one column name, which is why this file
           grew a _cnpj, a _cno and a _sicor variant. This form takes the column
           from the placeholder itself, so a dataset partitioned on
           `extraction_date` or `snapshot_date` needs no new macro. Numbers are
           emitted unquoted (INT64 partitions such as `ano`/`year`); anything else
           is quoted (DATE partitions). #}
        {#- Captura o texto casado INTEIRO junto com a coluna. Reconstruir o
            alvo como "__most_recent__(" ~ column ~ ")" erra quando o
            placeholder tem espaço — `__most_recent__( col )` casa no regex,
            loga o max e NÃO é substituído, e o placeholder cru chega no
            BigQuery. Mesmo modo de falha silenciosa do escopo do `for`. -#}
        {% set placeholders = modules.re.findall(
            "(__most_recent__\\(\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*\\))", where
        ) %}
        {#- A namespace, not a plain `{% set where = ... %}`. Jinja gives every
            `{% for %}` its own scope, so an assignment made inside the loop is
            DISCARDED at `{% endfor %}` — unlike the `{% if %}` blocks above,
            which share the macro's scope and can rewrite `where` directly.
            Without this the loop still runs and still logs the max value, so
            the run looks right, while the raw placeholder reaches BigQuery and
            the test dies with `Function not found: __most_recent__`. -#}
        {% set ns = namespace(where=where) %}
        {% for matched, column in placeholders %}
            {% set max_query = (
                "select max(" ~ column ~ ") as max_value from " ~ relation
            ) %}
            {% set max_result = run_query(max_query) %}
            {% if execute and max_result.rows[0][0] is not none %}
                {% set max_value = max_result.rows[0][0] %}
                {% if max_value is number %}
                    {% set literal = column ~ " = " ~ max_value %}
                {% else %} {% set literal = column ~ " = '" ~ max_value ~ "'" %}
                {% endif %}
                {% set ns.where = ns.where | replace(matched, literal) %}
                {% do log(
                    "The test will filter by the most recent "
                    ~ column
                    ~ ": "
                    ~ max_value,
                    info=True,
                ) %}
            {% endif %}
        {% endfor %}
        {% set where = ns.where %}

        {# Return the filtered subquery #}
        {% set filtered = (
            "(select * from "
            ~ relation
            ~ " where "
            ~ where
            ~ ") dbt_subquery"
        ) %}
        {% do return(filtered) %}
    {% else %} {% do return(relation) %}
    {% endif %}
{% endmacro %}
