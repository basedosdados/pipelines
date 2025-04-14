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
                {% set where = where | replace(
                    "__most_recent_year__", "ano = '" ~ max_year ~ "'"
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
