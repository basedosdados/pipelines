{% macro set_datalake_project(table_path) %}
    {% if target.name == "dev" %} {% set prefix = "basedosdados-dev" %}
    {% elif target.name == "prod" %} {% set prefix = "basedosdados-staging" %}
    {% elif target.name == "data-api" %} {% set prefix = "basedosdados-data-api" %}
    {% else %}
        {% do exceptions.raise_compiler_error(
            "Invalid target: " ~ target.name ~ ". Use 'dev', 'prod' or 'data-api'."
        ) %}
    {% endif %}

    `{{ prefix }}.{{ table_path }}`
{% endmacro %}
