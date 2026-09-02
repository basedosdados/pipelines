{% macro set_datalake_project(table_path) %}
    {# Optional local override, e.g. --vars '{datalake_project: sandbox-507414}'.
       Official dev/prod runs do not set this and keep the historical prefixes. #}
    {% set override = var("datalake_project", none) %}
    {% if override %} {% set prefix = override %}
    {% elif target.name == "dev" %} {% set prefix = "basedosdados-dev" %}
    {% elif target.name == "prod" %} {% set prefix = "basedosdados-staging" %}
    {% else %}
        {% do exceptions.raise_compiler_error(
            "Invalid target: " ~ target.name ~ ". Use 'dev' or 'prod'."
        ) %}
    {% endif %}

    `{{ prefix }}.{{ table_path }}`
{% endmacro %}
