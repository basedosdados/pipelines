{% macro br_me_cnpj_redirect_notice() %}
    {% if target.name == "prod" %}
        {% set old_table = api.Relation.create(
            database=this.database,
            schema="br_me_cnpj",
            identifier=this.identifier,
        ) %}
        {% set notice = (
            "O endereço desta tabela mudou. Para capturar os dados mais recentes, acesse basedosdados.br_rf_cnpj."
            ~ this.identifier
        ) %}
        insert into {{ old_table }}
        select
        {% for column in adapter.get_columns_in_relation(old_table) %}
            {% if column.name == "ano" %} extract(year from competencia)
            {% elif column.name == "mes" %} extract(month from competencia)
            {% elif column.name == "data" %} competencia
            {% elif column.dtype == "STRING" %} '{{ notice }}'
            {% else %} cast(null as {{ column.dtype }})
            {% endif %} as {{ column.name }}
            {% if not loop.last %},{% endif %}
        {% endfor %}
        from
            (
                select safe.parse_date('%Y%m', partition_id) competencia
                from `{{ this.database }}.br_rf_cnpj.INFORMATION_SCHEMA.PARTITIONS`
                where table_name = '{{ this.identifier }}'
            )
        where
            competencia > (
                select max(safe.parse_date('%Y%m%d', partition_id))
                from `{{ this.database }}.br_me_cnpj.INFORMATION_SCHEMA.PARTITIONS`
                where table_name = '{{ this.identifier }}'
            )
    {% endif %}
{% endmacro %}

{% macro br_me_cnpj_redirect_policy() %}
    {% if target.name == "prod" %}
        create or replace row access policy redirect_notice_filter
        on `{{ this.database }}.br_me_cnpj.{{ this.identifier }}`
        grant to ("allUsers")
        filter using (data > "2026-07-12")
    {% endif %}
{% endmacro %}
