{% macro add_ano_mes_operacao_data(
    join_columns=["id_referencia_bacen", "numero_ordem"]
) %}
    left join
        (
            select distinct
                id_referencia_bacen,
                {% if "numero_ordem" in join_columns %} numero_ordem, {% endif %}
                ano_emissao,
                mes_emissao
            from {{ ref("br_bcb_sicor__microdados_operacao") }}
        ) as mo using ({{ join_columns | join(", ") }})
{% endmacro %}
