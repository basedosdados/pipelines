{% test education_flow_rates_sum_to_100(
    model, tolerance=0.2, identifier_column=none
) %}

    with
        rates as (
            select
                {% if identifier_column %}
                    {{ identifier_column }}
                {% else %} cast(null as string)
                {% endif %} as entity_id,
                ano,
                localizacao,
                rede,
                'ef_anos_iniciais' as etapa,
                taxa_aprovacao_ef_anos_iniciais as aprovacao,
                taxa_reprovacao_ef_anos_iniciais as reprovacao,
                taxa_abandono_ef_anos_iniciais as abandono
            from {{ model }}

            union all

            select
                {% if identifier_column %}
                    {{ identifier_column }}
                {% else %} cast(null as string)
                {% endif %} as entity_id,
                ano,
                localizacao,
                rede,
                'ef_anos_finais' as etapa,
                taxa_aprovacao_ef_anos_finais as aprovacao,
                taxa_reprovacao_ef_anos_finais as reprovacao,
                taxa_abandono_ef_anos_finais as abandono
            from {{ model }}

            union all

            select
                {% if identifier_column %}
                    {{ identifier_column }}
                {% else %} cast(null as string)
                {% endif %} as entity_id,
                ano,
                localizacao,
                rede,
                'em' as etapa,
                taxa_aprovacao_em as aprovacao,
                taxa_reprovacao_em as reprovacao,
                taxa_abandono_em as abandono
            from {{ model }}
        )

    select *, aprovacao + reprovacao + abandono as total
    from rates
    where
        aprovacao is not null
        and reprovacao is not null
        and abandono is not null
        and abs(aprovacao + reprovacao + abandono - 100) > {{ tolerance }}

{% endtest %}
