{% test unique_key_in_latest_partition(
    model,
    key_column,
    partition_column,
    row_filter="true",
    max_proportion=0.001
) %}

    {{ config(severity="error") }}

    with
        base as (
            select {{ key_column }} as chave
            from {{ model }}
            where
                {{ partition_column }}
                = (select max({{ partition_column }}) from {{ model }})
                and {{ row_filter }}
        ),
        repetidas as (
            select count(*) - 1 as excedente
            from base
            group by chave
            having count(*) > 1
        ),
        totais as (
            select
                (
                    select coalesce(sum(excedente), 0) from repetidas
                ) as linhas_excedentes,
                (select count(*) from base) as linhas
        )

    select linhas_excedentes, linhas, safe_divide(linhas_excedentes, linhas) as taxa
    from totais
    where safe_divide(linhas_excedentes, linhas) > {{ max_proportion }}

{% endtest %}
