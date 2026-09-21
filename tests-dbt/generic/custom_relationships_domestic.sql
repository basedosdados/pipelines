{% test custom_relationships_domestic(
    model,
    column_name,
    to,
    field,
    exterior_column="sigla_uf",
    exterior_value="ZZ",
    proportion_allowed_failures=0.00
) %}

    {# Relationship test for a Brazilian-municipality foreign key that tolerates
       overseas voting/finance rows. TSE codes localities outside Brazil (where
       expatriates vote, or foreign donors/suppliers) with sigla_uf = 'ZZ', and
       such rows carry an id_municipio_tse that is intentionally absent from the
       domestic municipality directory. Rows with no state at all (NULL) are
       likewise exempt — there is no domestic municipality to validate. The FK is
       enforced only for rows that claim a Brazilian state. #}
    {{ config(severity="error") }}

    with
        child as (
            select {{ column_name }} as child_value
            from {{ model }}
            where
                {{ column_name }} is not null
                and {{ exterior_column }} is not null
                and {{ exterior_column }} != '{{ exterior_value }}'
        ),
        parent as (select {{ field }} as parent_value from {{ to }}),
        validation as (
            select child.child_value
            from child
            left join parent on child.child_value = parent.parent_value
            where parent.parent_value is null
        ),
        summary as (
            select
                count(*) as total_missing,
                (select count(*) from child) as total_child_records,
                round(
                    safe_divide(count(*), (select count(*) from child)), 2
                ) as failure_rate
            from validation
        )

    select total_missing, total_child_records, failure_rate
    from summary
    where failure_rate > {{ proportion_allowed_failures }}

{% endtest %}
