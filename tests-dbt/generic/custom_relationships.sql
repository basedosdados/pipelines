{% test custom_relationships(
    model,
    column_name,
    to,
    field,
    ignore_values=None,
    proportion_allowed_failures=0.05
) %}

    {{ config(severity="error") }}

    with
        child as (
            select {{ column_name }} as child_value
            from {{ model }}
            {% if ignore_values %}
                where
                    {{ column_name }} not in ('{{ ignore_values | join("', '") }}')
                    and {{ column_name }} is not null
            {% endif %}
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
                round(count(*) / (select count(*) from child), 2) as failure_rate
            from validation
        )

    select
        total_missing,
        total_child_records,
        failure_rate,
        case
            when failure_rate > {{ proportion_allowed_failures }}
            then
                'Test failed: Failure rate of '
                || failure_rate
                || '% exceeds allowed proportion of '
                || '{{ proportion_allowed_failures }}%'
            else
                'Test passed: Failure rate of '
                || failure_rate
                || '% within acceptable limits'
        end as result_message
    from summary
    where failure_rate > {{ proportion_allowed_failures }}

{% endtest %}
