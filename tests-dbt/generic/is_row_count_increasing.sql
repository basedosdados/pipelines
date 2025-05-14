{% test is_row_count_increasing(model, column_name) %}

    with

        date_nrows as (
            select {{ column_name }} as date_column, count(1) as nrows
            from {{ model }}
            group by {{ column_name }}
        ),

        windowed as (
            select
                date_column,
                nrows,
                lag(nrows) over (order by date_column) as nrows_previous_date
            from date_nrows
        ),

        validation_errors as (select * from windowed where nrows_previous_date > nrows)

    select *
    from validation_errors

{% endtest %}
