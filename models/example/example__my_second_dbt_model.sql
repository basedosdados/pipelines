{{
    config(
        schema="example",
        materialized="table",
        alias="my_second_dbt_model",
    )
}}
-- Use the `ref` function to select from other models
select *
from {{ ref("example__my_first_dbt_model") }}
where id = 1
