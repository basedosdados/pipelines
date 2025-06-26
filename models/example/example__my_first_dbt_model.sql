/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/
{{
    config(
        schema="example",
        materialized="table",
        alias="my_first_dbt_model",
    )
}}

with
    source_data as (

        select 1 as id
        union all
        select null as id

    )

select *
from source_data
