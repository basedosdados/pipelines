{{
    config(
        schema="au_doe_higher_education_finances",
        alias="line_item",
        materialized="table",
    )
}}


select
    safe_cast(statement as string) statement,
    safe_cast(line_item as string) line_item,
    safe_cast(first_year as int64) first_year,
    safe_cast(last_year as int64) last_year,
    safe_cast(n_years as int64) n_years
from
    {{ set_datalake_project("au_doe_higher_education_finances_staging.line_item") }}
    as t
