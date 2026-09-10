{{
    config(
        schema="au_abs_household_income_wealth",
        alias="dictionary",
        materialized="table",
    )
}}


select
    nullif(nullif(trim(safe_cast(table_id as string)), ''), '.') table_id,
    nullif(nullif(trim(safe_cast(column_name as string)), ''), '.') column_name,
    nullif(nullif(trim(safe_cast(key as string)), ''), '.') key,
    nullif(
        nullif(trim(safe_cast(temporal_coverage as string)), ''), '.'
    ) temporal_coverage,
    nullif(nullif(trim(safe_cast(value as string)), ''), '.') value
from
    {{ set_datalake_project("au_abs_household_income_wealth_staging.dictionary") }} as t
