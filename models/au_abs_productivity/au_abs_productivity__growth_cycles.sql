{{
    config(
        schema="au_abs_productivity",
        alias="growth_cycles",
        materialized="table",
    )
}}


select
    safe_cast(indicator_id as string) indicator_id,
    safe_cast(period as string) period,
    safe_cast(period_start_financial_year as string) period_start_financial_year,
    safe_cast(period_end_financial_year as string) period_end_financial_year,
    safe_cast(value as float64) value
from {{ set_datalake_project("au_abs_productivity_staging.growth_cycles") }} as t
