{{
    config(
        schema="us_fdic_bankfind",
        alias="indicator",
        materialized="table",
    )
}}


select
    safe_cast(indicator_id as string) indicator_id,
    safe_cast(name as string) name,
    safe_cast(description as string) description,
    safe_cast(measurement_unit as string) measurement_unit,
    safe_cast(is_ratio as string) is_ratio,
    safe_cast(is_quarterly as string) is_quarterly,
    safe_cast(is_flag as string) is_flag,
    safe_cast(financials_column as string) financials_column
from {{ set_datalake_project("us_fdic_bankfind_staging.indicator") }} as t
