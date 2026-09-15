{{
    config(
        schema="us_eia_consumption",
        alias="service_territory",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1990, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(utility_id as string) utility_id,
    safe_cast(utility_name as string) utility_name,
    safe_cast(state_id as string) state_id,
    safe_cast(county_id as string) county_id,
    safe_cast(county_name as string) county_name,
    safe_cast(short_form as string) short_form
from {{ set_datalake_project("us_eia_consumption_staging.service_territory") }} as t
