{{
    config(
        schema="us_eia_consumption",
        alias="utility",
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
    safe_cast(ownership_type as string) ownership_type,
    safe_cast(nerc_region as string) nerc_region
from {{ set_datalake_project("us_eia_consumption_staging.utility") }} as t
