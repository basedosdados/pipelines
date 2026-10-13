{{
    config(
        schema="us_eia_seds",
        alias="seds_consumption",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1960, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_id as string) state_id,
    safe_cast(state_code as string) state_code,
    safe_cast(msn as string) msn,
    safe_cast(measure_type as string) measure_type,
    safe_cast(value as float64) value,
    safe_cast(measurement_unit as string) measurement_unit,
    safe_cast(data_status as string) data_status
from {{ set_datalake_project("us_eia_seds_staging.seds_consumption") }} as t
