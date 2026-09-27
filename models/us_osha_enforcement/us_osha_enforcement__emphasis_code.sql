{{
    config(
        schema="us_osha_enforcement",
        alias="emphasis_code",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1970, "end": 2035, "interval": 1},
        },
        cluster_by=["inspection_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(inspection_id as string) inspection_id,
    safe_cast(program_type as string) program_type,
    safe_cast(program_value as string) program_value
from {{ set_datalake_project("us_osha_enforcement_staging.emphasis_code") }} as t
