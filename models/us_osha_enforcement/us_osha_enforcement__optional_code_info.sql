{{
    config(
        schema="us_osha_enforcement",
        alias="optional_code_info",
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
    safe_cast(information_type as string) information_type,
    safe_cast(information_id as string) information_id,
    safe_cast(information_value as string) information_value
from {{ set_datalake_project("us_osha_enforcement_staging.optional_code_info") }} as t
