{{
    config(
        schema="us_census_cog",
        alias="finance",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1967, "end": 2035, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(government_id as string) government_id,
    safe_cast(government_id_govs as string) government_id_govs,
    safe_cast(item_code as string) item_code,
    safe_cast(amount as int64) amount,
    safe_cast(data_flag as string) data_flag
from {{ set_datalake_project("us_census_cog_staging.finance") }} as t
