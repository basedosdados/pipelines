{{
    config(
        schema="us_stanford_dime",
        alias="contributor_cycle",
        materialized="table",
        partition_by={
            "field": "cycle",
            "data_type": "int64",
            "range": {"start": 1980, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(cycle as int64) cycle,
    safe_cast(contributor_id as string) contributor_id,
    safe_cast(amount as float64) amount
from {{ set_datalake_project("us_stanford_dime_staging.contributor_cycle") }} as t
