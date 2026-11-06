{{
    config(
        schema="au_apra_adi",
        alias="asset_quality",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2004, "end": 2035, "interval": 1},
        },
        cluster_by=["institution_type"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(institution_type as string) institution_type,
    safe_cast(measure as string) measure,
    safe_cast(unit as string) unit,
    safe_cast(value as float64) value
from {{ set_datalake_project("au_apra_adi_staging.asset_quality") }} as t
