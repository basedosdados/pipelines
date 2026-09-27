{{
    config(
        schema="au_abs_prices_inflation",
        alias="dwelling_value",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2002, "end": 2031, "interval": 1},
        },
        cluster_by=["region", "measure"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(series_id as string) series_id,
    safe_cast(measure as string) measure,
    safe_cast(owner_sector as string) owner_sector,
    safe_cast(region as string) region,
    safe_cast(unit as string) unit,
    safe_cast(value as float64) value
from {{ set_datalake_project("au_abs_prices_inflation_staging.dwelling_value") }} as t
