{{
    config(
        schema="world_bis_property_prices",
        alias="price_index",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1927, "end": 2031, "interval": 1},
        },
        cluster_by=["country_id", "value_type"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(country_id as string) country_id,
    safe_cast(reference_area_code as string) reference_area_code,
    safe_cast(reference_area_name as string) reference_area_name,
    safe_cast(value_type as string) value_type,
    safe_cast(measure as string) measure,
    safe_cast(unit as string) unit,
    safe_cast(bis_series_key as string) bis_series_key,
    safe_cast(value as float64) value
from {{ set_datalake_project("world_bis_property_prices_staging.price_index") }} as t
