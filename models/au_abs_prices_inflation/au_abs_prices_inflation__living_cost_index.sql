{{
    config(
        schema="au_abs_prices_inflation",
        alias="living_cost_index",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1998, "end": 2031, "interval": 1},
        },
        cluster_by=["household_type", "commodity_group"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(series_id as string) series_id,
    safe_cast(statistic as string) statistic,
    safe_cast(household_type as string) household_type,
    safe_cast(commodity_group as string) commodity_group,
    safe_cast(unit as string) unit,
    safe_cast(value as float64) value
from
    {{ set_datalake_project("au_abs_prices_inflation_staging.living_cost_index") }} as t
