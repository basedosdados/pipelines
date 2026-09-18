{{ config(schema="world_wb_wdi", alias="indicator_time", materialized="table") }}


select
    safe_cast(year as int64) year,
    safe_cast(indicator_id as string) indicator_id,
    safe_cast(description as string) description,
from {{ set_datalake_project("world_wb_wdi_staging.indicator_time") }} as t
