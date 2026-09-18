{{ config(schema="world_wb_wdi", alias="country_indicator", materialized="table") }}


select
    safe_cast(country_id as string) country_id,
    safe_cast(indicator_id as string) indicator_id,
    safe_cast(description as string) description,
from {{ set_datalake_project("world_wb_wdi_staging.country_indicator") }} as t
