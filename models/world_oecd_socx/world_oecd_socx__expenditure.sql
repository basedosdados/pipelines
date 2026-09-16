{{
    config(
        alias="expenditure",
        schema="world_oecd_socx",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1980, "end": 2027, "interval": 1},
        },
        cluster_by=["country_iso3_code"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(country_iso3_code as string) country_iso3_code,
    safe_cast(reference_area as string) reference_area,
    safe_cast(frequency as string) frequency,
    safe_cast(measure as string) measure,
    safe_cast(unit_measure as string) unit_measure,
    safe_cast(expenditure_source as string) expenditure_source,
    safe_cast(spending_type as string) spending_type,
    safe_cast(programme_type as string) programme_type,
    safe_cast(price_base as string) price_base,
    safe_cast(obs_value as float64) obs_value,
    safe_cast(base_period as string) base_period,
    safe_cast(currency as string) currency,
    safe_cast(decimals as string) decimals,
    safe_cast(obs_status as string) obs_status,
    safe_cast(unit_multiplier as string) unit_multiplier,
    safe_cast(source_flow as string) source_flow,
    safe_cast(source_flow_version as string) source_flow_version
from {{ set_datalake_project("world_oecd_socx_staging.expenditure") }} as t
