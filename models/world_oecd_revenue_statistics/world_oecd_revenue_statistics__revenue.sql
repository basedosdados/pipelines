{{
    config(
        schema="world_oecd_revenue_statistics",
        alias="revenue",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1965, "end": 2035, "interval": 1},
        },
        cluster_by=["country_iso3_code", "tax_category_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(country_iso3_code as string) country_iso3_code,
    safe_cast(reference_area as string) reference_area,
    safe_cast(government_level_id as string) government_level_id,
    safe_cast(tax_category_id as string) tax_category_id,
    safe_cast(tax_category_parent_id as string) tax_category_parent_id,
    safe_cast(tax_category_code as string) tax_category_code,
    safe_cast(pct_gdp as float64) pct_gdp,
    safe_cast(pct_institutional_sector as float64) pct_institutional_sector,
    safe_cast(pct_revenue_category as float64) pct_revenue_category,
    safe_cast(value_national_currency as float64) value_national_currency,
    safe_cast(value_usd as float64) value_usd,
    safe_cast(currency as string) currency,
    safe_cast(observation_status as string) observation_status,
    safe_cast(source_flow_version as string) source_flow_version
from {{ set_datalake_project("world_oecd_revenue_statistics_staging.revenue") }} as t
