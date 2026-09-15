{{
    config(
        schema="us_census_trade",
        alias="export",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {
                "start": 2010,
                "end": 2035,
                "interval": 1,
            },
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(month as int64) month,
    safe_cast(country_code as string) country_code,
    safe_cast(country_iso3_code as string) country_iso3_code,
    safe_cast(country_iso2_code as string) country_iso2_code,
    safe_cast(district_code as string) district_code,
    safe_cast(hs6_code as string) hs6_code,
    safe_cast(hs4_code as string) hs4_code,
    safe_cast(hs2_code as string) hs2_code,
    safe_cast(hs_revision as string) hs_revision,
    safe_cast(domestic_foreign_code as string) domestic_foreign_code,
    safe_cast(total_value as float64) total_value,
    safe_cast(quantity_1 as float64) quantity_1,
    safe_cast(quantity_2 as float64) quantity_2,
    safe_cast(quantity_1_unit as string) quantity_1_unit,
    safe_cast(quantity_2_unit as string) quantity_2_unit,
    safe_cast(air_value as float64) air_value,
    safe_cast(air_shipping_weight as float64) air_shipping_weight,
    safe_cast(vessel_value as float64) vessel_value,
    safe_cast(vessel_shipping_weight as float64) vessel_shipping_weight,
    safe_cast(containerized_vessel_value as float64) containerized_vessel_value,
    safe_cast(
        containerized_vessel_shipping_weight as float64
    ) containerized_vessel_shipping_weight,
    safe_cast(card_count as int64) card_count
from {{ set_datalake_project("us_census_trade_staging.export") }} as t
