{{
    config(
        schema="us_census_trade",
        alias="import_state",
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
    safe_cast(country_iso2_code as string) country_iso2_code,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(state_id as string) state_id,
    safe_cast(hs6_code as string) hs6_code,
    safe_cast(hs4_code as string) hs4_code,
    safe_cast(hs2_code as string) hs2_code,
    safe_cast(hs_revision as string) hs_revision,
    safe_cast(general_value as float64) general_value,
    safe_cast(consumption_value as float64) consumption_value,
    safe_cast(air_value as float64) air_value,
    safe_cast(air_shipping_weight as float64) air_shipping_weight,
    safe_cast(vessel_value as float64) vessel_value,
    safe_cast(vessel_shipping_weight as float64) vessel_shipping_weight,
    safe_cast(containerized_vessel_value as float64) containerized_vessel_value,
    safe_cast(
        containerized_vessel_shipping_weight as float64
    ) containerized_vessel_shipping_weight
from {{ set_datalake_project("us_census_trade_staging.import_state") }} as t
