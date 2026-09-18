{{
    config(
        schema="us_usda_nass",
        alias="census_of_agriculture_state",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1840, "end": 2035, "interval": 1},
        },
    )
}}

-- One row per natural key: the QuickStats bulk has a few rows that collapse to
-- the same key once source-only columns (load_time, week_ending, begin/end
-- codes) are dropped -- a handful fully identical, the rest differing only in a
-- geography name functionally determined by a code already in the key.
with
    src as (
        select
            safe_cast(year as int64) year,
            safe_cast(state_fips as string) state_fips,
            safe_cast(state_abbreviation as string) state_abbreviation,
            safe_cast(state_name as string) state_name,
            safe_cast(sector as string) sector,
            safe_cast(commodity_group as string) commodity_group,
            safe_cast(commodity as string) commodity,
            safe_cast(commodity_class as string) commodity_class,
            safe_cast(production_practice as string) production_practice,
            safe_cast(utilization_practice as string) utilization_practice,
            safe_cast(statistic_category as string) statistic_category,
            safe_cast(unit as string) unit,
            safe_cast(short_description as string) short_description,
            safe_cast(domain as string) domain,
            safe_cast(domain_category as string) domain_category,
            safe_cast(frequency as string) frequency,
            safe_cast(reference_period as string) reference_period,
            safe_cast(value as float64) value,
            safe_cast(value_suppression_flag as string) value_suppression_flag,
            safe_cast(coefficient_of_variation as float64) coefficient_of_variation
        from
            {{
                set_datalake_project(
                    "us_usda_nass_staging.census_of_agriculture_state"
                )
            }} as t
    )

select *
from src
qualify
    row_number() over (
        partition by
            year,
            state_fips,
            commodity,
            commodity_class,
            production_practice,
            utilization_practice,
            statistic_category,
            unit,
            domain,
            domain_category,
            reference_period
        order by value desc, short_description, state_name
    )
    = 1
