{{
    config(
        alias="surname",
        schema="us_census_surnames",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2000, "end": 2030, "interval": 1},
        },
    )
}}
select
    safe_cast(year as int64) year,
    safe_cast(surname as string) surname,
    safe_cast(rank as string) rank,
    safe_cast(count as int64) count,
    safe_cast(proportion_per_100k as float64) proportion_per_100k,
    safe_cast(cumulative_proportion_per_100k as float64) cumulative_proportion_per_100k,
    safe_cast(percent_white as float64) percent_white,
    safe_cast(percent_black as float64) percent_black,
    safe_cast(percent_asian_pacific_islander as float64) percent_asian_pacific_islander,
    safe_cast(
        percent_american_indian_alaska_native as float64
    ) percent_american_indian_alaska_native,
    safe_cast(percent_two_or_more_races as float64) percent_two_or_more_races,
    safe_cast(percent_hispanic as float64) percent_hispanic
from {{ set_datalake_project("us_census_surnames_staging.surname") }} as t
