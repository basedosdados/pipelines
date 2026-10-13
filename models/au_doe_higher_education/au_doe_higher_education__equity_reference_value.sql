{{
    config(
        schema="au_doe_higher_education",
        alias="equity_reference_value",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2011, "end": 2029, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(equity_group as string) equity_group,
    safe_cast(census_basis as string) census_basis,
    safe_cast(reference_value as float64) reference_value
from
    {{ set_datalake_project("au_doe_higher_education_staging.equity_reference_value") }}
    as t
