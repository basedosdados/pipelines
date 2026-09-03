{{
    config(
        schema="au_doe_higher_education",
        alias="application_offer",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2010, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(series as string) series,
    safe_cast(state_abbreviation as string) state_abbreviation,
    safe_cast(dimension as string) dimension,
    safe_cast(dimension_value as string) dimension_value,
    safe_cast(application_source as string) application_source,
    safe_cast(applicants as int64) applicants,
    safe_cast(offers as int64) offers,
    safe_cast(offer_rate as float64) offer_rate,
    safe_cast(acceptances as int64) acceptances
from
    {{ set_datalake_project("au_doe_higher_education_staging.application_offer") }}
    as t
