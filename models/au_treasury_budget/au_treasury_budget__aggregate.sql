{{
    config(
        schema="au_treasury_budget",
        alias="aggregate",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1970, "end": 2035, "interval": 1},
        },
        cluster_by=["release_id", "measure"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(financial_year as string) financial_year,
    safe_cast(release_id as string) release_id,
    safe_cast(release_label as string) release_label,
    safe_cast(release_type as string) release_type,
    safe_cast(release_financial_year as string) release_financial_year,
    safe_cast(sector as string) sector,
    safe_cast(measure as string) measure,
    safe_cast(estimate_type as string) estimate_type,
    safe_cast(source_tables as string) source_tables,
    safe_cast(value_aud_million as float64) value_aud_million,
    safe_cast(value_percent_gdp as float64) value_percent_gdp,
    safe_cast(value_percent_real_growth as float64) value_percent_real_growth,
    safe_cast(value_aud_per_person_real as float64) value_aud_per_person_real
from {{ set_datalake_project("au_treasury_budget_staging.aggregate") }} as t
