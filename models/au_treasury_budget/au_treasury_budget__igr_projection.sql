{{
    config(
        schema="au_treasury_budget",
        alias="igr_projection",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2020, "end": 2070, "interval": 1},
        },
        cluster_by=["scenario", "measure"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(financial_year as string) financial_year,
    safe_cast(igr_edition as string) igr_edition,
    safe_cast(scenario as string) scenario,
    safe_cast(measure_category as string) measure_category,
    safe_cast(measure as string) measure,
    safe_cast(source_table as string) source_table,
    safe_cast(value_percent_gdp as float64) value_percent_gdp,
    safe_cast(value_aud_per_person_real as float64) value_aud_per_person_real,
    safe_cast(value_percent as float64) value_percent,
    safe_cast(value_persons_million as float64) value_persons_million,
    safe_cast(value_years as float64) value_years,
    safe_cast(value_births_per_woman as float64) value_births_per_woman
from {{ set_datalake_project("au_treasury_budget_staging.igr_projection") }} as t
