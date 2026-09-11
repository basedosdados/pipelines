{{
    config(
        schema="au_treasury_budget",
        alias="payment_growth",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2020, "end": 2035, "interval": 1},
        },
        cluster_by=["payment_program"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(source_release_id as string) source_release_id,
    safe_cast(source_release_label as string) source_release_label,
    safe_cast(series_release_id as string) series_release_id,
    safe_cast(series_release_label as string) series_release_label,
    safe_cast(payment_program as string) payment_program,
    safe_cast(projection_period_start_year as int64) projection_period_start_year,
    safe_cast(projection_period_end_year as int64) projection_period_end_year,
    safe_cast(growth_basis as string) growth_basis,
    safe_cast(average_annual_growth_percent as float64) average_annual_growth_percent
from {{ set_datalake_project("au_treasury_budget_staging.payment_growth") }} as t
