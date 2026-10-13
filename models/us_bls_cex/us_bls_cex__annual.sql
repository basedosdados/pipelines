{{
    config(
        schema="us_bls_cex",
        alias="annual",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1984, "end": 2031, "interval": 1},
        },
        cluster_by=["series_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(series_id as string) series_id,
    safe_cast(mean as float64) mean,
    safe_cast(standard_error as float64) standard_error,
    safe_cast(relative_standard_error as float64) relative_standard_error,
    safe_cast(expenditure_share as float64) expenditure_share,
    safe_cast(aggregate_expenditure as float64) aggregate_expenditure,
    safe_cast(aggregate_share as float64) aggregate_share,
    safe_cast(percent_reporting as float64) percent_reporting,
    safe_cast(footnote_codes as string) footnote_codes
from {{ set_datalake_project("us_bls_cex_staging.annual") }} as t
