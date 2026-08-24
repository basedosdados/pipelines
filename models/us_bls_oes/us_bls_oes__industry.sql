{{
    config(
        schema="us_bls_oes",
        alias="industry",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2003, "end": 2030, "interval": 1},
        },
        cluster_by=["industry_id", "occupation_id"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(industry_id as string) industry_id,
    safe_cast(industry_group as string) industry_group,
    safe_cast(ownership_id as string) ownership_id,
    safe_cast(occupation_id as string) occupation_id,
    safe_cast(occupation_group as string) occupation_group,
    safe_cast(industry_name as string) industry_name,
    safe_cast(occupation_name as string) occupation_name,
    safe_cast(employment as int64) employment,
    safe_cast(employment_prse as float64) employment_prse,
    safe_cast(percent_total_employment as float64) percent_total_employment,
    safe_cast(
        percent_establishments_reporting as float64
    ) percent_establishments_reporting,
    safe_cast(
        establishments_reporting_below_threshold as string
    ) establishments_reporting_below_threshold,
    safe_cast(hourly_wage_mean as float64) hourly_wage_mean,
    safe_cast(annual_wage_mean as float64) annual_wage_mean,
    safe_cast(wage_mean_prse as float64) wage_mean_prse,
    safe_cast(hourly_wage_percentile_10 as float64) hourly_wage_percentile_10,
    safe_cast(hourly_wage_percentile_25 as float64) hourly_wage_percentile_25,
    safe_cast(hourly_wage_median as float64) hourly_wage_median,
    safe_cast(hourly_wage_percentile_75 as float64) hourly_wage_percentile_75,
    safe_cast(hourly_wage_percentile_90 as float64) hourly_wage_percentile_90,
    safe_cast(annual_wage_percentile_10 as float64) annual_wage_percentile_10,
    safe_cast(annual_wage_percentile_25 as float64) annual_wage_percentile_25,
    safe_cast(annual_wage_median as float64) annual_wage_median,
    safe_cast(annual_wage_percentile_75 as float64) annual_wage_percentile_75,
    safe_cast(annual_wage_percentile_90 as float64) annual_wage_percentile_90,
    safe_cast(annual_wage_only as string) annual_wage_only,
    safe_cast(hourly_wage_only as string) hourly_wage_only,
    safe_cast(wage_top_coded as string) wage_top_coded
from {{ set_datalake_project("us_bls_oes_staging.industry") }} as t
