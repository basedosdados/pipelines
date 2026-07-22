{{
    config(
        schema="us_census_cbp",
        alias="county",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1998, "end": 2028, "interval": 1},
        },
        cluster_by=["naics"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(id_state as string) id_state,
    safe_cast(id_county as string) id_county,
    safe_cast(naics as string) naics,
    safe_cast(naics_version as string) naics_version,
    safe_cast(establishments as int64) establishments,
    safe_cast(employment as int64) employment,
    safe_cast(employment_flag as string) employment_flag,
    safe_cast(employment_noise_flag as string) employment_noise_flag,
    safe_cast(payroll_first_quarter as int64) payroll_first_quarter,
    safe_cast(
        payroll_first_quarter_noise_flag as string
    ) payroll_first_quarter_noise_flag,
    safe_cast(payroll_annual as int64) payroll_annual,
    safe_cast(payroll_annual_noise_flag as string) payroll_annual_noise_flag,
    safe_cast(establishments_1_4 as int64) establishments_1_4,
    safe_cast(establishments_5_9 as int64) establishments_5_9,
    safe_cast(establishments_10_19 as int64) establishments_10_19,
    safe_cast(establishments_20_49 as int64) establishments_20_49,
    safe_cast(establishments_50_99 as int64) establishments_50_99,
    safe_cast(establishments_100_249 as int64) establishments_100_249,
    safe_cast(establishments_250_499 as int64) establishments_250_499,
    safe_cast(establishments_500_999 as int64) establishments_500_999,
    safe_cast(establishments_1000 as int64) establishments_1000,
    safe_cast(establishments_1000_1499 as int64) establishments_1000_1499,
    safe_cast(establishments_1500_2499 as int64) establishments_1500_2499,
    safe_cast(establishments_2500_4999 as int64) establishments_2500_4999,
    safe_cast(establishments_5000_more as int64) establishments_5000_more
from {{ set_datalake_project("us_census_cbp_staging.county") }} as t
