{{
    config(
        schema="us_bls_qcew",
        alias="sic_annual_national",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1975, "end": 2030, "interval": 1},
        },
        cluster_by=["industry_code", "own_code"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(area_fips as string) area_fips,
    safe_cast(own_code as string) own_code,
    safe_cast(industry_code as string) industry_code,
    safe_cast(agglvl_code as string) agglvl_code,
    safe_cast(size_code as string) size_code,
    safe_cast(disclosure_code as string) disclosure_code,
    safe_cast(annual_avg_estabs as int64) annual_avg_estabs,
    safe_cast(annual_avg_emplvl as int64) annual_avg_emplvl,
    safe_cast(total_annual_wages as float64) total_annual_wages,
    safe_cast(taxable_annual_wages as float64) taxable_annual_wages,
    safe_cast(annual_contributions as float64) annual_contributions,
    safe_cast(annual_avg_wkly_wage as float64) annual_avg_wkly_wage,
    safe_cast(avg_annual_pay as float64) avg_annual_pay
from {{ set_datalake_project("us_bls_qcew_staging.sic_annual_national") }} as t
