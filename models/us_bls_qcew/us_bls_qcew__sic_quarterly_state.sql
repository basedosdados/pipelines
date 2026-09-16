{{
    config(
        schema="us_bls_qcew",
        alias="sic_quarterly_state",
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
    safe_cast(qtr as int64) qtr,
    safe_cast(area_fips as string) area_fips,
    safe_cast(id_state as string) id_state,
    safe_cast(own_code as string) own_code,
    safe_cast(industry_code as string) industry_code,
    safe_cast(agglvl_code as string) agglvl_code,
    safe_cast(size_code as string) size_code,
    safe_cast(disclosure_code as string) disclosure_code,
    safe_cast(qtrly_estabs as int64) qtrly_estabs,
    safe_cast(month1_emplvl as int64) month1_emplvl,
    safe_cast(month2_emplvl as int64) month2_emplvl,
    safe_cast(month3_emplvl as int64) month3_emplvl,
    safe_cast(total_qtrly_wages as float64) total_qtrly_wages,
    safe_cast(taxable_qtrly_wages as float64) taxable_qtrly_wages,
    safe_cast(qtrly_contributions as float64) qtrly_contributions,
    safe_cast(avg_wkly_wage as float64) avg_wkly_wage
from {{ set_datalake_project("us_bls_qcew_staging.sic_quarterly_state") }} as t
