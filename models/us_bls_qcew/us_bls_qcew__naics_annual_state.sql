{{
    config(
        schema="us_bls_qcew",
        alias="naics_annual_state",
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
    safe_cast(id_state as string) id_state,
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
    safe_cast(avg_annual_pay as float64) avg_annual_pay,
    safe_cast(lq_disclosure_code as string) lq_disclosure_code,
    safe_cast(lq_annual_avg_estabs as float64) lq_annual_avg_estabs,
    safe_cast(lq_annual_avg_emplvl as float64) lq_annual_avg_emplvl,
    safe_cast(lq_total_annual_wages as float64) lq_total_annual_wages,
    safe_cast(lq_taxable_annual_wages as float64) lq_taxable_annual_wages,
    safe_cast(lq_annual_contributions as float64) lq_annual_contributions,
    safe_cast(lq_annual_avg_wkly_wage as float64) lq_annual_avg_wkly_wage,
    safe_cast(lq_avg_annual_pay as float64) lq_avg_annual_pay,
    safe_cast(oty_disclosure_code as string) oty_disclosure_code,
    safe_cast(oty_annual_avg_estabs_chg as int64) oty_annual_avg_estabs_chg,
    safe_cast(oty_annual_avg_estabs_pct_chg as float64) oty_annual_avg_estabs_pct_chg,
    safe_cast(oty_annual_avg_emplvl_chg as int64) oty_annual_avg_emplvl_chg,
    safe_cast(oty_annual_avg_emplvl_pct_chg as float64) oty_annual_avg_emplvl_pct_chg,
    safe_cast(oty_total_annual_wages_chg as float64) oty_total_annual_wages_chg,
    safe_cast(oty_total_annual_wages_pct_chg as float64) oty_total_annual_wages_pct_chg,
    safe_cast(oty_taxable_annual_wages_chg as float64) oty_taxable_annual_wages_chg,
    safe_cast(
        oty_taxable_annual_wages_pct_chg as float64
    ) oty_taxable_annual_wages_pct_chg,
    safe_cast(oty_annual_contributions_chg as float64) oty_annual_contributions_chg,
    safe_cast(
        oty_annual_contributions_pct_chg as float64
    ) oty_annual_contributions_pct_chg,
    safe_cast(oty_annual_avg_wkly_wage_chg as float64) oty_annual_avg_wkly_wage_chg,
    safe_cast(
        oty_annual_avg_wkly_wage_pct_chg as float64
    ) oty_annual_avg_wkly_wage_pct_chg,
    safe_cast(oty_avg_annual_pay_chg as float64) oty_avg_annual_pay_chg,
    safe_cast(oty_avg_annual_pay_pct_chg as float64) oty_avg_annual_pay_pct_chg
from {{ set_datalake_project("us_bls_qcew_staging.naics_annual_state") }} as t
