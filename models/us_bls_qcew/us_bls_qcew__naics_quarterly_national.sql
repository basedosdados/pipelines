{{
    config(
        schema="us_bls_qcew",
        alias="naics_quarterly_national",
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
    safe_cast(avg_wkly_wage as float64) avg_wkly_wage,
    safe_cast(lq_disclosure_code as string) lq_disclosure_code,
    safe_cast(lq_qtrly_estabs as float64) lq_qtrly_estabs,
    safe_cast(lq_month1_emplvl as float64) lq_month1_emplvl,
    safe_cast(lq_month2_emplvl as float64) lq_month2_emplvl,
    safe_cast(lq_month3_emplvl as float64) lq_month3_emplvl,
    safe_cast(lq_total_qtrly_wages as float64) lq_total_qtrly_wages,
    safe_cast(lq_taxable_qtrly_wages as float64) lq_taxable_qtrly_wages,
    safe_cast(lq_qtrly_contributions as float64) lq_qtrly_contributions,
    safe_cast(lq_avg_wkly_wage as float64) lq_avg_wkly_wage,
    safe_cast(oty_disclosure_code as string) oty_disclosure_code,
    safe_cast(oty_qtrly_estabs_chg as int64) oty_qtrly_estabs_chg,
    safe_cast(oty_qtrly_estabs_pct_chg as float64) oty_qtrly_estabs_pct_chg,
    safe_cast(oty_month1_emplvl_chg as int64) oty_month1_emplvl_chg,
    safe_cast(oty_month1_emplvl_pct_chg as float64) oty_month1_emplvl_pct_chg,
    safe_cast(oty_month2_emplvl_chg as int64) oty_month2_emplvl_chg,
    safe_cast(oty_month2_emplvl_pct_chg as float64) oty_month2_emplvl_pct_chg,
    safe_cast(oty_month3_emplvl_chg as int64) oty_month3_emplvl_chg,
    safe_cast(oty_month3_emplvl_pct_chg as float64) oty_month3_emplvl_pct_chg,
    safe_cast(oty_total_qtrly_wages_chg as float64) oty_total_qtrly_wages_chg,
    safe_cast(oty_total_qtrly_wages_pct_chg as float64) oty_total_qtrly_wages_pct_chg,
    safe_cast(oty_taxable_qtrly_wages_chg as float64) oty_taxable_qtrly_wages_chg,
    safe_cast(
        oty_taxable_qtrly_wages_pct_chg as float64
    ) oty_taxable_qtrly_wages_pct_chg,
    safe_cast(oty_qtrly_contributions_chg as float64) oty_qtrly_contributions_chg,
    safe_cast(
        oty_qtrly_contributions_pct_chg as float64
    ) oty_qtrly_contributions_pct_chg,
    safe_cast(oty_avg_wkly_wage_chg as float64) oty_avg_wkly_wage_chg,
    safe_cast(oty_avg_wkly_wage_pct_chg as float64) oty_avg_wkly_wage_pct_chg
from {{ set_datalake_project("us_bls_qcew_staging.naics_quarterly_national") }} as t
