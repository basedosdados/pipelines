{{
    config(
        schema="world_aiddata_gcdf",
        alias="projects",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2000, "end": 2026, "interval": 1},
        },
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(country_iso3_code as string) country_iso3_code,
    safe_cast(recipient_name as string) recipient_name,
    safe_cast(recipient_region as string) recipient_region,
    safe_cast(financier_country as string) financier_country,
    safe_cast(id_record as string) id_record,
    safe_cast(id_parent as string) id_parent,
    safe_cast(recommended_for_aggregates as string) recommended_for_aggregates,
    safe_cast(umbrella as string) umbrella,
    safe_cast(implementation_start_year as int64) implementation_start_year,
    safe_cast(completion_year as int64) completion_year,
    safe_cast(title as string) title,
    safe_cast(description as string) description,
    safe_cast(staff_comments as string) staff_comments,
    safe_cast(status as string) status,
    safe_cast(intent as string) intent,
    safe_cast(flow_type as string) flow_type,
    safe_cast(flow_type_simplified as string) flow_type_simplified,
    safe_cast(
        oecd_oda_concessionality_threshold as float64
    ) oecd_oda_concessionality_threshold,
    safe_cast(flow_class as string) flow_class,
    safe_cast(sector_code as string) sector_code,
    safe_cast(sector_name as string) sector_name,
    safe_cast(infrastructure as string) infrastructure,
    safe_cast(covid as string) covid,
    safe_cast(funding_agencies as string) funding_agencies,
    safe_cast(funding_agencies_type as string) funding_agencies_type,
    safe_cast(cofinanced as string) cofinanced,
    safe_cast(cofinancing_agencies as string) cofinancing_agencies,
    safe_cast(cofinancing_agencies_type as string) cofinancing_agencies_type,
    safe_cast(direct_receiving_agencies as string) direct_receiving_agencies,
    safe_cast(direct_receiving_agencies_type as string) direct_receiving_agencies_type,
    safe_cast(indirect_receiving_agencies as string) indirect_receiving_agencies,
    safe_cast(
        indirect_receiving_agencies_type as string
    ) indirect_receiving_agencies_type,
    safe_cast(on_lending as string) on_lending,
    safe_cast(implementing_agencies as string) implementing_agencies,
    safe_cast(implementing_agencies_type as string) implementing_agencies_type,
    safe_cast(guarantee_provided as string) guarantee_provided,
    safe_cast(guarantor as string) guarantor,
    safe_cast(guarantor_agency_type as string) guarantor_agency_type,
    safe_cast(insurance_provided as string) insurance_provided,
    safe_cast(insurance_provider as string) insurance_provider,
    safe_cast(insurance_provider_agency_type as string) insurance_provider_agency_type,
    safe_cast(collateralized as string) collateralized,
    safe_cast(collateral_provider as string) collateral_provider,
    safe_cast(
        collateral_provider_agency_type as string
    ) collateral_provider_agency_type,
    safe_cast(security_agent as string) security_agent,
    safe_cast(security_agent_type as string) security_agent_type,
    safe_cast(collateral as string) collateral,
    safe_cast(amount_original_currency as float64) amount_original_currency,
    safe_cast(original_currency as string) original_currency,
    safe_cast(amount_estimated as string) amount_estimated,
    safe_cast(amount_constant_usd_2021 as float64) amount_constant_usd_2021,
    safe_cast(amount_nominal_usd as float64) amount_nominal_usd,
    safe_cast(
        adjusted_amount_original_currency as float64
    ) adjusted_amount_original_currency,
    safe_cast(
        adjusted_amount_constant_usd_2021 as float64
    ) adjusted_amount_constant_usd_2021,
    safe_cast(adjusted_amount_nominal_usd as float64) adjusted_amount_nominal_usd,
    safe_cast(financial_distress as string) financial_distress,
    safe_cast(commitment_date as date) commitment_date,
    safe_cast(commitment_date_estimated as string) commitment_date_estimated,
    safe_cast(
        planned_implementation_start_date as date
    ) planned_implementation_start_date,
    safe_cast(
        actual_implementation_start_date as date
    ) actual_implementation_start_date,
    safe_cast(
        actual_implementation_start_date_estimated as string
    ) actual_implementation_start_date_estimated,
    safe_cast(
        deviation_planned_implementation_start_date as int64
    ) deviation_planned_implementation_start_date,
    safe_cast(planned_completion_date as date) planned_completion_date,
    safe_cast(actual_completion_date as date) actual_completion_date,
    safe_cast(
        actual_completion_date_estimated as string
    ) actual_completion_date_estimated,
    safe_cast(
        deviation_planned_completion_date as int64
    ) deviation_planned_completion_date,
    safe_cast(maturity as float64) maturity,
    safe_cast(interest_rate as float64) interest_rate,
    safe_cast(grace_period as float64) grace_period,
    safe_cast(management_fee as float64) management_fee,
    safe_cast(commitment_fee as float64) commitment_fee,
    safe_cast(insurance_fee_percent as float64) insurance_fee_percent,
    safe_cast(insurance_fee_nominal_usd as float64) insurance_fee_nominal_usd,
    safe_cast(default_interest_rate as float64) default_interest_rate,
    safe_cast(first_loan_repayment_date as date) first_loan_repayment_date,
    safe_cast(last_loan_repayment_date as date) last_loan_repayment_date,
    safe_cast(grant_element_oecd_cash_flow as float64) grant_element_oecd_cash_flow,
    safe_cast(grant_element_oecd_grant_equiv as float64) grant_element_oecd_grant_equiv,
    safe_cast(grant_element_imf as float64) grant_element_imf,
    safe_cast(number_of_lenders as string) number_of_lenders,
    safe_cast(export_buyers_credit as string) export_buyers_credit,
    safe_cast(suppliers_credit as string) suppliers_credit,
    safe_cast(interest_free_loan as string) interest_free_loan,
    safe_cast(refinancing as string) refinancing,
    safe_cast(investment_project_loan as string) investment_project_loan,
    safe_cast(mergers_acquisitions as string) mergers_acquisitions,
    safe_cast(working_capital as string) working_capital,
    safe_cast(epcf as string) epcf,
    safe_cast(lease as string) lease,
    safe_cast(fxsl_bop as string) fxsl_bop,
    safe_cast(cc_irs as string) cc_irs,
    safe_cast(rcf as string) rcf,
    safe_cast(gcl as string) gcl,
    safe_cast(pbc as string) pbc,
    safe_cast(pxf_commodity_prepayment as string) pxf_commodity_prepayment,
    safe_cast(inter_bank_loan as string) inter_bank_loan,
    safe_cast(
        overseas_project_contracting_loan as string
    ) overseas_project_contracting_loan,
    safe_cast(dpa as string) dpa,
    safe_cast(project_finance as string) project_finance,
    safe_cast(involving_multilateral as string) involving_multilateral,
    safe_cast(non_chinese_financier as string) non_chinese_financier,
    safe_cast(short_term as string) short_term,
    safe_cast(rescue as string) rescue,
    safe_cast(
        jv_spv_host_government_ownership as string
    ) jv_spv_host_government_ownership,
    safe_cast(
        jv_spv_chinese_government_ownership as string
    ) jv_spv_chinese_government_ownership,
    safe_cast(level_public_liability as string) level_public_liability,
    safe_cast(total_source_count as int64) total_source_count,
    safe_cast(official_source_count as int64) official_source_count,
    safe_cast(source_urls as string) source_urls,
    safe_cast(source_titles as string) source_titles,
    safe_cast(source_publishers as string) source_publishers,
    safe_cast(source_resource_types as string) source_resource_types,
    safe_cast(contact_name as string) contact_name,
    safe_cast(contact_position as string) contact_position,
    safe_cast(oda_eligible_recipient as string) oda_eligible_recipient,
    safe_cast(oecd_oda_income_group as string) oecd_oda_income_group,
    safe_cast(location_narrative as string) location_narrative,
    safe_cast(geographic_precision_available as string) geographic_precision_available,
    safe_cast(adm1_level_available as string) adm1_level_available,
    safe_cast(adm2_level_available as string) adm2_level_available,
    safe_cast(geospatial_feature_available as string) geospatial_feature_available,
    safe_cast(source_quality_score as int64) source_quality_score,
    safe_cast(data_completeness_score as int64) data_completeness_score,
    safe_cast(implementation_detail_score as int64) implementation_detail_score,
    safe_cast(loan_detail_score as int64) loan_detail_score
from {{ set_datalake_project("world_aiddata_gcdf_staging.projects") }} as t
