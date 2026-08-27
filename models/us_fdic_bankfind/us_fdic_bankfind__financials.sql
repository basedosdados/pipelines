{{
    config(
        schema="us_fdic_bankfind",
        alias="financials",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1984, "end": 2031, "interval": 1},
        },
        cluster_by=["cert"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(report_date as date) report_date,
    safe_cast(cert as string) cert,
    safe_cast(rssd_id as string) rssd_id,
    safe_cast(net_income_quarterly_netincq as float64) net_income_quarterly_netincq,
    safe_cast(equity_capital as float64) equity_capital,
    safe_cast(
        tier_1_risk_based_capital_ratio as float64
    ) tier_1_risk_based_capital_ratio,
    safe_cast(net_discontinued_operations as float64) net_discontinued_operations,
    safe_cast(
        total_fiduciary_and_related_assets as float64
    ) total_fiduciary_and_related_assets,
    safe_cast(loans_to_individuals_ratio as float64) loans_to_individuals_ratio,
    safe_cast(
        cash_dividends_to_net_income_ytd_only as float64
    ) cash_dividends_to_net_income_ytd_only,
    safe_cast(premises_and_fixed_assets as float64) premises_and_fixed_assets,
    safe_cast(
        real_estate_principal_securitised_asset_sold as float64
    ) real_estate_principal_securitised_asset_sold,
    safe_cast(intangible_assets as float64) intangible_assets,
    safe_cast(
        nonresidential_chargeoff_per_nonresidential_ratio as float64
    ) nonresidential_chargeoff_per_nonresidential_ratio,
    safe_cast(
        off_balance_sheet_derivatives_ratio as float64
    ) off_balance_sheet_derivatives_ratio,
    safe_cast(number_of_us_offices as float64) number_of_us_offices,
    safe_cast(
        n_90_d_p_per_d_total_loans_loss_sh as float64
    ) n_90_d_p_per_d_total_loans_loss_sh,
    safe_cast(
        all_other_loans_and_leases_including_farm_ratio as float64
    ) all_other_loans_and_leases_including_farm_ratio,
    safe_cast(securities_gains_and_losses as float64) securities_gains_and_losses,
    safe_cast(efficiency_ratio as float64) efficiency_ratio,
    safe_cast(
        common_equity_tier_1_capital_ratio as float64
    ) common_equity_tier_1_capital_ratio,
    safe_cast(
        multifamily_real_estate_chargeoff_per_multi_ratio as float64
    ) multifamily_real_estate_chargeoff_per_multi_ratio,
    safe_cast(
        deposits_held_in_domestic_offices as float64
    ) deposits_held_in_domestic_offices,
    safe_cast(income_before_disc_opr_ratio as float64) income_before_disc_opr_ratio,
    safe_cast(
        interest_income_to_earning_assets_ratio as float64
    ) interest_income_to_earning_assets_ratio,
    safe_cast(bank_equity_capital_per_assets as float64) bank_equity_capital_per_assets,
    safe_cast(net_income as float64) net_income,
    safe_cast(
        net_loans_and_leases_per_deposits_ratio as float64
    ) net_loans_and_leases_per_deposits_ratio,
    safe_cast(
        number_of_foreign_offices_ratio as float64
    ) number_of_foreign_offices_ratio,
    safe_cast(nonaccrual_total_assets as float64) nonaccrual_total_assets,
    safe_cast(
        net_chargeoffs_per_loans_and_leases_ratio as float64
    ) net_chargeoffs_per_loans_and_leases_ratio,
    safe_cast(estimated_uninsured_deposits as float64) estimated_uninsured_deposits,
    safe_cast(
        noninterest_bearing_deposits_dom as float64
    ) noninterest_bearing_deposits_dom,
    safe_cast(
        loan_loss_reserve_per_gross_loans_and_leases_ratio as float64
    ) loan_loss_reserve_per_gross_loans_and_leases_ratio,
    safe_cast(
        sold_w_per_recourse_n_per_secur_oth as float64
    ) sold_w_per_recourse_n_per_secur_oth,
    safe_cast(
        real_estate_principal_securitised_asset_sold_szlnres as float64
    ) real_estate_principal_securitised_asset_sold_szlnres,
    safe_cast(
        commercial_real_estate_chargeoff_per_comm_ratio as float64
    ) commercial_real_estate_chargeoff_per_comm_ratio,
    safe_cast(total_interest_expense as float64) total_interest_expense,
    safe_cast(
        deposit_liabilities_after_exclusions as float64
    ) deposit_liabilities_after_exclusions,
    safe_cast(core_deposits as float64) core_deposits,
    safe_cast(call_form_number as float64) call_form_number,
    safe_cast(federal_reserve_id_number as float64) federal_reserve_id_number,
    safe_cast(community_bank_ratio as float64) community_bank_ratio,
    safe_cast(securities as float64) securities,
    safe_cast(
        loan_loss_prov_per_nt_chg_offs_ratio as float64
    ) loan_loss_prov_per_nt_chg_offs_ratio,
    safe_cast(
        nonperf_assets_per_total_assets as float64
    ) nonperf_assets_per_total_assets,
    safe_cast(
        loan_loss_reserve_per_n_per_c_loans_ratio as float64
    ) loan_loss_reserve_per_n_per_c_loans_ratio,
    safe_cast(number_of_full_time_employees as float64) number_of_full_time_employees,
    safe_cast(total_assets as float64) total_assets,
    safe_cast(
        n_per_c_multifamly_real_estate_per_ratio as float64
    ) n_per_c_multifamly_real_estate_per_ratio,
    safe_cast(
        n_per_c_const_real_estate_per_const_real_ratio as float64
    ) n_per_c_const_real_estate_per_const_real_ratio,
    safe_cast(nonaccrual_total_loans_loss_sh as float64) nonaccrual_total_loans_loss_sh,
    safe_cast(
        net_loans_and_leases_to_core_deposits_ratio as float64
    ) net_loans_and_leases_to_core_deposits_ratio,
    safe_cast(
        real_estate_principal_securitised_asset_sold_szlauto as float64
    ) real_estate_principal_securitised_asset_sold_szlauto,
    safe_cast(n_90_days_p_per_d_total_assets as float64) n_90_days_p_per_d_total_assets,
    safe_cast(
        n_30_89_days_p_per_d_total_assets as float64
    ) n_30_89_days_p_per_d_total_assets,
    safe_cast(
        total_allowable_exclusions_including_foreign as float64
    ) total_allowable_exclusions_including_foreign,
    safe_cast(total_liabilities as float64) total_liabilities,
    safe_cast(applicable_income_taxes as float64) applicable_income_taxes,
    safe_cast(trading_accounts as float64) trading_accounts,
    safe_cast(net_interest_margin as float64) net_interest_margin,
    safe_cast(estimated_insured_deposits as float64) estimated_insured_deposits,
    safe_cast(net_operating_income_adj as float64) net_operating_income_adj,
    safe_cast(
        net_operating_income_adj_per_assets as float64
    ) net_operating_income_adj_per_assets,
    safe_cast(
        commercial_and_industrial_loans_ratio as float64
    ) commercial_and_industrial_loans_ratio,
    safe_cast(carry_amt_loss_share_ore as float64) carry_amt_loss_share_ore,
    safe_cast(
        provisions_for_credit_losses_ratio as float64
    ) provisions_for_credit_losses_ratio,
    safe_cast(
        real_estate_chargeoff_per_real_estate_loans_ratio as float64
    ) real_estate_chargeoff_per_real_estate_loans_ratio,
    safe_cast(total_interest_income as float64) total_interest_income,
    safe_cast(net_inc_bank_and_minority_int as float64) net_inc_bank_and_minority_int,
    safe_cast(sale_of_capital_stock as float64) sale_of_capital_stock,
    safe_cast(unused_commit_total as float64) unused_commit_total,
    safe_cast(carry_amt_loss_share_lnls as float64) carry_amt_loss_share_lnls,
    safe_cast(
        total_loans_and_leases_net_chargeoffs as float64
    ) total_loans_and_leases_net_chargeoffs,
    safe_cast(
        earning_assets_per_total_assets_ratio as float64
    ) earning_assets_per_total_assets_ratio,
    safe_cast(
        n_per_c_lns_and_ls_per_gross_lns_and_ls_ratio as float64
    ) n_per_c_lns_and_ls_per_gross_lns_and_ls_ratio,
    safe_cast(loans_and_leases_net as float64) loans_and_leases_net,
    safe_cast(
        loans_to_nondep_financial_inst_con as float64
    ) loans_to_nondep_financial_inst_con,
    safe_cast(
        loans_to_nondep_financial_inst_dom as float64
    ) loans_to_nondep_financial_inst_dom,
    safe_cast(total_deposits_for_ratio as float64) total_deposits_for_ratio,
    safe_cast(
        n_per_c_nonfarm_nonresidential_real_estate_ratio as float64
    ) n_per_c_nonfarm_nonresidential_real_estate_ratio,
    safe_cast(return_on_assets_roa as float64) return_on_assets_roa,
    safe_cast(
        n_30_89_d_p_per_d_total_loans_loss_sh as float64
    ) n_30_89_d_p_per_d_total_loans_loss_sh,
    safe_cast(
        cash_dividends_on_comm_and_pref as float64
    ) cash_dividends_on_comm_and_pref,
    safe_cast(
        retained_earnings_per_avg_bk_equity_ratio as float64
    ) retained_earnings_per_avg_bk_equity_ratio,
    safe_cast(return_on_equity_roe as float64) return_on_equity_roe,
    safe_cast(
        real_estate_principal_securitised_asset_sold_szlnhel as float64
    ) real_estate_principal_securitised_asset_sold_szlnhel,
    safe_cast(
        real_estate_principal_securitised_asset_sold_szlncrcd as float64
    ) real_estate_principal_securitised_asset_sold_szlncrcd,
    safe_cast(
        noninterest_exp_per_average_assets as float64
    ) noninterest_exp_per_average_assets,
    safe_cast(
        real_estate_principal_securitised_asset_sold_szlnoth as float64
    ) real_estate_principal_securitised_asset_sold_szlnoth,
    safe_cast(fed_funds_and_repos_sold as float64) fed_funds_and_repos_sold,
    safe_cast(
        cash_and_due_from_depository_inst as float64
    ) cash_and_due_from_depository_inst,
    safe_cast(carry_amt_loss_share_oth_asset as float64) carry_amt_loss_share_oth_asset,
    safe_cast(number_of_domestic_offices as float64) number_of_domestic_offices,
    safe_cast(
        sold_w_per_recourse_n_per_secur_res as float64
    ) sold_w_per_recourse_n_per_secur_res,
    safe_cast(
        n_per_c_real_estate_lns_per_real_estate_ratio as float64
    ) n_per_c_real_estate_lns_per_real_estate_ratio,
    safe_cast(all_oth_assets_ratio as float64) all_oth_assets_ratio,
    safe_cast(other_real_estate_owned as float64) other_real_estate_owned,
    safe_cast(
        const_real_estate_chargeoff_per_const_real_ratio as float64
    ) const_real_estate_chargeoff_per_const_real_ratio,
    safe_cast(
        n_per_c_1_4_family_real_estate_per_1_4_ratio as float64
    ) n_per_c_1_4_family_real_estate_per_1_4_ratio,
    safe_cast(interest_bearing_deposits_dom as float64) interest_bearing_deposits_dom,
    safe_cast(
        commercial_and_industrial_loans_ratio_idntcir as float64
    ) commercial_and_industrial_loans_ratio_idntcir,
    safe_cast(
        interest_expense_to_earning_assets_ratio as float64
    ) interest_expense_to_earning_assets_ratio,
    safe_cast(leverage_ratio_pca as float64) leverage_ratio_pca,
    safe_cast(
        n_1_4_fam_real_estate_chargeoff_per_1_4_fam_ratio as float64
    ) n_1_4_fam_real_estate_chargeoff_per_1_4_fam_ratio,
    safe_cast(assets_per_employee_in_million as float64) assets_per_employee_in_million,
    safe_cast(all_oth_assets as float64) all_oth_assets,
    safe_cast(pretax_return_on_assets as float64) pretax_return_on_assets,
    safe_cast(
        pre_tax_net_income_operating_income as float64
    ) pre_tax_net_income_operating_income,
    safe_cast(
        tot_domestic_deposit_per_asset_ratio as float64
    ) tot_domestic_deposit_per_asset_ratio,
    safe_cast(
        nc_commercial_real_estate_per_commercial_ratio as float64
    ) nc_commercial_real_estate_per_commercial_ratio,
    safe_cast(
        total_deposit_liab_bef_exclusion as float64
    ) total_deposit_liab_bef_exclusion,
    safe_cast(
        noninterest_inc_per_average_assets as float64
    ) noninterest_inc_per_average_assets,
    safe_cast(
        loans_to_individuals_ratio_idntconr as float64
    ) loans_to_individuals_ratio_idntconr,
    safe_cast(
        credit_loss_prov_per_ave_assets as float64
    ) credit_loss_prov_per_ave_assets,
    safe_cast(total_noninterest_expense as float64) total_noninterest_expense,
    safe_cast(
        net_loans_and_leases_per_assets as float64
    ) net_loans_and_leases_per_assets,
    safe_cast(total_deposits as float64) total_deposits,
    safe_cast(
        all_other_loans_and_leases_including_farm_ratio_idncothr as float64
    ) all_other_loans_and_leases_including_farm_ratio_idncothr,
    safe_cast(net_interest_income as float64) net_interest_income,
    safe_cast(total_rbc_ratio_pca as float64) total_rbc_ratio_pca,
    safe_cast(
        real_estate_principal_securitised_asset_sold_szlnci as float64
    ) real_estate_principal_securitised_asset_sold_szlnci,
    safe_cast(carry_amt_loss_share_debt_sec as float64) carry_amt_loss_share_debt_sec,
    safe_cast(
        noncurrent_loans_which_are_wholly_or_ratio as float64
    ) noncurrent_loans_which_are_wholly_or_ratio,
    safe_cast(
        earnings_coverage_of_net_loan_chargeoffs_x_ratio as float64
    ) earnings_coverage_of_net_loan_chargeoffs_x_ratio,
    safe_cast(
        number_of_fiduciary_accounts_and_related as float64
    ) number_of_fiduciary_accounts_and_related,
    safe_cast(total_noninterest_income as float64) total_noninterest_income,
    safe_cast(total_equity_capital as float64) total_equity_capital,
    safe_cast(real_estate_loans as float64) real_estate_loans,
    safe_cast(
        commercial_and_industrial_loans as float64
    ) commercial_and_industrial_loans,
    safe_cast(consumer_loans_other as float64) consumer_loans_other,
    safe_cast(agricultural_loans as float64) agricultural_loans,
    safe_cast(
        real_estate_construction_and_land_develop as float64
    ) real_estate_construction_and_land_develop,
    safe_cast(
        real_estate_nonfarm_nonresidential_prop as float64
    ) real_estate_nonfarm_nonresidential_prop,
    safe_cast(real_estate_1_4_family as float64) real_estate_1_4_family,
    safe_cast(real_estate_multifamily as float64) real_estate_multifamily,
    safe_cast(allow_for_loans_loss_adjusted as float64) allow_for_loans_loss_adjusted,
    safe_cast(loans_and_leases_total_ratio as float64) loans_and_leases_total_ratio,
    safe_cast(noninterest_bearing_deposits as float64) noninterest_bearing_deposits,
    safe_cast(interest_bearing_deposits as float64) interest_bearing_deposits,
    safe_cast(
        num_deposits_acc_equal_or_less_than_equal_to as float64
    ) num_deposits_acc_equal_or_less_than_equal_to,
    safe_cast(
        num_deposits_acc_greater_than_250_000 as float64
    ) num_deposits_acc_greater_than_250_000,
    safe_cast(u_s_treasury_and_agency as float64) u_s_treasury_and_agency,
    safe_cast(municipal_securities as float64) municipal_securities,
    safe_cast(oth_borrowed_funds as float64) oth_borrowed_funds,
    safe_cast(quarterly_return_on_assets as float64) quarterly_return_on_assets,
    safe_cast(quarterly_return_on_equity as float64) quarterly_return_on_equity,
    safe_cast(tier_1_rbc_adjusted_llr_pca as float64) tier_1_rbc_adjusted_llr_pca,
    safe_cast(
        total_loans_and_leases_chargeoffs as float64
    ) total_loans_and_leases_chargeoffs,
    safe_cast(
        total_loans_and_leases_recoveries as float64
    ) total_loans_and_leases_recoveries,
    safe_cast(additional_noninterest_income as float64) additional_noninterest_income,
    safe_cast(
        additional_noninterest_income_quarterly as float64
    ) additional_noninterest_income_quarterly,
    safe_cast(additional_noninterest_expense as float64) additional_noninterest_expense,
    safe_cast(
        additional_noninterest_expense_quarterly as float64
    ) additional_noninterest_expense_quarterly,
    safe_cast(
        real_estate_loan_recoveries_domestic_offices as float64
    ) real_estate_loan_recoveries_domestic_offices,
    safe_cast(
        real_estate_loan_recoveries_domestic_offices_crreoffdomq as float64
    ) real_estate_loan_recoveries_domestic_offices_crreoffdomq,
    safe_cast(
        est_uninsured_deposits_in_dom_off_in_insured as float64
    ) est_uninsured_deposits_in_dom_off_in_insured,
    safe_cast(
        real_estate_loan_chargeoffs_domestic_offices as float64
    ) real_estate_loan_chargeoffs_domestic_offices,
    safe_cast(
        real_estate_loan_chargeoffs_domestic_offices_drreoffdomq as float64
    ) real_estate_loan_chargeoffs_domestic_offices_drreoffdomq,
    safe_cast(total_liabilities_and_capital as float64) total_liabilities_and_capital,
    safe_cast(
        loans_to_depository_institutions_and as float64
    ) loans_to_depository_institutions_and,
    safe_cast(loans_and_leases_unearned_inc as float64) loans_and_leases_unearned_inc,
    safe_cast(
        loans_and_leases_total_adjusted as float64
    ) loans_and_leases_total_adjusted,
    safe_cast(loans_and_leases_gross as float64) loans_and_leases_gross,
    safe_cast(real_estate_loans_dom as float64) real_estate_loans_dom,
    safe_cast(real_estate_loans_adjusted as float64) real_estate_loans_adjusted,
    safe_cast(leases as float64) leases,
    safe_cast(nonaccrual_loans_and_leases as float64) nonaccrual_loans_and_leases,
    safe_cast(
        n_90_real_estate_loans_in_domestic_offices as float64
    ) n_90_real_estate_loans_in_domestic_offices,
    safe_cast(total_n_per_c_loans_and_leases as float64) total_n_per_c_loans_and_leases,
    safe_cast(
        net_chargeoffs_all_other_loans_and_leases as float64
    ) net_chargeoffs_all_other_loans_and_leases,
    safe_cast(
        real_estate_loan_net_chargeoffs_domestic as float64
    ) real_estate_loan_net_chargeoffs_domestic,
    safe_cast(
        real_estate_loan_net_chargeoffs_domestic_ntreoffdomq as float64
    ) real_estate_loan_net_chargeoffs_domestic_ntreoffdomq,
    safe_cast(
        n_1_4_fam_real_estate_chargeoff_per_1_4_fam as float64
    ) n_1_4_fam_real_estate_chargeoff_per_1_4_fam,
    safe_cast(
        real_estate_loan_net_chargeoffs_quarterly as float64
    ) real_estate_loan_net_chargeoffs_quarterly,
    safe_cast(time_deposits_over_100m as float64) time_deposits_over_100m,
    safe_cast(other_assets as float64) other_assets,
    safe_cast(domestic_multi_service_offices as float64) domestic_multi_service_offices,
    safe_cast(nondomestic_offices as float64) nondomestic_offices,
    safe_cast(domestic_other_offices as float64) domestic_other_offices,
    safe_cast(sod_offices as float64) sod_offices,
    safe_cast(total_offices as float64) total_offices,
    safe_cast(u_s_and_other_area_offices as float64) u_s_and_other_area_offices,
    safe_cast(
        p_per_d_30_89_real_estate_loans_in_domestic as float64
    ) p_per_d_30_89_real_estate_loans_in_domestic,
    safe_cast(
        n_90_days_p_per_d_loans_and_leases as float64
    ) n_90_days_p_per_d_loans_and_leases,
    safe_cast(
        n_90_real_estate_loans_in_domestic_offices_p9relndo as float64
    ) n_90_real_estate_loans_in_domestic_offices_p9relndo,
    safe_cast(
        pre_tax_net_income_operating_income_quarterly as float64
    ) pre_tax_net_income_operating_income_quarterly,
    safe_cast(securities_af as float64) securities_af,
    safe_cast(
        equity_securities_not_held_for_trading as float64
    ) equity_securities_not_held_for_trading,
    safe_cast(securities_ha as float64) securities_ha,
    safe_cast(
        priv_issued_res_mortgage_backed_securities as float64
    ) priv_issued_res_mortgage_backed_securities,
    safe_cast(u_s_treasury_securities as float64) u_s_treasury_securities,
    safe_cast(number_of_states_with_offices as float64) number_of_states_with_offices,
    safe_cast(interest_bearing_cash_and_due as float64) interest_bearing_cash_and_due,
    safe_cast(
        total_loans_and_leases_recoveries_quarterly as float64
    ) total_loans_and_leases_recoveries_quarterly,
    safe_cast(
        total_loans_and_leases_chargeoffs_quarterly as float64
    ) total_loans_and_leases_chargeoffs_quarterly,
    safe_cast(deposit_interest_expense as float64) deposit_interest_expense,
    safe_cast(deposit_interest_expense_dom as float64) deposit_interest_expense_dom,
    safe_cast(
        deposit_interest_expense_dom_quarterly as float64
    ) deposit_interest_expense_dom_quarterly,
    safe_cast(
        total_interest_expense_annually as float64
    ) total_interest_expense_annually,
    safe_cast(
        total_interest_expense_quarterly_eintxq as float64
    ) total_interest_expense_quarterly_eintxq,
    safe_cast(
        total_interest_expense_quarterly as float64
    ) total_interest_expense_quarterly,
    safe_cast(
        all_other_noninterest_expense_quarterly as float64
    ) all_other_noninterest_expense_quarterly,
    safe_cast(all_other_noninterest_expense as float64) all_other_noninterest_expense,
    safe_cast(
        premises_and_fixed_assets_expense as float64
    ) premises_and_fixed_assets_expense,
    safe_cast(
        premises_and_fixed_assets_expense_quarterly as float64
    ) premises_and_fixed_assets_expense_quarterly,
    safe_cast(
        cash_dividends_on_comm_and_pref_quarterly as float64
    ) cash_dividends_on_comm_and_pref_quarterly,
    safe_cast(up_net_and_other_capital_comp as float64) up_net_and_other_capital_comp,
    safe_cast(salaries_and_employee_benefits as float64) salaries_and_employee_benefits,
    safe_cast(
        salaries_and_employee_benefits_quarterly as float64
    ) salaries_and_employee_benefits_quarterly,
    safe_cast(
        income_before_inc_taxes_and_disc as float64
    ) income_before_inc_taxes_and_disc,
    safe_cast(loan_income_ann as float64) loan_income_ann,
    safe_cast(loan_income_dom as float64) loan_income_dom,
    safe_cast(loan_income_dom_quarterly as float64) loan_income_dom_quarterly,
    safe_cast(loan_and_lease_income_ann as float64) loan_and_lease_income_ann,
    safe_cast(loan_and_lease_income_qtr as float64) loan_and_lease_income_qtr,
    safe_cast(loan_income_qtr as float64) loan_income_qtr,
    safe_cast(
        total_interest_income_quarterly as float64
    ) total_interest_income_quarterly,
    safe_cast(iras_and_keogh_plans_deposits as float64) iras_and_keogh_plans_deposits,
    safe_cast(total_security_income as float64) total_security_income,
    safe_cast(total_security_income_ann as float64) total_security_income_ann,
    safe_cast(
        total_security_income_quarterly as float64
    ) total_security_income_quarterly,
    safe_cast(applicable_income_taxes_ann as float64) applicable_income_taxes_ann,
    safe_cast(
        applicable_income_taxes_quarterly as float64
    ) applicable_income_taxes_quarterly,
    safe_cast(
        applicable_income_taxes_qtr_ann as float64
    ) applicable_income_taxes_qtr_ann,
    safe_cast(executive_officer_loans_amount as float64) executive_officer_loans_amount,
    safe_cast(allowance_for_loan_and_leases as float64) allowance_for_loan_and_leases,
    safe_cast(
        nonaccrual_commercial_and_industrial_loans as float64
    ) nonaccrual_commercial_and_industrial_loans,
    safe_cast(net_income_quarterly as float64) net_income_quarterly,
    safe_cast(net_operating_income_qtr as float64) net_operating_income_qtr,
    safe_cast(
        cost_of_funding_earning_assets_quarterly as float64
    ) cost_of_funding_earning_assets_quarterly,
    safe_cast(avg_total_assets as float64) avg_total_assets,
    safe_cast(deposits_institution_loans as float64) deposits_institution_loans,
    safe_cast(interest_rate_total_contracts as float64) interest_rate_total_contracts,
    safe_cast(
        total_available_for_sale_at_amortized_cost as float64
    ) total_available_for_sale_at_amortized_cost,
    safe_cast(
        total_held_to_maturity_at_fair_value as float64
    ) total_held_to_maturity_at_fair_value,
    safe_cast(securities_mv as float64) securities_mv,
    safe_cast(pledged_securities as float64) pledged_securities,
    safe_cast(trading_liabilities as float64) trading_liabilities,
    safe_cast(unearned_income as float64) unearned_income,
    safe_cast(long_term_assets_5_years_qbp as float64) long_term_assets_5_years_qbp,
    safe_cast(average_assets_adjusted_pca as float64) average_assets_adjusted_pca,
    safe_cast(
        noninterest_bearing_cash_and_due as float64
    ) noninterest_bearing_cash_and_due,
    safe_cast(net_operating_cash_flow_ann as float64) net_operating_cash_flow_ann,
    safe_cast(
        net_operating_cash_flow_ann_quarterly as float64
    ) net_operating_cash_flow_ann_quarterly,
    safe_cast(commercial_loan_recoveries as float64) commercial_loan_recoveries,
    safe_cast(
        commercial_loan_recoveries_quarterly as float64
    ) commercial_loan_recoveries_quarterly,
    safe_cast(other_consumer_loan_recoveries as float64) other_consumer_loan_recoveries,
    safe_cast(
        other_consumer_loan_recoveries_quarterly as float64
    ) other_consumer_loan_recoveries_quarterly,
    safe_cast(
        consumer_loan_recoveries_quarterly as float64
    ) consumer_loan_recoveries_quarterly,
    safe_cast(credit_card_loan_recoveries as float64) credit_card_loan_recoveries,
    safe_cast(
        credit_card_loan_recoveries_quarterly as float64
    ) credit_card_loan_recoveries_quarterly,
    safe_cast(lease_recoveries as float64) lease_recoveries,
    safe_cast(lease_recoveries_quarterly as float64) lease_recoveries_quarterly,
    safe_cast(real_estate_loan_recoveries as float64) real_estate_loan_recoveries,
    safe_cast(
        farmland_real_estate_ln_recoveries as float64
    ) farmland_real_estate_ln_recoveries,
    safe_cast(
        farmland_real_estate_ln_recoveries_qtr as float64
    ) farmland_real_estate_ln_recoveries_qtr,
    safe_cast(
        construction_real_estate_ln_recover_qtr as float64
    ) construction_real_estate_ln_recover_qtr,
    safe_cast(
        construction_real_estate_ln_recoveries as float64
    ) construction_real_estate_ln_recoveries,
    safe_cast(
        line_of_credit_real_estate_ln_recoveries as float64
    ) line_of_credit_real_estate_ln_recoveries,
    safe_cast(
        line_of_credit_real_estate_ln_recoveries_crrelocq as float64
    ) line_of_credit_real_estate_ln_recoveries_crrelocq,
    safe_cast(
        multifamily_real_estate_ln_recoveries_qtr as float64
    ) multifamily_real_estate_ln_recoveries_qtr,
    safe_cast(
        multifamily_res_real_estate_ln_recoveries as float64
    ) multifamily_res_real_estate_ln_recoveries,
    safe_cast(
        nonfarm_nonresidential_real_estate_ln as float64
    ) nonfarm_nonresidential_real_estate_ln,
    safe_cast(
        nonfarm_nonresidential_real_estate_ln_crrenrsq as float64
    ) nonfarm_nonresidential_real_estate_ln_crrenrsq,
    safe_cast(
        real_estate_loan_recoveries_quarterly as float64
    ) real_estate_loan_recoveries_quarterly,
    safe_cast(
        real_estate_loans_1_4_family_recoveries as float64
    ) real_estate_loans_1_4_family_recoveries,
    safe_cast(
        real_estate_loans_1_4_family_recover_qtr as float64
    ) real_estate_loans_1_4_family_recover_qtr,
    safe_cast(commercial_loan_chargeoffs as float64) commercial_loan_chargeoffs,
    safe_cast(
        commercial_loan_chargeoffs_quarterly as float64
    ) commercial_loan_chargeoffs_quarterly,
    safe_cast(other_consumer_loan_chargeoffs as float64) other_consumer_loan_chargeoffs,
    safe_cast(
        other_consumer_loan_chargeoffs_quarterly as float64
    ) other_consumer_loan_chargeoffs_quarterly,
    safe_cast(
        consumer_loan_chargeoffs_quarterly as float64
    ) consumer_loan_chargeoffs_quarterly,
    safe_cast(credit_card_loan_chargeoffs as float64) credit_card_loan_chargeoffs,
    safe_cast(
        credit_card_loan_chargeoffs_quarterly as float64
    ) credit_card_loan_chargeoffs_quarterly,
    safe_cast(lease_chargeoffs as float64) lease_chargeoffs,
    safe_cast(lease_chargeoffs_quarterly as float64) lease_chargeoffs_quarterly,
    safe_cast(real_estate_loan_chargeoffs as float64) real_estate_loan_chargeoffs,
    safe_cast(
        farmland_real_estate_ln_chargeoffs as float64
    ) farmland_real_estate_ln_chargeoffs,
    safe_cast(
        construction_real_estate_ln_chargeoffs as float64
    ) construction_real_estate_ln_chargeoffs,
    safe_cast(
        line_of_credit_real_estate_ln_chargeoffs as float64
    ) line_of_credit_real_estate_ln_chargeoffs,
    safe_cast(
        line_of_credit_real_estate_ln_chargeoffs_drrelocq as float64
    ) line_of_credit_real_estate_ln_chargeoffs_drrelocq,
    safe_cast(
        multifamily_res_real_estate_ln_chargeoff as float64
    ) multifamily_res_real_estate_ln_chargeoff,
    safe_cast(
        nonfarm_nonresidential_real_estate_ln_drrenres as float64
    ) nonfarm_nonresidential_real_estate_ln_drrenres,
    safe_cast(
        real_estate_loan_chargeoffs_quarterly as float64
    ) real_estate_loan_chargeoffs_quarterly,
    safe_cast(
        real_estate_loans_1_4_family_chargeoffs as float64
    ) real_estate_loans_1_4_family_chargeoffs,
    safe_cast(
        real_estate_loans_1_4_family_chg_offs_qtr as float64
    ) real_estate_loans_1_4_family_chg_offs_qtr,
    safe_cast(efficiency_ratio_expense as float64) efficiency_ratio_expense
from {{ set_datalake_project("us_fdic_bankfind_staging.financials") }} as t
