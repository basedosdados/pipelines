{{ config(alias="country", schema="world_oecd_public_finance") }}
select
    safe_cast(year as int64) year,
    safe_cast(country as string) country,
    safe_cast(revenue_personal_income_tax as float64) revenue_personal_income_tax,
    safe_cast(
        revenue_social_security_contribution as float64
    ) revenue_social_security_contribution,
    safe_cast(revenue_corporate_tax as float64) revenue_corporate_tax,
    safe_cast(revenue_environmental_tax as float64) revenue_environmental_tax,
    safe_cast(revenue_other_consumption_tax as float64) revenue_other_consumption_tax,
    safe_cast(revenue_immovable_property_tax as float64) revenue_immovable_property_tax,
    safe_cast(revenue_other_property_tax as float64) revenue_other_property_tax,
    safe_cast(
        revenue_sales_goods_services_tax as float64
    ) revenue_sales_goods_services_tax,
    safe_cast(revenue_other_non_property_tax as float64) revenue_other_non_property_tax,
    safe_cast(revenue_property_income as float64) revenue_property_income,
    safe_cast(
        revenue_property_income_except_interest as float64
    ) revenue_property_income_except_interest,
    safe_cast(expenditure_education as float64) expenditure_education,
    safe_cast(expenditure_health as float64) expenditure_health,
    safe_cast(
        expenditure_wage_intermediate_consumption as float64
    ) expenditure_wage_intermediate_consumption,
    safe_cast(expenditure_pension as float64) expenditure_pension,
    safe_cast(
        expenditure_sickness_disability as float64
    ) expenditure_sickness_disability,
    safe_cast(
        expenditure_unemployment_benefit as float64
    ) expenditure_unemployment_benefit,
    safe_cast(expenditure_family_children as float64) expenditure_family_children,
    safe_cast(expenditure_subsidies as float64) expenditure_subsidies,
    safe_cast(expenditure_public_investment as float64) expenditure_public_investment,
    safe_cast(
        expenditure_other_primary_expenditure as float64
    ) expenditure_other_primary_expenditure,
    safe_cast(expenditure_property_income as float64) expenditure_property_income,
    safe_cast(
        expenditure_property_income_except_interest as float64
    ) expenditure_property_income_except_interest,
    safe_cast(
        revenue_personal_income_tax_adjusted as float64
    ) revenue_personal_income_tax_adjusted,
    safe_cast(
        revenue_social_security_contribution_adjusted as float64
    ) revenue_social_security_contribution_adjusted,
    safe_cast(revenue_corporate_tax_adjusted as float64) revenue_corporate_tax_adjusted,
    safe_cast(
        revenue_environmental_tax_adjusted as float64
    ) revenue_environmental_tax_adjusted,
    safe_cast(
        revenue_other_consumption_tax_adjusted as float64
    ) revenue_other_consumption_tax_adjusted,
    safe_cast(
        revenue_immovable_property_tax_adjusted as float64
    ) revenue_immovable_property_tax_adjusted,
    safe_cast(
        revenue_other_property_tax_adjusted as float64
    ) revenue_other_property_tax_adjusted,
    safe_cast(
        revenue_sales_goods_services_tax_adjusted as float64
    ) revenue_sales_goods_services_tax_adjusted,
    safe_cast(
        revenue_other_non_property_tax_adjusted as float64
    ) revenue_other_non_property_tax_adjusted,
    safe_cast(
        revenue_property_income_adjusted as float64
    ) revenue_property_income_adjusted,
    safe_cast(
        revenue_property_income_except_interest_adjusted as float64
    ) revenue_property_income_except_interest_adjusted,
    safe_cast(expenditure_education_adjusted as float64) expenditure_education_adjusted,
    safe_cast(expenditure_health_adjusted as float64) expenditure_health_adjusted,
    safe_cast(
        expenditure_wage_intermediate_consumption_adjusted as float64
    ) expenditure_wage_intermediate_consumption_adjusted,
    safe_cast(expenditure_pension_adjusted as float64) expenditure_pension_adjusted,
    safe_cast(
        expenditure_sickness_disability_adjusted as float64
    ) expenditure_sickness_disability_adjusted,
    safe_cast(
        expenditure_unemployment_benefit_adjusted as float64
    ) expenditure_unemployment_benefit_adjusted,
    safe_cast(
        expenditure_family_children_adjusted as float64
    ) expenditure_family_children_adjusted,
    safe_cast(expenditure_subsidies_adjusted as float64) expenditure_subsidies_adjusted,
    safe_cast(
        expenditure_public_investment_adjusted as float64
    ) expenditure_public_investment_adjusted,
    safe_cast(
        expenditure_other_primary_expenditure_adjusted as float64
    ) expenditure_other_primary_expenditure_adjusted,
    safe_cast(
        expenditure_property_income_adjusted as float64
    ) expenditure_property_income_adjusted,
    safe_cast(
        expenditure_property_income_except_interest_adjusted as float64
    ) expenditure_property_income_except_interest_adjusted,
    safe_cast(current_receipt as float64) current_receipt,
    safe_cast(
        current_receipt_except_interest as float64
    ) current_receipt_except_interest,
    safe_cast(current_receipt_adjusted as float64) current_receipt_adjusted,
    safe_cast(total_receipt as float64) total_receipt,
    safe_cast(current_expenditure as float64) current_expenditure,
    safe_cast(
        current_expenditure_except_interest as float64
    ) current_expenditure_except_interest,
    safe_cast(current_expenditure_adjusted as float64) current_expenditure_adjusted,
    safe_cast(
        current_expenditure_except_interest_adjusted as float64
    ) current_expenditure_except_interest_adjusted,
    safe_cast(total_expenditure as float64) total_expenditure,
    safe_cast(net_lending as float64) net_lending,
    safe_cast(primary_balance as float64) primary_balance,
    safe_cast(net_lending_adjusted as float64) net_lending_adjusted,
    safe_cast(primary_balance_adjusted as float64) primary_balance_adjusted,
    safe_cast(underlying_net_lending as float64) underlying_net_lending,
    safe_cast(underlying_primary_balance as float64) underlying_primary_balance,
    safe_cast(net_financial_liabilities as float64) net_financial_liabilities,
    safe_cast(financial_assets as float64) financial_assets,
    safe_cast(gross_interest_paid as float64) gross_interest_paid,
    safe_cast(gross_interest_received as float64) gross_interest_received,
    safe_cast(net_interest_paid as float64) net_interest_paid,
    safe_cast(
        gross_domestic_product_current_prices as float64
    ) gross_domestic_product_current_prices,
    safe_cast(gross_domestic_product_volume as float64) gross_domestic_product_volume,
    safe_cast(
        gross_domestic_product_potential_current_prices as float64
    ) gross_domestic_product_potential_current_prices,
    safe_cast(
        gross_domestic_product_potential_volume as float64
    ) gross_domestic_product_potential_volume,
    safe_cast(output_gap as float64) output_gap,
    safe_cast(short_term_interest_rate as float64) short_term_interest_rate,
    safe_cast(long_term_interest_rate as float64) long_term_interest_rate,
    safe_cast(consumer_price_index as float64) consumer_price_index,
    safe_cast(exchange_rate as float64) exchange_rate,
    safe_cast(
        nominal_effective_exchange_rate as float64
    ) nominal_effective_exchange_rate,
    safe_cast(real_effective_exchange_rate as float64) real_effective_exchange_rate,
    safe_cast(total_employment as float64) total_employment,
    safe_cast(government_employment as float64) government_employment,
    safe_cast(labor_force as float64) labor_force,
    safe_cast(unemployment_rate as float64) unemployment_rate,
    safe_cast(export as float64) export,
    safe_cast(import as float64) import,
    safe_cast(deflator_export as float64) deflator_export,
    safe_cast(deflator_import as float64) deflator_import,
    safe_cast(
        deflator_gross_domestic_product as float64
    ) deflator_gross_domestic_product,
    safe_cast(
        government_fixed_capital_formation as float64
    ) government_fixed_capital_formation,
    safe_cast(capital_transfers as float64) capital_transfers,
    safe_cast(
        government_consumption_fixed_capital as float64
    ) government_consumption_fixed_capital,
    safe_cast(capital_tax_transfers_receipts as float64) capital_tax_transfers_receipts,
    safe_cast(term_trade as float64) term_trade,
    safe_cast(trade_openness_ratio as float64) trade_openness_ratio,
    safe_cast(
        primary_total_expenditure_adjustred as float64
    ) primary_total_expenditure_adjustred,
    safe_cast(total_expenditure_adjusted as float64) total_expenditure_adjusted,
    safe_cast(total_receipt_adjusted as float64) total_receipt_adjusted,
    safe_cast(primary_total_receipt_adjusted as float64) primary_total_receipt_adjusted,
    safe_cast(
        expenditure_labor_policy_active as float64
    ) expenditure_labor_policy_active,
    safe_cast(
        expenditure_labor_policy_passive as float64
    ) expenditure_labor_policy_passive,
    safe_cast(size_municipalities as float64) size_municipalities,
    safe_cast(share_women_parliament as float64) share_women_parliament,
    safe_cast(share_women_minister as float64) share_women_minister,
    safe_cast(government_confidence as float64) government_confidence,
    safe_cast(rule_of_law_limited_power as float64) rule_of_law_limited_power,
    safe_cast(rule_of_law_rights as float64) rule_of_law_rights,
    safe_cast(expenditure_health_pc as float64) expenditure_health_pc,
    safe_cast(judicial_confidence as float64) judicial_confidence,
    safe_cast(
        rule_of_law_justice_enforcement as float64
    ) rule_of_law_justice_enforcement,
    safe_cast(
        rule_of_law_justice_government_influence as float64
    ) rule_of_law_justice_government_influence,
    safe_cast(index_ourdata as float64) index_ourdata,
    safe_cast(
        internet_interaction_authoriries as float64
    ) internet_interaction_authoriries,
    safe_cast(average_income_tax_rate as float64) average_income_tax_rate,
    safe_cast(
        average_employee_social_security_rate as float64
    ) average_employee_social_security_rate,
    safe_cast(
        average_employer_social_security_rate as float64
    ) average_employer_social_security_rate,
    safe_cast(
        average_income_social_security_rate as float64
    ) average_income_social_security_rate,
    safe_cast(net_personal_average_tax_rate as float64) net_personal_average_tax_rate,
    safe_cast(average_tax_wedge as float64) average_tax_wedge,
    safe_cast(marginal_tax_wedge as float64) marginal_tax_wedge,
    safe_cast(
        total_red_expenditure_intramural as float64
    ) total_red_expenditure_intramural,
    safe_cast(
        total_red_expenditure_government as float64
    ) total_red_expenditure_government,
    safe_cast(budget_aproppriation_red as float64) budget_aproppriation_red,
    safe_cast(
        basic_red_expenditure_intramural as float64
    ) basic_red_expenditure_intramural,
    safe_cast(
        basic_red_expenditure_government as float64
    ) basic_red_expenditure_government,
    safe_cast(
        female_labor_participation_rate as float64
    ) female_labor_participation_rate,
    safe_cast(male_labor_participation_rate as float64) male_labor_participation_rate,
    safe_cast(fertility_rate as float64) fertility_rate,
    safe_cast(life_expectancy as float64) life_expectancy,
    safe_cast(gini_disposable_income as float64) gini_disposable_income,
    safe_cast(gini_market_income as float64) gini_market_income,
    safe_cast(gini_government_income as float64) gini_government_income,
    safe_cast(poverty_rate as float64) poverty_rate,
    safe_cast(
        pmr_market_regulation_indicator as float64
    ) pmr_market_regulation_indicator,
    safe_cast(pmr_state_control as float64) pmr_state_control,
    safe_cast(pmr_barriers_entrepeneurship as float64) pmr_barriers_entrepeneurship,
    safe_cast(pmr_barriers_trade_investment as float64) pmr_barriers_trade_investment,
    safe_cast(
        employment_contract_protect_ex_collective_dismissal as float64
    ) employment_contract_protect_ex_collective_dismissal,
    safe_cast(
        employment_contract_protect_in_collective_dismissal as float64
    ) employment_contract_protect_in_collective_dismissal,
    safe_cast(cabinet_right as float64) cabinet_right,
    safe_cast(cabinet_center as float64) cabinet_center,
    safe_cast(cabinet_left as float64) cabinet_left,
    safe_cast(cabinet_composition as float64) cabinet_composition,
    safe_cast(
        cabinet_ideological_composition as float64
    ) cabinet_ideological_composition,
    safe_cast(cabinet_ideological_gap as float64) cabinet_ideological_gap,
    safe_cast(government_change as float64) government_change,
    safe_cast(election_turnout as float64) election_turnout,
    safe_cast(
        `budget_perspective_medium term` as float64
    ) budget_perspective_medium_term,
    safe_cast(performance_budget as float64) performance_budget,
    safe_cast(government_capital_stock as float64) government_capital_stock,
    safe_cast(
        public_private_partnership_capital_stock as float64
    ) public_private_partnership_capital_stock,
    safe_cast(corporate_income_tax_rate as float64) corporate_income_tax_rate,
    safe_cast(vat_rate as float64) vat_rate,
    safe_cast(voice_accountability as float64) voice_accountability,
    safe_cast(regulatory_quality as float64) regulatory_quality,
    safe_cast(rule_of_law as float64) rule_of_law,
    safe_cast(political_stability as float64) political_stability,
    safe_cast(government_effectiveness as float64) government_effectiveness,
    safe_cast(corruption_control as float64) corruption_control,
    safe_cast(
        indicator_fiscal_rule_expenditure as int64
    ) indicator_fiscal_rule_expenditure,
    safe_cast(indicator_fiscal_rule_revenue as int64) indicator_fiscal_rule_revenue,
    safe_cast(indicator_fiscal_rule_balance as int64) indicator_fiscal_rule_balance,
    safe_cast(indicator_fiscal_rule_debt as int64) indicator_fiscal_rule_debt,
    safe_cast(indicator_fiscal_council as int64) indicator_fiscal_council
from {{ set_datalake_project("world_oecd_public_finance_staging.country") }} as t
