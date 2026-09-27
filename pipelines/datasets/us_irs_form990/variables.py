"""Column -> concordance-variable mapping for the e-file tables.

Every value read out of a return XML goes through the Nonprofit Open Data
Collective master concordance: a column below names one or more concordance
``variable_name``s, and the concordance supplies the XPaths that variable has
taken across schema versions (2009v1.0 .. 2024v5.x). The transform never
addresses an XML element by a hand-written path.

Where a column lists several variables, the first one found in the return
wins. That covers the concordance's own split of one concept into two names
(``F9_01_REV_CONTR_TOT_CY`` for Form 990 and ``..._CY_V2`` for 990-EZ) and the
EZ balance-sheet totals that only exist in the Part II variables.
"""

# Header + Part I (one row per return). Column order here is the table order
# after the key columns that the transform fills itself (year, ein, form_type,
# object_id, return_version, xml_batch_id).
RETURN_SCALARS: dict[str, list[str]] = {
    "return_timestamp": ["F9_00_RETURN_TIME_STAMP"],
    "tax_period_begin": ["F9_00_TAX_PERIOD_BEGIN_DATE"],
    "tax_period_end": ["F9_00_TAX_PERIOD_END_DATE"],
    "is_amended": ["F9_00_RETURN_AMENDED_X"],
    "is_initial": ["F9_00_RETURN_INITIAL_X"],
    "is_final": ["F9_00_RETURN_FINAL_X"],
    "is_group_return": ["F9_00_RETURN_GROUP_X"],
    "organization_name": ["F9_00_ORG_NAME_L1"],
    "organization_name_line_2": ["F9_00_ORG_NAME_L2"],
    "doing_business_as_name": ["F9_00_ORG_NAME_DBA_L1"],
    "principal_officer_name": ["F9_00_PRIN_OFF_NAME_PERS"],
    "address_line_1": ["F9_00_ORG_ADDR_L1"],
    "city": ["F9_00_ORG_ADDR_CITY"],
    "state": ["F9_00_ORG_ADDR_STATE"],
    "zip_code": ["F9_00_ORG_ADDR_ZIP"],
    "country": ["F9_00_ORG_ADDR_CNTR"],
    "website": ["F9_00_ORG_WEBSITE"],
    "formation_year": ["F9_00_YEAR_FORMATION"],
    "legal_domicile_state": ["F9_00_LEGAL_DMCL_STATE"],
    "_type_corp": ["F9_00_TYPE_ORG_CORP_X"],
    "_type_trust": ["F9_00_TYPE_ORG_TRUST_X"],
    "_type_assoc": ["F9_00_TYPE_ORG_ASSOC_X"],
    "_type_other": ["F9_00_TYPE_ORG_OTH_X"],
    "_exempt_501c3": ["F9_00_EXEMPT_STAT_501C3_X"],
    "_exempt_501c": ["F9_00_EXEMPT_STAT_501C_X"],
    "_exempt_4947a1": ["F9_00_EXEMPT_STAT_4947A1_X"],
    "_exempt_527": ["F9_00_EXEMPT_STAT_527_X"],
    "group_exemption_number": ["F9_00_GROUP_EXEMPT_NUM"],
    "mission_description": [
        "F9_01_ACT_GVRN_ACT_MISSION",
        "F9_03_ORG_MISSION_PURPOSE",
    ],
    "gross_receipts": ["F9_00_GRO_RCPT"],
    "voting_members_count": ["F9_01_ACT_GVRN_NUM_VOTE_MEMB"],
    "independent_voting_members_count": ["F9_01_ACT_GVRN_NUM_VOTE_MEMB_IND"],
    "employees_count": ["F9_01_ACT_GVRN_EMPL_TOT"],
    "volunteers_count": ["F9_01_ACT_GVRN_VOL_TOT"],
    "unrelated_business_revenue": ["F9_01_ACT_GVRN_UBIZ_REV_TOT"],
    "unrelated_business_taxable_income": ["F9_01_ACT_GVRN_UBIZ_TAXABLE_NET"],
    "contributions_grants": [
        "F9_01_REV_CONTR_TOT_CY",
        "F9_01_REV_CONTR_TOT_CY_V2",
    ],
    "contributions_grants_prior_year": ["F9_01_REV_CONTR_TOT_PY"],
    "program_service_revenue": ["F9_01_REV_PROG_TOT_CY"],
    "program_service_revenue_prior_year": ["F9_01_REV_PROG_TOT_PY"],
    "investment_income": ["F9_01_REV_INVEST_TOT_CY"],
    "investment_income_prior_year": ["F9_01_REV_INVEST_TOT_PY"],
    "other_revenue": ["F9_01_REV_OTH_CY"],
    "other_revenue_prior_year": ["F9_01_REV_OTH_PY"],
    "total_revenue": ["F9_01_REV_TOT_CY"],
    "total_revenue_prior_year": ["F9_01_REV_TOT_PY"],
    "grants_paid": ["F9_01_EXP_GRANT_SIMILAR_CY"],
    "grants_paid_prior_year": ["F9_01_EXP_GRANT_SIMILAR_PY"],
    "benefits_paid_to_members": ["F9_01_EXP_BEN_PAID_MEMB_CY"],
    "benefits_paid_to_members_prior_year": ["F9_01_EXP_BEN_PAID_MEMB_PY"],
    "salaries_compensation": ["F9_01_EXP_SAL_ETC_CY"],
    "salaries_compensation_prior_year": ["F9_01_EXP_SAL_ETC_PY"],
    "professional_fundraising_fees": ["F9_01_EXP_PROF_FUNDR_TOT_CY"],
    "professional_fundraising_fees_prior_year": [
        "F9_01_EXP_PROF_FUNDR_TOT_PY"
    ],
    "total_fundraising_expenses": ["F9_01_EXP_FUNDR_TOT_CY"],
    "other_expenses": ["F9_01_EXP_OTH_CY", "F9_01_EXP_OTH_CY_V2"],
    "other_expenses_prior_year": ["F9_01_EXP_OTH_PY"],
    "total_expenses": ["F9_01_EXP_TOT_CY"],
    "total_expenses_prior_year": ["F9_01_EXP_TOT_PY"],
    "revenue_less_expenses": ["F9_01_EXP_REV_LESS_EXP_CY"],
    "revenue_less_expenses_prior_year": ["F9_01_EXP_REV_LESS_EXP_PY"],
    "total_assets_boy": ["F9_01_NAFB_ASSET_TOT_BOY", "F9_10_ASSET_TOT_BOY"],
    "total_assets_eoy": ["F9_01_NAFB_ASSET_TOT_EOY", "F9_10_ASSET_TOT_EOY"],
    "total_liabilities_boy": ["F9_01_NAFB_LIAB_TOT_BOY", "F9_10_LIAB_TOT_BOY"],
    "total_liabilities_eoy": ["F9_01_NAFB_LIAB_TOT_EOY", "F9_10_LIAB_TOT_EOY"],
    "net_assets_boy": ["F9_01_NAFB_TOT_BOY"],
    "net_assets_eoy": ["F9_01_NAFB_TOT_EOY"],
}

# Checkbox variables: "X"/"true"/"1" -> true, anything else present -> false,
# absent -> false (an unchecked box is simply not written to the XML).
RETURN_FLAGS = {
    "is_amended",
    "is_initial",
    "is_final",
    "is_group_return",
    "_type_corp",
    "_type_trust",
    "_type_assoc",
    "_type_other",
    "_exempt_501c3",
    "_exempt_501c",
    "_exempt_4947a1",
    "_exempt_527",
}

# Part VII Section A (Form 990) / Part IV (990-EZ): one row per listed
# officer, director, trustee, key employee or highest-compensated employee.
COMPENSATION_FIELDS: dict[str, list[str]] = {
    "person_name": ["F9_07_COMP_DTK_NAME_PERS"],
    "business_name": ["F9_07_COMP_DTK_NAME_ORG_L1"],
    "title": ["F9_07_COMP_DTK_TITLE"],
    "average_hours_per_week": ["F9_07_COMP_DTK_AVE_HOUR_WEEK"],
    "average_hours_per_week_related": ["F9_07_COMP_DTK_AVE_HOUR_WEEK_RL"],
    "is_individual_trustee_or_director": ["F9_07_COMP_DTK_POS_INDIV_TRUST_X"],
    "is_institutional_trustee": ["F9_07_COMP_DTK_POS_INST_TRUST_X"],
    "is_officer": ["F9_07_COMP_DTK_POS_OFF_X"],
    "is_key_employee": ["F9_07_COMP_DTK_POS_KEY_EMPL_X"],
    "is_highest_compensated_employee": ["F9_07_COMP_DTK_POS_HIGH_COMP_X"],
    "is_former": ["F9_07_COMP_DTK_POS_FORMER_X"],
    "reportable_compensation_from_organization": ["F9_07_COMP_DTK_COMP_ORG"],
    "reportable_compensation_from_related": ["F9_07_COMP_DTK_COMP_RLTD"],
    "other_compensation": ["F9_07_COMP_DTK_COMP_OTH"],
    "employee_benefit_contributions": ["F9_07_COMP_DTK_EMPL_BEN"],
}

COMPENSATION_FLAGS = {
    "is_individual_trustee_or_director",
    "is_institutional_trustee",
    "is_officer",
    "is_key_employee",
    "is_highest_compensated_employee",
    "is_former",
}

# Every variable the transform needs; build_concordance.py keeps exactly
# these rows from the master file.
ALL_VARIABLES = sorted(
    {v for vs in RETURN_SCALARS.values() for v in vs}
    | {v for vs in COMPENSATION_FIELDS.values() for v in vs}
    | {"F9_00_ORG_EIN", "F9_00_RETURN_TYPE", "F9_00_TAX_YEAR"}
)
