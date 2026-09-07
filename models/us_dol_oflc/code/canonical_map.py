"""Canonical column definitions and per-program source aliases for us_dol_oflc.

The OFLC disclosure files rename, split and merge columns almost every fiscal
year. This module is the single place where that churn is resolved: each
canonical column lists every source header that has ever carried it, and
``build_crosswalk.py`` turns those aliases into the committed per-year crosswalk
CSVs under ``code/crosswalk/``.

Rules:

* Aliases are matched case-insensitively after stripping whitespace; a source
  header may map to at most one canonical column per fiscal year.
* ``DROP`` lists source columns that are deliberately not published, with the
  reason. Anything neither aliased nor dropped is reported as UNMAPPED, which
  fails the crosswalk build — an unrecognised column is never silently ignored.
* Personal contact details of individuals (employer points of contact,
  attorneys, preparers — names, phones, emails, home addresses) are dropped.
  Business names are kept.
"""

# --------------------------------------------------------------------------
# Canonical column order, shared prefix first. (name, bigquery_type)
# --------------------------------------------------------------------------

CORE = [
    ("year", "INT64"),
    ("case_number", "STRING"),
    ("case_status", "STRING"),
    ("visa_class", "STRING"),
    ("received_date", "DATE"),
    ("decision_date", "DATE"),
    ("employment_begin_date", "DATE"),
    ("employment_end_date", "DATE"),
    ("employer_name", "STRING"),
    ("employer_trade_name", "STRING"),
    ("employer_address", "STRING"),
    ("employer_city", "STRING"),
    ("employer_state", "STRING"),
    ("employer_postal_code", "STRING"),
    ("employer_country", "STRING"),
    ("naics_id", "STRING"),
    ("job_title", "STRING"),
    ("soc_id", "STRING"),
    ("soc_title", "STRING"),
    ("full_time_position", "STRING"),
    ("wage_offered_from", "FLOAT64"),
    ("wage_offered_to", "FLOAT64"),
    ("wage_unit_of_pay", "STRING"),
    ("wage_offered_from_annual", "FLOAT64"),
    ("wage_offered_to_annual", "FLOAT64"),
    ("prevailing_wage", "FLOAT64"),
    ("prevailing_wage_unit_of_pay", "STRING"),
    ("prevailing_wage_annual", "FLOAT64"),
    ("prevailing_wage_level", "STRING"),
    ("prevailing_wage_source", "STRING"),
    ("prevailing_wage_source_year", "STRING"),
    ("prevailing_wage_tracking_number", "STRING"),
    ("worksite_address", "STRING"),
    ("worksite_city", "STRING"),
    ("worksite_county", "STRING"),
    ("worksite_state", "STRING"),
    ("worksite_postal_code", "STRING"),
    ("attorney_law_firm_name", "STRING"),
    ("agent_representing_employer", "STRING"),
]

EXTRA = {
    "lca": [
        ("original_certification_date", "DATE"),
        ("total_workers", "INT64"),
        ("new_employment", "INT64"),
        ("continued_employment", "INT64"),
        ("change_previous_employment", "INT64"),
        ("new_concurrent_employment", "INT64"),
        ("change_employer", "INT64"),
        ("amended_petition", "INT64"),
        ("worksite_workers", "INT64"),
        ("total_worksite_locations", "INT64"),
        ("secondary_entity", "STRING"),
        ("secondary_entity_business_name", "STRING"),
        ("h1b_dependent", "STRING"),
        ("willful_violator", "STRING"),
        ("support_h1b", "STRING"),
        ("statutory_basis", "STRING"),
        ("withdrawn", "STRING"),
    ],
    "perm": [
        ("application_type", "STRING"),
        ("refile", "STRING"),
        ("schedule_a_sheepherder", "STRING"),
        ("us_economic_sector", "STRING"),
        ("employer_num_employees", "INT64"),
        ("employer_year_commenced_business", "INT64"),
        ("prevailing_wage_determination_date", "DATE"),
        ("prevailing_wage_expiration_date", "DATE"),
        ("minimum_education", "STRING"),
        ("major_field_of_study", "STRING"),
        ("required_experience", "STRING"),
        ("required_experience_months", "INT64"),
        ("country_of_citizenship", "STRING"),
        ("foreign_worker_birth_country", "STRING"),
        ("class_of_admission", "STRING"),
        ("foreign_worker_education", "STRING"),
        ("is_multiple_worksites", "STRING"),
    ],
    "h2a": [
        ("certification_begin_date", "DATE"),
        ("certification_end_date", "DATE"),
        ("requested_begin_date", "DATE"),
        ("requested_end_date", "DATE"),
        ("type_of_employer_application", "STRING"),
        ("h2a_labor_contractor", "STRING"),
        ("nature_of_temporary_need", "STRING"),
        ("emergency_filing", "STRING"),
        ("primary_crop", "STRING"),
        ("job_order_number", "STRING"),
        ("workers_requested", "INT64"),
        ("workers_certified", "INT64"),
        ("anticipated_number_of_hours", "FLOAT64"),
        ("piece_rate_offer", "FLOAT64"),
        ("piece_rate_unit", "STRING"),
        ("overtime_rate_from", "FLOAT64"),
        ("overtime_rate_to", "FLOAT64"),
        ("frequency_of_pay", "STRING"),
        ("education_level", "STRING"),
        ("work_experience_months", "INT64"),
        ("housing_city", "STRING"),
        ("housing_state", "STRING"),
        ("housing_type", "STRING"),
        ("housing_total_occupancy", "INT64"),
        ("meals_provided", "STRING"),
        ("total_worksite_records", "INT64"),
    ],
    "h2b": [
        ("application_type", "STRING"),
        ("certification_begin_date", "DATE"),
        ("certification_end_date", "DATE"),
        ("requested_begin_date", "DATE"),
        ("requested_end_date", "DATE"),
        ("nature_of_temporary_need", "STRING"),
        ("cap_exempt", "STRING"),
        ("type_of_employer", "STRING"),
        ("job_order_number", "STRING"),
        ("job_order_submit_date", "DATE"),
        ("swa_state", "STRING"),
        ("workers_requested", "INT64"),
        ("workers_certified", "INT64"),
        ("anticipated_number_of_hours", "FLOAT64"),
        ("overtime_rate_from", "FLOAT64"),
        ("overtime_rate_to", "FLOAT64"),
        ("msa_name", "STRING"),
        ("education_level", "STRING"),
        ("work_experience_months", "INT64"),
    ],
}

TRAILING = [("source_file", "STRING")]


# Core columns a given program never reports in any fiscal year. Publishing an
# always-NULL column is worse than omitting it, so they are filtered out here.
# Each entry is a fact about the source forms, verified against the crosswalk.
OMIT = {
    "lca": set(),
    "perm": {
        # ETA-9089 has no visa class, no employment period, and one worker per
        # case; the wage source year is not published.
        "visa_class", "employment_begin_date", "employment_end_date",
        "total_workers", "prevailing_wage_source_year",
    },
    "h2a": {
        # ETA-790/9142A reports a single wage offer and no prevailing wage —
        # H-2A pay is set by the Adverse Effect Wage Rate, published separately.
        "total_workers", "wage_offered_to",
        "prevailing_wage", "prevailing_wage_unit_of_pay", "prevailing_wage_annual",
        "prevailing_wage_level", "prevailing_wage_source",
        "prevailing_wage_source_year", "prevailing_wage_tracking_number",
    },
    "h2b": {
        # ETA-9142B stopped publishing the prevailing wage amount after FY2015;
        # only the determination case number survives.
        "total_workers", "prevailing_wage_level", "prevailing_wage_source",
        "prevailing_wage_source_year",
    },
}


def columns(program: str) -> list[tuple[str, str]]:
    """Canonical column list, in order, for one program table."""
    omit = OMIT[program]
    return [c for c in CORE + EXTRA[program] + TRAILING if c[0] not in omit]


# --------------------------------------------------------------------------
# Source aliases. Order matters only for readability; matching is by set.
# --------------------------------------------------------------------------

_SHARED = {
    "case_number": ["CASE_NUMBER", "CASE_NO", "LCA_CASE_NUMBER"],
    "case_status": ["CASE_STATUS", "STATUS", "APPROVAL_STATUS"],
    "visa_class": ["VISA_CLASS", "VISA_TYPE", "PROGRAM", "PROGRAM_DESIGNATION"],
    "received_date": [
        "RECEIVED_DATE", "CASE_RECEIVED_DATE", "CASE_SUBMITTED", "SUBMITTED_DATE",
        "LCA_CASE_SUBMIT", "NPC_SUBMITTED_DATE", "ORIG_FILE_DATE",
    ],
    "decision_date": ["DECISION_DATE", "DOL_DECISION_DATE", "RECENT_DECISION_DATE"],
    "employment_begin_date": [
        "EMPLOYMENT_BEGIN_DATE", "EMPLOYMENT_START_DATE", "BEGIN_DATE",
        "LCA_CASE_EMPLOYMENT_START_DATE", "PERIOD_OF_EMPLOYMENT_START_DATE",
        "JOB_START_DATE",
    ],
    "employment_end_date": [
        "EMPLOYMENT_END_DATE", "END_DATE", "LCA_CASE_EMPLOYMENT_END_DATE",
        "PERIOD_OF_EMPLOYMENT_END_DATE", "JOB_END_DATE",
    ],
    "employer_name": [
        "EMPLOYER_NAME", "NAME", "LCA_CASE_EMPLOYER_NAME", "EMP_BUSINESS_NAME",
    ],
    "employer_trade_name": [
        "TRADE_NAME_DBA", "EMPLOYER_BUSINESS_DBA", "EMP_TRADE_NAME",
    ],
    "employer_address": [
        "EMPLOYER_ADDRESS", "EMPLOYER_ADDRESS1", "EMPLOYER_ADDRESS_1", "ADDRESS1",
        "LCA_CASE_EMPLOYER_ADDRESS", "LCA_CASE_EMPLOYER_ADDRESS1", "EMP_ADDR1",
    ],
    "employer_city": [
        "EMPLOYER_CITY", "CITY", "LCA_CASE_EMPLOYER_CITY", "EMP_CITY",
    ],
    "employer_state": [
        "EMPLOYER_STATE", "STATE", "LCA_CASE_EMPLOYER_STATE", "EMP_STATE",
        "EMPLOYER_STATE_PROVINCE",
    ],
    "employer_postal_code": [
        "EMPLOYER_POSTAL_CODE", "POSTAL_CODE", "LCA_CASE_EMPLOYER_POSTAL_CODE",
        "EMP_POSTCODE",
    ],
    "employer_country": ["EMPLOYER_COUNTRY", "EMP_COUNTRY"],
    "naics_id": [
        "NAICS_CODE", "NAIC_CODE", "LCA_CASE_NAICS_CODE", "EMP_NAICS",
        "NAICS_US_CODE", "2007_NAICS_US_CODE",
    ],
    "job_title": [
        "JOB_TITLE", "LCA_CASE_JOB_TITLE", "JOB_INFO_JOB_TITLE",
        "PW_JOB_TITLE_9089", "PW_JOB_TITLE", "ADD_THESE_PW_JOB_TITLE_9089",
        "OCCUPATIONAL_TITLE",
    ],
    "soc_id": [
        "SOC_CODE", "SOC_CODE_ID", "LCA_CASE_SOC_CODE", "PW_SOC_CODE",
        "PWD_SOC_CODE", "OCCUPATIONAL_CODE", "JOB_CODE",
    ],
    "soc_title": [
        "SOC_TITLE", "SOC_NAME", "LCA_CASE_SOC_NAME", "PW_SOC_TITLE",
        "PWD_SOC_TITLE",
    ],
    "full_time_position": [
        "FULL_TIME_POSITION", "FULL_TIME_POS", "FULL_TIME",
        "OTHER_REQ_IS_FULLTIME_EMP",
    ],
    "total_workers": [
        "TOTAL_WORKERS", "TOTAL WORKERS", "TOTAL_WORKER_POSITIONS",
        "NBR_IMMIGRANTS",
    ],
    "wage_offered_from": [
        "WAGE_RATE_OF_PAY_FROM", "WAGE_RATE_OF_PAY_FROM_1", "WAGE_RATE_OF_PAY",
        "LCA_CASE_WAGE_RATE_FROM", "WAGE_RATE_1", "WAGE_RATE__2",
        "WAGE_OFFER_FROM_9089", "WAGE_OFFERED_FROM_9089", "WAGE_OFFER_FROM",
        "JOB_OPP_WAGE_FROM",
        "BASIC_RATE_OF_PAY", "BASIC_WAGE_RATE_FROM", "WAGE_OFFER",
    ],
    "wage_offered_to": [
        "WAGE_RATE_OF_PAY_TO", "WAGE_RATE_OF_PAY_TO_1", "LCA_CASE_WAGE_RATE_TO",
        "MAX_RATE_1", "WAGE_OFFER_TO_9089", "WAGE_OFFERED_TO_9089", "WAGE_OFFER_TO",
        "JOB_OPP_WAGE_TO",
        "BASIC_WAGE_RATE_TO",
    ],
    "wage_unit_of_pay": [
        "WAGE_UNIT_OF_PAY", "WAGE_UNIT_OF_PAY_1", "LCA_CASE_WAGE_RATE_UNIT",
        "RATE_PER_1", "WAGE_OFFER_UNIT_OF_PAY_9089", "WAGE_OFFERED_UNIT_OF_PAY_9089",
        "WAGE_OFFER_UNIT_OF_PAY", "JOB_OPP_WAGE_PER", "BASIC_UNIT_OF_PAY",
        "PAY_RANGE_UNIT", "PER",
    ],
    "prevailing_wage": [
        "PREVAILING_WAGE", "PREVIALING_WAGE", "PREVAILING_WAGE_1", "PW_1",
        "PW_AMOUNT_9089", "PW_WAGE",
    ],
    "prevailing_wage_unit_of_pay": [
        "PW_UNIT_OF_PAY", "PW_UNIT_OF_PAY_1", "PW_UNIT_1",
        "PW_UNIT_OF_PAY_9089",
    ],
    "prevailing_wage_level": [
        "PW_WAGE_LEVEL", "PW_WAGE_LEVEL_1", "PW_LEVEL_9089", "PW_SKILL_LEVEL",
    ],
    "prevailing_wage_source": [
        "PW_SOURCE", "PW_WAGE_SOURCE", "PW_SOURCE_1", "WAGE_SOURCE_1",
        "PW_SOURCE_NAME_9089", "PW_OTHER_SOURCE", "PW_OTHER_SOURCE_1",
    ],
    "prevailing_wage_source_year": [
        "PW_SOURCE_YEAR", "PW_WAGE_SOURCE_YEAR", "YR_SOURCE_PUB_1",
        "PW_OES_YEAR", "PW_OES_YEAR_1",
    ],
    "prevailing_wage_tracking_number": [
        "PW_TRACKING_NUMBER", "PW_TRACKING_NUMBER_1", "PW_TRACK_NUM",
        "PW_TRACK_NUMBER", "JOB_OPP_PWD_NUMBER", "1st_PWD_CASE_NUMBER",
    ],
    "worksite_address": [
        "WORKSITE_ADDRESS", "WORKSITE_ADDRESS1", "WORKSITE_ADDRESS1_1",
        "WORKSITE_ADDRESS_1", "PRIMARY_WORKSITE_ADDR1",
    ],
    "worksite_city": [
        "WORKSITE_CITY", "WORKSITE_CITY_1", "CITY_1", "LCA_CASE_WORKLOC1_CITY",
        "WORK_LOCATION_CITY1", "JOB_INFO_WORK_CITY", "PRIMARY_WORKSITE_CITY",
        "WORKSITE_LOCATION_CITY", "EMPLOYEE_WORKSITE_CITY",
    ],
    "worksite_county": [
        "WORKSITE_COUNTY", "WORKSITE_COUNTY_1", "PRIMARY_WORKSITE_COUNTY",
        "EMPLOYEE_WORKSITE_COUNTY",
    ],
    "worksite_state": [
        "WORKSITE_STATE", "WORKSITE_STATE_1", "STATE_1",
        "LCA_CASE_WORKLOC1_STATE", "WORK_LOCATION_STATE1", "JOB_INFO_WORK_STATE",
        "PRIMARY_WORKSITE_STATE", "ALIEN_WORK_STATE", "WORKSITE_LOCATION_STATE",
        "EMPLOYEE_WORK_STATE",
    ],
    "worksite_postal_code": [
        "WORKSITE_POSTAL_CODE", "WORKSITE_POSTAL_CODE_1",
        "JOB_INFO_WORK_POSTAL_CODE", "PRIMARY_WORKSITE_POSTAL_CODE",
        "EMPLOYEE_POSTAL_CODE",
    ],
    "attorney_law_firm_name": [
        "LAWFIRM_NAME_BUSINESS_NAME", "LAWFIRM_NAME", "AGENT_FIRM_NAME",
        "AGENT_ATTORNEY_LAW_FIRM_BUSINESS_NAME", "AGENT_ATTORNEY_FIRM_NAME",
        "ATTY_AG_LAW_FIRM_NAME",
    ],
    "agent_representing_employer": [
        "AGENT_REPRESENTING_EMPLOYER", "TYPE_OF_REPRESENTATION",
        "ATTY_AG_REP_TYPE", "FW_INFO_ATTY_OR_AGENT", "EMPLOYER_REP_BY_AGENT",
        "AGENT_POC_EMPLOYER_REP_BY_AGENT",
    ],
}

ALIASES: dict[str, dict[str, list[str]]] = {
    "lca": dict(_SHARED, **{
        "original_certification_date": ["ORIGINAL_CERT_DATE"],
        "new_employment": ["NEW_EMPLOYMENT"],
        "continued_employment": ["CONTINUED_EMPLOYMENT"],
        "change_previous_employment": ["CHANGE_PREVIOUS_EMPLOYMENT"],
        "new_concurrent_employment": ["NEW_CONCURRENT_EMPLOYMENT", "NEW_CONCURRENT_EMP"],
        "change_employer": ["CHANGE_EMPLOYER"],
        "amended_petition": ["AMENDED_PETITION"],
        "worksite_workers": ["WORKSITE_WORKERS", "WORKSITE_WORKERS_1"],
        "total_worksite_locations": ["TOTAL_WORKSITE_LOCATIONS"],
        "secondary_entity": ["SECONDARY_ENTITY", "SECONDARY_ENTITY_1"],
        "secondary_entity_business_name": [
            "SECONDARY_ENTITY_BUSINESS_NAME", "SECONDARY_ENTITY_BUSINESS_NAME_1",
        ],
        "h1b_dependent": ["H-1B_DEPENDENT", "H1B_DEPENDENT", "H_1B_DEPENDENT"],
        "willful_violator": ["WILLFUL_VIOLATOR", "WILLFUL VIOLATOR"],
        "support_h1b": ["SUPPORT_H1B"],
        "statutory_basis": ["STATUTORY_BASIS"],
        "withdrawn": ["WITHDRAWN"],
    }),
    "perm": dict(_SHARED, **{
        "application_type": ["APPLICATION_TYPE", "OCCUPATION_TYPE"],
        "refile": ["REFILE"],
        "schedule_a_sheepherder": ["SCHD_A_SHEEPHERDER"],
        "us_economic_sector": ["US_ECONOMIC_SECTOR"],
        "employer_num_employees": ["EMPLOYER_NUM_EMPLOYEES", "EMP_NUM_PAYROLL"],
        "employer_year_commenced_business": [
            "EMPLOYER_YR_ESTAB", "EMPLOYER_YEAR_COMMENCED_BUSINESS",
            "EMP_YEAR_COMMENCED_BUSINESS", "EMP_YEAR_COMMENCED",
        ],
        "prevailing_wage_determination_date": ["PW_DETERM_DATE", "PW_DETERMINATION_DATE"],
        "prevailing_wage_expiration_date": ["PW_EXPIRE_DATE", "PW_EXPIRATION_DATE"],
        "minimum_education": ["JOB_INFO_EDUCATION", "MINIMUM_EDUCATION"],
        "major_field_of_study": ["JOB_INFO_MAJOR", "MAJOR_FIELD_OF_STUDY"],
        "required_experience": ["JOB_INFO_EXPERIENCE", "REQUIRED_EXPERIENCE"],
        "required_experience_months": [
            "JOB_INFO_EXPERIENCE_NUM_MONTHS", "REQUIRED_EXPERIENCE_MONTHS",
        ],
        "country_of_citizenship": [
            "COUNTRY_OF_CITIZENSHIP", "COUNTRY_OF_CITZENSHIP",
        ],
        "foreign_worker_birth_country": [
            "FW_INFO_BIRTH_COUNTRY", "FOREIGN_WORKER_BIRTH_COUNTRY",
        ],
        "class_of_admission": ["CLASS_OF_ADMISSION"],
        "foreign_worker_education": [
            "FOREIGN_WORKER_INFO_EDUCATION", "FOREIGN_WORKER_EDUCATION",
        ],
        "is_multiple_worksites": ["IS_MULTIPLE_LOCATIONS"],
    }),
    "h2a": dict(_SHARED, **{
        "certification_begin_date": ["CERTIFICATION_BEGIN_DATE"],
        "certification_end_date": ["CERTIFICATION_END_DATE"],
        "requested_begin_date": ["REQUESTED_BEGIN_DATE", "REQUESTED_START_DATE_OF_NEED"],
        "requested_end_date": ["REQUESTED_END_DATE", "REQUESTED_END_DATE_OF_NEED"],
        "type_of_employer_application": [
            "TYPE_OF_EMPLOYER_APPLICATION", "APPLICATION_TYPE", "ORGANIZATION_FLAG",
        ],
        "h2a_labor_contractor": ["H2A_LABOR_CONTRACTOR"],
        "nature_of_temporary_need": ["NATURE_OF_TEMPORARY_NEED"],
        "emergency_filing": ["EMERGENCY_FILING"],
        "primary_crop": ["PRIMARY_CROP"],
        "job_order_number": ["JOB_ORDER_NUMBER", "JOB_IDNUMBER"],
        "overtime_rate_from": ["OVERTIME_RATE_FROM"],
        "overtime_rate_to": ["OVERTIME_RATE_TO"],
        "workers_requested": [
            "NBR_WORKERS_REQUESTED", "TOTAL_WORKERS_H2A_REQUESTED",
            "TOTAL_WORKERS_NEEDED",
        ],
        "workers_certified": [
            "NBR_WORKERS_CERTIFIED", "TOTAL_WORKERS_H2A_CERTIFIED",
        ],
        "anticipated_number_of_hours": [
            "ANTICIPATED_NUMBER_OF_HOURS", "BASIC_NUMBER_OF_HOURS",
        ],
        "piece_rate_offer": ["PIECE_RATE_OFFER"],
        "piece_rate_unit": ["PIECE_RATE_UNIT"],
        "frequency_of_pay": ["FREQUENCY_OF_PAY"],
        "education_level": ["EDUCATION_LEVEL"],
        "work_experience_months": ["WORK_EXPERIENCE_MONTHS", "EMP_EXP_NUM_MONTHS"],
        "housing_city": ["HOUSING_CITY"],
        "housing_state": ["HOUSING_STATE"],
        "housing_type": ["TYPE_OF_HOUSING"],
        "housing_total_occupancy": ["TOTAL_OCCUPANCY"],
        "meals_provided": ["MEALS_PROVIDED"],
        "total_worksite_records": ["TOTAL_WORKSITE_RECORDS", "TOTAL_WORKSITES_RECORDS"],
    }),
    "h2b": dict(_SHARED, **{
        "application_type": ["APPLICATION_TYPE"],
        "certification_begin_date": ["CERTIFICATION_BEGIN_DATE"],
        "certification_end_date": ["CERTIFICATION_END_DATE"],
        "requested_begin_date": ["REQUESTED_BEGIN_DATE", "REQUESTED_START_DATE_OF_NEED"],
        "requested_end_date": ["REQUESTED_END_DATE", "REQUESTED_END_DATE_OF_NEED"],
        "nature_of_temporary_need": ["NATURE_OF_TEMPORARY_NEED"],
        "cap_exempt": ["CAP_EXEMPT"],
        "type_of_employer": ["TYPE_OF_EMPLOYER"],
        "job_order_number": ["JOB_IDNUMBER"],
        "job_order_submit_date": ["JOB_ORDER_SUBMIT_DATE"],
        "swa_state": ["SWA_STATE", "SWA_NAME"],
        "workers_requested": ["NBR_WORKERS_REQUESTED", "TOTAL_WORKERS_REQUESTED"],
        "workers_certified": ["NBR_WORKERS_CERTIFIED", "TOTAL_WORKERS_CERTIFIED"],
        "anticipated_number_of_hours": [
            "ANTICIPATED_NUMBER_OF_HOURS", "BASIC_NUMBER_OF_HOURS", "NUMBER_OF_HOURS",
        ],
        "overtime_rate_from": ["OVERTIME_RATE_FROM"],
        "overtime_rate_to": ["OVERTIME_RATE_TO"],
        "msa_name": ["MSA_NAME_OES_AREA_TITLE"],
        "education_level": ["EDUCATION_LEVEL"],
        "work_experience_months": ["WORK_EXPERIENCE_MONTHS", "EMP_EXP_NUM_MONTHS"],
    }),
}
