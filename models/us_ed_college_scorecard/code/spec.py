"""
Single source of truth for the us_ed_college_scorecard onboarding.

Defines the 11 Data Basis tables, how the 3,308 institution-level and 178
field-of-study source columns are routed into them, and the raw -> BD column
name map for the two wide tables.

Design note (why long tables): the source publishes institution measures as
~3,300 wide columns that are mechanically generated cross-tabs of
measure x subgroup x horizon. Held wide, the table is unusable and the
measures cannot be documented individually. Normalised by domain, the whole
institution panel is 122.7M non-null cells across 7 long tables, and NO
source column is dropped. Variable definitions live in the `variable` table.

Two source quirks that this spec exists to handle:

  1. 504 BBRR* repayment columns publish interval bands ('0.30-0.39',
     '<=0.10') rather than numbers, and 2 columns publish MM/DD/YYYY dates.
     A plain safe_cast(... as float64) turns all of these into NULL silently.
     Long tables therefore carry `value` (FLOAT64) AND `value_raw` (STRING).

  2. 'PrivacySuppressed' means the cell exists but is withheld; an empty cell
     means it was never collected. A wide table cannot tell them apart. In
     the long tables a suppressed cell is emitted as a row with value NULL
     and value_raw = 'PrivacySuppressed'; a missing cell produces no row.
"""

# ---------------------------------------------------------------- constants

# Sentinels the source itself declares in data.yaml (`null_value`).
# 'PrivacySuppressed' / 'PS' are kept as value_raw in long tables (see above).
NULL_TOKENS = ("NULL", "NA", "")
SUPPRESSED_TOKENS = ("PrivacySuppressed", "PS")

# API namespaces routed to the wide `institution` table. Everything else in
# the institution file goes to the long table named by its namespace.
WIDE_NAMESPACES = frozenset(
    {
        "id",
        "ope8_id",
        "ope6_id",
        "fed_sch_cd",
        "school",
        "location",
        "admissions",
    }
)

# Two enrollment measures promoted out of the `student` namespace into the
# wide table: they are the most-used filter in the whole dataset and an
# `institution` table that cannot report institution size is a poor product.
# This is the ONLY exception to the namespace rule, and it is logged as such.
PROMOTED_TO_WIDE = ("UGDS", "UG")

# API namespace -> long table slug
LONG_TABLES = {
    "academics": "academics",
    "student": "student_body",
    "cost": "cost",
    "aid": "aid_debt",
    "completion": "completion",
    "repayment": "repayment",
    "earnings": "earnings",
}

LONG_SCHEMA = ["year", "unitid", "variable_name", "value", "value_raw"]

TABLE_SLUGS = [
    "institution",
    *sorted(LONG_TABLES.values()),
    "field_of_study",
    "variable",
    "dicionario",
]

# ------------------------------------------------- wide `institution` names
# Order follows the Data Basis rule: partition column, then identifiers,
# then descriptive columns.

INSTITUTION_COLUMNS = [
    # partition
    ("year", None),
    # identifiers
    ("unitid", "UNITID"),
    ("opeid8", "OPEID"),
    ("opeid6", "OPEID6"),
    ("federal_school_code", "FEDSCHCD"),
    # identity
    ("institution_name", "INSTNM"),
    ("institution_alias", "ALIAS"),
    ("institution_url", "INSTURL"),
    ("net_price_calculator_url", "NPCURL"),
    # geography
    ("address", "ADDR"),
    ("city", "CITY"),
    ("state_abbreviation", "STABBR"),
    ("zip_code", "ZIP"),
    ("state_fips", "ST_FIPS"),
    ("region", "REGION"),
    ("locale", "LOCALE"),
    ("locale_degree_urbanization", "LOCALE2"),
    ("latitude", "LATITUDE"),
    ("longitude", "LONGITUDE"),
    # institutional characteristics
    ("control", "CONTROL"),
    ("control_peps", "CONTROL_PEPS"),
    ("ownership_peps", "SCHTYPE"),
    ("scorecard_sector", "SCORECARD_SECTOR"),
    ("institution_level", "ICLEVEL"),
    ("predominant_degree", "PREDDEG"),
    ("predominant_degree_recoded", "SCH_DEG"),
    ("highest_degree", "HIGHDEG"),
    ("main_campus", "MAIN"),
    ("branch_campuses", "NUMBRANCH"),
    ("carnegie_basic", "CCBASIC"),
    ("carnegie_undergraduate_profile", "CCUGPROF"),
    ("carnegie_size_setting", "CCSIZSET"),
    ("online_only", "DISTANCEONLY"),
    ("currently_operating", "CURROPER"),
    ("open_admissions_policy", "OPENADMP"),
    ("religious_affiliation", "RELAFFIL"),
    ("men_only", "MENONLY"),
    ("women_only", "WOMENONLY"),
    # minority-serving designations
    ("historically_black", "HBCU"),
    ("predominantly_black", "PBI"),
    ("alaska_native_hawaiian_serving", "ANNHI"),
    ("tribal_college", "TRIBAL"),
    ("asian_pacific_islander_serving", "AANAPII"),
    ("hispanic_serving", "HSI"),
    ("native_american_non_tribal", "NANTI"),
    # accreditation and federal aid eligibility
    ("accreditor_name", "ACCREDAGENCY"),
    ("accreditor_code", "ACCREDCODE"),
    ("title_iv_eligibility_type", "OPEFLAG"),
    ("title_iv_approval_date", "T4APPROVALDATE"),
    ("heightened_cash_monitoring", "HCM2"),
    ("dol_provider", "DOLPROVIDER"),
    # enrollment (promoted, see PROMOTED_TO_WIDE)
    ("undergraduate_enrollment", "UGDS"),
    ("undergraduate_enrollment_all", "UG"),
    # institutional finance
    ("tuition_revenue_per_fte", "TUITFTE"),
    ("instructional_expenditure_per_fte", "INEXPFTE"),
    ("average_faculty_salary", "AVGFACSAL"),
    ("full_time_faculty_rate", "PFTFAC"),
    ("endowment_begin", "ENDOWBEGIN"),
    ("endowment_end", "ENDOWEND"),
    # admissions
    ("admission_rate", "ADM_RATE"),
    ("admission_rate_all_campuses", "ADM_RATE_ALL"),
    ("admission_rate_suppressed", "ADM_RATE_SUPP"),
    ("test_score_requirement", "ADMCON7"),
    ("sat_average", "SAT_AVG"),
    ("sat_average_all_campuses", "SAT_AVG_ALL"),
    ("sat_reading_p25", "SATVR25"),
    ("sat_reading_p50", "SATVR50"),
    ("sat_reading_p75", "SATVR75"),
    ("sat_reading_midpoint", "SATVRMID"),
    ("sat_math_p25", "SATMT25"),
    ("sat_math_p50", "SATMT50"),
    ("sat_math_p75", "SATMT75"),
    ("sat_math_midpoint", "SATMTMID"),
    ("sat_writing_p25", "SATWR25"),
    ("sat_writing_p75", "SATWR75"),
    ("sat_writing_midpoint", "SATWRMID"),
    ("act_composite_p25", "ACTCM25"),
    ("act_composite_p50", "ACTCM50"),
    ("act_composite_p75", "ACTCM75"),
    ("act_composite_midpoint", "ACTCMMID"),
    ("act_english_p25", "ACTEN25"),
    ("act_english_p50", "ACTEN50"),
    ("act_english_p75", "ACTEN75"),
    ("act_english_midpoint", "ACTENMID"),
    ("act_math_p25", "ACTMT25"),
    ("act_math_p50", "ACTMT50"),
    ("act_math_p75", "ACTMT75"),
    ("act_math_midpoint", "ACTMTMID"),
    ("act_writing_p25", "ACTWR25"),
    ("act_writing_p75", "ACTWR75"),
    ("act_writing_midpoint", "ACTWRMID"),
]

# ---------------------------------------------- wide `field_of_study` names
# The 178 field-of-study columns keep their published names, lowercased: they
# are the names used by the technical documentation, the API and the
# `rscorecard` package, so lowercasing keeps the join to the source obvious.
# Only the key and label columns are renamed for readability.

FIELD_OF_STUDY_RENAMES = {
    "UNITID": "unitid",
    "OPEID6": "opeid6",
    "INSTNM": "institution_name",
    "CONTROL": "control",
    "MAIN": "main_campus",
    "CIPCODE": "cip_code",
    "CIPDESC": "cip_description",
    "CREDLEV": "credential_level",
    "CREDDESC": "credential_description",
    "DISTANCE": "distance_education",
}

FIELD_OF_STUDY_KEY = ["year", "unitid", "cip_code", "credential_level"]


def bd_name_field_of_study(raw: str) -> str:
    return FIELD_OF_STUDY_RENAMES.get(raw.upper(), raw.lower())
