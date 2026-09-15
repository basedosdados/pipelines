"""Value -> label mappings for the coded columns, rendered to the dicionario table.

Only columns whose STORED values are codes appear here. Columns the cleaner
already decodes to readable labels -- agency_id, loan_type, action_taken,
population_classification -- are covered_by_dictionary = no and are absent,
per .claude/rules/data-basis-style.md.

Sources: the FFIEC CRA "File Specifications" PDF for the CRA code lists, the
Call Report form instructions for the filing types, and the MDRM itself for the
item type letters.
"""

from __future__ import annotations

# key -> (english label, temporal coverage). Coverage is blank when the code has
# meant the same thing for the whole series.
Entries = dict[str, str]

TRACT_INCOME_GROUP: Entries = {
    "001": "Under 10% of area median family income",
    "002": "10% to 20% of area median family income",
    "003": "20% to 30% of area median family income",
    "004": "30% to 40% of area median family income",
    "005": "40% to 50% of area median family income",
    "006": "50% to 60% of area median family income",
    "007": "60% to 70% of area median family income",
    "008": "70% to 80% of area median family income",
    "009": "80% to 90% of area median family income",
    "010": "90% to 100% of area median family income",
    "011": "100% to 110% of area median family income",
    "012": "110% to 120% of area median family income",
    "013": "Over 120% of area median family income",
    "014": "Median family income not known, reported as zero",
    "015": "Census tract not known, reported as NA",
    "101": "Low income, under 50% of area median family income excluding zero",
    "102": "Moderate income, 50% to 80% of area median family income",
    "103": "Middle income, 80% to 120% of area median family income",
    "104": "Upper income, over 120% of area median family income",
    "105": "Income not known, reported as zero",
    "106": "Census tract not known, reported as NA",
}

REPORT_LEVEL: Entries = {
    "004": "Total inside and outside the assessment area, across all states",
    "006": "Total inside the assessment area, across all states",
    "008": "Total outside the assessment area, across all states",
    "010": "State total",
    "020": "Total inside the assessment area in the state",
    "030": "Total outside the assessment area in the state",
    "040": "County total",
    "050": "Total inside the assessment area in the county",
    "060": "Total outside the assessment area in the county",
    # 140, 150 and 160 appear in the disclosure files from 1996 to 2003 and are
    # documented in NO FFIEC file specification -- not the 1996, 1997, 2003 or
    # 2024 disclosure spec, nor the aggregate spec, which uses a different
    # scheme entirely (200 = county total, 210 = MA total). Every row carrying
    # them also carries an MSA/MD code, and they sit exactly 100 above the
    # county triple, so the metropolitan reading is an inference from the data
    # and is labelled as such rather than asserted.
    "140": "Metropolitan area total (level undocumented in the FFIEC specification, 1996 to 2003 only)",
    "150": "Total inside the assessment area in the metropolitan area (level undocumented in the FFIEC specification, 1996 to 2003 only)",
    "160": "Total outside the assessment area in the metropolitan area (level undocumented in the FFIEC specification, 1996 to 2003 only)",
}

MEASURE_BAND: Entries = {
    "amount_lt_100k": "Loans with an origination amount under $100,000",
    "amount_100k_250k": "Loans with an origination amount of $100,000 to $250,000",
    "amount_250k_1m": (
        "Small business loans with an origination amount of $250,000 to $1,000,000"
    ),
    "amount_250k_500k": (
        "Small farm loans with an origination amount of $250,000 to $500,000"
    ),
    "revenue_lt_1m": (
        "Loans to businesses or farms with gross annual revenues under $1 million"
    ),
    "affiliate": "Loans reported as affiliate loans",
}

CALL_REPORT_FORM: Entries = {
    "031": ("FFIEC 031, filed by banks with domestic and foreign offices"),
    "041": ("FFIEC 041, filed by banks with domestic offices only"),
    "051": (
        "FFIEC 051, the abbreviated report for eligible small institutions with "
        "domestic offices only"
    ),
}

ITEM_TYPE: Entries = {
    "F": "Financial, submitted by the reporting institution",
    "D": "Derived from other stored variables",
    "R": "Rate, stored as a decimal value",
    "P": "Percentage, stored as a percentage value",
    "S": "Structure, describing the institution rather than a quantity",
    "E": "Examination or supervision data",
    "J": "Projected value with an associated projection period",
}

SCHEDULE: Entries = {
    "RC": "Schedule RC, balance sheet",
    "RCA": "Schedule RC-A, cash and balances due from depository institutions",
    "RCB": "Schedule RC-B, securities",
    "RCCI": "Schedule RC-C Part I, loans and leases",
    "RCCII": "Schedule RC-C Part II, loans to small businesses and small farms",
    "RCD": "Schedule RC-D, trading assets and liabilities",
    "RCE": "Schedule RC-E, deposit liabilities",
    "RCEI": "Schedule RC-E Part I, deposits in domestic offices",
    "RCEII": "Schedule RC-E Part II, deposits in foreign offices",
    "RCF": "Schedule RC-F, other assets",
    "RCG": "Schedule RC-G, other liabilities",
    "RCH": "Schedule RC-H, selected balance sheet items for domestic offices",
    "RCI": "Schedule RC-I, assets and liabilities of IBFs",
    "RCK": "Schedule RC-K, quarterly averages",
    "RCL": "Schedule RC-L, derivatives and off-balance sheet items",
    "RCM": "Schedule RC-M, memoranda",
    "RCN": "Schedule RC-N, past due and nonaccrual loans, leases and other assets",
    "RCO": "Schedule RC-O, other data for deposit insurance assessments",
    "RCP": "Schedule RC-P, 1-4 family residential mortgage banking activities",
    "RCQ": "Schedule RC-Q, assets and liabilities measured at fair value",
    "RCRI": "Schedule RC-R Part I, regulatory capital components and ratios",
    "RCRII": "Schedule RC-R Part II, risk-weighted assets",
    "RCS": "Schedule RC-S, servicing, securitisation and asset sale activities",
    "RCT": "Schedule RC-T, fiduciary and related services",
    "RCV": "Schedule RC-V, variable interest entities",
    "RI": "Schedule RI, income statement",
    "RIA": "Schedule RI-A, changes in bank equity capital",
    "RIBI": "Schedule RI-B Part I, charge-offs and recoveries on loans and leases",
    "RIBII": "Schedule RI-B Part II, changes in the allowance for credit losses",
    "RIC": "Schedule RI-C, disaggregated data on the allowance for credit losses",
    "RID": "Schedule RI-D, income from foreign offices",
    "RIE": "Schedule RI-E, explanations",
    "CI": "Contact information for the filing institution",
    "ENT": "Entity and filing metadata",
    "GI": "General information on the submission",
    "GL": "General ledger reconciliation",
    "NARR": "Narrative statement accompanying the report",
    "SU": "Supplemental information for FFIEC 051 filers",
}

FEDERAL_RESERVE_DISTRICT: Entries = {
    "1": "Boston",
    "2": "New York",
    "3": "Philadelphia",
    "4": "Cleveland",
    "5": "Richmond",
    "6": "Atlanta",
    "7": "Chicago",
    "8": "St. Louis",
    "9": "Minneapolis",
    "10": "Kansas City",
    "11": "Dallas",
    "12": "San Francisco",
    # Not one of the twelve districts; the filer left it unset
    "0": "Not assigned",
}

# column -> (table it belongs to, entries)
DICTIONARY: dict[tuple[str, str], Entries] = {
    ("cra_lending", "tract_income_group"): TRACT_INCOME_GROUP,
    ("cra_assessment_area_tract", "tract_income_group"): TRACT_INCOME_GROUP,
    ("cra_lending", "report_level"): REPORT_LEVEL,
    ("cra_lending", "measure_band"): MEASURE_BAND,
    ("institution", "call_report_form_id"): CALL_REPORT_FORM,
    ("mdrm_item", "item_type"): ITEM_TYPE,
    ("call_report_item", "schedule"): SCHEDULE,
    (
        "holding_company",
        "federal_reserve_district_id",
    ): FEDERAL_RESERVE_DISTRICT,
}


def rows() -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for (table, column), entries in DICTIONARY.items():
        for key, value in entries.items():
            out.append(
                {
                    "table_id": table,
                    "column_name": column,
                    "key": key,
                    "temporal_coverage": "",
                    "value": value,
                }
            )
    return out
