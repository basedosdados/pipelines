"""Column definitions for every us_ffiec_bank_reporting table.

This module is the single source of truth. build_architecture.py renders it to
the architecture CSVs, clean.py reads it for column order and parquet schema,
and build_dbt.py renders the dbt models and schema.yml from it. Nothing else
declares a column.

Conventions applied (see .claude/rules/data-basis-style.md):
  * the data is English, so the columns are English -- `year`, not `ano`, and
    identifiers take the `_id` SUFFIX, not the `id_` prefix
  * INT64/FLOAT64 only where arithmetic is meaningful, and every such column
    carries a measurement_unit; codes, flags and identifiers are STRING
  * descriptions never end in a full stop
"""

from __future__ import annotations

# A column is (name, bigquery_type, description, covered_by_dictionary,
#              directory_column, measurement_unit, observations, original_name)
Col = tuple[str, str, str, str, str, str, str, str]


def col(
    name: str,
    typ: str,
    desc: str,
    *,
    dic: str = "no",
    directory: str = "",
    unit: str = "",
    obs: str = "",
    original: str = "",
) -> Col:
    return (name, typ, desc, dic, directory, unit, obs, original)


DIR_COUNTY = "br_bd_diretorios_us.county:id_county"
DIR_STATE = "br_bd_diretorios_us.state:id_state"
DIR_YEAR = "br_bd_diretorios_data_tempo.ano:ano"

_RSSD_DESC = (
    "RSSD identifier assigned by the Federal Reserve, unique per institution and "
    "stable across name and charter changes; the join key across every table in "
    "this dataset. To reach us_fdic_bankfind, join through institution.fdic_cert_id, "
    "which carries the FDIC certificate"
)

# --------------------------------------------------------------------------

TABLES: dict[str, list[Col]] = {}

TABLES["institution"] = [
    col(
        "year",
        "INT64",
        "Year of the reporting quarter",
        directory=DIR_YEAR,
        unit="year",
    ),
    col(
        "quarter",
        "INT64",
        "Calendar quarter of the report, 1 to 4",
        unit="quarter",
    ),
    col("report_date", "DATE", "Last calendar day of the reporting quarter"),
    col("rssd_id", "STRING", _RSSD_DESC, original="IDRSSD"),
    col(
        "fdic_cert_id",
        "STRING",
        "FDIC certificate number, the institution key used by us_fdic_bankfind",
        obs="Reported as 0 when the institution has no FDIC certificate; converted to NULL.",
        original="FDIC Certificate Number",
    ),
    col(
        "occ_charter_id",
        "STRING",
        "OCC charter number for nationally chartered banks",
        obs="Reported as 0 when not OCC chartered; converted to NULL.",
        original="OCC Charter Number",
    ),
    col(
        "ots_docket_id",
        "STRING",
        "Former OTS docket number for thrift institutions",
        obs="Reported as 0 when absent; converted to NULL.",
        original="OTS Docket Number",
    ),
    col(
        "aba_routing_id",
        "STRING",
        "Primary ABA routing number",
        obs="Reported as 0 when absent; converted to NULL.",
        original="Primary ABA Routing Number",
    ),
    col(
        "name",
        "STRING",
        "Legal name of the financial institution",
        original="Financial Institution Name",
    ),
    col(
        "address",
        "STRING",
        "Street address of the institution's main office",
        original="Financial Institution Address",
    ),
    col(
        "city",
        "STRING",
        "City of the institution's main office",
        original="Financial Institution City",
    ),
    col(
        "state_abbreviation",
        "STRING",
        "Two-letter postal abbreviation of the state of the main office",
        obs="Not linked to the state directory: its primary key is the FIPS code, "
        "and the abbreviation is a non-key column.",
        original="Financial Institution State",
    ),
    col(
        "zip_code",
        "STRING",
        "ZIP code of the institution's main office",
        original="Financial Institution Zip Code",
    ),
    col(
        "call_report_form_id",
        "STRING",
        "Call Report form the institution filed for the quarter",
        dic="yes",
        original="Financial Institution Filing Type",
    ),
    col(
        "last_submission_updated_at",
        "DATETIME",
        "Timestamp at which the institution last updated its submission for the quarter",
        obs="Later than the report date, and moves when an amended filing is accepted.",
        original="Last Date/Time Submission Updated On",
    ),
]

TABLES["call_report_item"] = [
    col(
        "year",
        "INT64",
        "Year of the reporting quarter",
        directory=DIR_YEAR,
        unit="year",
    ),
    col(
        "quarter",
        "INT64",
        "Calendar quarter of the report, 1 to 4",
        unit="quarter",
    ),
    col("report_date", "DATE", "Last calendar day of the reporting quarter"),
    col("rssd_id", "STRING", _RSSD_DESC, original="IDRSSD"),
    col(
        "schedule",
        "STRING",
        "Call Report schedule the item was filed on, such as RC for the balance "
        "sheet or RI for the income statement",
        dic="yes",
    ),
    col(
        "item_code",
        "STRING",
        "Eight-character MDRM identifier: a four-character mnemonic giving the "
        "report and consolidation basis, then the four-character item number; "
        "joins to mdrm_item for the item's name, type and unit",
    ),
    col(
        "value",
        "FLOAT64",
        "Value the institution reported for the item",
        obs="The unit varies by item and is given by mdrm_item.measurement_unit for "
        "the item_code. Dollar items are filed in thousands and are multiplied "
        "by 1,000 here, so they are plain USD; counts, percentages and ratios "
        "are carried as filed. Items whose measurement_unit is blank are yes/no "
        "answers or dates reported as numbers, not quantities.",
    ),
]

TABLES["holding_company"] = [
    col(
        "year",
        "INT64",
        "Year of the reporting quarter",
        directory=DIR_YEAR,
        unit="year",
    ),
    col(
        "quarter",
        "INT64",
        "Calendar quarter of the report, 1 to 4",
        unit="quarter",
    ),
    col(
        "report_date",
        "DATE",
        "Last calendar day of the reporting quarter",
        original="RSSD9999",
    ),
    col("rssd_id", "STRING", _RSSD_DESC, original="RSSD9001"),
    col(
        "name",
        "STRING",
        "Legal name of the holding company",
        original="RSSD9017",
    ),
    col(
        "short_name",
        "STRING",
        "Abbreviated name of the holding company",
        original="RSSD9010",
    ),
    col(
        "address",
        "STRING",
        "Street address of the holding company's physical location",
        original="RSSD9028",
    ),
    col(
        "city",
        "STRING",
        "City of the holding company's physical location",
        original="RSSD9130",
    ),
    col(
        "state_abbreviation",
        "STRING",
        "Two-letter postal abbreviation of the state of the physical location",
        obs="Not linked to the state directory: its primary key is the FIPS code.",
        original="RSSD9200",
    ),
    col(
        "zip_code",
        "STRING",
        "ZIP code of the holding company's physical location",
        original="RSSD9220",
    ),
    col(
        "county_id",
        "STRING",
        "Five-digit FIPS code of the county of the physical location",
        directory=DIR_COUNTY,
        obs="Built from the state FIPS code (RSSD9210) and the county code (RSSD9150), "
        "both of which the filer reports separately.",
        original="RSSD9210 + RSSD9150",
    ),
    # RSSD9050 (FDIC certificate), RSSD9055 (OCC charter) and RSSD9375 (head
    # office RSSD) are present in the BHCF header but the source writes a
    # literal 0 in all three for every filer: 9, 1 and 0 populated rows
    # respectively out of 55,565 sampled across 1986-2026. They are dropped
    # rather than published as permanently NULL columns.
    col(
        "tax_id",
        "STRING",
        "Employer identification number of the entity",
        original="RSSD6191",
    ),
    col(
        "charter_type_id",
        "STRING",
        "Charter type code assigned by the Federal Reserve",
        obs="The code list is published by the National Information Center rather than "
        "with the data, so the values are carried as filed and are not decoded here.",
        original="RSSD9048",
    ),
    col(
        "organization_type_id",
        "STRING",
        "Organization type code assigned by the Federal Reserve",
        obs="The code list is published by the National Information Center rather than "
        "with the data, so the values are carried as filed and are not decoded here.",
        original="RSSD9047",
    ),
    col(
        "primary_activity_id",
        "STRING",
        "NAICS code of the entity's primary activity, 551111 for a bank holding company",
        obs="Not linked to the NAICS directory: the series spans four decades and "
        "several NAICS vintages, which the source does not restate.",
        original="RSSD9132",
    ),
    col(
        "federal_reserve_district_id",
        "STRING",
        "Federal Reserve district that supervises the entity, 1 to 12",
        dic="yes",
        original="RSSD9032",
    ),
    col(
        "bank_count",
        "INT64",
        "Number of banks the holding company controls",
        unit="unit",
        original="RSSD9146",
    ),
    col(
        "is_financial_holding_company",
        "STRING",
        "Whether the entity has elected financial holding company status, 1 for yes",
        original="RSSD9016",
    ),
    col(
        "is_savings_loan_holding_company",
        "STRING",
        "Whether the entity is a savings and loan holding company, 1 for yes",
        original="RSSD9198",
    ),
]

TABLES["holding_company_item"] = [
    col(
        "year",
        "INT64",
        "Year of the reporting quarter",
        directory=DIR_YEAR,
        unit="year",
    ),
    col(
        "quarter",
        "INT64",
        "Calendar quarter of the report, 1 to 4",
        unit="quarter",
    ),
    col("report_date", "DATE", "Last calendar day of the reporting quarter"),
    col("rssd_id", "STRING", _RSSD_DESC, original="RSSD9001"),
    col(
        "item_code",
        "STRING",
        "Eight-character MDRM identifier of the FR Y-9C item; joins to mdrm_item "
        "for the item's name, type and unit",
    ),
    col(
        "value",
        "FLOAT64",
        "Value the holding company reported for the item",
        obs="The unit varies by item and is given by mdrm_item.measurement_unit for "
        "the item_code. Dollar items are filed in thousands and are multiplied "
        "by 1,000 here, so they are plain USD.",
    ),
]

TABLES["mdrm_item"] = [
    col(
        "item_code",
        "STRING",
        "Eight-character MDRM identifier, the concatenation of mnemonic and item number",
    ),
    col(
        "mnemonic",
        "STRING",
        "First four characters of the identifier, naming the report series and the "
        "consolidation basis, such as RCON for domestic offices or RCFD for "
        "consolidated domestic and foreign offices",
    ),
    col(
        "item_number",
        "STRING",
        "Last four characters of the identifier, naming the item itself; the same "
        "number carries the same meaning across reporting forms",
    ),
    col(
        "name", "STRING", "Item name as published in the MDRM data dictionary"
    ),
    col(
        "description",
        "STRING",
        "Full item definition as published in the MDRM",
    ),
    col(
        "item_type",
        "STRING",
        "MDRM item type: F financial, D derived, R rate, P percentage, S structure, "
        "E examination, J projected",
        dic="yes",
    ),
    col(
        "measurement_unit",
        "STRING",
        "Unit of the values carried for this item in call_report_item and "
        "holding_company_item",
        obs="Derived here, not published by the MDRM, which does not separate dollar "
        "amounts from counts. USD, percent, ratio, unit, or blank when the item "
        "is a yes/no answer, an indicator or a date rather than a quantity.",
    ),
    col(
        "is_flag",
        "STRING",
        "Whether the item is a yes/no answer or an indicator code rather than a "
        "magnitude, 1 for yes",
    ),
    col(
        "is_confidential",
        "STRING",
        "Whether the MDRM marks the item as confidential and therefore not publicly "
        "disclosed, 1 for yes",
    ),
    col(
        "reporting_form",
        "STRING",
        "Reporting forms that collect the item, separated by semicolons",
    ),
    col("start_date", "DATE", "First date on which the item was collected"),
    col(
        "end_date",
        "DATE",
        "Last date on which the item was collected",
        obs="9999-12-31 marks an item that is still collected.",
    ),
    col("series_glossary", "STRING", "MDRM glossary entry for the mnemonic"),
]

_CRA_RESPONDENT = (
    "Respondent identifier assigned by the institution's supervisory agency; "
    "unique only together with agency and year, and not an RSSD"
)
_CRA_AGENCY = (
    "Supervisory agency that assigned the respondent identifier: occ, frs, fdic, "
    "or ots for the Office of Thrift Supervision, which was abolished in 2011 and "
    "appears only up to 2010"
)
_CRA_RSSD = (
    "RSSD identifier of the reporting institution, taken from the CRA transmittal "
    "sheet for the same respondent, agency and year; the join key to institution, "
    "call_report_item and holding_company"
)
_TRACT_INCOME = (
    "Income group of the census tracts the lending is grouped into, as a share of "
    "area median family income"
)
_REPORT_LEVEL = (
    "Geographic and assessment-area level the row totals over, from a single county "
    "up to a nationwide total"
)

TABLES["cra_lending"] = [
    col(
        "year",
        "INT64",
        "Year the lending activity was reported for",
        directory=DIR_YEAR,
        unit="year",
        original="Activity Year",
    ),
    col("respondent_id", "STRING", _CRA_RESPONDENT, original="Respondent ID"),
    col("agency_id", "STRING", _CRA_AGENCY, original="Agency Code"),
    col("rssd_id", "STRING", _CRA_RSSD),
    col(
        "loan_type",
        "STRING",
        "Whether the row covers small business or small farm lending",
        original="Loan Type",
    ),
    col(
        "action_taken",
        "STRING",
        "Whether the loans were originated by the reporting institution or purchased "
        "from another lender",
        original="Action Taken Type",
    ),
    col(
        "state_id",
        "STRING",
        "Two-digit state FIPS code",
        directory=DIR_STATE,
        obs="Blank on rows that total across all states.",
        original="State",
    ),
    col(
        "county_id",
        "STRING",
        "Five-digit county FIPS code, the state code followed by the county code",
        directory=DIR_COUNTY,
        obs="Blank on rows that total across counties. County FIPS codes change over "
        "time and are not restated by the source, so early years carry the codes "
        "in force at the time.",
        original="State + County",
    ),
    col(
        "msa_md_id",
        "STRING",
        "Metropolitan statistical area or metropolitan division code as defined by OMB",
        obs="NA outside a metropolitan area, blank on rows that total across areas.",
        original="MSA/MD",
    ),
    col(
        "assessment_area_id",
        "STRING",
        "Assessment area number the institution assigned to the area",
        obs="NA outside any assessment area, blank on totals.",
        original="Assessment Area Number",
    ),
    col(
        "is_partial_county",
        "STRING",
        "Whether the assessment area covers only part of the county, 1 for yes",
        original="Partial County Indicator",
    ),
    col(
        "is_split_county",
        "STRING",
        "Whether the county is split across more than one assessment area, 1 for yes",
        original="Split County Indicator",
    ),
    col(
        "population_classification",
        "STRING",
        "Whether the county has fewer or more than 500,000 residents",
        original="Population Classification",
    ),
    col(
        "tract_income_group",
        "STRING",
        _TRACT_INCOME,
        dic="yes",
        original="Income Group Total",
    ),
    col(
        "report_level",
        "STRING",
        _REPORT_LEVEL,
        dic="yes",
        original="Report Level",
    ),
    col(
        "measure_band",
        "STRING",
        "Loan size or borrower revenue band the count and amount refer to",
        dic="yes",
    ),
    col("loan_count", "INT64", "Number of loans in the band", unit="unit"),
    col(
        "loan_amount",
        "FLOAT64",
        "Total amount of the loans in the band",
        unit="USD",
        obs="Filed in thousands of dollars and multiplied by 1,000 here.",
    ),
]

TABLES["cra_assessment_area_tract"] = [
    col(
        "year",
        "INT64",
        "Year the assessment area was reported for",
        directory=DIR_YEAR,
        unit="year",
        original="Activity Year",
    ),
    col("respondent_id", "STRING", _CRA_RESPONDENT, original="Respondent ID"),
    col("agency_id", "STRING", _CRA_AGENCY, original="Agency Code"),
    col("rssd_id", "STRING", _CRA_RSSD),
    col(
        "state_id",
        "STRING",
        "Two-digit state FIPS code",
        directory=DIR_STATE,
        original="State",
    ),
    col(
        "county_id",
        "STRING",
        "Five-digit county FIPS code, the state code followed by the county code",
        directory=DIR_COUNTY,
        original="State + County",
    ),
    col(
        "msa_md_id",
        "STRING",
        "Metropolitan statistical area or metropolitan division code as defined by OMB",
        obs="NA outside a metropolitan area.",
        original="MSA/MD",
    ),
    col(
        "census_tract_id",
        "STRING",
        "Eleven-digit census tract identifier, the county FIPS code followed by the "
        "six-digit tract number",
        obs="Not linked to the census tract directory: the file spans four decennial "
        "tract vintages (1990, 2000, 2010 and 2020 boundaries in different years) "
        "and the source does not restate earlier years onto current boundaries, so "
        "a single-vintage foreign key would be wrong for most of the series.",
        original="Census Tract",
    ),
    col(
        "assessment_area_id",
        "STRING",
        "Assessment area number the institution assigned to the area",
        obs="NA for tracts outside any assessment area.",
        original="Assessment Area Number",
    ),
    col(
        "is_partial_county",
        "STRING",
        "Whether the assessment area covers only part of the county, 1 for yes",
        original="Partial County Indicator",
    ),
    col(
        "is_split_county",
        "STRING",
        "Whether the county is split across more than one assessment area, 1 for yes",
        original="Split County Indicator",
    ),
    col(
        "population_classification",
        "STRING",
        "Whether the county has fewer or more than 500,000 residents",
        original="Population Classification",
    ),
    col(
        "tract_income_group",
        "STRING",
        _TRACT_INCOME,
        dic="yes",
        original="Income Group",
    ),
]

TABLES["cra_respondent"] = [
    col(
        "year",
        "INT64",
        "Year the institution reported CRA data for",
        directory=DIR_YEAR,
        unit="year",
        original="Activity Year",
    ),
    col("respondent_id", "STRING", _CRA_RESPONDENT, original="Respondent ID"),
    col("agency_id", "STRING", _CRA_AGENCY, original="Agency Code"),
    col("rssd_id", "STRING", _CRA_RSSD, original="ID_RSSD"),
    col(
        "name",
        "STRING",
        "Name of the reporting institution",
        original="Respondent Name",
    ),
    col(
        "address",
        "STRING",
        "Street address of the reporting institution",
        original="Respondent Address",
    ),
    col(
        "city",
        "STRING",
        "City of the reporting institution",
        original="Respondent City",
    ),
    col(
        "state_abbreviation",
        "STRING",
        "Two-letter postal abbreviation of the institution's state",
        obs="Not linked to the state directory: its primary key is the FIPS code.",
        original="Respondent State",
    ),
    col(
        "zip_code",
        "STRING",
        "ZIP code of the reporting institution",
        original="Respondent Zip Code",
    ),
    col(
        "tax_id",
        "STRING",
        "Employer identification number of the institution",
        original="Tax ID",
    ),
    col(
        "total_assets",
        "FLOAT64",
        "Total assets reported on the prior year-end Call Report",
        unit="USD",
        obs="Filed in thousands of dollars and multiplied by 1,000 here.",
        original="Assets",
    ),
]

# English dataset, so the dictionary table and its columns are English --
# `dictionary` with table_id/column_name/key/temporal_coverage/value, matching
# us_dol_oflc. The `custom_dictionary_coverage_eng` generic test reads `key`
# and `value` by name, so the Portuguese `dicionario` schema errors with
# "Unrecognized name: value; Did you mean valor?".
TABLES["dictionary"] = [
    col("table_id", "STRING", "Name of the table the coded column belongs to"),
    col("column_name", "STRING", "Name of the coded column"),
    col("key", "STRING", "Value as stored in the column"),
    col(
        "temporal_coverage",
        "STRING",
        "Years the key applies to, blank when it applies to the table's whole span",
    ),
    col("value", "STRING", "Meaning of the key"),
]

ARCH_HEADER = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
]

PARTITIONED = {
    "institution": ["year"],
    "call_report_item": ["year"],
    "holding_company": ["year"],
    "holding_company_item": ["year"],
    "cra_lending": ["year"],
    "cra_assessment_area_tract": ["year"],
    "cra_respondent": ["year"],
}


def columns(table: str) -> list[str]:
    return [c[0] for c in TABLES[table]]


def types(table: str) -> dict[str, str]:
    return {c[0]: c[1] for c in TABLES[table]}
