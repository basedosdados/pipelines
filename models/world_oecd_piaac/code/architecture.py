"""Column definitions for every world_oecd_piaac table.

One module defines the schema, and the architecture CSVs, the cleaning transform
and the dbt models are all generated from it, so they cannot drift apart.

Two shape decisions are encoded here:

1. Each cycle splits into a wide `respondent_*` table and a long
   `item_response_*` table. Two-thirds of the PUF columns are per-item measures
   (Cycle 2: 1,699 of 2,483) that only psychometricians use, and a 2,483-column
   table is unusable on the site.

2. The item table carries `scored_response_label` next to the raw code. PIAAC's
   scoring codes are item-specific -- code 1 is "Full Credit" for 299 Cycle 2
   items but "Partial Credit" for 6 others, and code 11 means four different
   things -- so a Data Basis dictionary keyed on (table_id, column_name, key)
   cannot describe a long `scored_response` column without lying about 40 items.
   Decoding each item with its own scheme at cleaning time is lossless and
   self-describing; the raw code is kept alongside.
"""

from __future__ import annotations

from dataclasses import dataclass, field

DATASET_ID = "world_oecd_piaac"
PAIS = "br_bd_diretorios_mundo.pais"


@dataclass
class Column:
    name: str
    bigquery_type: str
    description: str
    covered_by_dictionary: str = "no"
    directory_column: str = ""
    measurement_unit: str = ""
    has_sensitive_data: str = "no"
    observations: str = ""
    original_name: str = ""
    temporal_coverage: str = ""


@dataclass
class Table:
    slug: str
    name_en: str
    description_en: str
    columns: list[Column] = field(default_factory=list)


# Present on every microdata table, in this order: partition first, then the rest
# of the grain, then the payload. PIAAC ships none of these as such -- year, cycle
# and round are derived from which file a row came from, and the country columns
# are split out of CNTRYID / CNTRYID_E.
def grain_columns(cycle: str) -> list[Column]:
    return [
        Column(
            "year",
            "INT64",
            "Reference year of data collection, derived from the survey cycle and round",
            measurement_unit="year",
            observations=(
                "PIAAC carries no year-of-collection variable. Cycle 1 Round 1 was "
                "collected Aug 2011-Mar 2012 (2012), Round 2 Apr 2014-Mar 2015 "
                "(2015), Round 3 Jul-Dec 2017 (2017); Cycle 2 Round 1 was collected "
                "Sep 2022-Aug 2023 (2023)"
            ),
        ),
        Column(
            "cycle",
            "STRING",
            "Survey cycle of the Survey of Adult Skills (1 or 2)",
        ),
        Column(
            "round",
            "STRING",
            "Round of data collection within the cycle",
            observations="Cycle 1 ran three rounds; Cycle 2 has run one so far",
        ),
        Column(
            "country_id_iso_3",
            "STRING",
            "ISO 3166-1 alpha-3 code of the participating country",
            directory_column=f"{PAIS}:sigla_iso3",
        ),
        Column(
            "country_id_m49",
            "STRING",
            "UN M49 numeric code of the participating country",
            directory_column=f"{PAIS}:id_m49",
            original_name="CNTRYID",
        ),
        Column(
            "country_entity_id",
            "STRING",
            "Code of the participating country or sub-national entity as reported by PIAAC",
            original_name="CNTRYID_E",
            observations=(
                "Distinct from country_id_m49 where a country took part as a "
                "sub-national entity: Belgium participated as Flanders in both "
                "cycles and the United Kingdom as England in Cycle 2. Not a country "
                "code, so it carries no directory link"
            ),
        ),
        Column(
            "respondent_id",
            "STRING",
            "Randomly derived sequential identifier of the respondent",
            original_name="SEQID",
            observations=(
                "Unique within a country and cycle only, never across them; "
                "combine with year and country_id_iso_3 for a unique key"
            ),
        ),
    ]


ITEM_COLUMNS: list[Column] = [
    Column(
        "item_code",
        "STRING",
        "Code of the assessment item, shared by every measure recorded for it",
        observations=(
            "The PIAAC variable name without its measure suffix, e.g. C301C05 for "
            "variables C301C05S, C301C05TT, C301C05A"
        ),
    ),
    Column(
        "domain",
        "STRING",
        "Assessment domain the item belongs to",
        observations=(
            "Literacy, numeracy, problem solving, reading or numeracy components, "
            "plus the tutorial and effort-question blocks, which share the same "
            "per-item measure structure"
        ),
    ),
    Column(
        "scored_response",
        "STRING",
        "Scored response code as recorded by PIAAC",
        original_name="<item>S",
        observations=(
            "Codes are item-specific: 1 is Full Credit for most items but Partial "
            "Credit for some, and 11 has four distinct meanings across items. Read "
            "scored_response_label rather than interpreting the code directly"
        ),
    ),
    Column(
        "scored_response_label",
        "STRING",
        "Meaning of scored_response, decoded with the item's own scoring scheme",
    ),
    Column(
        "raw_response",
        "STRING",
        "Response as entered by the respondent, for items that record one",
        original_name="<item>R",
        observations=(
            "Recorded for reading and numeracy component items. For numeric-entry "
            "items this is the answer itself, so it carries no decoded label"
        ),
    ),
    Column(
        "timing_seconds",
        "FLOAT64",
        "Total time the respondent spent on the item",
        measurement_unit="second",
        original_name="<item>T / <item>TT",
    ),
    Column(
        "timing_first_action_seconds",
        "FLOAT64",
        "Time elapsed before the respondent's first action on the item",
        measurement_unit="second",
        original_name="<item>F",
    ),
    Column(
        "n_actions",
        "INT64",
        "Number of actions the respondent took on the item",
        measurement_unit="action",
        original_name="<item>A",
    ),
    Column(
        "n_visits",
        "INT64",
        "Number of times the respondent visited the item",
        measurement_unit="visit",
        original_name="<item>V",
    ),
    Column(
        "n_short_visits",
        "INT64",
        "Number of visits to the item shorter than the PIAAC minimum duration",
        measurement_unit="visit",
        original_name="<item>VS",
    ),
]

VARIABLE_COLUMNS: list[Column] = [
    Column("cycle", "STRING", "Survey cycle the variable belongs to (1 or 2)"),
    Column("variable_name", "STRING", "PIAAC variable name, lowercased"),
    Column(
        "table_id", "STRING", "Data Basis table the variable was loaded into"
    ),
    Column(
        "column_name",
        "STRING",
        "Column name the variable maps to in that table",
    ),
    Column(
        "label",
        "STRING",
        "Variable label as published in the international codebook",
    ),
    Column("domain", "STRING", "Thematic domain assigned by the codebook"),
    Column("level", "STRING", "Measurement level assigned by the codebook"),
    Column(
        "bigquery_type",
        "STRING",
        "BigQuery type the variable was loaded as",
        observations=(
            "Assigned from an explicit unit rule, not from the codebook's level "
            "field, which disagrees across cycles"
        ),
    ),
    Column(
        "measurement_unit",
        "STRING",
        "Measurement unit, empty where the variable is not a quantity",
    ),
    Column(
        "item_code",
        "STRING",
        "Item the variable measures, for item-level variables only",
    ),
    Column(
        "measure",
        "STRING",
        "Which per-item measure it records, for item-level variables only",
    ),
]

DICTIONARY_COLUMNS: list[Column] = [
    Column("table_id", "STRING", "Table the covered column belongs to"),
    Column(
        "column_name", "STRING", "Name of the column covered by the dictionary"
    ),
    Column("key", "STRING", "Coded value stored in the data table"),
    Column("temporal_coverage", "STRING", "Years the mapping applies to"),
    Column("value", "STRING", "Meaning of the coded value"),
]

TABLE_DESCRIPTIONS = {
    "respondent_cycle_1": (
        "One row per respondent to Cycle 1 of the Survey of Adult Skills (PIAAC), "
        "covering the background questionnaire, derived variables, the ten "
        "plausible values for each proficiency domain, and the final and replicate "
        "sampling weights. Thirty-five internationally comparable Public Use Files "
        "from three rounds of data collection (2011-2012, 2014-2015 and 2017). "
        "Item-level assessment responses are in item_response_cycle_1."
    ),
    "respondent_cycle_2": (
        "One row per respondent to Cycle 2 of the Survey of Adult Skills (PIAAC), "
        "covering the background questionnaire, derived variables, the ten "
        "plausible values for each proficiency domain, and the final and replicate "
        "sampling weights. Thirty Public Use Files collected between September 2022 "
        "and August 2023. Item-level assessment responses are in "
        "item_response_cycle_2."
    ),
    "respondent_cycle_1_usa_national": (
        "One row per respondent to the United States Round 3 (2017) national Public "
        "Use File of the Survey of Adult Skills (PIAAC). The OECD publishes no "
        "internationally comparable file for this round in the United States. The "
        "national file adds 131 United States-specific variables and suppresses 110 "
        "international ones, including exact age, so it is held separately rather "
        "than stacked into respondent_cycle_1."
    ),
    "item_response_cycle_1": (
        "One row per respondent and assessment item in Cycle 1 of the Survey of "
        "Adult Skills (PIAAC), recording the scored response and the response-process "
        "measures captured by the assessment platform. Respondents see only a subset "
        "of items, so a row exists only where at least one measure was recorded."
    ),
    "item_response_cycle_2": (
        "One row per respondent and assessment item in Cycle 2 of the Survey of "
        "Adult Skills (PIAAC), recording the scored response and the response-process "
        "measures captured by the assessment platform. Respondents see only a subset "
        "of items, so a row exists only where at least one measure was recorded."
    ),
    "variable": (
        "One row per variable in the PIAAC international codebooks, mapping each "
        "published variable name to the Data Basis table and column it was loaded "
        "into, with its label, domain and assigned type. Use it to find a variable "
        "by name when following published PIAAC documentation or analysis code."
    ),
    "dictionary": (
        "Mapping from coded values to their meanings for every dictionary-covered "
        "column in the dataset. PIAAC publishes value labels in English only."
    ),
}
