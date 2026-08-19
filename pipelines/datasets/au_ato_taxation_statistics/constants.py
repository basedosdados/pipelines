"""Constants for the au_ato_taxation_statistics dataset."""

from enum import Enum


class constants(Enum):
    """Dataset-level constants for ATO Taxation Statistics."""

    CKAN_API = "https://data.gov.au/data/api/3/action/package_search"
    CKAN_PACKAGE = "https://data.gov.au/data/api/3/action/package_show"
    CKAN_ORG = "australiantaxationoffice"

    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    )

    # Resource selectors, anchored on BOTH the table number and its
    # descriptive slug. Anchoring on the number alone is unsafe: the ATO
    # reused "gst4" for petroleum resource rent tax before 2014-15 and for
    # the by-industry table afterwards.
    TABLE_SELECTORS = {
        "individuals_income_state": r"individual04(sex|gender)",
        "individuals_industry": r"individual05(sex|gender)",
        "individuals_postcode": r"individual06taxablestatus",
        "company_industry": r"company04industry",
        "gst_industry": r"gst04byindustry",
    }

    # Worksheet to read within each workbook. Sheet names drift across
    # releases ("Company Table 4A" -> "Table 4A"), so these are regexes.
    # Company 4A (broad x fine industry) is preferred over 4B: 4B carries
    # only 7 measures before 2018-19, while 4A is wide in every release.
    # Individuals 6A is preferred over 6B: 6B is 6A totalled over
    # taxable status, so taking both would double-count.
    SHEET_SELECTORS = {
        "individuals_income_state": r"table\s*4(?![ab])",
        "individuals_industry": r"table\s*5",
        "individuals_postcode": r"table\s*6a",
        "company_industry": r"table\s*4a",
        "gst_industry": r"table\s*4(?![ab])",
    }

    # Raw header label -> architecture column name. Keys are matched after
    # footnote digits are stripped and whitespace collapsed.
    DIMENSION_NAMES = {
        "sex": "sex",
        "gender": "sex",
        "taxable status": "taxable_status",
        "state/territory": "state_abbreviation",
        "state/ territory": "state_abbreviation",
        "postcode": "postcode",
        "statistical area level 4 (sa4)": "sa4_name",
        "broad industry": "broad_industry",
        "fine industry": "fine_industry",
        "taxable income range": "taxable_income_range",
        "taxable income range - tax brackets": "taxable_income_bracket",
    }

    # Dimensions whose values carry a leading sort/classification prefix
    # ("A. Mining", "011 Nursery ...", "ab. $6,001 to $10,000"). Each is
    # split into an identifier column and a readable label column. The
    # suffix distinguishes the two kinds: ANZSIC industry codes are real
    # classification identifiers (_id), while the income-range prefixes are
    # presentation sort keys (_code).
    PREFIXED_DIMENSIONS = {
        "broad_industry": "id",
        "fine_industry": "id",
        "taxable_income_range": "code",
        "taxable_income_bracket": "code",
    }

    # Item labels that differ only by capitalisation between releases.
    # Canonicalised so a single item does not fragment the panel.
    ITEM_ALIASES = {
        "gross interest": "Gross interest",
        "total income": "Total income",
        "taxable income or loss": "Taxable income or loss",
        "loss carry back tax offset": "Loss carry back tax offset",
        "other income category 2 (ato interest)": (
            "Other income category 2 (ATO interest)"
        ),
    }

    # Column order of the staging parquet the cleaner writes. Geography
    # leads the non-temporal dimensions, per the house column-ordering rule.
    DIMENSIONS = {
        "individuals_income_state": [
            "state_abbreviation",
            "sex",
            "taxable_status",
            "taxable_income_range_code",
            "taxable_income_range",
            "taxable_income_bracket_code",
            "taxable_income_bracket",
        ],
        "individuals_industry": [
            "state_abbreviation",
            "sex",
            "broad_industry_id",
            "broad_industry",
        ],
        "individuals_postcode": [
            "state_abbreviation",
            "sa4_name",
            "postcode",
            "taxable_status",
        ],
        "company_industry": [
            "broad_industry_id",
            "broad_industry",
            "fine_industry_id",
            "fine_industry",
        ],
        "gst_industry": [
            "broad_industry_id",
            "broad_industry",
            "fine_industry_id",
            "fine_industry",
        ],
    }

    # Columns the dbt model derives rather than the cleaner: {table:
    # {column: column it is inserted after}}. sa4_id is resolved by joining
    # the ATO's SA4 name against br_bd_diretorios_au.sa4_2021, because the
    # source publishes the SA4 name only and never its ABS code.
    DERIVED_COLUMNS = {
        "individuals_postcode": {"sa4_id": "state_abbreviation"},
    }

    MEASURES = ["item", "record_count", "amount"]

    # Columns whose distinct values are recorded in the dicionario table.
    DICTIONARY_COLUMNS = {
        "individuals_income_state": [
            "state_abbreviation",
            "sex",
            "taxable_status",
            "taxable_income_range_code",
            "taxable_income_bracket_code",
        ],
        "individuals_industry": [
            "state_abbreviation",
            "sex",
            "broad_industry_id",
        ],
        "individuals_postcode": ["state_abbreviation", "taxable_status"],
        "company_industry": ["broad_industry_id", "fine_industry_id"],
        "gst_industry": ["broad_industry_id", "fine_industry_id"],
    }
