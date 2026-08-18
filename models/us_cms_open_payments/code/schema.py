"""Column types, dictionary flags, directory links and measurement units.

Types follow arithmetic meaning rather than storage: a column is INT64 or
FLOAT64 only when summing or averaging it means something. Everything CMS
stores as digits but uses as a label -- NPIs, profile identifiers, CCNs, NDC
and PDI product codes, ZIP codes, nature-of-payment codes -- stays STRING.
"""

import re

# --- types ------------------------------------------------------------------
DATE_COLUMNS = {"payment_date", "publication_date"}

# US dollar amounts.
_MONEY = re.compile(
    r"^(payment_amount|amount_invested_total|interest_value|amount_total"
    r"|general_payment_amount_total|research_payment_amount_total"
    r"|ownership_amount_invested_total|ownership_interest_value_total"
    r"|associated_research_payment_amount_total)"
)

# Counts. Means and medians of a count are not integers, so they are FLOAT64.
_COUNT = re.compile(
    r"(^payment_count$|_count$|^payment_count_(total|mean|median)_|^transaction_count$"
    r"|^physician_count$|^non_physician_practitioner_count$|^teaching_hospital_count$)"
)
_FRACTIONAL = re.compile(
    r"^(payment_amount_(mean|median)|payment_count_(mean|median))"
)


def bigquery_type(table: str, column: str) -> str:
    if column == "year":
        return "INT64"
    if column in DATE_COLUMNS:
        return "DATE"
    if table == "summary_dashboard" and column == "value":
        # The dashboard mixes dollar totals and record counts in one column.
        return "FLOAT64"
    if _MONEY.match(column) or _FRACTIONAL.match(column):
        return "FLOAT64"
    if _COUNT.search(column):
        return "INT64"
    return "STRING"


# --- measurement units ------------------------------------------------------
# Plain counts of transactions or organisations have no unit in the Data Basis
# vocabulary; they are dimensionless and the observations column says so.
PEOPLE_COUNTS = {"physician_count", "non_physician_practitioner_count"}


def measurement_unit(table: str, column: str) -> str:
    if column == "year":
        return "year"
    if table == "summary_dashboard" and column == "value":
        # Rows mix dollar totals with record counts, so no single unit applies.
        return ""
    if bigquery_type(table, column) == "FLOAT64" and not _FRACTIONAL.match(
        column
    ):
        return "usd"
    if _FRACTIONAL.match(column):
        return "usd" if column.startswith("payment_amount") else ""
    if column in PEOPLE_COUNTS:
        return "person"
    return ""


# --- dictionary coverage ----------------------------------------------------
# Bounded categorical columns whose value set belongs in the dicionario table.
# Free-text fields (product and study names, contextual information) and
# unbounded identifier spaces (NDC, PDI, NPI, taxonomy codes) are excluded:
# a dictionary of every National Drug Code would be an identifier list, not a
# value set. Cardinality is re-checked against the real data during cleaning
# and anything unexpectedly large is demoted -- see verify_dictionary_scope.
DICTIONARY_EXACT = {
    "change_type",
    "covered_recipient_type",
    "recipient_type",
    "profile_type",
    "payment_form",
    "payment_nature",
    "payment_nature_code",
    "payment_type",
    "dispute_status",
    "related_product_indicator",
    "physician_ownership_indicator",
    "third_party_payment_recipient_indicator",
    "third_party_equals_covered_recipient_indicator",
    "charity_indicator",
    "delay_in_publication_indicator",
    "preclinical_research_indicator",
    "interest_held_by_physician_or_family",
    "physician_primary_type",
    "has_multiple_ids",
    "metric_level",
}

DICTIONARY_PATTERNS = [
    re.compile(r"^covered_recipient_primary_type_\d$"),
    re.compile(r"^primary_type_\d$"),
    re.compile(r"^product_covered_indicator_\d$"),
    re.compile(r"^product_type_\d$"),
    re.compile(r"^expenditure_category_\d$"),
]


def covered_by_dictionary(table: str, column: str) -> str:
    if table == "dicionario":
        return "no"
    if bigquery_type(table, column) != "STRING":
        return "no"
    if column in DICTIONARY_EXACT or any(
        p.match(column) for p in DICTIONARY_PATTERNS
    ):
        return "yes"
    return "no"


# --- directory links --------------------------------------------------------
US_STATE = "br_bd_diretorios_us.state:abbreviation"
TIME_YEAR = "br_bd_diretorios_data_tempo.ano:ano"

# Two-letter US state columns. The directory link is kept because it is the
# correct semantic reference and the backend does not enforce it, but no dbt
# relationships test is emitted for these: CMS also publishes the armed-forces
# codes AA, AE and AP, which br_bd_diretorios_us.state does not hold, plus a
# handful of malformed entries ('0R', 'NU'). That is 792 rows out of 27 million
# in general_legacy -- too few to matter, and enough to fail a strict test.
STATE_NOT_IN_DIRECTORY = (
    "Além das siglas de estados e territórios dos Estados Unidos, a coluna traz os códigos "
    "das forças armadas AA, AE e AP, ausentes do diretório br_bd_diretorios_us.state."
)
STATE_COLUMNS = {
    ("general", "recipient_state"),
    ("general_legacy", "recipient_state"),
    ("research", "recipient_state"),
    ("research_legacy", "recipient_state"),
    ("ownership", "recipient_state"),
    ("research_principal_investigator", "state"),
    ("covered_recipient_profile", "state"),
    ("teaching_hospital_profile", "state"),
    ("summary_teaching_hospital", "state"),
    ("summary_physician", "state"),
}


def directory_column(table: str, column: str) -> str:
    if column == "year":
        return TIME_YEAR
    if (table, column) in STATE_COLUMNS:
        return US_STATE
    return ""


# --- observations -----------------------------------------------------------
OBSERVATIONS = {
    "taxonomy_code": (
        "Código da taxonomia NUCC de prestadores. A tabela summary_national_by_specialty "
        "traz a correspondência entre código e rótulo; um diretório compartilhado de "
        "taxonomia de prestadores seria o destino adequado a longo prazo."
    ),
    "covered_recipient_npi": "Identificador nacional de prestador; armazenado como STRING por não admitir aritmética.",
    "physician_npi": "Identificador nacional de prestador; armazenado como STRING por não admitir aritmética.",
    "payment_nature_code": "Código numérico da natureza do pagamento; o rótulo correspondente está no dicionário.",
}

_COUNT_NOTE = "Contagem adimensional, sem unidade de medida aplicável."
_DASHBOARD_NOTE = (
    "A unidade varia por linha: algumas métricas do painel são valores em dólares "
    "americanos e outras são contagens de registros. Ver a coluna metric."
)


def observations(table: str, column: str) -> str:
    if (table, column) in STATE_COLUMNS:
        return STATE_NOT_IN_DIRECTORY
    if table == "summary_dashboard" and column == "value":
        return _DASHBOARD_NOTE
    if column in OBSERVATIONS:
        return OBSERVATIONS[column]
    if column.startswith("taxonomy_") and column != "taxonomy_code":
        return OBSERVATIONS["taxonomy_code"]
    numeric = bigquery_type(table, column) in {"INT64", "FLOAT64"}
    if numeric and column != "year" and not measurement_unit(table, column):
        return _COUNT_NOTE
    return ""


# --- sensitivity ------------------------------------------------------------
# Open Payments publishes named individuals by statute; the data is public but
# the name and address columns are personal, so they are flagged as such.
_PERSONAL = re.compile(
    r"(first_name|middle_name|last_name|name_suffix|address_line|_npi$|^recipient_city$"
    r"|^recipient_zip_code$|^recipient_postal_code$|^city$|^zip_code$)"
)


def has_sensitive_data(table: str, column: str) -> str:
    if table in {
        "teaching_hospital_profile",
        "reporting_entity_profile",
        "summary_teaching_hospital",
    }:
        return "no"
    return "yes" if _PERSONAL.search(column) else "no"
