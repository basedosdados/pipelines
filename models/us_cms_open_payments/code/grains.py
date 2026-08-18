"""The identifying grain of every table, and how each one is clustered.

The grain drives both the dbt uniqueness test and the not_null tests. Payment
records carry a system-assigned Record_ID unique within a program year, so the
detail tables key on (year, record_id); the summary reports key on whatever
combination CMS grouped by.
"""

GRAIN = {
    "general": ["year", "record_id"],
    "general_legacy": ["year", "record_id"],
    "research": ["year", "record_id"],
    "research_legacy": ["year", "record_id"],
    "ownership": ["year", "record_id"],
    "research_principal_investigator": [
        "year",
        "record_id",
        "principal_investigator_number",
    ],
    "covered_recipient_profile": ["covered_recipient_profile_id"],
    "teaching_hospital_profile": ["teaching_hospital_ccn"],
    "reporting_entity_profile": ["reporting_entity_id"],
    "provider_profile_mapping": ["primary_profile_id", "secondary_profile_id"],
    "summary_by_recipient_nature": [
        "year",
        "recipient_id",
        "recipient_type",
        "payment_nature_code",
    ],
    "summary_by_recipient_entity": [
        "year",
        "recipient_id",
        "recipient_type",
        "reporting_entity_id",
        "payment_type",
    ],
    "summary_by_entity_nature": [
        "year",
        "reporting_entity_id",
        "payment_nature_code",
    ],
    "summary_by_entity_recipient_nature": [
        "year",
        "reporting_entity_id",
        "recipient_id",
        "recipient_type",
        "payment_nature_code",
    ],
    "summary_state_by_nature": [
        "year",
        "country_code",
        "state_code",
        "payment_nature",
        "recipient_type",
    ],
    "summary_national": [
        "year",
        "metric_level",
        "payment_type",
        "recipient_type",
    ],
    "summary_national_by_specialty": [
        "year",
        "payment_type",
        "recipient_type",
        "taxonomy_code",
    ],
    "summary_state": [
        "year",
        "country_code",
        "state_code",
        "payment_type",
        "recipient_type",
    ],
    "summary_teaching_hospital": ["year", "teaching_hospital_ccn"],
    "summary_reporting_entity": ["year", "reporting_entity_id"],
    "summary_physician": ["year", "covered_recipient_profile_id"],
    "summary_dashboard": ["year", "dashboard_row_number"],
    "dicionario": ["id_tabela", "nome_coluna", "chave", "cobertura_temporal"],
}

# BigQuery clustering for the tables large enough to benefit. Recipient and
# reporting entity are the two columns nearly every analytical query filters on.
CLUSTER = {
    "general": ["covered_recipient_profile_id", "reporting_entity_id"],
    "general_legacy": ["physician_profile_id", "reporting_entity_id"],
    "research": ["covered_recipient_profile_id", "reporting_entity_id"],
    "research_legacy": ["physician_profile_id", "reporting_entity_id"],
    "research_principal_investigator": [
        "covered_recipient_profile_id",
        "record_id",
    ],
    "summary_by_recipient_nature": ["recipient_id"],
    "summary_by_recipient_entity": ["recipient_id", "reporting_entity_id"],
    "summary_by_entity_recipient_nature": [
        "reporting_entity_id",
        "recipient_id",
    ],
    "summary_physician": ["covered_recipient_profile_id"],
}
