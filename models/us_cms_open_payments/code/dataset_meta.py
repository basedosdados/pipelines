"""Trilingual dataset and table names and descriptions for the backend."""

DATASET = {
    "slug": "open_payments",
    "name_pt": "Open Payments — Pagamentos da Indústria a Médicos e Hospitais",
    "name_en": "Open Payments — Industry Payments to Physicians and Teaching Hospitals",
    "name_es": "Open Payments — Pagos de la Industria a Médicos y Hospitales",
    "description_pt": (
        "Pagamentos e transferências de valor de fabricantes de medicamentos e dispositivos "
        "médicos e de organizações de compras a médicos, profissionais não médicos e hospitais "
        "universitários dos Estados Unidos, publicados pelo Centers for Medicare & Medicaid "
        "Services sob o Physician Payments Sunshine Act. Cobre os anos do programa 2013 a 2025 "
        "em três categorias — pagamentos gerais, pagamentos de pesquisa e participações "
        "societárias — acompanhadas dos cadastros de beneficiários, hospitais universitários e "
        "entidades declarantes e dos relatórios agregados do CMS."
    ),
    "description_en": (
        "Payments and other transfers of value from drug and medical device manufacturers and "
        "group purchasing organizations to physicians, non-physician practitioners and teaching "
        "hospitals in the United States, published by the Centers for Medicare & Medicaid "
        "Services under the Physician Payments Sunshine Act. Covers program years 2013 to 2025 "
        "across three categories — general payments, research payments and ownership interests — "
        "together with the recipient, teaching hospital and reporting entity profiles and the "
        "CMS summary reports."
    ),
    "description_es": (
        "Pagos y transferencias de valor de fabricantes de medicamentos y dispositivos médicos y "
        "de organizaciones de compras a médicos, profesionales no médicos y hospitales "
        "universitarios de Estados Unidos, publicados por los Centers for Medicare & Medicaid "
        "Services bajo el Physician Payments Sunshine Act. Cubre los años del programa 2013 a "
        "2025 en tres categorías — pagos generales, pagos de investigación y participaciones "
        "societarias — junto con los perfiles de beneficiarios, hospitales universitarios y "
        "entidades declarantes y los informes agregados del CMS."
    ),
}

# table -> (name_pt, name_en, name_es)
TABLE_NAMES = {
    "general": ("Pagamentos Gerais", "General Payments", "Pagos Generales"),
    "general_legacy": (
        "Pagamentos Gerais (2013-2015)",
        "General Payments (2013-2015)",
        "Pagos Generales (2013-2015)",
    ),
    "research": (
        "Pagamentos de Pesquisa",
        "Research Payments",
        "Pagos de Investigación",
    ),
    "research_legacy": (
        "Pagamentos de Pesquisa (2013-2015)",
        "Research Payments (2013-2015)",
        "Pagos de Investigación (2013-2015)",
    ),
    "research_principal_investigator": (
        "Pesquisadores Principais",
        "Principal Investigators",
        "Investigadores Principales",
    ),
    "ownership": (
        "Participações Societárias",
        "Ownership and Investment Interests",
        "Participaciones Societarias",
    ),
    "covered_recipient_profile": (
        "Perfis de Beneficiários",
        "Covered Recipient Profiles",
        "Perfiles de Beneficiarios",
    ),
    "teaching_hospital_profile": (
        "Perfis de Hospitais Universitários",
        "Teaching Hospital Profiles",
        "Perfiles de Hospitales Universitarios",
    ),
    "reporting_entity_profile": (
        "Perfis de Entidades Declarantes",
        "Reporting Entity Profiles",
        "Perfiles de Entidades Declarantes",
    ),
    "provider_profile_mapping": (
        "Correspondência de Perfis de Prestadores",
        "Provider Profile Mapping",
        "Correspondencia de Perfiles de Prestadores",
    ),
    "summary_by_recipient_nature": (
        "Resumo por Beneficiário e Natureza",
        "Summary by Recipient and Nature of Payment",
        "Resumen por Beneficiario y Naturaleza",
    ),
    "summary_by_recipient_entity": (
        "Resumo por Beneficiário e Entidade Declarante",
        "Summary by Recipient and Reporting Entity",
        "Resumen por Beneficiario y Entidad Declarante",
    ),
    "summary_by_entity_nature": (
        "Resumo por Entidade Declarante e Natureza",
        "Summary by Reporting Entity and Nature of Payment",
        "Resumen por Entidad Declarante y Naturaleza",
    ),
    "summary_by_entity_recipient_nature": (
        "Resumo por Entidade, Beneficiário e Natureza",
        "Summary by Reporting Entity, Recipient and Nature of Payment",
        "Resumen por Entidad, Beneficiario y Naturaleza",
    ),
    "summary_state_by_nature": (
        "Resumo por Estado e Natureza",
        "Summary by State and Nature of Payment",
        "Resumen por Estado y Naturaleza",
    ),
    "summary_national": (
        "Resumo Nacional",
        "National Summary",
        "Resumen Nacional",
    ),
    "summary_national_by_specialty": (
        "Resumo Nacional por Especialidade",
        "National Summary by Specialty",
        "Resumen Nacional por Especialidad",
    ),
    "summary_state": (
        "Resumo por Estado",
        "State Summary",
        "Resumen por Estado",
    ),
    "summary_teaching_hospital": (
        "Resumo por Hospital Universitário",
        "Summary by Teaching Hospital",
        "Resumen por Hospital Universitario",
    ),
    "summary_reporting_entity": (
        "Resumo por Entidade Declarante",
        "Summary by Reporting Entity",
        "Resumen por Entidad Declarante",
    ),
    "summary_physician": (
        "Resumo por Médico",
        "Summary by Physician",
        "Resumen por Médico",
    ),
    "summary_dashboard": (
        "Painel Resumo",
        "Summary Dashboard",
        "Panel Resumen",
    ),
    "dicionario": ("Dicionário", "Dictionary", "Diccionario"),
}

# Observation levels per table, as entity slugs. The detail tables are one row
# per payment record; the child table adds the investigator.
OBSERVATION_LEVELS = {
    "general": ["year", "payment"],
    "general_legacy": ["year", "payment"],
    "research": ["year", "payment"],
    "research_legacy": ["year", "payment"],
    "ownership": ["year", "payment"],
    "research_principal_investigator": ["year", "payment", "person"],
    "covered_recipient_profile": ["person"],
    "teaching_hospital_profile": ["hospital"],
    "reporting_entity_profile": ["company"],
    "provider_profile_mapping": ["person"],
    "summary_by_recipient_nature": ["year", "person"],
    "summary_by_recipient_entity": ["year", "person", "company"],
    "summary_by_entity_nature": ["year", "company"],
    "summary_by_entity_recipient_nature": ["year", "company", "person"],
    "summary_state_by_nature": ["year", "state"],
    "summary_national": ["year"],
    "summary_national_by_specialty": ["year", "occupation"],
    "summary_state": ["year", "state"],
    "summary_teaching_hospital": ["year", "hospital"],
    "summary_reporting_entity": ["year", "company"],
    "summary_physician": ["year", "person"],
    "summary_dashboard": ["year"],
    "dicionario": [],
}

# Which column identifies each observation level, so the site does not render
# the level as "Não informado".
OBSERVATION_LEVEL_COLUMN = {
    ("general", "payment"): "record_id",
    ("general_legacy", "payment"): "record_id",
    ("research", "payment"): "record_id",
    ("research_legacy", "payment"): "record_id",
    ("ownership", "payment"): "record_id",
    ("research_principal_investigator", "payment"): "record_id",
    (
        "research_principal_investigator",
        "person",
    ): "covered_recipient_profile_id",
    ("covered_recipient_profile", "person"): "covered_recipient_profile_id",
    ("teaching_hospital_profile", "hospital"): "teaching_hospital_ccn",
    ("reporting_entity_profile", "company"): "reporting_entity_id",
    ("provider_profile_mapping", "person"): "primary_profile_id",
    ("summary_by_recipient_nature", "person"): "recipient_id",
    ("summary_by_recipient_entity", "person"): "recipient_id",
    ("summary_by_recipient_entity", "company"): "reporting_entity_id",
    ("summary_by_entity_nature", "company"): "reporting_entity_id",
    ("summary_by_entity_recipient_nature", "company"): "reporting_entity_id",
    ("summary_by_entity_recipient_nature", "person"): "recipient_id",
    ("summary_state_by_nature", "state"): "state_code",
    ("summary_national_by_specialty", "occupation"): "taxonomy_code",
    ("summary_state", "state"): "state_code",
    ("summary_teaching_hospital", "hospital"): "teaching_hospital_ccn",
    ("summary_reporting_entity", "company"): "reporting_entity_id",
    ("summary_physician", "person"): "covered_recipient_profile_id",
}

ENTITY_IDS = {
    "payment": "7cd9f097-f7ad-4b8a-8c07-746b6fbef450",
    "person": "b4e76213-888b-40ea-b877-d82ce76d71a2",
    "hospital": "72cd18a6-42c4-4fbb-987e-cb26272a5c14",
    "company": "b585c285-3ad7-4b86-9c36-6195e4760a46",
    "state": "839765a7-9c7a-44bd-bb88-357cedba03f6",
    "occupation": "859cabcb-db31-4d57-aa3d-ca6b6d840b9c",
    "year": "e1bf146e-b6bb-4b65-bee7-c800876e80a5",
}
