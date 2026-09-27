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

# Entity slugs used as observation levels; ids resolved per environment.
ENTITY_SLUGS = [
    "payment",
    "person",
    "hospital",
    "company",
    "state",
    "occupation",
    "year",
]


# --- raw data sources -------------------------------------------------------
# Each table is linked to exactly one raw source. The backend's
# client._raw_source_id resolver raises when a table has two or more, which
# would make a recurring pipeline fail at its first poll.
RAW_SOURCES = {
    "detail": {
        "name_pt": "Open Payments — arquivos detalhados por ano do programa",
        "name_en": "Open Payments — detailed files by program year",
        "name_es": "Open Payments — archivos detallados por año del programa",
        "description_pt": (
            "Arquivos CSV com um registro por pagamento declarado, publicados por ano do "
            "programa. Os anos de 2019 a 2025 são distribuídos como CSV avulsos; 2013 a 2018 "
            "estão arquivados, um ZIP por ano do programa."
        ),
        "description_en": (
            "CSV files with one row per reported payment, published by program year. Program "
            "years 2019 to 2025 are distributed as loose CSVs; 2013 to 2018 are archived, one "
            "ZIP per program year."
        ),
        "description_es": (
            "Archivos CSV con un registro por pago declarado, publicados por año del programa. "
            "Los años 2019 a 2025 se distribuyen como CSV sueltos; 2013 a 2018 están archivados, "
            "un ZIP por año del programa."
        ),
        "url": "https://openpaymentsdata.cms.gov/datasets",
        "tables": [
            "general",
            "general_legacy",
            "research",
            "research_legacy",
            "research_principal_investigator",
            "ownership",
            "dicionario",
        ],
    },
    "profile": {
        "name_pt": "Open Payments — arquivos de perfil de beneficiários e entidades",
        "name_en": "Open Payments — recipient and entity profile files",
        "name_es": "Open Payments — archivos de perfil de beneficiarios y entidades",
        "description_pt": (
            "Cadastros de médicos e profissionais não médicos, hospitais universitários e "
            "entidades declarantes, publicados como instantâneo do ciclo de publicação corrente."
        ),
        "description_en": (
            "Registers of physicians and non-physician practitioners, teaching hospitals and "
            "reporting entities, published as a snapshot of the current publication cycle."
        ),
        "description_es": (
            "Registros de médicos y profesionales no médicos, hospitales universitarios y "
            "entidades declarantes, publicados como instantánea del ciclo de publicación actual."
        ),
        "url": "https://openpaymentsdata.cms.gov/datasets",
        "tables": [
            "covered_recipient_profile",
            "teaching_hospital_profile",
            "reporting_entity_profile",
            "provider_profile_mapping",
        ],
    },
    "summary": {
        "name_pt": "Open Payments — relatórios agregados",
        "name_en": "Open Payments — summary reports",
        "name_es": "Open Payments — informes agregados",
        "description_pt": (
            "Relatórios agregados publicados pelo CMS com totais, médias e medianas de pagamento "
            "por beneficiário, entidade declarante, natureza do pagamento, estado e "
            "especialidade. Reconstruídos apenas para o ciclo de publicação corrente, portanto "
            "cobrem 2019 a 2025."
        ),
        "description_en": (
            "Summary reports published by CMS with payment totals, means and medians by "
            "recipient, reporting entity, nature of payment, state and specialty. Rebuilt only "
            "for the current publication cycle, so they cover 2019 to 2025."
        ),
        "description_es": (
            "Informes agregados publicados por el CMS con totales, medias y medianas de pago por "
            "beneficiario, entidad declarante, naturaleza del pago, estado y especialidad. "
            "Reconstruidos solo para el ciclo de publicación actual, por lo que cubren 2019 a 2025."
        ),
        "url": "https://openpaymentsdata.cms.gov/datasets",
        "tables": [
            "summary_by_recipient_nature",
            "summary_by_recipient_entity",
            "summary_by_entity_nature",
            "summary_by_entity_recipient_nature",
            "summary_state_by_nature",
            "summary_national",
            "summary_national_by_specialty",
            "summary_state",
            "summary_teaching_hospital",
            "summary_reporting_entity",
            "summary_physician",
            "summary_dashboard",
        ],
    },
}

# Reference objects are named by slug, not id: most UUIDs happen to match
# between staging and prod, but the cc0 licence does not, and a hardcoded id
# would have written the wrong licence to prod without any error.
LICENSE_SLUG = "cc0"
AVAILABILITY_SLUG = "online"
ORGANIZATION_SLUG = "centers_for_medicare_medicaid_services_cms"
THEME_SLUGS = ["health", "economics", "government"]

# The entity tables are a snapshot of the current publication cycle, so they
# describe the recipients and entities appearing in 2019-2025, not the whole
# 2013-2025 payment history.
ENTITY_TABLE_COVERAGE = (2019, 2025)

# Tags. Six already existed; conflict-of-interest, pharmaceutical-industry and
# medical-device were created for this dataset, with English kebab-case slugs
# per the tag convention. Deliberately absent: `saude`, which would restate the
# health theme, and any place name, which the coverage metadata already carries.
# Prod renamed many tags to English slugs while keeping the same records, so
# each tag is listed as the aliases to try in order. `medicamento` on staging
# and `medication` on prod are the same UUID.
TAG_SLUGS = [
    ("medicamento", "medication"),
    ("hospital",),
    ("transparencia", "transparency"),
    ("lobby",),
    ("pesquisa", "research"),
    ("investment",),
    ("conflict-of-interest",),
    ("pharmaceutical-industry",),
    ("medical-device",),
]

# Tags created for this dataset, so they can be recreated on prod where the
# vocabulary is a separate set of records.
NEW_TAGS = [
    (
        "conflict-of-interest",
        "conflito de interesse",
        "conflict of interest",
        "conflicto de interés",
    ),
    (
        "pharmaceutical-industry",
        "indústria farmacêutica",
        "pharmaceutical industry",
        "industria farmacéutica",
    ),
    (
        "medical-device",
        "dispositivo médico",
        "medical device",
        "dispositivo médico",
    ),
]
