"""Table specification for us_fema_openfema (shared by the pipeline and the build).

Four tables, each backed by exactly one OpenFEMA set. `dicionario` is built
separately by ``build_dicionario.py``.

Scope decision for pass one (logged): `nfip_policy` is IN. It is the largest
table by far — 74.3M rows, a 3.7 GB parquet — but FEMA ships it as one
well-typed file, so the marginal cost is compute rather than design, and
claims without policies is a denominator-free dataset: take-up rates and loss
ratios, the main analytical use of NFIP data, need both.

The two sets named in the original brief, `FimaNfipClaims` v2 and
`FimaNfipPolicies` v2, are DEPRECATED — removed 2026-10-15 and frozen at
2026-06-01 — so this builds against their live successors, `NfipClaims` v3 and
`NfipPolicies` v3.
"""

from __future__ import annotations

# Source columns dropped everywhere: FEMA's internal change-detection
# bookkeeping, not data about disasters or policies.
DROP = {"hash", "last_refresh"}

# Source name (snake_cased) -> harmonised name. The NFIP claim and policy files
# spell the same concept differently; one name is used for both.
HARMONISE = {
    "disaster_assistance_coverage_required_code": (
        "disaster_assistance_coverage_required"
    ),
    "number_of_floors_in_the_insured_building": (
        "number_of_floors_in_insured_building"
    ),
    "house_worship": "house_of_worship_indicator",
    "pre_firm_indicator": "pre_firm_construction_indicator",
    # `state` (claims) and `property_state` (policies) are both the USPS
    # abbreviation; `state` in the declarations file is too, despite what
    # FEMA's field dictionary says.
    "state": "state_abbreviation",
    "property_state": "state_abbreviation",
    "fips_state_code": "state_id",
    "state_number_code": "state_id",
}

# The Public Assistance file does not use FIPS for two Pacific territories: it
# emits FEMA-internal codes. Verified against DisasterDeclarationsSummaries,
# which does use FIPS for the same territories.
PA_STATE_CODE_FIX = {
    "1001": "69",  # Northern Mariana Islands (MP)
    "1003": "60",  # American Samoa (AS)
}

# County codes that mean "no county": a statewide or unassigned designation.
# These must become NULL rather than a county that does not exist.
NO_COUNTY = {"0", "00", "000"}

# Where OpenFEMA's field dictionary disagrees with the parquet it actually
# ships, the parquet wins — that is what gets ingested. Verified by
# validate_types.py against the downloaded files.
TYPE_OVERRIDE = {
    # The dictionary calls this `datetime`; the parquet ships date32[day].
    ("nfip_claim", "date_of_loss"): "DATE",
}

TABLES = {
    "disaster_declaration": {
        "source": ("DisasterDeclarationsSummaries", 2),
        "name_pt": "Declarações de desastre",
        "name_en": "Disaster declarations",
        "name_es": "Declaraciones de desastre",
        "description_pt": (
            "Declarações de desastre emitidas sob o Stafford Act desde 1953, "
            "com uma linha por área designada (condado ou equivalente) dentro "
            "de cada declaração. Traz o tipo de declaração, o tipo de "
            "incidente, as datas do incidente e quais programas de assistência "
            "foram declarados."
        ),
        "description_en": (
            "Disaster declarations issued under the Stafford Act since 1953, "
            "one row per designated area (county or equivalent) within each "
            "declaration. Carries the declaration type, the incident type, the "
            "incident dates and which assistance programs were declared."
        ),
        "description_es": (
            "Declaraciones de desastre emitidas bajo el Stafford Act desde "
            "1953, con una fila por área designada (condado o equivalente) "
            "dentro de cada declaración. Incluye el tipo de declaración, el "
            "tipo de incidente, las fechas del incidente y qué programas de "
            "asistencia fueron declarados."
        ),
        # Renames applied on top of HARMONISE, for this table only.
        "rename": {"id": "record_id"},
        "partition": "year",
        "partition_source": "declaration_date",
        "primary_key": ["disaster_number", "place_code"],
        "observation_levels": ["disaster", "county"],
        # Columns placed first, after the partition column.
        "lead": [
            "state_id",
            "county_id",
            "disaster_number",
            "place_code",
            "fema_declaration_string",
            "incident_id",
        ],
    },
    "public_assistance_project": {
        "source": ("PublicAssistanceFundedProjectsDetails", 2),
        "name_pt": "Projetos financiados pelo Public Assistance",
        "name_en": "Public Assistance funded projects",
        "name_es": "Proyectos financiados por Public Assistance",
        "description_pt": (
            "Projetos financiados pelo programa Public Assistance da FEMA "
            "desde 1998, com uma linha por projeto. Traz a categoria de dano, "
            "a situação e a etapa do projeto, o custo estimado e os valores "
            "federais obrigados."
        ),
        "description_en": (
            "Projects funded by FEMA's Public Assistance program since 1998, "
            "one row per project. Carries the damage category, the project's "
            "status and process step, the estimated cost and the federal "
            "amounts obligated."
        ),
        "description_es": (
            "Proyectos financiados por el programa Public Assistance de FEMA "
            "desde 1998, con una fila por proyecto. Incluye la categoría de "
            "daño, la situación y la etapa del proyecto, el costo estimado y "
            "los montos federales obligados."
        ),
        "rename": {},
        "partition": "year",
        "partition_source": "declaration_date",
        "primary_key": ["gm_project_id"],
        "observation_levels": ["project", "county"],
        "lead": [
            "state_id",
            "county_id",
            "gm_project_id",
            "disaster_number",
            "pw_number",
            "applicant_id",
            "gm_applicant_id",
        ],
    },
    "nfip_claim": {
        "source": ("NfipClaims", 3),
        "name_pt": "Sinistros do NFIP",
        "name_en": "NFIP claims",
        "name_es": "Siniestros del NFIP",
        "description_pt": (
            "Sinistros redigidos do National Flood Insurance Program desde "
            "1970, com uma linha por sinistro. Traz a data da perda, a "
            "cobertura contratada, os valores pagos separados por edificação, "
            "conteúdo e Increased Cost of Compliance, a zona de inundação, o "
            "tipo de ocupação e a localização aproximada. A FEMA publica este "
            "arquivo já redigido: latitude e longitude vêm arredondadas a uma "
            "casa decimal e o endereço é reduzido a cidade, código postal e "
            "grupo de quadras censitário."
        ),
        "description_en": (
            "Redacted National Flood Insurance Program claims since 1970, one "
            "row per claim. Carries the date of loss, the coverage held, the "
            "amounts paid split by building, contents and Increased Cost of "
            "Compliance, the flood zone, the occupancy type and the "
            "approximate location. FEMA publishes this file already redacted: "
            "latitude and longitude arrive rounded to one decimal place and "
            "the address is reduced to city, postal code and census block "
            "group."
        ),
        "description_es": (
            "Siniestros redactados del National Flood Insurance Program desde "
            "1970, con una fila por siniestro. Incluye la fecha de la pérdida, "
            "la cobertura contratada, los montos pagados separados por "
            "edificación, contenido e Increased Cost of Compliance, la zona de "
            "inundación, el tipo de ocupación y la ubicación aproximada. FEMA "
            "publica este archivo ya redactado: latitud y longitud llegan "
            "redondeadas a un decimal y la dirección se reduce a ciudad, "
            "código postal y grupo de manzanas censal."
        ),
        "rename": {
            "id": "claim_id",
            "county_code": "county_id",
            "census_geoid": "census_block_group_id",
        },
        "partition": "year",
        "partition_source": "year_of_loss",
        "primary_key": ["claim_id"],
        "observation_levels": ["property", "county"],
        "lead": [
            "state_abbreviation",
            "county_id",
            "census_tract_id",
            "census_block_group_id",
            "claim_id",
            "date_of_loss",
        ],
    },
    "nfip_policy": {
        "source": ("NfipPolicies", 3),
        "name_pt": "Apólices do NFIP",
        "name_en": "NFIP policies",
        "name_es": "Pólizas del NFIP",
        "description_pt": (
            "Apólices redigidas do National Flood Insurance Program em vigor "
            "desde 2009, com uma linha por apólice. Traz as datas de vigência, "
            "a importância segurada de edificação e conteúdo, o prêmio e suas "
            "componentes, a base de tarifação, a zona de inundação e a "
            "localização aproximada. A FEMA publica este arquivo já redigido, "
            "nos mesmos termos do arquivo de sinistros."
        ),
        "description_en": (
            "Redacted National Flood Insurance Program policies in force since "
            "2009, one row per policy. Carries the term dates, the building "
            "and contents coverage amounts, the premium and its components, "
            "the rating basis, the flood zone and the approximate location. "
            "FEMA publishes this file already redacted, on the same terms as "
            "the claims file."
        ),
        "description_es": (
            "Pólizas redactadas del National Flood Insurance Program vigentes "
            "desde 2009, con una fila por póliza. Incluye las fechas de "
            "vigencia, la suma asegurada de edificación y contenido, la prima "
            "y sus componentes, la base de tarificación, la zona de inundación "
            "y la ubicación aproximada. FEMA publica este archivo ya "
            "redactado, en los mismos términos que el archivo de siniestros."
        ),
        "rename": {
            "id": "policy_id",
            "census_geoid": "census_block_group_id",
        },
        "partition": "year",
        "partition_source": "policy_effective_date",
        "primary_key": ["policy_id"],
        "observation_levels": ["property", "county"],
        "lead": [
            "state_abbreviation",
            "county_id",
            "census_tract_id",
            "census_block_group_id",
            "policy_id",
            "policy_effective_date",
        ],
    },
}

# Columns computed during cleaning rather than read from the source. `after`
# names the source column they are derived from, for the observations note.
DERIVED = {
    "disaster_declaration": [
        (
            "county_id",
            "STRING",
            "state_id || fips_county_code, null when fips_county_code = '000'",
        ),
    ],
    "public_assistance_project": [
        (
            "county_id",
            "STRING",
            "zero-padded state_id || zero-padded county_code",
        ),
    ],
    "nfip_claim": [
        ("census_tract_id", "STRING", "first 11 digits of census_geoid"),
    ],
    "nfip_policy": [
        ("county_id", "STRING", "first 5 digits of census_geoid"),
        ("census_tract_id", "STRING", "first 11 digits of census_geoid"),
    ],
}
