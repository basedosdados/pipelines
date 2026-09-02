"""Emit columns_json + architecture CSVs (BD source of truth) for us_hhs_nppes.

Single spec, two outputs:
  * ``code/columns_json/<table>.json`` — trilingual column definitions, used to
    patch EN/ES descriptions at metadata registration (``bulk_upsert_columns``).
  * ``code/architecture/<table>.csv`` — the BD architecture table, uploaded to
    Drive and consumed by ``upload_columns_from_sheet`` (which is the only tool
    that sets ``bigquery_type`` at column creation).

The cleaning transform reads column ORDER from the architecture CSVs, so this
file is the one place a column is defined.
"""

import csv
import json
from pathlib import Path

HERE = Path(__file__).parent
JSON_DIR = HERE / "columns_json"
ARCH_DIR = HERE / "architecture"

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

# Directory foreign keys. NPPES state fields carry USPS 2-letter abbreviations,
# so they point at the directory's `abbreviation` column, not its FIPS key.
DIR_STATE = "br_bd_diretorios_us.state:abbreviation"

# Observations are written once in English in the spec below and translated
# here, so every column ships notes in all three languages. Leaving PT-only
# notes is how 3,022 production columns ended up untranslated.
_OBS_I18N = {
    "Partition column; taken from the end of the source file name date range": (
        "Coluna de partição; extraída do fim do intervalo de datas no nome do "
        "arquivo de origem",
        "Columna de partición; extraída del final del intervalo de fechas en el "
        "nombre del archivo de origen",
    ),
    "CMS masks values reported as SSN, ITIN or EIN; those masks are mapped to "
    "null here": (
        "O CMS mascara valores informados como SSN, ITIN ou EIN; essas máscaras "
        "são convertidas em nulo aqui",
        "El CMS enmascara valores informados como SSN, ITIN o EIN; esas máscaras "
        "se convierten en nulo aquí",
    ),
    "Also carries Canadian provinces, US military codes (AA/AE/AP) and ZZ for "
    "foreign addresses, which are outside the US state directory": (
        "Também contém províncias canadenses, códigos militares dos EUA "
        "(AA/AE/AP) e ZZ para endereços no exterior, que estão fora do diretório "
        "de estados dos EUA",
        "También contiene provincias canadienses, códigos militares de EE. UU. "
        "(AA/AE/AP) y ZZ para direcciones en el exterior, que están fuera del "
        "directorio de estados de EE. UU.",
    ),
    "USPS standard; 9-digit ZIP+4 when furnished, unpunctuated": (
        "Padrão USPS; ZIP+4 de 9 dígitos quando informado, sem pontuação",
        "Estándar USPS; ZIP+4 de 9 dígitos cuando se informa, sin puntuación",
    ),
    "Slot number 1-50 in the source's repeating group": (
        "Número da posição 1-50 no grupo repetido do arquivo de origem",
        "Número de posición 1-50 en el grupo repetido del archivo de origen",
    ),
    "Slot number 1-15 in the source's repeating group; ordering is the "
    "source's, not a ranking": (
        "Número da posição 1-15 no grupo repetido do arquivo de origem; a ordem "
        "é a da fonte, não uma classificação",
        "Número de posición 1-15 en el grupo repetido del archivo de origen; el "
        "orden es el de la fuente, no una clasificación",
    ),
    "Empty for deactivated NPIs, which carry only npi and deactivation_date": (
        "Vazio para NPIs desativados, que trazem apenas npi e deactivation_date",
        "Vacío para NPI desactivados, que traen solo npi y deactivation_date",
    ),
    "Free text; not validated by CMS": (
        "Texto livre; não validado pelo CMS",
        "Texto libre; no validado por el CMS",
    ),
    "Value 6 means the organization's other names are listed in the other_name "
    "table rather than in this column": (
        "O valor 6 indica que os outros nomes da organização estão na tabela "
        "other_name, e não nesta coluna",
        "El valor 6 indica que los otros nombres de la organización están en la "
        "tabla other_name, y no en esta columna",
    ),
    "Derived: the Healthcare Provider Taxonomy Code whose primary switch is Y. "
    "Code labels are published by NUCC and are not redistributed here": (
        "Derivada: o código de taxonomia cuja marcação de taxonomia primária é "
        "Y. As descrições dos códigos são publicadas pela NUCC e não são "
        "redistribuídas aqui",
        "Derivada: el código de taxonomía cuya marca de taxonomía primaria es Y. "
        "Las descripciones de los códigos son publicadas por la NUCC y no se "
        "redistribuyen aquí",
    ),
    "Deactivated NPIs that were not reactivated carry only npi and this date; "
    "every other column is blank": (
        "NPIs desativados e não reativados trazem apenas npi e esta data; todas "
        "as demais colunas ficam vazias",
        "Los NPI desactivados y no reactivados traen solo npi y esta fecha; "
        "todas las demás columnas quedan vacías",
    ),
    "Code set is maintained by NUCC and licensed by the AMA; the labels are not "
    "redistributed in this dataset": (
        "O conjunto de códigos é mantido pela NUCC e licenciado pela AMA; as "
        "descrições não são redistribuídas neste conjunto de dados",
        "El conjunto de códigos es mantenido por la NUCC y licenciado por la "
        "AMA; las descripciones no se redistribuyen en este conjunto de datos",
    ),
    "Split from the source field, which concatenates code and label": (
        "Separada do campo de origem, que concatena código e descrição",
        "Separada del campo de origen, que concatena código y descripción",
    ),
    "Free text; the same code appears with more than one wording": (
        "Texto livre; o mesmo código aparece com mais de uma redação",
        "Texto libre; el mismo código aparece con más de una redacción",
    ),
}


def _obs(text: str) -> tuple[str, str, str]:
    """(pt, en, es) for an observation; every note must be translated."""
    if not text:
        return "", "", ""
    if text not in _OBS_I18N:
        raise SystemExit(f"Untranslated observation: {text!r}")
    pt, es = _OBS_I18N[text]
    return pt, text, es


# c(name, type, pt, en, es, **opts) -> dict. opts: dict, dir, sens, obs, orig, unit
def c(
    name,
    typ,
    pt,
    en,
    es,
    dict_=False,
    dir_="",
    sens=False,
    obs="",
    orig="",
    unit="",
):
    return {
        "name": name,
        "bigquery_type": typ,
        "description_pt": pt,
        "description_en": en,
        "description_es": es,
        "covered_by_dictionary": dict_,
        "directory_column": dir_,
        "has_sensitive_data": sens,
        "observations": obs,
        "original_name": orig,
        "measurement_unit": unit,
    }


EXTRACTION_DATE = c(
    "extraction_date",
    "DATE",
    "Data de referência do snapshot mensal do NPPES; cada extração é uma "
    "fotografia completa do cadastro e as extrações são empilhadas",
    "Reference date of the monthly NPPES snapshot; each extraction is a full "
    "photograph of the registry and successive extractions are stacked",
    "Fecha de referencia de la instantánea mensual del NPPES; cada extracción "
    "es una fotografía completa del registro y las extracciones se apilan",
    obs="Partition column; taken from the end of the source file name date range",
    orig="(file name date range)",
)

NPI = c(
    "npi",
    "STRING",
    "Identificador Nacional de Prestador (NPI), código numérico de 10 dígitos "
    "que identifica de forma única o prestador de serviços de saúde",
    "National Provider Identifier (NPI), a 10-digit numeric code uniquely "
    "identifying the health care provider",
    "Identificador Nacional de Proveedor (NPI), código numérico de 10 dígitos "
    "que identifica de forma única al proveedor de servicios de salud",
    orig="NPI",
)


def addr_block(prefix, label_pt, label_en, label_es, orig_prefix):
    """The 8 address columns NPPES repeats for mailing and practice location."""
    return [
        c(
            f"{prefix}_address_line_1",
            "STRING",
            f"Primeira linha do endereço {label_pt}",
            f"First line of the {label_en} address",
            f"Primera línea de la dirección {label_es}",
            sens=True,
            orig=f"Provider First Line {orig_prefix}",
        ),
        c(
            f"{prefix}_address_line_2",
            "STRING",
            f"Segunda linha do endereço {label_pt}",
            f"Second line of the {label_en} address",
            f"Segunda línea de la dirección {label_es}",
            sens=True,
            orig=f"Provider Second Line {orig_prefix}",
        ),
        c(
            f"{prefix}_address_city",
            "STRING",
            f"Município do endereço {label_pt}",
            f"City of the {label_en} address",
            f"Ciudad de la dirección {label_es}",
            orig=f"Provider {orig_prefix} City Name",
        ),
        c(
            f"{prefix}_address_state",
            "STRING",
            f"Sigla de duas letras do estado do endereço {label_pt}",
            f"Two-letter state abbreviation of the {label_en} address",
            f"Sigla de dos letras del estado de la dirección {label_es}",
            dir_=DIR_STATE,
            obs="Also carries Canadian provinces, US military codes (AA/AE/AP) "
            "and ZZ for foreign addresses, which are outside the US state "
            "directory",
            orig=f"Provider {orig_prefix} State Name",
        ),
        c(
            f"{prefix}_address_postal_code",
            "STRING",
            f"Código postal (ZIP) do endereço {label_pt}, de 5 ou 9 dígitos",
            f"Postal (ZIP) code of the {label_en} address, 5 or 9 digits",
            f"Código postal (ZIP) de la dirección {label_es}, de 5 o 9 dígitos",
            obs="USPS standard; 9-digit ZIP+4 when furnished, unpunctuated",
            orig=f"Provider {orig_prefix} Postal Code",
        ),
        c(
            f"{prefix}_address_country_code",
            "STRING",
            f"Código ISO de duas letras do país do endereço {label_pt}",
            f"Two-letter ISO country code of the {label_en} address",
            f"Código ISO de dos letras del país de la dirección {label_es}",
            dict_=True,
            orig=f"Provider {orig_prefix} Country Code (If outside U.S.)",
        ),
        c(
            f"{prefix}_address_telephone_number",
            "STRING",
            f"Telefone do endereço {label_pt}",
            f"Telephone number of the {label_en} address",
            f"Teléfono de la dirección {label_es}",
            sens=True,
            orig=f"Provider {orig_prefix} Telephone Number",
        ),
        c(
            f"{prefix}_address_fax_number",
            "STRING",
            f"Fax do endereço {label_pt}",
            f"Fax number of the {label_en} address",
            f"Fax de la dirección {label_es}",
            sens=True,
            orig=f"Provider {orig_prefix} Fax Number",
        ),
    ]


PROVIDER = [
    EXTRACTION_DATE,
    NPI,
    c(
        "replacement_npi",
        "STRING",
        "NPI que substituiu este registro quando o prestador recebeu um novo "
        "identificador",
        "NPI that replaced this record when the provider was issued a new "
        "identifier",
        "NPI que sustituyó este registro cuando el proveedor recibió un nuevo "
        "identificador",
        orig="Replacement NPI",
    ),
    c(
        "entity_type_code",
        "STRING",
        "Tipo de entidade: pessoa física (1) ou organização (2)",
        "Entity type: individual (1) or organization (2)",
        "Tipo de entidad: persona física (1) u organización (2)",
        dict_=True,
        obs="Empty for deactivated NPIs, which carry only npi and "
        "deactivation_date",
        orig="Entity Type Code",
    ),
    c(
        "organization_name",
        "STRING",
        "Razão social da organização prestadora (entidades do tipo 2)",
        "Legal business name of the provider organization (type 2 entities)",
        "Razón social de la organización proveedora (entidades de tipo 2)",
        orig="Provider Organization Name (Legal Business Name)",
    ),
    c(
        "last_name",
        "STRING",
        "Sobrenome legal do prestador pessoa física",
        "Legal last name of the individual provider",
        "Apellido legal del proveedor persona física",
        sens=True,
        orig="Provider Last Name (Legal Name)",
    ),
    c(
        "first_name",
        "STRING",
        "Primeiro nome do prestador pessoa física",
        "First name of the individual provider",
        "Nombre de pila del proveedor persona física",
        sens=True,
        orig="Provider First Name",
    ),
    c(
        "middle_name",
        "STRING",
        "Nome do meio do prestador pessoa física",
        "Middle name of the individual provider",
        "Segundo nombre del proveedor persona física",
        sens=True,
        orig="Provider Middle Name",
    ),
    c(
        "name_prefix",
        "STRING",
        "Prefixo do nome do prestador (por exemplo Dr., Mr., Ms.)",
        "Name prefix of the provider (for example Dr., Mr., Ms.)",
        "Prefijo del nombre del proveedor (por ejemplo Dr., Mr., Ms.)",
        orig="Provider Name Prefix Text",
    ),
    c(
        "name_suffix",
        "STRING",
        "Sufixo do nome do prestador (por exemplo Jr., Sr., III)",
        "Name suffix of the provider (for example Jr., Sr., III)",
        "Sufijo del nombre del proveedor (por ejemplo Jr., Sr., III)",
        orig="Provider Name Suffix Text",
    ),
    c(
        "credential",
        "STRING",
        "Credenciais profissionais declaradas pelo prestador (por exemplo MD, DO, RN)",
        "Professional credentials reported by the provider (for example MD, DO, RN)",
        "Credenciales profesionales declaradas por el proveedor (por ejemplo MD, DO, RN)",
        obs="Free text; not validated by CMS",
        orig="Provider Credential Text",
    ),
    c(
        "other_organization_name",
        "STRING",
        "Outro nome da organização prestadora, como nome fantasia ou razão "
        "social anterior",
        "Other name of the provider organization, such as a doing-business-as "
        "or former legal business name",
        "Otro nombre de la organización proveedora, como nombre comercial o "
        "razón social anterior",
        orig="Provider Other Organization Name",
    ),
    c(
        "other_organization_name_type_code",
        "STRING",
        "Tipo do outro nome da organização",
        "Type of the organization's other name",
        "Tipo del otro nombre de la organización",
        dict_=True,
        obs="Value 6 means the organization's other names are listed in the "
        "other_name table rather than in this column",
        orig="Provider Other Organization Name Type Code",
    ),
    c(
        "other_last_name",
        "STRING",
        "Outro sobrenome do prestador pessoa física",
        "Other last name of the individual provider",
        "Otro apellido del proveedor persona física",
        sens=True,
        orig="Provider Other Last Name",
    ),
    c(
        "other_first_name",
        "STRING",
        "Outro primeiro nome do prestador pessoa física",
        "Other first name of the individual provider",
        "Otro nombre de pila del proveedor persona física",
        sens=True,
        orig="Provider Other First Name",
    ),
    c(
        "other_middle_name",
        "STRING",
        "Outro nome do meio do prestador pessoa física",
        "Other middle name of the individual provider",
        "Otro segundo nombre del proveedor persona física",
        sens=True,
        orig="Provider Other Middle Name",
    ),
    c(
        "other_name_prefix",
        "STRING",
        "Prefixo do outro nome do prestador",
        "Name prefix of the provider's other name",
        "Prefijo del otro nombre del proveedor",
        orig="Provider Other Name Prefix Text",
    ),
    c(
        "other_name_suffix",
        "STRING",
        "Sufixo do outro nome do prestador",
        "Name suffix of the provider's other name",
        "Sufijo del otro nombre del proveedor",
        orig="Provider Other Name Suffix Text",
    ),
    c(
        "other_credential",
        "STRING",
        "Credenciais associadas ao outro nome do prestador",
        "Credentials associated with the provider's other name",
        "Credenciales asociadas al otro nombre del proveedor",
        orig="Provider Other Credential Text",
    ),
    c(
        "other_last_name_type_code",
        "STRING",
        "Tipo do outro sobrenome do prestador pessoa física",
        "Type of the individual provider's other last name",
        "Tipo del otro apellido del proveedor persona física",
        dict_=True,
        orig="Provider Other Last Name Type Code",
    ),
    c(
        "sex_code",
        "STRING",
        "Sexo declarado do prestador pessoa física",
        "Reported sex of the individual provider",
        "Sexo declarado del proveedor persona física",
        dict_=True,
        orig="Provider Sex Code",
    ),
    c(
        "is_sole_proprietor",
        "STRING",
        "Indica se o prestador pessoa física é um empresário individual",
        "Whether the individual provider is a sole proprietor",
        "Indica si el proveedor persona física es un empresario individual",
        dict_=True,
        orig="Is Sole Proprietor",
    ),
    c(
        "is_organization_subpart",
        "STRING",
        "Indica se a organização é uma subparte de outra organização",
        "Whether the organization is a subpart of another organization",
        "Indica si la organización es una subparte de otra organización",
        dict_=True,
        orig="Is Organization Subpart",
    ),
    c(
        "parent_organization_legal_business_name",
        "STRING",
        "Razão social da organização controladora, quando a organização é uma "
        "subparte",
        "Legal business name of the parent organization, when the organization "
        "is a subpart",
        "Razón social de la organización matriz, cuando la organización es una "
        "subparte",
        orig="Parent Organization LBN",
    ),
    *addr_block(
        "mailing",
        "de correspondência do prestador",
        "provider business mailing",
        "postal del proveedor",
        "Business Mailing Address",
    ),
    *addr_block(
        "practice",
        "do local de atendimento principal do prestador",
        "provider primary practice location",
        "del lugar de atención principal del proveedor",
        "Business Practice Location Address",
    ),
    c(
        "primary_taxonomy_code",
        "STRING",
        "Código de taxonomia primária do prestador, isto é, a taxonomia marcada "
        "como primária entre as até 15 informadas",
        "Primary taxonomy code of the provider, that is, the taxonomy flagged as "
        "primary among the up to 15 reported",
        "Código de taxonomía primaria del proveedor, es decir, la taxonomía "
        "marcada como primaria entre las hasta 15 informadas",
        obs="Derived: the Healthcare Provider Taxonomy Code whose primary switch "
        "is Y. Code labels are published by NUCC and are not redistributed here",
        orig="Healthcare Provider Taxonomy Code_1..15 (where switch = Y)",
    ),
    c(
        "authorized_official_last_name",
        "STRING",
        "Sobrenome do responsável autorizado pela organização",
        "Last name of the organization's authorized official",
        "Apellido del responsable autorizado de la organización",
        sens=True,
        orig="Authorized Official Last Name",
    ),
    c(
        "authorized_official_first_name",
        "STRING",
        "Primeiro nome do responsável autorizado pela organização",
        "First name of the organization's authorized official",
        "Nombre de pila del responsable autorizado de la organización",
        sens=True,
        orig="Authorized Official First Name",
    ),
    c(
        "authorized_official_middle_name",
        "STRING",
        "Nome do meio do responsável autorizado pela organização",
        "Middle name of the organization's authorized official",
        "Segundo nombre del responsable autorizado de la organización",
        sens=True,
        orig="Authorized Official Middle Name",
    ),
    c(
        "authorized_official_name_prefix",
        "STRING",
        "Prefixo do nome do responsável autorizado",
        "Name prefix of the authorized official",
        "Prefijo del nombre del responsable autorizado",
        orig="Authorized Official Name Prefix Text",
    ),
    c(
        "authorized_official_name_suffix",
        "STRING",
        "Sufixo do nome do responsável autorizado",
        "Name suffix of the authorized official",
        "Sufijo del nombre del responsable autorizado",
        orig="Authorized Official Name Suffix Text",
    ),
    c(
        "authorized_official_credential",
        "STRING",
        "Credenciais do responsável autorizado",
        "Credentials of the authorized official",
        "Credenciales del responsable autorizado",
        orig="Authorized Official Credential Text",
    ),
    c(
        "authorized_official_title_or_position",
        "STRING",
        "Cargo ou função do responsável autorizado na organização",
        "Title or position of the authorized official in the organization",
        "Cargo o función del responsable autorizado en la organización",
        orig="Authorized Official Title or Position",
    ),
    c(
        "authorized_official_telephone_number",
        "STRING",
        "Telefone do responsável autorizado",
        "Telephone number of the authorized official",
        "Teléfono del responsable autorizado",
        sens=True,
        orig="Authorized Official Telephone Number",
    ),
    c(
        "enumeration_date",
        "DATE",
        "Data em que o NPI foi atribuído ao prestador",
        "Date the NPI was assigned to the provider",
        "Fecha en que se asignó el NPI al proveedor",
        orig="Provider Enumeration Date",
    ),
    c(
        "last_update_date",
        "DATE",
        "Data da última atualização do registro do prestador no NPPES",
        "Date the provider's NPPES record was last updated",
        "Fecha de la última actualización del registro del proveedor en el NPPES",
        orig="Last Update Date",
    ),
    c(
        "certification_date",
        "DATE",
        "Data em que o prestador certificou pela última vez que os dados do "
        "registro estão corretos",
        "Date the provider last certified that the record's data are correct",
        "Fecha en que el proveedor certificó por última vez que los datos del "
        "registro son correctos",
        orig="Certification Date",
    ),
    c(
        "deactivation_date",
        "DATE",
        "Data em que o NPI foi desativado",
        "Date the NPI was deactivated",
        "Fecha en que se desactivó el NPI",
        obs="Deactivated NPIs that were not reactivated carry only npi and this "
        "date; every other column is blank",
        orig="NPI Deactivation Date",
    ),
    c(
        "reactivation_date",
        "DATE",
        "Data em que o NPI desativado voltou a ser ativo",
        "Date the deactivated NPI was reactivated",
        "Fecha en que el NPI desactivado volvió a estar activo",
        orig="NPI Reactivation Date",
    ),
]

TAXONOMY = [
    EXTRACTION_DATE,
    NPI,
    c(
        "taxonomy_sequence",
        "STRING",
        "Posição da taxonomia na lista de até 15 taxonomias informadas pelo "
        "prestador",
        "Position of the taxonomy in the list of up to 15 taxonomies reported by "
        "the provider",
        "Posición de la taxonomía en la lista de hasta 15 taxonomías informadas "
        "por el proveedor",
        obs="Slot number 1-15 in the source's repeating group; ordering is the "
        "source's, not a ranking",
        orig="(repeating group index)",
    ),
    c(
        "taxonomy_code",
        "STRING",
        "Código de taxonomia de prestador de serviços de saúde, de 10 caracteres, "
        "que identifica a especialidade ou o tipo de prestador",
        "Ten-character health care provider taxonomy code identifying the "
        "provider's specialty or type",
        "Código de taxonomía de proveedor de servicios de salud, de 10 caracteres, "
        "que identifica la especialidad o el tipo de proveedor",
        obs="Code set is maintained by NUCC and licensed by the AMA; the labels "
        "are not redistributed in this dataset",
        orig="Healthcare Provider Taxonomy Code_1..15",
    ),
    c(
        "is_primary_taxonomy",
        "STRING",
        "Indica se esta é a taxonomia primária do prestador; há no máximo uma "
        "por NPI",
        "Whether this is the provider's primary taxonomy; at most one per NPI",
        "Indica si esta es la taxonomía primaria del proveedor; hay como máximo "
        "una por NPI",
        dict_=True,
        orig="Healthcare Provider Primary Taxonomy Switch_1..15",
    ),
    c(
        "license_number",
        "STRING",
        "Número da licença profissional associada a esta taxonomia",
        "Professional license number associated with this taxonomy",
        "Número de licencia profesional asociada a esta taxonomía",
        sens=True,
        obs="CMS masks values reported as SSN, ITIN or EIN; those masks are "
        "mapped to null here",
        orig="Provider License Number_1..15",
    ),
    c(
        "license_state_code",
        "STRING",
        "Sigla de duas letras do estado que emitiu a licença profissional",
        "Two-letter abbreviation of the state that issued the professional license",
        "Sigla de dos letras del estado que emitió la licencia profesional",
        dir_=DIR_STATE,
        orig="Provider License Number State Code_1..15",
    ),
    c(
        "taxonomy_group_code",
        "STRING",
        "Código do grupo de taxonomia declarado pela organização",
        "Taxonomy group code reported by the organization",
        "Código del grupo de taxonomía declarado por la organización",
        dict_=True,
        obs="Split from the source field, which concatenates code and label",
        orig="Healthcare Provider Taxonomy Group_1..15",
    ),
    c(
        "taxonomy_group_name",
        "STRING",
        "Descrição do grupo de taxonomia exatamente como declarada na fonte",
        "Taxonomy group label exactly as reported in the source",
        "Descripción del grupo de taxonomía exactamente como se declara en la fuente",
        obs="Free text; the same code appears with more than one wording",
        orig="Healthcare Provider Taxonomy Group_1..15",
    ),
]

OTHER_IDENTIFIER = [
    EXTRACTION_DATE,
    NPI,
    c(
        "identifier_sequence",
        "STRING",
        "Posição do identificador na lista de até 50 outros identificadores "
        "informados pelo prestador",
        "Position of the identifier in the list of up to 50 other identifiers "
        "reported by the provider",
        "Posición del identificador en la lista de hasta 50 otros identificadores "
        "informados por el proveedor",
        obs="Slot number 1-50 in the source's repeating group",
        orig="(repeating group index)",
    ),
    c(
        "other_identifier",
        "STRING",
        "Outro identificador do prestador atribuído por um plano de saúde, "
        "anterior ou paralelo ao NPI",
        "Other provider identifier assigned by a health plan, predating or "
        "running alongside the NPI",
        "Otro identificador del proveedor asignado por un plan de salud, anterior "
        "o paralelo al NPI",
        sens=True,
        obs="CMS masks values reported as SSN, ITIN or EIN; those masks are "
        "mapped to null here",
        orig="Other Provider Identifier_1..50",
    ),
    c(
        "other_identifier_type_code",
        "STRING",
        "Tipo do emissor do outro identificador",
        "Type of the other identifier's issuer",
        "Tipo del emisor del otro identificador",
        dict_=True,
        orig="Other Provider Identifier Type Code_1..50",
    ),
    c(
        "other_identifier_state_code",
        "STRING",
        "Sigla de duas letras do estado do plano Medicaid emissor, quando aplicável",
        "Two-letter abbreviation of the issuing Medicaid plan's state, when "
        "applicable",
        "Sigla de dos letras del estado del plan Medicaid emisor, cuando "
        "corresponde",
        dir_=DIR_STATE,
        orig="Other Provider Identifier State_1..50",
    ),
    c(
        "other_identifier_issuer",
        "STRING",
        "Nome do plano de saúde que emitiu o outro identificador",
        "Name of the health plan that issued the other identifier",
        "Nombre del plan de salud que emitió el otro identificador",
        orig="Other Provider Identifier Issuer_1..50",
    ),
]

OTHER_NAME = [
    EXTRACTION_DATE,
    NPI,
    c(
        "other_organization_name",
        "STRING",
        "Outro nome associado à organização prestadora",
        "Other name associated with the provider organization",
        "Otro nombre asociado a la organización proveedora",
        orig="Provider Other Organization Name",
    ),
    c(
        "other_organization_name_type_code",
        "STRING",
        "Tipo do outro nome da organização",
        "Type of the organization's other name",
        "Tipo del otro nombre de la organización",
        dict_=True,
        orig="Provider Other Organization Name Type Code",
    ),
    c(
        "created_date",
        "DATE",
        "Data em que o outro nome foi registrado no NPPES",
        "Date the other name was recorded in NPPES",
        "Fecha en que se registró el otro nombre en el NPPES",
        orig="Created Date",
    ),
]

PRACTICE_LOCATION = [
    EXTRACTION_DATE,
    NPI,
    c(
        "address_line_1",
        "STRING",
        "Primeira linha do endereço do local de atendimento secundário",
        "First line of the secondary practice location address",
        "Primera línea de la dirección del lugar de atención secundario",
        sens=True,
        orig="Provider Secondary Practice Location Address- Address Line 1",
    ),
    c(
        "address_line_2",
        "STRING",
        "Segunda linha do endereço do local de atendimento secundário",
        "Second line of the secondary practice location address",
        "Segunda línea de la dirección del lugar de atención secundario",
        sens=True,
        orig="Provider Secondary Practice Location Address-  Address Line 2",
    ),
    c(
        "address_city",
        "STRING",
        "Município do local de atendimento secundário",
        "City of the secondary practice location",
        "Ciudad del lugar de atención secundario",
        orig="Provider Secondary Practice Location Address - City Name",
    ),
    c(
        "address_state",
        "STRING",
        "Sigla de duas letras do estado do local de atendimento secundário",
        "Two-letter state abbreviation of the secondary practice location",
        "Sigla de dos letras del estado del lugar de atención secundario",
        dir_=DIR_STATE,
        orig="Provider Secondary Practice Location Address - State Name",
    ),
    c(
        "address_postal_code",
        "STRING",
        "Código postal (ZIP) do local de atendimento secundário, de 5 ou 9 dígitos",
        "Postal (ZIP) code of the secondary practice location, 5 or 9 digits",
        "Código postal (ZIP) del lugar de atención secundario, de 5 o 9 dígitos",
        orig="Provider Secondary Practice Location Address - Postal Code",
    ),
    c(
        "address_country_code",
        "STRING",
        "Código ISO de duas letras do país do local de atendimento secundário",
        "Two-letter ISO country code of the secondary practice location",
        "Código ISO de dos letras del país del lugar de atención secundario",
        dict_=True,
        orig="Provider Secondary Practice Location Address - Country Code "
        "(If outside U.S.)",
    ),
    c(
        "telephone_number",
        "STRING",
        "Telefone do local de atendimento secundário",
        "Telephone number of the secondary practice location",
        "Teléfono del lugar de atención secundario",
        sens=True,
        orig="Provider Secondary Practice Location Address - Telephone Number",
    ),
    c(
        "telephone_extension",
        "STRING",
        "Ramal telefônico do local de atendimento secundário",
        "Telephone extension of the secondary practice location",
        "Extensión telefónica del lugar de atención secundario",
        sens=True,
        orig="Provider Secondary Practice Location Address - Telephone Extension",
    ),
    c(
        "fax_number",
        "STRING",
        "Fax do local de atendimento secundário",
        "Fax number of the secondary practice location",
        "Fax del lugar de atención secundario",
        sens=True,
        orig="Provider Practice Location Address - Fax Number",
    ),
]

ENDPOINT = [
    EXTRACTION_DATE,
    NPI,
    c(
        "endpoint_type",
        "STRING",
        "Tipo do endpoint eletrônico de troca de informações de saúde",
        "Type of the electronic health information exchange endpoint",
        "Tipo del endpoint electrónico de intercambio de información de salud",
        dict_=True,
        orig="Endpoint Type",
    ),
    # Named `endpoint_address`, not `endpoint`: a column sharing its table's
    # name resolves to the table's row struct in BigQuery when referenced
    # unqualified, which silently neuters every dbt test that names it — the
    # not_null and uniqueness tests both passed vacuously before the rename.
    c(
        "endpoint_address",
        "STRING",
        "Endereço do endpoint eletrônico, como um endereço Direct ou uma URL de "
        "serviço",
        "Address of the electronic endpoint, such as a Direct address or a "
        "service URL",
        "Dirección del endpoint electrónico, como una dirección Direct o una URL "
        "de servicio",
        orig="Endpoint",
    ),
    c(
        "endpoint_description",
        "STRING",
        "Descrição livre do endpoint informada pelo prestador",
        "Free-text description of the endpoint reported by the provider",
        "Descripción libre del endpoint informada por el proveedor",
        orig="Endpoint Description",
    ),
    c(
        "use_code",
        "STRING",
        "Finalidade de uso do endpoint",
        "Intended use of the endpoint",
        "Finalidad de uso del endpoint",
        dict_=True,
        orig="Use Code",
    ),
    c(
        "other_use_description",
        "STRING",
        "Descrição da finalidade de uso quando o código de uso é OTHER",
        "Description of the intended use when the use code is OTHER",
        "Descripción de la finalidad de uso cuando el código de uso es OTHER",
        orig="Other Use Description",
    ),
    c(
        "content_type",
        "STRING",
        "Tipo de conteúdo trocado pelo endpoint",
        "Type of content exchanged through the endpoint",
        "Tipo de contenido intercambiado por el endpoint",
        dict_=True,
        orig="Content Type",
    ),
    c(
        "other_content_description",
        "STRING",
        "Descrição do tipo de conteúdo quando o tipo de conteúdo é OTHER",
        "Description of the content type when the content type is OTHER",
        "Descripción del tipo de contenido cuando el tipo de contenido es OTHER",
        orig="Other Content Description",
    ),
    c(
        "affiliation",
        "STRING",
        "Indica se o endpoint pertence a outra organização à qual o prestador é "
        "afiliado",
        "Whether the endpoint belongs to another organization the provider is "
        "affiliated with",
        "Indica si el endpoint pertenece a otra organización a la que el "
        "proveedor está afiliado",
        dict_=True,
        orig="Affiliation",
    ),
    c(
        "affiliation_legal_business_name",
        "STRING",
        "Razão social da organização afiliada dona do endpoint",
        "Legal business name of the affiliated organization that owns the endpoint",
        "Razón social de la organización afiliada propietaria del endpoint",
        orig="Affiliation Legal Business Name",
    ),
    c(
        "affiliation_address_line_1",
        "STRING",
        "Primeira linha do endereço da organização afiliada",
        "First line of the affiliated organization's address",
        "Primera línea de la dirección de la organización afiliada",
        orig="Affiliation Address Line One",
    ),
    c(
        "affiliation_address_line_2",
        "STRING",
        "Segunda linha do endereço da organização afiliada",
        "Second line of the affiliated organization's address",
        "Segunda línea de la dirección de la organización afiliada",
        orig="Affiliation Address Line Two",
    ),
    c(
        "affiliation_address_city",
        "STRING",
        "Município da organização afiliada",
        "City of the affiliated organization",
        "Ciudad de la organización afiliada",
        orig="Affiliation Address City",
    ),
    c(
        "affiliation_address_state",
        "STRING",
        "Sigla de duas letras do estado da organização afiliada",
        "Two-letter state abbreviation of the affiliated organization",
        "Sigla de dos letras del estado de la organización afiliada",
        dir_=DIR_STATE,
        orig="Affiliation Address State",
    ),
    c(
        "affiliation_address_postal_code",
        "STRING",
        "Código postal da organização afiliada",
        "Postal code of the affiliated organization",
        "Código postal de la organización afiliada",
        orig="Affiliation Address Postal Code",
    ),
    c(
        "affiliation_address_country_code",
        "STRING",
        "Código do país da organização afiliada",
        "Country code of the affiliated organization",
        "Código del país de la organización afiliada",
        dict_=True,
        orig="Affiliation Address Country",
    ),
]

DICIONARIO = [
    c(
        "id_tabela",
        "STRING",
        "Slug da tabela de us_hhs_nppes que a entrada do dicionário descreve",
        "Slug of the us_hhs_nppes table the dictionary entry describes",
        "Slug de la tabla de us_hhs_nppes que describe la entrada del diccionario",
    ),
    c(
        "nome_coluna",
        "STRING",
        "Nome da coluna que a entrada do dicionário descreve",
        "Name of the column the dictionary entry describes",
        "Nombre de la columna que describe la entrada del diccionario",
    ),
    c(
        "chave",
        "STRING",
        "Valor codificado (chave) exatamente como armazenado nos dados",
        "Coded value (key) exactly as stored in the data",
        "Valor codificado (clave) exactamente como se almacena en los datos",
    ),
    c(
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal da entrada do dicionário",
        "Temporal coverage of the dictionary entry",
        "Cobertura temporal de la entrada del diccionario",
    ),
    c(
        "valor",
        "STRING",
        "Descrição legível do valor codificado",
        "Human-readable description of the coded value",
        "Descripción legible del valor codificado",
    ),
]

TABLES = {
    "provider": PROVIDER,
    "taxonomy": TAXONOMY,
    "other_identifier": OTHER_IDENTIFIER,
    "other_name": OTHER_NAME,
    "practice_location": PRACTICE_LOCATION,
    "endpoint": ENDPOINT,
    "dicionario": DICIONARIO,
}


def _json_row(x: dict) -> dict:
    """One bulk_upsert_columns row: everything the backend can store."""
    obs_pt, obs_en, obs_es = _obs(x["observations"])
    row = {
        "name": x["name"],
        "bigquery_type": x["bigquery_type"],
        "description_pt": x["description_pt"],
        "description_en": x["description_en"],
        "description_es": x["description_es"],
        "covered_by_dictionary": x["covered_by_dictionary"],
        "has_sensitive_data": x["has_sensitive_data"],
    }
    if x["directory_column"]:
        row["directory_column"] = x["directory_column"]
    if x["measurement_unit"]:
        row["measurement_unit"] = x["measurement_unit"]
    if obs_pt:
        row["observations_pt"] = obs_pt
        row["observations_en"] = obs_en
        row["observations_es"] = obs_es
    return row


def main():
    JSON_DIR.mkdir(parents=True, exist_ok=True)
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    for table, cols in TABLES.items():
        names = [x["name"] for x in cols]
        assert len(names) == len(set(names)), (
            f"{table}: duplicate column names"
        )
        with open(JSON_DIR / f"{table}.json", "w", encoding="utf-8") as fh:
            json.dump(
                [_json_row(x) for x in cols], fh, ensure_ascii=False, indent=1
            )
            fh.write("\n")
        with open(
            ARCH_DIR / f"{table}.csv", "w", newline="", encoding="utf-8"
        ) as fh:
            w = csv.writer(fh)
            w.writerow(ARCH_HEADER)
            for x in cols:
                w.writerow(
                    [
                        x["name"],
                        x["bigquery_type"],
                        x["description_pt"],
                        "",
                        "yes" if x["covered_by_dictionary"] else "no",
                        x["directory_column"],
                        x["measurement_unit"],
                        "yes" if x["has_sensitive_data"] else "no",
                        x["observations"],
                        x["original_name"],
                    ]
                )
        print(f"{table}: {len(cols)} columns")


if __name__ == "__main__":
    main()
