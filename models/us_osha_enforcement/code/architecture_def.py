"""Architecture for ``us_osha_enforcement`` — the single source of truth.

Everything downstream (cleaning schema, architecture CSVs, dbt models,
``schema.yml`` and the backend ``columns_json``) is generated from this module,
so the architecture cannot drift from the code that writes the data.

Columns are declared as tuples::

    (name, bigquery_type, source_column, description_pt, description_en,
     description_es, extra)

``source_column`` is the uppercase column name in the DOL bulk CSV, or ``None``
for a column derived during cleaning (``year``) or reassembled from several
source rows (``narrative``, ``text``).  ``extra`` is an optional dict carrying
``dict`` (covered_by_dictionary), ``dir`` (directory_column), ``unit``
(measurement_unit), ``obs`` (observations) and ``sensitive``.

Column order follows the Data Basis style manual: partition columns first, then
identifiers, then descriptive columns.  Types follow arithmetic meaning: a
numeric-looking code with no sensible unit is STRING.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# --------------------------------------------------------------------------- #
# scaffolding
# --------------------------------------------------------------------------- #


@dataclass
class Col:
    name: str
    bigquery_type: str
    src: str | None
    description: str
    description_en: str
    description_es: str
    temporal_coverage: str = ""
    covered_by_dictionary: str = "no"
    directory_column: str = ""
    measurement_unit: str = ""
    has_sensitive_data: str = "no"
    observations: str = ""

    @property
    def original_name(self) -> str:
        return self.src or ""


def c(
    name, btype, src, pt, en, es, extra: dict[str, Any] | None = None
) -> Col:
    extra = extra or {}
    return Col(
        name=name,
        bigquery_type=btype,
        src=src,
        description=pt,
        description_en=en,
        description_es=es,
        covered_by_dictionary="yes" if extra.get("dict") else "no",
        directory_column=extra.get("dir", ""),
        measurement_unit=extra.get("unit", ""),
        has_sensitive_data="yes" if extra.get("sensitive") else "no",
        observations=extra.get("obs", ""),
    )


@dataclass
class Table:
    slug: str
    name_pt: str
    name_en: str
    name_es: str
    description_pt: str
    description_en: str
    description_es: str
    source_file: str
    year_from: str  # "inspection" | "accident" | "" (unpartitioned)
    primary_key: list[str]
    columns: list[Col] = field(default_factory=list)
    reassembled: bool = False

    @property
    def partition(self) -> list[str]:
        return ["year"] if self.year_from else []


# --------------------------------------------------------------------------- #
# shared columns
# --------------------------------------------------------------------------- #

TIME_DIR = "br_bd_diretorios_data_tempo.ano:ano"

YEAR_INSP = c(
    "year",
    "INT64",
    None,
    "Ano de abertura da inspeção associada",
    "Year the associated inspection was opened",
    "Año de apertura de la inspección asociada",
    {
        "unit": "year",
        "dir": TIME_DIR,
        "obs": "Derivado de inspection.open_date. Registros cujo inspection_id "
        "não existe em inspection recebem year = 9999",
    },
)

YEAR_ACC = c(
    "year",
    "INT64",
    None,
    "Ano do acidente",
    "Year of the incident",
    "Año del accidente",
    {
        "unit": "year",
        "dir": TIME_DIR,
        "obs": "Derivado de accident.event_date; quando o acidente não consta em "
        "accident, derivado da data de abertura da inspeção associada",
    },
)

INSP_ID = lambda: c(  # noqa: E731
    "inspection_id",
    "STRING",
    "ACTIVITY_NR",
    "Identificador único da inspeção (activity number) da OSHA",
    "Unique OSHA inspection identifier (activity number)",
    "Identificador único de la inspección (activity number) de OSHA",
)

CITATION_ID = lambda: c(  # noqa: E731
    "citation_id",
    "STRING",
    "CITATION_ID",
    "Identificador da citação: número da citação, do item e do grupo do item",
    "Citation identifier: citation number, item number and item group",
    "Identificador de la citación: número de citación, de ítem y de grupo",
)


# --------------------------------------------------------------------------- #
# 1. inspection
# --------------------------------------------------------------------------- #

INSPECTION = Table(
    slug="inspection",
    name_pt="Inspeção",
    name_en="Inspection",
    name_es="Inspección",
    description_pt=(
        "Uma linha por inspeção da OSHA (federal ou de plano estadual), "
        "identificada pelo activity number. Contém o estabelecimento "
        "inspecionado e seu endereço, a classificação industrial (SIC e "
        "NAICS), o motivo e o escopo da inspeção, a representação sindical e "
        "as datas de abertura e encerramento do caso. Cobre 1972 a 2026."
    ),
    description_en=(
        "One row per OSHA inspection (federal or state-plan), identified by "
        "its activity number. Carries the inspected establishment and its "
        "address, the industry classification (SIC and NAICS), the reason for "
        "and scope of the inspection, union representation, and the case open "
        "and close dates. Covers 1972 to 2026."
    ),
    description_es=(
        "Una fila por inspección de OSHA (federal o de plan estatal), "
        "identificada por su activity number. Contiene el establecimiento "
        "inspeccionado y su dirección, la clasificación industrial (SIC y "
        "NAICS), el motivo y el alcance de la inspección, la representación "
        "sindical y las fechas de apertura y cierre del caso. Cubre 1972 a 2026."
    ),
    source_file="OSHA_inspection",
    year_from="open_date",
    primary_key=["inspection_id"],
    columns=[
        c(
            "year",
            "INT64",
            None,
            "Ano de abertura da inspeção",
            "Year the inspection was opened",
            "Año de apertura de la inspección",
            {"unit": "year", "dir": TIME_DIR, "obs": "Derivado de open_date"},
        ),
        INSP_ID(),
        c(
            "reporting_office_id",
            "STRING",
            "REPORTING_ID",
            "Código da jurisdição da OSHA (escritório federal ou plano estadual) responsável pela inspeção",
            "Code of the OSHA reporting jurisdiction (federal office or state plan) responsible for the inspection",
            "Código de la jurisdicción de OSHA (oficina federal o plan estatal) responsable de la inspección",
            {
                "obs": "Preserva zeros à esquerda (ex.: 0111100), por isso STRING"
            },
        ),
        c(
            "establishment_key",
            "STRING",
            "HOST_EST_KEY",
            "Chave interna do estabelecimento no sistema IMIS da OSHA",
            "Internal OSHA IMIS establishment key",
            "Clave interna del establecimiento en el sistema IMIS de OSHA",
            {
                "obs": "Preenchida em 58,6% das linhas; contém espaços internos e não é um identificador estável entre anos"
            },
        ),
        c(
            "establishment_name",
            "STRING",
            "ESTAB_NAME",
            "Nome do estabelecimento inspecionado",
            "Name of the inspected establishment",
            "Nombre del establecimiento inspeccionado",
            {
                "obs": "Para autônomos e microempresas o nome do estabelecimento é o nome da pessoa física citada"
            },
        ),
        c(
            "site_address",
            "STRING",
            "SITE_ADDRESS",
            "Logradouro do local inspecionado",
            "Street address of the inspected site",
            "Dirección del sitio inspeccionado",
        ),
        c(
            "site_city",
            "STRING",
            "SITE_CITY",
            "Município do local inspecionado",
            "City of the inspected site",
            "Ciudad del sitio inspeccionado",
        ),
        c(
            "site_state",
            "STRING",
            "SITE_STATE",
            "Sigla postal do estado do local inspecionado",
            "State postal abbreviation of the inspected site",
            "Abreviatura postal del estado del sitio inspeccionado",
            {
                "obs": "63 valores distintos: as 50 siglas USPS, DC, territórios (PR, VI, GU, AS, MP) e códigos históricos hoje extintos (CZ, JQ, PI, UK, FN, MQ). Por isso não há vínculo com o diretório de estados"
            },
        ),
        c(
            "site_zip_code",
            "STRING",
            "SITE_ZIP",
            "CEP (ZIP code) de cinco dígitos do local inspecionado",
            "Five-digit ZIP code of the inspected site",
            "Código postal (ZIP) de cinco dígitos del sitio inspeccionado",
            {"obs": "00000 é usado como sentinela de ausência"},
        ),
        c(
            "mailing_address",
            "STRING",
            "MAIL_STREET",
            "Logradouro do endereço de correspondência do empregador",
            "Street of the employer mailing address",
            "Dirección postal del empleador",
        ),
        c(
            "mailing_city",
            "STRING",
            "MAIL_CITY",
            "Município do endereço de correspondência do empregador",
            "City of the employer mailing address",
            "Ciudad de la dirección postal del empleador",
        ),
        c(
            "mailing_state",
            "STRING",
            "MAIL_STATE",
            "Sigla postal do estado do endereço de correspondência do empregador",
            "State postal abbreviation of the employer mailing address",
            "Abreviatura postal del estado de la dirección postal del empleador",
            {"obs": "Contém valores fora do padrão USPS (ex.: AC, XX)"},
        ),
        c(
            "mailing_zip_code",
            "STRING",
            "MAIL_ZIP",
            "CEP (ZIP code) do endereço de correspondência do empregador",
            "ZIP code of the employer mailing address",
            "Código postal (ZIP) de la dirección postal del empleador",
        ),
        c(
            "sic_code",
            "STRING",
            "SIC_CODE",
            "Código SIC de quatro dígitos do estabelecimento, na versão de 1987 do manual",
            "Four-digit SIC code of the establishment, 1987 edition of the manual",
            "Código SIC de cuatro dígitos del establecimiento, edición 1987 del manual",
            {
                "obs": "Ausente em 17,5% das inspeções, sobretudo após a migração para NAICS; 5.027 valores têm apenas três dígitos"
            },
        ),
        c(
            "naics_code",
            "STRING",
            "NAICS_CODE",
            "Código NAICS de seis dígitos do estabelecimento",
            "Six-digit NAICS code of the establishment",
            "Código NAICS de seis dígitos del establecimiento",
            {
                "obs": "A fonte não informa a safra do NAICS, que muda a cada cinco anos; por isso não há vínculo com um diretório NAICS específico"
            },
        ),
        c(
            "owner_type",
            "STRING",
            "OWNER_TYPE",
            "Tipo de empregador: privado, governo local, governo estadual ou federal",
            "Type of employer: private, local government, state government or federal",
            "Tipo de empleador: privado, gobierno local, gobierno estatal o federal",
            {"dict": True},
        ),
        c(
            "owner_code",
            "STRING",
            "OWNER_CODE",
            "Código da agência federal empregadora, preenchido apenas quando owner_type indica governo federal",
            "Federal employing agency code, populated only when owner_type indicates federal government",
            "Código de la agencia federal empleadora, solo cuando owner_type indica gobierno federal",
        ),
        c(
            "inspection_type",
            "STRING",
            "INSP_TYPE",
            "Motivo da inspeção: acidente, denúncia, encaminhamento, programada, entre outros",
            "Reason for the inspection: accident, complaint, referral, planned, among others",
            "Motivo de la inspección: accidente, denuncia, derivación, programada, entre otros",
            {"dict": True},
        ),
        c(
            "inspection_scope",
            "STRING",
            "INSP_SCOPE",
            "Escopo da inspeção: completa, parcial, apenas registros ou inspeção não realizada",
            "Scope of the inspection: complete, partial, records only, or no inspection",
            "Alcance de la inspección: completa, parcial, solo registros o sin inspección",
            {"dict": True},
        ),
        c(
            "no_inspection_reason",
            "STRING",
            "WHY_NO_INSP",
            "Motivo pelo qual a inspeção não foi realizada",
            "Reason the inspection was not carried out",
            "Motivo por el cual no se realizó la inspección",
            {"dict": True},
        ),
        c(
            "safety_health",
            "STRING",
            "SAFETY_HLTH",
            "Indica se a inspeção foi de segurança (S) ou de saúde ocupacional (H)",
            "Whether the inspection covered safety (S) or occupational health (H)",
            "Indica si la inspección fue de seguridad (S) o de salud ocupacional (H)",
            {"dict": True},
        ),
        c(
            "advance_notice",
            "STRING",
            "ADV_NOTICE",
            "Indica se houve aviso prévio ao empregador sobre a inspeção",
            "Whether the employer was given advance notice of the inspection",
            "Indica si se dio aviso previo al empleador sobre la inspección",
            {"dict": True},
        ),
        c(
            "union_status",
            "STRING",
            "UNION_STATUS",
            "Indica se havia representação sindical dos trabalhadores durante a inspeção",
            "Whether workers had union representation during the inspection",
            "Indica si había representación sindical de los trabajadores durante la inspección",
            {"dict": True},
        ),
        c(
            "safety_program_manufacturing",
            "STRING",
            "SAFETY_MANUF",
            "Indica uso do guia de planejamento de segurança para a indústria de transformação",
            "Whether the manufacturing safety planning guide was used",
            "Indica el uso de la guía de planificación de seguridad para manufactura",
            {"dict": True},
        ),
        c(
            "safety_program_construction",
            "STRING",
            "SAFETY_CONST",
            "Indica uso do guia de planejamento de segurança para a construção",
            "Whether the construction safety planning guide was used",
            "Indica el uso de la guía de planificación de seguridad para construcción",
            {"dict": True},
        ),
        c(
            "safety_program_maritime",
            "STRING",
            "SAFETY_MARIT",
            "Indica uso do guia de planejamento de segurança para o setor marítimo",
            "Whether the maritime safety planning guide was used",
            "Indica el uso de la guía de planificación de seguridad para el sector marítimo",
            {"dict": True},
        ),
        c(
            "health_program_manufacturing",
            "STRING",
            "HEALTH_MANUF",
            "Indica uso do guia de planejamento de saúde para a indústria de transformação",
            "Whether the manufacturing health planning guide was used",
            "Indica el uso de la guía de planificación de salud para manufactura",
            {"dict": True},
        ),
        c(
            "health_program_construction",
            "STRING",
            "HEALTH_CONST",
            "Indica uso do guia de planejamento de saúde para a construção",
            "Whether the construction health planning guide was used",
            "Indica el uso de la guía de planificación de salud para construcción",
            {"dict": True},
        ),
        c(
            "health_program_maritime",
            "STRING",
            "HEALTH_MARIT",
            "Indica uso do guia de planejamento de saúde para o setor marítimo",
            "Whether the maritime health planning guide was used",
            "Indica el uso de la guía de planificación de salud para el sector marítimo",
            {"dict": True},
        ),
        c(
            "migrant_labor",
            "STRING",
            "MIGRANT",
            "Indica inspeção de alojamento ou trabalho de mão de obra migrante",
            "Whether the inspection covered migrant labour housing or work",
            "Indica inspección de alojamiento o trabajo de mano de obra migrante",
            {"dict": True},
        ),
        c(
            "employees_in_establishment",
            "INT64",
            "NR_IN_ESTAB",
            "Número de empregados no estabelecimento inspecionado",
            "Number of employees at the inspected establishment",
            "Número de empleados en el establecimiento inspeccionado",
            {
                "unit": "employee",
                "obs": "Valores como 9999999999 são sentinelas de preenchimento da fonte e foram mantidos fielmente",
            },
        ),
        c(
            "open_date",
            "DATE",
            "OPEN_DATE",
            "Data de abertura da inspeção",
            "Date the inspection was opened",
            "Fecha de apertura de la inspección",
        ),
        c(
            "closing_conference_date",
            "DATE",
            "CLOSE_CONF_DATE",
            "Data da conferência de encerramento com o empregador",
            "Date of the closing conference with the employer",
            "Fecha de la conferencia de cierre con el empleador",
            {
                "obs": "Contém erros de digitação da fonte, incluindo datas futuras (máximo observado 2112-12-05)"
            },
        ),
        c(
            "close_case_date",
            "DATE",
            "CLOSE_CASE_DATE",
            "Data de encerramento do caso",
            "Date the case was closed",
            "Fecha de cierre del caso",
            {
                "obs": "Contém erros de digitação da fonte, incluindo anos de um a três dígitos (mínimo observado 0120-11-18)"
            },
        ),
        c(
            "case_modified_date",
            "DATE",
            "CASE_MOD_DATE",
            "Data da última alteração na inspeção ou em suas violações",
            "Date the inspection or its violations were last changed",
            "Fecha del último cambio en la inspección o sus violaciones",
            {
                "obs": "Só passou a ser registrada em abril de 2004; nula para casos não alterados desde então"
            },
        ),
    ],
)


# --------------------------------------------------------------------------- #
# 2. violation
# --------------------------------------------------------------------------- #

MOVING_PENALTY_PT = (
    "current_penalty é um valor móvel: a multa é contestada e revisada por anos "
    "após a citação, e só se estabiliza quando o caso é encerrado. Compare com "
    "initial_penalty e acompanhe o histórico completo em violation_event."
)
MOVING_PENALTY_EN = (
    "current_penalty is a moving figure: the penalty is contested and revised "
    "for years after the citation and only settles when the case closes. "
    "Compare it with initial_penalty and follow the full history in "
    "violation_event."
)
MOVING_PENALTY_ES = (
    "current_penalty es un valor móvil: la multa se impugna y revisa durante "
    "años tras la citación y solo se estabiliza al cerrarse el caso. Compárela "
    "con initial_penalty y siga el historial completo en violation_event."
)

VIOLATION = Table(
    slug="violation",
    name_pt="Violação",
    name_en="Violation",
    name_es="Violación",
    description_pt=(
        "Uma linha por citação emitida, identificada pelo par inspeção e "
        "citação. Contém a norma citada, a classificação da violação (grave, "
        "deliberada, reincidente, outra), as multas inicial e corrente, os "
        "prazos de correção e as datas de contestação e de ordem final. "
        + MOVING_PENALTY_PT
    ),
    description_en=(
        "One row per citation issued, identified by the inspection and "
        "citation pair. Carries the standard cited, the violation "
        "classification (serious, willful, repeat, other), the initial and "
        "current penalties, the abatement deadlines and the contest and final "
        "order dates. " + MOVING_PENALTY_EN
    ),
    description_es=(
        "Una fila por citación emitida, identificada por el par inspección y "
        "citación. Contiene la norma citada, la clasificación de la violación "
        "(grave, deliberada, reincidente, otra), las multas inicial y "
        "corriente, los plazos de corrección y las fechas de impugnación y de "
        "orden final. " + MOVING_PENALTY_ES
    ),
    source_file="OSHA_violation",
    year_from="inspection",
    primary_key=["inspection_id", "citation_id"],
    columns=[
        YEAR_INSP,
        INSP_ID(),
        CITATION_ID(),
        c(
            "standard",
            "STRING",
            "STANDARD",
            "Norma da OSHA citada, no formato interno do IMIS",
            "OSHA standard cited, in the internal IMIS format",
            "Norma de OSHA citada, en el formato interno de IMIS",
            {
                "obs": "Codificação posicional do IMIS: 19260501 C01 corresponde a 29 CFR 1926.501(c)(1). Não é um código com dicionário fechado"
            },
        ),
        c(
            "violation_type",
            "STRING",
            "VIOL_TYPE",
            "Classificação da violação: grave, deliberada, reincidente, outra ou não classificada",
            "Violation classification: serious, willful, repeat, other or unclassified",
            "Clasificación de la violación: grave, deliberada, reincidente, otra o no clasificada",
            {
                "dict": True,
                "obs": "O valor P aparece na fonte mas não consta na documentação publicada pela OSHA e por isso não recebe rótulo no dicionário",
            },
        ),
        c(
            "deleted",
            "STRING",
            "DELETE_FLAG",
            "Indica que a citação foi excluída (X)",
            "Whether the citation was deleted (X)",
            "Indica que la citación fue eliminada (X)",
            {
                "dict": True,
                "obs": "Marca 3,4% das citações. Elas são mantidas na tabela; filtre deleted is null para contar apenas citações vigentes",
            },
        ),
        c(
            "issuance_date",
            "DATE",
            "ISSUANCE_DATE",
            "Data de emissão da citação",
            "Date the citation was issued",
            "Fecha de emisión de la citación",
        ),
        c(
            "initial_penalty",
            "FLOAT64",
            "INITIAL_PENALTY",
            "Multa proposta na emissão da citação",
            "Penalty proposed when the citation was issued",
            "Multa propuesta al emitirse la citación",
            {
                "unit": "USD",
                "obs": "Nula em 49,4% das citações, que não têm multa proposta. Valores nominais, sem correção pela inflação",
            },
        ),
        c(
            "current_penalty",
            "FLOAT64",
            "CURRENT_PENALTY",
            "Multa corrente após contestações e acordos",
            "Current penalty after contests and settlements",
            "Multa corriente tras impugnaciones y acuerdos",
            {"unit": "USD", "obs": MOVING_PENALTY_PT},
        ),
        c(
            "abatement_due_date",
            "DATE",
            "ABATE_DATE",
            "Prazo para correção da condição citada",
            "Deadline to abate the cited condition",
            "Plazo para corregir la condición citada",
            {
                "obs": "Contém erros de digitação da fonte (mínimo observado 0210-06-16)"
            },
        ),
        c(
            "abatement_completion_code",
            "STRING",
            "ABATE_COMPLETE",
            "Código de situação da correção da condição citada",
            "Code for the abatement status of the cited condition",
            "Código de situación de la corrección de la condición citada",
            {
                "obs": "A OSHA não publica a legenda destes 16 códigos, que por isso não constam do dicionário"
            },
        ),
        c(
            "contest_date",
            "DATE",
            "CONTEST_DATE",
            "Data em que a citação foi contestada pelo empregador",
            "Date the citation was contested by the employer",
            "Fecha en que la citación fue impugnada por el empleador",
        ),
        c(
            "final_order_date",
            "DATE",
            "FINAL_ORDER_DATE",
            "Data da ordem final que encerra a citação",
            "Date of the final order closing the citation",
            "Fecha de la orden final que cierra la citación",
            {
                "obs": "Contém erros de digitação da fonte (mínimo observado 0018-05-30, máximo 2103-05-22)"
            },
        ),
        c(
            "instances",
            "INT64",
            "NR_INSTANCES",
            "Número de ocorrências da condição citada",
            "Number of instances of the cited condition",
            "Número de ocurrencias de la condición citada",
            {"unit": "instance"},
        ),
        c(
            "employees_exposed",
            "INT64",
            "NR_EXPOSED",
            "Número de empregados expostos à condição citada",
            "Number of employees exposed to the cited condition",
            "Número de empleados expuestos a la condición citada",
            {"unit": "employee"},
        ),
        c(
            "gravity",
            "STRING",
            "GRAVITY",
            "Nível de gravidade atribuído a violações graves",
            "Gravity level assigned to serious violations",
            "Nivel de gravedad asignado a violaciones graves",
            {
                "obs": "Larguras inconsistentes na fonte (0 e 00, 1 e 01). A OSHA não publica a legenda, que por isso não consta do dicionário"
            },
        ),
        c(
            "related_event_code",
            "STRING",
            "REC",
            "Códigos de eventos relacionados à citação, separados por ponto e vírgula",
            "Codes for events related to the citation, semicolon separated",
            "Códigos de eventos relacionados con la citación, separados por punto y coma",
            {
                "obs": "Multivalorado (ex.: A;C;I;R). A OSHA não publica a legenda dos códigos"
            },
        ),
        c(
            "emphasis_program",
            "STRING",
            "EMPHASIS",
            "Indica que a citação decorre de um programa de ênfase (X)",
            "Whether the citation arose from an emphasis program (X)",
            "Indica que la citación deriva de un programa de énfasis (X)",
            {"dict": True},
        ),
        c(
            "hazard_category",
            "STRING",
            "HAZCAT",
            "Categoria de risco da norma da indústria geral",
            "General industry standard hazard category",
            "Categoría de riesgo de la norma de la industria general",
            {
                "obs": "Preenchida em 0,4% das citações; contém o valor espúrio & na fonte"
            },
        ),
        c(
            "hazardous_substance_1",
            "STRING",
            "HAZSUB1",
            "Primeiro código de substância perigosa associada à citação",
            "First hazardous substance code associated with the citation",
            "Primer código de sustancia peligrosa asociada a la citación",
        ),
        c(
            "hazardous_substance_2",
            "STRING",
            "HAZSUB2",
            "Segundo código de substância perigosa associada à citação",
            "Second hazardous substance code associated with the citation",
            "Segundo código de sustancia peligrosa asociada a la citación",
        ),
        c(
            "hazardous_substance_3",
            "STRING",
            "HAZSUB3",
            "Terceiro código de substância perigosa associada à citação",
            "Third hazardous substance code associated with the citation",
            "Tercer código de sustancia peligrosa asociada a la citación",
        ),
        c(
            "hazardous_substance_4",
            "STRING",
            "HAZSUB4",
            "Quarto código de substância perigosa associada à citação",
            "Fourth hazardous substance code associated with the citation",
            "Cuarto código de sustancia peligrosa asociada a la citación",
        ),
        c(
            "hazardous_substance_5",
            "STRING",
            "HAZSUB5",
            "Quinto código de substância perigosa associada à citação",
            "Fifth hazardous substance code associated with the citation",
            "Quinto código de sustancia peligrosa asociada a la citación",
        ),
        c(
            "fta_inspection_id",
            "STRING",
            "FTA_INSP_NR",
            "Identificador da inspeção de falha em corrigir (failure to abate)",
            "Identifier of the failure-to-abate inspection",
            "Identificador de la inspección por falta de corrección (failure to abate)",
        ),
        c(
            "fta_issuance_date",
            "DATE",
            "FTA_ISSUANCE_DATE",
            "Data de emissão da citação por falha em corrigir",
            "Date the failure-to-abate citation was issued",
            "Fecha de emisión de la citación por falta de corrección",
        ),
        c(
            "fta_penalty",
            "FLOAT64",
            "FTA_PENALTY",
            "Multa por falha em corrigir",
            "Failure-to-abate penalty",
            "Multa por falta de corrección",
            {
                "unit": "USD",
                "obs": "Contém valores negativos na fonte (mínimo -1,00)",
            },
        ),
        c(
            "fta_contest_date",
            "DATE",
            "FTA_CONTEST_DATE",
            "Data de contestação da citação por falha em corrigir",
            "Date the failure-to-abate citation was contested",
            "Fecha de impugnación de la citación por falta de corrección",
        ),
        c(
            "fta_final_order_date",
            "DATE",
            "FTA_FINAL_ORDER_DATE",
            "Data da ordem final da citação por falha em corrigir",
            "Date of the final order on the failure-to-abate citation",
            "Fecha de la orden final de la citación por falta de corrección",
        ),
    ],
)


# --------------------------------------------------------------------------- #
# 3. violation_event
# --------------------------------------------------------------------------- #

VIOLATION_EVENT = Table(
    slug="violation_event",
    name_pt="Evento de violação",
    name_en="Violation event",
    name_es="Evento de violación",
    description_pt=(
        "Histórico de eventos de cada citação: contestações, acordos, revisões "
        "de multa e mudanças de prazo de correção. É o registro que explica "
        "por que violation.current_penalty difere de violation.initial_penalty "
        "e como a multa evoluiu até o encerramento do caso."
    ),
    description_en=(
        "Event history for each citation: contests, settlements, penalty "
        "revisions and changes to the abatement deadline. This is the record "
        "that explains why violation.current_penalty differs from "
        "violation.initial_penalty and how the penalty moved until the case "
        "closed."
    ),
    description_es=(
        "Historial de eventos de cada citación: impugnaciones, acuerdos, "
        "revisiones de multa y cambios de plazo de corrección. Es el registro "
        "que explica por qué violation.current_penalty difiere de "
        "violation.initial_penalty y cómo evolucionó la multa hasta el cierre "
        "del caso."
    ),
    source_file="OSHA_violation_event",
    year_from="inspection",
    primary_key=["inspection_id", "citation_id", "event_date", "event_code"],
    columns=[
        YEAR_INSP,
        INSP_ID(),
        CITATION_ID(),
        c(
            "penalty_or_fta",
            "STRING",
            "PEN_FTA",
            "Indica se o evento se refere à multa (P) ou à falha em corrigir (F)",
            "Whether the event concerns the penalty (P) or the failure to abate (F)",
            "Indica si el evento se refiere a la multa (P) o a la falta de corrección (F)",
            {"dict": True},
        ),
        c(
            "event_code",
            "STRING",
            "HIST_EVENT",
            "Código de classificação do evento",
            "Event classification code",
            "Código de clasificación del evento",
            {
                "obs": "A OSHA não publica a legenda destes 22 códigos, que por isso não constam do dicionário"
            },
        ),
        c(
            "event_date",
            "DATE",
            "HIST_DATE",
            "Data do evento",
            "Date of the event",
            "Fecha del evento",
            {
                "obs": "Contém erros de digitação da fonte (máximo observado 2106-05-02)"
            },
        ),
        c(
            "penalty",
            "FLOAT64",
            "HIST_PENALTY",
            "Multa avaliada no evento",
            "Penalty assessed at the event",
            "Multa evaluada en el evento",
            {
                "unit": "USD",
                "obs": "Contém valores negativos na fonte (mínimo -1)",
            },
        ),
        c(
            "abatement_date",
            "DATE",
            "HIST_ABATE_DATE",
            "Prazo de correção vigente após o evento; aplicável apenas quando penalty_or_fta é P",
            "Abatement deadline in force after the event; applies only when penalty_or_fta is P",
            "Plazo de corrección vigente tras el evento; solo aplica cuando penalty_or_fta es P",
            {
                "obs": "Contém erros de digitação da fonte (mínimo 0210-06-16, máximo 5015-08-28)"
            },
        ),
        c(
            "violation_type",
            "STRING",
            "HIST_VTYPE",
            "Classificação da violação no evento; aplicável apenas quando penalty_or_fta é F",
            "Violation classification at the event; applies only when penalty_or_fta is F",
            "Clasificación de la violación en el evento; solo aplica cuando penalty_or_fta es F",
            {"dict": True},
        ),
        c(
            "fta_inspection_id",
            "STRING",
            "HIST_INSP_NR",
            "Identificador da inspeção por falha em corrigir; aplicável apenas quando penalty_or_fta é F",
            "Failure-to-abate inspection identifier; applies only when penalty_or_fta is F",
            "Identificador de la inspección por falta de corrección; solo aplica cuando penalty_or_fta es F",
        ),
    ],
)


# --------------------------------------------------------------------------- #
# 4. violation_text
# --------------------------------------------------------------------------- #

VIOLATION_TEXT = Table(
    slug="violation_text",
    name_pt="Texto da violação",
    name_en="Violation text",
    name_es="Texto de la violación",
    description_pt=(
        "Texto integral da descrição da violação que acompanha cada citação, "
        "remontado a partir das linhas do arquivo de origem. Traz a norma "
        "citada por extenso e a narrativa da condição observada, com endereço "
        "do local e data. Cobre 1.005.228 citações, 7,6% do total: a fonte "
        "praticamente não guarda o texto de citações anteriores a 2010 (0,4% "
        "das da década de 1990, 0,8% das de 2000) e cobre 31% das citações da "
        "década de 2010 e 39% das da de 2020. Apesar do nome do arquivo de "
        "origem (osha_violation_gen_duty_std), apenas 4,6% das citações aqui "
        "invocam a cláusula de dever geral 5(a)(1); as normas mais frequentes "
        "são a de proteção contra quedas (1926.501, 10,1%) e a de comunicação "
        "de perigos (1910.1200, 6,1%). Como toda citação é um documento "
        "público que identifica o autuado, o texto nomeia pessoas físicas "
        "quando o empregador citado é um profissional autônomo — o mesmo nome "
        "que consta em inspection.establishment_name."
    ),
    description_en=(
        "Full text of the alleged violation description accompanying each "
        "citation, reassembled from the lines of the source file. Carries the "
        "standard cited in full and the narrative of the observed condition, "
        "with the site address and date. It covers 1,005,228 citations, 7.6% "
        "of the total: the source barely keeps the text of citations issued "
        "before 2010 (0.4% of those from the 1990s, 0.8% from the 2000s) and "
        "covers 31% of citations issued in the 2010s and 39% of those in the "
        "2020s. Despite the source file name (osha_violation_gen_duty_std), "
        "only 4.6% of the citations here invoke General Duty Clause 5(a)(1); "
        "the most frequent standards are fall protection (1926.501, 10.1%) and "
        "hazard communication (1910.1200, 6.1%). Because a citation is a "
        "public document naming the respondent, the text names individuals "
        "where the cited employer is a sole proprietor — the same name that "
        "appears in inspection.establishment_name."
    ),
    description_es=(
        "Texto íntegro de la descripción de la violación que acompaña a cada "
        "citación, reensamblado a partir de las líneas del archivo de origen. "
        "Contiene la norma citada completa y la narrativa de la condición "
        "observada, con la dirección del sitio y la fecha. Cubre 1.005.228 "
        "citaciones, 7,6% del total: la fuente casi no conserva el texto de "
        "citaciones anteriores a 2010 (0,4% de las de la década de 1990, 0,8% "
        "de las de 2000) y cubre el 31% de las citaciones de la década de 2010 "
        "y el 39% de las de 2020. Pese al nombre del archivo de origen "
        "(osha_violation_gen_duty_std), solo el 4,6% de las citaciones aquí "
        "invocan la cláusula de deber general 5(a)(1); las normas más "
        "frecuentes son la de protección contra caídas (1926.501, 10,1%) y la "
        "de comunicación de peligros (1910.1200, 6,1%). Como toda citación es "
        "un documento público que identifica al citado, el texto nombra a "
        "personas físicas cuando el empleador citado es un trabajador "
        "autónomo — el mismo nombre que figura en "
        "inspection.establishment_name."
    ),
    source_file="OSHA_violation_gen_duty_std",
    year_from="inspection",
    primary_key=["inspection_id", "citation_id"],
    reassembled=True,
    columns=[
        YEAR_INSP,
        INSP_ID(),
        CITATION_ID(),
        c(
            "text",
            "STRING",
            None,
            "Texto integral da descrição da violação",
            "Full text of the alleged violation description",
            "Texto íntegro de la descripción de la violación",
            {
                "sensitive": True,
                "obs": "Remontado juntando as linhas de line_nr em ordem, separadas por espaço",
            },
        ),
        c(
            "line_count",
            "INT64",
            None,
            "Número de linhas do arquivo de origem que compõem o texto",
            "Number of source-file lines making up the text",
            "Número de líneas del archivo de origen que componen el texto",
            {"unit": "line"},
        ),
    ],
)


# --------------------------------------------------------------------------- #
# 5. related_activity
# --------------------------------------------------------------------------- #

RELATED_ACTIVITY = Table(
    slug="related_activity",
    name_pt="Atividade relacionada",
    name_en="Related activity",
    name_es="Actividad relacionada",
    description_pt=(
        "Liga cada inspeção às atividades que a originaram ou que dela "
        "decorreram: denúncias, encaminhamentos, acidentes e outras inspeções. "
        "Uma linha por par de atividades relacionadas."
    ),
    description_en=(
        "Links each inspection to the activities that prompted it or followed "
        "from it: complaints, referrals, accidents and other inspections. One "
        "row per related-activity pair."
    ),
    description_es=(
        "Vincula cada inspección con las actividades que la originaron o que "
        "derivaron de ella: denuncias, derivaciones, accidentes y otras "
        "inspecciones. Una fila por par de actividades relacionadas."
    ),
    source_file="OSHA_related_activity",
    year_from="inspection",
    primary_key=["inspection_id", "related_activity_id", "related_type"],
    columns=[
        YEAR_INSP,
        INSP_ID(),
        c(
            "related_activity_id",
            "STRING",
            "REL_ACT_NR",
            "Identificador da atividade relacionada",
            "Identifier of the related activity",
            "Identificador de la actividad relacionada",
        ),
        c(
            "related_type",
            "STRING",
            "REL_TYPE",
            "Tipo da atividade relacionada",
            "Type of the related activity",
            "Tipo de la actividad relacionada",
            {
                "obs": "A OSHA não publica a legenda dos códigos A, C, I, N e R, que por isso não constam do dicionário"
            },
        ),
        c(
            "related_safety",
            "STRING",
            "REL_SAFETY",
            "Indica que a denúncia ou encaminhamento relacionado trata de segurança (X)",
            "Whether the related complaint or referral concerns safety (X)",
            "Indica que la denuncia o derivación relacionada trata de seguridad (X)",
            {"dict": True},
        ),
        c(
            "related_health",
            "STRING",
            "REL_HEALTH",
            "Indica que a denúncia ou encaminhamento relacionado trata de saúde ocupacional (X)",
            "Whether the related complaint or referral concerns occupational health (X)",
            "Indica que la denuncia o derivación relacionada trata de salud ocupacional (X)",
            {"dict": True},
        ),
    ],
)


# --------------------------------------------------------------------------- #
# 6. emphasis_code
# --------------------------------------------------------------------------- #

EMPHASIS_CODE = Table(
    slug="emphasis_code",
    name_pt="Programa de ênfase",
    name_en="Emphasis program",
    name_es="Programa de énfasis",
    description_pt=(
        "Programas de ênfase associados a cada inspeção — nacionais, "
        "estratégicos, locais e estaduais. Uma linha por par de inspeção e "
        "programa; uma inspeção pode estar ligada a vários programas."
    ),
    description_en=(
        "Emphasis programs associated with each inspection — national, "
        "strategic, local and state. One row per inspection and program pair; "
        "an inspection may be linked to several programs."
    ),
    description_es=(
        "Programas de énfasis asociados a cada inspección — nacionales, "
        "estratégicos, locales y estatales. Una fila por par de inspección y "
        "programa; una inspección puede estar vinculada a varios programas."
    ),
    source_file="OSHA_emphasis_codes",
    year_from="inspection",
    primary_key=["inspection_id", "program_type", "program_value"],
    columns=[
        YEAR_INSP,
        INSP_ID(),
        c(
            "program_type",
            "STRING",
            "PROG_TYPE",
            "Tipo do programa de ênfase",
            "Type of emphasis program",
            "Tipo del programa de énfasis",
            {
                "obs": "A OSHA não publica a legenda dos códigos L, N, P e S, que por isso não constam do dicionário"
            },
        ),
        c(
            "program_value",
            "STRING",
            "PROG_VALUE",
            "Descrição do programa de ênfase específico",
            "Description of the specific emphasis program",
            "Descripción del programa de énfasis específico",
            {
                "obs": "Texto livre digitado no escritório de origem, com grafias inconsistentes e ocasionais caracteres espúrios"
            },
        ),
    ],
)


# --------------------------------------------------------------------------- #
# 7. optional_code_info
# --------------------------------------------------------------------------- #

OPTIONAL_CODE_INFO = Table(
    slug="optional_code_info",
    name_pt="Informação opcional",
    name_en="Optional information",
    name_es="Información opcional",
    description_pt=(
        "Informações opcionais coletadas em cada inspeção, gravadas como pares "
        "de identificador e valor. A OSHA não publica o significado dos "
        "identificadores nem o formato dos valores, de modo que o conteúdo é "
        "opaco: em geral são números de processo de planos estaduais."
    ),
    description_en=(
        "Optional information collected during each inspection, stored as "
        "identifier and value pairs. OSHA publishes neither the meaning of the "
        "identifiers nor the format of the values, so the content is opaque: "
        "most entries appear to be state-plan case numbers."
    ),
    description_es=(
        "Información opcional recogida en cada inspección, almacenada como "
        "pares de identificador y valor. OSHA no publica el significado de los "
        "identificadores ni el formato de los valores, por lo que el contenido "
        "es opaco: en general son números de expediente de planes estatales."
    ),
    source_file="OSHA_optional_code_info",
    year_from="inspection",
    primary_key=["inspection_id", "information_id", "information_value"],
    columns=[
        YEAR_INSP,
        INSP_ID(),
        c(
            "information_type",
            "STRING",
            "OPT_TYPE",
            "Tipo da informação opcional; apenas o tipo N é publicado",
            "Type of optional information; only type N is published",
            "Tipo de la información opcional; solo se publica el tipo N",
        ),
        c(
            "information_id",
            "STRING",
            "OPT_ID",
            "Identificador da informação opcional",
            "Identifier of the optional information",
            "Identificador de la información opcional",
            {"obs": "A OSHA não publica o significado destes identificadores"},
        ),
        c(
            "information_value",
            "STRING",
            "OPT_VALUE",
            "Valor da informação opcional",
            "Value of the optional information",
            "Valor de la información opcional",
        ),
    ],
)


# --------------------------------------------------------------------------- #
# 8. accident
# --------------------------------------------------------------------------- #

ACCIDENT = Table(
    slug="accident",
    name_pt="Acidente",
    name_en="Incident",
    name_es="Accidente",
    description_pt=(
        "Uma linha por acidente de trabalho comunicado pelo empregador e "
        "investigado pela OSHA, identificado pelo número do formulário OSHA-170. "
        "Contém a data e a descrição curta do evento, as palavras-chave "
        "atribuídas na revisão, os atributos da obra quando o acidente ocorre "
        "na construção civil e a indicação de óbito. Cobre 1972 a 2025."
    ),
    description_en=(
        "One row per workplace incident reported by the employer and "
        "investigated by OSHA, identified by its OSHA-170 form number. Carries "
        "the date and short description of the event, the keywords assigned "
        "during review, the construction-project attributes where the incident "
        "occurred on a construction site, and a fatality indicator. Covers "
        "1972 to 2025."
    ),
    description_es=(
        "Una fila por accidente laboral comunicado por el empleador e "
        "investigado por OSHA, identificado por el número del formulario "
        "OSHA-170. Contiene la fecha y la descripción corta del evento, las "
        "palabras clave asignadas en la revisión, los atributos de la obra "
        "cuando el accidente ocurre en la construcción y la indicación de "
        "fallecimiento. Cubre 1972 a 2025."
    ),
    source_file="OSHA_accident",
    year_from="event_date",
    primary_key=["accident_id"],
    columns=[
        c(
            "year",
            "INT64",
            None,
            "Ano do acidente",
            "Year of the incident",
            "Año del accidente",
            {"unit": "year", "dir": TIME_DIR, "obs": "Derivado de event_date"},
        ),
        c(
            "accident_id",
            "STRING",
            "SUMMARY_NR",
            "Identificador único do acidente (número do formulário OSHA-170)",
            "Unique incident identifier (OSHA-170 form number)",
            "Identificador único del accidente (número del formulario OSHA-170)",
        ),
        c(
            "reporting_office_id",
            "STRING",
            "REPORT_ID",
            "Código da jurisdição da OSHA (escritório federal ou plano estadual) que registrou o acidente",
            "Code of the OSHA reporting jurisdiction (federal office or state plan) that recorded the incident",
            "Código de la jurisdicción de OSHA (oficina federal o plan estatal) que registró el accidente",
            {"obs": "Preserva zeros à esquerda, por isso STRING"},
        ),
        c(
            "event_date",
            "DATE",
            "EVENT_DATE",
            "Data do acidente",
            "Date of the incident",
            "Fecha del accidente",
        ),
        c(
            "event_description",
            "STRING",
            "EVENT_DESC",
            "Descrição curta do evento, sem identificação dos trabalhadores envolvidos",
            "Short description of the event, with no identification of the workers involved",
            "Descripción corta del evento, sin identificación de los trabajadores involucrados",
        ),
        c(
            "event_keyword",
            "STRING",
            "EVENT_KEYWORD",
            "Palavras-chave atribuídas ao acidente na revisão, separadas por vírgula",
            "Keywords assigned to the incident during review, comma separated",
            "Palabras clave asignadas al accidente en la revisión, separadas por coma",
            {"obs": "Multivalorado; até 685 caracteres"},
        ),
        c(
            "fatality",
            "STRING",
            "FATALITY",
            "Indica que há óbito associado ao acidente (X)",
            "Whether a fatality is associated with the incident (X)",
            "Indica que hay fallecimiento asociado al accidente (X)",
            {"dict": True},
        ),
        c(
            "sic_list",
            "STRING",
            "SIC_LIST",
            "Códigos SIC de quatro dígitos das inspeções relacionadas, separados por vírgula",
            "Four-digit SIC codes of the related inspections, comma separated",
            "Códigos SIC de cuatro dígitos de las inspecciones relacionadas, separados por coma",
            {"obs": "Multivalorado"},
        ),
        c(
            "construction_end_use",
            "STRING",
            "CONST_END_USE",
            "Destinação da obra, quando o acidente ocorre na construção civil",
            "End use of the construction project, where the incident occurred on a construction site",
            "Destino de la obra, cuando el accidente ocurre en la construcción",
            {"dict": True},
        ),
        c(
            "project_type",
            "STRING",
            "PROJECT_TYPE",
            "Tipo da obra, quando o acidente ocorre na construção civil",
            "Type of the construction project, where the incident occurred on a construction site",
            "Tipo de la obra, cuando el accidente ocurre en la construcción",
            {"dict": True},
        ),
        c(
            "project_cost",
            "STRING",
            "PROJECT_COST",
            "Faixa de custo da obra, quando o acidente ocorre na construção civil",
            "Cost band of the construction project, where the incident occurred on a construction site",
            "Rango de costo de la obra, cuando el accidente ocurre en la construcción",
            {"dict": True},
        ),
        c(
            "building_stories",
            "INT64",
            "BUILD_STORIES",
            "Número de pavimentos da edificação, quando o acidente ocorre na construção civil",
            "Number of storeys of the building, where the incident occurred on a construction site",
            "Número de pisos de la edificación, cuando el accidente ocurre en la construcción",
            {"unit": "storey"},
        ),
        c(
            "nonbuilding_height",
            "INT64",
            "NONBUILD_HT",
            "Altura em pés da estrutura, quando a obra não é uma edificação",
            "Height in feet of the structure, where the project is not a building",
            "Altura en pies de la estructura, cuando la obra no es una edificación",
            {"unit": "foot"},
        ),
    ],
)


# --------------------------------------------------------------------------- #
# 9. accident_injury
# --------------------------------------------------------------------------- #

ACCIDENT_INJURY = Table(
    slug="accident_injury",
    name_pt="Lesão em acidente",
    name_en="Incident injury",
    name_es="Lesión en accidente",
    description_pt=(
        "Uma linha por pessoa lesionada em cada acidente investigado. Contém "
        "idade, sexo e ocupação da pessoa, a natureza da lesão, a parte do "
        "corpo atingida, a fonte e o tipo do evento, os fatores ambiental e "
        "humano e a gravidade (óbito, hospitalização ou não hospitalização). "
        "As pessoas não são nomeadas: identificam-se apenas pelo número de "
        "linha dentro do acidente. Os códigos são interpretados pela tabela "
        "dicionario."
    ),
    description_en=(
        "One row per person injured in each investigated incident. Carries the "
        "person's age, sex and occupation, the nature of the injury, the part "
        "of the body affected, the source and type of the event, the "
        "environmental and human factors, and the severity (fatality, "
        "hospitalised or not hospitalised). People are not named: they are "
        "identified only by their line number within the incident. Codes are "
        "resolved through the dicionario table."
    ),
    description_es=(
        "Una fila por persona lesionada en cada accidente investigado. "
        "Contiene edad, sexo y ocupación de la persona, la naturaleza de la "
        "lesión, la parte del cuerpo afectada, la fuente y el tipo del evento, "
        "los factores ambiental y humano y la gravedad (fallecimiento, "
        "hospitalización o no hospitalización). Las personas no son nombradas: "
        "se identifican solo por el número de línea dentro del accidente. Los "
        "códigos se interpretan con la tabla dicionario."
    ),
    source_file="OSHA_accident_injury",
    year_from="accident_or_inspection",
    primary_key=["accident_id", "injury_line_number"],
    columns=[
        YEAR_ACC,
        c(
            "accident_id",
            "STRING",
            "SUMMARY_NR",
            "Identificador do acidente a que a lesão pertence",
            "Identifier of the incident the injury belongs to",
            "Identificador del accidente al que pertenece la lesión",
        ),
        c(
            "inspection_id",
            "STRING",
            "REL_INSP_NR",
            "Identificador da inspeção associada à investigação do acidente",
            "Identifier of the inspection associated with the incident investigation",
            "Identificador de la inspección asociada a la investigación del accidente",
        ),
        c(
            "injury_line_number",
            "STRING",
            "INJURY_LINE_NR",
            "Número de linha da pessoa lesionada dentro do acidente, referenciado no texto da narrativa",
            "Line number of the injured person within the incident, referenced in the narrative text",
            "Número de línea de la persona lesionada dentro del accidente, referenciado en el texto de la narrativa",
        ),
        c(
            "age",
            "INT64",
            "AGE",
            "Idade da pessoa lesionada",
            "Age of the injured person",
            "Edad de la persona lesionada",
            {
                "unit": "year",
                "obs": "O valor 0 é sentinela de idade desconhecida, não uma idade real",
            },
        ),
        c(
            "sex",
            "STRING",
            "SEX",
            "Sexo da pessoa lesionada",
            "Sex of the injured person",
            "Sexo de la persona lesionada",
            {"dict": True},
        ),
        c(
            "degree_of_injury",
            "STRING",
            "DEGREE_OF_INJ",
            "Gravidade da lesão: óbito, hospitalização ou sem hospitalização",
            "Severity of the injury: fatality, hospitalised or not hospitalised",
            "Gravedad de la lesión: fallecimiento, hospitalización o sin hospitalización",
            {"dict": True},
        ),
        c(
            "nature_of_injury",
            "STRING",
            "NATURE_OF_INJ",
            "Natureza da lesão (tabela de códigos IN)",
            "Nature of the injury (code table IN)",
            "Naturaleza de la lesión (tabla de códigos IN)",
            {
                "dict": True,
                "obs": "Não segue a codificação OIICS exigida pelas normas atuais de registro",
            },
        ),
        c(
            "part_of_body",
            "STRING",
            "PART_OF_BODY",
            "Parte do corpo atingida (tabela de códigos BD)",
            "Part of the body affected (code table BD)",
            "Parte del cuerpo afectada (tabla de códigos BD)",
            {"dict": True, "obs": "Não segue a codificação OIICS"},
        ),
        c(
            "source_of_injury",
            "STRING",
            "SRC_OF_INJURY",
            "Fonte da lesão (tabela de códigos SO)",
            "Source of the injury (code table SO)",
            "Fuente de la lesión (tabla de códigos SO)",
            {"dict": True, "obs": "Não segue a codificação OIICS"},
        ),
        c(
            "event_type",
            "STRING",
            "EVENT_TYPE",
            "Tipo do evento que causou a lesão (tabela de códigos FT)",
            "Type of event that caused the injury (code table FT)",
            "Tipo del evento que causó la lesión (tabla de códigos FT)",
            {"dict": True, "obs": "Não segue a codificação OIICS"},
        ),
        c(
            "environmental_factor",
            "STRING",
            "EVN_FACTOR",
            "Fator ambiental associado à lesão (tabela de códigos EN)",
            "Environmental factor associated with the injury (code table EN)",
            "Factor ambiental asociado a la lesión (tabla de códigos EN)",
            {"dict": True},
        ),
        c(
            "human_factor",
            "STRING",
            "HUM_FACTOR",
            "Fator humano associado à lesão (tabela de códigos HU)",
            "Human factor associated with the injury (code table HU)",
            "Factor humano asociado a la lesión (tabla de códigos HU)",
            {"dict": True},
        ),
        c(
            "occupation_code",
            "STRING",
            "OCC_CODE",
            "Ocupação da pessoa lesionada (tabela de códigos OCC)",
            "Occupation of the injured person (code table OCC)",
            "Ocupación de la persona lesionada (tabla de códigos OCC)",
            {
                "dict": True,
                "obs": "Codificação própria da OSHA; não corresponde à classificação SOC e por isso não há vínculo com o diretório de ocupações",
            },
        ),
        c(
            "task_assigned",
            "STRING",
            "TASK_ASSIGNED",
            "Indica se a tarefa executada era regularmente atribuída à pessoa",
            "Whether the task being performed was regularly assigned to the person",
            "Indica si la tarea realizada era regularmente asignada a la persona",
            {"dict": True},
        ),
        c(
            "hazardous_substance",
            "STRING",
            "HAZSUB",
            "Código da substância perigosa envolvida na lesão",
            "Code of the hazardous substance involved in the injury",
            "Código de la sustancia peligrosa involucrada en la lesión",
        ),
        c(
            "construction_operation",
            "STRING",
            "CONST_OP",
            "Operação de construção em curso (tabela de códigos OPER)",
            "Construction operation under way (code table OPER)",
            "Operación de construcción en curso (tabla de códigos OPER)",
            {"dict": True},
        ),
        c(
            "construction_operation_cause",
            "STRING",
            "CONST_OP_CAUSE",
            "Causa da lesão na operação de construção (tabela de códigos OPER)",
            "Cause of the injury in the construction operation (code table OPER)",
            "Causa de la lesión en la operación de construcción (tabla de códigos OPER)",
            {"dict": True},
        ),
        c(
            "fatality_cause",
            "STRING",
            "FAT_CAUSE",
            "Causa do óbito em obra de construção (tabela de códigos CAUS)",
            "Cause of the fatality on a construction site (code table CAUS)",
            "Causa del fallecimiento en obra de construcción (tabla de códigos CAUS)",
            {"dict": True},
        ),
        c(
            "fall_distance",
            "INT64",
            "FALL_DISTANCE",
            "Distância da queda, em pés",
            "Distance of the fall, in feet",
            "Distancia de la caída, en pies",
            {
                "unit": "foot",
                "obs": "Preenchida em 1,9% das linhas; o valor máximo observado é 99, o que sugere truncamento na fonte",
            },
        ),
        c(
            "fall_height",
            "INT64",
            "FALL_HT",
            "Altura em que a pessoa se encontrava ao cair, em pés",
            "Height at which the person was standing when they fell, in feet",
            "Altura a la que se encontraba la persona al caer, en pies",
            {
                "unit": "foot",
                "obs": "Preenchida em 6,6% das linhas; o valor máximo observado é 99, o que sugere truncamento na fonte",
            },
        ),
    ],
)


# --------------------------------------------------------------------------- #
# 10. accident_narrative
# --------------------------------------------------------------------------- #

ACCIDENT_NARRATIVE = Table(
    slug="accident_narrative",
    name_pt="Narrativa do acidente",
    name_en="Incident narrative",
    name_es="Narrativa del accidente",
    description_pt=(
        "Uma linha por acidente, com a narrativa da investigação remontada a "
        "partir das linhas do arquivo de origem. A narrativa descreve a "
        "sequência do evento e o desfecho clínico. As pessoas lesionadas não "
        'são nomeadas: a OSHA as designa como "Employee #1" ou "an '
        'employee" — em uma amostra de 21.133 narrativas não há nome próprio, '
        "número de documento nem telefone. A narrativa contém, porém, idade, "
        "sexo, ocupação, empregador, data e hora exatas, local e detalhe da "
        "lesão, o que pode identificar a pessoa para quem conhece o local de "
        "trabalho. O conteúdo é publicado literalmente pela própria OSHA."
    ),
    description_en=(
        "One row per incident, with the investigation narrative reassembled "
        "from the lines of the source file. The narrative describes the "
        "sequence of the event and the clinical outcome. Injured people are "
        'not named: OSHA refers to them as "Employee #1" or "an employee" '
        "— across a sample of 21,133 narratives there is no personal name, "
        "identity number or telephone number. The narrative does carry age, "
        "sex, occupation, employer, exact date and time, location and injury "
        "detail, which can identify the person to anyone who knows the "
        "workplace. The content is published verbatim by OSHA itself."
    ),
    description_es=(
        "Una fila por accidente, con la narrativa de la investigación "
        "reensamblada a partir de las líneas del archivo de origen. La "
        "narrativa describe la secuencia del evento y el desenlace clínico. "
        "Las personas lesionadas no son nombradas: OSHA las designa como "
        '"Employee #1" o "an employee" — en una muestra de 21.133 '
        "narrativas no hay nombre propio, número de documento ni teléfono. La "
        "narrativa sí contiene edad, sexo, ocupación, empleador, fecha y hora "
        "exactas, lugar y detalle de la lesión, lo que puede identificar a la "
        "persona para quien conoce el lugar de trabajo. El contenido lo "
        "publica literalmente la propia OSHA."
    ),
    source_file="OSHA_accident_abstract",
    year_from="accident",
    primary_key=["accident_id"],
    reassembled=True,
    columns=[
        c(
            "year",
            "INT64",
            None,
            "Ano do acidente",
            "Year of the incident",
            "Año del accidente",
            {
                "unit": "year",
                "dir": TIME_DIR,
                "obs": "Derivado de accident.event_date",
            },
        ),
        c(
            "accident_id",
            "STRING",
            "SUMMARY_NR",
            "Identificador do acidente a que a narrativa se refere",
            "Identifier of the incident the narrative describes",
            "Identificador del accidente al que se refiere la narrativa",
        ),
        c(
            "narrative",
            "STRING",
            None,
            "Narrativa da investigação do acidente",
            "Narrative of the incident investigation",
            "Narrativa de la investigación del accidente",
            {
                "sensitive": True,
                "obs": "Remontada juntando as linhas de line_nr em ordem. A fonte usa duas quebras de linha diferentes: registros antigos quebram em exatamente 80 caracteres no meio da palavra (junção sem separador) e registros novos quebram na palavra consumindo o espaço (junção com espaço). O critério é por registro: se toda linha não final tem exatamente 80 caracteres, junta-se sem separador",
            },
        ),
        c(
            "line_count",
            "INT64",
            None,
            "Número de linhas do arquivo de origem que compõem a narrativa",
            "Number of source-file lines making up the narrative",
            "Número de líneas del archivo de origen que componen la narrativa",
            {"unit": "line"},
        ),
        c(
            "wrap_style",
            "STRING",
            None,
            "Regime de quebra de linha do registro na fonte, usado na remontagem",
            "Line-wrapping regime of the record in the source, used in the reassembly",
            "Régimen de salto de línea del registro en la fuente, usado en el reensamblaje",
            {"dict": True},
        ),
    ],
)


# --------------------------------------------------------------------------- #
# 11. dicionario
# --------------------------------------------------------------------------- #

DICIONARIO = Table(
    slug="dicionario",
    name_pt="Dicionário",
    name_en="Dictionary",
    name_es="Diccionario",
    description_pt=(
        "Traduz os códigos usados nas demais tabelas para rótulos legíveis. "
        "Reúne duas fontes: as 14 tabelas de código publicadas pela OSHA no "
        "arquivo osha_accident_lookup2, que interpretam as colunas de "
        "accident e accident_injury, e as listas de códigos publicadas no "
        "dicionário de dados do Catálogo de Dados de Fiscalização do DOL. "
        "Códigos para os quais a OSHA não publica legenda — gravity, "
        "abatement_completion_code, related_event_code, hazard_category, "
        "event_code, related_type e program_type — não constam desta tabela; "
        "a coluna correspondente registra a ausência em suas observações."
    ),
    description_en=(
        "Translates the codes used in the other tables into readable labels. "
        "It draws on two sources: the 14 code tables OSHA publishes in the "
        "osha_accident_lookup2 file, which resolve the columns of accident and "
        "accident_injury, and the code lists published in the data dictionary "
        "of the DOL Enforcement Data Catalog. Codes for which OSHA publishes "
        "no legend — gravity, abatement_completion_code, related_event_code, "
        "hazard_category, event_code, related_type and program_type — are "
        "absent from this table; the corresponding column records the gap in "
        "its observations."
    ),
    description_es=(
        "Traduce los códigos usados en las demás tablas a etiquetas legibles. "
        "Reúne dos fuentes: las 14 tablas de códigos que OSHA publica en el "
        "archivo osha_accident_lookup2, que interpretan las columnas de "
        "accident y accident_injury, y las listas de códigos publicadas en el "
        "diccionario de datos del Catálogo de Datos de Fiscalización del DOL. "
        "Los códigos para los que OSHA no publica leyenda — gravity, "
        "abatement_completion_code, related_event_code, hazard_category, "
        "event_code, related_type y program_type — no figuran en esta tabla; "
        "la columna correspondiente registra la ausencia en sus observaciones."
    ),
    source_file="OSHA_accident_lookup2",
    year_from="",
    primary_key=["id_tabela", "nome_coluna", "chave"],
    columns=[
        c(
            "id_tabela",
            "STRING",
            None,
            "Nome da tabela que contém a coluna codificada",
            "Name of the table holding the coded column",
            "Nombre de la tabla que contiene la columna codificada",
        ),
        c(
            "nome_coluna",
            "STRING",
            None,
            "Nome da coluna codificada",
            "Name of the coded column",
            "Nombre de la columna codificada",
        ),
        c(
            "chave",
            "STRING",
            None,
            "Código armazenado na coluna",
            "Code stored in the column",
            "Código almacenado en la columna",
        ),
        c(
            "cobertura_temporal",
            "STRING",
            None,
            "Cobertura temporal do código",
            "Temporal coverage of the code",
            "Cobertura temporal del código",
        ),
        c(
            "valor",
            "STRING",
            None,
            "Rótulo legível correspondente ao código",
            "Readable label corresponding to the code",
            "Etiqueta legible correspondiente al código",
        ),
    ],
)


TABLES: list[Table] = [
    INSPECTION,
    VIOLATION,
    VIOLATION_EVENT,
    VIOLATION_TEXT,
    RELATED_ACTIVITY,
    EMPHASIS_CODE,
    OPTIONAL_CODE_INFO,
    ACCIDENT,
    ACCIDENT_INJURY,
    ACCIDENT_NARRATIVE,
    DICIONARIO,
]

BY_SLUG = {t.slug: t for t in TABLES}
