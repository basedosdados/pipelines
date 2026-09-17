"""Single source of truth for the us_state_foreign_assistance architecture.

Generates one architecture CSV per table (BD schema) from the column specs
below. The cleaning transform (``pipelines/datasets/us_state_foreign_assistance/
utils.py``) and the dbt generator (``../gen_dbt.py``) read the CSVs, so column
names, order, types, and descriptions live here only.

Dataset: ForeignAssistance.gov, the U.S. government's foreign assistance data
portal (Department of State and USAID). Three tables:

    transaction  obligations and disbursements at activity level, FY1946-present
    budget       President's budget requests and appropriations, FY2004-present
    dicionario   labels for every coded column

English column names (``_id`` suffix). Descriptions in PT / EN / ES. Types by
arithmetic meaning: INT64/FLOAT64 only for real quantities; every code, flag,
and identifier stays STRING and is covered by the dictionary.
"""

import csv
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATASET = "us_state_foreign_assistance"

DIR_YEAR = "br_bd_diretorios_data_tempo.ano:ano"
DIR_PAIS = "br_bd_diretorios_mundo.pais:sigla_iso3"

FIELDS = [
    "name",
    "bigquery_type",
    "description",
    "description_en",
    "description_es",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
]


def col(
    name,
    ty,
    en,
    pt,
    es,
    *,
    unit="",
    dic="no",
    directory="",
    obs="",
    orig="",
):
    return dict(
        name=name,
        bigquery_type=ty,
        description=pt,
        description_en=en,
        description_es=es,
        temporal_coverage="",
        covered_by_dictionary=dic,
        directory_column=directory,
        measurement_unit=unit,
        has_sensitive_data="no",
        observations=obs,
        original_name=orig,
    )


# ---------------------------------------------------------------------------
# Shared column definitions (used by transaction and budget)
# ---------------------------------------------------------------------------

YEAR = col(
    "year",
    "INT64",
    "Fiscal year (October 1 to September 30, designated by the calendar year "
    "in which it ends); the July-September 1976 transition quarter is assigned "
    "to 1976",
    "Ano fiscal (1º de outubro a 30 de setembro, designado pelo ano civil em "
    "que termina); o trimestre de transição de julho a setembro de 1976 é "
    "atribuído a 1976",
    "Año fiscal (1 de octubre a 30 de septiembre, designado por el año civil "
    "en que termina); el trimestre de transición de julio a septiembre de 1976 "
    "se asigna a 1976",
    unit="year",
    directory=DIR_YEAR,
    orig="Fiscal Year",
)

TRANSACTION_TYPE = col(
    "transaction_type_id",
    "STRING",
    "Transaction type code: 2 = obligations, 3 = disbursements, "
    "1 = appropriated and planned, 18 = President's budget request",
    "Código do tipo de transação: 2 = obrigações (empenhos), 3 = desembolsos, "
    "1 = apropriado e planejado, 18 = requisição orçamentária do Presidente",
    "Código del tipo de transacción: 2 = obligaciones, 3 = desembolsos, "
    "1 = asignado y planificado, 18 = solicitud presupuestaria del Presidente",
    dic="yes",
    orig="Transaction Type ID",
)

COUNTRY_ISO3 = col(
    "country_iso3_code",
    "STRING",
    "ISO 3166-1 alpha-3 code of the recipient country; empty for regional "
    "recipients and former states",
    "Código ISO 3166-1 alfa-3 do país receptor; vazio para receptores "
    "regionais e Estados extintos",
    "Código ISO 3166-1 alfa-3 del país receptor; vacío para receptores "
    "regionales y Estados extintos",
    directory=DIR_PAIS,
    obs=(
        "Derived from country_code: kept only when the recipient has a "
        "3-digit ISO numeric country_id and the code exists in the world "
        "country directory (drops SCG, YUF and SDF, former states)."
    ),
    orig="Country Code",
)

COUNTRY_ID = col(
    "country_id",
    "STRING",
    "ForeignAssistance.gov recipient identifier: the ISO 3166-1 numeric code "
    "for countries, or a 4-digit internal code for regions and entities "
    "without an ISO code",
    "Identificador do receptor no ForeignAssistance.gov: código numérico ISO "
    "3166-1 para países, ou código interno de 4 dígitos para regiões e "
    "entidades sem código ISO",
    "Identificador del receptor en ForeignAssistance.gov: código numérico ISO "
    "3166-1 para países, o código interno de 4 dígitos para regiones y "
    "entidades sin código ISO",
    dic="yes",
    orig="Country ID",
)

COUNTRY_CODE = col(
    "country_code",
    "STRING",
    "Recipient code as published: the ISO alpha-3 code for countries, or a "
    "3-letter internal code for regions (e.g. WLD = World, SSN = Sub-Saharan "
    "Africa Region)",
    "Código do receptor conforme publicado: código ISO alfa-3 para países, ou "
    "código interno de 3 letras para regiões (ex.: WLD = Mundo, SSN = Região "
    "da África Subsaariana)",
    "Código del receptor según publicado: código ISO alfa-3 para países, o "
    "código interno de 3 letras para regiones (p. ej. WLD = Mundo, SSN = "
    "Región de África Subsahariana)",
    obs="Labels are in the dictionary under country_id.",
    orig="Country Code",
)

REGION = col(
    "region_id",
    "STRING",
    "Code of the Department of State region that groups the recipient",
    "Código da região do Departamento de Estado que agrupa o receptor",
    "Código de la región del Departamento de Estado que agrupa al receptor",
    dic="yes",
    orig="Region ID",
)

INCOME_GROUP = col(
    "income_group_id",
    "STRING",
    "World Bank income group code of the recipient country",
    "Código do grupo de renda do Banco Mundial do país receptor",
    "Código del grupo de ingreso del Banco Mundial del país receptor",
    dic="yes",
    orig="Income Group ID",
)

MANAGING_AGENCY = col(
    "managing_agency_id",
    "STRING",
    "Code of the U.S. government agency that obligates and disburses the "
    "assistance, directly or through an implementing partner",
    "Código da agência do governo dos EUA que empenha e desembolsa a "
    "assistência, diretamente ou por meio de um parceiro implementador",
    "Código de la agencia del gobierno de EE. UU. que obliga y desembolsa la "
    "asistencia, directamente o a través de un socio implementador",
    dic="yes",
    orig="Managing Agency ID",
)

MANAGING_SUBAGENCY = col(
    "managing_subagency_id",
    "STRING",
    "Code of the bureau, office or sub-agency within the managing agency; "
    "999 = not applicable",
    "Código do escritório, departamento ou subagência dentro da agência "
    "gestora; 999 = não aplicável",
    "Código de la oficina, departamento o subagencia dentro de la agencia "
    "gestora; 999 = no aplicable",
    dic="yes",
    orig="Managing Sub-agency or Bureau ID",
)

FUNDING_AGENCY = col(
    "funding_agency_id",
    "STRING",
    "Code of the agency to which the funds were appropriated (for funds "
    "appropriated to the Executive Office of the President, the agency that "
    "obligates them)",
    "Código da agência à qual os recursos foram apropriados (para recursos "
    "apropriados ao Gabinete Executivo do Presidente, a agência que os "
    "empenha)",
    "Código de la agencia a la que se asignaron los fondos (para fondos "
    "asignados a la Oficina Ejecutiva del Presidente, la agencia que los "
    "obliga)",
    dic="yes",
    orig="Funding Agency ID",
)

FUNDING_ACCOUNT = col(
    "funding_account_id",
    "STRING",
    "Treasury Account Symbol (TAS) of the appropriation account that funds "
    "the transaction",
    "Símbolo de Conta do Tesouro (TAS) da conta de apropriação que financia a "
    "transação",
    "Símbolo de Cuenta del Tesoro (TAS) de la cuenta de asignación que "
    "financia la transacción",
    dic="yes",
    orig="Funding Account ID",
)

INTL_CATEGORY = col(
    "international_category_id",
    "STRING",
    "Code of the ForeignAssistance.gov international sector category "
    "(Agriculture, Education, Health and Population, Humanitarian, etc.) that "
    "aggregates OECD/DAC sectors",
    "Código da categoria setorial internacional do ForeignAssistance.gov "
    "(Agricultura, Educação, Saúde e População, Humanitário etc.) que agrega "
    "os setores da OCDE/CAD",
    "Código de la categoría sectorial internacional de ForeignAssistance.gov "
    "(Agricultura, Educación, Salud y Población, Humanitario, etc.) que agrega "
    "los sectores de la OCDE/CAD",
    dic="yes",
    orig="International Category ID",
)

INTL_SECTOR = col(
    "international_sector_code",
    "STRING",
    "OECD/DAC Creditor Reporting System 3-digit sector code (the first three "
    "digits of the purpose code)",
    "Código setorial de 3 dígitos do Sistema de Notificação do Credor (CRS) "
    "da OCDE/CAD (três primeiros dígitos do código de propósito)",
    "Código sectorial de 3 dígitos del Sistema de Notificación del Acreedor "
    "(CRS) de la OCDE/CAD (tres primeros dígitos del código de propósito)",
    dic="yes",
    orig="International Sector Code",
)

INTL_PURPOSE = col(
    "international_purpose_code",
    "STRING",
    "OECD/DAC Creditor Reporting System 5-digit purpose code of the activity",
    "Código de propósito de 5 dígitos do CRS da OCDE/CAD da atividade",
    "Código de propósito de 5 dígitos del CRS de la OCDE/CAD de la actividad",
    dic="yes",
    orig="International Purpose Code",
)

US_CATEGORY = col(
    "us_category_id",
    "STRING",
    "Code of the U.S. government foreign assistance category (first level of "
    "the Standardized Program Structure and Definitions framework)",
    "Código da categoria de assistência externa do governo dos EUA (primeiro "
    "nível da estrutura Standardized Program Structure and Definitions)",
    "Código de la categoría de asistencia exterior del gobierno de EE. UU. "
    "(primer nivel del marco Standardized Program Structure and Definitions)",
    dic="yes",
    orig="US Category ID",
)

US_SECTOR = col(
    "us_sector_id",
    "STRING",
    "Code of the U.S. government foreign assistance sector (second level of "
    "the framework)",
    "Código do setor de assistência externa do governo dos EUA (segundo nível "
    "da estrutura)",
    "Código del sector de asistencia exterior del gobierno de EE. UU. (segundo "
    "nivel del marco)",
    dic="yes",
    orig="US Sector ID",
)

ACTIVITY_ID = col(
    "activity_id",
    "STRING",
    "ForeignAssistance.gov internal identifier of the implementing activity "
    "(project, program, cash transfer, contribution, etc.)",
    "Identificador interno no ForeignAssistance.gov da atividade "
    "implementadora (projeto, programa, transferência, contribuição etc.)",
    "Identificador interno en ForeignAssistance.gov de la actividad "
    "implementadora (proyecto, programa, transferencia, contribución, etc.)",
    orig="Activity ID",
)

ACTIVITY_NAME = col(
    "activity_name",
    "STRING",
    "Name of the assistance activity",
    "Nome da atividade de assistência",
    "Nombre de la actividad de asistencia",
    orig="Activity Name",
)

ACTIVITY_DESCRIPTION = col(
    "activity_description",
    "STRING",
    "Description of the assistance activity",
    "Descrição da atividade de assistência",
    "Descripción de la actividad de asistencia",
    orig="Activity Description",
)

CURRENT_AMOUNT = col(
    "current_amount",
    "FLOAT64",
    "Amount in current (nominal) U.S. dollars; negative values are "
    "de-obligations or corrections",
    "Valor em dólares americanos correntes (nominais); valores negativos são "
    "desobrigações ou correções",
    "Monto en dólares estadounidenses corrientes (nominales); los valores "
    "negativos son desobligaciones o correcciones",
    unit="USD",
    orig="Current Dollar Amount / current_amount",
)

CONSTANT_AMOUNT = col(
    "constant_amount",
    "FLOAT64",
    "Amount in constant 2025 U.S. dollars (inflation-adjusted by "
    "ForeignAssistance.gov)",
    "Valor em dólares americanos constantes de 2025 (ajustado pela inflação "
    "pelo ForeignAssistance.gov)",
    "Monto en dólares estadounidenses constantes de 2025 (ajustado por "
    "inflación por ForeignAssistance.gov)",
    unit="USD",
    orig="Constant Dollar Amount / constant_amount",
)

# ---------------------------------------------------------------------------
# transaction
# ---------------------------------------------------------------------------

TRANSACTION = [
    YEAR,
    col(
        "fiscal_period",
        "STRING",
        "Fiscal period as published: the fiscal year, or 1976TQ for the "
        "July-September 1976 transition quarter",
        "Período fiscal conforme publicado: o ano fiscal, ou 1976TQ para o "
        "trimestre de transição de julho a setembro de 1976",
        "Período fiscal según publicado: el año fiscal, o 1976TQ para el "
        "trimestre de transición de julio a septiembre de 1976",
        orig="Fiscal Year",
    ),
    col(
        "transaction_date",
        "DATE",
        "Date the financial record was transacted in the agency's accounting "
        "system, consolidated by quarter for some agencies; not reported "
        "before FY2002",
        "Data em que o registro financeiro foi transacionado no sistema "
        "contábil da agência, consolidada por trimestre para algumas "
        "agências; não informada antes do ano fiscal de 2002",
        "Fecha en que el registro financiero fue transado en el sistema "
        "contable de la agencia, consolidada por trimestre para algunas "
        "agencias; no informada antes del año fiscal 2002",
        obs="Source format DDMONYYYY (e.g. 01SEP2024).",
        orig="Transaction Date",
    ),
    TRANSACTION_TYPE,
    COUNTRY_ISO3,
    COUNTRY_ID,
    COUNTRY_CODE,
    REGION,
    INCOME_GROUP,
    MANAGING_AGENCY,
    MANAGING_SUBAGENCY,
    FUNDING_AGENCY,
    FUNDING_ACCOUNT,
    col(
        "implementing_partner_id",
        "STRING",
        "ForeignAssistance.gov identifier of the implementing partner (channel "
        "of delivery) that received the funds",
        "Identificador no ForeignAssistance.gov do parceiro implementador "
        "(canal de entrega) que recebeu os recursos",
        "Identificador en ForeignAssistance.gov del socio implementador (canal "
        "de entrega) que recibió los fondos",
        orig="Implementing Partner ID",
    ),
    col(
        "implementing_partner_name",
        "STRING",
        "Name of the implementing partner: the government agency, firm, "
        "organization or other party that carries out the assistance activity",
        "Nome do parceiro implementador: a agência governamental, empresa, "
        "organização ou outra parte que executa a atividade de assistência",
        "Nombre del socio implementador: la agencia gubernamental, empresa, "
        "organización u otra parte que ejecuta la actividad de asistencia",
        orig="Implementing Partner Name",
    ),
    col(
        "implementing_partner_category_id",
        "STRING",
        "Code of the broad delivery-channel category of the implementing "
        "partner (government, NGO, multilateral, enterprise, etc.)",
        "Código da categoria ampla do canal de entrega do parceiro "
        "implementador (governo, ONG, multilateral, empresa etc.)",
        "Código de la categoría amplia del canal de entrega del socio "
        "implementador (gobierno, ONG, multilateral, empresa, etc.)",
        dic="yes",
        orig="Implementing Partner Category ID",
    ),
    col(
        "implementing_partner_subcategory_id",
        "STRING",
        "Code of the delivery-channel subcategory of the implementing partner "
        "(e.g. NGO - United States, NGO - International)",
        "Código da subcategoria do canal de entrega do parceiro implementador "
        "(ex.: ONG - Estados Unidos, ONG - Internacional)",
        "Código de la subcategoría del canal de entrega del socio "
        "implementador (p. ej. ONG - Estados Unidos, ONG - Internacional)",
        dic="yes",
        orig="Implementing Partner Sub-category ID",
    ),
    INTL_CATEGORY,
    INTL_SECTOR,
    INTL_PURPOSE,
    US_CATEGORY,
    US_SECTOR,
    col(
        "objective_id",
        "STRING",
        "Greenbook assistance objective code: 1 = economic, 2 = military",
        "Código do objetivo da assistência no Greenbook: 1 = econômica, "
        "2 = militar",
        "Código del objetivo de la asistencia en el Greenbook: 1 = económica, "
        "2 = militar",
        dic="yes",
        orig="Foreign Assistance Objective ID",
    ),
    col(
        "aid_type_group_id",
        "STRING",
        "Code of the OECD/DAC aid type group (budget support, core "
        "contributions, project-type, technical assistance, debt relief, "
        "administrative costs, other)",
        "Código do grupo de tipo de ajuda da OCDE/CAD (apoio orçamentário, "
        "contribuições básicas, tipo projeto, assistência técnica, alívio de "
        "dívida, custos administrativos, outros)",
        "Código del grupo de tipo de ayuda de la OCDE/CAD (apoyo "
        "presupuestario, contribuciones básicas, tipo proyecto, asistencia "
        "técnica, alivio de deuda, costos administrativos, otros)",
        dic="yes",
        orig="Aid Type Group ID",
    ),
    col(
        "aid_type_id",
        "STRING",
        "Code of the OECD/DAC aid type, the level below the aid type group",
        "Código do tipo de ajuda da OCDE/CAD, nível abaixo do grupo de tipo "
        "de ajuda",
        "Código del tipo de ayuda de la OCDE/CAD, nivel inferior al grupo de "
        "tipo de ayuda",
        dic="yes",
        orig="aid_type_id",
    ),
    ACTIVITY_ID,
    col(
        "submission_id",
        "STRING",
        "Internal identifier of the data submission by a department, bureau, "
        "office or program",
        "Identificador interno da submissão de dados por um departamento, "
        "escritório ou programa",
        "Identificador interno del envío de datos por un departamento, oficina "
        "o programa",
        orig="Submission ID",
    ),
    ACTIVITY_NAME,
    ACTIVITY_DESCRIPTION,
    col(
        "activity_project_number",
        "STRING",
        "Award or project number used by the reporting agency's financial "
        "system to track the activity",
        "Número do prêmio ou projeto usado pelo sistema financeiro da agência "
        "declarante para acompanhar a atividade",
        "Número de adjudicación o proyecto usado por el sistema financiero de "
        "la agencia declarante para dar seguimiento a la actividad",
        orig="Activity Project Number",
    ),
    col(
        "activity_start_date",
        "DATE",
        "Start date of the implementing activity or mechanism",
        "Data de início da atividade ou mecanismo implementador",
        "Fecha de inicio de la actividad o mecanismo implementador",
        orig="Activity Start Date",
    ),
    col(
        "activity_end_date",
        "DATE",
        "End date of the implementing activity or mechanism",
        "Data de término da atividade ou mecanismo implementador",
        "Fecha de término de la actividad o mecanismo implementador",
        orig="Activity End Date",
    ),
    col(
        "activity_budget_amount",
        "FLOAT64",
        "Total expected cost or budget of the activity, when available",
        "Custo total esperado ou orçamento total da atividade, quando "
        "disponível",
        "Costo total esperado o presupuesto total de la actividad, cuando "
        "está disponible",
        unit="USD",
        orig="activity_budget_amount",
    ),
    CURRENT_AMOUNT,
    CONSTANT_AMOUNT,
]

# ---------------------------------------------------------------------------
# budget
# ---------------------------------------------------------------------------

BUDGET = [
    YEAR,
    TRANSACTION_TYPE,
    COUNTRY_ISO3,
    COUNTRY_ID,
    COUNTRY_CODE,
    REGION,
    INCOME_GROUP,
    MANAGING_SUBAGENCY,
    col(
        "operating_unit",
        "STRING",
        "Operating unit (country mission, regional or functional bureau "
        "program) that holds the budget line",
        "Unidade operacional (missão no país, programa de escritório regional "
        "ou funcional) responsável pela linha orçamentária",
        "Unidad operativa (misión en el país, programa de oficina regional o "
        "funcional) responsable de la línea presupuestaria",
        orig="Operating Unit",
    ),
    FUNDING_AGENCY,
    FUNDING_ACCOUNT,
    INTL_CATEGORY,
    INTL_SECTOR,
    INTL_PURPOSE,
    US_CATEGORY,
    US_SECTOR,
    col(
        "oco_flag",
        "STRING",
        "Overseas Contingency Operations flag: 1 when the budget line is OCO "
        "funding, empty otherwise",
        "Marcador de Operações de Contingência no Exterior (OCO): 1 quando a "
        "linha orçamentária é financiamento OCO, vazio caso contrário",
        "Marcador de Operaciones de Contingencia en el Exterior (OCO): 1 "
        "cuando la línea presupuestaria es financiamiento OCO, vacío en caso "
        "contrario",
        dic="yes",
        orig="OCO Flag",
    ),
    ACTIVITY_ID,
    ACTIVITY_NAME,
    ACTIVITY_DESCRIPTION,
    CURRENT_AMOUNT,
    CONSTANT_AMOUNT,
]

# ---------------------------------------------------------------------------
# dicionario
# ---------------------------------------------------------------------------

DICIONARIO = [
    col(
        "id_tabela",
        "STRING",
        "Slug of the table the dictionary entry describes",
        "Nome da tabela que a entrada do dicionário descreve",
        "Nombre de la tabla que describe la entrada del diccionario",
        orig="",
    ),
    col(
        "nome_coluna",
        "STRING",
        "Name of the coded column",
        "Nome da coluna codificada",
        "Nombre de la columna codificada",
    ),
    col(
        "chave",
        "STRING",
        "Coded value exactly as stored in the table",
        "Valor codificado exatamente como armazenado na tabela",
        "Valor codificado exactamente como se almacena en la tabla",
    ),
    col(
        "cobertura_temporal",
        "STRING",
        "Temporal coverage of the key",
        "Cobertura temporal da chave",
        "Cobertura temporal de la clave",
    ),
    col(
        "valor",
        "STRING",
        "Label corresponding to the coded value",
        "Rótulo correspondente ao valor codificado",
        "Etiqueta correspondiente al valor codificado",
    ),
]

TABLES = {
    "transaction": TRANSACTION,
    "budget": BUDGET,
    "dicionario": DICIONARIO,
}


def write_csvs() -> None:
    for table, cols in TABLES.items():
        names = [c["name"] for c in cols]
        assert len(names) == len(set(names)), f"duplicate column in {table}"
        path = HERE / f"{DATASET}__{table}.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writeheader()
            for c in cols:
                w.writerow(c)
        print(f"wrote {path.name}: {len(cols)} columns")


if __name__ == "__main__":
    write_csvs()
