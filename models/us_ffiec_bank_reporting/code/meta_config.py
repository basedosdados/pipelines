"""Dataset-level metadata: names, descriptions, sources, grains, coverage.

Separate from register_metadata.py so the prose is reviewable on its own.
"""

from __future__ import annotations

DATASET_SLUG = "bank_reporting"
GCP_DATASET = "us_ffiec_bank_reporting"

TABLE_ORDER = [
    "institution",
    "call_report_item",
    "holding_company",
    "holding_company_item",
    "mdrm_item",
    "cra_respondent",
    "cra_lending",
    "cra_assessment_area_tract",
    "dictionary",
]

NAMES = {
    "institution": ("Instituições", "Institutions", "Instituciones"),
    "call_report_item": (
        "Itens do Call Report",
        "Call Report Items",
        "Partidas del Call Report",
    ),
    "holding_company": (
        "Holdings Bancárias",
        "Holding Companies",
        "Sociedades Controladoras",
    ),
    "holding_company_item": (
        "Itens do FR Y-9C",
        "FR Y-9C Items",
        "Partidas del FR Y-9C",
    ),
    "mdrm_item": (
        "Dicionário de Itens MDRM",
        "MDRM Item Dictionary",
        "Diccionario de Partidas MDRM",
    ),
    "cra_respondent": (
        "Declarantes do CRA",
        "CRA Respondents",
        "Declarantes del CRA",
    ),
    "cra_lending": (
        "Crédito a Pequenas Empresas e Produtores Rurais (CRA)",
        "Small Business and Small Farm Lending (CRA)",
        "Crédito a Pequeñas Empresas y Productores Agrícolas (CRA)",
    ),
    "cra_assessment_area_tract": (
        "Setores Censitários das Áreas de Avaliação (CRA)",
        "Assessment Area Census Tracts (CRA)",
        "Sectores Censales de las Áreas de Evaluación (CRA)",
    ),
    "dictionary": ("Dicionário", "Dictionary", "Diccionario"),
}

DESCRIPTIONS = {
    "institution": (
        "Cadastro de todos os bancos que entregaram o Call Report, uma linha por "
        "instituição e trimestre de referência. Reúne os identificadores que ligam o "
        "banco entre reguladores: o RSSD do Federal Reserve, o certificado FDIC que "
        "faz a junção com us_fdic_bankfind, o número da carta patente do OCC e o "
        "antigo registro no OTS.",
        "Roster of every bank that filed a Call Report, one row per institution and "
        "reporting quarter. Carries the identifiers that link the bank across "
        "regulators: the Federal Reserve RSSD, the FDIC certificate that joins to "
        "us_fdic_bankfind, the OCC charter number and the former OTS docket number.",
        "Registro de todos los bancos que presentaron el Call Report, una fila por "
        "institución y trimestre de referencia. Reúne los identificadores que "
        "vinculan al banco entre reguladores: el RSSD de la Reserva Federal, el "
        "certificado FDIC que permite la unión con us_fdic_bankfind, el número de "
        "licencia de la OCC y el antiguo registro en la OTS.",
    ),
    "call_report_item": (
        "Todos os valores declarados por todos os bancos no Call Report trimestral, "
        "uma linha por instituição, trimestre e código de item do MDRM. O formato é "
        "longo e não largo porque o conjunto de itens muda a cada poucos trimestres, "
        "com linhas criadas e descontinuadas. Junte mdrm_item por item_code para "
        "obter o nome, o tipo e a unidade de cada item. Valores monetários estão em "
        "dólares, convertidos dos milhares em que o FFIEC publica.",
        "Every value every bank reported on its quarterly Call Report, one row per "
        "institution, quarter and MDRM item code. Long rather than wide because the "
        "item set changes every few quarters as lines are added and retired. Join "
        "mdrm_item on item_code for the item's name, type and unit. Monetary values "
        "are in dollars, converted from the thousands the FFIEC publishes.",
        "Todos los valores declarados por todos los bancos en el Call Report "
        "trimestral, una fila por institución, trimestre y código de partida del "
        "MDRM. El formato es largo y no ancho porque el conjunto de partidas cambia "
        "cada pocos trimestres, con líneas creadas y descontinuadas. Una mdrm_item "
        "por item_code para obtener el nombre, el tipo y la unidad de cada partida. "
        "Los valores monetarios están en dólares, convertidos de los miles en que "
        "publica el FFIEC.",
    ),
    "holding_company": (
        "Cadastro de todas as holdings bancárias que entregaram os relatórios "
        "financeiros ao Federal Reserve, uma linha por empresa e trimestre de "
        "referência, com razão social, localização e códigos de estrutura "
        "societária. Os trimestres de junho e dezembro trazem cerca de 4.200 "
        "empresas e os de março e setembro cerca de 420, porque o FR Y-9SP das "
        "holdings menores é semestral enquanto o FR Y-9C é trimestral.",
        "Roster of every bank holding company that filed financial reports with the "
        "Federal Reserve, one row per company and reporting quarter, with its name, "
        "location and structure codes. June and December quarters carry about 4,200 "
        "companies and March and September about 420, because the FR Y-9SP filed by "
        "smaller holding companies is semiannual while the FR Y-9C is quarterly.",
        "Registro de todas las sociedades controladoras de bancos que presentaron "
        "informes financieros a la Reserva Federal, una fila por empresa y trimestre "
        "de referencia, con razón social, ubicación y códigos de estructura "
        "societaria. Los trimestres de junio y diciembre traen cerca de 4.200 "
        "empresas y los de marzo y septiembre cerca de 420, porque el FR Y-9SP de "
        "las sociedades menores es semestral mientras el FR Y-9C es trimestral.",
    ),
    "holding_company_item": (
        "Todos os valores declarados pelas holdings bancárias nos relatórios "
        "financeiros ao Federal Reserve, uma linha por empresa, trimestre e código "
        "de item do MDRM. Abrange dois formulários: o FR Y-9C consolidado "
        "trimestral, cujos códigos começam por BHCK, BHCA ou BHDM, e o FR Y-9SP "
        "semestral das holdings menores, cujos códigos começam por BHSP. Junte "
        "mdrm_item por item_code para obter o nome, o tipo, a unidade e o "
        "formulário de cada item. Valores monetários estão em dólares, convertidos "
        "dos milhares publicados.",
        "Every value bank holding companies reported on their financial filings with "
        "the Federal Reserve, one row per company, quarter and MDRM item code. It "
        "spans two forms: the quarterly consolidated FR Y-9C, whose codes begin with "
        "BHCK, BHCA or BHDM, and the semiannual FR Y-9SP filed by smaller holding "
        "companies, whose codes begin with BHSP. Join mdrm_item on item_code for the "
        "item's name, type, unit and reporting form. Monetary values are in dollars, "
        "converted from the published thousands.",
        "Todos los valores declarados por las sociedades controladoras de bancos en "
        "sus informes financieros a la Reserva Federal, una fila por empresa, "
        "trimestre y código de partida del MDRM. Abarca dos formularios: el FR Y-9C "
        "consolidado trimestral, cuyos códigos comienzan con BHCK, BHCA o BHDM, y el "
        "FR Y-9SP semestral de las sociedades menores, cuyos códigos comienzan con "
        "BHSP. Una mdrm_item por item_code para obtener el nombre, el tipo, la "
        "unidad y el formulario de cada partida. Los valores monetarios están en "
        "dólares, convertidos de los miles publicados.",
    ),
    "mdrm_item": (
        "O Micro Data Reference Manual do Federal Reserve: o catálogo oficial de "
        "todos os códigos de item coletados em todos os relatórios regulatórios, e a "
        "tabela de correspondência que torna call_report_item e holding_company_item "
        "legíveis. A coluna measurement_unit é derivada aqui e não publicada pelo "
        "MDRM, que não separa valores monetários de contagens.",
        "The Federal Reserve's Micro Data Reference Manual: the authoritative "
        "catalogue of every item code collected on every regulatory report, and the "
        "crosswalk that makes call_report_item and holding_company_item readable. "
        "The measurement_unit column is derived here and is not published by the "
        "MDRM, which does not separate dollar amounts from counts.",
        "El Micro Data Reference Manual de la Reserva Federal: el catálogo oficial "
        "de todos los códigos de partida recolectados en todos los informes "
        "regulatorios, y la tabla de correspondencia que hace legibles "
        "call_report_item y holding_company_item. La columna measurement_unit se "
        "deriva aquí y no la publica el MDRM, que no separa montos monetarios de "
        "conteos.",
    ),
    "cra_respondent": (
        "Instituições que entregaram dados do CRA em cada ano, a partir da folha de "
        "transmissão. É o único arquivo do CRA que traz o identificador RSSD e "
        "portanto a ponte entre as tabelas do CRA e o restante deste conjunto: o "
        "identificador do declarante do CRA é atribuído pela agência supervisora e "
        "não é um RSSD.",
        "The institutions that filed CRA data each year, from the transmittal sheet. "
        "The only CRA file carrying the RSSD identifier, and therefore the bridge "
        "between the CRA tables and the rest of this dataset: the CRA respondent "
        "identifier is assigned by the supervising agency and is not an RSSD.",
        "Las instituciones que presentaron datos del CRA cada año, a partir de la "
        "hoja de transmisión. Es el único archivo del CRA que trae el identificador "
        "RSSD y por lo tanto el puente entre las tablas del CRA y el resto de este "
        "conjunto: el identificador del declarante del CRA lo asigna la agencia "
        "supervisora y no es un RSSD.",
    ),
    "cra_lending": (
        "Crédito a pequenas empresas e a pequenos produtores rurais divulgado sob o "
        "Community Reinvestment Act, por instituição, ano, condado e faixa de renda "
        "dos setores censitários, dividido em faixas de valor do empréstimo e de "
        "receita do tomador. Declarado apenas por instituições de grande porte, e "
        "agregado pelo FFIEC ao nível de condado: os arquivos de divulgação não "
        "trazem montantes por setor censitário.",
        "Small business and small farm lending disclosed under the Community "
        "Reinvestment Act, by institution, year, county and census tract income "
        "group, split into loan size and borrower revenue bands. Reported only by "
        "large institutions, and aggregated by the FFIEC to county level: the "
        "disclosure files carry no per-tract loan amounts.",
        "Crédito a pequeñas empresas y a pequeños productores agrícolas divulgado "
        "bajo el Community Reinvestment Act, por institución, año, condado y grupo "
        "de ingreso de los sectores censales, dividido en rangos de monto del "
        "préstamo y de ingresos del prestatario. Declarado solo por instituciones de "
        "gran porte, y agregado por el FFIEC a nivel de condado: los archivos de "
        "divulgación no traen montos por sector censal.",
    ),
    "cra_assessment_area_tract": (
        "Setores censitários que compõem as áreas de avaliação do CRA de cada "
        "instituição, uma linha por instituição, ano e setor. É a contrapartida ao "
        "nível de setor censitário de cra_lending, que só é publicada por condado.",
        "The census tracts that make up each institution's CRA assessment areas, one "
        "row per institution, year and tract. The tract-level companion to "
        "cra_lending, which is only published at county level.",
        "Sectores censales que componen las áreas de evaluación del CRA de cada "
        "institución, una fila por institución, año y sector. Es la contraparte a "
        "nivel de sector censal de cra_lending, que solo se publica por condado.",
    ),
    "dictionary": (
        "Dicionário de valores codificados das colunas deste conjunto de dados",
        "Dictionary of the coded values of the columns in this dataset",
        "Diccionario de los valores codificados de las columnas de este conjunto de datos",
    ),
}

ORGANIZATION = {
    "slug": "ffiec",
    "name": "Federal Financial Institutions Examination Council (FFIEC)",
    "description_pt": (
        "Órgão interagências do governo dos Estados Unidos que uniformiza os "
        "princípios, padrões e formulários de supervisão das instituições "
        "financeiras federais. Reúne o Federal Reserve, a FDIC, o OCC, a NCUA e o "
        "CFPB, e publica os Call Reports trimestrais de todos os bancos, os dados de "
        "divulgação do Community Reinvestment Act e do Home Mortgage Disclosure Act."
    ),
    "description_en": (
        "Interagency body of the United States government that prescribes uniform "
        "principles, standards and report forms for the federal examination of "
        "financial institutions. It brings together the Federal Reserve, the FDIC, "
        "the OCC, the NCUA and the CFPB, and publishes the quarterly Call Reports of "
        "every bank, the Community Reinvestment Act disclosure data and the Home "
        "Mortgage Disclosure Act data."
    ),
    "description_es": (
        "Órgano interinstitucional del gobierno de los Estados Unidos que uniforma "
        "los principios, normas y formularios de supervisión de las instituciones "
        "financieras federales. Reúne a la Reserva Federal, la FDIC, la OCC, la NCUA "
        "y el CFPB, y publica los Call Reports trimestrales de todos los bancos, los "
        "datos de divulgación del Community Reinvestment Act y del Home Mortgage "
        "Disclosure Act."
    ),
    "website": "https://www.ffiec.gov/",
}

DATASET_NAME = (
    "Relatórios Regulatórios Bancários (Call Report, FR Y-9C e CRA)",
    "Bank Regulatory Reporting (Call Report, FR Y-9C and CRA)",
    "Informes Regulatorios Bancarios (Call Report, FR Y-9C y CRA)",
)

DATASET_DESCRIPTION = (
    "Os relatórios regulatórios completos do sistema bancário dos Estados Unidos: "
    "os anexos trimestrais do Call Report de todos os bancos (2009-2026), as "
    "demonstrações consolidadas FR Y-9C das holdings bancárias (1986-2026) e a "
    "divulgação anual de crédito a pequenas empresas e pequenos produtores rurais "
    "do Community Reinvestment Act (1996-2024). Aprofunda o núcleo de finanças em "
    "vez de duplicá-lo: us_fdic_bankfind traz o resumo curado da FDIC, este "
    "conjunto traz os anexos regulatórios completos que estão por baixo dele, e o "
    "arquivo do CRA por setor censitário é a contrapartida de crédito a pequenas "
    "empresas dos dados hipotecários de us_cfpb_hmda. Todos os itens são "
    "identificados pelo código do MDRM, o dicionário oficial do Federal Reserve, "
    "que acompanha o conjunto como tabela própria. O identificador RSSD é a chave "
    "de junção entre todas as tabelas deste conjunto; a ponte para us_fdic_bankfind "
    "é a coluna institution.fdic_cert_id, que carrega o certificado da FDIC.",
    "The full regulatory filings of the United States banking system: the quarterly "
    "Call Report schedules of every bank (2009-2026), the FR Y-9C consolidated "
    "financial statements of bank holding companies (1986-2026), and the annual "
    "Community Reinvestment Act disclosure of small business and small farm lending "
    "(1996-2024). This deepens the finance cluster rather than duplicating it: "
    "us_fdic_bankfind carries the FDIC's curated summary, this carries the full "
    "regulatory schedules underneath it, and the CRA tract-level lending file is the "
    "small-business-credit counterpart to the already-onboarded us_cfpb_hmda "
    "mortgage data. Every item is identified by its MDRM code, the Federal Reserve's "
    "official data dictionary, which ships with the dataset as its own table. The "
    "RSSD identifier is the join key across every table in this dataset; the bridge "
    "to us_fdic_bankfind is institution.fdic_cert_id, which carries the FDIC "
    "certificate.",
    "Los informes regulatorios completos del sistema bancario de los Estados Unidos: "
    "los anexos trimestrales del Call Report de todos los bancos (2009-2026), los "
    "estados consolidados FR Y-9C de las sociedades controladoras de bancos "
    "(1986-2026) y la divulgación anual de crédito a pequeñas empresas y pequeños "
    "productores agrícolas del Community Reinvestment Act (1996-2024). Profundiza el "
    "núcleo de finanzas en lugar de duplicarlo: us_fdic_bankfind trae el resumen "
    "curado de la FDIC, este conjunto trae los anexos regulatorios completos que "
    "están debajo, y el archivo del CRA por sector censal es la contraparte de "
    "crédito a pequeñas empresas de los datos hipotecarios de us_cfpb_hmda. Todas "
    "las partidas se identifican por el código del MDRM, el diccionario oficial de "
    "la Reserva Federal, que acompaña al conjunto como tabla propia. El "
    "identificador RSSD es la clave de unión entre todas las tablas de este "
    "conjunto; el puente hacia us_fdic_bankfind es institution.fdic_cert_id, que "
    "lleva el certificado de la FDIC.",
)

TAGS = [
    "banking",
    "financial-institution",
    "credit",
    "loan",
    "balance-sheet",
    "deposit",
    "bank-supervision",
]
# Tags that do not exist on the backend yet and are created by
# register_metadata.py. Slugs are English kebab-case; names are lower case in
# all three languages. Flagged to the user before creation, per
# .claude/rules/metadata-schema.md.
NEW_TAGS: dict[str, tuple[str, str, str]] = {
    "regulatory-reporting": (
        "relatório regulatório",
        "regulatory reporting",
        "informe regulatorio",
    ),
    "small-business": (
        "pequena empresa",
        "small business",
        "pequeña empresa",
    ),
}

SOURCES = {
    "call": {
        "name": (
            "FFIEC Central Data Repository — dados em massa do Call Report",
            "FFIEC Central Data Repository — Call Report bulk data",
            "FFIEC Central Data Repository — datos masivos del Call Report",
        ),
        "url": "https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx",
        "description": (
            "Distribuição pública do Central Data Repository do FFIEC, que publica "
            "os anexos do Call Report de cada trimestre em arquivos delimitados por "
            "tabulação, um por anexo, com uma linha por instituição e uma coluna por "
            "código de item do MDRM. O repositório anuncia períodos desde 2001, mas "
            "devolve arquivos vazios para todos os trimestres anteriores a 2007 e "
            "para seis trimestres de 2007 a 2009.",
            "The FFIEC Central Data Repository's public distribution, which publishes "
            "each quarter's Call Report schedules as tab-delimited files, one per "
            "schedule, with one row per institution and one column per MDRM item "
            "code. The repository advertises periods back to 2001 but returns empty "
            "files for every quarter before 2007 and for six quarters between 2007 "
            "and 2009.",
            "Distribución pública del Central Data Repository del FFIEC, que publica "
            "los anexos del Call Report de cada trimestre en archivos delimitados "
            "por tabulación, uno por anexo, con una fila por institución y una "
            "columna por código de partida del MDRM. El repositorio anuncia períodos "
            "desde 2001, pero devuelve archivos vacíos para todos los trimestres "
            "anteriores a 2007 y para seis trimestres entre 2007 y 2009.",
        ),
    },
    "mdrm": {
        "name": (
            "Micro Data Reference Manual (MDRM) do Federal Reserve",
            "Federal Reserve Micro Data Reference Manual (MDRM)",
            "Micro Data Reference Manual (MDRM) de la Reserva Federal",
        ),
        "url": "https://www.federalreserve.gov/apps/mdrm/",
        "description": (
            "Dicionário de dados oficial do Federal Reserve, que define cada código "
            "de item de oito caracteres coletado em cada relatório regulatório, com "
            "nome, definição, tipo, formulários que o coletam e período de coleta. É "
            "a fonte autoritativa usada aqui para nomear os itens.",
            "The Federal Reserve's official data dictionary, defining every "
            "eight-character item code collected on every regulatory report, with "
            "its name, definition, type, the forms that collect it and the period "
            "over which it was collected. It is the authoritative source used here "
            "to name the items.",
            "Diccionario de datos oficial de la Reserva Federal, que define cada "
            "código de partida de ocho caracteres recolectado en cada informe "
            "regulatorio, con nombre, definición, tipo, formularios que lo "
            "recolectan y período de recolección. Es la fuente autoritativa usada "
            "aquí para nombrar las partidas.",
        ),
    },
    "bhc": {
        "name": (
            "FR Y-9C — dados financeiros do National Information Center",
            "FR Y-9C — National Information Center financial data",
            "FR Y-9C — datos financieros del National Information Center",
        ),
        "url": "https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload",
        "description": (
            "Demonstrações financeiras consolidadas trimestrais das holdings "
            "bancárias no formulário FR Y-9C, publicadas pelo National Information "
            "Center em um arquivo por trimestre, com uma linha por empresa e uma "
            "coluna por código de item do MDRM. Cobre de 2000 em diante; os "
            "trimestres de 1986 a 1999 vêm do arquivo histórico do Federal Reserve "
            "Bank of Chicago.",
            "Quarterly consolidated financial statements of bank holding companies "
            "on form FR Y-9C, published by the National Information Center as one "
            "file per quarter, with one row per company and one column per MDRM item "
            "code. Covers 2000 onward; the 1986 to 1999 quarters come from the "
            "Federal Reserve Bank of Chicago's historical archive.",
            "Estados financieros consolidados trimestrales de las sociedades "
            "controladoras de bancos en el formulario FR Y-9C, publicados por el "
            "National Information Center en un archivo por trimestre, con una fila "
            "por empresa y una columna por código de partida del MDRM. Cubre desde "
            "2000; los trimestres de 1986 a 1999 provienen del archivo histórico del "
            "Federal Reserve Bank of Chicago.",
        ),
    },
    "cra": {
        "name": (
            "CRA — arquivos planos de divulgação e agregados",
            "CRA aggregate and disclosure flat files",
            "CRA — archivos planos de divulgación y agregados",
        ),
        "url": "https://www.ffiec.gov/data/cra/flat-files",
        "description": (
            "Arquivos planos anuais de divulgação do Community Reinvestment Act, com "
            "o crédito a pequenas empresas e a pequenos produtores rurais declarado "
            "por instituições de grande porte, agregado por condado e faixa de renda "
            "dos setores censitários, e a composição das áreas de avaliação por setor "
            "censitário. Registros de largura fixa, com a especificação de layout "
            "publicada em PDF a cada ano.",
            "Annual Community Reinvestment Act disclosure flat files, carrying the "
            "small business and small farm lending reported by large institutions, "
            "aggregated by county and census tract income group, and the composition "
            "of each assessment area by census tract. Fixed-width records, with the "
            "layout specification published as a PDF each year.",
            "Archivos planos anuales de divulgación del Community Reinvestment Act, "
            "con el crédito a pequeñas empresas y a pequeños productores agrícolas "
            "declarado por instituciones de gran porte, agregado por condado y grupo "
            "de ingreso de los sectores censales, y la composición de las áreas de "
            "evaluación por sector censal. Registros de ancho fijo, con la "
            "especificación de layout publicada en PDF cada año.",
        ),
    },
}

# Exactly one raw source per table: client._raw_source_id raises on two or more,
# which would break the recurring pipeline's poll on its first run.
TABLE_SOURCE = {
    "institution": "call",
    "call_report_item": "call",
    "holding_company": "bhc",
    "holding_company_item": "bhc",
    "mdrm_item": "mdrm",
    "cra_respondent": "cra",
    "cra_lending": "cra",
    "cra_assessment_area_tract": "cra",
    "dictionary": "mdrm",
}

# The columns that identify each grain. Without these links the site renders the
# observation level's columns as "Não informado".
GRAIN = {
    "institution": {"company": ["rssd_id"], "quarter": ["year", "quarter"]},
    "call_report_item": {
        "company": ["rssd_id"],
        "quarter": ["year", "quarter"],
        "series": ["item_code"],
    },
    "holding_company": {
        "company": ["rssd_id"],
        "quarter": ["year", "quarter"],
    },
    "holding_company_item": {
        "company": ["rssd_id"],
        "quarter": ["year", "quarter"],
        "series": ["item_code"],
    },
    "mdrm_item": {"series": ["item_code"]},
    "cra_respondent": {"company": ["rssd_id"], "year": ["year"]},
    "cra_lending": {
        "company": ["rssd_id"],
        "year": ["year"],
        "county": ["county_id"],
    },
    "cra_assessment_area_tract": {
        "company": ["rssd_id"],
        "year": ["year"],
        "census_tract": ["census_tract_id"],
    },
    "dictionary": {},
}

PARTITION = {
    table: "year"
    for table in TABLE_ORDER
    if table not in ("mdrm_item", "dictionary")
}

# (start year, start month, end year, end month); None for a table with no
# temporal extent of its own.
COVERAGE = {
    "institution": (2009, 9, 2026, 6),
    "call_report_item": (2009, 9, 2026, 6),
    "holding_company": (1986, 9, 2026, 6),
    "holding_company_item": (1986, 9, 2026, 6),
    "cra_respondent": (1996, None, 2024, None),
    "cra_lending": (1996, None, 2024, None),
    "cra_assessment_area_tract": (1996, None, 2024, None),
    "mdrm_item": None,
    "dictionary": None,
}

# Update entity per table: quarterly filings vs the annual CRA release.
UPDATE_ENTITY = {
    "institution": "quarter",
    "call_report_item": "quarter",
    "holding_company": "quarter",
    "holding_company_item": "quarter",
    "mdrm_item": "quarter",
    "cra_respondent": "year",
    "cra_lending": "year",
    "cra_assessment_area_tract": "year",
    "dictionary": "year",
}

# What the SOURCE last published -- a coverage date, never today's date.
SOURCE_LATEST = {
    "call": "2026-06-30T00:00:00",
    "bhc": "2026-06-30T00:00:00",
    "cra": "2024-12-31T00:00:00",
    "mdrm": "2026-09-11T00:00:00",
}
