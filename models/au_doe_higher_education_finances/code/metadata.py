"""Dataset and table metadata for the Data Basis backend, in three languages.

Kept beside the architecture so the descriptions registered in the backend and
the ones in the dbt schema come from one place and can be re-registered without
being retyped.
"""

DATASET = {
    "slug": "au_doe_higher_education_finances",
    "name_pt": "Finanças do Ensino Superior da Austrália",
    "name_en": "Australian Higher Education Finances",
    "name_es": "Finanzas de la Educación Superior de Australia",
    "description_pt": (
        "Demonstrações financeiras auditadas e receita de pesquisa dos provedores de "
        "ensino superior da Austrália, publicadas pelo Departamento de Educação. As "
        "quatro demonstrações (resultado, posição patrimonial, mutações do patrimônio "
        "líquido e fluxo de caixa) vêm da Finance Publication; a receita de pesquisa "
        "por categoria vem da Higher Education Research Data Collection (HERDC), que "
        "embasa a distribuição dos research block grants. Todos os valores estão em "
        "dólares australianos. Fonte: education.gov.au, licenciado sob CC BY 4.0."
    ),
    "description_en": (
        "Audited financial statements and research income of Australian higher "
        "education providers, published by the Department of Education. The four "
        "statements (financial performance, financial position, changes in equity and "
        "cash flows) come from the Finance Publication; research income by category "
        "comes from the Higher Education Research Data Collection (HERDC), which "
        "underpins the allocation of research block grants. All amounts are in "
        "Australian dollars. Source: education.gov.au, licensed CC BY 4.0."
    ),
    "description_es": (
        "Estados financieros auditados e ingresos de investigación de los proveedores "
        "de educación superior de Australia, publicados por el Departamento de "
        "Educación. Los cuatro estados (resultado, posición financiera, cambios en el "
        "patrimonio y flujos de efectivo) provienen de la Finance Publication; los "
        "ingresos de investigación por categoría provienen de la Higher Education "
        "Research Data Collection (HERDC), que sustenta la asignación de los research "
        "block grants. Todos los montos están en dólares australianos. Fuente: "
        "education.gov.au, con licencia CC BY 4.0."
    ),
}

TABLES = {
    "income_statement": {
        "name_pt": "Demonstração do Resultado",
        "name_en": "Income Statement",
        "name_es": "Estado de Resultados",
        "description_pt": (
            "Demonstração ajustada do resultado de cada provedor de ensino superior, "
            "com uma linha por provedor, ano, setor e rubrica de receita ou despesa. "
            "Provedores de setor duplo também reportam a separação entre ensino "
            "superior e educação profissional."
        ),
        "description_en": (
            "Adjusted statement of financial performance of each higher education "
            "provider, one row per provider, year, sector and revenue or expense line. "
            "Dual-sector providers additionally report a higher education and "
            "vocational education split."
        ),
        "description_es": (
            "Estado ajustado de resultados de cada proveedor de educación superior, "
            "con una fila por proveedor, año, sector y partida de ingreso o gasto. Los "
            "proveedores de sector dual también reportan la separación entre educación "
            "superior y educación vocacional."
        ),
    },
    "balance_sheet": {
        "name_pt": "Balanço Patrimonial",
        "name_en": "Balance Sheet",
        "name_es": "Balance General",
        "description_pt": (
            "Demonstração ajustada da posição patrimonial de cada provedor de ensino "
            "superior, com uma linha por provedor, ano e rubrica de ativo, passivo ou "
            "patrimônio líquido."
        ),
        "description_en": (
            "Adjusted statement of financial position of each higher education "
            "provider, one row per provider, year and asset, liability or equity line."
        ),
        "description_es": (
            "Estado ajustado de la posición financiera de cada proveedor de educación "
            "superior, con una fila por proveedor, año y partida de activo, pasivo o "
            "patrimonio."
        ),
    },
    "equity_movement": {
        "name_pt": "Mutações do Patrimônio Líquido",
        "name_en": "Changes in Equity",
        "name_es": "Cambios en el Patrimonio",
        "description_pt": (
            "Demonstração ajustada das mutações do patrimônio líquido e do resultado "
            "abrangente de cada provedor de ensino superior, com uma linha por "
            "provedor, ano e rubrica de movimentação."
        ),
        "description_en": (
            "Adjusted statement of changes in equity and comprehensive income of each "
            "higher education provider, one row per provider, year and movement line."
        ),
        "description_es": (
            "Estado ajustado de cambios en el patrimonio y resultado integral de cada "
            "proveedor de educación superior, con una fila por proveedor, año y "
            "partida de movimiento."
        ),
    },
    "cash_flow": {
        "name_pt": "Fluxo de Caixa",
        "name_en": "Cash Flow Statement",
        "name_es": "Estado de Flujos de Efectivo",
        "description_pt": (
            "Demonstração ajustada dos fluxos de caixa de cada provedor de ensino "
            "superior, com uma linha por provedor, ano e rubrica de fluxo de caixa."
        ),
        "description_en": (
            "Adjusted statement of cash flows of each higher education provider, one "
            "row per provider, year and cash flow line."
        ),
        "description_es": (
            "Estado ajustado de flujos de efectivo de cada proveedor de educación "
            "superior, con una fila por proveedor, año y partida de flujo de efectivo."
        ),
    },
    "research_income": {
        "name_pt": "Receita de Pesquisa",
        "name_en": "Research Income",
        "name_es": "Ingresos de Investigación",
        "description_pt": (
            "Receita de pesquisa e desenvolvimento de cada provedor de ensino "
            "superior, com uma linha por provedor, ano, categoria e subcategoria. "
            "Coletada pela Higher Education Research Data Collection (HERDC) e usada "
            "na distribuição dos research block grants. Um valor nulo indica que a "
            "subcategoria não estava em uso naquele ano, o que difere de um valor zero."
        ),
        "description_en": (
            "Research and development income of each higher education provider, one "
            "row per provider, year, category and sub-category. Collected through the "
            "Higher Education Research Data Collection (HERDC) and used to allocate "
            "research block grants. A null amount means the sub-category was not in use "
            "that year, which differs from a reported nil."
        ),
        "description_es": (
            "Ingresos de investigación y desarrollo de cada proveedor de educación "
            "superior, con una fila por proveedor, año, categoría y subcategoría. "
            "Recopilados por la Higher Education Research Data Collection (HERDC) y "
            "usados para asignar los research block grants. Un valor nulo indica que la "
            "subcategoría no estaba en uso ese año, lo que difiere de un cero reportado."
        ),
    },
    "line_item": {
        "name_pt": "Rubricas das Demonstrações",
        "name_en": "Statement Line Items",
        "name_es": "Partidas de los Estados",
        "description_pt": (
            "Quais rótulos de rubrica aparecem em cada demonstração e em quais anos. O "
            "Departamento de Educação renomeia rubricas ao longo da série, de modo que "
            "um painel longo construído sobre um único rótulo pode truncar sem aviso."
        ),
        "description_en": (
            "Which line item labels appear in each financial statement and over which "
            "years. The Department of Education relabels lines across the series, so a "
            "long panel built on a single label can truncate without warning."
        ),
        "description_es": (
            "Qué etiquetas de partida aparecen en cada estado financiero y en qué años. "
            "El Departamento de Educación renombra partidas a lo largo de la serie, por "
            "lo que un panel largo construido sobre una sola etiqueta puede truncarse "
            "sin aviso."
        ),
    },
    "dicionario": {
        "name_pt": "Dicionário",
        "name_en": "Dictionary",
        "name_es": "Diccionario",
        "description_pt": (
            "Dicionário dos valores codificados usados nas tabelas do conjunto."
        ),
        "description_en": (
            "Dictionary of the coded values used across the dataset's tables."
        ),
        "description_es": (
            "Diccionario de los valores codificados usados en las tablas del conjunto."
        ),
    },
}

ORGANIZATION = {
    "slug": "au_doe",
    "name_pt": "Departamento de Educação da Austrália (DoE)",
    "name_en": "Australian Department of Education (DoE)",
    "name_es": "Departamento de Educación de Australia (DoE)",
    "website": "https://www.education.gov.au",
}

RAW_DATA_SOURCES = [
    {
        "name_pt": "Relatórios Financeiros dos Provedores de Ensino Superior",
        "name_en": "Financial Reports of Higher Education Providers",
        "name_es": "Informes Financieros de los Proveedores de Educación Superior",
        "url": "https://www.education.gov.au/higher-education-publications/finance-publication",
        "tables": [
            "income_statement",
            "balance_sheet",
            "equity_movement",
            "cash_flow",
        ],
    },
    {
        "name_pt": "Coleta de Dados de Pesquisa do Ensino Superior (HERDC)",
        "name_en": "Higher Education Research Data Collection (HERDC)",
        "name_es": "Recopilación de Datos de Investigación de Educación Superior (HERDC)",
        "url": "https://www.education.gov.au/research-block-grants/higher-education-research-data-collection",
        "tables": ["research_income"],
    },
]
