"""Backend metadata for us_cms_hcris: descriptions, references and coverage.

Kept apart from ``register.py`` so the prose is reviewable on its own and the
registration script stays mechanical.
"""

DATASET_SLUG = "hcris"
GCP_DATASET_ID = "us_cms_hcris"
ORGANIZATION = "centers_for_medicare_medicaid_services_cms"
THEMES = ["health", "economics"]

# Existing tags, checked against the backend's 873-tag vocabulary.
TAGS_EXISTING = [
    "hospital",
    "health-facilities",
    "accounting",
    "balance-sheet",
    "financial-statement",
]
# Tags that do not exist yet and are created on registration. Slugs are English
# kebab-case; names are lowercase in all three languages.
TAGS_NEW = {
    "medicare": ("medicare", "medicare", "medicare"),
    "uncompensated-care": (
        "atendimento não remunerado",
        "uncompensated care",
        "atención no remunerada",
    ),
}

DATASET_NAME = (
    "Relatórios de Custos de Hospitais (HCRIS)",
    "Hospital Cost Reports (HCRIS)",
    "Informes de Costos de Hospitales (HCRIS)",
)

DATASET_DESCRIPTION = (
    "Relatórios anuais de custos do Medicare apresentados por todos os "
    "hospitais certificados pelo Medicare nos Estados Unidos, do exercício "
    "fiscal de 1996 em diante, extraídos do Healthcare Cost Report Information "
    "System (HCRIS) da CMS. Cada relatório declara receitas, despesas, "
    "cobranças, leitos, altas, atendimento filantrópico e não remunerado e o "
    "balanço patrimonial da instituição. O conjunto é publicado em duas "
    "camadas: a forma bruta do formulário, em que cada célula declarada é uma "
    "linha, e uma camada tratada com as medidas nomeadas de uso corrente, cada "
    "uma rastreável até a planilha, linha e coluna de origem.",
    "Annual Medicare cost reports filed by every Medicare-certified hospital "
    "in the United States from fiscal year 1996 onwards, drawn from CMS's "
    "Healthcare Cost Report Information System (HCRIS). Each report states the "
    "institution's revenues, expenses, charges, bed counts, discharges, "
    "charity and uncompensated care, and balance sheet. The dataset is "
    "published in two layers: the raw form, in which every reported cell is a "
    "row, and a curated layer of the named measures in common use, each "
    "traceable to the worksheet, line and column it is read from.",
    "Informes anuales de costos de Medicare presentados por todos los "
    "hospitales certificados por Medicare en Estados Unidos desde el ejercicio "
    "fiscal 1996, extraídos del Healthcare Cost Report Information System "
    "(HCRIS) de CMS. Cada informe declara ingresos, gastos, cargos, camas, "
    "altas, atención de caridad y no remunerada, y el balance de la "
    "institución. El conjunto se publica en dos capas: la forma cruda del "
    "formulario, donde cada celda declarada es una fila, y una capa curada con "
    "las medidas nombradas de uso corriente, cada una rastreable hasta la "
    "hoja, línea y columna de origen.",
)

RAW_SOURCE = {
    "name": (
        "Relatórios de custos por exercício fiscal",
        "Cost Reports by Fiscal Year",
        "Informes de costos por ejercicio fiscal",
    ),
    "url": (
        "https://www.cms.gov/data-research/statistics-trends-and-reports/"
        "cost-reports/cost-reports-fiscal-year"
    ),
    "description": (
        "Página da CMS que publica um extrato por exercício fiscal federal, "
        "para os formulários CMS-2552-96 (1996 a 2011) e CMS-2552-10 (2010 em "
        "diante). Cada extrato é um arquivo ZIP com os arquivos RPT (índice de "
        "relatórios), NMRC (valores numéricos) e ALPHA (valores de texto); os "
        "extratos do 2552-96 trazem também um arquivo ROLLUP, que não é "
        "ingerido. Os extratos são reeditados trimestralmente à medida que "
        "relatórios são recebidos, homologados, reabertos ou retificados.",
        "The CMS page that publishes one extract per US federal fiscal year, "
        "for CMS Form 2552-96 (1996 to 2011) and CMS Form 2552-10 (2010 "
        "onwards). Each extract is a ZIP holding the RPT (report index), NMRC "
        "(numeric values) and ALPHA (text values) files; the 2552-96 extracts "
        "also carry a ROLLUP file, which is not ingested. The extracts are "
        "reissued quarterly as reports are received, settled, reopened or "
        "amended.",
        "La página de CMS que publica un extracto por año fiscal federal "
        "estadounidense, para los formularios CMS-2552-96 (1996 a 2011) y "
        "CMS-2552-10 (2010 en adelante). Cada extracto es un ZIP con los "
        "archivos RPT (índice de informes), NMRC (valores numéricos) y ALPHA "
        "(valores de texto); los extractos del 2552-96 traen además un archivo "
        "ROLLUP, que no se ingiere. Los extractos se reeditan trimestralmente a "
        "medida que se reciben, homologan, reabren o rectifican informes.",
    ),
}

# Coverage of every table, read from the cleaned parquet: the earliest and
# latest calendar year a reporting period ends in.
COVERAGE = {"start_year": 1996, "end_year": 2026}

INCOMPLETE_TAIL = (
    " Os dois exercícios fiscais mais recentes estão substancialmente "
    "incompletos: a CMS continua recebendo, homologando e retificando "
    "relatórios por anos após o fim do exercício, de modo que um ano recente "
    "ganha relatórios a cada extrato trimestral.",
    " The two most recent fiscal years are substantially incomplete: CMS keeps "
    "receiving, settling and amending reports for years after a fiscal year "
    "ends, so a recent year gains reports with every quarterly extract.",
    " Los dos ejercicios fiscales más recientes están sustancialmente "
    "incompletos: CMS sigue recibiendo, homologando y rectificando informes "
    "durante años después del cierre del ejercicio, por lo que un año reciente "
    "gana informes con cada extracto trimestral.",
)

TABLE_NAMES = {
    "report": ("Relatórios", "Reports", "Informes"),
    "report_value": (
        "Valores declarados",
        "Reported values",
        "Valores declarados",
    ),
    "hospital_financial": (
        "Finanças hospitalares",
        "Hospital financials",
        "Finanzas hospitalarias",
    ),
    "dicionario": ("Dicionário", "Dictionary", "Diccionario"),
}

TABLE_DESCRIPTIONS = {
    "report": (
        "Índice dos relatórios de custos, uma linha por relatório: prestador, "
        "período do exercício fiscal, situação, versão do formulário e datas de "
        "recebimento e processamento. Corresponde ao arquivo RPT dos extratos "
        "da CMS. report_id é único em todos os extratos e em ambas as versões "
        "do formulário, e une esta tabela às demais.",
        "Index of the cost reports, one row per report: provider, fiscal year "
        "period, status, form version and the receipt and processing dates. "
        "Corresponds to the RPT file of the CMS extracts. report_id is unique "
        "across every extract and both form versions, and joins this table to "
        "the others.",
        "Índice de los informes de costos, una fila por informe: proveedor, "
        "período del ejercicio fiscal, situación, versión del formulario y las "
        "fechas de recepción y procesamiento. Corresponde al archivo RPT de los "
        "extractos de CMS. report_id es único en todos los extractos y en ambas "
        "versiones del formulario, y une esta tabla con las demás.",
    ),
    "report_value": (
        "Todos os valores declarados nos relatórios, em formato longo: uma "
        "linha por célula do formulário, identificada por relatório, planilha, "
        "linha e coluna, com o valor numérico e o valor de texto em colunas "
        "separadas. Corresponde aos arquivos NMRC e ALPHA da CMS, unidos pela "
        "chave da célula. É a forma completa do formulário — 538 milhões de "
        "células, 884 planilhas — e a fonte de onde a tabela "
        "hospital_financial é derivada.",
        "Every value reported on the cost reports, in long form: one row per "
        "form cell, identified by report, worksheet, line and column, with the "
        "numeric value and the text value in separate columns. Corresponds to "
        "the CMS NMRC and ALPHA files, joined on the cell key. This is the "
        "complete form — 538 million cells across 884 worksheets — and the "
        "source the hospital_financial table is derived from.",
        "Todos los valores declarados en los informes, en formato largo: una "
        "fila por celda del formulario, identificada por informe, hoja, línea y "
        "columna, con el valor numérico y el valor de texto en columnas "
        "separadas. Corresponde a los archivos NMRC y ALPHA de CMS, unidos por "
        "la clave de la celda. Es la forma completa del formulario — 538 "
        "millones de celdas en 884 hojas — y la fuente de la que se deriva la "
        "tabla hospital_financial.",
    ),
    "hospital_financial": (
        "Medidas financeiras e operacionais nomeadas, uma linha por relatório "
        "de custos: receitas, despesas, cobranças, leitos, altas, atendimento "
        "não remunerado, pagamentos do Medicare e balanço patrimonial. Cada "
        "coluna é lida de um endereço de planilha, linha e coluna registrado no "
        "campo observations da própria coluna, a partir de mapeamentos "
        "publicados e mantidos por terceiros, não construídos aqui. O grão é o "
        "relatório e não o hospital-ano: a CMS documenta que um hospital pode "
        "apresentar dois ou mais relatórios para o mesmo ano, por mudança de "
        "exercício fiscal ou de controle, e as regras de consolidação usadas "
        "pelos mapeamentos publicados divergem entre si.",
        "Named financial and operating measures, one row per cost report: "
        "revenues, expenses, charges, beds, discharges, uncompensated care, "
        "Medicare payments and the balance sheet. Each column is read from a "
        "worksheet, line and column address recorded in that column's own "
        "observations field, using mappings published and maintained by others "
        "rather than constructed here. The grain is the report, not the "
        "hospital-year: CMS documents that one hospital can file two or more "
        "reports for the same year, on a fiscal year change or a change of "
        "ownership, and the collapse rules the published mappings use for that "
        "disagree with each other.",
        "Medidas financieras y operativas nombradas, una fila por informe de "
        "costos: ingresos, gastos, cargos, camas, altas, atención no "
        "remunerada, pagos de Medicare y el balance. Cada columna se lee de una "
        "dirección de hoja, línea y columna registrada en el campo observations "
        "de la propia columna, a partir de mapeos publicados y mantenidos por "
        "terceros, no construidos aquí. El grano es el informe y no el "
        "hospital-año: CMS documenta que un hospital puede presentar dos o más "
        "informes para el mismo año, por cambio de ejercicio fiscal o de "
        "control, y las reglas de consolidación de los mapeos publicados "
        "difieren entre sí.",
    ),
    "dicionario": (
        "Dicionário dos códigos usados nas colunas codificadas deste conjunto, "
        "transcrito da documentação da CMS: situação do relatório, tipo de "
        "controle do prestador, nível de utilização do Medicare, versão do "
        "formulário e os indicadores do registro.",
        "Dictionary of the codes used in this dataset's coded columns, "
        "transcribed from CMS's own documentation: report status, provider "
        "control type, Medicare utilization level, form version and the record "
        "indicators.",
        "Diccionario de los códigos usados en las columnas codificadas de este "
        "conjunto, transcrito de la documentación de CMS: situación del "
        "informe, tipo de control del proveedor, nivel de utilización de "
        "Medicare, versión del formulario y los indicadores del registro.",
    ),
}

# Observation levels per table, as (entity slug, identifying column). The
# dicionario has none.
OBSERVATION_LEVELS = {
    "report": [
        ("year", "year"),
        ("hospital", "provider_ccn"),
        ("state", "state_id"),
    ],
    "report_value": [("year", "year"), ("hospital", "provider_ccn")],
    "hospital_financial": [
        ("year", "year"),
        ("hospital", "provider_ccn"),
        ("state", "state_id"),
    ],
}

AUXILIARY_FILES = {
    table: (
        "https://storage.googleapis.com/basedosdados/auxiliary_files/"
        f"{GCP_DATASET_ID}/{table}/auxiliary_files.zip"
    )
    for table in ("report", "report_value", "hospital_financial")
}
