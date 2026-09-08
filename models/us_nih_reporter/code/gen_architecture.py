"""Write the architecture CSVs for us_nih_reporter.

The CSVs under ``architecture/`` are the single source of truth for column
names, order, BigQuery types, the raw -> clean name mapping and the trilingual
descriptions. The cleaning transform, the dbt models and the backend column
registration all read them, so a column can only be added or renamed here.

Run: uv run python models/us_nih_reporter/code/gen_architecture.py
"""

import csv
from pathlib import Path

ARCH = Path(__file__).resolve().parent / "architecture"

FIELDS = [
    "name",
    "bigquery_type",
    "description_pt",
    "description_en",
    "description_es",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations_pt",
    "observations_en",
    "observations_es",
    "original_name",
]

TIME_DIR = "br_bd_diretorios_data_tempo.ano:ano"


def col(
    name,
    bq,
    pt,
    en,
    es,
    *,
    dictionary=False,
    directory="",
    unit="",
    coverage="",
    sensitive=False,
    obs=("", "", ""),
    original="",
):
    return {
        "name": name,
        "bigquery_type": bq,
        "description_pt": pt,
        "description_en": en,
        "description_es": es,
        "temporal_coverage": coverage,
        "covered_by_dictionary": "yes" if dictionary else "no",
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": "yes" if sensitive else "no",
        "observations_pt": obs[0],
        "observations_en": obs[1],
        "observations_es": obs[2],
        "original_name": original,
    }


# --------------------------------------------------------------------------
# project
# --------------------------------------------------------------------------

OBS_FISCAL_YEAR = (
    "Ano FISCAL federal dos Estados Unidos, que vai de 1 de outubro a 30 de "
    "setembro e é nomeado pelo ano em que termina. Não é o ano-calendário. "
    "Coluna de particionamento.",
    "United States federal FISCAL year, running from 1 October to 30 September "
    "and named for the year in which it ends. It is not the calendar year. "
    "Partition column.",
    "Año FISCAL federal de los Estados Unidos, que va del 1 de octubre al 30 de "
    "septiembre y se nombra por el año en que termina. No es el año calendario. "
    "Columna de particionamiento.",
)

OBS_CORE_VS_COMPONENTS = (
    "Sempre é uma subsequência de full_project_num, nas 2.951.523 linhas do "
    "período. Difere da concatenação de activity, administering_ic e "
    "serial_number em 91.147 linhas (3,09%), porque o instituto administrador "
    "muda quando a concessão é transferida enquanto o número mantém as letras "
    "originais. É esta coluna, e não as componentes, que liga a project as "
    "tabelas publication_link, patent_link e clinical_study_link.",
    "Always a substring of full_project_num, on all 2,951,523 rows of the "
    "period. It differs from the concatenation of activity, administering_ic "
    "and serial_number on 91,147 rows (3.09%), because the administering "
    "institute moves when an award is transferred while the number keeps the "
    "original letters. This column, not the components, is what joins project "
    "to publication_link, patent_link and clinical_study_link.",
    "Siempre es una subcadena de full_project_num, en las 2.951.523 filas del "
    "período. Difiere de la concatenación de activity, administering_ic y "
    "serial_number en 91.147 filas (3,09%), porque el instituto administrador "
    "cambia cuando la subvención se transfiere mientras el número conserva las "
    "letras originales. Es esta columna, y no las componentes, la que une "
    "project con publication_link, patent_link y clinical_study_link.",
)

OBS_ORG_STATE = (
    "Não é um campo restrito a unidades federativas dos Estados Unidos: dos 70 "
    "valores observados, onze são províncias canadenses (ON, PQ, QC, BC, AB, "
    "MB, NS, SK, NL, NB, PE) e oito são territórios ou estados livremente "
    "associados (PR, GU, VI, AS, MP, PW, MH, FM). Por isso não recebe ligação a "
    "um diretório de estados norte-americanos. Vazio em 5,71% das linhas.",
    "Not restricted to US states: of the 70 observed values, eleven are "
    "Canadian provinces (ON, PQ, QC, BC, AB, MB, NS, SK, NL, NB, PE) and eight "
    "are territories or freely associated states (PR, GU, VI, AS, MP, PW, MH, "
    "FM). It therefore carries no link to a US state directory. Blank on 5.71% "
    "of rows.",
    "No es un campo restringido a estados de los Estados Unidos: de los 70 "
    "valores observados, once son provincias canadienses (ON, PQ, QC, BC, AB, "
    "MB, NS, SK, NL, NB, PE) y ocho son territorios o estados libremente "
    "asociados (PR, GU, VI, AS, MP, PW, MH, FM). Por eso no recibe enlace a un "
    "directorio de estados estadounidenses. Vacío en el 5,71% de las filas.",
)

OBS_COST = (
    "Valores só estão disponíveis para concessões do NIH, CDC, FDA e ACF; "
    "outras agências que publicam no RePORTER não reportam financiamento. Para "
    "os anos fiscais de 1985 a 1999 o custo não consta do arquivo principal e "
    "foi incorporado do arquivo acessório de custos e DUNS que o NIH publica "
    "separadamente, ligado por application_id.",
    "Values are available only for NIH, CDC, FDA and ACF awards; the other "
    "agencies that publish in RePORTER do not report funding. For fiscal years "
    "1985 to 1999 the cost is absent from the main file and was merged in from "
    "the separate cost and DUNS accessory file NIH publishes, joined on "
    "application_id.",
    "Los valores solo están disponibles para subvenciones del NIH, CDC, FDA y "
    "ACF; las demás agencias que publican en RePORTER no reportan "
    "financiamiento. Para los años fiscales de 1985 a 1999 el costo no consta "
    "en el archivo principal y fue incorporado del archivo accesorio de costos "
    "y DUNS que el NIH publica por separado, unido por application_id.",
)

OBS_DATE_FORMATS = (
    "A fonte publica esta data em dois formatos ao longo do período, "
    "AAAA-MM-DD e M/D/AAAA, este último predominante entre 1985 e 2005; ambos "
    "foram normalizados para AAAA-MM-DD na limpeza.",
    "The source publishes this date in two formats across the period, "
    "YYYY-MM-DD and M/D/YYYY, the latter predominant between 1985 and 2005; "
    "both were normalised to YYYY-MM-DD in cleaning.",
    "La fuente publica esta fecha en dos formatos a lo largo del período, "
    "AAAA-MM-DD y M/D/AAAA, este último predominante entre 1985 y 2005; ambos "
    "fueron normalizados a AAAA-MM-DD en la limpieza.",
)

PROJECT = [
    col(
        "year",
        "INT64",
        "Ano fiscal da apropriação de que os recursos do projeto foram obrigados",
        "Fiscal year of the appropriation from which the project's funds were obligated",
        "Año fiscal de la apropiación de la que se obligaron los recursos del proyecto",
        directory=TIME_DIR,
        unit="year",
        obs=OBS_FISCAL_YEAR,
        original="FY",
    ),
    col(
        "application_id",
        "STRING",
        "Identificador único do registro do projeto na base RePORTER",
        "Unique identifier of the project record in the RePORTER database",
        "Identificador único del registro del proyecto en la base RePORTER",
        obs=(
            "Chave lógica da tabela junto com year. Único dentro de cada ano "
            "fiscal, sem exceção; em todo o período repete-se uma única vez, "
            "no identificador 11169374.",
            "Logical key of the table together with year. Unique within each "
            "fiscal year without exception; across the whole period it repeats "
            "exactly once, on identifier 11169374.",
            "Clave lógica de la tabla junto con year. Único dentro de cada año "
            "fiscal, sin excepción; en todo el período se repite una sola vez, "
            "en el identificador 11169374.",
        ),
        original="APPLICATION_ID",
    ),
    col(
        "full_project_num",
        "STRING",
        "Número completo do projeto, formado pelo tipo de solicitação, código de atividade, código do instituto, número de série, ano de apoio e sufixo",
        "Full project number, made up of the application type, activity code, institute code, serial number, support year and suffix",
        "Número completo del proyecto, formado por el tipo de solicitud, código de actividad, código del instituto, número de serie, año de apoyo y sufijo",
        obs=(
            "Também chamado de número da concessão, do projeto intramuros ou do "
            "contrato. As colunas application_type, activity, "
            "administering_ic, serial_number, support_year e suffix repetem "
            "essas componentes tal como a fonte as publica, mas não são uma "
            "decomposição literal desta string: ver core_project_num.",
            "Also called the grant number, intramural project number or "
            "contract number. The columns application_type, activity, "
            "administering_ic, serial_number, support_year and suffix repeat "
            "those components as the source publishes them, but are not a "
            "literal decomposition of this string: see core_project_num.",
            "También llamado número de la subvención, del proyecto intramuros o "
            "del contrato. Las columnas application_type, activity, "
            "administering_ic, serial_number, support_year y suffix repiten "
            "esas componentes tal como la fuente las publica, pero no son una "
            "descomposición literal de esta cadena: ver core_project_num.",
        ),
        original="FULL_PROJECT_NUM",
    ),
    col(
        "core_project_num",
        "STRING",
        "Identificador do projeto de pesquisa que não varia entre anos de apoio, formado pelo código de atividade, código do instituto e número de série",
        "Identifier of the research project that does not vary across support years, made up of the activity code, institute code and serial number",
        "Identificador del proyecto de investigación que no varía entre años de apoyo, formado por el código de actividad, código del instituto y número de serie",
        obs=OBS_CORE_VS_COMPONENTS,
        original="CORE_PROJECT_NUM",
    ),
    col(
        "subproject_id",
        "STRING",
        "Identificador do subprojeto dentro de uma concessão multiprojeto",
        "Identifier of the subproject within a multi-project award",
        "Identificador del subproyecto dentro de una subvención multiproyecto",
        obs=(
            "Vazio em 79,53% das linhas, que são registros de projeto único ou "
            "o registro-pai de uma concessão multiprojeto.",
            "Blank on 79.53% of rows, which are single-project records or the "
            "parent record of a multi-project award.",
            "Vacío en el 79,53% de las filas, que son registros de proyecto "
            "único o el registro padre de una subvención multiproyecto.",
        ),
        original="SUBPROJECT_ID",
    ),
    col(
        "application_type",
        "STRING",
        "Código de um dígito que identifica o tipo de solicitação financiada",
        "One-digit code identifying the type of application funded",
        "Código de un dígito que identifica el tipo de solicitud financiada",
        dictionary=True,
        obs=(
            "O dicionário de dados da fonte define os valores 1, 2, 3, 4, 5, 7 "
            "e 9. Os valores 6 e 8 ocorrem nos dados, em 4.885 e 2.213 linhas, "
            "e não são definidos em documentação alguma.",
            "The source's data dictionary defines values 1, 2, 3, 4, 5, 7 and "
            "9. Values 6 and 8 occur in the data, on 4,885 and 2,213 rows, and "
            "are defined in no documentation.",
            "El diccionario de datos de la fuente define los valores 1, 2, 3, "
            "4, 5, 7 y 9. Los valores 6 y 8 ocurren en los datos, en 4.885 y "
            "2.213 filas, y no están definidos en ninguna documentación.",
        ),
        original="APPLICATION_TYPE",
    ),
    col(
        "activity",
        "STRING",
        "Código de três caracteres do programa de concessão, contrato ou atividade intramuros que apoia o projeto",
        "Three-character code of the grant, contract or intramural activity programme supporting the project",
        "Código de tres caracteres del programa de subvención, contrato o actividad intramuros que apoya el proyecto",
        dictionary=True,
        obs=(
            "372 valores distintos no período, dos quais 233 têm rótulo no "
            "dicionário. O registro oficial do NIH cobre 258 códigos de "
            "concessões e acordos de cooperação; contratos de pesquisa e "
            "desenvolvimento (iniciados por N), projetos intramuros (iniciados "
            "por Z) e códigos de outras agências ficam sem rótulo.",
            "372 distinct values over the period, of which 233 carry a label in "
            "the dictionary. The official NIH register covers 258 grant and "
            "cooperative agreement codes; research and development contracts "
            "(beginning with N), intramural projects (beginning with Z) and "
            "other agencies' codes have no label.",
            "372 valores distintos en el período, de los cuales 233 tienen "
            "etiqueta en el diccionario. El registro oficial del NIH cubre 258 "
            "códigos de subvenciones y acuerdos de cooperación; los contratos "
            "de investigación y desarrollo (iniciados por N), los proyectos "
            "intramuros (iniciados por Z) y los códigos de otras agencias "
            "quedan sin etiqueta.",
        ),
        original="ACTIVITY",
    ),
    col(
        "administering_ic",
        "STRING",
        "Código de dois caracteres da agência, instituto ou centro que administra a concessão",
        "Two-character code of the agency, institute or centre administering the award",
        "Código de dos caracteres de la agencia, instituto o centro que administra la subvención",
        dictionary=True,
        obs=(
            "114 códigos no período. O nome por extenso está em ic_name; 13 dos "
            "114 códigos aparecem com mais de uma grafia do nome ao longo do "
            "tempo, e o dicionário registra a grafia mais frequente.",
            "114 codes over the period. The full name is in ic_name; 13 of the "
            "114 codes appear under more than one spelling of the name over "
            "time, and the dictionary records the most frequent spelling.",
            "114 códigos en el período. El nombre completo está en ic_name; 13 "
            "de los 114 códigos aparecen con más de una grafía del nombre a lo "
            "largo del tiempo, y el diccionario registra la grafía más "
            "frecuente.",
        ),
        original="ADMINISTERING_IC",
    ),
    col(
        "serial_number",
        "STRING",
        "Número de série atribuído em ordem sequencial dentro de cada organização administradora",
        "Serial number assigned in sequence within each administering organisation",
        "Número de serie asignado en orden secuencial dentro de cada organización administradora",
        obs=(
            "Publicado sem zeros à esquerda, com um a seis dígitos, enquanto "
            "aparece com seis posições dentro de core_project_num e "
            "full_project_num.",
            "Published without leading zeros, with one to six digits, while it "
            "appears in six positions inside core_project_num and "
            "full_project_num.",
            "Publicado sin ceros a la izquierda, con uno a seis dígitos, "
            "mientras que aparece con seis posiciones dentro de "
            "core_project_num y full_project_num.",
        ),
        original="SERIAL_NUMBER",
    ),
    col(
        "suffix",
        "STRING",
        "Sufixo do número da solicitação que identifica versões emendadas e suplementos",
        "Suffix to the application number identifying amended versions and supplements",
        "Sufijo del número de la solicitud que identifica versiones enmendadas y suplementos",
        obs=(
            "A letra A seguida de número indica solicitação emendada e a letra "
            "S seguida de número indica suplemento; ocorrem também as formas "
            "A1S1, X1, M001 e sequências puramente numéricas. Vazio em 85,82% "
            "das linhas.",
            "The letter A followed by a number marks an amended application and "
            "the letter S followed by a number a supplement; the forms A1S1, "
            "X1, M001 and purely numeric sequences also occur. Blank on 85.82% "
            "of rows.",
            "La letra A seguida de número indica solicitud enmendada y la letra "
            "S seguida de número indica suplemento; también ocurren las formas "
            "A1S1, X1, M001 y secuencias puramente numéricas. Vacío en el "
            "85,82% de las filas.",
        ),
        original="SUFFIX",
    ),
    col(
        "support_year",
        "INT64",
        "Ano de apoio do projeto, conforme consta do número completo do projeto",
        "Year of support of the project, as shown in the full project number",
        "Año de apoyo del proyecto, según consta en el número completo del proyecto",
        unit="year",
        obs=(
            "Assume valores de 0 a 95. Valores acima de 50 são raros e "
            "correspondem sobretudo a centros e contratos de longa duração.",
            "Takes values from 0 to 95. Values above 50 are rare and correspond "
            "mostly to long-running centres and contracts.",
            "Asume valores de 0 a 95. Los valores por encima de 50 son raros y "
            "corresponden sobre todo a centros y contratos de larga duración.",
        ),
        original="SUPPORT_YEAR",
    ),
    col(
        "assistance_listing_number",
        "STRING",
        "Número do programa federal no catálogo de auxílios federais, antes chamado código CFDA",
        "Number of the federal programme in the assistance listings catalogue, formerly called the CFDA code",
        "Número del programa federal en el catálogo de asistencias federales, antes llamado código CFDA",
        obs=(
            "A fonte chamou esta coluna de CFDA_CODE até o ano fiscal de 2024 e "
            "ASSISTANCE_LISTING_NUMBER a partir de 2025, sem mudança de "
            "conteúdo.",
            "The source called this column CFDA_CODE through fiscal year 2024 "
            "and ASSISTANCE_LISTING_NUMBER from 2025 on, with no change of "
            "content.",
            "La fuente llamó a esta columna CFDA_CODE hasta el año fiscal de "
            "2024 y ASSISTANCE_LISTING_NUMBER a partir de 2025, sin cambio de "
            "contenido.",
        ),
        original="CFDA_CODE / ASSISTANCE_LISTING_NUMBER",
    ),
    col(
        "opportunity_number",
        "STRING",
        "Número do anúncio de oportunidade de financiamento sob o qual a solicitação foi apresentada",
        "Number of the funding opportunity announcement under which the application was solicited",
        "Número del anuncio de oportunidad de financiamiento bajo el cual se presentó la solicitud",
        obs=(
            "A fonte chamou esta coluna de FOA_NUMBER até o ano fiscal de 2005 "
            'e "OPPORTUNITY NUMBER", com espaço, a partir de 2006. Vazio em '
            "48,96% das linhas.",
            "The source called this column FOA_NUMBER through fiscal year 2005 "
            'and "OPPORTUNITY NUMBER", with a space, from 2006 on. Blank on '
            "48.96% of rows.",
            "La fuente llamó a esta columna FOA_NUMBER hasta el año fiscal de "
            '2005 y "OPPORTUNITY NUMBER", con espacio, a partir de 2006. Vacío '
            "en el 48,96% de las filas.",
        ),
        original="FOA_NUMBER / OPPORTUNITY NUMBER",
    ),
    col(
        "study_section",
        "STRING",
        "Código do painel de revisão por pares que avaliou o mérito científico e técnico da solicitação",
        "Code of the peer review panel that assessed the scientific and technical merit of the application",
        "Código del panel de revisión por pares que evaluó el mérito científico y técnico de la solicitud",
        obs=(
            "1.578 códigos distintos. O nome do painel está em "
            "study_section_name, que fica vazio quando a revisão coube a um "
            "painel de ênfase especial e não a um comitê permanente. Vazio em "
            "14,48% das linhas.",
            "1,578 distinct codes. The panel's name is in study_section_name, "
            "which is blank when the review fell to a special emphasis panel "
            "rather than a standing committee. Blank on 14.48% of rows.",
            "1.578 códigos distintos. El nombre del panel está en "
            "study_section_name, que queda vacío cuando la revisión "
            "correspondió a un panel de énfasis especial y no a un comité "
            "permanente. Vacío en el 14,48% de las filas.",
        ),
        original="STUDY_SECTION",
    ),
    col(
        "org_ipf_id",
        "STRING",
        "Identificador interno do NIH que associa a instituição a seus registros nos sistemas eletrônicos da agência",
        "Internal NIH identifier associating the institution with its records in the agency's electronic systems",
        "Identificador interno del NIH que asocia la institución con sus registros en los sistemas electrónicos de la agencia",
        obs=(
            "Publicado apenas a partir do ano fiscal de 2006.",
            "Published only from fiscal year 2006 on.",
            "Publicado solo a partir del año fiscal de 2006.",
        ),
        original="ORG_IPF_CODE",
    ),
    col(
        "org_duns",
        "STRING",
        "Número DUNS da organização beneficiária",
        "DUNS number of the recipient organisation",
        "Número DUNS de la organización beneficiaria",
        obs=(
            "Pode conter mais de um número separado por ponto e vírgula, o que "
            "ocorre em 18.716 linhas. Para os anos fiscais de 1985 a 1999 o "
            "valor vem do arquivo acessório de custos e DUNS.",
            "May contain more than one number separated by a semicolon, which "
            "happens on 18,716 rows. For fiscal years 1985 to 1999 the value "
            "comes from the cost and DUNS accessory file.",
            "Puede contener más de un número separado por punto y coma, lo que "
            "ocurre en 18.716 filas. Para los años fiscales de 1985 a 1999 el "
            "valor proviene del archivo accesorio de costos y DUNS.",
        ),
        original="ORG_DUNS",
    ),
    col(
        "pi_ids",
        "STRING",
        "Identificadores dos pesquisadores principais do projeto, separados por ponto e vírgula",
        "Identifiers of the project's principal investigators, separated by semicolons",
        "Identificadores de los investigadores principales del proyecto, separados por punto y coma",
        obs=(
            "O identificador de contato é marcado com o sufixo (contact). Cada "
            "pesquisador tem um identificador estável entre projetos e anos, "
            "com exceção de investigadores que tiveram mais de uma conta no "
            "sistema. Metade das linhas tem um pesquisador e quase metade tem "
            "dois.",
            "The contact identifier is marked with the suffix (contact). Each "
            "investigator has an identifier that is stable across projects and "
            "years, except for investigators who have had more than one account "
            "in the system. Half the rows carry one investigator and almost "
            "half carry two.",
            "El identificador de contacto está marcado con el sufijo (contact). "
            "Cada investigador tiene un identificador estable entre proyectos y "
            "años, salvo los investigadores que tuvieron más de una cuenta en el "
            "sistema. La mitad de las filas tiene un investigador y casi la "
            "mitad tiene dos.",
        ),
        original="PI_IDS",
    ),
    col(
        "project_title",
        "STRING",
        "Título da concessão, contrato ou projeto intramuros financiado",
        "Title of the funded grant, contract or intramural project",
        "Título de la subvención, contrato o proyecto intramuros financiado",
        original="PROJECT_TITLE",
    ),
    col(
        "ic_name",
        "STRING",
        "Nome por extenso da agência, instituto ou centro administrador",
        "Full name of the administering agency, institute or centre",
        "Nombre completo de la agencia, instituto o centro administrador",
        original="IC_NAME",
    ),
    col(
        "funding_mechanism",
        "STRING",
        "Categoria de mecanismo de financiamento usada nas tabelas orçamentárias do NIH",
        "Funding mechanism category used in the NIH budget mechanism tables",
        "Categoría de mecanismo de financiamiento usada en las tablas presupuestarias del NIH",
        obs=(
            "Doze categorias. Publicado apenas a partir do ano fiscal de 2006; "
            "nos arquivos anteriores a coluna não existe.",
            "Twelve categories. Published only from fiscal year 2006 on; the "
            "column does not exist in the earlier files.",
            "Doce categorías. Publicado solo a partir del año fiscal de 2006; "
            "en los archivos anteriores la columna no existe.",
        ),
        original="FUNDING_MECHANISM",
    ),
    col(
        "funding_ics",
        "STRING",
        "Institutos e centros que financiaram o projeto no ano fiscal, cada um seguido do valor aportado",
        "Institutes and centres that funded the project in the fiscal year, each followed by the amount it provided",
        "Institutos y centros que financiaron el proyecto en el año fiscal, cada uno seguido del monto aportado",
        obs=(
            "Cada instituto é seguido de dois pontos e do valor, e institutos "
            "sucessivos são separados por ponto e vírgula. Disponível apenas "
            "para projetos do NIH, CDC, FDA e ACF. Vazio em 32,63% das linhas.",
            "Each institute is followed by a colon and the amount, and "
            "successive institutes are separated by semicolons. Available only "
            "for NIH, CDC, FDA and ACF projects. Blank on 32.63% of rows.",
            "Cada instituto va seguido de dos puntos y el monto, y los "
            "institutos sucesivos se separan con punto y coma. Disponible solo "
            "para proyectos del NIH, CDC, FDA y ACF. Vacío en el 32,63% de las "
            "filas.",
        ),
        original="FUNDING_ICs",
    ),
    col(
        "arra_funded",
        "STRING",
        "Indica se o projeto foi apoiado por recursos apropriados pela Lei de Recuperação e Reinvestimento Americana de 2009",
        "Indicates whether the project was supported by funds appropriated through the American Recovery and Reinvestment Act of 2009",
        "Indica si el proyecto fue apoyado por recursos apropiados por la Ley de Recuperación y Reinversión Americana de 2009",
        dictionary=True,
        obs=(
            "Vazio em 44,43% das linhas, concentradas nos anos fiscais "
            "anteriores a 2009.",
            "Blank on 44.43% of rows, concentrated in fiscal years before 2009.",
            "Vacío en el 44,43% de las filas, concentradas en los años fiscales "
            "anteriores a 2009.",
        ),
        original="ARRA_FUNDED",
    ),
    col(
        "study_section_name",
        "STRING",
        "Nome do comitê permanente de revisão que avaliou a solicitação",
        "Name of the standing review committee that assessed the application",
        "Nombre del comité permanente de revisión que evaluó la solicitud",
        obs=(
            "Solicitações avaliadas por painéis que não são comitês permanentes "
            "aparecem como painel de ênfase especial. Vazio em 21,43% das "
            "linhas.",
            "Applications reviewed by panels other than standing committees "
            "appear as special emphasis panel. Blank on 21.43% of rows.",
            "Las solicitudes evaluadas por paneles que no son comités "
            "permanentes aparecen como panel de énfasis especial. Vacío en el "
            "21,43% de las filas.",
        ),
        original="STUDY_SECTION_NAME",
    ),
    col(
        "program_officer_name",
        "STRING",
        "Nome do servidor do instituto que coordena os aspectos substantivos do projeto",
        "Name of the institute staff member who coordinates the substantive aspects of the project",
        "Nombre del funcionario del instituto que coordina los aspectos sustantivos del proyecto",
        obs=(
            "Vazio em 46,70% das linhas.",
            "Blank on 46.70% of rows.",
            "Vacío en el 46,70% de las filas.",
        ),
        original="PROGRAM_OFFICER_NAME",
    ),
    col(
        "pi_names",
        "STRING",
        "Nomes dos pesquisadores principais designados para dirigir o projeto, separados por ponto e vírgula",
        "Names of the principal investigators designated to direct the project, separated by semicolons",
        "Nombres de los investigadores principales designados para dirigir el proyecto, separados por punto y coma",
        obs=(
            "O pesquisador de contato é marcado com o sufixo (contact), na "
            "mesma ordem de pi_ids.",
            "The contact investigator is marked with the suffix (contact), in "
            "the same order as pi_ids.",
            "El investigador de contacto está marcado con el sufijo (contact), "
            "en el mismo orden que pi_ids.",
        ),
        original="PI_NAMEs",
    ),
    col(
        "org_name",
        "STRING",
        "Nome da instituição de ensino, organização de pesquisa, empresa ou órgão público que recebeu o financiamento",
        "Name of the educational institution, research organisation, business or government agency that received the funding",
        "Nombre de la institución educativa, organización de investigación, empresa u órgano público que recibió el financiamiento",
        original="ORG_NAME",
    ),
    col(
        "org_dept",
        "STRING",
        "Departamento de vínculo do pesquisador principal de contato, em categorização padronizada",
        "Departmental affiliation of the contact principal investigator, in a standardised categorisation",
        "Departamento de vinculación del investigador principal de contacto, en categorización estandarizada",
        obs=(
            "A fonte só nomeia departamentos de escolas de medicina. Vazio em "
            "50,64% das linhas.",
            "The source names departments only for medical schools. Blank on "
            "50.64% of rows.",
            "La fuente solo nombra departamentos de escuelas de medicina. Vacío "
            "en el 50,64% de las filas.",
        ),
        original="ORG_DEPT",
    ),
    col(
        "ed_inst_type",
        "STRING",
        "Nome do agrupamento de componentes da instituição usado pelo NIH",
        "Name of the grouping of institutional components used by NIH",
        "Nombre de la agrupación de componentes de la institución usada por el NIH",
        obs=(
            "109 valores distintos, publicados como rótulos legíveis e não como "
            "códigos. Vazio em 42,29% das linhas.",
            "109 distinct values, published as readable labels rather than "
            "codes. Blank on 42.29% of rows.",
            "109 valores distintos, publicados como etiquetas legibles y no "
            "como códigos. Vacío en el 42,29% de las filas.",
        ),
        original="ED_INST_TYPE",
    ),
    col(
        "org_city",
        "STRING",
        "Cidade do escritório administrativo da organização beneficiária",
        "City of the business office of the recipient organisation",
        "Ciudad de la oficina administrativa de la organización beneficiaria",
        obs=(
            "Pode diferir do local de execução da pesquisa. Projetos "
            "intramuros do NIH usam Bethesda, Maryland.",
            "May differ from the research performance site. NIH intramural "
            "projects use Bethesda, Maryland.",
            "Puede diferir del lugar de ejecución de la investigación. Los "
            "proyectos intramuros del NIH usan Bethesda, Maryland.",
        ),
        original="ORG_CITY",
    ),
    col(
        "org_state",
        "STRING",
        "Sigla da unidade subnacional do escritório administrativo da organização beneficiária",
        "Abbreviation of the subnational unit of the recipient organisation's business office",
        "Sigla de la unidad subnacional de la oficina administrativa de la organización beneficiaria",
        obs=OBS_ORG_STATE,
        original="ORG_STATE",
    ),
    col(
        "org_zipcode",
        "STRING",
        "Código postal do escritório administrativo da organização beneficiária",
        "Postal code of the business office of the recipient organisation",
        "Código postal de la oficina administrativa de la organización beneficiaria",
        original="ORG_ZIPCODE",
    ),
    col(
        "org_district",
        "STRING",
        "Distrito congressual do escritório administrativo da organização beneficiária",
        "Congressional district of the business office of the recipient organisation",
        "Distrito congresional de la oficina administrativa de la organización beneficiaria",
        obs=(
            "Publicado sem o código do estado, e por isso não identifica um "
            "distrito sozinho. Vazio em 7,99% das linhas.",
            "Published without the state code, and therefore does not identify "
            "a district on its own. Blank on 7.99% of rows.",
            "Publicado sin el código del estado, y por eso no identifica un "
            "distrito por sí solo. Vacío en el 7,99% de las filas.",
        ),
        original="ORG_DISTRICT",
    ),
    col(
        "org_country",
        "STRING",
        "País do escritório administrativo da organização beneficiária",
        "Country of the business office of the recipient organisation",
        "País de la oficina administrativa de la organización beneficiaria",
        obs=(
            "131 países no período; 96,7% das linhas são dos Estados Unidos. "
            "Publicado como nome e não como código; o código FIPS "
            "correspondente está em org_fips.",
            "131 countries over the period; 96.7% of rows are United States. "
            "Published as a name rather than a code; the matching FIPS code is "
            "in org_fips.",
            "131 países en el período; el 96,7% de las filas son de los Estados "
            "Unidos. Publicado como nombre y no como código; el código FIPS "
            "correspondiente está en org_fips.",
        ),
        original="ORG_COUNTRY",
    ),
    col(
        "org_fips",
        "STRING",
        "Código FIPS do país da organização beneficiária",
        "FIPS country code of the recipient organisation",
        "Código FIPS del país de la organización beneficiaria",
        obs=(
            "É o código FIPS 10-4 de país, não o código ISO: o Reino Unido "
            "aparece como UK e a Suíça como SZ. 130 códigos no período.",
            "This is the FIPS 10-4 country code, not the ISO code: the United "
            "Kingdom appears as UK and Switzerland as SZ. 130 codes over the "
            "period.",
            "Es el código FIPS 10-4 de país, no el código ISO: el Reino Unido "
            "aparece como UK y Suiza como SZ. 130 códigos en el período.",
        ),
        original="ORG_FIPS",
    ),
    col(
        "project_start",
        "DATE",
        "Data de início do projeto",
        "Start date of the project",
        "Fecha de inicio del proyecto",
        obs=OBS_DATE_FORMATS,
        original="PROJECT_START",
    ),
    col(
        "project_end",
        "DATE",
        "Data de término do projeto, incluindo anos futuros já comprometidos",
        "End date of the project, including future years already committed",
        "Fecha de término del proyecto, incluyendo años futuros ya comprometidos",
        obs=OBS_DATE_FORMATS,
        original="PROJECT_END",
    ),
    col(
        "budget_start",
        "DATE",
        "Data em que começa o financiamento do projeto no ano fiscal",
        "Date on which the project's funding for the fiscal year begins",
        "Fecha en que comienza el financiamiento del proyecto en el año fiscal",
        obs=OBS_DATE_FORMATS,
        original="BUDGET_START",
    ),
    col(
        "budget_end",
        "DATE",
        "Data em que termina o financiamento do projeto no ano fiscal",
        "Date on which the project's funding for the fiscal year ends",
        "Fecha en que termina el financiamiento del proyecto en el año fiscal",
        obs=OBS_DATE_FORMATS,
        original="BUDGET_END",
    ),
    col(
        "award_notice_date",
        "DATE",
        "Data do aviso de concessão que obriga os recursos e define o período e as condições do apoio",
        "Date of the award notice that obligates the funds and defines the period and terms of support",
        "Fecha del aviso de concesión que obliga los recursos y define el período y las condiciones del apoyo",
        obs=(
            "Publicado em três formatos ao longo do período, AAAA-MM-DD, "
            "M/D/AAAA e AAAA-MM-DD com hora, todos normalizados para "
            "AAAA-MM-DD na limpeza. Vazio em 21,93% das linhas.",
            "Published in three formats across the period, YYYY-MM-DD, "
            "M/D/YYYY and YYYY-MM-DD with a time, all normalised to YYYY-MM-DD "
            "in cleaning. Blank on 21.93% of rows.",
            "Publicado en tres formatos a lo largo del período, AAAA-MM-DD, "
            "M/D/AAAA y AAAA-MM-DD con hora, todos normalizados a AAAA-MM-DD en "
            "la limpieza. Vacío en el 21,93% de las filas.",
        ),
        original="AWARD_NOTICE_DATE",
    ),
    col(
        "nih_spending_cats",
        "STRING",
        "Categorias de gasto do NIH em que o projeto foi classificado, separadas por ponto e vírgula",
        "NIH spending categories the project was classified into, separated by semicolons",
        "Categorías de gasto del NIH en que el proyecto fue clasificado, separadas por punto y coma",
        obs=(
            "Categorias de reporte determinadas pelo Congresso, atribuídas "
            "apenas a partir do ano fiscal de 2008 e divulgadas no ano seguinte "
            "ao da concessão. Vazio em 57,41% das linhas.",
            "Congressionally mandated reporting categories, assigned only from "
            "fiscal year 2008 on and released in the year following the award. "
            "Blank on 57.41% of rows.",
            "Categorías de reporte determinadas por el Congreso, asignadas solo "
            "a partir del año fiscal de 2008 y divulgadas en el año siguiente al "
            "de la subvención. Vacío en el 57,41% de las filas.",
        ),
        original="NIH_SPENDING_CATS",
    ),
    col(
        "project_terms",
        "STRING",
        "Termos que descrevem o conteúdo do projeto, separados por ponto e vírgula",
        "Terms describing the content of the project, separated by semicolons",
        "Términos que describen el contenido del proyecto, separados por punto y coma",
        obs=(
            "Até o ano fiscal de 2007 são termos de tesauro atribuídos por "
            "indexadores; a partir de 2008 são conceitos extraídos "
            "automaticamente do título, do resumo e dos objetivos específicos. "
            "As duas gerações de termos não são comparáveis entre si.",
            "Through fiscal year 2007 these are thesaurus terms assigned by "
            "indexers; from 2008 on they are concepts mined automatically from "
            "the title, abstract and specific aims. The two generations of "
            "terms are not comparable with each other.",
            "Hasta el año fiscal de 2007 son términos de tesauro asignados por "
            "indexadores; a partir de 2008 son conceptos extraídos "
            "automáticamente del título, del resumen y de los objetivos "
            "específicos. Las dos generaciones de términos no son comparables "
            "entre sí.",
        ),
        original="PROJECT_TERMS",
    ),
    col(
        "public_health_relevance",
        "STRING",
        "Declaração apresentada com a solicitação sobre o potencial do projeto de melhorar a saúde pública",
        "Statement submitted with the application on the project's potential to improve public health",
        "Declaración presentada con la solicitud sobre el potencial del proyecto de mejorar la salud pública",
        obs=(
            "Vazio em 66,71% das linhas.",
            "Blank on 66.71% of rows.",
            "Vacío en el 66,71% de las filas.",
        ),
        original="PHR",
    ),
    col(
        "direct_cost",
        "FLOAT64",
        "Custo direto total do projeto no ano fiscal, somados todos os institutos e centros",
        "Total direct cost of the project in the fiscal year, across all institutes and centres",
        "Costo directo total del proyecto en el año fiscal, sumados todos los institutos y centros",
        unit="USD",
        obs=(
            "Publicado apenas a partir do ano fiscal de 2006 e preenchido "
            "somente para concessões do NIH a partir de 2012; não se aplica a "
            "concessões SBIR e STTR.",
            "Published only from fiscal year 2006 on and filled only for NIH "
            "awards from 2012 on; it does not apply to SBIR and STTR awards.",
            "Publicado solo a partir del año fiscal de 2006 y completado solo "
            "para subvenciones del NIH a partir de 2012; no se aplica a "
            "subvenciones SBIR y STTR.",
        ),
        original="DIRECT_COST_AMT",
    ),
    col(
        "indirect_cost",
        "FLOAT64",
        "Custo indireto total do projeto no ano fiscal, somados todos os institutos e centros",
        "Total indirect cost of the project in the fiscal year, across all institutes and centres",
        "Costo indirecto total del proyecto en el año fiscal, sumados todos los institutos y centros",
        unit="USD",
        obs=(
            "Publicado apenas a partir do ano fiscal de 2006 e preenchido "
            "somente para concessões do NIH a partir de 2012; não se aplica a "
            "concessões SBIR e STTR.",
            "Published only from fiscal year 2006 on and filled only for NIH "
            "awards from 2012 on; it does not apply to SBIR and STTR awards.",
            "Publicado solo a partir del año fiscal de 2006 y completado solo "
            "para subvenciones del NIH a partir de 2012; no se aplica a "
            "subvenciones SBIR y STTR.",
        ),
        original="INDIRECT_COST_AMT",
    ),
    col(
        "total_cost",
        "FLOAT64",
        "Financiamento total do projeto no ano fiscal, somados todos os institutos e centros",
        "Total funding of the project in the fiscal year, across all institutes and centres",
        "Financiamiento total del proyecto en el año fiscal, sumados todos los institutos y centros",
        unit="USD",
        obs=(
            OBS_COST[0]
            + " Em concessões multiprojeto o valor cobre todos os subprojetos e "
            "consta apenas do registro-pai; o custo de cada subprojeto está em "
            "total_cost_subproject.",
            OBS_COST[1]
            + " On multi-project awards the value covers every subproject and "
            "appears only on the parent record; each subproject's cost is in "
            "total_cost_subproject.",
            OBS_COST[2]
            + " En subvenciones multiproyecto el valor cubre todos los "
            "subproyectos y consta solo en el registro padre; el costo de cada "
            "subproyecto está en total_cost_subproject.",
        ),
        original="TOTAL_COST",
    ),
    col(
        "total_cost_subproject",
        "FLOAT64",
        "Financiamento total do subprojeto no ano fiscal, somados todos os institutos e centros",
        "Total funding of the subproject in the fiscal year, across all institutes and centres",
        "Financiamiento total del subproyecto en el año fiscal, sumados todos los institutos y centros",
        unit="USD",
        obs=(
            "Aplica-se somente a registros de subprojeto. Vazio em 87,50% das "
            "linhas.",
            "Applies only to subproject records. Blank on 87.50% of rows.",
            "Se aplica solo a registros de subproyecto. Vacío en el 87,50% de "
            "las filas.",
        ),
        original="TOTAL_COST_SUB_PROJECT",
    ),
]

# --------------------------------------------------------------------------
# project_abstract
# --------------------------------------------------------------------------

PROJECT_ABSTRACT = [
    col(
        "year",
        "INT64",
        "Ano fiscal do arquivo de resumos de que o registro provém",
        "Fiscal year of the abstract file the record comes from",
        "Año fiscal del archivo de resúmenes del que proviene el registro",
        directory=TIME_DIR,
        unit="year",
        obs=OBS_FISCAL_YEAR,
        original="",
    ),
    col(
        "application_id",
        "STRING",
        "Identificador único do registro do projeto na base RePORTER",
        "Unique identifier of the project record in the RePORTER database",
        "Identificador único del registro del proyecto en la base RePORTER",
        obs=(
            "Liga esta tabela a project pelo par (year, application_id). A "
            "correspondência é quase completa: 943 das 2.599.720 linhas de "
            "resumo (0,04%) não têm registro correspondente em project no mesmo "
            "ano fiscal, uma inconsistência entre os dois arquivos da própria "
            "fonte, mantida como publicada.",
            "Joins this table to project on the pair (year, application_id). "
            "The match is all but complete: 943 of the 2,599,720 abstract rows "
            "(0.04%) have no matching project record in the same fiscal year, "
            "an inconsistency between the source's own two files, kept as "
            "published.",
            "Une esta tabla con project por el par (year, application_id). La "
            "correspondencia es casi completa: 943 de las 2.599.720 filas de "
            "resumen (0,04%) no tienen registro correspondiente en project en el "
            "mismo año fiscal, una inconsistencia entre los dos archivos de la "
            "propia fuente, mantenida como se publica.",
        ),
        original="APPLICATION_ID",
    ),
    col(
        "abstract_text",
        "STRING",
        "Resumo da pesquisa realizada no projeto",
        "Abstract of the research carried out in the project",
        "Resumen de la investigación realizada en el proyecto",
        obs=(
            "Nas concessões o resumo é fornecido ao NIH pelo beneficiário. "
            "Nem todo projeto tem resumo publicado.",
            "For grants the abstract is supplied to NIH by the recipient. Not "
            "every project has a published abstract.",
            "En las subvenciones el resumen es proporcionado al NIH por el "
            "beneficiario. No todo proyecto tiene resumen publicado.",
        ),
        original="ABSTRACT_TEXT",
    ),
]

# --------------------------------------------------------------------------
# publication
# --------------------------------------------------------------------------

OBS_CALENDAR_YEAR = (
    "Ano-CALENDÁRIO de divulgação do arquivo de publicações de que o registro "
    "provém, e não o ano fiscal usado nas tabelas project e project_abstract. "
    "Coluna de particionamento.",
    "CALENDAR year of release of the publication file the record comes from, "
    "not the fiscal year used in the project and project_abstract tables. "
    "Partition column.",
    "Año CALENDARIO de divulgación del archivo de publicaciones del que "
    "proviene el registro, y no el año fiscal usado en las tablas project y "
    "project_abstract. Columna de particionamiento.",
)

PUBLICATION = [
    col(
        "year",
        "INT64",
        "Ano-calendário do arquivo de publicações de que o registro provém",
        "Calendar year of the publication file the record comes from",
        "Año calendario del archivo de publicaciones del que proviene el registro",
        directory=TIME_DIR,
        unit="year",
        obs=OBS_CALENDAR_YEAR,
        original="",
    ),
    col(
        "pmid",
        "STRING",
        "Identificador único da publicação no PubMed",
        "Unique identifier of the publication in PubMed",
        "Identificador único de la publicación en PubMed",
        obs=(
            "Número de acesso de um a oito dígitos, sem zeros à esquerda. Liga "
            "esta tabela a publication_link.",
            "One- to eight-digit accession number with no leading zeros. Joins "
            "this table to publication_link.",
            "Número de acceso de uno a ocho dígitos, sin ceros a la izquierda. "
            "Une esta tabla con publication_link.",
        ),
        original="PMID",
    ),
    col(
        "pmc_id",
        "STRING",
        "Identificador único do artigo no PubMed Central",
        "Unique identifier of the article in PubMed Central",
        "Identificador único del artículo en PubMed Central",
        original="PMC_ID",
    ),
    col(
        "issn",
        "STRING",
        "Número internacional normalizado que identifica o periódico",
        "International standard serial number identifying the journal",
        "Número internacional normalizado que identifica la revista",
        original="ISSN",
    ),
    col(
        "publication_title",
        "STRING",
        "Título do artigo",
        "Title of the journal article",
        "Título del artículo",
        obs=(
            "Sempre em inglês; títulos originalmente publicados em outra língua "
            "e traduzidos aparecem entre colchetes.",
            "Always in English; titles originally published in another language "
            "and translated appear in square brackets.",
            "Siempre en inglés; los títulos publicados originalmente en otra "
            "lengua y traducidos aparecen entre corchetes.",
        ),
        original="PUB_TITLE",
    ),
    col(
        "journal_title",
        "STRING",
        "Título completo do periódico",
        "Full title of the journal",
        "Título completo de la revista",
        original="JOURNAL_TITLE",
    ),
    col(
        "journal_title_abbreviation",
        "STRING",
        "Abreviatura padronizada do título do periódico",
        "Standard abbreviation of the journal title",
        "Abreviatura estandarizada del título de la revista",
        original="JOURNAL_TITLE_ABBR",
    ),
    col(
        "journal_volume",
        "STRING",
        "Volume do periódico em que o artigo foi publicado",
        "Volume of the journal in which the article was published",
        "Volumen de la revista en que se publicó el artículo",
        original="JOURNAL_VOLUME",
    ),
    col(
        "journal_issue",
        "STRING",
        "Número, parte ou suplemento do periódico em que o artigo foi publicado",
        "Issue, part or supplement of the journal in which the article was published",
        "Número, parte o suplemento de la revista en que se publicó el artículo",
        original="JOURNAL_ISSUE",
    ),
    col(
        "page_number",
        "STRING",
        "Páginas do artigo no periódico",
        "Pages of the article in the journal",
        "Páginas del artículo en la revista",
        obs=(
            "A paginação pode não ser numérica; artigos eletrônicos trazem aqui "
            "o número do documento.",
            "The pagination need not be numeric; electronic articles carry the "
            "document number here.",
            "La paginación puede no ser numérica; los artículos electrónicos "
            "traen aquí el número del documento.",
        ),
        original="PAGE_NUMBER",
    ),
    col(
        "publication_date",
        "STRING",
        "Data de publicação do fascículo, conforme impressa no periódico",
        "Publication date of the journal issue, as printed in the journal",
        "Fecha de publicación del fascículo, tal como está impresa en la revista",
        obs=(
            "Mantida como texto porque a fonte a publica como os elementos que "
            "constam do fascículo, que nem sempre incluem mês e dia.",
            "Kept as text because the source publishes whichever elements the "
            "issue carries, which do not always include a month and a day.",
            "Mantenida como texto porque la fuente la publica con los elementos "
            "que constan en el fascículo, que no siempre incluyen mes y día.",
        ),
        original="PUB_DATE",
    ),
    col(
        "publication_year",
        "INT64",
        "Ano de publicação do artigo",
        "Year the article was published",
        "Año de publicación del artículo",
        unit="year",
        obs=(
            "Extraído pela fonte de publication_date. Pode diferir do ano da "
            "coluna year, que é o do arquivo de divulgação.",
            "Extracted by the source from publication_date. It can differ from "
            "the year in the year column, which is that of the release file.",
            "Extraído por la fuente de publication_date. Puede diferir del año "
            "de la columna year, que es el del archivo de divulgación.",
        ),
        original="PUB_YEAR",
    ),
    col(
        "author_list",
        "STRING",
        "Autores do artigo, separados por ponto e vírgula",
        "Authors of the article, separated by semicolons",
        "Autores del artículo, separados por punto y coma",
        obs=(
            "Cada autor aparece como sobrenome seguido de até duas iniciais e, "
            "quando aplicável, de uma abreviatura de sufixo.",
            "Each author appears as a surname followed by up to two initials "
            "and, where applicable, a suffix abbreviation.",
            "Cada autor aparece como apellido seguido de hasta dos iniciales y, "
            "cuando corresponde, de una abreviatura de sufijo.",
        ),
        original="AUTHOR_LIST",
    ),
    col(
        "affiliation",
        "STRING",
        "Vínculo institucional do primeiro autor",
        "Institutional affiliation of the first author",
        "Vinculación institucional del primer autor",
        original="AFFILIATION",
    ),
    col(
        "country",
        "STRING",
        "País de publicação do periódico",
        "Country of publication of the journal",
        "País de publicación de la revista",
        obs=(
            "Usa os nomes de país da categoria Z do vocabulário MeSH da "
            "Biblioteca Nacional de Medicina dos Estados Unidos.",
            "Uses the country names in category Z of the United States National "
            "Library of Medicine's MeSH vocabulary.",
            "Usa los nombres de país de la categoría Z del vocabulario MeSH de "
            "la Biblioteca Nacional de Medicina de los Estados Unidos.",
        ),
        original="COUNTRY",
    ),
    col(
        "language",
        "STRING",
        "Língua em que o artigo foi publicado",
        "Language in which the article was published",
        "Lengua en que se publicó el artículo",
        obs=(
            "Abreviaturas de três letras em caixa baixa, como eng, fre e ger.",
            "Lower-case three-letter abbreviations, such as eng, fre and ger.",
            "Abreviaturas de tres letras en minúsculas, como eng, fre y ger.",
        ),
        original="LANG",
    ),
]

# --------------------------------------------------------------------------
# publication_link
# --------------------------------------------------------------------------

PUBLICATION_LINK = [
    col(
        "year",
        "INT64",
        "Ano-calendário do arquivo de ligações de que o registro provém",
        "Calendar year of the link file the record comes from",
        "Año calendario del archivo de enlaces del que proviene el registro",
        directory=TIME_DIR,
        unit="year",
        obs=OBS_CALENDAR_YEAR,
        original="",
    ),
    col(
        "pmid",
        "STRING",
        "Identificador único da publicação no PubMed",
        "Unique identifier of the publication in PubMed",
        "Identificador único de la publicación en PubMed",
        original="PMID",
    ),
    col(
        "core_project_num",
        "STRING",
        "Identificador do projeto de pesquisa citado como fonte de apoio da publicação",
        "Identifier of the research project cited as supporting the publication",
        "Identificador del proyecto de investigación citado como fuente de apoyo de la publicación",
        obs=(
            "Liga esta tabela a project pela coluna core_project_num. A "
            "associação vem dos agradecimentos do artigo ou do sistema de "
            "submissão de manuscritos do NIH, e não identifica um ano do "
            "projeto nem um ano fiscal de financiamento. 342.576 das 7.582.090 "
            "linhas (4,5%) citam um projeto que não consta de project, porque "
            "o arquivo de projetos começa no ano fiscal de 1985 e as "
            "publicações mais antigas citam concessões anteriores a ele: a "
            "proporção sem correspondência cai de 58% no arquivo de 1980 para "
            "cerca de 1% a partir de 2020, e os códigos de atividade sem "
            "correspondência são os que saíram de uso, como R23, K04 e T01.",
            "Joins this table to project on the core_project_num column. The "
            "association comes from the article's acknowledgements or from the "
            "NIH manuscript submission system, and identifies neither a year of "
            "the project nor a fiscal year of funding. 342,576 of the 7,582,090 "
            "rows (4.5%) cite a project absent from project, because the "
            "project file starts in fiscal year 1985 and older publications "
            "cite awards that predate it: the unmatched share falls from 58% in "
            "the 1980 file to about 1% from 2020 on, and the unmatched activity "
            "codes are the discontinued ones, such as R23, K04 and T01.",
            "Une esta tabla con project por la columna core_project_num. La "
            "asociación proviene de los agradecimientos del artículo o del "
            "sistema de envío de manuscritos del NIH, y no identifica un año "
            "del proyecto ni un año fiscal de financiamiento. 342.576 de las "
            "7.582.090 filas (4,5%) citan un proyecto que no consta en project, "
            "porque el archivo de proyectos comienza en el año fiscal de 1985 y "
            "las publicaciones más antiguas citan subvenciones anteriores: la "
            "proporción sin correspondencia cae del 58% en el archivo de 1980 a "
            "cerca del 1% a partir de 2020, y los códigos de actividad sin "
            "correspondencia son los que dejaron de usarse, como R23, K04 y T01.",
        ),
        original="PROJECT_NUMBER",
    ),
]

# --------------------------------------------------------------------------
# patent_link
# --------------------------------------------------------------------------

PATENT_LINK = [
    col(
        "patent_id",
        "STRING",
        "Número da patente no banco de patentes concedidas do escritório de patentes e marcas dos Estados Unidos",
        "Patent number in the United States Patent and Trademark Office database of issued patents",
        "Número de la patente en el banco de patentes concedidas de la oficina de patentes y marcas de los Estados Unidos",
        obs=(
            "Predominantemente numérico, com sete ou oito dígitos; também "
            "ocorrem os prefixos RE, para reemissões, D, para patentes de "
            "desenho, H, para invenções estatutárias, e PP, para patentes de "
            "plantas.",
            "Predominantly numeric, with seven or eight digits; the prefixes RE "
            "for reissues, D for design patents, H for statutory inventions and "
            "PP for plant patents also occur.",
            "Predominantemente numérico, con siete u ocho dígitos; también "
            "ocurren los prefijos RE, para reemisiones, D, para patentes de "
            "diseño, H, para invenciones estatutarias, y PP, para patentes de "
            "plantas.",
        ),
        original="PATENT_ID",
    ),
    col(
        "core_project_num",
        "STRING",
        "Identificador do projeto de pesquisa reconhecido como apoio ao desenvolvimento da patente",
        "Identifier of the research project acknowledged as supporting development of the patent",
        "Identificador del proyecto de investigación reconocido como apoyo al desarrollo de la patente",
        obs=(
            "Liga esta tabela a project pela coluna core_project_num. A "
            "associação vem do sistema iEdison, em que as organizações "
            "beneficiárias reportam invenções, e não identifica um ano do "
            "projeto nem um ano fiscal de financiamento.",
            "Joins this table to project on the core_project_num column. The "
            "association comes from the iEdison system, through which recipient "
            "organisations report inventions, and identifies neither a year of "
            "the project nor a fiscal year of funding.",
            "Une esta tabla con project por la columna core_project_num. La "
            "asociación proviene del sistema iEdison, en que las organizaciones "
            "beneficiarias reportan invenciones, y no identifica un año del "
            "proyecto ni un año fiscal de financiamiento.",
        ),
        original="PROJECT_ID",
    ),
    col(
        "patent_title",
        "STRING",
        "Título da patente",
        "Title of the patent",
        "Título de la patente",
        original="PATENT_TITLE",
    ),
    col(
        "patent_org_name",
        "STRING",
        "Nome da organização ou pessoa titular da patente",
        "Name of the organisation or person that owns the patent",
        "Nombre de la organización o persona titular de la patente",
        obs=(
            "Pode diferir da organização que recebeu a concessão ou o contrato. "
            "Vazio em 2.343 das 92.936 linhas publicadas. Faz parte da chave da "
            "tabela: 36 pares de patente e projeto aparecem duas vezes, sempre "
            "com o mesmo título e dois titulares diferentes, e a tripla "
            "(patent_id, core_project_num, patent_org_name) é única em todas as "
            "92.936 linhas.",
            "May differ from the organisation that received the grant or "
            "contract. Blank on 2,343 of the 92,936 published rows. It is part "
            "of the table's key: 36 patent-project pairs appear twice, always "
            "with the same title and two different owners, and the triple "
            "(patent_id, core_project_num, patent_org_name) is unique on all "
            "92,936 rows.",
            "Puede diferir de la organización que recibió la subvención o el "
            "contrato. Vacío en 2.343 de las 92.936 filas publicadas. Forma "
            "parte de la clave de la tabla: 36 pares de patente y proyecto "
            "aparecen dos veces, siempre con el mismo título y dos titulares "
            "diferentes, y la tripla (patent_id, core_project_num, "
            "patent_org_name) es única en las 92.936 filas.",
        ),
        original="PATENT_ORG_NAME",
    ),
]

# --------------------------------------------------------------------------
# clinical_study_link
# --------------------------------------------------------------------------

CLINICAL_STUDY_LINK = [
    col(
        "nct_id",
        "STRING",
        "Identificador do estudo clínico no registro ClinicalTrials.gov",
        "Identifier of the clinical study in the ClinicalTrials.gov registry",
        "Identificador del estudio clínico en el registro ClinicalTrials.gov",
        obs=(
            "Formado pelas letras NCT seguidas de oito dígitos, em todas as "
            "39.560 linhas publicadas.",
            "Made up of the letters NCT followed by eight digits, on all 39,560 "
            "published rows.",
            "Formado por las letras NCT seguidas de ocho dígitos, en todas las "
            "39.560 filas publicadas.",
        ),
        original="ClinicalTrials.gov ID",
    ),
    col(
        "core_project_num",
        "STRING",
        "Identificador do projeto de pesquisa reconhecido como apoio ao estudo clínico",
        "Identifier of the research project acknowledged as supporting the clinical study",
        "Identificador del proyecto de investigación reconocido como apoyo al estudio clínico",
        obs=(
            "Liga esta tabela a project pela coluna core_project_num. A "
            "associação vem do próprio ClinicalTrials.gov, que informa ao "
            "RePORTER os números de concessão declarados no registro do estudo, "
            "e não identifica um ano do projeto nem um ano fiscal de "
            "financiamento.",
            "Joins this table to project on the core_project_num column. The "
            "association comes from ClinicalTrials.gov itself, which reports to "
            "RePORTER the grant numbers entered in the study's registration, and "
            "identifies neither a year of the project nor a fiscal year of "
            "funding.",
            "Une esta tabla con project por la columna core_project_num. La "
            "asociación proviene del propio ClinicalTrials.gov, que informa al "
            "RePORTER los números de subvención declarados en el registro del "
            "estudio, y no identifica un año del proyecto ni un año fiscal de "
            "financiamiento.",
        ),
        original="Core Project Number",
    ),
    col(
        "study_title",
        "STRING",
        "Título do estudo clínico conforme consta do ClinicalTrials.gov",
        "Title of the clinical study as it appears in ClinicalTrials.gov",
        "Título del estudio clínico tal como consta en ClinicalTrials.gov",
        original="Study",
    ),
    col(
        "study_status",
        "STRING",
        "Estágio atual do estudo clínico",
        "Current stage of the clinical study",
        "Etapa actual del estudio clínico",
        obs=(
            "Catorze valores, publicados como rótulos legíveis e não como "
            "códigos. Descreve a situação do estudo na data de extração do "
            "arquivo, e não no ano fiscal do projeto.",
            "Fourteen values, published as readable labels rather than codes. It "
            "describes the study's situation on the file's extraction date, not "
            "in the project's fiscal year.",
            "Catorce valores, publicados como etiquetas legibles y no como "
            "códigos. Describe la situación del estudio en la fecha de "
            "extracción del archivo, y no en el año fiscal del proyecto.",
        ),
        original="Study Status",
    ),
]

# --------------------------------------------------------------------------
# dicionario
# --------------------------------------------------------------------------

DICIONARIO = [
    col(
        "id_tabela",
        "STRING",
        "Nome da tabela a que a coluna codificada pertence",
        "Name of the table the coded column belongs to",
        "Nombre de la tabla a la que pertenece la columna codificada",
    ),
    col(
        "nome_coluna",
        "STRING",
        "Nome da coluna codificada",
        "Name of the coded column",
        "Nombre de la columna codificada",
    ),
    col(
        "chave",
        "STRING",
        "Valor armazenado na coluna",
        "Value stored in the column",
        "Valor almacenado en la columna",
    ),
    col(
        "cobertura_temporal",
        "STRING",
        "Anos fiscais em que o valor aparece, na notação início(intervalo)fim",
        "Fiscal years in which the value appears, in start(interval)end notation",
        "Años fiscales en que aparece el valor, en la notación inicio(intervalo)fin",
    ),
    col(
        "valor",
        "STRING",
        "Significado do valor armazenado",
        "Meaning of the stored value",
        "Significado del valor almacenado",
        obs=(
            "Vazio quando a fonte não publica rótulo para o código, o que "
            "ocorre nos códigos de atividade de contratos, de projetos "
            "intramuros e de agências que não o NIH.",
            "Blank when the source publishes no label for the code, which "
            "happens for the activity codes of contracts, intramural projects "
            "and agencies other than NIH.",
            "Vacío cuando la fuente no publica etiqueta para el código, lo que "
            "ocurre en los códigos de actividad de contratos, de proyectos "
            "intramuros y de agencias distintas del NIH.",
        ),
    ),
]

TABLES = {
    "project": PROJECT,
    "project_abstract": PROJECT_ABSTRACT,
    "publication": PUBLICATION,
    "publication_link": PUBLICATION_LINK,
    "patent_link": PATENT_LINK,
    "clinical_study_link": CLINICAL_STUDY_LINK,
    "dicionario": DICIONARIO,
}


def main() -> None:
    ARCH.mkdir(parents=True, exist_ok=True)
    for table, cols in TABLES.items():
        path = ARCH / f"sheet_{table}.csv"
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS)
            w.writeheader()
            for c in cols:
                w.writerow(c)
        print(f"wrote {path.name} ({len(cols)} columns)")


if __name__ == "__main__":
    main()
