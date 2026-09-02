"""
Trilingual column descriptions for us_ed_college_scorecard.

Data Basis requires every column description in Portuguese, English and
Spanish. Normalising the source into long tables cut the number of columns
needing a description from 3,486 to 316, which is small enough to write
deliberately rather than machine-translate.

The 179 `field_of_study` descriptions are composed from a phrase glossary
applied to the U.S. Department of Education's own labels: those labels are
templated ("Median Stafford and Grad PLUS loan debt disbursed to males at
this institution"), so composing beats free translation for consistency.
"""

# --------------------------------------------------------- shared fragments

YEAR = (
    "Ano de referência da coorte, conforme o arquivo anual publicado pela fonte",
    "Reference year of the cohort, as published in the source's annual file",
    "Año de referencia de la cohorte, según el archivo anual publicado por la fuente",
)

UNITID = (
    "Código de identificação da instituição no IPEDS (UNITID). Mesma chave da tabela "
    "us_ed_ipeds; como lá o tipo é INT64, o cruzamento exige safe_cast(unitid as int64)",
    "IPEDS institution identifier (UNITID). Same key as the us_ed_ipeds dataset; that "
    "dataset types it as INT64, so joining requires safe_cast(unitid as int64)",
    "Código de identificación de la institución en IPEDS (UNITID). Misma clave que la tabla "
    "us_ed_ipeds; allí el tipo es INT64, por lo que el cruce exige safe_cast(unitid as int64)",
)

VARIABLE_NAME = (
    "Nome da variável publicada pela fonte (ex.: MD_EARN_WNE_P10). Definição, rótulo e "
    "unidade estão na tabela variable",
    "Variable name as published by the source (e.g. MD_EARN_WNE_P10). Its definition, label "
    "and unit are in the variable table",
    "Nombre de la variable publicada por la fuente (ej.: MD_EARN_WNE_P10). Su definición, "
    "etiqueta y unidad están en la tabla variable",
)

VALUE = (
    "Valor numérico publicado. A unidade depende da variável; ver a tabela variable. Nulo "
    "quando a fonte publicou um intervalo, uma data, um rótulo textual ou a supressão por "
    "sigilo; nesses casos o conteúdo está em value_raw",
    "Published numeric value. The unit depends on the variable; see the variable table. Null "
    "when the source published an interval, a date, a text label or a privacy suppression "
    "instead; in those cases the content is in value_raw",
    "Valor numérico publicado. La unidad depende de la variable; ver la tabla variable. Nulo "
    "cuando la fuente publicó un intervalo, una fecha, una etiqueta textual o la supresión "
    "por confidencialidad; en esos casos el contenido está en value_raw",
)

VALUE_RAW = (
    "Conteúdo publicado quando não é um número: PrivacySuppressed (célula suprimida por "
    "sigilo), um intervalo de arredondamento como 0.30-0.39 ou <=0.10 (colunas BBRR), uma "
    "data MM/DD/AAAA ou um rótulo textual. Nulo quando value está preenchido",
    "Published content when it is not a number: PrivacySuppressed (cell withheld for "
    "privacy), a rounding interval such as 0.30-0.39 or <=0.10 (BBRR columns), an MM/DD/YYYY "
    "date or a text label. Null when value is populated",
    "Contenido publicado cuando no es un número: PrivacySuppressed (celda suprimida por "
    "confidencialidad), un intervalo de redondeo como 0.30-0.39 o <=0.10 (columnas BBRR), una "
    "fecha MM/DD/AAAA o una etiqueta textual. Nulo cuando value está lleno",
)

LONG_COLUMNS = [
    ("year", YEAR),
    ("unitid", UNITID),
    ("variable_name", VARIABLE_NAME),
    ("value", VALUE),
    ("value_raw", VALUE_RAW),
]

# ---------------------------------------------------- `variable` catalogue

VARIABLE_TABLE = [
    (
        "variable_name",
        (
            "Nome da variável conforme publicado pela fonte",
            "Variable name as published by the source",
            "Nombre de la variable según lo publicado por la fuente",
        ),
    ),
    (
        "source_file",
        (
            "Arquivo de origem da variável: institution ou field_of_study",
            "Source file the variable comes from: institution or field_of_study",
            "Archivo de origen de la variable: institution o field_of_study",
        ),
    ),
    (
        "table_name",
        (
            "Tabela deste conjunto em que a variável foi carregada",
            "Table of this dataset the variable was loaded into",
            "Tabla de este conjunto en la que se cargó la variable",
        ),
    ),
    (
        "api_name",
        (
            "Nome hierárquico da variável na API do College Scorecard",
            "Hierarchical name of the variable in the College Scorecard API",
            "Nombre jerárquico de la variable en la API del College Scorecard",
        ),
    ),
    (
        "data_type",
        (
            "Tipo declarado pela fonte para a variável",
            "Data type the source declares for the variable",
            "Tipo declarado por la fuente para la variable",
        ),
    ),
    (
        "label",
        (
            "Definição da variável no dicionário de dados oficial, incluindo os rótulos dos "
            "códigos quando existem",
            "Definition of the variable in the official data dictionary, including code labels "
            "where they exist",
            "Definición de la variable en el diccionario de datos oficial, incluidas las "
            "etiquetas de los códigos cuando existen",
        ),
    ),
]

DICIONARIO_TABLE = [
    (
        "id_tabela",
        (
            "Nome da tabela que contém a coluna traduzida por este dicionário",
            "Name of the table holding the column this dictionary translates",
            "Nombre de la tabla que contiene la columna traducida por este diccionario",
        ),
    ),
    (
        "nome_coluna",
        (
            "Nome da coluna traduzida por este dicionário",
            "Name of the column this dictionary translates",
            "Nombre de la columna traducida por este diccionario",
        ),
    ),
    (
        "chave",
        (
            "Código presente na coluna original",
            "Code as stored in the original column",
            "Código presente en la columna original",
        ),
    ),
    (
        "cobertura_temporal",
        (
            "Cobertura temporal em que a chave é válida",
            "Temporal coverage over which the key is valid",
            "Cobertura temporal en que la clave es válida",
        ),
    ),
    (
        "valor",
        (
            "Rótulo correspondente ao código, conforme o dicionário de dados oficial",
            "Label matching the code, per the official data dictionary",
            "Etiqueta correspondiente al código, según el diccionario de datos oficial",
        ),
    ),
]


# ------------------------------------------------- wide `institution` table
# (pt, en, es) per column. No trailing period; first letter capitalised.

INSTITUTION = {
    "year": YEAR,
    "unitid": UNITID,
    "opeid8": (
        "Código OPE de 8 dígitos da instituição, atribuído pelo Escritório de Educação Superior",
        "8-digit OPE ID of the institution, assigned by the Office of Postsecondary Education",
        "Código OPE de 8 dígitos de la institución, asignado por la Oficina de Educación Superior",
    ),
    "opeid6": (
        "Código OPE de 6 dígitos da instituição, comum a todos os campi de uma mesma entidade",
        "6-digit OPE ID of the institution, shared by every campus of the same entity",
        "Código OPE de 6 dígitos de la institución, común a todos los campus de una misma entidad",
    ),
    "federal_school_code": (
        "Código federal da instituição usado no formulário FAFSA",
        "Federal school code used on the FAFSA form",
        "Código federal de la institución usado en el formulario FAFSA",
    ),
    "institution_name": (
        "Nome da instituição",
        "Institution name",
        "Nombre de la institución",
    ),
    "institution_alias": (
        "Nomes alternativos e siglas pelos quais a instituição é conhecida",
        "Alternative names and acronyms the institution is known by",
        "Nombres alternativos y siglas por los que se conoce la institución",
    ),
    "institution_url": (
        "Endereço do sítio eletrônico da instituição",
        "URL of the institution's website",
        "Dirección del sitio web de la institución",
    ),
    "net_price_calculator_url": (
        "Endereço da calculadora de preço líquido da instituição",
        "URL of the institution's net price calculator",
        "Dirección de la calculadora de precio neto de la institución",
    ),
    "address": (
        "Endereço da instituição",
        "Street address of the institution",
        "Dirección de la institución",
    ),
    "city": (
        "Município da instituição",
        "City of the institution",
        "Ciudad de la institución",
    ),
    "state_abbreviation": (
        "Sigla do estado norte-americano em que a instituição está localizada",
        "Two-letter postal code of the U.S. state where the institution is located",
        "Sigla del estado de los Estados Unidos donde se ubica la institución",
    ),
    "zip_code": (
        "Código postal (ZIP) da instituição",
        "ZIP code of the institution",
        "Código postal (ZIP) de la institución",
    ),
    "state_fips": (
        "Código FIPS do estado em que a instituição está localizada",
        "FIPS code of the state where the institution is located",
        "Código FIPS del estado donde se ubica la institución",
    ),
    "region": (
        "Região do IPEDS em que a instituição está localizada",
        "IPEDS region where the institution is located",
        "Región del IPEDS donde se ubica la institución",
    ),
    "locale": (
        "Grau de urbanização do local da instituição, segundo a classificação do NCES",
        "Degree of urbanisation of the institution's locale, per the NCES classification",
        "Grado de urbanización del lugar de la institución, según la clasificación del NCES",
    ),
    "locale_degree_urbanization": (
        "Grau de urbanização agregado do local da instituição",
        "Aggregated degree of urbanisation of the institution's locale",
        "Grado de urbanización agregado del lugar de la institución",
    ),
    "latitude": (
        "Latitude da instituição em graus decimais",
        "Latitude of the institution in decimal degrees",
        "Latitud de la institución en grados decimales",
    ),
    "longitude": (
        "Longitude da instituição em graus decimais",
        "Longitude of the institution in decimal degrees",
        "Longitud de la institución en grados decimales",
    ),
    "control": (
        "Natureza administrativa da instituição, conforme o IPEDS",
        "Control of the institution, per IPEDS",
        "Naturaleza administrativa de la institución, según el IPEDS",
    ),
    "control_peps": (
        "Natureza administrativa da instituição, conforme o sistema PEPS",
        "Control of the institution, per the PEPS system",
        "Naturaleza administrativa de la institución, según el sistema PEPS",
    ),
    "ownership_peps": (
        "Tipo de mantenedora da instituição, conforme o sistema PEPS",
        "Ownership type of the institution, per the PEPS system",
        "Tipo de titularidad de la institución, según el sistema PEPS",
    ),
    "scorecard_sector": (
        "Setor da instituição segundo a classificação do College Scorecard",
        "Sector of the institution under the College Scorecard classification",
        "Sector de la institución según la clasificación del College Scorecard",
    ),
    "institution_level": (
        "Nível da instituição segundo as características institucionais do IPEDS",
        "Level of the institution per IPEDS institutional characteristics",
        "Nivel de la institución según las características institucionales del IPEDS",
    ),
    "predominant_degree": (
        "Grau predominante concedido pela instituição na graduação",
        "Predominant undergraduate degree awarded by the institution",
        "Grado predominante otorgado por la institución en el pregrado",
    ),
    "predominant_degree_recoded": (
        "Grau predominante concedido, com os códigos 0 e 4 recodificados",
        "Predominant degree awarded, with codes 0 and 4 recoded",
        "Grado predominante otorgado, con los códigos 0 y 4 recodificados",
    ),
    "highest_degree": (
        "Grau mais alto concedido pela instituição",
        "Highest degree awarded by the institution",
        "Grado más alto otorgado por la institución",
    ),
    "main_campus": (
        "Indica se a unidade é o campus principal da instituição",
        "Whether the unit is the institution's main campus",
        "Indica si la unidad es el campus principal de la institución",
    ),
    "branch_campuses": (
        "Número de campi da instituição",
        "Number of campuses of the institution",
        "Número de campus de la institución",
    ),
    "carnegie_basic": (
        "Classificação Carnegie básica da instituição",
        "Carnegie basic classification of the institution",
        "Clasificación Carnegie básica de la institución",
    ),
    "carnegie_undergraduate_profile": (
        "Perfil de graduação da instituição na classificação Carnegie",
        "Carnegie undergraduate profile of the institution",
        "Perfil de pregrado de la institución en la clasificación Carnegie",
    ),
    "carnegie_size_setting": (
        "Porte e regime de residência da instituição na classificação Carnegie",
        "Carnegie size and setting of the institution",
        "Tamaño y régimen de residencia de la institución en la clasificación Carnegie",
    ),
    "online_only": (
        "Indica se a instituição oferece exclusivamente ensino a distância",
        "Whether the institution offers distance education only",
        "Indica si la institución ofrece exclusivamente educación a distancia",
    ),
    "currently_operating": (
        "Indica se a instituição estava em funcionamento no ano de referência",
        "Whether the institution was operating in the reference year",
        "Indica si la institución estaba en funcionamiento en el año de referencia",
    ),
    "open_admissions_policy": (
        "Indica se a instituição adota política de admissão aberta",
        "Whether the institution has an open admissions policy",
        "Indica si la institución aplica una política de admisión abierta",
    ),
    "religious_affiliation": (
        "Afiliação religiosa da instituição",
        "Religious affiliation of the institution",
        "Afiliación religiosa de la institución",
    ),
    "men_only": (
        "Indica se a instituição atende exclusivamente homens",
        "Whether the institution serves men only",
        "Indica si la institución atiende exclusivamente a hombres",
    ),
    "women_only": (
        "Indica se a instituição atende exclusivamente mulheres",
        "Whether the institution serves women only",
        "Indica si la institución atiende exclusivamente a mujeres",
    ),
    "historically_black": (
        "Indica se a instituição é uma faculdade ou universidade historicamente negra",
        "Whether the institution is a historically black college or university",
        "Indica si la institución es una universidad históricamente negra",
    ),
    "predominantly_black": (
        "Indica se a instituição é classificada como predominantemente negra",
        "Whether the institution is classified as predominantly black",
        "Indica si la institución se clasifica como predominantemente negra",
    ),
    "alaska_native_hawaiian_serving": (
        "Indica se a instituição atende predominantemente nativos do Alasca ou nativos havaianos",
        "Whether the institution predominantly serves Alaska Natives or Native Hawaiians",
        "Indica si la institución atiende predominantemente a nativos de Alaska o nativos hawaianos",
    ),
    "tribal_college": (
        "Indica se a instituição é uma faculdade tribal",
        "Whether the institution is a tribal college",
        "Indica si la institución es una universidad tribal",
    ),
    "asian_pacific_islander_serving": (
        "Indica se a instituição atende predominantemente asiático-americanos, nativos americanos ou nativos das ilhas do Pacífico",
        "Whether the institution predominantly serves Asian Americans, Native Americans or Pacific Islanders",
        "Indica si la institución atiende predominantemente a asiático-americanos, nativos americanos o isleños del Pacífico",
    ),
    "hispanic_serving": (
        "Indica se a instituição atende predominantemente estudantes hispânicos",
        "Whether the institution predominantly serves Hispanic students",
        "Indica si la institución atiende predominantemente a estudiantes hispanos",
    ),
    "native_american_non_tribal": (
        "Indica se a instituição atende predominantemente nativos americanos sem ser uma faculdade tribal",
        "Whether the institution predominantly serves Native Americans without being a tribal college",
        "Indica si la institución atiende predominantemente a nativos americanos sin ser una universidad tribal",
    ),
    "accreditor_name": (
        "Nome da agência acreditadora da instituição",
        "Name of the institution's accrediting agency",
        "Nombre de la agencia acreditadora de la institución",
    ),
    "accreditor_code": (
        "Código da agência acreditadora da instituição",
        "Code of the institution's accrediting agency",
        "Código de la agencia acreditadora de la institución",
    ),
    "title_iv_eligibility_type": (
        "Tipo de elegibilidade da instituição ao Título IV da Lei do Ensino Superior",
        "Institution's eligibility type under Title IV of the Higher Education Act",
        "Tipo de elegibilidad de la institución al Título IV de la Ley de Educación Superior",
    ),
    "title_iv_approval_date": (
        "Data de aprovação da instituição para o Título IV",
        "Date the institution was approved for Title IV",
        "Fecha de aprobación de la institución para el Título IV",
    ),
    "heightened_cash_monitoring": (
        "Indica se a instituição está sob monitoramento reforçado de repasses (Heightened Cash Monitoring 2)",
        "Whether the institution is under Heightened Cash Monitoring 2",
        "Indica si la institución está bajo monitoreo reforzado de desembolsos (Heightened Cash Monitoring 2)",
    ),
    "dol_provider": (
        "Indica se a instituição é provedora registrada junto ao Departamento do Trabalho",
        "Whether the institution is a registered Department of Labor provider",
        "Indica si la institución es proveedora registrada ante el Departamento de Trabajo",
    ),
    "undergraduate_enrollment": (
        "Número de estudantes de graduação matriculados em cursos de grau",
        "Number of degree-seeking undergraduate students enrolled",
        "Número de estudiantes de pregrado matriculados en carreras de grado",
    ),
    "undergraduate_enrollment_all": (
        "Número total de estudantes de graduação, incluindo os não matriculados em cursos de grau",
        "Total number of undergraduate students, including non-degree-seeking students",
        "Número total de estudiantes de pregrado, incluidos los no matriculados en carreras de grado",
    ),
    "tuition_revenue_per_fte": (
        "Receita líquida de mensalidades por estudante equivalente a tempo integral",
        "Net tuition revenue per full-time equivalent student",
        "Ingreso neto por matrícula por estudiante equivalente a tiempo completo",
    ),
    "instructional_expenditure_per_fte": (
        "Despesa com ensino por estudante equivalente a tempo integral",
        "Instructional expenditure per full-time equivalent student",
        "Gasto en enseñanza por estudiante equivalente a tiempo completo",
    ),
    "average_faculty_salary": (
        "Salário mensal médio do corpo docente",
        "Average monthly faculty salary",
        "Salario mensual promedio del cuerpo docente",
    ),
    "full_time_faculty_rate": (
        "Proporção do corpo docente contratada em tempo integral, de 0 a 1",
        "Share of faculty employed full time, from 0 to 1",
        "Proporción del cuerpo docente contratada a tiempo completo, de 0 a 1",
    ),
    "endowment_begin": (
        "Valor do fundo patrimonial da instituição no início do exercício",
        "Value of the institution's endowment at the beginning of the fiscal year",
        "Valor del fondo patrimonial de la institución al inicio del ejercicio",
    ),
    "endowment_end": (
        "Valor do fundo patrimonial da instituição no fim do exercício",
        "Value of the institution's endowment at the end of the fiscal year",
        "Valor del fondo patrimonial de la institución al final del ejercicio",
    ),
    "admission_rate": (
        "Taxa de admissão da instituição, de 0 a 1",
        "Admission rate of the institution, from 0 to 1",
        "Tasa de admisión de la institución, de 0 a 1",
    ),
    "admission_rate_all_campuses": (
        "Taxa de admissão agregada por código OPE de 6 dígitos, de 0 a 1",
        "Admission rate aggregated by 6-digit OPE ID, from 0 to 1",
        "Tasa de admisión agregada por código OPE de 6 dígitos, de 0 a 1",
    ),
    "admission_rate_suppressed": (
        "Taxa de admissão com supressão aplicada a coortes pequenas, de 0 a 1",
        "Admission rate with suppression applied to small cohorts, from 0 to 1",
        "Tasa de admisión con supresión aplicada a cohortes pequeñas, de 0 a 1",
    ),
    "test_score_requirement": (
        "Exigência da instituição quanto a notas de exames padronizados na admissão",
        "Institution's requirement for standardised test scores in admissions",
        "Exigencia de la institución sobre puntajes de exámenes estandarizados en la admisión",
    ),
    "sat_average": (
        "Nota média no SAT entre os estudantes admitidos",
        "Average SAT score among admitted students",
        "Puntaje promedio del SAT entre los estudiantes admitidos",
    ),
    "sat_average_all_campuses": (
        "Nota média no SAT agregada por código OPE de 6 dígitos",
        "Average SAT score aggregated by 6-digit OPE ID",
        "Puntaje promedio del SAT agregado por código OPE de 6 dígitos",
    ),
}


def _score(exam, part, stat):
    pt_stat = {
        "p25": "no percentil 25",
        "p50": "na mediana",
        "p75": "no percentil 75",
        "midpoint": "no ponto médio",
    }[stat]
    en_stat = {
        "p25": "at the 25th percentile",
        "p50": "at the median",
        "p75": "at the 75th percentile",
        "midpoint": "at the midpoint",
    }[stat]
    es_stat = {
        "p25": "en el percentil 25",
        "p50": "en la mediana",
        "p75": "en el percentil 75",
        "midpoint": "en el punto medio",
    }[stat]
    pt_part, en_part, es_part = {
        "reading": (
            "de leitura crítica",
            "critical reading",
            "de lectura crítica",
        ),
        "math": ("de matemática", "math", "de matemáticas"),
        "writing": ("de redação", "writing", "de redacción"),
        "composite": ("composta", "cumulative", "compuesta"),
        "english": ("de inglês", "English", "de inglés"),
    }[part]
    return (
        f"Nota {pt_part} no {exam} entre os estudantes admitidos, {pt_stat}",
        f"{exam} {en_part} score among admitted students, {en_stat}",
        f"Puntaje {es_part} del {exam} entre los estudiantes admitidos, {es_stat}",
    )


for _exam, _parts in (
    ("SAT", ("reading", "math", "writing")),
    ("ACT", ("composite", "english", "math", "writing")),
):
    for _part in _parts:
        for _stat in ("p25", "p50", "p75", "midpoint"):
            _key = f"{_exam.lower()}_{_part}_{_stat}"
            INSTITUTION.setdefault(_key, _score(_exam, _part, _stat))


# ------------------------------------------- `field_of_study` descriptions
# The 178 published field-of-study labels are templated, so Portuguese and
# Spanish are composed by ordered phrase substitution over the U.S.
# Department of Education's own English label. Longest phrases first: the
# order of this list is load-bearing.

FOS_GLOSSARY = [
    (
        "Borrower count for average/median",
        "Número de tomadores usado no cálculo da média e da mediana d",
        "Número de prestatarios usado en el cálculo del promedio y la mediana de",
    ),
    (
        "Student recipient count for average/median",
        "Número de estudantes beneficiários usado no cálculo da média e da mediana d",
        "Número de estudiantes beneficiarios usado en el cálculo del promedio y la mediana de",
    ),
    (
        "Median estimated monthly payment for",
        "Estimativa mediana da prestação mensal d",
        "Estimación mediana de la cuota mensual de",
    ),
    (
        "Stafford and Grad PLUS loan debt disbursed",
        "a dívida de empréstimos Stafford e Grad PLUS desembolsada",
        "la deuda de préstamos Stafford y Grad PLUS desembolsada",
    ),
    (
        "Parent PLUS loan debt disbursed",
        "a dívida de empréstimos Parent PLUS desembolsada",
        "la deuda de préstamos Parent PLUS desembolsada",
    ),
    (
        "at all institutions",
        "no conjunto das instituições",
        "en el conjunto de las instituciones",
    ),
    ("at this institution", "nesta instituição", "en esta institución"),
    (
        "to non-Pell-recipients",
        "a estudantes sem bolsa Pell",
        "a estudiantes sin beca Pell",
    ),
    (
        "to Pell recipients",
        "a estudantes com bolsa Pell",
        "a estudiantes con beca Pell",
    ),
    (
        "to non-males",
        "a estudantes não masculinos",
        "a estudiantes no masculinos",
    ),
    (
        "to males",
        "a estudantes do sexo masculino",
        "a estudiantes de sexo masculino",
    ),
    (
        "Number of awards to all students in year 1 of the pooled debt cohort",
        "Número de diplomas concedidos no primeiro ano da coorte agrupada de endividamento",
        "Número de títulos otorgados en el primer año de la cohorte agrupada de endeudamiento",
    ),
    (
        "Number of awards to all students in year 2 of the pooled debt cohort",
        "Número de diplomas concedidos no segundo ano da coorte agrupada de endividamento",
        "Número de títulos otorgados en el segundo año de la cohorte agrupada de endeudamiento",
    ),
    (
        "who earned more than 150% of the single-person household poverty threshold",
        "que ganharam mais de 150% da linha de pobreza de um domicílio unipessoal",
        "que ganaron más del 150% de la línea de pobreza de un hogar unipersonal",
    ),
    (
        "who earned more than a high school graduate",
        "que ganharam mais do que um concluinte do ensino médio",
        "que ganaron más que un egresado de la enseñanza media",
    ),
    (
        "who went on to earn a higher credential",
        "que posteriormente obtiveram uma credencial superior",
        "que posteriormente obtuvieron una credencial superior",
    ),
    (
        "who were employed within the same state as the institution",
        "que estavam empregados no mesmo estado da instituição",
        "que estaban empleados en el mismo estado de la institución",
    ),
    (
        "who received a Pell Grant and were",
        "que receberam bolsa Pell e estavam",
        "que recibieron beca Pell y estaban",
    ),
    (
        "who did not receive a Pell Grant and were",
        "que não receberam bolsa Pell e estavam",
        "que no recibieron beca Pell y estaban",
    ),
    (
        "Federal student loan borrower-based",
        "Contagem de tomadores de empréstimo estudantil federal em",
        "Conteo de prestatarios de préstamo estudiantil federal en",
    ),
    (
        "borrower count of completers",
        ", entre concluintes",
        ", entre egresados",
    ),
    (
        "Percentage of undergraduate completer undergraduate federal student loan borrowers",
        "Proporção, de 0 a 1, dos tomadores de empréstimo estudantil federal concluintes da graduação",
        "Proporción, de 0 a 1, de los prestatarios de préstamo estudiantil federal egresados del pregrado",
    ),
    (
        "with all loans discharged after",
        "com todos os empréstimos extintos após",
        "con todos los préstamos extinguidos después de",
    ),
    (
        "not making progress after",
        "sem progresso na amortização após",
        "sin progreso en la amortización después de",
    ),
    (
        "making progress after",
        "com progresso na amortização após",
        "con progreso en la amortización después de",
    ),
    (
        "paid in full after",
        "com o saldo integralmente pago após",
        "con el saldo íntegramente pagado después de",
    ),
    ("in delinquency after", "em atraso após", "en mora después de"),
    (
        "in forbearance after",
        "em suspensão temporária de pagamento após",
        "en suspensión temporal de pago después de",
    ),
    (
        "in deferment after",
        "em diferimento após",
        "en aplazamiento después de",
    ),
    (
        "in default after",
        "em inadimplência após",
        "en incumplimiento después de",
    ),
    (
        "25th percentile earnings",
        "Rendimento no percentil 25",
        "Ingreso en el percentil 25",
    ),
    (
        "75th percentile earnings",
        "Rendimento no percentil 75",
        "Ingreso en el percentil 75",
    ),
    ("Median earnings", "Rendimento mediano", "Ingreso mediano"),
    (
        "Number of graduates in the nation",
        "Número de concluintes no país",
        "Número de egresados en el país",
    ),
    (
        "Number of male graduates",
        "Número de concluintes do sexo masculino",
        "Número de egresados de sexo masculino",
    ),
    (
        "Number of non-male graduates",
        "Número de concluintes não masculinos",
        "Número de egresados no masculinos",
    ),
    ("Number of graduates", "Número de concluintes", "Número de egresados"),
    (
        "of male graduates",
        "de concluintes do sexo masculino",
        "de egresados de sexo masculino",
    ),
    (
        "of non-male graduates",
        "de concluintes não masculinos",
        "de egresados no masculinos",
    ),
    (
        "of graduates in the nation",
        "de concluintes no país",
        "de egresados en el país",
    ),
    ("of graduates", "de concluintes", "de egresados"),
    (
        "not working and not enrolled",
        "sem trabalhar e sem estar matriculados",
        "sin trabajar y sin estar matriculados",
    ),
    (
        "working and not enrolled",
        "trabalhando e sem estar matriculados",
        "trabajando y sin estar matriculados",
    ),
    ("not enrolled", "sem estar matriculados", "sin estar matriculados"),
    (
        "after completing highest credential",
        "após concluírem a credencial mais alta",
        "después de concluir la credencial más alta",
    ),
    ("after completing", "após a conclusão", "después de la conclusión"),
    ("Average", "Média d", "Promedio de"),
    ("Median", "Mediana d", "Mediana de"),
    ("Unit ID for institution", UNITID[0], UNITID[2]),
    (
        "6-digit OPE ID for institution",
        "Código OPE de 6 dígitos da instituição",
        "Código OPE de 6 dígitos de la institución",
    ),
    ("Institution name", "Nome da instituição", "Nombre de la institución"),
    (
        "Control of institution",
        "Natureza administrativa da instituição",
        "Naturaleza administrativa de la institución",
    ),
    (
        "Flag for main campus",
        "Indica se a unidade é o campus principal da instituição",
        "Indica si la unidad es el campus principal de la institución",
    ),
    (
        "Classification of Instructional Programs (CIP) code for the field of study",
        "Código CIP de 4 dígitos da área de formação",
        "Código CIP de 4 dígitos del campo de estudio",
    ),
    (
        "Text description of the field of study CIP Code",
        "Nome da área de formação correspondente ao código CIP",
        "Nombre del campo de estudio correspondiente al código CIP",
    ),
    (
        "Level of credential",
        "Nível da credencial concedida",
        "Nivel de la credencial otorgada",
    ),
    (
        "Text description of the level of credential",
        "Nome do nível da credencial concedida",
        "Nombre del nivel de la credencial otorgada",
    ),
    (
        "Distance education indiciator for the field of study",
        "Indica em que medida a área de formação pode ser cursada integralmente a distância",
        "Indica en qué medida el campo de estudio puede cursarse íntegramente a distancia",
    ),
    ("1 year", "1 ano", "1 año"),
    ("2 years", "2 anos", "2 años"),
    ("3 years", "3 anos", "3 años"),
    ("4 years", "4 anos", "4 años"),
    ("5 years", "5 anos", "5 años"),
    ("2 year", "2 anos", "2 años"),
    ("3 year", "3 anos", "3 años"),
    ("4 year", "4 anos", "4 años"),
    ("5 year", "5 anos", "5 años"),
    ("1-year", "1 ano", "1 año"),
    ("2-year", "2 anos", "2 años"),
    ("3-year", "3 anos", "3 años"),
    ("4-year", "4 anos", "4 años"),
]


def translate_fos(label):
    """Compose (pt, en, es) for one field-of-study label."""
    pt = es = label.strip().rstrip(".")
    for phrase, pt_r, es_r in FOS_GLOSSARY:
        pt = pt.replace(phrase, pt_r)
        es = es.replace(phrase, es_r)
    for a, b in (("  ", " "), (" ,", ","), (" .", ".")):
        pt = pt.replace(a, b)
        es = es.replace(a, b)
    pt = pt[:1].upper() + pt[1:]
    es = es[:1].upper() + es[1:]
    return pt, label.strip().rstrip("."), es
