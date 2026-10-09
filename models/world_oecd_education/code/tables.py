"""The 15 cubes that make up world_oecd_education, and how each is assembled.

One table per SDMX data structure definition. The 141 education dataflows OECD
publishes are views over these 15 cubes: verified by pulling
``DF_UOE_NF_ENRL_RATE`` and the full-database flow filtered to the same
``UNIT_MEASURE``, which returned identical row counts. So each table is built from
the one flow per DSD carrying the most observations, and the named slices are
documentation rather than data.

``stack`` says which flow versions to union, and it is set per cube from measured
evidence rather than a blanket rule:

* Cubes with no ``TIME_PERIOD`` dimension encode the vintage in the flow version,
  and successive versions are **disjoint** — actual salaries v1.1 carries
  ``REF_PERIOD`` 2020-2022 and v2.1 carries 2023-2025. Those stack.
* Cubes with a ``TIME_PERIOD`` carry their own history, so the latest version wins.
  Student v1.0 and v1.1 both span 2005-2024; v1.1 is a revision, not an extension.
* The ``UOE_FIN`` family is the awkward case: it rolls an ~18-year window forward,
  so v3.2 (2000-2023) genuinely lacks years v3.1 (1995-2022) had. Measured, that
  loss is almost nothing — the entire 1995-1999 slice of ``UOE_FIN`` is 1,546,721
  rows carrying 23,852 values, all in 1995, for a 227 MB download, and ``FIN_ENR``
  has 580. Not worth stacking. ``FIN_ANNEX`` is the exception: 2,609 values across
  all five years in a 2,744-row fetch, so it takes a cheap pre-2000 backfill.
"""

# slug -> cube definition
#   dsd            SDMX data structure definition id
#   dsd_version    the version whose dimension signature the table follows
#   flow           dataflow id (without the "DSD@" prefix) carrying the full cube
#   versions       flow versions to union, newest last
#   backfill       (version, start, end) to add only for the years the newest lacks
TABLES = {
    "student": dict(
        dsd="DSD_EAG_UOE_NON_FIN_STUD",
        dsd_version="1.1",
        flow="DSD_EAG_UOE_NON_FIN_STUD@DF_UOE_NF_STUD_DB",
        versions=["1.1"],
        name_pt="Estudantes",
        name_en="Students",
        name_es="Estudiantes",
        description_pt=(
            "Matrículas, ingressantes, concluintes e repetentes por nível de ensino, área de "
            "formação, idade, sexo e mobilidade internacional, na coleta UOE conduzida em "
            "conjunto pela OCDE, pelo Instituto de Estatística da UNESCO e pelo Eurostat."
        ),
        description_en=(
            "Students enrolled, new entrants, graduates and repeaters by level of education, "
            "field of education, age, sex and international mobility, from the UOE data "
            "collection run jointly by the OECD, the UNESCO Institute for Statistics and Eurostat."
        ),
        description_es=(
            "Matrículas, nuevos ingresos, graduados y repetidores por nivel de enseñanza, ámbito "
            "de formación, edad, sexo y movilidad internacional, en la recolección UOE realizada "
            "conjuntamente por la OCDE, el Instituto de Estadística de la UNESCO y Eurostat."
        ),
    ),
    "teacher": dict(
        dsd="DSD_EAG_UOE_NON_FIN_PERS",
        dsd_version="1.1",
        flow="DSD_EAG_UOE_NON_FIN_PERS@DF_UOE_NF_TEACH_DB",
        versions=["1.1"],
        name_pt="Pessoal docente",
        name_en="Teaching personnel",
        name_es="Personal docente",
        description_pt=(
            "Docentes, pessoal de direção e auxiliares por nível de ensino, tipo de instituição, "
            "idade e sexo, com razões aluno-docente e tamanho médio de turma, na coleta UOE."
        ),
        description_en=(
            "Classroom teachers, school heads and teacher aides by level of education, type of "
            "institution, age and sex, with student-teacher ratios and average class size, from "
            "the UOE data collection."
        ),
        description_es=(
            "Docentes, personal de dirección y auxiliares por nivel de enseñanza, tipo de "
            "institución, edad y sexo, con razones alumno-docente y tamaño medio de clase, en la "
            "recolección UOE."
        ),
    ),
    "finance": dict(
        dsd="DSD_EAG_UOE_FIN",
        dsd_version="3.2",
        flow="DSD_EAG_UOE_FIN@DF_UOE_FIN_INDIC_SOURCE_NATURE",
        versions=["3.2"],
        name_pt="Financiamento da educação",
        name_en="Education finance",
        name_es="Financiamiento de la educación",
        description_pt=(
            "Despesa com instituições de ensino por origem dos recursos, destino e natureza do "
            "gasto, em valores absolutos, por estudante e como proporção do PIB. A fonte publica "
            "uma janela móvel de cerca de 18 anos, descartando os anos mais antigos a cada edição."
        ),
        description_en=(
            "Expenditure on educational institutions by financing source, destination and nature "
            "of the spending, in absolute terms, per student and as a share of GDP. The source "
            "publishes a rolling window of about 18 years, dropping the oldest years each edition."
        ),
        description_es=(
            "Gasto en instituciones educativas por origen de los recursos, destino y naturaleza "
            "del gasto, en valores absolutos, por estudiante y como proporción del PIB. La fuente "
            "publica una ventana móvil de unos 18 años, descartando los años más antiguos en cada "
            "edición."
        ),
    ),
    "finance_enrolment": dict(
        dsd="DSD_EAG_UOE_FIN_ENR",
        dsd_version="3.2",
        flow="DSD_EAG_UOE_FIN_ENR@DF_UOE_FIN_ENR",
        versions=["3.2"],
        name_pt="Matrículas ajustadas ao ano financeiro",
        name_en="Enrolment adjusted to the financial year",
        name_es="Matrículas ajustadas al año financiero",
        description_pt=(
            "Matrículas em equivalente de tempo integral ajustadas ao ano financeiro, que servem "
            "de denominador aos indicadores de despesa por estudante."
        ),
        description_en=(
            "Full-time equivalent enrolment adjusted to the financial year, used as the "
            "denominator of the expenditure-per-student indicators."
        ),
        description_es=(
            "Matrículas en equivalente de tiempo completo ajustadas al año financiero, usadas "
            "como denominador de los indicadores de gasto por estudiante."
        ),
    ),
    "finance_reference": dict(
        dsd="DSD_EAG_UOE_FIN_ANNEX",
        dsd_version="3.2",
        flow="DSD_EAG_UOE_FIN_ANNEX@DF_UOE_FIN_ANNEX",
        versions=["3.2"],
        backfill=("3.1", 1995, 1999),
        name_pt="Estatísticas de referência do financiamento",
        name_en="Education finance reference statistics",
        name_es="Estadísticas de referencia del financiamiento",
        description_pt=(
            "PIB, deflatores, paridades de poder de compra e população que servem de referência "
            "aos indicadores de financiamento da educação."
        ),
        description_en=(
            "GDP, deflators, purchasing power parities and population underpinning the education "
            "finance indicators."
        ),
        description_es=(
            "PIB, deflactores, paridades de poder adquisitivo y población que sirven de "
            "referencia a los indicadores de financiamiento de la educación."
        ),
    ),
    "finance_subnational": dict(
        dsd="DSD_EAG_SUBNAT_UOE_FIN",
        dsd_version="1.0",
        flow="DSD_EAG_SUBNAT_UOE_FIN@DF_SUBNAT_UOE_FIN",
        versions=["1.0"],
        name_pt="Financiamento da educação por entidade subnacional",
        name_en="Education finance by subnational entity",
        name_es="Financiamiento de la educación por entidad subnacional",
        description_pt=(
            "Despesa total com instituições de ensino por estudante em equivalente de tempo "
            "integral, desagregada por entidade subnacional dos países que reportam esse recorte."
        ),
        description_en=(
            "Total expenditure on educational institutions per full-time equivalent student, "
            "broken down by subnational entity for the countries that report at that level."
        ),
        description_es=(
            "Gasto total en instituciones educativas por estudiante en equivalente de tiempo "
            "completo, desagregado por entidad subnacional en los países que reportan ese nivel."
        ),
    ),
    "labour_market_outcome": dict(
        dsd="DSD_EAG_LSO_EA",
        dsd_version="1.0",
        flow="DSD_EAG_LSO_EA@DF_LSO_NEAC_ALL",
        versions=["1.0"],
        name_pt="Escolaridade e resultados no mercado de trabalho",
        name_en="Educational attainment and labour market outcomes",
        name_es="Escolaridad y resultados en el mercado laboral",
        description_pt=(
            "Distribuição da população adulta por nível de escolaridade e seus resultados no "
            "mercado de trabalho: taxas de emprego, desemprego e inatividade, rendimentos "
            "relativos e transição da escola para o trabalho, por idade, sexo, área de formação e "
            "situação migratória."
        ),
        description_en=(
            "Distribution of the adult population by educational attainment and the labour market "
            "outcomes that follow: employment, unemployment and inactivity rates, relative "
            "earnings and the transition from school to work, by age, sex, field of study and "
            "migration status."
        ),
        description_es=(
            "Distribución de la población adulta por nivel de escolaridad y sus resultados en el "
            "mercado laboral: tasas de empleo, desempleo e inactividad, ingresos relativos y "
            "transición de la escuela al trabajo, por edad, sexo, ámbito de formación y situación "
            "migratoria."
        ),
    ),
    "salary_actual": dict(
        dsd="DSD_EAG_SAL_ACT",
        dsd_version="2.0",
        flow="DSD_EAG_SAL_ACT@DF_ALL",
        versions=["2.1"],
        stack=[("DSD_EAG_SAL_ACT@DF_EAG_SAL_ACT_ALL", "1.1")],
        name_pt="Salários efetivos de docentes e diretores",
        name_en="Actual salaries of teachers and school heads",
        name_es="Salarios efectivos de docentes y directores",
        description_pt=(
            "Salários efetivamente recebidos por docentes e diretores escolares, em valores "
            "absolutos e em relação aos rendimentos de trabalhadores com ensino superior. A fonte "
            "não usa dimensão temporal: cada edição publica seus próprios anos de referência, e a "
            "tabela empilha as edições."
        ),
        description_en=(
            "Salaries actually received by teachers and school heads, in absolute terms and "
            "relative to the earnings of tertiary-educated workers. The source carries no time "
            "dimension: each edition publishes its own reference years, and this table stacks the "
            "editions."
        ),
        description_es=(
            "Salarios efectivamente percibidos por docentes y directores escolares, en valores "
            "absolutos y en relación con los ingresos de trabajadores con educación superior. La "
            "fuente no usa dimensión temporal: cada edición publica sus propios años de "
            "referencia, y la tabla apila las ediciones."
        ),
    ),
    "salary_statutory": dict(
        dsd="DSD_EAG_SAL_STA",
        dsd_version="2.0",
        flow="DSD_EAG_SAL_STA@DF_ALL",
        versions=["2.1"],
        stack=[("DSD_EAG_SAL_STA@DF_EAG_SAL_STA_ALL", "1.1")],
        name_pt="Salários estatutários de docentes e diretores",
        name_en="Statutory salaries of teachers and school heads",
        name_es="Salarios estatutarios de docentes y directores",
        description_pt=(
            "Salários previstos em lei ou acordo coletivo para docentes e diretores escolares, "
            "por nível de qualificação e tempo de experiência. Como nos salários efetivos, a "
            "fonte não usa dimensão temporal e a tabela empilha as edições."
        ),
        description_en=(
            "Salaries set by law or collective agreement for teachers and school heads, by "
            "qualification level and years of experience. As with actual salaries, the source "
            "carries no time dimension and this table stacks the editions."
        ),
        description_es=(
            "Salarios establecidos por ley o convenio colectivo para docentes y directores "
            "escolares, por nivel de cualificación y años de experiencia. Como en los salarios "
            "efectivos, la fuente no usa dimensión temporal y la tabla apila las ediciones."
        ),
    ),
    "salary_trend": dict(
        dsd="DSD_EAG_SAL_TREND",
        dsd_version="2.0",
        flow="DSD_EAG_SAL_TREND@DF_ALL",
        versions=["2.1"],
        name_pt="Evolução dos salários docentes desde 2000",
        name_en="Trends in teachers' salaries since 2000",
        name_es="Evolución de los salarios docentes desde 2000",
        description_pt=(
            "Série anual dos salários efetivos e estatutários de docentes desde 2000, a preços "
            "constantes, permitindo acompanhar a evolução real da remuneração."
        ),
        description_en=(
            "Annual series of teachers' actual and statutory salaries since 2000, at constant "
            "prices, tracking how pay has moved in real terms."
        ),
        description_es=(
            "Serie anual de los salarios efectivos y estatutarios de docentes desde 2000, a "
            "precios constantes, que permite seguir la evolución real de la remuneración."
        ),
    ),
    "working_time": dict(
        dsd="DSD_EAG_WT",
        dsd_version="2.0",
        flow="DSD_EAG_WT@DF_ALL",
        versions=["2.0"],
        stack=[("DSD_EAG_WT@DF_EAG_WT_ALL", "1.1")],
        name_pt="Tempo de ensino e de trabalho docente",
        name_en="Teaching and working time of teachers",
        name_es="Tiempo de enseñanza y de trabajo docente",
        description_pt=(
            "Horas anuais de ensino efetivo e estatutário de docentes e horas de trabalho de "
            "diretores, por nível de ensino. A fonte não usa dimensão temporal e a tabela empilha "
            "as edições."
        ),
        description_en=(
            "Annual hours of actual and statutory teaching by teachers and working hours of "
            "school heads, by level of education. The source carries no time dimension and this "
            "table stacks the editions."
        ),
        description_es=(
            "Horas anuales de enseñanza efectiva y estatutaria de docentes y horas de trabajo de "
            "directores, por nivel de enseñanza. La fuente no usa dimensión temporal y la tabla "
            "apila las ediciones."
        ),
    ),
    "working_time_trend": dict(
        dsd="DSD_EAG_WT_TREND",
        dsd_version="2.0",
        flow="DSD_EAG_WT_TREND@DF_ALL",
        versions=["2.0"],
        name_pt="Evolução do tempo de ensino desde 2000",
        name_en="Trends in teaching time since 2000",
        name_es="Evolución del tiempo de enseñanza desde 2000",
        description_pt=(
            "Série anual das horas estatutárias de ensino desde 2000, por nível de ensino."
        ),
        description_en=(
            "Annual series of statutory teaching hours since 2000, by level of education."
        ),
        description_es=(
            "Serie anual de las horas estatutarias de enseñanza desde 2000, por nivel de enseñanza."
        ),
    ),
    "instruction_time": dict(
        dsd="DSD_EAG_IT",
        dsd_version="1.1",
        flow="DSD_EAG_IT@DF_EAG_IT_ALL",
        versions=["1.1"],
        name_pt="Tempo de instrução dos estudantes",
        name_en="Student instruction time",
        name_es="Tiempo de instrucción de los estudiantes",
        description_pt=(
            "Horas de instrução obrigatória previstas no currículo do ensino geral, por idade, "
            "nível de ensino e disciplina."
        ),
        description_en=(
            "Compulsory instruction hours set by the curriculum in general education, by age, "
            "level of education and subject."
        ),
        description_es=(
            "Horas de instrucción obligatoria previstas en el currículo de la enseñanza general, "
            "por edad, nivel de enseñanza y asignatura."
        ),
    ),
    "talis_teacher": dict(
        dsd="DSD_TALIS",
        dsd_version="1.0",
        flow="DSD_TALIS@DF_TALIS",
        versions=["1.0"],
        name_pt="TALIS: respostas de docentes",
        name_en="TALIS: teacher responses",
        name_es="TALIS: respuestas de docentes",
        description_pt=(
            "Resultados agregados do questionário de docentes da Pesquisa Internacional sobre "
            "Ensino e Aprendizagem (TALIS), por idade, sexo, tipo de instituição, grau de "
            "urbanização e perfil docente."
        ),
        description_en=(
            "Aggregated results of the teacher questionnaire of the Teaching and Learning "
            "International Survey (TALIS), by age, sex, institution type, degree of urbanisation "
            "and teacher profile."
        ),
        description_es=(
            "Resultados agregados del cuestionario de docentes de la Encuesta Internacional sobre "
            "Enseñanza y Aprendizaje (TALIS), por edad, sexo, tipo de institución, grado de "
            "urbanización y perfil docente."
        ),
    ),
    "talis_principal": dict(
        dsd="DSD_TALIS_PQ",
        dsd_version="1.0",
        flow="DSD_TALIS_PQ@DF_TALIS_PQ",
        versions=["1.0"],
        name_pt="TALIS: respostas de diretores",
        name_en="TALIS: principal responses",
        name_es="TALIS: respuestas de directores",
        description_pt=(
            "Resultados agregados do questionário de diretores da Pesquisa Internacional sobre "
            "Ensino e Aprendizagem (TALIS), com os mesmos recortes do questionário de docentes."
        ),
        description_en=(
            "Aggregated results of the principal questionnaire of the Teaching and Learning "
            "International Survey (TALIS), with the same breakdowns as the teacher questionnaire."
        ),
        description_es=(
            "Resultados agregados del cuestionario de directores de la Encuesta Internacional "
            "sobre Enseñanza y Aprendizaje (TALIS), con los mismos desgloses que el cuestionario "
            "de docentes."
        ),
    ),
}
