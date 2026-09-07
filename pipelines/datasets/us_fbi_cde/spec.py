"""Table specification for us_fbi_cde — the single source of truth.

The cleaning transform, the architecture CSVs, the dbt models and the backend
metadata all read this module, so column order, types, keys and descriptions
cannot drift between them.

Column tuples are ``(name, bigquery_type, description_pt, description_en,
description_es, measurement_unit, directory_column, covered_by_dictionary,
observations, original_name)``. Use :func:`column` rather than writing the
tuples by hand.

Design notes that the reader needs in order to follow the shape:

* The FBI bundles are fifth-normal-form and key everything on opaque surrogate
  integers (``location_id``, ``race_id``, ``weapon_id``, …) that are meaningless
  outside one download. Cleaning resolves every one of them to the FBI's own
  published *code* (``location_code`` ``20``, ``race_code`` ``W``) using the
  lookup tables shipped inside the same bundle, and the code→label mapping lands
  in ``dicionario``. Codes are stable and documented; the surrogates are not.
* Four "up to N per parent" link tables (weapon, bias motivation, victim injury,
  suspected drug) are folded onto their parent as the lowest-sorting code plus a
  count of how many were recorded, so a user can see when folding lost detail.
  ``victim_offense`` and ``victim_offender_relationship`` are *not* folded: the
  FBI's own README warns that an incident's offences must not be attributed to
  all of its victims, so the mapping is load-bearing for correctness.
* NIBRS coverage is partial and changes every year. ``agency`` carries
  ``summary_months_reported``, ``nibrs_months_reported`` and ``population`` so a
  user can build their own coverage weights instead of trusting a raw national
  sum.
"""

from __future__ import annotations

ARCHITECTURE_HEADER = [
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

DIR_YEAR = "diretorios_data_tempo.ano:ano"
DIR_STATE = "diretorios_us.state:id_state"
DIR_COUNTY = "diretorios_us.county:id_county"


def column(
    name,
    bigquery_type,
    pt,
    en,
    es,
    unit="",
    directory="",
    dictionary="no",
    observations="",
    original="",
):
    """Build one column specification.

    ``pt``/``en``/``es`` are the three descriptions, capitalised and without a
    trailing full stop (house style). ``unit`` is mandatory for every INT64 or
    FLOAT64 that is a real quantity.
    """
    return {
        "name": name,
        "bigquery_type": bigquery_type,
        "description_pt": pt,
        "description_en": en,
        "description_es": es,
        "measurement_unit": unit,
        "directory_column": directory,
        "covered_by_dictionary": dictionary,
        "observations": observations,
        "original_name": original,
    }


# --------------------------------------------------------------------------
# Column groups repeated across tables. Defined once so the wording, the type
# and the dictionary flag stay identical everywhere they appear.
# --------------------------------------------------------------------------


def _year(original="data_year"):
    return column(
        "year",
        "INT64",
        "Ano de referência dos dados, usado como coluna de partição",
        "Reference year of the data, used as the partition column",
        "Año de referencia de los datos, utilizado como columna de partición",
        unit="year",
        directory=DIR_YEAR,
        original=original,
    )


def _state_abbr(original="state_abbr"):
    return column(
        "state_abbr",
        "STRING",
        "Sigla de duas letras do estado, distrito ou território da agência",
        "Two-letter abbreviation of the agency's state, district or territory",
        "Sigla de dos letras del estado, distrito o territorio de la agencia",
        original=original,
    )


def _ori(original="ori"):
    return column(
        "ori",
        "STRING",
        "Originating Agency Identifier, o identificador da agência policial no "
        "programa UCR e a chave de junção entre as tabelas",
        "Originating Agency Identifier, the law enforcement agency's identifier "
        "in the UCR program and the join key across tables",
        "Originating Agency Identifier, el identificador de la agencia policial "
        "en el programa UCR y la clave de unión entre las tablas",
        observations=(
            "Nove caracteres: o ORI de sete caracteres do NCIC seguido de um "
            "sufixo de duas posições que identifica a subunidade, '00' para a "
            "própria agência. Nine characters: the seven-character NCIC ORI "
            "followed by a two-digit sub-unit suffix, '00' for the agency itself."
        ),
        original=original,
    )


def _incident_id(nullable_note=""):
    return column(
        "incident_id",
        "STRING",
        "Identificador do incidente atribuído pelo FBI, único dentro do estado e do ano",
        "Incident identifier assigned by the FBI, unique within the state and year",
        "Identificador del incidente asignado por el FBI, único dentro del estado y del año",
        observations=nullable_note,
        original="incident_id",
    )


def _age_block(prefix_pt, prefix_en, prefix_es):
    return [
        column(
            "age_code",
            "STRING",
            f"Código da faixa etária {prefix_pt}, incluindo os códigos de "
            "desconhecido e de menor de um ano",
            f"Age code {prefix_en}, including the unknown and under-one-year codes",
            f"Código del rango de edad {prefix_es}, incluidos los códigos de "
            "desconocido y de menor de un año",
            dictionary="yes",
            original="age_id",
        ),
        column(
            "age",
            "INT64",
            f"Idade exata {prefix_pt} em anos, quando informada",
            f"Exact age {prefix_en} in years, when reported",
            f"Edad exacta {prefix_es} en años, cuando se informa",
            unit="year",
            original="age_num",
        ),
        column(
            "age_range_low",
            "INT64",
            f"Limite inferior da faixa etária {prefix_pt}, quando a idade exata "
            "não é informada",
            f"Lower bound of the age range {prefix_en}, when the exact age is not "
            "reported",
            f"Límite inferior del rango de edad {prefix_es}, cuando no se informa "
            "la edad exacta",
            unit="year",
            original="age_range_low_num",
        ),
        column(
            "age_range_high",
            "INT64",
            f"Limite superior da faixa etária {prefix_pt}, quando a idade exata "
            "não é informada",
            f"Upper bound of the age range {prefix_en}, when the exact age is not "
            "reported",
            f"Límite superior del rango de edad {prefix_es}, cuando no se informa "
            "la edad exacta",
            unit="year",
            original="age_range_high_num",
        ),
    ]


def _demographics(prefix_pt, prefix_en, prefix_es):
    return [
        column(
            "sex_code",
            "STRING",
            f"Código do sexo {prefix_pt}",
            f"Sex code {prefix_en}",
            f"Código del sexo {prefix_es}",
            dictionary="yes",
            original="sex_code",
        ),
        column(
            "race_code",
            "STRING",
            f"Código da raça {prefix_pt}, tal como registrado pela agência",
            f"Race code {prefix_en}, as recorded by the agency",
            f"Código de la raza {prefix_es}, tal como lo registró la agencia",
            dictionary="yes",
            original="race_id",
        ),
        column(
            "ethnicity_code",
            "STRING",
            f"Código da etnia hispânica ou latina {prefix_pt}",
            f"Hispanic or Latino ethnicity code {prefix_en}",
            f"Código de etnia hispana o latina {prefix_es}",
            dictionary="yes",
            observations=(
                "Coletado apenas por parte das agências; a cobertura varia muito "
                "entre estados e anos. Collected by only some agencies; coverage "
                "varies widely across states and years."
            ),
            original="ethnicity_id",
        ),
    ]


# --------------------------------------------------------------------------
# Tables
# --------------------------------------------------------------------------

AGENCY = [
    _year(),
    _ori(),
    column(
        "state_id",
        "STRING",
        "Código FIPS de duas posições do estado, distrito ou território",
        "Two-digit FIPS code of the state, district or territory",
        "Código FIPS de dos posiciones del estado, distrito o territorio",
        directory=DIR_STATE,
        observations=(
            "Derivado na limpeza a partir da sigla do estado. "
            "Derived during cleaning from the state abbreviation."
        ),
        original="state_abbr",
    ),
    _state_abbr(),
    column(
        "county_id",
        "STRING",
        "Código FIPS de cinco posições do condado principal da agência",
        "Five-digit FIPS code of the agency's primary county",
        "Código FIPS de cinco posiciones del condado principal de la agencia",
        directory=DIR_COUNTY,
        observations=(
            "A fonte publica apenas o nome do condado; o código é obtido na "
            "limpeza pelo cruzamento com a lista ANSI de condados do Census e é "
            "nulo quando o nome não tem correspondência única. The source "
            "publishes county names only; the code is matched during cleaning "
            "against the Census ANSI county list and is null when the name has "
            "no unique match."
        ),
        original="county_name",
    ),
    column(
        "county_name",
        "STRING",
        "Nome do condado principal atendido pela agência, tal como publicado",
        "Name of the primary county served by the agency, as published",
        "Nombre del condado principal atendido por la agencia, tal como se publica",
        observations=(
            "Agências que atendem mais de um condado aparecem com os nomes "
            "separados por vírgula. Agencies serving more than one county appear "
            "with the names separated by commas."
        ),
        original="county_name",
    ),
    column(
        "agency_name",
        "STRING",
        "Nome público da agência policial",
        "Public name of the law enforcement agency",
        "Nombre público de la agencia policial",
        original="pub_agency_name",
    ),
    column(
        "agency_unit",
        "STRING",
        "Unidade ou subdivisão da agência, quando a agência reporta por unidade",
        "Unit or subdivision of the agency, when the agency reports by unit",
        "Unidad o subdivisión de la agencia, cuando la agencia informa por unidad",
        original="pub_agency_unit",
    ),
    column(
        "agency_type",
        "STRING",
        "Tipo de agência, por exemplo cidade, condado, universidade ou polícia estadual",
        "Type of agency, for example city, county, university or state police",
        "Tipo de agencia, por ejemplo ciudad, condado, universidad o policía estatal",
        original="agency_type_name",
    ),
    column(
        "division_name",
        "STRING",
        "Divisão censitária dos Estados Unidos em que a agência se localiza",
        "United States census division in which the agency is located",
        "División censal de los Estados Unidos en la que se ubica la agencia",
        original="division_name",
    ),
    column(
        "region_name",
        "STRING",
        "Região censitária dos Estados Unidos em que a agência se localiza",
        "United States census region in which the agency is located",
        "Región censal de los Estados Unidos en la que se ubica la agencia",
        original="region_name",
    ),
    column(
        "population_group_code",
        "STRING",
        "Código do grupo populacional do UCR, que classifica a agência por porte "
        "e tipo de jurisdição",
        "UCR population group code, classifying the agency by size and type of "
        "jurisdiction",
        "Código del grupo poblacional del UCR, que clasifica a la agencia por "
        "tamaño y tipo de jurisdicción",
        dictionary="yes",
        original="population_group_code",
    ),
    column(
        "population_group_description",
        "STRING",
        "Descrição do grupo populacional do UCR",
        "Description of the UCR population group",
        "Descripción del grupo poblacional del UCR",
        original="population_group_desc",
    ),
    column(
        "core_city_flag",
        "STRING",
        "Indica se a agência atende a cidade núcleo de uma área metropolitana",
        "Indicates whether the agency serves the core city of a metropolitan area",
        "Indica si la agencia atiende a la ciudad núcleo de un área metropolitana",
        dictionary="yes",
        original="core_city_indication",
    ),
    column(
        "population",
        "INT64",
        "População residente coberta pela jurisdição da agência no ano",
        "Resident population covered by the agency's jurisdiction in the year",
        "Población residente cubierta por la jurisdicción de la agencia en el año",
        unit="inhabitant",
        original="population",
    ),
    column(
        "officer_count",
        "INT64",
        "Número de policiais juramentados empregados pela agência",
        "Number of sworn officers employed by the agency",
        "Número de policías juramentados empleados por la agencia",
        unit="person",
        original="officer_ct",
    ),
    column(
        "civilian_count",
        "INT64",
        "Número de funcionários civis empregados pela agência",
        "Number of civilian employees employed by the agency",
        "Número de empleados civiles empleados por la agencia",
        unit="person",
        original="civilian_ct",
    ),
    column(
        "employee_count",
        "INT64",
        "Número total de empregados da agência, somando policiais e civis",
        "Total number of agency employees, sworn officers plus civilians",
        "Número total de empleados de la agencia, policías más civiles",
        unit="person",
        original="total_pe_ct",
    ),
    column(
        "male_officer_count",
        "INT64",
        "Número de policiais juramentados do sexo masculino",
        "Number of male sworn officers",
        "Número de policías juramentados de sexo masculino",
        unit="person",
        original="male_officer_ct",
    ),
    column(
        "male_civilian_count",
        "INT64",
        "Número de funcionários civis do sexo masculino",
        "Number of male civilian employees",
        "Número de empleados civiles de sexo masculino",
        unit="person",
        original="male_cilvilian_ct",
    ),
    column(
        "female_officer_count",
        "INT64",
        "Número de policiais juramentados do sexo feminino",
        "Number of female sworn officers",
        "Número de policías juramentadas de sexo femenino",
        unit="person",
        original="female_officer_ct",
    ),
    column(
        "female_civilian_count",
        "INT64",
        "Número de funcionárias civis do sexo feminino",
        "Number of female civilian employees",
        "Número de empleadas civiles de sexo femenino",
        unit="person",
        original="female_cilvilian_ct",
    ),
    column(
        "employee_per_1000_inhabitants",
        "FLOAT64",
        "Empregados da agência por mil habitantes da jurisdição",
        "Agency employees per thousand inhabitants of the jurisdiction",
        "Empleados de la agencia por mil habitantes de la jurisdicción",
        unit="employee_per_1000_inhabitants",
        original="pe_ct_per_1000",
    ),
    column(
        "summary_months_reported",
        "INT64",
        "Último mês do ano para o qual a agência enviou o formulário resumido "
        "Return A, de 0 a 12",
        "Last month of the year for which the agency filed the Return A summary "
        "form, from 0 to 12",
        "Último mes del año para el que la agencia presentó el formulario "
        "resumido Return A, de 0 a 12",
        unit="month",
        observations=(
            "É o mês mais alto recebido, não a contagem de meses completos: uma "
            "agência que só enviou outubro também registra 10. Use com "
            "'population' para construir pesos de cobertura. This is the highest "
            "month received, not a count of complete months: an agency that filed "
            "only October also records 10. Use together with 'population' to "
            "build coverage weights."
        ),
        original="number_of_months_reported",
    ),
    column(
        "nibrs_months_reported",
        "INT64",
        "Número de meses do ano em que a agência enviou um relatório NIBRS de "
        "incidente ou um relatório zero, de 0 a 12",
        "Number of months in the year for which the agency filed a NIBRS incident "
        "report or a zero report, from 0 to 12",
        "Número de meses del año en que la agencia presentó un informe NIBRS de "
        "incidente o un informe cero, de 0 a 12",
        unit="month",
        observations=(
            "Calculado na limpeza a partir da tabela mensal do NIBRS. Derived "
            "during cleaning from the NIBRS monthly table."
        ),
        original="nibrs_month",
    ),
    column(
        "nibrs_participated",
        "STRING",
        "Indica se a agência participou do NIBRS no ano",
        "Indicates whether the agency participated in NIBRS in the year",
        "Indica si la agencia participó en el NIBRS en el año",
        dictionary="yes",
        original="nibrs_participated",
    ),
    column(
        "nibrs_start_date",
        "DATE",
        "Data em que a agência começou a enviar dados no formato NIBRS",
        "Date on which the agency began submitting data in NIBRS format",
        "Fecha en que la agencia comenzó a enviar datos en formato NIBRS",
        original="nibrs_start_date",
    ),
    column(
        "covered_by_ori",
        "STRING",
        "ORI da agência que envia os dados de criminalidade em nome desta agência",
        "ORI of the agency that submits crime data on behalf of this agency",
        "ORI de la agencia que envía los datos de criminalidad en nombre de esta agencia",
        observations=(
            "Quando preenchido, os crimes desta agência estão contados na agência "
            "indicada e somá-las duplica os registros. When populated, this "
            "agency's crimes are counted under the named agency and summing both "
            "double counts."
        ),
        original="covered_by",
    ),
    column(
        "officer_killed_felonious_count",
        "INT64",
        "Número de policiais mortos por atos criminosos no ano",
        "Number of officers killed by felonious acts in the year",
        "Número de policías muertos por actos delictivos en el año",
        unit="person",
        original="number_of_officers_killed_by_felonious_acts",
    ),
    column(
        "officer_killed_accidental_count",
        "INT64",
        "Número de policiais mortos por atos acidentais ou negligentes no ano",
        "Number of officers killed by accidental or negligent acts in the year",
        "Número de policías muertos por actos accidentales o negligentes en el año",
        unit="person",
        original="number_of_officers_killed_accidental",
    ),
    column(
        "officer_assaulted_count",
        "INT64",
        "Número de agressões contra policiais registradas no ano",
        "Number of assaults on officers recorded in the year",
        "Número de agresiones contra policías registradas en el año",
        unit="assault",
        original="number_of_officers_assaulted",
    ),
]

INCIDENT = [
    _year(),
    _state_abbr(),
    _ori(),
    _incident_id(),
    column(
        "incident_date",
        "DATE",
        "Data em que o incidente ocorreu",
        "Date on which the incident occurred",
        "Fecha en que ocurrió el incidente",
        original="incident_date",
    ),
    column(
        "incident_hour",
        "STRING",
        "Hora do dia em que o incidente ocorreu, de 00 a 23",
        "Hour of the day on which the incident occurred, from 00 to 23",
        "Hora del día en que ocurrió el incidente, de 00 a 23",
        observations=(
            "Nulo quando a agência não informou a hora. Null when the agency did "
            "not report the hour."
        ),
        original="incident_hour",
    ),
    column(
        "report_date_flag",
        "STRING",
        "Indica que a data do incidente é, na verdade, a data do registro da "
        "ocorrência",
        "Indicates that the incident date is in fact the report date",
        "Indica que la fecha del incidente es en realidad la fecha del informe",
        dictionary="yes",
        original="report_date_flag",
    ),
    column(
        "cargo_theft_flag",
        "STRING",
        "Indica que o incidente envolveu roubo de carga",
        "Indicates that the incident involved cargo theft",
        "Indica que el incidente involucró robo de carga",
        dictionary="yes",
        observations=(
            "Coletado a partir de 2013. Collected from 2013 onward."
        ),
        original="cargo_theft_flag",
    ),
    column(
        "cleared_except_code",
        "STRING",
        "Código do motivo pelo qual o incidente foi esclarecido por meio "
        "excepcional, e não por prisão",
        "Code for the reason the incident was cleared exceptionally rather than "
        "by arrest",
        "Código del motivo por el cual el incidente se esclareció por medio "
        "excepcional y no por arresto",
        dictionary="yes",
        original="cleared_except_id",
    ),
    column(
        "cleared_except_date",
        "DATE",
        "Data do esclarecimento excepcional do incidente",
        "Date of the exceptional clearance of the incident",
        "Fecha del esclarecimiento excepcional del incidente",
        original="cleared_except_date",
    ),
    column(
        "incident_status",
        "STRING",
        "Situação do incidente no processamento do FBI",
        "Status of the incident in the FBI's processing",
        "Situación del incidente en el procesamiento del FBI",
        dictionary="yes",
        original="incident_status",
    ),
    column(
        "submission_date",
        "DATE",
        "Data em que a agência enviou o registro ao FBI",
        "Date on which the agency submitted the record to the FBI",
        "Fecha en que la agencia envió el registro al FBI",
        original="submission_date",
    ),
]

OFFENSE = [
    _year(),
    _state_abbr(),
    column(
        "offense_id",
        "STRING",
        "Identificador da ofensa atribuído pelo FBI, único dentro do estado e do ano",
        "Offense identifier assigned by the FBI, unique within the state and year",
        "Identificador del delito asignado por el FBI, único dentro del año",
        original="offense_id",
    ),
    _incident_id(),
    column(
        "offense_code",
        "STRING",
        "Código NIBRS da ofensa, por exemplo 13B para agressão simples",
        "NIBRS offense code, for example 13B for simple assault",
        "Código NIBRS del delito, por ejemplo 13B para agresión simple",
        dictionary="yes",
        original="offense_code",
    ),
    column(
        "attempt_complete_flag",
        "STRING",
        "Indica se a ofensa foi consumada ou apenas tentada",
        "Indicates whether the offense was completed or only attempted",
        "Indica si el delito fue consumado o solo intentado",
        dictionary="yes",
        original="attempt_complete_flag",
    ),
    column(
        "location_code",
        "STRING",
        "Código do tipo de local em que a ofensa ocorreu",
        "Code for the type of location where the offense occurred",
        "Código del tipo de lugar donde ocurrió el delito",
        dictionary="yes",
        observations=(
            "O tipo reflete o uso do local no momento da ofensa. The type "
            "reflects how the location was being used at the time of the offense."
        ),
        original="location_id",
    ),
    column(
        "premises_entered_count",
        "INT64",
        "Número de unidades invadidas, informado apenas para arrombamento em "
        "edificações com várias unidades",
        "Number of premises entered, reported only for burglary of buildings with "
        "multiple units",
        "Número de unidades allanadas, informado solo para robo con allanamiento "
        "en edificios de varias unidades",
        unit="premise",
        original="num_premises_entered",
    ),
    column(
        "method_entry_code",
        "STRING",
        "Indica se houve arrombamento com ou sem uso de força, informado apenas "
        "para ofensas de invasão",
        "Indicates whether entry was forced or unforced, reported only for "
        "burglary and breaking-and-entering offenses",
        "Indica si hubo entrada forzada o no forzada, informado solo para delitos "
        "de allanamiento",
        dictionary="yes",
        original="method_entry_code",
    ),
    column(
        "weapon_code",
        "STRING",
        "Código da arma ou força usada na ofensa",
        "Code for the weapon or force used in the offense",
        "Código del arma o fuerza utilizada en el delito",
        dictionary="yes",
        observations=(
            "O NIBRS admite até três armas por ofensa; guarda-se aqui o menor "
            "código e 'weapon_count' informa quantas foram registradas. NIBRS "
            "allows up to three weapons per offense; the lowest code is kept here "
            "and 'weapon_count' reports how many were recorded."
        ),
        original="weapon_id",
    ),
    column(
        "weapon_count",
        "INT64",
        "Número de armas registradas para a ofensa",
        "Number of weapons recorded for the offense",
        "Número de armas registradas para el delito",
        unit="weapon",
        original="nibrs_weapon",
    ),
    column(
        "bias_motivation_code",
        "STRING",
        "Código da motivação por preconceito atribuída à ofensa",
        "Code for the bias motivation attributed to the offense",
        "Código de la motivación por prejuicio atribuida al delito",
        dictionary="yes",
        observations=(
            "O código 88 significa nenhuma motivação por preconceito e é o valor "
            "da grande maioria das ofensas; até cinco motivações são admitidas "
            "desde 2013 e 'bias_motivation_count' informa quantas foram "
            "registradas. Code 88 means no bias motivation and is the value for "
            "the large majority of offenses; up to five motivations are allowed "
            "since 2013 and 'bias_motivation_count' reports how many were recorded."
        ),
        original="bias_id",
    ),
    column(
        "bias_motivation_count",
        "INT64",
        "Número de motivações por preconceito registradas para a ofensa",
        "Number of bias motivations recorded for the offense",
        "Número de motivaciones por prejuicio registradas para el delito",
        unit="bias_motivation",
        original="nibrs_bias_motivation",
    ),
]

OFFENDER = [
    _year(),
    _state_abbr(),
    column(
        "offender_id",
        "STRING",
        "Identificador do agressor atribuído pelo FBI, único dentro do estado e do ano",
        "Offender identifier assigned by the FBI, unique within the state and year",
        "Identificador del agresor asignado por el FBI, único dentro del año",
        original="offender_id",
    ),
    _incident_id(),
    column(
        "offender_sequence_number",
        "STRING",
        "Número de ordem do agressor dentro do incidente",
        "Sequence number of the offender within the incident",
        "Número de orden del agresor dentro del incidente",
        observations=(
            "O valor 0 identifica agressores desconhecidos, cujos atributos "
            "demográficos são todos nulos. The value 0 identifies unknown "
            "offenders, whose demographic attributes are all null."
        ),
        original="offender_seq_num",
    ),
    *_age_block("do agressor", "of the offender", "del agresor"),
    *_demographics("do agressor", "of the offender", "del agresor"),
]

VICTIM = [
    _year(),
    _state_abbr(),
    column(
        "victim_id",
        "STRING",
        "Identificador da vítima atribuído pelo FBI, único dentro do estado e do ano",
        "Victim identifier assigned by the FBI, unique within the state and year",
        "Identificador de la víctima asignado por el FBI, único dentro del estado y del año",
        original="victim_id",
    ),
    _incident_id(),
    column(
        "victim_sequence_number",
        "STRING",
        "Número de ordem da vítima dentro do incidente",
        "Sequence number of the victim within the incident",
        "Número de orden de la víctima dentro del incidente",
        original="victim_seq_num",
    ),
    column(
        "victim_type_code",
        "STRING",
        "Código do tipo de vítima, que pode ser pessoa, empresa, governo ou "
        "sociedade",
        "Code for the type of victim, which may be an individual, a business, "
        "government or society",
        "Código del tipo de víctima, que puede ser persona, empresa, gobierno o "
        "sociedad",
        dictionary="yes",
        observations=(
            "Nem toda vítima é uma pessoa: crimes contra a sociedade têm uma "
            "única vítima do tipo sociedade. Not every victim is a person: crimes "
            "against society have a single victim of type society."
        ),
        original="victim_type_id",
    ),
    *_age_block("da vítima", "of the victim", "de la víctima"),
    *_demographics("da vítima", "of the victim", "de la víctima"),
    column(
        "resident_status_code",
        "STRING",
        "Indica se a vítima residia na localidade em que o incidente ocorreu",
        "Indicates whether the victim resided in the locality where the incident "
        "occurred",
        "Indica si la víctima residía en la localidad donde ocurrió el incidente",
        dictionary="yes",
        original="resident_status_code",
    ),
    column(
        "assignment_type_code",
        "STRING",
        "Código do tipo de escala do policial vitimado, informado apenas quando a "
        "vítima é um policial ferido ou morto em serviço",
        "Code for the assignment type of the victimised officer, reported only "
        "when the victim is an officer injured or killed on duty",
        "Código del tipo de asignación del policía victimizado, informado solo "
        "cuando la víctima es un policía herido o muerto en servicio",
        dictionary="yes",
        original="assignment_type_id",
    ),
    column(
        "activity_type_code",
        "STRING",
        "Código da atividade que o policial vitimado exercia, informado apenas "
        "quando a vítima é um policial ferido ou morto em serviço",
        "Code for the activity the victimised officer was engaged in, reported "
        "only when the victim is an officer injured or killed on duty",
        "Código de la actividad que realizaba el policía victimizado, informado "
        "solo cuando la víctima es un policía herido o muerto en servicio",
        dictionary="yes",
        original="activity_type_id",
    ),
    column(
        "injury_code",
        "STRING",
        "Código do tipo de lesão sofrida pela vítima",
        "Code for the type of injury suffered by the victim",
        "Código del tipo de lesión sufrida por la víctima",
        dictionary="yes",
        observations=(
            "O NIBRS admite até dez lesões por vítima; guarda-se aqui o menor "
            "código e 'injury_count' informa quantas foram registradas. NIBRS "
            "allows up to ten injuries per victim; the lowest code is kept here "
            "and 'injury_count' reports how many were recorded."
        ),
        original="injury_id",
    ),
    column(
        "injury_count",
        "INT64",
        "Número de lesões registradas para a vítima",
        "Number of injuries recorded for the victim",
        "Número de lesiones registradas para la víctima",
        unit="injury",
        original="nibrs_victim_injury",
    ),
]

VICTIM_OFFENSE = [
    _year(),
    _state_abbr(),
    column(
        "victim_id",
        "STRING",
        "Identificador da vítima",
        "Victim identifier",
        "Identificador de la víctima",
        original="victim_id",
    ),
    column(
        "offense_id",
        "STRING",
        "Identificador da ofensa sofrida pela vítima",
        "Identifier of the offense suffered by the victim",
        "Identificador del delito sufrido por la víctima",
        original="offense_id",
    ),
]

VICTIM_OFFENDER_RELATIONSHIP = [
    _year(),
    _state_abbr(),
    column(
        "victim_id",
        "STRING",
        "Identificador da vítima",
        "Victim identifier",
        "Identificador de la víctima",
        original="victim_id",
    ),
    column(
        "offender_id",
        "STRING",
        "Identificador do agressor",
        "Offender identifier",
        "Identificador del agresor",
        original="offender_id",
    ),
    column(
        "relationship_code",
        "STRING",
        "Código da relação da vítima com o agressor",
        "Code for the victim's relationship to the offender",
        "Código de la relación de la víctima con el agresor",
        dictionary="yes",
        observations=(
            "Obrigatório apenas quando o incidente inclui um crime contra a "
            "pessoa ou um roubo. Sete pares vítima-agressor em 68,9 milhões "
            "trazem duas relações distintas, registradas assim pela própria "
            "agência, e por isso o código integra a chave da tabela. Mandatory "
            "only when the incident includes a crime against a person or a "
            "robbery. Seven victim-offender pairs out of 68.9 million carry two "
            "distinct relationships, recorded that way by the agency itself, "
            "which is why the code forms part of the table's key."
        ),
        original="relationship_id",
    ),
]

ARRESTEE = [
    _year(),
    _state_abbr(),
    _ori(),
    column(
        "arrestee_id",
        "STRING",
        "Identificador da pessoa presa atribuído pelo FBI, único dentro do ano e "
        "do grupo de prisão",
        "Identifier of the arrested person assigned by the FBI, unique within the "
        "year and arrest group",
        "Identificador de la persona detenida asignado por el FBI, único dentro "
        "del año y del grupo de arresto",
        original="arrestee_id",
    ),
    _incident_id(
        nullable_note=(
            "Nulo nas prisões do Grupo B, que não têm relatório de incidente. "
            "Null for Group B arrests, which have no incident report."
        )
    ),
    column(
        "arrest_group",
        "STRING",
        "Indica se a prisão vem de um relatório de incidente do Grupo A ou de um "
        "relatório de prisão do Grupo B",
        "Indicates whether the arrest comes from a Group A incident report or a "
        "Group B arrest report",
        "Indica si el arresto proviene de un informe de incidente del Grupo A o "
        "de un informe de arresto del Grupo B",
        dictionary="yes",
        observations=(
            "As prisões do Grupo B chegam sem vínculo com incidente ou agência "
            "nos arquivos publicados, de modo que 'ori' e 'incident_id' são "
            "nulos e só o estado e o ano são conhecidos. Group B arrests arrive "
            "with no incident or agency link in the published files, so 'ori' and "
            "'incident_id' are null and only the state and year are known."
        ),
        original="",
    ),
    column(
        "arrestee_sequence_number",
        "STRING",
        "Número de ordem da pessoa presa dentro do incidente",
        "Sequence number of the arrested person within the incident",
        "Número de orden de la persona detenida dentro del incidente",
        original="arrestee_seq_num",
    ),
    column(
        "arrest_date",
        "DATE",
        "Data da prisão",
        "Date of the arrest",
        "Fecha del arresto",
        original="arrest_date",
    ),
    column(
        "arrest_type_code",
        "STRING",
        "Código do tipo de prisão, por exemplo em flagrante ou por mandado",
        "Code for the type of arrest, for example on-view or on a warrant",
        "Código del tipo de arresto, por ejemplo en flagrancia o por orden judicial",
        dictionary="yes",
        original="arrest_type_id",
    ),
    column(
        "multiple_arrestee_indicator",
        "STRING",
        "Indica se a prisão faz parte de uma prisão múltipla já contada em outro "
        "registro",
        "Indicates whether the arrest is part of a multiple arrest already counted "
        "in another record",
        "Indica si el arresto forma parte de un arresto múltiple ya contado en "
        "otro registro",
        dictionary="yes",
        original="multiple_indicator",
    ),
    column(
        "offense_code",
        "STRING",
        "Código NIBRS da ofensa pela qual a pessoa foi presa",
        "NIBRS code of the offense for which the person was arrested",
        "Código NIBRS del delito por el cual la persona fue detenida",
        dictionary="yes",
        original="offense_code",
    ),
    *_age_block(
        "da pessoa presa", "of the arrested person", "de la persona detenida"
    ),
    *_demographics(
        "da pessoa presa", "of the arrested person", "de la persona detenida"
    ),
    column(
        "resident_status_code",
        "STRING",
        "Indica se a pessoa presa residia na localidade da prisão",
        "Indicates whether the arrested person resided in the locality of the arrest",
        "Indica si la persona detenida residía en la localidad del arresto",
        dictionary="yes",
        original="resident_code",
    ),
    column(
        "under_18_disposition_code",
        "STRING",
        "Código do encaminhamento dado a pessoas presas com menos de 18 anos",
        "Code for the disposition given to arrested persons under 18",
        "Código de la disposición dada a personas detenidas menores de 18 años",
        dictionary="yes",
        original="under_18_disposition_code",
    ),
    column(
        "weapon_code",
        "STRING",
        "Código da arma portada pela pessoa no momento da prisão",
        "Code for the weapon carried by the person at the time of arrest",
        "Código del arma portada por la persona en el momento del arresto",
        dictionary="yes",
        observations=(
            "Guarda-se o menor código quando há mais de uma arma. The lowest code "
            "is kept when more than one weapon was recorded."
        ),
        original="weapon_id",
    ),
]

PROPERTY = [
    _year(),
    _state_abbr(),
    column(
        "property_description_id",
        "STRING",
        "Identificador da descrição de bem atribuído pelo FBI, único dentro do estado e do ano",
        "Property description identifier assigned by the FBI, unique within the state and year",
        "Identificador de la descripción del bien asignado por el FBI, único "
        "dentro del año",
        original="nibrs_prop_desc_id",
    ),
    column(
        "property_id",
        "STRING",
        "Identificador do registro de bem a que a descrição pertence",
        "Identifier of the property record to which the description belongs",
        "Identificador del registro de bien al que pertenece la descripción",
        original="property_id",
    ),
    _incident_id(),
    column(
        "property_loss_code",
        "STRING",
        "Código do tipo de perda do bem, por exemplo furtado, recuperado, "
        "apreendido ou destruído",
        "Code for the type of property loss, for example stolen, recovered, seized "
        "or destroyed",
        "Código del tipo de pérdida del bien, por ejemplo robado, recuperado, "
        "incautado o destruido",
        dictionary="yes",
        original="prop_loss_id",
    ),
    column(
        "property_description_code",
        "STRING",
        "Código do tipo de bem envolvido",
        "Code for the type of property involved",
        "Código del tipo de bien involucrado",
        dictionary="yes",
        observations=(
            "Nulo quando o registro de bem não traz descrição, o que ocorre "
            "sobretudo em incidentes sem perda de bens. Null when the property "
            "record carries no description, which happens mostly in incidents "
            "with no property loss."
        ),
        original="prop_desc_id",
    ),
    column(
        "property_value",
        "INT64",
        "Valor do bem em dólares correntes do ano do incidente",
        "Value of the property in current dollars of the incident year",
        "Valor del bien en dólares corrientes del año del incidente",
        unit="usd",
        observations=(
            "Valores nominais, sem correção pela inflação. Nominal values, not "
            "adjusted for inflation."
        ),
        original="property_value",
    ),
    column(
        "date_recovered",
        "DATE",
        "Data em que o bem foi recuperado",
        "Date on which the property was recovered",
        "Fecha en que se recuperó el bien",
        original="date_recovered",
    ),
    column(
        "stolen_count",
        "INT64",
        "Número de veículos furtados no incidente, informado apenas para furto de "
        "veículo",
        "Number of vehicles stolen in the incident, reported only for motor "
        "vehicle theft",
        "Número de vehículos robados en el incidente, informado solo para robo de "
        "vehículo",
        unit="vehicle",
        original="stolen_count",
    ),
    column(
        "recovered_count",
        "INT64",
        "Número de veículos recuperados no incidente, informado apenas para furto "
        "de veículo",
        "Number of vehicles recovered in the incident, reported only for motor "
        "vehicle theft",
        "Número de vehículos recuperados en el incidente, informado solo para robo "
        "de vehículo",
        unit="vehicle",
        original="recovered_count",
    ),
    column(
        "suspected_drug_code",
        "STRING",
        "Código do tipo de droga suspeita apreendida",
        "Code for the type of suspected drug seized",
        "Código del tipo de droga sospechosa incautada",
        dictionary="yes",
        observations=(
            "Preenchido apenas em ofensas de drogas; guarda-se o menor código "
            "quando há mais de uma droga. Populated only for drug offenses; the "
            "lowest code is kept when more than one drug was recorded."
        ),
        original="suspected_drug_type_id",
    ),
    column(
        "drug_quantity",
        "FLOAT64",
        "Quantidade estimada da droga apreendida, na unidade indicada por "
        "'drug_measure_code'",
        "Estimated quantity of the drug seized, in the unit given by "
        "'drug_measure_code'",
        "Cantidad estimada de la droga incautada, en la unidad indicada por "
        "'drug_measure_code'",
        unit="drug_measure_unit",
        original="est_drug_qty",
    ),
    column(
        "drug_measure_code",
        "STRING",
        "Código da unidade de medida da quantidade de droga",
        "Code for the unit of measurement of the drug quantity",
        "Código de la unidad de medida de la cantidad de droga",
        dictionary="yes",
        original="drug_measure_type_id",
    ),
]

HATE_CRIME = [
    _year(),
    _state_abbr(),
    _ori(),
    column(
        "incident_id",
        "STRING",
        "Identificador do incidente de crime de ódio atribuído pelo FBI",
        "Hate crime incident identifier assigned by the FBI",
        "Identificador del incidente de delito de odio asignado por el FBI",
        observations=(
            "Numeração própria da série de crimes de ódio; não corresponde ao "
            "'incident_id' das tabelas do NIBRS. Numbered within the hate crime "
            "series; it does not correspond to 'incident_id' in the NIBRS tables."
        ),
        original="incident_id",
    ),
    column(
        "incident_date",
        "DATE",
        "Data em que o incidente ocorreu",
        "Date on which the incident occurred",
        "Fecha en que ocurrió el incidente",
        original="incident_date",
    ),
    column(
        "agency_name",
        "STRING",
        "Nome público da agência que registrou o incidente",
        "Public name of the agency that recorded the incident",
        "Nombre público de la agencia que registró el incidente",
        original="pug_agency_name",
    ),
    column(
        "agency_unit",
        "STRING",
        "Unidade ou subdivisão da agência que registrou o incidente",
        "Unit or subdivision of the agency that recorded the incident",
        "Unidad o subdivisión de la agencia que registró el incidente",
        original="pub_agency_unit",
    ),
    column(
        "agency_type",
        "STRING",
        "Tipo da agência que registrou o incidente",
        "Type of the agency that recorded the incident",
        "Tipo de la agencia que registró el incidente",
        original="agency_type_name",
    ),
    column(
        "division_name",
        "STRING",
        "Divisão censitária dos Estados Unidos em que a agência se localiza",
        "United States census division in which the agency is located",
        "División censal de los Estados Unidos en la que se ubica la agencia",
        original="division_name",
    ),
    column(
        "region_name",
        "STRING",
        "Região censitária dos Estados Unidos em que a agência se localiza",
        "United States census region in which the agency is located",
        "Región censal de los Estados Unidos en la que se ubica la agencia",
        original="region_name",
    ),
    column(
        "population_group_code",
        "STRING",
        "Código do grupo populacional do UCR da agência",
        "UCR population group code of the agency",
        "Código del grupo poblacional del UCR de la agencia",
        dictionary="yes",
        original="population_group_code",
    ),
    column(
        "offense_name",
        "STRING",
        "Nomes das ofensas do incidente, separados por ponto e vírgula quando há "
        "mais de uma",
        "Names of the offenses in the incident, separated by semicolons when there "
        "is more than one",
        "Nombres de los delitos del incidente, separados por punto y coma cuando "
        "hay más de uno",
        original="offense_name",
    ),
    column(
        "bias_description",
        "STRING",
        "Descrições da motivação por preconceito, separadas por ponto e vírgula "
        "quando há mais de uma",
        "Descriptions of the bias motivation, separated by semicolons when there "
        "is more than one",
        "Descripciones de la motivación por prejuicio, separadas por punto y coma "
        "cuando hay más de una",
        original="bias_desc",
    ),
    column(
        "location_name",
        "STRING",
        "Nome do tipo de local em que o incidente ocorreu",
        "Name of the type of location where the incident occurred",
        "Nombre del tipo de lugar donde ocurrió el incidente",
        original="location_name",
    ),
    column(
        "victim_types",
        "STRING",
        "Tipos de vítima do incidente, separados por ponto e vírgula quando há "
        "mais de um",
        "Types of victim in the incident, separated by semicolons when there is "
        "more than one",
        "Tipos de víctima del incidente, separados por punto y coma cuando hay más "
        "de uno",
        original="victim_types",
    ),
    column(
        "victim_count",
        "INT64",
        "Número de vítimas do incidente, contando vítimas que não são pessoas",
        "Number of victims in the incident, counting victims that are not people",
        "Número de víctimas del incidente, contando víctimas que no son personas",
        unit="victim",
        original="victim_count",
    ),
    column(
        "individual_victim_count",
        "INT64",
        "Número de vítimas individuais do incidente",
        "Number of individual victims in the incident",
        "Número de víctimas individuales del incidente",
        unit="person",
        original="total_individual_victims",
    ),
    column(
        "adult_victim_count",
        "INT64",
        "Número de vítimas adultas do incidente",
        "Number of adult victims in the incident",
        "Número de víctimas adultas del incidente",
        unit="person",
        original="adult_victim_count",
    ),
    column(
        "juvenile_victim_count",
        "INT64",
        "Número de vítimas menores de 18 anos do incidente",
        "Number of victims under 18 in the incident",
        "Número de víctimas menores de 18 años del incidente",
        unit="person",
        original="juvenile_victim_count",
    ),
    column(
        "offender_count",
        "INT64",
        "Número de agressores do incidente",
        "Number of offenders in the incident",
        "Número de agresores del incidente",
        unit="person",
        observations=(
            "O valor 0 indica que o número de agressores é desconhecido. The "
            "value 0 indicates that the number of offenders is unknown."
        ),
        original="total_offender_count",
    ),
    column(
        "adult_offender_count",
        "INT64",
        "Número de agressores adultos do incidente",
        "Number of adult offenders in the incident",
        "Número de agresores adultos del incidente",
        unit="person",
        original="adult_offender_count",
    ),
    column(
        "juvenile_offender_count",
        "INT64",
        "Número de agressores menores de 18 anos do incidente",
        "Number of offenders under 18 in the incident",
        "Número de agresores menores de 18 años del incidente",
        unit="person",
        original="juvenile_offender_count",
    ),
    column(
        "offender_race",
        "STRING",
        "Raça percebida dos agressores do incidente",
        "Perceived race of the offenders in the incident",
        "Raza percibida de los agresores del incidente",
        original="offender_race",
    ),
    column(
        "offender_ethnicity",
        "STRING",
        "Etnia percebida dos agressores do incidente",
        "Perceived ethnicity of the offenders in the incident",
        "Etnia percibida de los agresores del incidente",
        original="offender_ethnicity",
    ),
    column(
        "multiple_offense_flag",
        "STRING",
        "Indica se o incidente envolveu mais de uma ofensa",
        "Indicates whether the incident involved more than one offense",
        "Indica si el incidente involucró más de un delito",
        dictionary="yes",
        original="multiple_offense",
    ),
    column(
        "multiple_bias_flag",
        "STRING",
        "Indica se o incidente envolveu mais de uma motivação por preconceito",
        "Indicates whether the incident involved more than one bias motivation",
        "Indica si el incidente involucró más de una motivación por prejuicio",
        dictionary="yes",
        original="multiple_bias",
    ),
]

UCR_SUMMARY = [
    _year(original="year"),
    _state_abbr(original="numeric_state_code"),
    column(
        "ori",
        "STRING",
        "Originating Agency Identifier de nove caracteres, para junção com as "
        "demais tabelas",
        "Nine-character Originating Agency Identifier, for joining to the other "
        "tables",
        "Originating Agency Identifier de nueve caracteres, para unir con las "
        "demás tablas",
        observations=(
            "O arquivo Return A traz apenas o ORI de sete caracteres do NCIC; a "
            "forma de nove é obtida na limpeza acrescentando o sufixo '00' da "
            "própria agência. A regra resolve para um ORI existente em 87,6% das "
            "agências em 2023 e 96,5% em 1995; as demais são agências que nunca "
            "enviaram o formulário de pessoal. The Return A file carries only the "
            "seven-character NCIC ORI; the nine-character form is derived during "
            "cleaning by appending the agency's own '00' suffix. The rule resolves "
            "to an existing ORI for 87.6% of agencies in 2023 and 96.5% in 1995; "
            "the rest are agencies that never filed an employee return."
        ),
        original="ori_code",
    ),
    column(
        "legacy_ori",
        "STRING",
        "ORI de sete caracteres do NCIC, tal como publicado no arquivo Return A",
        "Seven-character NCIC ORI, as published in the Return A file",
        "ORI de siete caracteres del NCIC, tal como se publica en el archivo Return A",
        original="ori_code",
    ),
    column(
        "record_number",
        "STRING",
        "Ordem do registro físico Return A da agência dentro do ano, a partir de 1",
        "Ordinal of the agency's physical Return A record within the year, from 1",
        "Orden del registro físico Return A de la agencia dentro del año, desde 1",
        observations=(
            "Quase sempre 1. O FBI passou a enviar mais de um registro por "
            "agência e ano nos arquivos recentes — 272 agências em 2022, uma "
            "delas 16 vezes — cada um com suas próprias contagens; nada no "
            "registro diz se substituem ou somam-se uns aos outros, por isso são "
            "mantidos e numerados. Almost always 1. The FBI began shipping more "
            "than one record per agency-year in the recent files — 272 agencies "
            "in 2022, one of them 16 times — each with its own counts; nothing "
            "in the record says whether they replace or supplement one another, "
            "so they are kept and numbered."
        ),
        original="",
    ),
    column(
        "month",
        "INT64",
        "Mês de referência dos dados, de 1 a 12",
        "Reference month of the data, from 1 to 12",
        "Mes de referencia de los datos, de 1 a 12",
        unit="month",
        original="month",
    ),
    column(
        "offense_code",
        "STRING",
        "Item de ofensa do formulário Return A a que as contagens se referem",
        "Return A form offense line item to which the counts refer",
        "Ítem de delito del formulario Return A al que se refieren los conteos",
        dictionary="yes",
        observations=(
            "São os 27 itens do formulário resumido Return A, não os códigos do "
            "NIBRS: alguns são totais que somam os itens seguintes, de modo que "
            "somar todos os itens conta duas vezes. These are the 27 line items of "
            "the Return A summary form, not NIBRS codes: some are totals that add "
            "up the following items, so summing every item double counts."
        ),
        original="",
    ),
    column(
        "actual_count",
        "INT64",
        "Número de ofensas efetivamente ocorridas, já descontadas as infundadas",
        "Number of actual offenses, with unfounded offenses already deducted",
        "Número de delitos efectivamente ocurridos, ya descontados los infundados",
        unit="offense",
        original="card_1_actual_offenses",
    ),
    column(
        "unfounded_count",
        "INT64",
        "Número de ofensas denunciadas que a investigação concluiu não terem "
        "ocorrido",
        "Number of reported offenses that the investigation found did not occur",
        "Número de delitos denunciados que la investigación concluyó que no "
        "ocurrieron",
        unit="offense",
        observations=(
            "Zero em todos os registros anteriores a 1983. Zero for every record "
            "before 1983."
        ),
        original="card_0_unfounded_offenses",
    ),
    column(
        "cleared_count",
        "INT64",
        "Número de ofensas esclarecidas por prisão ou por meio excepcional",
        "Number of offenses cleared by arrest or exceptional means",
        "Número de delitos esclarecidos por arresto o por medios excepcionales",
        unit="offense",
        original="card_2_total_offenses_cleared",
    ),
    column(
        "juvenile_cleared_count",
        "INT64",
        "Número de ofensas esclarecidas envolvendo apenas pessoas menores de 18 anos",
        "Number of offenses cleared involving only persons under 18",
        "Número de delitos esclarecidos que involucran solo a personas menores de "
        "18 años",
        unit="offense",
        original="card_3_clearances_under_18",
    ),
    column(
        "record_type_code",
        "STRING",
        "Código do tipo de registro do mês, que distingue retorno normal, ajuste e "
        "dado indisponível",
        "Code for the month's record type, distinguishing a normal return, an "
        "adjustment and unavailable data",
        "Código del tipo de registro del mes, que distingue retorno normal, ajuste "
        "y dato no disponible",
        dictionary="yes",
        observations=(
            "Registros de ajuste podem trazer contagens negativas, que corrigem "
            "meses anteriores. Adjustment records may carry negative counts, which "
            "correct earlier months."
        ),
        original="card_1_type",
    ),
    column(
        "breakdown_reported_flag",
        "STRING",
        "Indica se a agência informou a abertura por subtipo de ofensa ou apenas "
        "os totais",
        "Indicates whether the agency reported the breakdown by offense subtype or "
        "only the totals",
        "Indica si la agencia informó el desglose por subtipo de delito o solo los "
        "totales",
        dictionary="yes",
        original="card_1_p_t",
    ),
]

DICIONARIO = [
    column(
        "id_tabela",
        "STRING",
        "Nome da tabela a que o código se aplica",
        "Name of the table the code applies to",
        "Nombre de la tabla a la que se aplica el código",
    ),
    column(
        "nome_coluna",
        "STRING",
        "Nome da coluna a que o código se aplica",
        "Name of the column the code applies to",
        "Nombre de la columna a la que se aplica el código",
    ),
    column(
        "chave",
        "STRING",
        "Código, tal como armazenado na coluna",
        "Code, as stored in the column",
        "Código, tal como se almacena en la columna",
    ),
    column(
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal do código",
        "Temporal coverage of the code",
        "Cobertura temporal del código",
    ),
    column(
        "valor",
        "STRING",
        "Rótulo do código, em inglês, a língua da fonte",
        "Label of the code, in English, the language of the source",
        "Etiqueta del código, en inglés, el idioma de la fuente",
    ),
]


TABLES = {
    "agency": {
        "columns": AGENCY,
        "partitions": ["year"],
        "unique_key": ["year", "ori"],
        "first_year": 1960,
        "last_year": 2025,
    },
    "incident": {
        "columns": INCIDENT,
        "partitions": ["year", "state_abbr"],
        "unique_key": ["year", "state_abbr", "incident_id"],
        "first_year": 1991,
        "last_year": 2025,
    },
    "offense": {
        "columns": OFFENSE,
        "partitions": ["year", "state_abbr"],
        "unique_key": ["year", "state_abbr", "offense_id"],
        "first_year": 1991,
        "last_year": 2025,
    },
    "offender": {
        "columns": OFFENDER,
        "partitions": ["year", "state_abbr"],
        "unique_key": ["year", "state_abbr", "offender_id"],
        "first_year": 1991,
        "last_year": 2025,
    },
    "victim": {
        "columns": VICTIM,
        "partitions": ["year", "state_abbr"],
        "unique_key": ["year", "state_abbr", "victim_id"],
        "first_year": 1991,
        "last_year": 2025,
    },
    "victim_offense": {
        "columns": VICTIM_OFFENSE,
        "partitions": ["year", "state_abbr"],
        "unique_key": ["year", "state_abbr", "victim_id", "offense_id"],
        "first_year": 1991,
        "last_year": 2025,
    },
    "victim_offender_relationship": {
        "columns": VICTIM_OFFENDER_RELATIONSHIP,
        "partitions": ["year", "state_abbr"],
        "unique_key": [
            "year",
            "state_abbr",
            "victim_id",
            "offender_id",
            "relationship_code",
        ],
        "first_year": 1991,
        "last_year": 2025,
    },
    "arrestee": {
        "columns": ARRESTEE,
        "partitions": ["year", "state_abbr"],
        "unique_key": ["year", "state_abbr", "arrest_group", "arrestee_id"],
        "first_year": 1991,
        "last_year": 2025,
    },
    "property": {
        "columns": PROPERTY,
        "partitions": ["year", "state_abbr"],
        "unique_key": ["year", "state_abbr", "property_description_id"],
        "first_year": 1991,
        "last_year": 2025,
    },
    "hate_crime": {
        "columns": HATE_CRIME,
        "partitions": ["year"],
        "unique_key": ["year", "incident_id"],
        "first_year": 1991,
        "last_year": 2025,
    },
    "ucr_summary": {
        "columns": UCR_SUMMARY,
        "partitions": ["year"],
        "unique_key": [
            "year",
            "ori",
            "record_number",
            "month",
            "offense_code",
        ],
        "first_year": 1985,
        "last_year": 2025,
    },
    "dicionario": {
        "columns": DICIONARIO,
        "partitions": [],
        "unique_key": [
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
        ],
        "first_year": None,
        "last_year": None,
    },
}


def column_names(table):
    return [c["name"] for c in TABLES[table]["columns"]]
