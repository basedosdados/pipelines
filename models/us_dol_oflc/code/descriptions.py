"""Trilingual descriptions and column metadata for the us_dol_oflc tables.

Keys are canonical column names; a ``name:program`` key overrides the shared
entry where the meaning differs by program. Each value is
``(description_en, description_pt, description_es)``.

Descriptions start with a capital letter and carry no trailing period, per the
Data Basis style manual.
"""

D: dict[str, tuple[str, str, str]] = {
    # ---- keys and case lifecycle -------------------------------------------
    "year": (
        "Federal fiscal year of the disclosure file the case was published in, running from 1 October to 30 September",
        "Ano fiscal federal do arquivo de divulgação em que o caso foi publicado, de 1º de outubro a 30 de setembro",
        "Año fiscal federal del archivo de divulgación en que el caso fue publicado, del 1 de octubre al 30 de septiembre",
    ),
    "case_number": (
        "Case number assigned by the Office of Foreign Labor Certification, unique within the fiscal year",
        "Número do caso atribuído pelo Office of Foreign Labor Certification, único dentro do ano fiscal",
        "Número de caso asignado por la Office of Foreign Labor Certification, único dentro del año fiscal",
    ),
    "case_status": (
        "Outcome of the application at the time the file was published",
        "Resultado da solicitação no momento da publicação do arquivo",
        "Resultado de la solicitud en el momento de la publicación del archivo",
    ),
    "visa_class": (
        "Visa classification requested on the application",
        "Classificação de visto solicitada na aplicação",
        "Clasificación de visa solicitada en la aplicación",
    ),
    "received_date": (
        "Date the application was received by the Department of Labor",
        "Data em que a solicitação foi recebida pelo Departamento do Trabalho",
        "Fecha en que la solicitud fue recibida por el Departamento de Trabajo",
    ),
    "decision_date": (
        "Date the Department of Labor issued its determination on the application",
        "Data em que o Departamento do Trabalho emitiu sua decisão sobre a solicitação",
        "Fecha en que el Departamento de Trabajo emitió su decisión sobre la solicitud",
    ),
    "original_certification_date": (
        "Date of the original certification when the case is an amendment or a withdrawal of a certified application",
        "Data da certificação original quando o caso é uma emenda ou uma retirada de solicitação já certificada",
        "Fecha de la certificación original cuando el caso es una enmienda o un retiro de solicitud ya certificada",
    ),
    "employment_begin_date": (
        "First day of the requested period of employment",
        "Primeiro dia do período de emprego solicitado",
        "Primer día del período de empleo solicitado",
    ),
    "employment_end_date": (
        "Last day of the requested period of employment",
        "Último dia do período de emprego solicitado",
        "Último día del período de empleo solicitado",
    ),
    "certification_begin_date": (
        "First day of the certified period of employment",
        "Primeiro dia do período de emprego certificado",
        "Primer día del período de empleo certificado",
    ),
    "certification_end_date": (
        "Last day of the certified period of employment",
        "Último dia do período de emprego certificado",
        "Último día del período de empleo certificado",
    ),
    "requested_begin_date": (
        "First day of the period of need stated by the employer",
        "Primeiro dia do período de necessidade declarado pelo empregador",
        "Primer día del período de necesidad declarado por el empleador",
    ),
    "requested_end_date": (
        "Last day of the period of need stated by the employer",
        "Último dia do período de necessidade declarado pelo empregador",
        "Último día del período de necesidad declarado por el empleador",
    ),
    # ---- employer -----------------------------------------------------------
    "employer_name": (
        "Legal business name of the employer filing the application",
        "Razão social do empregador que apresenta a solicitação",
        "Razón social del empleador que presenta la solicitud",
    ),
    "employer_trade_name": (
        "Trade name or doing-business-as name of the employer",
        "Nome fantasia do empregador",
        "Nombre comercial del empleador",
    ),
    "employer_address": (
        "First line of the employer's business address",
        "Primeira linha do endereço comercial do empregador",
        "Primera línea de la dirección comercial del empleador",
    ),
    "employer_city": (
        "City of the employer's business address",
        "Município do endereço comercial do empregador",
        "Ciudad de la dirección comercial del empleador",
    ),
    "employer_state": (
        "USPS two-letter abbreviation of the state of the employer's business address",
        "Sigla de duas letras do USPS do estado do endereço comercial do empregador",
        "Sigla de dos letras del USPS del estado de la dirección comercial del empleador",
    ),
    "employer_postal_code": (
        "ZIP code of the employer's business address",
        "Código postal (ZIP) do endereço comercial do empregador",
        "Código postal (ZIP) de la dirección comercial del empleador",
    ),
    "employer_country": (
        "Country of the employer's business address",
        "País do endereço comercial do empregador",
        "País de la dirección comercial del empleador",
    ),
    "naics_id": (
        "North American Industry Classification System code of the employer's industry",
        "Código do North American Industry Classification System do setor do empregador",
        "Código del North American Industry Classification System del sector del empleador",
    ),
    "employer_num_employees": (
        "Number of employees on the employer's payroll",
        "Número de empregados na folha de pagamento do empregador",
        "Número de empleados en la nómina del empleador",
    ),
    "employer_year_commenced_business": (
        "Year the employer commenced business",
        "Ano em que o empregador iniciou suas atividades",
        "Año en que el empleador inició sus actividades",
    ),
    # ---- job ---------------------------------------------------------------
    "job_title": (
        "Job title of the position offered, as written by the employer",
        "Cargo da posição oferecida, conforme escrito pelo empregador",
        "Cargo del puesto ofrecido, según lo escrito por el empleador",
    ),
    "soc_id": (
        "Standard Occupational Classification code of the position offered",
        "Código da Standard Occupational Classification da posição oferecida",
        "Código de la Standard Occupational Classification del puesto ofrecido",
    ),
    "soc_title": (
        "Standard Occupational Classification title of the position offered",
        "Título da Standard Occupational Classification da posição oferecida",
        "Título de la Standard Occupational Classification del puesto ofrecido",
    ),
    "full_time_position": (
        "Whether the position offered is full time",
        "Indica se a posição oferecida é de tempo integral",
        "Indica si el puesto ofrecido es de tiempo completo",
    ),
    "total_workers": (
        "Number of worker positions requested on the application",
        "Número de posições de trabalhadores solicitadas na aplicação",
        "Número de puestos de trabajadores solicitados en la aplicación",
    ),
    "workers_requested": (
        "Number of workers requested by the employer",
        "Número de trabalhadores solicitados pelo empregador",
        "Número de trabajadores solicitados por el empleador",
    ),
    "workers_certified": (
        "Number of workers certified by the Department of Labor",
        "Número de trabalhadores certificados pelo Departamento do Trabalho",
        "Número de trabajadores certificados por el Departamento de Trabajo",
    ),
    # ---- wages -------------------------------------------------------------
    "wage_offered_from": (
        "Wage offered to the worker, at the bottom of the offered range, in the unit given by wage_unit_of_pay",
        "Salário oferecido ao trabalhador, no piso da faixa oferecida, na unidade indicada por wage_unit_of_pay",
        "Salario ofrecido al trabajador, en el piso del rango ofrecido, en la unidad indicada por wage_unit_of_pay",
    ),
    "wage_offered_to": (
        "Wage offered to the worker, at the top of the offered range, in the unit given by wage_unit_of_pay",
        "Salário oferecido ao trabalhador, no teto da faixa oferecida, na unidade indicada por wage_unit_of_pay",
        "Salario ofrecido al trabajador, en el techo del rango ofrecido, en la unidad indicada por wage_unit_of_pay",
    ),
    "wage_unit_of_pay": (
        "Period the offered wage refers to, harmonised by Data Basis to hour, day, week, bi-weekly, semi-monthly, month, year or piece rate",
        "Período a que se refere o salário oferecido, harmonizado pela Data Basis em hora, dia, semana, quinzena, quinzena fixa, mês, ano ou peça produzida",
        "Período al que se refiere el salario ofrecido, armonizado por Data Basis en hora, día, semana, quincena, quincena fija, mes, año o pieza producida",
    ),
    "wage_offered_from_annual": (
        "Offered wage at the bottom of the range converted to an annual amount by Data Basis, null when the unit is missing or is a piece rate",
        "Salário oferecido no piso da faixa convertido para valor anual pela Data Basis, nulo quando a unidade está ausente ou é por peça produzida",
        "Salario ofrecido en el piso del rango convertido a un monto anual por Data Basis, nulo cuando la unidad está ausente o es por pieza producida",
    ),
    "wage_offered_to_annual": (
        "Offered wage at the top of the range converted to an annual amount by Data Basis, null when the unit is missing or is a piece rate",
        "Salário oferecido no teto da faixa convertido para valor anual pela Data Basis, nulo quando a unidade está ausente ou é por peça produzida",
        "Salario ofrecido en el techo del rango convertido a un monto anual por Data Basis, nulo cuando la unidad está ausente o es por pieza producida",
    ),
    "prevailing_wage": (
        "Prevailing wage determined for the occupation and area, in the unit given by prevailing_wage_unit_of_pay",
        "Salário prevalecente determinado para a ocupação e a área, na unidade indicada por prevailing_wage_unit_of_pay",
        "Salario prevaleciente determinado para la ocupación y el área, en la unidad indicada por prevailing_wage_unit_of_pay",
    ),
    "prevailing_wage_unit_of_pay": (
        "Period the prevailing wage refers to, harmonised by Data Basis to hour, day, week, bi-weekly, semi-monthly, month, year or piece rate",
        "Período a que se refere o salário prevalecente, harmonizado pela Data Basis em hora, dia, semana, quinzena, quinzena fixa, mês, ano ou peça produzida",
        "Período al que se refiere el salario prevaleciente, armonizado por Data Basis en hora, día, semana, quincena, quincena fija, mes, año o pieza producida",
    ),
    "prevailing_wage_annual": (
        "Prevailing wage converted to an annual amount by Data Basis, null when the unit is missing or is a piece rate",
        "Salário prevalecente convertido para valor anual pela Data Basis, nulo quando a unidade está ausente ou é por peça produzida",
        "Salario prevaleciente convertido a un monto anual por Data Basis, nulo cuando la unidad está ausente o es por pieza producida",
    ),
    "prevailing_wage_level": (
        "Skill level of the prevailing wage determination, from Level I to Level IV",
        "Nível de qualificação da determinação do salário prevalecente, do Nível I ao Nível IV",
        "Nivel de calificación de la determinación del salario prevaleciente, del Nivel I al Nivel IV",
    ),
    "prevailing_wage_source": (
        "Source of the prevailing wage determination, such as the OES survey or an employer-provided survey",
        "Fonte da determinação do salário prevalecente, como a pesquisa OES ou uma pesquisa fornecida pelo empregador",
        "Fuente de la determinación del salario prevaleciente, como la encuesta OES o una encuesta proporcionada por el empleador",
    ),
    "prevailing_wage_source_year": (
        "Publication year of the wage survey used in the prevailing wage determination",
        "Ano de publicação da pesquisa salarial usada na determinação do salário prevalecente",
        "Año de publicación de la encuesta salarial usada en la determinación del salario prevaleciente",
    ),
    "prevailing_wage_tracking_number": (
        "Tracking or case number of the prevailing wage determination issued for the application",
        "Número de rastreamento ou de caso da determinação de salário prevalecente emitida para a solicitação",
        "Número de seguimiento o de caso de la determinación de salario prevaleciente emitida para la solicitud",
    ),
    "prevailing_wage_determination_date": (
        "Date the prevailing wage determination was issued",
        "Data em que a determinação do salário prevalecente foi emitida",
        "Fecha en que se emitió la determinación del salario prevaleciente",
    ),
    "prevailing_wage_expiration_date": (
        "Date the prevailing wage determination expires",
        "Data de expiração da determinação do salário prevalecente",
        "Fecha de vencimiento de la determinación del salario prevaleciente",
    ),
    "overtime_rate_from": (
        "Overtime rate offered, at the bottom of the offered range",
        "Taxa de hora extra oferecida, no piso da faixa oferecida",
        "Tarifa de horas extra ofrecida, en el piso del rango ofrecido",
    ),
    "overtime_rate_to": (
        "Overtime rate offered, at the top of the offered range",
        "Taxa de hora extra oferecida, no teto da faixa oferecida",
        "Tarifa de horas extra ofrecida, en el techo del rango ofrecido",
    ),
    "piece_rate_offer": (
        "Piece rate offered to the worker, when pay is per unit produced",
        "Valor por peça oferecido ao trabalhador, quando a remuneração é por unidade produzida",
        "Valor por pieza ofrecido al trabajador, cuando la remuneración es por unidad producida",
    ),
    "piece_rate_unit": (
        "Unit the piece rate is paid per, as written by the employer",
        "Unidade pela qual o valor por peça é pago, conforme escrito pelo empregador",
        "Unidad por la cual se paga el valor por pieza, según lo escrito por el empleador",
    ),
    "frequency_of_pay": (
        "How often the worker is paid",
        "Frequência com que o trabalhador é pago",
        "Frecuencia con la que se paga al trabajador",
    ),
    "anticipated_number_of_hours": (
        "Number of hours of work anticipated per week",
        "Número de horas de trabalho previstas por semana",
        "Número de horas de trabajo previstas por semana",
    ),
    # ---- worksite ----------------------------------------------------------
    "worksite_address": (
        "First line of the address of the primary worksite",
        "Primeira linha do endereço do local de trabalho principal",
        "Primera línea de la dirección del lugar de trabajo principal",
    ),
    "worksite_city": (
        "City of the primary worksite",
        "Município do local de trabalho principal",
        "Ciudad del lugar de trabajo principal",
    ),
    "worksite_county": (
        "County of the primary worksite",
        "Condado do local de trabalho principal",
        "Condado del lugar de trabajo principal",
    ),
    "worksite_state": (
        "USPS two-letter abbreviation of the state of the primary worksite",
        "Sigla de duas letras do USPS do estado do local de trabalho principal",
        "Sigla de dos letras del USPS del estado del lugar de trabajo principal",
    ),
    "worksite_postal_code": (
        "ZIP code of the primary worksite",
        "Código postal (ZIP) do local de trabalho principal",
        "Código postal (ZIP) del lugar de trabajo principal",
    ),
    "msa_name": (
        "Name of the metropolitan statistical area or OES wage area of the worksite",
        "Nome da área estatística metropolitana ou área salarial OES do local de trabalho",
        "Nombre del área estadística metropolitana o área salarial OES del lugar de trabajo",
    ),
    "worksite_workers": (
        "Number of workers assigned to the primary worksite",
        "Número de trabalhadores alocados no local de trabalho principal",
        "Número de trabajadores asignados al lugar de trabajo principal",
    ),
    "total_worksite_locations": (
        "Total number of worksite locations listed on the application",
        "Número total de locais de trabalho listados na aplicação",
        "Número total de lugares de trabajo listados en la aplicación",
    ),
    "total_worksite_records": (
        "Total number of worksite records attached to the application",
        "Número total de registros de locais de trabalho anexados à solicitação",
        "Número total de registros de lugares de trabajo adjuntos a la solicitud",
    ),
    "is_multiple_worksites": (
        "Whether the job opportunity covers more than one worksite",
        "Indica se a oportunidade de trabalho abrange mais de um local de trabalho",
        "Indica si la oportunidad de trabajo abarca más de un lugar de trabajo",
    ),
    "secondary_entity": (
        "Whether the worker will be placed at a secondary entity rather than at the employer's own premises",
        "Indica se o trabalhador será alocado em uma entidade secundária e não nas instalações do próprio empregador",
        "Indica si el trabajador será asignado a una entidad secundaria y no a las instalaciones del propio empleador",
    ),
    "secondary_entity_business_name": (
        "Business name of the secondary entity where the worker will be placed",
        "Razão social da entidade secundária onde o trabalhador será alocado",
        "Razón social de la entidad secundaria donde será asignado el trabajador",
    ),
    # ---- representation ----------------------------------------------------
    "attorney_law_firm_name": (
        "Business name of the law firm or agency representing the employer",
        "Razão social do escritório de advocacia ou agência que representa o empregador",
        "Razón social del bufete de abogados o agencia que representa al empleador",
    ),
    "agent_representing_employer": (
        "Whether an attorney or agent represents the employer on the application",
        "Indica se um advogado ou agente representa o empregador na solicitação",
        "Indica si un abogado o agente representa al empleador en la solicitud",
    ),
    # ---- LCA-specific ------------------------------------------------------
    "new_employment": (
        "Number of worker positions requested for new employment",
        "Número de posições solicitadas para novo emprego",
        "Número de puestos solicitados para nuevo empleo",
    ),
    "continued_employment": (
        "Number of worker positions requested for continued employment",
        "Número de posições solicitadas para continuidade de emprego",
        "Número de puestos solicitados para continuidad de empleo",
    ),
    "change_previous_employment": (
        "Number of worker positions requested for a change in previously approved employment",
        "Número de posições solicitadas para mudança em emprego previamente aprovado",
        "Número de puestos solicitados para cambio en empleo previamente aprobado",
    ),
    "new_concurrent_employment": (
        "Number of worker positions requested for new concurrent employment",
        "Número de posições solicitadas para novo emprego concomitante",
        "Número de puestos solicitados para nuevo empleo concurrente",
    ),
    "change_employer": (
        "Number of worker positions requested for a change of employer",
        "Número de posições solicitadas para mudança de empregador",
        "Número de puestos solicitados para cambio de empleador",
    ),
    "amended_petition": (
        "Number of worker positions requested on an amended petition",
        "Número de posições solicitadas em petição emendada",
        "Número de puestos solicitados en petición enmendada",
    ),
    "h1b_dependent": (
        "Whether the employer is H-1B dependent under the statutory ratio of H-1B workers to total workforce",
        "Indica se o empregador é dependente de H-1B segundo a razão legal entre trabalhadores H-1B e força de trabalho total",
        "Indica si el empleador es dependiente de H-1B según la razón legal entre trabajadores H-1B y fuerza laboral total",
    ),
    "willful_violator": (
        "Whether the employer has been found to be a willful violator of the H-1B program rules",
        "Indica se o empregador foi considerado infrator deliberado das regras do programa H-1B",
        "Indica si el empleador ha sido considerado infractor deliberado de las reglas del programa H-1B",
    ),
    "support_h1b": (
        "Whether the labor condition application supports an H-1B petition",
        "Indica se a labor condition application dá suporte a uma petição H-1B",
        "Indica si la labor condition application respalda una petición H-1B",
    ),
    "statutory_basis": (
        "Statutory basis the employer claims for exemption from the additional H-1B dependent attestations",
        "Base legal alegada pelo empregador para isenção das atestações adicionais de dependência de H-1B",
        "Base legal alegada por el empleador para la exención de las atestaciones adicionales de dependencia de H-1B",
    ),
    "withdrawn": (
        "Whether the application was withdrawn by the employer, as recorded in the legacy H-1B eFile system",
        "Indica se a solicitação foi retirada pelo empregador, conforme registrado no sistema legado H-1B eFile",
        "Indica si la solicitud fue retirada por el empleador, según lo registrado en el sistema heredado H-1B eFile",
    ),
    # ---- PERM-specific -----------------------------------------------------
    "application_type": (
        "Type of application filed",
        "Tipo de solicitação apresentada",
        "Tipo de solicitud presentada",
    ),
    "refile": (
        "Whether the application is a refiling of a previously submitted case",
        "Indica se a solicitação é um reenvio de caso apresentado anteriormente",
        "Indica si la solicitud es un reenvío de un caso presentado anteriormente",
    ),
    "schedule_a_sheepherder": (
        "Whether the application is filed under the Schedule A sheepherder provision",
        "Indica se a solicitação é apresentada sob a provisão de pastor de ovelhas do Schedule A",
        "Indica si la solicitud se presenta bajo la disposición de pastor de ovejas del Schedule A",
    ),
    "us_economic_sector": (
        "Broad economic sector of the employer as classified on the application",
        "Setor econômico amplo do empregador conforme classificado na solicitação",
        "Sector económico amplio del empleador según su clasificación en la solicitud",
    ),
    "minimum_education": (
        "Minimum level of education required for the job opportunity",
        "Nível mínimo de escolaridade exigido para a oportunidade de trabalho",
        "Nivel mínimo de escolaridad exigido para la oportunidad de trabajo",
    ),
    "major_field_of_study": (
        "Major field of study required for the job opportunity",
        "Área principal de formação exigida para a oportunidade de trabalho",
        "Área principal de formación exigida para la oportunidad de trabajo",
    ),
    "required_experience": (
        "Whether experience in the job offered is required",
        "Indica se é exigida experiência na função oferecida",
        "Indica si se exige experiencia en el puesto ofrecido",
    ),
    "required_experience_months": (
        "Months of experience in the job offered required by the employer",
        "Meses de experiência na função oferecida exigidos pelo empregador",
        "Meses de experiencia en el puesto ofrecido exigidos por el empleador",
    ),
    "country_of_citizenship": (
        "Country of citizenship of the foreign worker named on the application",
        "País de cidadania do trabalhador estrangeiro indicado na solicitação",
        "País de ciudadanía del trabajador extranjero indicado en la solicitud",
    ),
    "foreign_worker_birth_country": (
        "Country of birth of the foreign worker named on the application",
        "País de nascimento do trabalhador estrangeiro indicado na solicitação",
        "País de nacimiento del trabajador extranjero indicado en la solicitud",
    ),
    "class_of_admission": (
        "Non-immigrant visa class the foreign worker holds at the time of filing",
        "Classe de visto de não imigrante que o trabalhador estrangeiro possui no momento do protocolo",
        "Clase de visa de no inmigrante que el trabajador extranjero posee al momento de la presentación",
    ),
    "foreign_worker_education": (
        "Highest level of education completed by the foreign worker",
        "Nível mais alto de escolaridade concluído pelo trabalhador estrangeiro",
        "Nivel más alto de escolaridad completado por el trabajador extranjero",
    ),
    # ---- H-2-specific ------------------------------------------------------
    "type_of_employer_application": (
        "Whether the application is filed by an individual employer, an agricultural association or an agent",
        "Indica se a solicitação é apresentada por empregador individual, associação agrícola ou agente",
        "Indica si la solicitud es presentada por un empleador individual, una asociación agrícola o un agente",
    ),
    "type_of_employer": (
        "Type of employer filing the application",
        "Tipo de empregador que apresenta a solicitação",
        "Tipo de empleador que presenta la solicitud",
    ),
    "h2a_labor_contractor": (
        "Whether the applicant is an H-2A labor contractor",
        "Indica se o solicitante é um contratante de mão de obra H-2A",
        "Indica si el solicitante es un contratista de mano de obra H-2A",
    ),
    "nature_of_temporary_need": (
        "Nature of the employer's temporary need, such as seasonal or peak load",
        "Natureza da necessidade temporária do empregador, como sazonal ou pico de demanda",
        "Naturaleza de la necesidad temporal del empleador, como estacional o pico de demanda",
    ),
    "emergency_filing": (
        "Whether the application was filed as an emergency filing",
        "Indica se a solicitação foi apresentada como protocolo de emergência",
        "Indica si la solicitud se presentó como presentación de emergencia",
    ),
    "primary_crop": (
        "Primary crop or agricultural activity of the job opportunity",
        "Cultura ou atividade agrícola principal da oportunidade de trabalho",
        "Cultivo o actividad agrícola principal de la oportunidad de trabajo",
    ),
    "job_order_number": (
        "Job order number assigned by the state workforce agency",
        "Número da ordem de serviço atribuído pela agência estadual de trabalho",
        "Número de la orden de trabajo asignado por la agencia estatal de trabajo",
    ),
    "job_order_submit_date": (
        "Date the job order was submitted to the state workforce agency",
        "Data em que a ordem de serviço foi enviada à agência estadual de trabalho",
        "Fecha en que la orden de trabajo fue enviada a la agencia estatal de trabajo",
    ),
    "swa_state": (
        "State workforce agency that received the job order",
        "Agência estadual de trabalho que recebeu a ordem de serviço",
        "Agencia estatal de trabajo que recibió la orden de trabajo",
    ),
    "cap_exempt": (
        "Whether the application is exempt from the H-2B statutory cap",
        "Indica se a solicitação é isenta do teto legal do H-2B",
        "Indica si la solicitud está exenta del tope legal del H-2B",
    ),
    "education_level": (
        "Minimum level of education required for the job opportunity",
        "Nível mínimo de escolaridade exigido para a oportunidade de trabalho",
        "Nivel mínimo de escolaridad exigido para la oportunidad de trabajo",
    ),
    "work_experience_months": (
        "Months of work experience required for the job opportunity",
        "Meses de experiência de trabalho exigidos para a oportunidade de trabalho",
        "Meses de experiencia laboral exigidos para la oportunidad de trabajo",
    ),
    "housing_city": (
        "City of the housing the employer provides to the workers",
        "Município da moradia que o empregador fornece aos trabalhadores",
        "Ciudad de la vivienda que el empleador provee a los trabajadores",
    ),
    "housing_state": (
        "USPS two-letter abbreviation of the state of the housing the employer provides to the workers",
        "Sigla de duas letras do USPS do estado da moradia que o empregador fornece aos trabalhadores",
        "Sigla de dos letras del USPS del estado de la vivienda que el empleador provee a los trabajadores",
    ),
    "housing_type": (
        "Type of housing the employer provides to the workers",
        "Tipo de moradia que o empregador fornece aos trabalhadores",
        "Tipo de vivienda que el empleador provee a los trabajadores",
    ),
    "housing_total_occupancy": (
        "Total number of workers the provided housing can accommodate",
        "Número total de trabalhadores que a moradia fornecida pode acomodar",
        "Número total de trabajadores que la vivienda provista puede alojar",
    ),
    "meals_provided": (
        "Whether the employer provides meals to the workers",
        "Indica se o empregador fornece refeições aos trabalhadores",
        "Indica si el empleador provee comidas a los trabajadores",
    ),
    # ---- provenance --------------------------------------------------------
    "source_file": (
        "Name of the Department of Labor disclosure file the row was read from",
        "Nome do arquivo de divulgação do Departamento do Trabalho de onde a linha foi lida",
        "Nombre del archivo de divulgación del Departamento de Trabajo del que se leyó la fila",
    ),
}

# Per-program overrides where the shared wording would be wrong.
D["visa_class:h2a"] = (
    "Visa classification of the certification, H-2A for temporary agricultural work",
    "Classificação de visto da certificação, H-2A para trabalho agrícola temporário",
    "Clasificación de visa de la certificación, H-2A para trabajo agrícola temporal",
)
D["visa_class:h2b"] = (
    "Visa classification of the certification, H-2B for temporary non-agricultural work",
    "Classificação de visto da certificação, H-2B para trabalho não agrícola temporário",
    "Clasificación de visa de la certificación, H-2B para trabajo no agrícola temporal",
)
D["case_number:perm"] = (
    "Case number assigned by the Office of Foreign Labor Certification to the permanent labor certification application, unique within the fiscal year",
    "Número do caso atribuído pelo Office of Foreign Labor Certification à solicitação de certificação laboral permanente, único dentro do ano fiscal",
    "Número de caso asignado por la Office of Foreign Labor Certification a la solicitud de certificación laboral permanente, único dentro del año fiscal",
)
D["application_type:h2b"] = (
    "Type of temporary labor certification application filed",
    "Tipo de solicitação de certificação laboral temporária apresentada",
    "Tipo de solicitud de certificación laboral temporal presentada",
)


def get(name: str, program: str) -> tuple[str, str, str]:
    return D.get(f"{name}:{program}") or D[name]
