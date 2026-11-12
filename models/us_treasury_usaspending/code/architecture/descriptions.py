"""Trilingual descriptions for the non-flag columns of us_treasury_usaspending.

``CODE_PAIRS`` covers the elements the source ships as a code column plus a
label column. Each entry is keyed by the code column and gives the label
column together with the concept as a noun phrase in the three languages; the
builder turns those into "Código de X" / "Descrição de X".

``DESCRIPTIONS`` covers everything else, one entry per column, as
``(pt, en, es)``. Wording follows the USAspending / DATA Act element
dictionary. No description ends in a period, per the Data Basis style manual.
"""

# code column -> (label column(s), pt concept, en concept, es concept)
CODE_PAIRS = {
    "foreign_funding": (
        ["foreign_funding_description"],
        "uso de recursos de origem estrangeira no contrato",
        "use of foreign funding in the contract",
        "uso de recursos de origen extranjero en el contrato",
    ),
    "sam_exception": (
        ["sam_exception_description"],
        "exceção que dispensa o fornecedor de registro no SAM",
        "exception releasing the vendor from SAM registration",
        "excepción que exime al proveedor del registro en SAM",
    ),
    "award_type_code": (
        ["award_type"],
        "tipo de contrato firmado",
        "type of contract awarded",
        "tipo de contrato firmado",
    ),
    "idv_type_code": (
        ["idv_type"],
        "tipo de veículo de entrega indefinida (IDV)",
        "type of indefinite delivery vehicle (IDV)",
        "tipo de vehículo de entrega indefinida (IDV)",
    ),
    "multiple_or_single_award_idv_code": (
        ["multiple_or_single_award_idv"],
        "indicação de que o IDV admite um ou múltiplos contratados",
        "whether the IDV allows a single or multiple awardees",
        "indicación de si el IDV admite uno o varios contratistas",
    ),
    "type_of_idc_code": (
        ["type_of_idc"],
        "tipo de contrato de entrega indefinida (IDC)",
        "type of indefinite delivery contract (IDC)",
        "tipo de contrato de entrega indefinida (IDC)",
    ),
    "type_of_contract_pricing_code": (
        ["type_of_contract_pricing"],
        "regime de preços do contrato",
        "contract pricing arrangement",
        "régimen de precios del contrato",
    ),
    "action_type_code": (
        ["action_type", "action_type_description"],
        "tipo de ação registrada na transação",
        "type of action recorded by the transaction",
        "tipo de acción registrada en la transacción",
    ),
    "inherently_governmental_functions": (
        ["inherently_governmental_functions_description"],
        "classificação do serviço quanto a funções inerentemente governamentais",
        "classification of the service with respect to inherently governmental functions",
        "clasificación del servicio respecto a funciones inherentemente gubernamentales",
    ),
    "product_or_service_code": (
        ["product_or_service_code_description"],
        "produto ou serviço adquirido, segundo o PSC Manual",
        "product or service purchased, per the PSC Manual",
        "producto o servicio adquirido, según el PSC Manual",
    ),
    "contract_bundling_code": (
        ["contract_bundling"],
        "agrupamento (bundling) de requisitos no contrato",
        "bundling of requirements in the contract",
        "agrupamiento (bundling) de requisitos en el contrato",
    ),
    "dod_claimant_program_code": (
        ["dod_claimant_program_description"],
        "programa reivindicante do Departamento de Defesa",
        "Department of Defense claimant program",
        "programa reclamante del Departamento de Defensa",
    ),
    "naics_code": (
        ["naics_description"],
        "atividade econômica do contratado na classificação NAICS",
        "economic activity of the contractor under the NAICS classification",
        "actividad económica del contratista en la clasificación NAICS",
    ),
    "recovered_materials_sustainability_code": (
        ["recovered_materials_sustainability"],
        "cláusulas de materiais recuperados e sustentabilidade aplicadas",
        "recovered materials and sustainability clauses applied",
        "cláusulas de materiales recuperados y sostenibilidad aplicadas",
    ),
    "domestic_or_foreign_entity_code": (
        ["domestic_or_foreign_entity"],
        "natureza doméstica ou estrangeira do contratado",
        "domestic or foreign status of the contractor",
        "naturaleza nacional o extranjera del contratista",
    ),
    "dod_acquisition_program_code": (
        ["dod_acquisition_program_description"],
        "programa de aquisição do Departamento de Defesa",
        "Department of Defense acquisition program",
        "programa de adquisición del Departamento de Defensa",
    ),
    "information_technology_commercial_item_category_code": (
        ["information_technology_commercial_item_category"],
        "categoria comercial do item de tecnologia da informação adquirido",
        "commercial category of the information technology item purchased",
        "categoría comercial del artículo de tecnología de la información adquirido",
    ),
    "epa_designated_product_code": (
        ["epa_designated_product"],
        "atendimento aos requisitos de produtos designados pela EPA",
        "compliance with EPA designated product requirements",
        "cumplimiento de los requisitos de productos designados por la EPA",
    ),
    "country_of_product_or_service_origin_code": (
        ["country_of_product_or_service_origin"],
        "país de origem do produto ou serviço adquirido",
        "country of origin of the product or service purchased",
        "país de origen del producto o servicio adquirido",
    ),
    "place_of_manufacture_code": (
        ["place_of_manufacture"],
        "local de fabricação do produto ou de prestação do serviço",
        "place where the product was manufactured or the service performed",
        "lugar de fabricación del producto o de prestación del servicio",
    ),
    "subcontracting_plan_code": (
        ["subcontracting_plan"],
        "exigência de plano de subcontratação",
        "subcontracting plan requirement",
        "exigencia de plan de subcontratación",
    ),
    "extent_competed_code": (
        ["extent_competed"],
        "grau de competição na contratação",
        "extent to which the award was competed",
        "grado de competencia en la contratación",
    ),
    "solicitation_procedures_code": (
        ["solicitation_procedures"],
        "procedimento de solicitação de propostas utilizado",
        "solicitation procedure used",
        "procedimiento de solicitud de propuestas utilizado",
    ),
    "type_of_set_aside_code": (
        ["type_of_set_aside"],
        "tipo de reserva de mercado (set-aside) aplicada",
        "type of set-aside applied",
        "tipo de reserva de mercado (set-aside) aplicada",
    ),
    "evaluated_preference_code": (
        ["evaluated_preference"],
        "preferência avaliada na comparação de propostas",
        "preference evaluated when comparing offers",
        "preferencia evaluada en la comparación de propuestas",
    ),
    "research_code": (
        ["research"],
        "enquadramento da ação em programa de pesquisa para pequenas empresas",
        "classification of the action under a small business research program",
        "clasificación de la acción en un programa de investigación para pequeñas empresas",
    ),
    "fair_opportunity_limited_sources_code": (
        ["fair_opportunity_limited_sources"],
        "justificativa para limitar fontes sob a regra de oportunidade justa",
        "justification for limiting sources under the fair opportunity rule",
        "justificación para limitar fuentes bajo la regla de oportunidad justa",
    ),
    "other_than_full_and_open_competition_code": (
        ["other_than_full_and_open_competition"],
        "fundamento legal para dispensa de competição ampla e aberta",
        "statutory authority for other than full and open competition",
        "fundamento legal para prescindir de competencia amplia y abierta",
    ),
    "commercial_item_acquisition_procedures_code": (
        ["commercial_item_acquisition_procedures"],
        "procedimento de aquisição de item comercial aplicado",
        "commercial item acquisition procedure applied",
        "procedimiento de adquisición de artículo comercial aplicado",
    ),
    "simplified_procedures_for_certain_commercial_items_code": (
        ["simplified_procedures_for_certain_commercial_items"],
        "uso de procedimentos simplificados para certos itens comerciais",
        "use of simplified procedures for certain commercial items",
        "uso de procedimientos simplificados para ciertos artículos comerciales",
    ),
    "a76_fair_act_action_code": (
        ["a76_fair_act_action"],
        "vínculo da ação a estudo de terceirização A-76 (FAIR Act)",
        "link between the action and an A-76 (FAIR Act) competition",
        "vínculo de la acción con un estudio de tercerización A-76 (FAIR Act)",
    ),
    "fed_biz_opps_code": (
        ["fed_biz_opps"],
        "publicação da oportunidade no FedBizOpps",
        "publication of the opportunity on FedBizOpps",
        "publicación de la oportunidad en FedBizOpps",
    ),
    "local_area_set_aside_code": (
        ["local_area_set_aside"],
        "reserva de mercado para empresas da área atingida por desastre",
        "set-aside for firms in a disaster-affected local area",
        "reserva de mercado para empresas del área afectada por un desastre",
    ),
    "clinger_cohen_act_planning_code": (
        ["clinger_cohen_act_planning"],
        "conformidade da aquisição de TI com o Clinger-Cohen Act",
        "compliance of the IT acquisition with the Clinger-Cohen Act",
        "conformidad de la adquisición de TI con la Clinger-Cohen Act",
    ),
    "materials_supplies_articles_equipment_code": (
        ["materials_supplies_articles_equipment"],
        "aplicação da cláusula Walsh-Healey de materiais e equipamentos",
        "applicability of the Walsh-Healey materials and equipment clause",
        "aplicación de la cláusula Walsh-Healey de materiales y equipos",
    ),
    "labor_standards_code": (
        ["labor_standards"],
        "aplicação das normas trabalhistas do Service Contract Act",
        "applicability of Service Contract Act labor standards",
        "aplicación de las normas laborales del Service Contract Act",
    ),
    "construction_wage_rate_requirements_code": (
        ["construction_wage_rate_requirements"],
        "aplicação dos pisos salariais de construção do Davis-Bacon Act",
        "applicability of Davis-Bacon Act construction wage rates",
        "aplicación de los salarios mínimos de construcción del Davis-Bacon Act",
    ),
    "interagency_contracting_authority_code": (
        ["interagency_contracting_authority"],
        "fundamento legal da contratação interagências",
        "statutory authority for interagency contracting",
        "fundamento legal de la contratación interagencial",
    ),
    "parent_award_type_code": (
        ["parent_award_type"],
        "tipo do contrato-mãe (IDV) ao qual a ação se vincula",
        "type of the parent award (IDV) the action belongs to",
        "tipo del contrato matriz (IDV) al que se vincula la acción",
    ),
    "parent_award_single_or_multiple_code": (
        ["parent_award_single_or_multiple"],
        "indicação de contratado único ou múltiplo no contrato-mãe",
        "whether the parent award has a single or multiple awardees",
        "indicación de contratista único o múltiple en el contrato matriz",
    ),
    "national_interest_action_code": (
        ["national_interest_action"],
        "evento de interesse nacional associado à ação",
        "national interest event associated with the action",
        "evento de interés nacional asociado a la acción",
    ),
    "cost_or_pricing_data_code": (
        ["cost_or_pricing_data"],
        "obtenção de dados de custo ou preço do fornecedor",
        "whether cost or pricing data were obtained from the vendor",
        "obtención de datos de costo o precio del proveedor",
    ),
    "cost_accounting_standards_clause_code": (
        ["cost_accounting_standards_clause"],
        "inclusão da cláusula de normas de contabilidade de custos (CAS)",
        "inclusion of the Cost Accounting Standards (CAS) clause",
        "inclusión de la cláusula de normas de contabilidad de costos (CAS)",
    ),
    "government_furnished_property_code": (
        ["government_furnished_property"],
        "uso de bens ou equipamentos fornecidos pelo governo",
        "use of government furnished property or equipment",
        "uso de bienes o equipos suministrados por el gobierno",
    ),
    "sea_transportation_code": (
        ["sea_transportation"],
        "previsão de transporte marítimo na execução do contrato",
        "whether the contract involves sea transportation",
        "previsión de transporte marítimo en la ejecución del contrato",
    ),
    "undefinitized_action_code": (
        ["undefinitized_action"],
        "situação da ação quanto à definição de termos e preço",
        "status of the action with respect to definitized terms and price",
        "situación de la acción respecto a la definición de términos y precio",
    ),
    "consolidated_contract_code": (
        ["consolidated_contract"],
        "consolidação de requisitos anteriormente contratados em separado",
        "consolidation of requirements previously awarded separately",
        "consolidación de requisitos anteriormente contratados por separado",
    ),
    "performance_based_service_acquisition_code": (
        ["performance_based_service_acquisition"],
        "uso de aquisição de serviços baseada em desempenho",
        "use of performance-based service acquisition",
        "uso de adquisición de servicios basada en desempeño",
    ),
    "multi_year_contract_code": (
        ["multi_year_contract"],
        "enquadramento como contrato plurianual",
        "whether the contract is a multi-year contract",
        "clasificación como contrato plurianual",
    ),
    "contract_financing_code": (
        ["contract_financing"],
        "modalidade de financiamento do contrato",
        "contract financing arrangement",
        "modalidad de financiamiento del contrato",
    ),
    "purchase_card_as_payment_method_code": (
        ["purchase_card_as_payment_method"],
        "uso do cartão de compras governamental como meio de pagamento",
        "use of the government purchase card as the payment method",
        "uso de la tarjeta de compras gubernamental como medio de pago",
    ),
    "contingency_humanitarian_or_peacekeeping_operation_code": (
        ["contingency_humanitarian_or_peacekeeping_operation"],
        "vínculo da ação a operação de contingência, humanitária ou de paz",
        "link between the action and a contingency, humanitarian or peacekeeping operation",
        "vínculo de la acción con una operación de contingencia, humanitaria o de paz",
    ),
    "contracting_officers_determination_of_business_size_code": (
        ["contracting_officers_determination_of_business_size"],
        "porte da empresa conforme determinação do agente de contratação",
        "business size as determined by the contracting officer",
        "tamaño de la empresa según la determinación del agente de contratación",
    ),
    "assistance_type_code": (
        ["assistance_type_description"],
        "tipo de assistência financeira concedida",
        "type of financial assistance awarded",
        "tipo de asistencia financiera concedida",
    ),
    "business_funds_indicator_code": (
        ["business_funds_indicator_description"],
        "origem dos recursos quanto ao Recovery Act",
        "whether the funds come from the Recovery Act",
        "origen de los recursos respecto a la Recovery Act",
    ),
    "business_types_code": (
        ["business_types_description"],
        "tipo de organização do beneficiário da assistência",
        "organization type of the assistance recipient",
        "tipo de organización del beneficiario de la asistencia",
    ),
    "correction_delete_indicator_code": (
        ["correction_delete_indicator_description"],
        "situação do registro quanto a correção ou exclusão",
        "status of the record with respect to correction or deletion",
        "situación del registro respecto a corrección o eliminación",
    ),
    "record_type_code": (
        ["record_type_description"],
        "natureza agregada ou individual do registro",
        "whether the record is aggregate or individual",
        "naturaleza agregada o individual del registro",
    ),
}

# One entry per remaining column: (pt, en, es)
DESCRIPTIONS = {
    # ---- partition ---------------------------------------------------------
    "fiscal_year": (
        "Ano fiscal federal norte-americano da data da ação, iniciado em 1º de outubro do ano civil anterior",
        "United States federal fiscal year of the action date, beginning on October 1 of the previous calendar year",
        "Año fiscal federal estadounidense de la fecha de la acción, iniciado el 1 de octubre del año civil anterior",
    ),
    # ---- keys and identifiers ---------------------------------------------
    "contract_transaction_unique_key": (
        "Chave única da transação de contrato, gerada pelo USAspending",
        "Unique key of the contract transaction, generated by USAspending",
        "Clave única de la transacción de contrato, generada por USAspending",
    ),
    "contract_award_unique_key": (
        "Chave única do contrato ao qual a transação pertence",
        "Unique key of the contract award the transaction belongs to",
        "Clave única del contrato al que pertenece la transacción",
    ),
    "assistance_transaction_unique_key": (
        "Chave única da transação de assistência financeira, gerada pelo USAspending",
        "Unique key of the assistance transaction, generated by USAspending",
        "Clave única de la transacción de asistencia financiera, generada por USAspending",
    ),
    "assistance_award_unique_key": (
        "Chave única do auxílio ao qual a transação pertence",
        "Unique key of the assistance award the transaction belongs to",
        "Clave única de la ayuda a la que pertenece la transacción",
    ),
    "award_id_piid": (
        "Identificador do instrumento de compra (PIID) atribuído pela agência",
        "Procurement instrument identifier (PIID) assigned by the agency",
        "Identificador del instrumento de compra (PIID) asignado por la agencia",
    ),
    "award_id_fain": (
        "Número de identificação federal do auxílio (FAIN)",
        "Federal award identification number (FAIN)",
        "Número de identificación federal de la ayuda (FAIN)",
    ),
    "award_id_uri": (
        "Identificador único do registro (URI) para auxílios sem FAIN",
        "Unique record identifier (URI) for assistance awards without a FAIN",
        "Identificador único del registro (URI) para ayudas sin FAIN",
    ),
    "sai_number": (
        "Número atribuído pelo processo estadual de revisão prévia (SAI)",
        "Number assigned by the state single point of contact review process (SAI)",
        "Número asignado por el proceso estatal de revisión previa (SAI)",
    ),
    "modification_number": (
        "Número do aditivo que identifica esta modificação do contrato",
        "Number of the modification that identifies this change to the award",
        "Número de la modificación que identifica este cambio del contrato",
    ),
    "transaction_number": (
        "Número que distingue transações registradas com o mesmo PIID e aditivo",
        "Number distinguishing transactions filed with the same PIID and modification",
        "Número que distingue transacciones registradas con el mismo PIID y modificación",
    ),
    "parent_award_agency_id": (
        "Código da agência que firmou o contrato-mãe (IDV)",
        "Code of the agency that awarded the parent award (IDV)",
        "Código de la agencia que firmó el contrato matriz (IDV)",
    ),
    "parent_award_agency_name": (
        "Nome da agência que firmou o contrato-mãe (IDV)",
        "Name of the agency that awarded the parent award (IDV)",
        "Nombre de la agencia que firmó el contrato matriz (IDV)",
    ),
    "parent_award_id_piid": (
        "Identificador (PIID) do contrato-mãe ao qual a ação se vincula",
        "Identifier (PIID) of the parent award the action belongs to",
        "Identificador (PIID) del contrato matriz al que se vincula la acción",
    ),
    "parent_award_modification_number": (
        "Número do aditivo do contrato-mãe",
        "Modification number of the parent award",
        "Número de la modificación del contrato matriz",
    ),
    "solicitation_identifier": (
        "Identificador do edital que originou a contratação",
        "Identifier of the solicitation that originated the award",
        "Identificador de la convocatoria que originó la contratación",
    ),
    "usaspending_permalink": (
        "Endereço permanente da página do auxílio ou contrato no USAspending.gov",
        "Permanent link to the award page on USAspending.gov",
        "Dirección permanente de la página de la ayuda o contrato en USAspending.gov",
    ),
    # ---- amounts -----------------------------------------------------------
    "federal_action_obligation": (
        "Valor empenhado ou desempenhado pelo governo federal nesta transação",
        "Amount obligated or de-obligated by the federal government in this transaction",
        "Monto comprometido o descomprometido por el gobierno federal en esta transacción",
    ),
    "total_dollars_obligated": (
        "Valor total empenhado no contrato, somadas todas as transações",
        "Total amount obligated on the award across all transactions",
        "Monto total comprometido en el contrato, sumadas todas las transacciones",
    ),
    "total_obligated_amount": (
        "Valor total empenhado no auxílio, somadas todas as transações",
        "Total amount obligated on the assistance award across all transactions",
        "Monto total comprometido en la ayuda, sumadas todas las transacciones",
    ),
    "total_outlayed_amount_for_overall_award": (
        "Valor total desembolsado no auxílio ou contrato até a data",
        "Total amount outlayed on the award to date",
        "Monto total desembolsado en la ayuda o contrato hasta la fecha",
    ),
    "base_and_exercised_options_value": (
        "Valor do contrato considerando a base e as opções já exercidas nesta transação",
        "Award value covering the base and the options exercised in this transaction",
        "Valor del contrato considerando la base y las opciones ya ejercidas en esta transacción",
    ),
    "current_total_value_of_award": (
        "Valor total corrente do contrato, incluindo as opções já exercidas",
        "Current total value of the award, including exercised options",
        "Valor total actual del contrato, incluidas las opciones ya ejercidas",
    ),
    "base_and_all_options_value": (
        "Valor do contrato considerando a base e todas as opções previstas nesta transação",
        "Award value covering the base and all options contemplated in this transaction",
        "Valor del contrato considerando la base y todas las opciones previstas en esta transacción",
    ),
    "potential_total_value_of_award": (
        "Valor total potencial do contrato caso todas as opções sejam exercidas",
        "Potential total value of the award if all options are exercised",
        "Valor total potencial del contrato si se ejercen todas las opciones",
    ),
    "indirect_cost_federal_share_amount": (
        "Parcela federal dos custos indiretos do auxílio",
        "Federal share of the indirect costs of the assistance award",
        "Parte federal de los costos indirectos de la ayuda",
    ),
    "non_federal_funding_amount": (
        "Valor de contrapartida não federal aportado nesta transação",
        "Non-federal funding contributed in this transaction",
        "Monto de contrapartida no federal aportado en esta transacción",
    ),
    "total_non_federal_funding_amount": (
        "Valor total de contrapartida não federal do auxílio",
        "Total non-federal funding of the assistance award",
        "Monto total de contrapartida no federal de la ayuda",
    ),
    "face_value_of_loan": (
        "Valor de face do empréstimo concedido nesta transação",
        "Face value of the loan awarded in this transaction",
        "Valor nominal del préstamo concedido en esta transacción",
    ),
    "total_face_value_of_loan": (
        "Valor de face total do empréstimo, somadas todas as transações",
        "Total face value of the loan across all transactions",
        "Valor nominal total del préstamo, sumadas todas las transacciones",
    ),
    "original_loan_subsidy_cost": (
        "Custo de subsídio do empréstimo estimado nesta transação",
        "Subsidy cost of the loan estimated in this transaction",
        "Costo de subsidio del préstamo estimado en esta transacción",
    ),
    "total_loan_subsidy_cost": (
        "Custo de subsídio total do empréstimo, somadas todas as transações",
        "Total subsidy cost of the loan across all transactions",
        "Costo de subsidio total del préstamo, sumadas todas las transacciones",
    ),
    "generated_pragmatic_obligations": (
        "Valor comparável entre modalidades, igual ao subsídio nos empréstimos e ao empenho nos demais auxílios",
        "Cross-comparable amount, equal to the subsidy cost for loans and to the obligation for other assistance",
        "Monto comparable entre modalidades, igual al subsidio en los préstamos y al compromiso en las demás ayudas",
    ),
    "outlayed_amount_from_COVID-19_supplementals_for_overall_award": (
        "Valor desembolsado no auxílio ou contrato com recursos suplementares de resposta à COVID-19",
        "Amount outlayed on the award from COVID-19 supplemental appropriations",
        "Monto desembolsado en la ayuda o contrato con recursos suplementarios de respuesta a la COVID-19",
    ),
    "obligated_amount_from_COVID-19_supplementals_for_overall_award": (
        "Valor empenhado no auxílio ou contrato com recursos suplementares de resposta à COVID-19",
        "Amount obligated on the award from COVID-19 supplemental appropriations",
        "Monto comprometido en la ayuda o contrato con recursos suplementarios de respuesta a la COVID-19",
    ),
    "outlayed_amount_from_IIJA_supplemental_for_overall_award": (
        "Valor desembolsado no auxílio ou contrato com recursos suplementares da lei de infraestrutura (IIJA)",
        "Amount outlayed on the award from the Infrastructure Investment and Jobs Act (IIJA) supplemental",
        "Monto desembolsado en la ayuda o contrato con recursos suplementarios de la ley de infraestructura (IIJA)",
    ),
    "obligated_amount_from_IIJA_supplemental_for_overall_award": (
        "Valor empenhado no auxílio ou contrato com recursos suplementares da lei de infraestrutura (IIJA)",
        "Amount obligated on the award from the Infrastructure Investment and Jobs Act (IIJA) supplemental",
        "Monto comprometido en la ayuda o contrato con recursos suplementarios de la ley de infraestructura (IIJA)",
    ),
    "disaster_emergency_fund_codes_for_observed": (
        "",
        "",
        "",
    ),  # placeholder, unused
    "disaster_emergency_fund_codes_for_overall_award": (
        "Códigos de fundos de emergência e desastre que financiam o auxílio ou contrato, separados por ponto e vírgula",
        "Disaster and emergency fund codes financing the award, semicolon separated",
        "Códigos de fondos de emergencia y desastre que financian la ayuda o contrato, separados por punto y coma",
    ),
    # ---- dates -------------------------------------------------------------
    "action_date": (
        "Data em que a ação foi assinada ou passou a vigorar",
        "Date the action was signed or became effective",
        "Fecha en que la acción fue firmada o entró en vigor",
    ),
    "period_of_performance_start_date": (
        "Data de início do período de execução",
        "Start date of the period of performance",
        "Fecha de inicio del período de ejecución",
    ),
    "period_of_performance_current_end_date": (
        "Data de término corrente do período de execução",
        "Current end date of the period of performance",
        "Fecha de término actual del período de ejecución",
    ),
    "period_of_performance_potential_end_date": (
        "Data de término do período de execução caso todas as opções sejam exercidas",
        "End date of the period of performance if all options are exercised",
        "Fecha de término del período de ejecución si se ejercen todas las opciones",
    ),
    "ordering_period_end_date": (
        "Data-limite para emissão de novos pedidos sob o contrato-mãe",
        "Last date on which new orders may be placed under the parent award",
        "Fecha límite para emitir nuevos pedidos bajo el contrato matriz",
    ),
    "solicitation_date": (
        "Data de publicação do edital",
        "Date the solicitation was issued",
        "Fecha de publicación de la convocatoria",
    ),
    "initial_report_date": (
        "Data e hora do primeiro envio do registro ao sistema de origem",
        "Date and time the record was first submitted to the source system",
        "Fecha y hora del primer envío del registro al sistema de origen",
    ),
    "last_modified_date": (
        "Data e hora da última modificação do registro no sistema de origem",
        "Date and time the record was last modified in the source system",
        "Fecha y hora de la última modificación del registro en el sistema de origen",
    ),
    # ---- agencies and accounts --------------------------------------------
    "awarding_agency_code": (
        "Código CGAC da agência que concedeu o auxílio ou contrato",
        "CGAC code of the agency that made the award",
        "Código CGAC de la agencia que concedió la ayuda o contrato",
    ),
    "awarding_agency_name": (
        "Nome da agência que concedeu o auxílio ou contrato",
        "Name of the agency that made the award",
        "Nombre de la agencia que concedió la ayuda o contrato",
    ),
    "awarding_sub_agency_code": (
        "Código da subagência que concedeu o auxílio ou contrato",
        "Code of the sub-agency that made the award",
        "Código de la subagencia que concedió la ayuda o contrato",
    ),
    "awarding_sub_agency_name": (
        "Nome da subagência que concedeu o auxílio ou contrato",
        "Name of the sub-agency that made the award",
        "Nombre de la subagencia que concedió la ayuda o contrato",
    ),
    "awarding_office_code": (
        "Código do escritório que concedeu o auxílio ou contrato",
        "Code of the office that made the award",
        "Código de la oficina que concedió la ayuda o contrato",
    ),
    "awarding_office_name": (
        "Nome do escritório que concedeu o auxílio ou contrato",
        "Name of the office that made the award",
        "Nombre de la oficina que concedió la ayuda o contrato",
    ),
    "funding_agency_code": (
        "Código CGAC da agência que forneceu os recursos",
        "CGAC code of the agency that provided the funds",
        "Código CGAC de la agencia que aportó los recursos",
    ),
    "funding_agency_name": (
        "Nome da agência que forneceu os recursos",
        "Name of the agency that provided the funds",
        "Nombre de la agencia que aportó los recursos",
    ),
    "funding_sub_agency_code": (
        "Código da subagência que forneceu os recursos",
        "Code of the sub-agency that provided the funds",
        "Código de la subagencia que aportó los recursos",
    ),
    "funding_sub_agency_name": (
        "Nome da subagência que forneceu os recursos",
        "Name of the sub-agency that provided the funds",
        "Nombre de la subagencia que aportó los recursos",
    ),
    "funding_office_code": (
        "Código do escritório que forneceu os recursos",
        "Code of the office that provided the funds",
        "Código de la oficina que aportó los recursos",
    ),
    "funding_office_name": (
        "Nome do escritório que forneceu os recursos",
        "Name of the office that provided the funds",
        "Nombre de la oficina que aportó los recursos",
    ),
    "treasury_accounts_funding_this_award": (
        "Contas do Tesouro (TAS) que financiam o auxílio ou contrato, separadas por ponto e vírgula",
        "Treasury accounts (TAS) funding the award, semicolon separated",
        "Cuentas del Tesoro (TAS) que financian la ayuda o contrato, separadas por punto y coma",
    ),
    "federal_accounts_funding_this_award": (
        "Contas federais que financiam o auxílio ou contrato, separadas por ponto e vírgula",
        "Federal accounts funding the award, semicolon separated",
        "Cuentas federales que financian la ayuda o contrato, separadas por punto y coma",
    ),
    "object_classes_funding_this_award": (
        "Classes de objeto de despesa que financiam o auxílio ou contrato, separadas por ponto e vírgula",
        "Object classes funding the award, semicolon separated",
        "Clases de objeto de gasto que financian la ayuda o contrato, separadas por punto y coma",
    ),
    "program_activities_funding_this_award": (
        "Atividades programáticas que financiam o auxílio ou contrato, separadas por ponto e vírgula",
        "Program activities funding the award, semicolon separated",
        "Actividades programáticas que financian la ayuda o contrato, separadas por punto y coma",
    ),
    "cfda_number": (
        "Número do programa federal de assistência no catálogo CFDA (Assistance Listings)",
        "Number of the federal assistance program in the CFDA catalog (Assistance Listings)",
        "Número del programa federal de asistencia en el catálogo CFDA (Assistance Listings)",
    ),
    "cfda_title": (
        "Título do programa federal de assistência no catálogo CFDA (Assistance Listings)",
        "Title of the federal assistance program in the CFDA catalog (Assistance Listings)",
        "Título del programa federal de asistencia en el catálogo CFDA (Assistance Listings)",
    ),
    "funding_opportunity_number": (
        "Número do edital de chamamento que originou o auxílio",
        "Number of the funding opportunity announcement that originated the award",
        "Número de la convocatoria que originó la ayuda",
    ),
    "funding_opportunity_goals_text": (
        "Texto descritivo dos objetivos do edital de chamamento",
        "Text describing the goals of the funding opportunity announcement",
        "Texto descriptivo de los objetivos de la convocatoria",
    ),
    "program_acronym": (
        "Sigla do programa ao qual a contratação se vincula",
        "Acronym of the program the award belongs to",
        "Sigla del programa al que se vincula la contratación",
    ),
    "major_program": (
        "Nome do programa principal ao qual a contratação se vincula",
        "Name of the major program the award belongs to",
        "Nombre del programa principal al que se vincula la contratación",
    ),
    "other_statutory_authority": (
        "Descrição de outra autoridade estatutária que fundamenta a contratação",
        "Description of other statutory authority supporting the award",
        "Descripción de otra autoridad estatutaria que fundamenta la contratación",
    ),
    # ---- recipient ---------------------------------------------------------
    "recipient_uei": (
        "Identificador único de entidade (UEI) do beneficiário",
        "Unique entity identifier (UEI) of the recipient",
        "Identificador único de entidad (UEI) del beneficiario",
    ),
    "recipient_duns": (
        "Número DUNS do beneficiário, identificador usado antes da adoção do UEI",
        "DUNS number of the recipient, the identifier used before UEI adoption",
        "Número DUNS del beneficiario, identificador usado antes de la adopción del UEI",
    ),
    "recipient_name": (
        "Nome do beneficiário, padronizado a partir do registro no SAM",
        "Name of the recipient, standardized from its SAM registration",
        "Nombre del beneficiario, estandarizado a partir de su registro en SAM",
    ),
    "recipient_name_raw": (
        "Nome do beneficiário exatamente como informado pela agência",
        "Name of the recipient exactly as reported by the agency",
        "Nombre del beneficiario exactamente como fue informado por la agencia",
    ),
    "recipient_doing_business_as_name": (
        "Nome fantasia do beneficiário",
        "Doing-business-as name of the recipient",
        "Nombre comercial del beneficiario",
    ),
    "cage_code": (
        "Código CAGE do beneficiário, atribuído pelo Departamento de Defesa",
        "CAGE code of the recipient, assigned by the Department of Defense",
        "Código CAGE del beneficiario, asignado por el Departamento de Defensa",
    ),
    "recipient_parent_uei": (
        "Identificador único de entidade (UEI) da controladora do beneficiário",
        "Unique entity identifier (UEI) of the recipient's parent entity",
        "Identificador único de entidad (UEI) de la controladora del beneficiario",
    ),
    "recipient_parent_duns": (
        "Número DUNS da controladora do beneficiário",
        "DUNS number of the recipient's parent entity",
        "Número DUNS de la controladora del beneficiario",
    ),
    "recipient_parent_name": (
        "Nome da controladora do beneficiário, padronizado a partir do registro no SAM",
        "Name of the recipient's parent entity, standardized from its SAM registration",
        "Nombre de la controladora del beneficiario, estandarizado a partir de su registro en SAM",
    ),
    "recipient_parent_name_raw": (
        "Nome da controladora do beneficiário exatamente como informado pela agência",
        "Name of the recipient's parent entity exactly as reported by the agency",
        "Nombre de la controladora del beneficiario exactamente como fue informado por la agencia",
    ),
    "recipient_country_code": (
        "Código do país do endereço do beneficiário",
        "Country code of the recipient address",
        "Código del país de la dirección del beneficiario",
    ),
    "recipient_country_name": (
        "Nome do país do endereço do beneficiário",
        "Country name of the recipient address",
        "Nombre del país de la dirección del beneficiario",
    ),
    "recipient_address_line_1": (
        "Primeira linha do endereço do beneficiário",
        "First line of the recipient address",
        "Primera línea de la dirección del beneficiario",
    ),
    "recipient_address_line_2": (
        "Segunda linha do endereço do beneficiário",
        "Second line of the recipient address",
        "Segunda línea de la dirección del beneficiario",
    ),
    "recipient_city_code": (
        "Código GNIS da cidade do beneficiário",
        "GNIS code of the recipient city",
        "Código GNIS de la ciudad del beneficiario",
    ),
    "recipient_city_name": (
        "Nome da cidade do beneficiário",
        "Name of the recipient city",
        "Nombre de la ciudad del beneficiario",
    ),
    "prime_award_transaction_recipient_county_fips_code": (
        "Código FIPS de cinco dígitos do condado do beneficiário",
        "Five-digit FIPS code of the recipient county",
        "Código FIPS de cinco dígitos del condado del beneficiario",
    ),
    "recipient_county_name": (
        "Nome do condado do beneficiário",
        "Name of the recipient county",
        "Nombre del condado del beneficiario",
    ),
    "prime_award_transaction_recipient_state_fips_code": (
        "Código FIPS de dois dígitos do estado do beneficiário",
        "Two-digit FIPS code of the recipient state",
        "Código FIPS de dos dígitos del estado del beneficiario",
    ),
    "recipient_state_code": (
        "Sigla de duas letras do estado do beneficiário",
        "Two-letter abbreviation of the recipient state",
        "Sigla de dos letras del estado del beneficiario",
    ),
    "recipient_state_name": (
        "Nome do estado do beneficiário",
        "Name of the recipient state",
        "Nombre del estado del beneficiario",
    ),
    "recipient_zip_4_code": (
        "CEP do beneficiário no formato ZIP+4, de nove dígitos",
        "Recipient ZIP+4 code, nine digits",
        "Código postal del beneficiario en formato ZIP+4, de nueve dígitos",
    ),
    "recipient_zip_code": (
        "CEP de cinco dígitos do beneficiário",
        "Five-digit ZIP code of the recipient",
        "Código postal de cinco dígitos del beneficiario",
    ),
    "recipient_zip_last_4_code": (
        "Quatro dígitos finais do CEP do beneficiário",
        "Last four digits of the recipient ZIP code",
        "Cuatro dígitos finales del código postal del beneficiario",
    ),
    "prime_award_transaction_recipient_cd_original": (
        "Distrito eleitoral do beneficiário na delimitação vigente na data da ação",
        "Congressional district of the recipient under the districting in force on the action date",
        "Distrito electoral del beneficiario según la delimitación vigente en la fecha de la acción",
    ),
    "prime_award_transaction_recipient_cd_current": (
        "Distrito eleitoral do beneficiário na delimitação vigente atualmente",
        "Congressional district of the recipient under the current districting",
        "Distrito electoral del beneficiario según la delimitación vigente actualmente",
    ),
    "recipient_phone_number": (
        "Telefone do beneficiário",
        "Telephone number of the recipient",
        "Teléfono del beneficiario",
    ),
    "recipient_fax_number": (
        "Fax do beneficiário",
        "Fax number of the recipient",
        "Fax del beneficiario",
    ),
    "recipient_foreign_city_name": (
        "Nome da cidade estrangeira do beneficiário",
        "Name of the foreign city of the recipient",
        "Nombre de la ciudad extranjera del beneficiario",
    ),
    "recipient_foreign_province_name": (
        "Nome da província estrangeira do beneficiário",
        "Name of the foreign province of the recipient",
        "Nombre de la provincia extranjera del beneficiario",
    ),
    "recipient_foreign_postal_code": (
        "Código postal estrangeiro do beneficiário",
        "Foreign postal code of the recipient",
        "Código postal extranjero del beneficiario",
    ),
    # ---- place of performance ---------------------------------------------
    "primary_place_of_performance_country_code": (
        "Código do país onde a execução ocorre",
        "Country code of the primary place of performance",
        "Código del país donde ocurre la ejecución",
    ),
    "primary_place_of_performance_country_name": (
        "Nome do país onde a execução ocorre",
        "Country name of the primary place of performance",
        "Nombre del país donde ocurre la ejecución",
    ),
    "primary_place_of_performance_city_name": (
        "Nome da cidade onde a execução ocorre",
        "City name of the primary place of performance",
        "Nombre de la ciudad donde ocurre la ejecución",
    ),
    "prime_award_transaction_place_of_performance_county_fips_code": (
        "Código FIPS de cinco dígitos do condado onde a execução ocorre",
        "Five-digit FIPS code of the county of the primary place of performance",
        "Código FIPS de cinco dígitos del condado donde ocurre la ejecución",
    ),
    "primary_place_of_performance_county_name": (
        "Nome do condado onde a execução ocorre",
        "County name of the primary place of performance",
        "Nombre del condado donde ocurre la ejecución",
    ),
    "prime_award_transaction_place_of_performance_state_fips_code": (
        "Código FIPS de dois dígitos do estado onde a execução ocorre",
        "Two-digit FIPS code of the state of the primary place of performance",
        "Código FIPS de dos dígitos del estado donde ocurre la ejecución",
    ),
    "primary_place_of_performance_state_code": (
        "Sigla de duas letras do estado onde a execução ocorre",
        "Two-letter abbreviation of the state of the primary place of performance",
        "Sigla de dos letras del estado donde ocurre la ejecución",
    ),
    "primary_place_of_performance_state_name": (
        "Nome do estado onde a execução ocorre",
        "State name of the primary place of performance",
        "Nombre del estado donde ocurre la ejecución",
    ),
    "primary_place_of_performance_zip_4": (
        "CEP do local de execução no formato ZIP+4",
        "ZIP+4 code of the primary place of performance",
        "Código postal del lugar de ejecución en formato ZIP+4",
    ),
    "prime_award_transaction_place_of_performance_cd_original": (
        "Distrito eleitoral do local de execução na delimitação vigente na data da ação",
        "Congressional district of the place of performance under the districting in force on the action date",
        "Distrito electoral del lugar de ejecución según la delimitación vigente en la fecha de la acción",
    ),
    "prime_award_transaction_place_of_performance_cd_current": (
        "Distrito eleitoral do local de execução na delimitação vigente atualmente",
        "Congressional district of the place of performance under the current districting",
        "Distrito electoral del lugar de ejecución según la delimitación vigente actualmente",
    ),
    "primary_place_of_performance_scope": (
        "Abrangência geográfica do local de execução",
        "Geographic scope of the primary place of performance",
        "Alcance geográfico del lugar de ejecución",
    ),
    "primary_place_of_performance_code": (
        "Código composto que identifica o local de execução e sua abrangência",
        "Composite code identifying the primary place of performance and its scope",
        "Código compuesto que identifica el lugar de ejecución y su alcance",
    ),
    "primary_place_of_performance_foreign_location": (
        "Descrição do local estrangeiro de execução",
        "Description of the foreign primary place of performance",
        "Descripción del lugar extranjero de ejecución",
    ),
    # ---- classification and text ------------------------------------------
    "award_or_idv_flag": (
        "Indica se o registro é um contrato ou um veículo de entrega indefinida (IDV)",
        "Indicates whether the record is an award or an indefinite delivery vehicle (IDV)",
        "Indica si el registro es un contrato o un vehículo de entrega indefinida (IDV)",
    ),
    "transaction_description": (
        "Descrição do objeto desta transação",
        "Description of the purpose of this transaction",
        "Descripción del objeto de esta transacción",
    ),
    "prime_award_base_transaction_description": (
        "Descrição do objeto da transação inicial do auxílio ou contrato",
        "Description of the purpose of the award's base transaction",
        "Descripción del objeto de la transacción inicial de la ayuda o contrato",
    ),
    "number_of_actions": (
        "Número de ações agrupadas neste registro",
        "Number of actions grouped in this record",
        "Número de acciones agrupadas en este registro",
    ),
    "number_of_offers_received": (
        "Número de propostas recebidas na licitação",
        "Number of offers received in the solicitation",
        "Número de propuestas recibidas en la licitación",
    ),
    "price_evaluation_adjustment_preference_percent_difference": (
        "Percentual de ajuste de preço aplicado em favor de pequenas empresas desfavorecidas",
        "Percentage price evaluation adjustment applied in favor of small disadvantaged businesses",
        "Porcentaje de ajuste de precio aplicado en favor de pequeñas empresas desfavorecidas",
    ),
    "small_business_competitiveness_demonstration_program": (
        "Indica se a contratação integra o programa de demonstração de competitividade de pequenas empresas",
        "Indicates whether the award is part of the Small Business Competitiveness Demonstration Program",
        "Indica si la contratación forma parte del programa de demostración de competitividad de pequeñas empresas",
    ),
    "small_disadvantaged_business": (
        "Indica se o beneficiário é uma pequena empresa desfavorecida certificada",
        "Indicates whether the recipient is a certified small disadvantaged business",
        "Indica si el beneficiario es una pequeña empresa desfavorecida certificada",
    ),
    "hospital_flag": (
        "Indica se o beneficiário é um hospital",
        "Indicates whether the recipient is a hospital",
        "Indica si el beneficiario es un hospital",
    ),
    "organizational_type": (
        "Tipo de organização do beneficiário conforme registro no SAM",
        "Organization type of the recipient as registered in SAM",
        "Tipo de organización del beneficiario según su registro en SAM",
    ),
    # ---- highly compensated officers --------------------------------------
    **{
        f"highly_compensated_officer_{i}_name": (
            f"Nome do {i}º dirigente mais bem remunerado do beneficiário, divulgado nos termos da FFATA",
            f"Name of the recipient's {i}{'st' if i == 1 else 'nd' if i == 2 else 'rd' if i == 3 else 'th'} most highly compensated officer, disclosed under FFATA",
            f"Nombre del {i}º directivo mejor remunerado del beneficiario, divulgado conforme a la FFATA",
        )
        for i in range(1, 6)
    },
    **{
        f"highly_compensated_officer_{i}_amount": (
            f"Remuneração anual do {i}º dirigente mais bem remunerado do beneficiário, divulgada nos termos da FFATA",
            f"Annual compensation of the recipient's {i}{'st' if i == 1 else 'nd' if i == 2 else 'rd' if i == 3 else 'th'} most highly compensated officer, disclosed under FFATA",
            f"Remuneración anual del {i}º directivo mejor remunerado del beneficiario, divulgada conforme a la FFATA",
        )
        for i in range(1, 6)
    },
}

DESCRIPTIONS.pop("disaster_emergency_fund_codes_for_observed", None)
