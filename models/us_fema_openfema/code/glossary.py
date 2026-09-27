"""Trilingual column glossary for us_fema_openfema.

One entry per harmonised snake_case column name, shared across tables — the
NFIP claim and policy files describe 48 columns identically, so they are
described once here.

Descriptions are condensed from OpenFEMA's own field dictionary (cached in
``source_metadata.json``) into Data Basis house style: capitalised first
letter, no trailing period, unit stated where there is one.

``UNIT`` maps a column to a backend MeasurementUnit slug. Only slugs that
exist in the backend are used (verified against ``allMeasurementunit``):
``usd``, ``foot``, ``inch``, ``hour``, ``year``, ``percent``. Quantities with
no matching slug (latitude/longitude in degrees, bare counts) carry a blank
unit and say so in ``observations``.

``CODED`` lists columns whose stored values are opaque codes resolved by the
``dicionario`` table. Per house convention these are STRING regardless of the
source's numeric storage — arithmetic on them is meaningless.
"""

from __future__ import annotations

# name -> (pt, en, es)
COLUMNS: dict[str, tuple[str, str, str]] = {
    # ---------------------------------------------------------------- shared
    "year": (
        "Ano de referência, usado como coluna de partição",
        "Reference year, used as the partition column",
        "Año de referencia, utilizado como columna de partición",
    ),
    "state_id": (
        "Código FIPS de duas posições do estado, distrito ou território",
        "Two-digit FIPS code of the state, district or territory",
        "Código FIPS de dos posiciones del estado, distrito o territorio",
    ),
    "state_abbreviation": (
        "Sigla de duas letras do estado, distrito ou território",
        "Two-letter abbreviation of the state, district or territory",
        "Sigla de dos letras del estado, distrito o territorio",
    ),
    "county_id": (
        "Código FIPS de cinco posições do condado",
        "Five-digit FIPS code of the county",
        "Código FIPS de cinco posiciones del condado",
    ),
    "county": (
        "Nome do condado, paróquia, borough ou cidade independente",
        "Name of the county, parish, borough or independent city",
        "Nombre del condado, parroquia, borough o ciudad independiente",
    ),
    "census_tract_id": (
        "Código de onze posições do setor censitário (census tract)",
        "Eleven-digit census tract identifier",
        "Código de once posiciones del sector censal (census tract)",
    ),
    "census_block_group_id": (
        "Código de doze posições do grupo de quadras censitário (block group)",
        "Twelve-digit census block group identifier",
        "Código de doce posiciones del grupo de manzanas censal (block group)",
    ),
    "disaster_number": (
        "Número sequencial que designa o evento declarado como desastre",
        "Sequential number designating the event declared as a disaster",
        "Número secuencial que designa el evento declarado como desastre",
    ),
    "declaration_date": (
        "Data em que o desastre foi declarado",
        "Date the disaster was declared",
        "Fecha en que el desastre fue declarado",
    ),
    "incident_type": (
        "Tipo primário do incidente, como incêndio ou inundação",
        "Primary type of the incident, such as fire or flood",
        "Tipo primario del incidente, como incendio o inundación",
    ),
    "as_of_date": (
        "Data de vigência dos dados no arquivo: quando o registro foi criado "
        "ou atualizado pela última vez no sistema de origem",
        "Effective date of the data in the file: when the record was created "
        "or last updated in the source system",
        "Fecha de vigencia de los datos en el archivo: cuándo el registro fue "
        "creado o actualizado por última vez en el sistema de origen",
    ),
    # -------------------------------------------- disaster_declaration only
    "fema_declaration_string": (
        "Identificador padrão da declaração sob o Stafford Act, formado pela "
        "concatenação do tipo de declaração, do número do desastre e da sigla "
        "do estado",
        "Agency-standard identifier for a Stafford Act declaration, "
        "concatenating declaration type, disaster number and state "
        "abbreviation",
        "Identificador estándar de la declaración bajo el Stafford Act, "
        "formado por la concatenación del tipo de declaración, el número del "
        "desastre y la sigla del estado",
    ),
    "declaration_type": (
        "Código de duas letras que distingue desastre de grande porte, "
        "emergência e gestão de incêndio",
        "Two-letter code distinguishing a major disaster, an emergency and a "
        "fire management declaration",
        "Código de dos letras que distingue desastre mayor, emergencia y "
        "gestión de incendios",
    ),
    "fy_declared": (
        "Ano fiscal federal em que o desastre foi declarado",
        "Federal fiscal year in which the disaster was declared",
        "Año fiscal federal en que el desastre fue declarado",
    ),
    "declaration_title": (
        "Título atribuído ao desastre",
        "Title given to the disaster",
        "Título atribuido al desastre",
    ),
    "ih_program_declared": (
        "Indica se o programa Individuals and Households foi declarado para "
        "este desastre",
        "Whether the Individuals and Households program was declared for this "
        "disaster",
        "Indica si el programa Individuals and Households fue declarado para "
        "este desastre",
    ),
    "ia_program_declared": (
        "Indica se o programa Individual Assistance foi declarado para este "
        "desastre",
        "Whether the Individual Assistance program was declared for this "
        "disaster",
        "Indica si el programa Individual Assistance fue declarado para este "
        "desastre",
    ),
    "pa_program_declared": (
        "Indica se o programa Public Assistance foi declarado para este "
        "desastre",
        "Whether the Public Assistance program was declared for this disaster",
        "Indica si el programa Public Assistance fue declarado para este "
        "desastre",
    ),
    "hm_program_declared": (
        "Indica se o programa Hazard Mitigation foi declarado para este "
        "desastre",
        "Whether the Hazard Mitigation program was declared for this disaster",
        "Indica si el programa Hazard Mitigation fue declarado para este "
        "desastre",
    ),
    "incident_begin_date": (
        "Data de início do incidente",
        "Date the incident began",
        "Fecha de inicio del incidente",
    ),
    "incident_end_date": (
        "Data de término do incidente",
        "Date the incident ended",
        "Fecha de término del incidente",
    ),
    "disaster_closeout_date": (
        "Data em que todas as transações financeiras de todos os programas "
        "foram concluídas",
        "Date all financial transactions for all programs were completed",
        "Fecha en que todas las transacciones financieras de todos los "
        "programas fueron concluidas",
    ),
    "tribal_request": (
        "Indica que o pedido de declaração foi submetido diretamente ao "
        "Presidente por uma nação tribal, independentemente de um estado",
        "Whether the declaration request was submitted directly to the "
        "President by a Tribal Nation, independently of a state",
        "Indica que la solicitud de declaración fue presentada directamente al "
        "Presidente por una nación tribal, independientemente de un estado",
    ),
    "fips_county_code": (
        "Código FIPS de três posições do condado dentro do estado",
        "Three-digit FIPS code of the county within the state",
        "Código FIPS de tres posiciones del condado dentro del estado",
    ),
    "place_code": (
        "Código interno da FEMA para a localidade designada, formado por '99' "
        "seguido do código FIPS de três posições do condado",
        "FEMA-internal code for the designated location, formed by '99' "
        "followed by the three-digit county FIPS code",
        "Código interno de FEMA para la localidad designada, formado por '99' "
        "seguido del código FIPS de tres posiciones del condado",
    ),
    "designated_area": (
        "Nome da área geográfica incluída na declaração",
        "Name of the geographic area included in the declaration",
        "Nombre del área geográfica incluida en la declaración",
    ),
    "declaration_request_number": (
        "Número atribuído ao pedido de declaração",
        "Number assigned to the declaration request",
        "Número asignado a la solicitud de declaración",
    ),
    "declaration_request_date": (
        "Data em que o pedido de declaração foi feito",
        "Date the declaration request was made",
        "Fecha en que se realizó la solicitud de declaración",
    ),
    "last_ia_filing_date": (
        "Último dia para protocolar pedidos de Individual Assistance; "
        "disponível apenas a partir de 1998 e somente quando o programa foi "
        "aprovado",
        "Last day on which Individual Assistance requests can be filed; "
        "available from 1998 onward and only where the program was approved",
        "Último día para presentar solicitudes de Individual Assistance; "
        "disponible solo a partir de 1998 y únicamente cuando el programa fue "
        "aprobado",
    ),
    "incident_id": (
        "Identificador único do incidente; incidentes podem ou não se tornar "
        "desastres declarados",
        "Unique identifier of the incident; incidents may or may not become "
        "declared disasters",
        "Identificador único del incidente; los incidentes pueden o no "
        "convertirse en desastres declarados",
    ),
    "region": (
        "Número de 1 a 10 que identifica a região da FEMA em que o desastre "
        "ocorreu",
        "Number from 1 to 10 identifying the FEMA region where the disaster "
        "occurred",
        "Número de 1 a 10 que identifica la región de FEMA en que ocurrió el "
        "desastre",
    ),
    "designated_incident_types": (
        "Lista separada por vírgulas dos tipos de incidente designados para o "
        "desastre",
        "Comma-separated list of incident types designated for the disaster",
        "Lista separada por comas de los tipos de incidente designados para el "
        "desastre",
    ),
    "record_id": (
        "Identificador único do registro atribuído pela OpenFEMA",
        "Unique record identifier assigned by OpenFEMA",
        "Identificador único del registro asignado por OpenFEMA",
    ),
    # ------------------------------------- public_assistance_project only
    "pw_number": (
        "Número sequencial que identifica o Project Worksheet",
        "Sequential number identifying the Project Worksheet",
        "Número secuencial que identifica el Project Worksheet",
    ),
    "application_title": (
        "Título da aplicação, em texto livre e não único",
        "Free-text, non-unique application title",
        "Título de la solicitud, en texto libre y no único",
    ),
    "applicant_id": (
        "Identificador do requerente do programa Public Assistance",
        "Public Assistance applicant identifier",
        "Identificador del solicitante del programa Public Assistance",
    ),
    "damage_category_code": (
        "Código da categoria de trabalho elegível a reembolso pelo Public "
        "Assistance",
        "Code for the category of work eligible for Public Assistance "
        "reimbursement",
        "Código de la categoría de trabajo elegible para reembolso por Public "
        "Assistance",
    ),
    "damage_category_descrip": (
        "Descrição da categoria de dano correspondente ao código de categoria",
        "Description of the damage category matching the category code",
        "Descripción de la categoría de daño correspondiente al código de "
        "categoría",
    ),
    "project_status": (
        "Situação do projeto quanto à sua elegibilidade",
        "Status of the project with respect to its eligibility",
        "Situación del proyecto en cuanto a su elegibilidad",
    ),
    "project_process_step": (
        "Etapa atual do projeto, da formulação ao encerramento, incluindo "
        "estágios pendentes e descontinuados",
        "Current stage of the project, from formulation through closeout, "
        "including pending and discontinued stages",
        "Etapa actual del proyecto, de la formulación al cierre, incluyendo "
        "etapas pendientes y descontinuadas",
    ),
    "project_size": (
        "Porte do projeto, grande ou pequeno, definido por um limiar monetário "
        "que afeta a gestão do caso",
        "Project size, large or small, set by a monetary threshold that "
        "affects case management",
        "Tamaño del proyecto, grande o pequeño, definido por un umbral "
        "monetario que afecta la gestión del caso",
    ),
    "project_amount": (
        "Custo total estimado do projeto em dólares, sem custos "
        "administrativos",
        "Estimated total project cost in dollars, excluding administrative "
        "costs",
        "Costo total estimado del proyecto en dólares, sin costos "
        "administrativos",
    ),
    "federal_share_obligated": (
        "Parcela federal do financiamento disponibilizada ao estado, em "
        "dólares",
        "Federal share of the grant funding made available to the state, in "
        "dollars",
        "Parte federal del financiamiento puesta a disposición del estado, en "
        "dólares",
    ),
    "total_obligated": (
        "Parcela federal do valor elegível somada às contrapartidas estadual e "
        "local e aos custos administrativos, em dólares",
        "Federal share of the eligible project amount plus state and "
        "sub-grantee shares and administrative costs, in dollars",
        "Parte federal del monto elegible sumada a las contrapartidas estatal "
        "y local y a los costos administrativos, en dólares",
    ),
    "last_obligation_date": (
        "Data da última obrigação de recursos do projeto",
        "Date funds were last obligated to the project",
        "Fecha de la última obligación de recursos del proyecto",
    ),
    "first_obligation_date": (
        "Data da primeira obrigação de recursos do projeto",
        "Date funds were first obligated to the project",
        "Fecha de la primera obligación de recursos del proyecto",
    ),
    "mitigation_amount": (
        "Valor proposto aplicável à mitigação prevista na seção 406, em "
        "dólares",
        "Amount proposed for section 406 mitigation, in dollars",
        "Monto propuesto aplicable a la mitigación prevista en la sección 406, "
        "en dólares",
    ),
    "gm_project_id": (
        "Identificador numérico do projeto no sistema Grants Manager",
        "Numeric project identifier in the Grants Manager system",
        "Identificador numérico del proyecto en el sistema Grants Manager",
    ),
    "gm_applicant_id": (
        "Identificador numérico do requerente no sistema Grants Manager",
        "Numeric applicant identifier in the Grants Manager system",
        "Identificador numérico del solicitante en el sistema Grants Manager",
    ),
    "state_number_code": (
        "Código numérico do estado ou território, correspondente aos dois "
        "primeiros dígitos do código FIPS do condado",
        "Numeric state or territory code, matching the first two digits of the "
        "county FIPS code",
        "Código numérico del estado o territorio, correspondiente a los dos "
        "primeros dígitos del código FIPS del condado",
    ),
    "county_code": (
        "Código FIPS do condado tal como publicado na fonte",
        "County FIPS code as published in the source",
        "Código FIPS del condado tal como se publica en la fuente",
    ),
}

# ---------------------------------------------------------------------------
# NFIP building and location attributes, shared by nfip_claim and nfip_policy
# ---------------------------------------------------------------------------
COLUMNS.update(
    {
        "agriculture_structure_indicator": (
            "Indica se o imóvel foi declarado como estrutura agrícola na proposta",
            "Whether the building was reported as an agricultural structure on the "
            "application",
            "Indica si el inmueble fue declarado como estructura agrícola en la "
            "solicitud",
        ),
        "basement_enclosure_crawlspace_type": (
            "Tipo de porão, área fechada ou vão sanitário do imóvel segurado",
            "Type of basement, enclosure or crawlspace of the insured building",
            "Tipo de sótano, área cerrada o entrepiso sanitario del inmueble "
            "asegurado",
        ),
        "policy_count": (
            "Número de unidades seguradas em situação ativa",
            "Number of insured units in an active status",
            "Número de unidades aseguradas en situación activa",
        ),
        "crs_class_code": (
            "Classificação do Community Rating System usada para tarifar a "
            "apólice, que determina o desconto no prêmio",
            "Community Rating System classification used to rate the policy, which "
            "determines the premium discount",
            "Clasificación del Community Rating System usada para tarificar la "
            "póliza, que determina el descuento en la prima",
        ),
        "elevated_building_indicator": (
            "Indica se o imóvel atende à definição de edificação elevada do NFIP",
            "Whether the building meets the NFIP definition of an elevated "
            "building",
            "Indica si el inmueble cumple la definición de edificación elevada del "
            "NFIP",
        ),
        "elevation_certificate_indicator": (
            "Indica se a apólice foi tarifada com Elevation Certificate",
            "Whether the policy was rated with an Elevation Certificate",
            "Indica si la póliza fue tarificada con Elevation Certificate",
        ),
        "elevation_difference": (
            "Diferença em pés entre a elevação do piso mais baixo usado na "
            "tarifação e a cota de inundação base",
            "Difference in feet between the elevation of the lowest floor used for "
            "rating and the base flood elevation",
            "Diferencia en pies entre la elevación del piso más bajo usado en la "
            "tarificación y la cota de inundación base",
        ),
        "base_flood_elevation": (
            "Cota de inundação base em pés, elevação com 1% de chance anual de ser "
            "atingida ou superada",
            "Base flood elevation in feet, the level with a 1% annual chance of "
            "being reached or exceeded",
            "Cota de inundación base en pies, elevación con 1% de probabilidad "
            "anual de ser alcanzada o superada",
        ),
        "rated_flood_zone": (
            "Zona de inundação do FIRM usada para tarifar o imóvel segurado",
            "FIRM flood zone used to rate the insured property",
            "Zona de inundación del FIRM usada para tarificar el inmueble "
            "asegurado",
        ),
        "flood_zone_current": (
            "Zona de inundação do FIRM em que o imóvel segurado se situa "
            "atualmente",
            "FIRM flood zone in which the insured property is currently located",
            "Zona de inundación del FIRM en que el inmueble asegurado se sitúa "
            "actualmente",
        ),
        "house_of_worship_indicator": (
            "Indica se o imóvel foi declarado como templo religioso na proposta",
            "Whether the building was reported as a house of worship on the "
            "application",
            "Indica si el inmueble fue declarado como templo religioso en la "
            "solicitud",
        ),
        "location_of_contents": (
            "Código que indica onde o conteúdo segurado está situado dentro da "
            "edificação",
            "Code indicating where within the building the insured contents are "
            "located",
            "Código que indica dónde está situado el contenido asegurado dentro de "
            "la edificación",
        ),
        "lowest_adjacent_grade": (
            "Cota natural mais baixa adjacente à estrutura segurada antes de "
            "escavação ou aterro, em pés",
            "Lowest natural grade adjacent to the insured structure prior to "
            "excavation or fill, in feet",
            "Cota natural más baja adyacente a la estructura asegurada antes de "
            "excavación o relleno, en pies",
        ),
        "lowest_floor_elevation": (
            "Elevação em pés do piso mais baixo da edificação, incluindo porão, "
            "área fechada ou vão sanitário",
            "Elevation in feet of the building's lowest floor, including basement, "
            "enclosure or crawlspace",
            "Elevación en pies del piso más bajo de la edificación, incluyendo "
            "sótano, área cerrada o entrepiso sanitario",
        ),
        "number_of_floors_in_insured_building": (
            "Código que indica o número de pavimentos da edificação segurada",
            "Code indicating the number of floors in the insured building",
            "Código que indica el número de pisos de la edificación asegurada",
        ),
        "non_profit_indicator": (
            "Indica se o imóvel foi declarado como pertencente a entidade sem fins "
            "lucrativos na proposta",
            "Whether the building was reported as belonging to a non-profit on the "
            "application",
            "Indica si el inmueble fue declarado como perteneciente a una entidad "
            "sin fines de lucro en la solicitud",
        ),
        "obstruction_type": (
            "Código que descreve o tipo de obstrução presente sob uma edificação "
            "elevada",
            "Code describing the type of obstruction present beneath an elevated "
            "building",
            "Código que describe el tipo de obstrucción presente bajo una "
            "edificación elevada",
        ),
        "occupancy_type": (
            "Código que indica o uso e o tipo de ocupação da estrutura segurada",
            "Code indicating the use and occupancy type of the insured structure",
            "Código que indica el uso y el tipo de ocupación de la estructura "
            "asegurada",
        ),
        "original_construction_date": (
            "Data original de construção da edificação",
            "Original construction date of the building",
            "Fecha original de construcción de la edificación",
        ),
        "original_nb_date": (
            "Data original de emissão da apólice de inundação",
            "Original issue date of the flood policy",
            "Fecha original de emisión de la póliza de inundación",
        ),
        "post_firm_construction_indicator": (
            "Indica se a construção começou após a publicação do FIRM",
            "Whether construction started after the FIRM was published",
            "Indica si la construcción comenzó después de la publicación del FIRM",
        ),
        "pre_firm_construction_indicator": (
            "Indica se a construção começou antes da publicação do FIRM",
            "Whether construction started before the FIRM was published",
            "Indica si la construcción comenzó antes de la publicación del FIRM",
        ),
        "rate_method": (
            "Código do método de tarifação aplicado à apólice",
            "Code for the rating method applied to the policy",
            "Código del método de tarificación aplicado a la póliza",
        ),
        "small_business_indicator_building": (
            "Indica se o segurado é uma pequena empresa",
            "Whether the insured is a small business",
            "Indica si el asegurado es una pequeña empresa",
        ),
        "total_building_insurance_coverage": (
            "Importância segurada da edificação, em dólares inteiros",
            "Total insured amount on the building, in whole dollars",
            "Suma asegurada de la edificación, en dólares enteros",
        ),
        "total_contents_insurance_coverage": (
            "Importância segurada do conteúdo, em dólares inteiros",
            "Total insured amount on the contents, in whole dollars",
            "Suma asegurada del contenido, en dólares enteros",
        ),
        "primary_residence_indicator": (
            "Indica se a edificação é residência principal do segurado",
            "Whether the building is the insured's primary residence",
            "Indica si la edificación es residencia principal del asegurado",
        ),
        "building_deductible_code": (
            "Código da franquia aplicável à edificação principal e às acessórias",
            "Code for the deductible applying to the main and appurtenant buildings",
            "Código del deducible aplicable a la edificación principal y a las "
            "accesorias",
        ),
        "contents_deductible_code": (
            "Código da franquia aplicável ao conteúdo",
            "Code for the deductible applying to the contents",
            "Código del deducible aplicable al contenido",
        ),
        "condominium_coverage_type_code": (
            "Código do tipo de cobertura de condomínio contratada para a edificação",
            "Code for the type of condominium coverage held by the building",
            "Código del tipo de cobertura de condominio contratada para la "
            "edificación",
        ),
        "disaster_assistance_coverage_required": (
            "Código da agência federal que exigiu a contratação do seguro de "
            "inundação como condição de auxílio em desastre",
            "Code for the federal agency that required flood insurance as a "
            "condition of disaster assistance",
            "Código de la agencia federal que exigió la contratación del seguro de "
            "inundación como condición de ayuda en desastre",
        ),
        "floodproofed_indicator": (
            "Indica se a estrutura segurada é à prova de inundação",
            "Whether the insured structure is floodproofed",
            "Indica si la estructura asegurada es a prueba de inundación",
        ),
        "building_replacement_cost": (
            "Custo estimado de reposição da edificação em dólares inteiros, "
            "conforme informado pela seguradora",
            "Estimated cost to replace the building in whole dollars, as reported "
            "by the insurer",
            "Costo estimado de reposición de la edificación en dólares enteros, "
            "según lo informado por la aseguradora",
        ),
        "building_description_code": (
            "Código que descreve o uso da edificação segurada",
            "Code describing the use of the insured building",
            "Código que describe el uso de la edificación asegurada",
        ),
        "state_owned_indicator": (
            "Indica se o imóvel segurado pertence ao estado",
            "Whether the insured property is state owned",
            "Indica si el inmueble asegurado pertenece al estado",
        ),
        "rental_property_indicator": (
            "Indica se o imóvel é destinado a locação",
            "Whether the property is a rental property",
            "Indica si el inmueble está destinado a alquiler",
        ),
        "reported_city": (
            "Cidade do imóvel segurado conforme informada pelas seguradoras "
            "parceiras Write Your Own",
            "City of the insured property as reported by Write Your Own partner "
            "insurers",
            "Ciudad del inmueble asegurado según lo informado por las aseguradoras "
            "asociadas Write Your Own",
        ),
        "reported_zip_code": (
            "Código postal de cinco dígitos do imóvel segurado conforme informado "
            "pelas seguradoras parceiras",
            "Five-digit postal code of the insured property as reported by partner "
            "insurers",
            "Código postal de cinco dígitos del inmueble asegurado según lo "
            "informado por las aseguradoras asociadas",
        ),
        "census_geoid": (
            "Código censitário do imóvel segurado conforme publicado na fonte",
            "Census identifier of the insured property as published in the source",
            "Código censal del inmueble asegurado según se publica en la fuente",
        ),
        "latitude": (
            "Latitude aproximada da edificação segurada, arredondada a uma casa "
            "decimal pela FEMA",
            "Approximate latitude of the insured building, rounded by FEMA to one "
            "decimal place",
            "Latitud aproximada de la edificación asegurada, redondeada a un "
            "decimal por FEMA",
        ),
        "longitude": (
            "Longitude aproximada da edificação segurada, arredondada a uma casa "
            "decimal pela FEMA",
            "Approximate longitude of the insured building, rounded by FEMA to one "
            "decimal place",
            "Longitud aproximada de la edificación asegurada, redondeada a un "
            "decimal por FEMA",
        ),
        "foundation_type": (
            "Código do tipo de fundação da edificação",
            "Code for the building's foundation type",
            "Código del tipo de cimentación de la edificación",
        ),
        "nfip_rated_community_number": (
            "Código de seis posições da comunidade NFIP usada na tarifação",
            "Six-digit identifier of the NFIP community used for rating",
            "Código de seis posiciones de la comunidad NFIP usada en la "
            "tarificación",
        ),
        "nfip_community_number_current": (
            "Código de seis posições da comunidade NFIP atual do imóvel",
            "Six-digit identifier of the property's current NFIP community",
            "Código de seis posiciones de la comunidad NFIP actual del inmueble",
        ),
        "nfip_community_name": (
            "Nome da entidade político-administrativa responsável por adotar e "
            "fazer cumprir as normas de planície de inundação",
            "Name of the political entity responsible for adopting and enforcing "
            "floodplain ordinances",
            "Nombre de la entidad político-administrativa responsable de adoptar y "
            "hacer cumplir las normas de llanura de inundación",
        ),
    }
)

# ---------------------------------------------------------------------------
# nfip_claim only — the loss event, what was paid, and what was recovered
# ---------------------------------------------------------------------------
COLUMNS.update(
    {
        "claim_id": (
            "Identificador único do sinistro atribuído pela OpenFEMA",
            "Unique claim identifier assigned by OpenFEMA",
            "Identificador único del siniestro asignado por OpenFEMA",
        ),
        "date_of_loss": (
            "Data em que a água entrou pela primeira vez na edificação segurada",
            "Date on which water first entered the insured building",
            "Fecha en que el agua entró por primera vez en la edificación asegurada",
        ),
        "cause_of_damage": (
            "Código do modo pelo qual o imóvel e o conteúdo foram danificados",
            "Code for the way the property and contents were damaged",
            "Código del modo por el cual el inmueble y el contenido fueron dañados",
        ),
        "flood_characteristics_indicator": (
            "Código das características das águas da inundação",
            "Code for the characteristics of the flood waters",
            "Código de las características de las aguas de la inundación",
        ),
        "flood_water_duration": (
            "Número de horas em que a água permaneceu na edificação segurada",
            "Number of hours flood water remained in the insured building",
            "Número de horas en que el agua permaneció en la edificación asegurada",
        ),
        "flood_event": (
            "Nome atribuído à catástrofe de inundação",
            "Name given to the flooding catastrophe",
            "Nombre atribuido a la catástrofe de inundación",
        ),
        "event_designation_number": (
            "Identificador único do evento catastrófico de inundação, que "
            "substituiu o FICO number",
            "Unique identifier of the catastrophic flood event, which replaced the "
            "FICO number",
            "Identificador único del evento catastrófico de inundación, que "
            "sustituyó al FICO number",
        ),
        "fico_number": (
            "Número atribuído no momento de um evento significativo de inundação "
            "para identificar cada estado afetado",
            "Number assigned at the time of a significant flood event to identify "
            "each affected state",
            "Número atribuido en el momento de un evento significativo de "
            "inundación para identificar cada estado afectado",
        ),
        "water_depth": (
            "Profundidade da água da inundação, em polegadas",
            "Depth of the flood water, in inches",
            "Profundidad del agua de la inundación, en pulgadas",
        ),
        "exterior_water_depth": (
            "Profundidade da água em polegadas medida por marcas externas, em "
            "relação à cota adjacente mais baixa",
            "Depth of water in inches from exterior water marks, relative to the "
            "lowest adjacent grade",
            "Profundidad del agua en pulgadas medida por marcas externas, en "
            "relación a la cota adyacente más baja",
        ),
        "interior_water_depth": (
            "Profundidade da água em polegadas medida por marcas internas, em "
            "relação à cota adjacente mais baixa",
            "Depth of water in inches from interior water marks, relative to the "
            "lowest adjacent grade",
            "Profundidad del agua en pulgadas medida por marcas internas, en "
            "relación a la cota adyacente más baja",
        ),
        "amount_paid_on_building_claim": (
            "Valor pago pelo sinistro da edificação, em dólares",
            "Amount paid on the building claim, in dollars",
            "Monto pagado por el siniestro de la edificación, en dólares",
        ),
        "amount_paid_on_contents_claim": (
            "Valor pago pelo sinistro do conteúdo, em dólares",
            "Amount paid on the contents claim, in dollars",
            "Monto pagado por el siniestro del contenido, en dólares",
        ),
        "amount_paid_on_increased_cost_of_compliance_claim": (
            "Valor pago pela cobertura Increased Cost of Compliance, em dólares",
            "Amount paid on the Increased Cost of Compliance claim, in dollars",
            "Monto pagado por la cobertura Increased Cost of Compliance, en dólares",
        ),
        "net_building_payment_amount": (
            "Valor líquido pago ao segurado pela edificação, descontadas as "
            "recuperações, em dólares",
            "Net amount paid to the insured for the building, net of recoveries, "
            "in dollars",
            "Monto neto pagado al asegurado por la edificación, descontadas las "
            "recuperaciones, en dólares",
        ),
        "net_contents_payment_amount": (
            "Valor líquido pago ao segurado pelo conteúdo, descontadas as "
            "recuperações, em dólares",
            "Net amount paid to the insured for the contents, net of recoveries, "
            "in dollars",
            "Monto neto pagado al asegurado por el contenido, descontadas las "
            "recuperaciones, en dólares",
        ),
        "net_icc_payment_amount": (
            "Valor líquido pago ao segurado pela cobertura Increased Cost of "
            "Compliance, em dólares",
            "Net Increased Cost of Compliance amount paid to the insured, in "
            "dollars",
            "Monto neto pagado al asegurado por la cobertura Increased Cost of "
            "Compliance, en dólares",
        ),
        "building_damage_amount": (
            "Valor em dinheiro do dano à edificação principal, em dólares inteiros",
            "Actual cash value of the damage to the main building, in whole dollars",
            "Valor en efectivo del daño a la edificación principal, en dólares "
            "enteros",
        ),
        "contents_damage_amount": (
            "Valor em dinheiro do dano ao conteúdo, em dólares inteiros",
            "Actual cash value of the damage to the contents, in whole dollars",
            "Valor en efectivo del daño al contenido, en dólares enteros",
        ),
        "building_property_value": (
            "Valor em dinheiro da edificação principal antes da inundação, "
            "estimado por regulador, em dólares inteiros",
            "Actual cash value of the main building before the flood, as estimated "
            "by an adjuster, in whole dollars",
            "Valor en efectivo de la edificación principal antes de la inundación, "
            "estimado por un ajustador, en dólares enteros",
        ),
        "contents_property_value": (
            "Valor em dinheiro do conteúdo antes da inundação, estimado por "
            "regulador, em dólares inteiros",
            "Actual cash value of the contents before the flood, as estimated by "
            "an adjuster, in whole dollars",
            "Valor en efectivo del contenido antes de la inundación, estimado por "
            "un ajustador, en dólares enteros",
        ),
        "contents_replacement_cost": (
            "Custo estimado de reposição do conteúdo em dólares inteiros, conforme "
            "informado pela seguradora",
            "Estimated cost to replace the contents in whole dollars, as reported "
            "by the insurer",
            "Costo estimado de reposición del contenido en dólares enteros, según "
            "lo informado por la aseguradora",
        ),
        "replacement_cost_basis": (
            "Indica se o sinistro da edificação foi liquidado com base no custo de "
            "reposição",
            "Whether the building claim was settled on a replacement cost basis",
            "Indica si el siniestro de la edificación fue liquidado con base en el "
            "costo de reposición",
        ),
        "icc_coverage": (
            "Importância segurada da cobertura Increased Cost of Compliance da "
            "edificação, em dólares inteiros",
            "Amount of Increased Cost of Compliance coverage on the building, in "
            "whole dollars",
            "Suma asegurada de la cobertura Increased Cost of Compliance de la "
            "edificación, en dólares enteros",
        ),
        "non_payment_reason_building": (
            "Código do motivo pelo qual o sinistro da edificação não foi pago",
            "Code for the reason the building claim was not paid",
            "Código del motivo por el cual el siniestro de la edificación no fue "
            "pagado",
        ),
        "non_payment_reason_contents": (
            "Código do motivo pelo qual o sinistro do conteúdo não foi pago",
            "Code for the reason the contents claim was not paid",
            "Código del motivo por el cual el siniestro del contenido no fue pagado",
        ),
        "number_of_units": (
            "Número de unidades residenciais e não residenciais cobertas pela "
            "apólice coletiva de condomínio",
            "Number of residential and non-residential units covered by the "
            "condominium master policy",
            "Número de unidades residenciales y no residenciales cubiertas por la "
            "póliza colectiva de condominio",
        ),
        "open_date": (
            "Data de abertura do sinistro no sistema",
            "Date the claim was opened in the system",
            "Fecha de apertura del siniestro en el sistema",
        ),
        "most_recent_payment_date": (
            "Data do pagamento mais recente emitido ao segurado",
            "Date of the most recent claim payment issued to the insured",
            "Fecha del pago más reciente emitido al asegurado",
        ),
        "most_recent_recovery_date": (
            "Data em que um pagamento a maior foi recuperado do segurado",
            "Date on which an overpayment was recovered from the insured",
            "Fecha en que un pago en exceso fue recuperado del asegurado",
        ),
        "total_salvage_recovery": (
            "Valor recuperado com a venda ou destinação de bens danificados "
            "considerados aproveitáveis, em dólares",
            "Amount recovered from the sale or disposal of damaged property deemed "
            "salvageable, in dollars",
            "Monto recuperado con la venta o disposición de bienes dañados "
            "considerados aprovechables, en dólares",
        ),
        "total_bldg_claim_pmt_recovery": (
            "Valor recuperado do valor residual de materiais ou componentes "
            "danificados da edificação, em dólares",
            "Amount recovered from the salvage value of damaged building materials "
            "or components, in dollars",
            "Monto recuperado del valor residual de materiales o componentes "
            "dañados de la edificación, en dólares",
        ),
        "total_contents_claim_pmt_recovery": (
            "Valor recuperado do valor residual de bens do conteúdo danificados, "
            "em dólares",
            "Amount recovered from the salvage value of damaged contents, in "
            "dollars",
            "Monto recuperado del valor residual de bienes del contenido dañados, "
            "en dólares",
        ),
        "total_icc_claim_pmt_recovery": (
            "Valor recuperado de pagamento indevido da cobertura Increased Cost of "
            "Compliance, em dólares",
            "Amount recovered from an Increased Cost of Compliance payment found "
            "to be improper, in dollars",
            "Monto recuperado de un pago indebido de la cobertura Increased Cost "
            "of Compliance, en dólares",
        ),
        "total_subrogation_recovery": (
            "Valor recuperado pelo NFIP junto a terceiro responsável pelo "
            "sinistro, em dólares",
            "Amount recovered by the NFIP from a third party responsible for the "
            "loss, in dollars",
            "Monto recuperado por el NFIP de un tercero responsable del siniestro, "
            "en dólares",
        ),
    }
)

# ---------------------------------------------------------------------------
# nfip_policy only — the contract, its term, its pricing and its rating basis
# ---------------------------------------------------------------------------
COLUMNS.update(
    {
        "policy_id": (
            "Identificador único da apólice atribuído pela OpenFEMA",
            "Unique policy identifier assigned by OpenFEMA",
            "Identificador único de la póliza asignado por OpenFEMA",
        ),
        "policy_effective_date": (
            "Data de início de vigência da apólice de inundação",
            "Effective date of the flood policy",
            "Fecha de inicio de vigencia de la póliza de inundación",
        ),
        "policy_termination_date": (
            "Data em que a apólice deixou de vigorar, por cancelamento ou por "
            "decurso do prazo",
            "Date the policy ceased to be in force, by cancellation or lapse",
            "Fecha en que la póliza dejó de estar vigente, por cancelación o por "
            "vencimiento del plazo",
        ),
        "cancellation_date_of_flood_policy": (
            "Data de cancelamento da apólice de inundação, quando houver",
            "Cancellation date of the flood policy, where there is one",
            "Fecha de cancelación de la póliza de inundación, cuando la haya",
        ),
        "cancellation_voidance_reason_code": (
            "Código do motivo de cancelamento ou anulação da apólice",
            "Code for the reason the policy was cancelled or voided",
            "Código del motivo de cancelación o anulación de la póliza",
        ),
        "policy_term_indicator": (
            "Código do prazo de vigência da apólice",
            "Code for the length of time the policy is in effect",
            "Código del plazo de vigencia de la póliza",
        ),
        "endorsement_effective_date": (
            "Data em que um endosso da apólice entra em vigor",
            "Date a policy endorsement goes into effect",
            "Fecha en que un endoso de la póliza entra en vigor",
        ),
        "property_purchase_date": (
            "Data em que o imóvel segurado foi adquirido",
            "Date the insured property was purchased",
            "Fecha en que el inmueble asegurado fue adquirido",
        ),
        "policy_cost": (
            "Custo total da apólice em dólares, somando prêmio calculado, "
            "contribuição ao fundo de reserva, taxa federal e sobretaxa HFIAA",
            "Total policy cost in dollars: calculated premium plus reserve fund "
            "assessment, federal policy fee and HFIAA surcharge",
            "Costo total de la póliza en dólares, sumando prima calculada, "
            "contribución al fondo de reserva, tasa federal y sobretasa HFIAA",
        ),
        "total_insurance_premium_of_the_policy": (
            "Prêmio total da apólice em dólares inteiros; valores negativos "
            "indicam restituição",
            "Total policy premium in whole dollars; negative values indicate a "
            "refund",
            "Prima total de la póliza en dólares enteros; los valores negativos "
            "indican devolución",
        ),
        "full_risk_premium": (
            "Prêmio devido pelo imóvel com base no risco de inundação apurado e no "
            "custo integral das perdas previstas, em dólares",
            "Premium chargeable for the property based on its assessed flood risk "
            "and the full cost of anticipated losses, in dollars",
            "Prima debida por el inmueble con base en el riesgo de inundación "
            "determinado y el costo íntegro de las pérdidas previstas, en dólares",
        ),
        "federal_policy_fee": (
            "Valor da taxa federal da apólice, em dólares",
            "Amount of the federal policy fee, in dollars",
            "Monto de la tasa federal de la póliza, en dólares",
        ),
        "hfiaa_surcharge": (
            "Sobretaxa anual instituída pelo HFIAA, obrigatória em apólices novas e "
            "renovadas a partir de 1 de abril de 2015, em dólares",
            "Congressionally mandated annual HFIAA surcharge, required on new and "
            "renewal policies from 1 April 2015, in dollars",
            "Sobretasa anual establecida por el HFIAA, obligatoria en pólizas "
            "nuevas y renovadas a partir del 1 de abril de 2015, en dólares",
        ),
        "icc_premium": (
            "Prêmio da cobertura Increased Cost of Compliance antes de descontos, "
            "em dólares inteiros",
            "Increased Cost of Compliance premium before any discounts, in whole "
            "dollars",
            "Prima de la cobertura Increased Cost of Compliance antes de "
            "descuentos, en dólares enteros",
        ),
        "reserve_fund_assessment": (
            "Contribuição ao fundo de reserva informada pela seguradora, em dólares",
            "Reserve fund assessment as reported by the insurer, in dollars",
            "Contribución al fondo de reserva informada por la aseguradora, en "
            "dólares",
        ),
        "community_probation_surcharge": (
            "Sobretaxa de probation calculada pela seguradora para tarifar a "
            "apólice, em dólares inteiros",
            "Probation surcharge calculated by the insurer to rate the policy, in "
            "whole dollars",
            "Sobretasa de probation calculada por la aseguradora para tarificar la "
            "póliza, en dólares enteros",
        ),
        "premium_payment_indicator": (
            "Código do meio de pagamento usado para contratar a apólice",
            "Code for the payment mechanism used to purchase the policy",
            "Código del medio de pago usado para contratar la póliza",
        ),
        "basic_building_rate": (
            "Taxa básica da edificação escolhida pela seguradora",
            "Basic building rate selected by the insurer",
            "Tasa básica de la edificación seleccionada por la aseguradora",
        ),
        "additional_building_rate": (
            "Taxa adicional da edificação escolhida pela seguradora",
            "Additional building rate selected by the insurer",
            "Tasa adicional de la edificación seleccionada por la aseguradora",
        ),
        "basic_contents_rate": (
            "Taxa básica do conteúdo escolhida pela seguradora",
            "Basic contents rate selected by the insurer",
            "Tasa básica del contenido seleccionada por la aseguradora",
        ),
        "additional_contents_rate": (
            "Taxa adicional do conteúdo escolhida pela seguradora",
            "Additional contents rate selected by the insurer",
            "Tasa adicional del contenido seleccionada por la aseguradora",
        ),
        "subsidized_rate_type": (
            "Código que indica se a apólice foi tarifada com subsídio e de que tipo",
            "Code indicating whether and how the policy was subsidised in rating",
            "Código que indica si la póliza fue tarificada con subsidio y de qué "
            "tipo",
        ),
        "grandfathering_type_code": (
            "Código do tipo de grandfathering aplicado à tarifação, quando houver",
            "Code for the type of grandfathering applied in rating, where any "
            "applies",
            "Código del tipo de grandfathering aplicado a la tarificación, cuando "
            "corresponda",
        ),
        "insurance_to_value_code": (
            "Código da razão entre a cobertura contratada para a edificação e seu "
            "custo de reposição",
            "Code for the ratio of building coverage purchased to the building's "
            "replacement cost value",
            "Código de la razón entre la cobertura contratada para la edificación "
            "y su costo de reposición",
        ),
        "enclosure_type_code": (
            "Código que indica a existência e o tipo de área fechada abaixo do "
            "piso elevado mais baixo",
            "Code for whether there is an enclosure below the lowest elevated "
            "floor, and of what type",
            "Código que indica la existencia y el tipo de área cerrada bajo el "
            "piso elevado más bajo",
        ),
        "waiting_period_type": (
            "Código do tipo de carência aplicado à emissão da apólice",
            "Code for the type of waiting period applied when issuing the policy",
            "Código del tipo de carencia aplicado a la emisión de la póliza",
        ),
        "rollover_transfer_code": (
            "Código do tipo de novo negócio informado pela seguradora",
            "Code for the type of new business reported by the insurer",
            "Código del tipo de nuevo negocio informado por la aseguradora",
        ),
        "regular_emergency_program_indicator": (
            "Código da fase do NFIP em que a comunidade participa, que determina a "
            "cobertura disponível",
            "Code for the NFIP phase the community participates in, which "
            "determines the coverage available",
            "Código de la fase del NFIP en que participa la comunidad, que "
            "determina la cobertura disponible",
        ),
        "program_type_indicator": (
            "Indica se o imóvel se qualifica para a cobertura do programa de "
            "emergência em vez da cobertura regular",
            "Whether the property qualifies for emergency program coverage rather "
            "than regular coverage",
            "Indica si el inmueble califica para la cobertura del programa de "
            "emergencia en lugar de la cobertura regular",
        ),
        "mandatory_purchase_flag": (
            "Indica se a apólice é exigida pelo credor hipotecário",
            "Whether the policy is required by the mortgage lender",
            "Indica si la póliza es exigida por el acreedor hipotecario",
        ),
        "tenant_indicator": (
            "Indica que o segurado é locatário e não proprietário do imóvel",
            "Whether the policyholder is a tenant rather than the property owner",
            "Indica que el asegurado es arrendatario y no propietario del inmueble",
        ),
        "construction": (
            "Indica se a edificação está em obras",
            "Whether the building is under construction",
            "Indica si la edificación está en obras",
        ),
        "seasonally_occupied": (
            "Indica se a edificação atende à definição de ocupação sazonal do NFIP",
            "Whether the building meets the NFIP definition of seasonal occupancy",
            "Indica si la edificación cumple la definición de ocupación estacional "
            "del NFIP",
        ),
        "building_on_federal_land": (
            "Indica se a edificação atende à definição do NFIP de estar situada em "
            "terra federal",
            "Whether the building meets the NFIP definition of being located on "
            "federal land",
            "Indica si la edificación cumple la definición del NFIP de estar "
            "situada en tierra federal",
        ),
        "building_purpose": (
            "Código que indica se a edificação é residencial, não residencial ou "
            "de uso misto",
            "Code indicating whether the building is residential, non-residential "
            "or mixed use",
            "Código que indica si la edificación es residencial, no residencial o "
            "de uso mixto",
        ),
        "building_over_water_type": (
            "Código que indica se a edificação segurada está total, parcial ou "
            "nada sobre a água",
            "Code indicating whether the insured building is fully, partially or "
            "not at all over water",
            "Código que indica si la edificación asegurada está total, parcial o "
            "nada sobre el agua",
        ),
        "post_firm_vzone_indicator": (
            "Indica, para a tarifação de zona V pós-FIRM de 1981, se foi "
            "apresentada certificação nos termos da seção 60.3(e)(4)",
            "Whether, for 1981 post-FIRM Zone V rating, certification was provided "
            "under section 60.3(e)(4)",
            "Indica, para la tarificación de zona V post-FIRM de 1981, si se "
            "presentó certificación conforme a la sección 60.3(e)(4)",
        ),
        "map_panel_number": (
            "Número do painel do FIRM do imóvel, corrente ou usado na tarifação",
            "FIRM map panel number for the property, either current or used for "
            "rating",
            "Número del panel del FIRM del inmueble, corriente o usado en la "
            "tarificación",
        ),
        "map_panel_suffix": (
            "Sufixo do painel do FIRM do imóvel, corrente ou usado na tarifação",
            "FIRM map panel suffix for the property, either current or used for "
            "rating",
            "Sufijo del panel del FIRM del inmueble, corriente o usado en la "
            "tarificación",
        ),
        "fema_region": (
            "Número da região da FEMA em que o imóvel segurado se situa",
            "Number of the FEMA region in which the insured property is located",
            "Número de la región de FEMA en que se sitúa el inmueble asegurado",
        ),
    }
)

# ---------------------------------------------------------------------------
# Measurement units. Every INT64/FLOAT64 column must appear here; the build
# fails otherwise. A blank value is a deliberate declaration that the quantity
# has no unit slug in the backend, and OBSERVATIONS must then say so.
# ---------------------------------------------------------------------------
UNIT: dict[str, str] = {
    "year": "year",
    "fy_declared": "year",
    # money
    **{
        c: "usd"
        for c in (
            "project_amount",
            "federal_share_obligated",
            "total_obligated",
            "mitigation_amount",
            "amount_paid_on_building_claim",
            "amount_paid_on_contents_claim",
            "amount_paid_on_increased_cost_of_compliance_claim",
            "net_building_payment_amount",
            "net_contents_payment_amount",
            "net_icc_payment_amount",
            "building_damage_amount",
            "contents_damage_amount",
            "building_property_value",
            "contents_property_value",
            "building_replacement_cost",
            "contents_replacement_cost",
            "total_building_insurance_coverage",
            "total_contents_insurance_coverage",
            "icc_coverage",
            "total_salvage_recovery",
            "total_bldg_claim_pmt_recovery",
            "total_contents_claim_pmt_recovery",
            "total_icc_claim_pmt_recovery",
            "total_subrogation_recovery",
            "policy_cost",
            "total_insurance_premium_of_the_policy",
            "full_risk_premium",
            "federal_policy_fee",
            "hfiaa_surcharge",
            "icc_premium",
            "reserve_fund_assessment",
            "community_probation_surcharge",
            "basic_building_rate",
            "additional_building_rate",
            "basic_contents_rate",
            "additional_contents_rate",
        )
    },
    # elevations, in feet
    **{
        c: "foot"
        for c in (
            "base_flood_elevation",
            "lowest_adjacent_grade",
            "lowest_floor_elevation",
            "elevation_difference",
        )
    },
    # water depth, in inches
    **{
        c: "inch"
        for c in (
            "water_depth",
            "exterior_water_depth",
            "interior_water_depth",
        )
    },
    "flood_water_duration": "hour",
    # Quantities the backend has no MeasurementUnit slug for. Blank on purpose.
    "latitude": "",
    "longitude": "",
    "policy_count": "",
    "number_of_units": "",
    "pw_number": "",
    "region": "",
    "fema_region": "",
}

# ---------------------------------------------------------------------------
# Columns whose stored values are opaque codes resolved by `dicionario`.
# STRING regardless of how the source stores them — arithmetic is meaningless.
# ---------------------------------------------------------------------------
CODED: frozenset[str] = frozenset(
    {
        # disaster_declaration
        "declaration_type",
        # public_assistance_project
        "damage_category_code",
        # NOTE deliberately absent, because their stored values are already
        # readable labels rather than codes: incident_type ("Hurricane"),
        # project_status ("Eligible"), project_process_step ("Project Closed
        # Out"), project_size ("Small"). covered_by_dictionary marks codes that
        # need a legend, not "is this categorical".
        # NFIP, shared
        "basement_enclosure_crawlspace_type",
        "crs_class_code",
        "elevation_certificate_indicator",
        "rated_flood_zone",
        "flood_zone_current",
        "location_of_contents",
        "number_of_floors_in_insured_building",
        "obstruction_type",
        "occupancy_type",
        "rate_method",
        "building_deductible_code",
        "contents_deductible_code",
        "condominium_coverage_type_code",
        "disaster_assistance_coverage_required",
        "building_description_code",
        "foundation_type",
        # nfip_claim
        "cause_of_damage",
        "flood_characteristics_indicator",
        "non_payment_reason_building",
        "non_payment_reason_contents",
        "replacement_cost_basis",
        # nfip_policy
        "cancellation_voidance_reason_code",
        "policy_term_indicator",
        "premium_payment_indicator",
        "subsidized_rate_type",
        "grandfathering_type_code",
        "insurance_to_value_code",
        "enclosure_type_code",
        "waiting_period_type",
        "rollover_transfer_code",
        "regular_emergency_program_indicator",
        "building_purpose",
        "building_over_water_type",
    }
)

# Numeric-typed in the source but an identifier, not a quantity -> STRING and
# not dictionary-covered (nothing labels them; they identify).
IDENTIFIER: frozenset[str] = frozenset(
    {
        "disaster_number",
        "gm_project_id",
        "gm_applicant_id",
        "fico_number",
        "claim_id",
        "policy_id",
    }
)

# ---------------------------------------------------------------------------
# Directory foreign keys. The target must be the directory table's own primary
# key and the string uses the BACKEND slug (`diretorios_us`, not
# `br_bd_diretorios_us`). `state_abbreviation` is deliberately absent: the US
# state directory is keyed on `id_state` (the FIPS code), so a USPS
# abbreviation cannot be linked and is checked with a dbt test instead.
# ---------------------------------------------------------------------------
DIRECTORY: dict[str, str] = {
    "year": "diretorios_data_tempo.ano:ano",
    "state_id": "diretorios_us.state:id_state",
    "county_id": "diretorios_us.county:id_county",
}

# ---------------------------------------------------------------------------
# Per-column notes, in the three languages. Used for caveats a user must know
# before computing with the column.
# ---------------------------------------------------------------------------
OBSERVATIONS: dict[str, tuple[str, str, str]] = {
    "latitude": (
        "Sem unidade de medida cadastrada no back-end (graus decimais). "
        "Arredondada a uma casa decimal na origem: é a localização aproximada, "
        "não o endereço do imóvel",
        "No measurement unit registered in the backend (decimal degrees). "
        "Rounded to one decimal place at source: this is an approximate "
        "location, not the property address",
        "Sin unidad de medida registrada en el back-end (grados decimales). "
        "Redondeada a un decimal en el origen: es la ubicación aproximada, no "
        "la dirección del inmueble",
    ),
    "longitude": (
        "Sem unidade de medida cadastrada no back-end (graus decimais). "
        "Arredondada a uma casa decimal na origem: é a localização aproximada, "
        "não o endereço do imóvel",
        "No measurement unit registered in the backend (decimal degrees). "
        "Rounded to one decimal place at source: this is an approximate "
        "location, not the property address",
        "Sin unidad de medida registrada en el back-end (grados decimales). "
        "Redondeada a un decimal en el origen: es la ubicación aproximada, no "
        "la dirección del inmueble",
    ),
    "policy_count": (
        "Contagem sem unidade de medida cadastrada no back-end",
        "A count, with no measurement unit registered in the backend",
        "Conteo sin unidad de medida registrada en el back-end",
    ),
    "number_of_units": (
        "Contagem sem unidade de medida cadastrada no back-end",
        "A count, with no measurement unit registered in the backend",
        "Conteo sin unidad de medida registrada en el back-end",
    ),
    "pw_number": (
        "Número sequencial sem unidade de medida cadastrada no back-end",
        "A sequence number, with no measurement unit registered in the backend",
        "Número secuencial sin unidad de medida registrada en el back-end",
    ),
    "region": (
        "Número da região da FEMA, de 1 a 10; sem unidade de medida cadastrada "
        "no back-end",
        "FEMA region number, 1 to 10; no measurement unit registered in the "
        "backend",
        "Número de la región de FEMA, de 1 a 10; sin unidad de medida "
        "registrada en el back-end",
    ),
    "fema_region": (
        "Número da região da FEMA; sem unidade de medida cadastrada no back-end",
        "FEMA region number; no measurement unit registered in the backend",
        "Número de la región de FEMA; sin unidad de medida registrada en el "
        "back-end",
    ),
    "county_id": (
        "Derivado na limpeza; nulo quando a fonte não identifica um condado",
        "Derived during cleaning; null where the source identifies no county",
        "Derivado en la limpieza; nulo cuando la fuente no identifica un "
        "condado",
    ),
    "census_tract_id": (
        "Derivado dos onze primeiros dígitos de census_geoid. A vintage do "
        "código censitário não é documentada pela FEMA, portanto não há ligação "
        "com o diretório de setores censitários",
        "Derived from the first eleven digits of census_geoid. FEMA does not "
        "document the census vintage, so this is not linked to the census "
        "tract directory",
        "Derivado de los once primeros dígitos de census_geoid. La vintage del "
        "código censal no está documentada por FEMA, por lo que no hay enlace "
        "con el directorio de sectores censales",
    ),
    "census_block_group_id": (
        "Doze dígitos: estado, condado, setor censitário e grupo de quadras",
        "Twelve digits: state, county, census tract and block group",
        "Doce dígitos: estado, condado, sector censal y grupo de manzanas",
    ),
    "state_abbreviation": (
        "Não vinculado ao diretório de estados, que é chaveado pelo código "
        "FIPS e não pela sigla; a integridade é verificada por teste dbt",
        "Not linked to the state directory, which is keyed on the FIPS code "
        "rather than the abbreviation; integrity is checked by a dbt test",
        "No vinculado al directorio de estados, que está indexado por el "
        "código FIPS y no por la sigla; la integridad se verifica con una "
        "prueba dbt",
    ),
    "reported_zip_code": (
        "Normalizado para cinco dígitos: strings vazias viram nulo e valores "
        "ZIP+4 são truncados",
        "Normalised to five digits: empty strings become null and ZIP+4 values "
        "are truncated",
        "Normalizado a cinco dígitos: las cadenas vacías se convierten en nulo "
        "y los valores ZIP+4 se truncan",
    ),
}
