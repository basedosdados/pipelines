"""Dataset, table and observation-level metadata for us_eia_electricity.

Pure data: the dataset record, the five table records, the observation levels,
the coverage and the tags, in Portuguese, English and Spanish. ``register.py``
reads this and writes it to a backend; nothing here talks to the network, so the
text can be reviewed as text.

Column metadata is **not** here — it comes from the architecture CSVs via
``gen_columns_json.py``, so the backend, the dbt models and the transform all
describe one schema.
"""

DATASET_SLUG = "electricity"
GCP_DATASET_ID = "us_eia_electricity"

DATASET = {
    "slug": DATASET_SLUG,
    "name_pt": "Setor Elétrico dos Estados Unidos (EIA-860 e EIA-923)",
    "name_en": "United States Electric Power Sector (EIA-860 and EIA-923)",
    "name_es": "Sector Eléctrico de los Estados Unidos (EIA-860 y EIA-923)",
    "description_pt": (
        "Microdados dos formulários EIA-860 e EIA-923 da U.S. Energy Information "
        "Administration, que juntos descrevem o setor elétrico dos Estados Unidos: o "
        "inventário anual de usinas e geradores, com localização, capacidade, máquina "
        "primária, combustíveis, tecnologia de combustão e datas de entrada em operação e "
        "de aposentadoria, e a declaração mensal de geração líquida, consumo de "
        "combustível e entregas de combustível com custo entregue na usina. plant_id e "
        "utility_id são os identificadores atribuídos pela EIA e são as chaves de junção "
        "estáveis entre os dois formulários e ao longo do tempo. As tabelas trazem a "
        "microdata como declarada: onde a EIA não coletou um campo em determinado ano, a "
        "coluna fica nula em vez de ser imputada a partir de outros anos."
    ),
    "description_en": (
        "Microdata from the U.S. Energy Information Administration's Forms EIA-860 and "
        "EIA-923, which together describe the United States electric power sector: the "
        "annual inventory of plants and generators, with location, capacity, prime mover, "
        "fuels, combustion technology and the dates each generator entered service and "
        "retired, and the monthly report of net generation, fuel consumption and fuel "
        "deliveries with the cost delivered to the plant. plant_id and utility_id are the "
        "identifiers EIA assigns and are the stable join keys across both forms and over "
        "time. The tables carry the microdata as filed: where EIA did not collect a field "
        "in a given year the column is null rather than imputed from other years."
    ),
    "description_es": (
        "Microdatos de los formularios EIA-860 y EIA-923 de la U.S. Energy Information "
        "Administration, que en conjunto describen el sector eléctrico de los Estados "
        "Unidos: el inventario anual de plantas y generadores, con ubicación, capacidad, "
        "máquina primaria, combustibles, tecnología de combustión y las fechas de entrada "
        "en operación y de retiro, y la declaración mensual de generación neta, consumo de "
        "combustible y entregas de combustible con el costo entregado en la planta. "
        "plant_id y utility_id son los identificadores asignados por la EIA y son las "
        "claves de unión estables entre ambos formularios y a lo largo del tiempo. Las "
        "tablas traen los microdatos tal como fueron declarados: donde la EIA no recolectó "
        "un campo en un año dado, la columna queda nula en vez de imputada desde otros "
        "años."
    ),
    "organization_slugs": ["eia"],
    "theme_slugs": ["energy", "infrastructure-transportation"],
    # Existing tags only — every one describes the content rather than repeating
    # metadata the dataset already carries as a structured field (area, theme,
    # organization). No new tag is needed for this dataset.
    "tag_slugs": [
        "eletricidade",
        "usina",
        "combustivel",
        "gas_natural",
        "petroleo",
        "empresa",
        "preco",
        "consumo",
    ],
}

RAW_SOURCES = {
    "eia860": {
        "name_pt": "Formulário EIA-860 — inventário anual de geradores",
        "name_en": "Form EIA-860 — Annual Electric Generator Report",
        "name_es": "Formulario EIA-860 — inventario anual de generadores",
        "url": "https://www.eia.gov/electricity/data/eia860/",
        "description_pt": (
            "Um arquivo ZIP por ano de referência, com planilhas Excel de empresas, "
            "usinas, geradores em operação, propostos e aposentados, propriedade e "
            "equipamento ambiental, além do formulário em branco, das instruções e do "
            "layout de colunas"
        ),
        "description_en": (
            "One ZIP per report year, holding Excel workbooks for utilities, plants, "
            "operable, proposed and retired generators, ownership and environmental "
            "equipment, plus the blank form, the instructions and the column layout"
        ),
        "description_es": (
            "Un archivo ZIP por año de referencia, con hojas de cálculo de empresas, "
            "plantas, generadores en operación, propuestos y retirados, propiedad y equipo "
            "ambiental, además del formulario en blanco, las instrucciones y el diseño de "
            "columnas"
        ),
    },
    "eia923": {
        "name_pt": "Formulário EIA-923 — operação mensal de usinas",
        "name_en": "Form EIA-923 — Power Plant Operations Report",
        "name_es": "Formulario EIA-923 — operación mensual de plantas",
        "url": "https://www.eia.gov/electricity/data/eia923/",
        "description_pt": (
            "Um arquivo ZIP por ano de referência, republicado mensalmente durante o ano "
            "corrente e novamente como early release e como revisão final. Os anos de 2001 "
            "a 2007 vêm dos formulários antecessores EIA-906 e EIA-920"
        ),
        "description_en": (
            "One ZIP per report year, republished monthly during the current year and again "
            "as an early release and as a final revision. Report years 2001 to 2007 come "
            "from the predecessor Forms EIA-906 and EIA-920"
        ),
        "description_es": (
            "Un archivo ZIP por año de referencia, republicado mensualmente durante el año "
            "corriente y nuevamente como early release y como revisión final. Los años 2001 "
            "a 2007 provienen de los formularios antecesores EIA-906 y EIA-920"
        ),
    },
}

# Exactly one raw source per table: client._raw_source_id raises when a table has
# two or more, which would make the recurring pipeline unable to poll at all.
TABLE_RAW_SOURCE = {
    "plant": "eia860",
    "generator": "eia860",
    "generation_fuel": "eia923",
    "fuel_receipts_costs": "eia923",
    "dicionario": "eia860",
}

TABLES = {
    "plant": {
        "name_pt": "Usina",
        "name_en": "Plant",
        "name_es": "Planta",
        "description_pt": (
            "Um registro por usina de geração de eletricidade por ano-calendário, conforme o "
            "Anexo 2 do Formulário EIA-860: localização, empresa operadora, autoridade de "
            "balanceamento, situação regulatória, setor econômico e infraestrutura "
            "associada. plant_id é a chave de junção estável entre todas as tabelas deste "
            "conjunto e entre os dois formulários. O escopo do formulário cresceu ao longo "
            "do período — endereço passa a ser coletado em 2007, coordenadas e "
            "infraestrutura de gás em 2013 —, de modo que uma coluna vazia num ano antigo "
            "indica ausência de coleta e não ausência de informação"
        ),
        "description_en": (
            "One row per electricity generating plant per calendar year, from Schedule 2 of "
            "Form EIA-860: location, operating utility, balancing authority, regulatory "
            "status, economic sector and associated infrastructure. plant_id is the stable "
            "join key across every table in this dataset and between the two forms. The "
            "form's scope grew over the period — street address from 2007, coordinates and "
            "gas infrastructure from 2013 — so an empty column in an early year means the "
            "field was not collected, not that the information is missing"
        ),
        "description_es": (
            "Una fila por planta de generación eléctrica por año calendario, según el Anexo "
            "2 del Formulario EIA-860: ubicación, empresa operadora, autoridad de balance, "
            "situación regulatoria, sector económico e infraestructura asociada. plant_id es "
            "la clave de unión estable entre todas las tablas de este conjunto y entre ambos "
            "formularios. El alcance del formulario creció a lo largo del período — "
            "dirección desde 2007, coordenadas e infraestructura de gas desde 2013 —, de "
            "modo que una columna vacía en un año antiguo indica ausencia de recolección y "
            "no ausencia de información"
        ),
        "observation_levels": ["year", "plant"],
        "level_columns": {"year": "year", "plant": "plant_id"},
    },
    "generator": {
        "name_pt": "Gerador",
        "name_en": "Generator",
        "name_es": "Generador",
        "description_pt": (
            "Um registro por gerador por ano-calendário, conforme o Anexo 3 do Formulário "
            "EIA-860: capacidade nominal, de verão e de inverno, máquina primária, "
            "combustíveis, datas de entrada em operação e de aposentadoria, situação "
            "operacional e tecnologia de combustão. A chave é (year, plant_id, "
            "generator_id), porque generator_id é único apenas dentro de uma usina. A partir "
            "de 2009 o formulário separa geradores em operação, propostos e aposentados em "
            "planilhas distintas, unidas aqui em uma tabela só com generator_status_group "
            "registrando a origem: somar capacity_mw sem filtrar essa coluna soma capacidade "
            "instalada, proposta e aposentada no mesmo número"
        ),
        "description_en": (
            "One row per generator per calendar year, from Schedule 3 of Form EIA-860: "
            "nameplate, summer and winter capacity, prime mover, fuels, the dates it entered "
            "service and retired, operational status and combustion technology. The key is "
            "(year, plant_id, generator_id), because generator_id is unique only within a "
            "plant. From 2009 the form splits operable, proposed and retired generators "
            "across three sheets, unioned here into one table with generator_status_group "
            "recording which: summing capacity_mw without filtering that column adds "
            "installed, proposed and retired capacity into one number"
        ),
        "description_es": (
            "Una fila por generador por año calendario, según el Anexo 3 del Formulario "
            "EIA-860: capacidad nominal, de verano y de invierno, máquina primaria, "
            "combustibles, fechas de entrada en operación y de retiro, situación operativa y "
            "tecnología de combustión. La clave es (year, plant_id, generator_id), porque "
            "generator_id es único solo dentro de una planta. Desde 2009 el formulario "
            "separa generadores en operación, propuestos y retirados en hojas distintas, "
            "unidas aquí en una sola tabla con generator_status_group registrando el origen: "
            "sumar capacity_mw sin filtrar esa columna suma capacidad instalada, propuesta y "
            "retirada en un mismo número"
        ),
        "observation_levels": ["year", "plant", "equipment"],
        "level_columns": {
            "year": "year",
            "plant": "plant_id",
            "equipment": "generator_id",
        },
    },
    "generation_fuel": {
        "name_pt": "Geração e consumo de combustível",
        "name_en": "Generation and fuel consumption",
        "name_es": "Generación y consumo de combustible",
        "description_pt": (
            "Geração líquida de eletricidade e consumo de combustível por usina, "
            "combustível, máquina primária e mês, conforme a página 1 do Formulário EIA-923. "
            "A fonte publica esta página em formato largo, com doze colunas por medida e um "
            "total anual; aqui ela está em formato longo, uma linha por mês, e os totais "
            "anuais foram descartados por serem a soma das doze colunas mensais. As "
            "quantidades físicas de combustível estão em unidades que variam por "
            "combustível, indicadas em fuel_unit; para agregar entre combustíveis use as "
            "colunas em MMBtu. Respondentes anuais, identificados em "
            "reporting_frequency_code, declaram o total do ano na coluna de dezembro"
        ),
        "description_en": (
            "Net electricity generation and fuel consumption by plant, fuel, prime mover and "
            "month, from page 1 of Form EIA-923. The source publishes this page wide, with "
            "twelve columns per measure plus an annual total; here it is long, one row per "
            "month, and the annual totals are dropped because they are the sum of the twelve "
            "monthly columns. The physical fuel quantities are in units that vary by fuel, "
            "named per row in fuel_unit; use the MMBtu columns to aggregate across fuels. "
            "Annual respondents, identified in reporting_frequency_code, report the whole "
            "year in the December column"
        ),
        "description_es": (
            "Generación neta de electricidad y consumo de combustible por planta, "
            "combustible, máquina primaria y mes, según la página 1 del Formulario EIA-923. "
            "La fuente publica esta página en formato ancho, con doce columnas por medida y "
            "un total anual; aquí está en formato largo, una fila por mes, y los totales "
            "anuales fueron descartados por ser la suma de las doce columnas mensuales. Las "
            "cantidades físicas de combustible están en unidades que varían según el "
            "combustible, indicadas en fuel_unit; para agregar entre combustibles use las "
            "columnas en MMBtu. Los encuestados anuales, identificados en "
            "reporting_frequency_code, declaran el total del año en la columna de diciembre"
        ),
        "observation_levels": ["year", "month", "plant"],
        "level_columns": {
            "year": "year",
            "month": "month",
            "plant": "plant_id",
        },
    },
    "fuel_receipts_costs": {
        "name_pt": "Entregas e custos de combustível",
        "name_en": "Fuel receipts and costs",
        "name_es": "Entregas y costos de combustible",
        "description_pt": (
            "Uma linha por entrega de combustível a uma usina de geração, conforme o Anexo 2 "
            "Parte C do Formulário EIA-923: quantidade recebida, conteúdo energético, custo "
            "entregue, fornecedor, mina de origem, modo de transporte e teores de enxofre, "
            "cinzas, umidade, mercúrio e cloro. O custo é publicado pela fonte em centavos de "
            "dólar por MMBtu e está convertido para dólares em fuel_cost_per_mmbtu; inclui "
            "transporte e está ausente onde a EIA o suprime por confidencialidade do "
            "respondente. Uma entrega não tem identificador publicado e duas entregas "
            "idênticas no mesmo mês são um fato da fonte, de modo que a tabela não afirma "
            "unicidade sobre nenhuma combinação de colunas"
        ),
        "description_en": (
            "One row per fuel delivery to a generating plant, from Schedule 2 Part C of Form "
            "EIA-923: quantity received, heat content, delivered cost, supplier, mine of "
            "origin, transport mode and sulfur, ash, moisture, mercury and chlorine content. "
            "The source publishes the cost in cents per MMBtu and it is converted to dollars "
            "in fuel_cost_per_mmbtu; it includes transport and is absent where EIA "
            "suppresses it for respondent confidentiality. A delivery has no published "
            "identifier and two identical deliveries in one month are a fact of the source, "
            "so the table asserts uniqueness over no combination of columns"
        ),
        "description_es": (
            "Una fila por entrega de combustible a una planta de generación, según el Anexo "
            "2 Parte C del Formulario EIA-923: cantidad recibida, contenido energético, "
            "costo entregado, proveedor, mina de origen, modo de transporte y contenidos de "
            "azufre, cenizas, humedad, mercurio y cloro. La fuente publica el costo en "
            "centavos de dólar por MMBtu y aquí está convertido a dólares en "
            "fuel_cost_per_mmbtu; incluye transporte y está ausente donde la EIA lo suprime "
            "por confidencialidad del encuestado. Una entrega no tiene identificador "
            "publicado y dos entregas idénticas en un mismo mes son un hecho de la fuente, "
            "de modo que la tabla no afirma unicidad sobre ninguna combinación de columnas"
        ),
        "observation_levels": ["year", "month", "plant", "delivery"],
        "level_columns": {
            "year": "year",
            "month": "month",
            "plant": "plant_id",
        },
    },
    "dicionario": {
        "name_pt": "Dicionário",
        "name_en": "Dictionary",
        "name_es": "Diccionario",
        "description_pt": (
            "Registro dos valores assumidos pelas colunas codificadas das demais tabelas, "
            "com o significado de cada código e a cobertura temporal em que ele aparece. Os "
            "rótulos vêm dos vocabulários publicados pela EIA, na forma em que o projeto "
            "Public Utility Data Liberation os mantém"
        ),
        "description_en": (
            "A register of the values taken by the coded columns of the other tables, with "
            "the meaning of each code and the years over which it appears. The labels come "
            "from EIA's published vocabularies, in the form the Public Utility Data "
            "Liberation project maintains them"
        ),
        "description_es": (
            "Registro de los valores que toman las columnas codificadas de las demás tablas, "
            "con el significado de cada código y la cobertura temporal en que aparece. Las "
            "etiquetas provienen de los vocabularios publicados por la EIA, en la forma en "
            "que el proyecto Public Utility Data Liberation los mantiene"
        ),
        "observation_levels": [],
        "level_columns": {},
    },
}
