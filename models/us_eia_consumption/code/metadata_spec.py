"""Dataset, table and observation-level metadata for us_eia_consumption.

Pure data: the dataset record, the four table records, the observation levels,
the coverage, the raw sources and the tags, in Portuguese, English and Spanish.
``register.py`` reads this and writes it to a backend; nothing here talks to the
network.

This dataset holds Form EIA-861 (the retail / demand side of US electricity).
SEDS lives in its own dataset, us_eia_seds. Column metadata comes from the
architecture CSVs via ``gen_columns_json.py``.
"""

DATASET_SLUG = "consumption"
GCP_DATASET_ID = "us_eia_consumption"

DATASET = {
    "slug": DATASET_SLUG,
    "name_pt": "Vendas de Eletricidade no Varejo nos Estados Unidos (EIA-861)",
    "name_en": "United States Retail Electricity Sales (EIA-861)",
    "name_es": "Ventas Minoristas de Electricidad en los Estados Unidos (EIA-861)",
    "description_pt": (
        "O lado da demanda da eletricidade nos Estados Unidos, a partir do Formulário "
        "EIA-861 da U.S. Energy Information Administration: um censo anual das "
        "distribuidoras e comercializadoras de eletricidade, com vendas, receita e número "
        "de consumidores por empresa, unidade federativa e setor de consumo (Anexo 4), as "
        "características de cada empresa e os condados que ela atende, mais o Formulário "
        "EIA-861M, que traz vendas, receita, consumidores e preço mensais por unidade "
        "federativa e setor de 1990 em diante. É o complemento de duas outras bases da "
        "EIA: us_eia_electricity, que cobre o lado da oferta de eletricidade (usinas, "
        "geradores e combustível) e junta-se a esta por utility_id, e us_eia_seds, o "
        "sistema estadual de consumo de energia de todos os combustíveis."
    ),
    "description_en": (
        "The demand side of electricity in the United States, from the U.S. Energy "
        "Information Administration's Form EIA-861: an annual census of electricity "
        "distributors and marketers, with sales, revenue and customer counts by utility, "
        "state and customer sector (Schedule 4), each utility's own characteristics, and "
        "the counties it serves, plus Form EIA-861M, which carries monthly sales, revenue, "
        "customers and price by state and sector from 1990 onward. It is the counterpart of "
        "two other EIA datasets: us_eia_electricity, which covers the electricity supply "
        "side (plants, generators and fuel) and joins to this one on utility_id, and "
        "us_eia_seds, the state energy consumption system across every fuel."
    ),
    "description_es": (
        "El lado de la demanda de electricidad en los Estados Unidos, a partir del "
        "Formulario EIA-861 de la U.S. Energy Information Administration: un censo anual de "
        "las distribuidoras y comercializadoras de electricidad, con ventas, ingresos y "
        "número de consumidores por empresa, estado y sector de consumo (Anexo 4), las "
        "características de cada empresa y los condados que atiende, además del Formulario "
        "EIA-861M, que trae ventas, ingresos, consumidores y precio mensuales por estado y "
        "sector desde 1990. Es el complemento de otras dos bases de la EIA: "
        "us_eia_electricity, que cubre el lado de la oferta de electricidad (plantas, "
        "generadores y combustible) y se une a esta por utility_id, y us_eia_seds, el "
        "sistema estatal de consumo de energía de todos los combustibles."
    ),
    "organization_slugs": ["eia"],
    "theme_slugs": ["energy"],
    "tag_slugs": [
        "eletricidade",
        "consumo",
        "preco",
        "receita",
        "empresa",
    ],
}

RAW_SOURCES = {
    "eia861": {
        "name_pt": "Formulário EIA-861 — relatório anual da indústria de energia elétrica",
        "name_en": "Form EIA-861 — Annual Electric Power Industry Report",
        "name_es": "Formulario EIA-861 — informe anual de la industria de energía eléctrica",
        "url": "https://www.eia.gov/electricity/data/eia861/",
        "description_pt": (
            "Um arquivo ZIP por ano de referência, com planilhas de vendas a consumidores "
            "finais, características das empresas, território de serviço e outros anexos. O "
            "leiaute mudou ao longo do tempo — arquivos file1/file2 de 2001 a 2011 e "
            "planilhas nomeadas de 2012 em diante — e a EIA renomeia o arquivo do ano "
            "corrente a cada publicação"
        ),
        "description_en": (
            "One ZIP per report year, with workbooks for sales to ultimate customers, "
            "utility characteristics, service territory and other schedules. The layout "
            "changed over time — file1/file2 workbooks from 2001 to 2011 and named "
            "workbooks from 2012 on — and EIA renames the current year's file on every "
            "release"
        ),
        "description_es": (
            "Un archivo ZIP por año de referencia, con hojas de ventas a consumidores "
            "finales, características de las empresas, territorio de servicio y otros "
            "anexos. El diseño cambió con el tiempo — archivos file1/file2 de 2001 a 2011 y "
            "hojas nombradas desde 2012 — y la EIA renombra el archivo del año corriente en "
            "cada publicación"
        ),
    },
    "eia861m": {
        "name_pt": "Formulário EIA-861M — vendas e receita mensais de eletricidade",
        "name_en": "Form EIA-861M — Monthly Electric Power Industry Report",
        "name_es": "Formulario EIA-861M — ventas e ingresos mensuales de electricidad",
        "url": "https://www.eia.gov/electricity/data/eia861m/",
        "description_pt": (
            "Uma planilha única com vendas, receita, número de consumidores e preço mensais "
            "de eletricidade a consumidores finais por unidade federativa e setor, de 1990 "
            "ao mês mais recente; a série de 1990 a 2009 vem de um arquivo histórico "
            "arquivado com o mesmo leiaute"
        ),
        "description_en": (
            "A single workbook of monthly electricity sales, revenue, customer counts and "
            "price to ultimate customers by state and sector, from 1990 to the latest "
            "month; the 1990-2009 series comes from an archived historical file with the "
            "same layout"
        ),
        "description_es": (
            "Una hoja única con ventas, ingresos, número de consumidores y precio mensuales "
            "de electricidad a consumidores finales por estado y sector, desde 1990 hasta "
            "el mes más reciente; la serie de 1990 a 2009 proviene de un archivo histórico "
            "archivado con el mismo diseño"
        ),
    },
}

TABLE_RAW_SOURCE = {
    "utility": "eia861",
    "retail_sales": "eia861",
    "service_territory": "eia861",
    "eia861m": "eia861m",
    "dicionario": "eia861",
}

TABLES = {
    "utility": {
        "name_pt": "Empresa",
        "name_en": "Utility",
        "name_es": "Empresa",
        "description_pt": (
            "Um registro por empresa de eletricidade por ano: nome, tipo de propriedade, "
            "unidade federativa e região de confiabilidade da NERC, conforme o quadro de "
            "empresas do Formulário EIA-861. utility_id é a chave de junção estável com "
            "us_eia_electricity e entre os formulários da EIA. O conjunto de empresas que "
            "preenchem o quadro varia de ano para ano"
        ),
        "description_en": (
            "One row per electricity utility per year: name, ownership type, state and NERC "
            "reliability region, from the utility frame of Form EIA-861. utility_id is the "
            "stable join key with us_eia_electricity and across EIA forms. The set of "
            "utilities filing the frame varies from year to year"
        ),
        "description_es": (
            "Una fila por empresa de electricidad por año: nombre, tipo de propiedad, "
            "estado y región de confiabilidad de la NERC, según el cuadro de empresas del "
            "Formulario EIA-861. utility_id es la clave de unión estable con "
            "us_eia_electricity y entre los formularios de la EIA. El conjunto de empresas "
            "que presentan el cuadro varía de año a año"
        ),
        "observation_levels": ["year", "company"],
        "level_columns": {"year": "year", "company": "utility_id"},
    },
    "retail_sales": {
        "name_pt": "Vendas no varejo",
        "name_en": "Retail sales",
        "name_es": "Ventas minoristas",
        "description_pt": (
            "Vendas de eletricidade a consumidores finais por empresa, unidade federativa, "
            "tipo de serviço e setor de consumo (Anexo 4 do Formulário EIA-861): receita, "
            "vendas em MWh, número de consumidores e um preço médio derivado. A tabela está "
            "em formato longo, uma linha por setor, e o setor Total foi descartado. Somar "
            "sobre a coluna service_type conta a mesma energia duas vezes — em mercados "
            "reestruturados ela aparece como Delivery e como Energy; para um total sem dupla "
            "contagem, some apenas Bundled e Energy, excluindo Delivery"
        ),
        "description_en": (
            "Electricity sales to end-use customers by utility, state, service type and "
            "customer sector (Schedule 4 of Form EIA-861): revenue, sales in MWh, customer "
            "counts and a derived average price. The table is long, one row per sector, and "
            "the Total sector is dropped. Summing over the service_type column double-counts "
            "energy — in restructured markets it appears as both Delivery and Energy; for a "
            "total without double-counting, sum only Bundled and Energy, excluding Delivery"
        ),
        "description_es": (
            "Ventas de electricidad a consumidores finales por empresa, estado, tipo de "
            "servicio y sector de consumo (Anexo 4 del Formulario EIA-861): ingresos, "
            "ventas en MWh, número de consumidores y un precio medio derivado. La tabla está "
            "en formato largo, una fila por sector, y el sector Total fue descartado. Sumar "
            "sobre la columna service_type cuenta la misma energía dos veces — en mercados "
            "reestructurados aparece como Delivery y como Energy; para un total sin doble "
            "conteo, sume solo Bundled y Energy, excluyendo Delivery"
        ),
        "observation_levels": ["year", "company", "state"],
        "level_columns": {
            "year": "year",
            "company": "utility_id",
            "state": "state_id",
        },
    },
    "service_territory": {
        "name_pt": "Território de serviço",
        "name_en": "Service territory",
        "name_es": "Territorio de servicio",
        "description_pt": (
            "Um registro por empresa, ano e condado atendido, conforme o arquivo de "
            "território de serviço do Formulário EIA-861, publicado como arquivo próprio a "
            "partir de 2012. county_id resolve o código FIPS do condado a partir do nome "
            "declarado, dentro da unidade federativa"
        ),
        "description_en": (
            "One row per utility, year and county served, from the service territory file "
            "of Form EIA-861, published as its own file from 2012 on. county_id resolves the "
            "county FIPS code from the reported name, within the state"
        ),
        "description_es": (
            "Una fila por empresa, año y condado atendido, según el archivo de territorio de "
            "servicio del Formulario EIA-861, publicado como archivo propio desde 2012. "
            "county_id resuelve el código FIPS del condado a partir del nombre declarado, "
            "dentro del estado"
        ),
        "observation_levels": ["year", "company", "county"],
        "level_columns": {
            "year": "year",
            "company": "utility_id",
            "county": "county_id",
        },
    },
    "eia861m": {
        "name_pt": "Vendas mensais (EIA-861M)",
        "name_en": "Monthly sales (EIA-861M)",
        "name_es": "Ventas mensuales (EIA-861M)",
        "description_pt": (
            "Vendas mensais de eletricidade a consumidores finais por unidade federativa e "
            "setor de consumo, conforme o Formulário EIA-861M: receita, vendas em MWh, "
            "número de consumidores e preço médio, de 1990 ao mês mais recente. O setor "
            "Other aparece apenas de 1990 a 2009. state_code cobre as unidades federativas, "
            "o total nacional e os territórios; state_id resolve o FIPS apenas para as "
            "unidades federativas"
        ),
        "description_en": (
            "Monthly electricity sales to end-use customers by state and customer sector, "
            "from Form EIA-861M: revenue, sales in MWh, customer counts and average price, "
            "from 1990 to the latest month. The Other sector appears only from 1990 to 2009. "
            "state_code covers the states, the national total and the territories; state_id "
            "resolves the FIPS code for the states only"
        ),
        "description_es": (
            "Ventas mensuales de electricidad a consumidores finales por estado y sector de "
            "consumo, según el Formulario EIA-861M: ingresos, ventas en MWh, número de "
            "consumidores y precio medio, desde 1990 hasta el mes más reciente. El sector "
            "Other aparece solo de 1990 a 2009. state_code cubre los estados, el total "
            "nacional y los territorios; state_id resuelve el FIPS solo para los estados"
        ),
        "observation_levels": ["year", "state"],
        "level_columns": {"year": "year", "state": "state_id"},
    },
    "dicionario": {
        "name_pt": "Dicionário",
        "name_en": "Dictionary",
        "name_es": "Diccionario",
        "description_pt": (
            "Registro dos valores assumidos pelas colunas codificadas das tabelas de vendas "
            "no varejo e de território de serviço, com o significado de cada código e a "
            "cobertura temporal em que ele aparece"
        ),
        "description_en": (
            "A register of the values taken by the coded columns of the retail sales and "
            "service territory tables, with the meaning of each code and the years over "
            "which it appears"
        ),
        "description_es": (
            "Registro de los valores que toman las columnas codificadas de las tablas de "
            "ventas minoristas y de territorio de servicio, con el significado de cada "
            "código y la cobertura temporal en que aparece"
        ),
        "observation_levels": [],
        "level_columns": {},
    },
}
