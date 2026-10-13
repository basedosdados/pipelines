"""Dataset, table and observation-level metadata for us_eia_seds.

Pure data: the dataset record, the table records, the observation levels, the
coverage and the tags, in Portuguese, English and Spanish. ``register.py`` reads
this and writes it to a backend; nothing here talks to the network, so the text
can be reviewed as text.

Column metadata is **not** here — it comes from the architecture CSV via
``gen_columns_json.py``, so the backend, the dbt model and the transform all
describe one schema.
"""

DATASET_SLUG = "seds"
GCP_DATASET_ID = "us_eia_seds"

DATASET = {
    "slug": DATASET_SLUG,
    "name_pt": "Sistema de Dados de Energia por Estado dos EUA (SEDS)",
    "name_en": "United States State Energy Data System (SEDS)",
    "name_es": "Sistema de Datos de Energía por Estado de EE. UU. (SEDS)",
    "description_pt": (
        "State Energy Data System (SEDS) da U.S. Energy Information Administration: as "
        "estimativas completas de consumo, preço, despesa e emissões de CO2 de energia por "
        "estado, cobrindo todas as fontes de energia de 1960 ao ano completo mais recente. "
        "Cada série é identificada pelo Mnemonic Series Name (MSN) de cinco caracteres, cujo "
        "significado completo está na tabela dicionario; como as unidades variam por série, "
        "a coluna value não carrega uma unidade única e a unidade de cada linha está em "
        "measurement_unit. Este conjunto é o lado da demanda por todas as fontes de energia, "
        "complementar ao us_eia_electricity (geração de eletricidade) e ao us_eia_consumption "
        "(vendas de eletricidade a consumidores finais pelo Formulário EIA-861)."
    ),
    "description_en": (
        "The U.S. Energy Information Administration's State Energy Data System (SEDS): the "
        "complete estimates of energy consumption, price, expenditure and CO2 emissions by "
        "state, covering every energy source from 1960 to the latest complete year. Each "
        "series is identified by its five-character Mnemonic Series Name (MSN), whose full "
        "meaning is in the dicionario table; because units vary by series, the value column "
        "carries no single unit and each row's unit is in measurement_unit. This dataset is "
        "the demand side across all energy sources, complementary to us_eia_electricity "
        "(electricity generation) and us_eia_consumption (retail electricity sales to "
        "end-use customers from Form EIA-861)."
    ),
    "description_es": (
        "El Sistema de Datos de Energía por Estado (SEDS) de la U.S. Energy Information "
        "Administration: las estimaciones completas de consumo, precio, gasto y emisiones de "
        "CO2 de energía por estado, cubriendo todas las fuentes de energía desde 1960 hasta "
        "el año completo más reciente. Cada serie se identifica por su nombre mnemónico de "
        "serie (MSN) de cinco caracteres, cuyo significado completo está en la tabla "
        "dicionario; como las unidades varían por serie, la columna value no lleva una "
        "unidad única y la unidad de cada fila está en measurement_unit. Este conjunto es el "
        "lado de la demanda para todas las fuentes de energía, complementario a "
        "us_eia_electricity (generación de electricidad) y us_eia_consumption (ventas "
        "minoristas de electricidad del Formulario EIA-861)."
    ),
    "organization_slugs": ["eia"],
    "theme_slugs": ["energy"],
    # Existing content tags only — each describes what the data measures, not
    # metadata the dataset already carries as a structured field (area, theme,
    # organization). Verified present on staging.
    "tag_slugs": [
        "consumo",
        "preco",
        "despesa",
        "combustivel",
        "petroleo",
        "gas_natural",
        "eletricidade",
        "emissao",
    ],
}

RAW_SOURCES = {
    "seds": {
        "name_pt": "State Energy Data System (SEDS) — estimativas completas",
        "name_en": "State Energy Data System (SEDS) — complete estimates",
        "name_es": "State Energy Data System (SEDS) — estimaciones completas",
        "url": "https://www.eia.gov/state/seds/seds-data-complete.php",
        "description_pt": (
            "Um único arquivo CSV, Complete_SEDS.csv, em formato longo (Data_Status, MSN, "
            "StateCode, Year, Data), com toda a série do sistema de 1960 em diante, "
            "acompanhado da planilha Codes_and_Descriptions.xlsx que decodifica os nomes "
            "mnemônicos de série e os códigos de estado. A EIA restaura a série completa a "
            "cada nova safra anual"
        ),
        "description_en": (
            "A single CSV, Complete_SEDS.csv, in long form (Data_Status, MSN, StateCode, "
            "Year, Data), holding the entire system's series from 1960 onward, alongside the "
            "Codes_and_Descriptions.xlsx workbook that decodes the mnemonic series names and "
            "state codes. EIA restates the whole series with each new annual vintage"
        ),
        "description_es": (
            "Un único archivo CSV, Complete_SEDS.csv, en formato largo (Data_Status, MSN, "
            "StateCode, Year, Data), con toda la serie del sistema desde 1960 en adelante, "
            "junto con la hoja Codes_and_Descriptions.xlsx que decodifica los nombres "
            "mnemónicos de serie y los códigos de estado. La EIA rehace la serie completa con "
            "cada nueva cosecha anual"
        ),
    },
}

# Exactly one raw source per table: client._raw_source_id raises when a table has
# two or more, which would make the recurring pipeline unable to poll at all.
TABLE_RAW_SOURCE = {
    "seds_consumption": "seds",
    "dicionario": "seds",
}

TABLES = {
    "seds_consumption": {
        "name_pt": "Consumo de energia por estado",
        "name_en": "State energy consumption",
        "name_es": "Consumo de energía por estado",
        "description_pt": (
            "Uma linha por estado, ano e série do State Energy Data System: consumo, preço, "
            "despesa, emissões de CO2 e indicadores relacionados de energia por estado, "
            "cobrindo todas as fontes de energia de 1960 ao ano completo mais recente. A "
            "série é identificada pelo Mnemonic Series Name (MSN) de cinco caracteres, cujo "
            "significado completo está na tabela dicionario. A coluna value não carrega uma "
            "unidade única porque a unidade varia por série; a unidade de cada linha está em "
            "measurement_unit e a família da medida em measure_type. state_code cobre os 50 "
            "estados, o Distrito de Columbia, o total nacional (US) e as áreas federais "
            "offshore (X3, X5); state_id resolve o código FIPS apenas para os estados"
        ),
        "description_en": (
            "One row per state, year and State Energy Data System series: energy "
            "consumption, price, expenditure, CO2 emissions and related indicators by state, "
            "covering every energy source from 1960 to the latest complete year. The series "
            "is identified by its five-character Mnemonic Series Name (MSN), whose full "
            "meaning is in the dicionario table. The value column carries no single unit "
            "because the unit varies by series; each row's unit is in measurement_unit and "
            "the measure family in measure_type. state_code covers the 50 states, the "
            "District of Columbia, the national total (US) and the federal offshore areas "
            "(X3, X5); state_id resolves the FIPS code for the states only"
        ),
        "description_es": (
            "Una fila por estado, año y serie del State Energy Data System: consumo, precio, "
            "gasto, emisiones de CO2 e indicadores relacionados de energía por estado, "
            "cubriendo todas las fuentes de energía desde 1960 hasta el año completo más "
            "reciente. La serie se identifica por su nombre mnemónico de serie (MSN) de cinco "
            "caracteres, cuyo significado completo está en la tabla dicionario. La columna "
            "value no lleva una unidad única porque la unidad varía por serie; la unidad de "
            "cada fila está en measurement_unit y la familia de la medida en measure_type. "
            "state_code cubre los 50 estados, el Distrito de Columbia, el total nacional (US) "
            "y las áreas federales costa afuera (X3, X5); state_id resuelve el código FIPS "
            "solo para los estados"
        ),
        "observation_levels": ["year", "state"],
        "level_columns": {"year": "year", "state": "state_id"},
    },
    "dicionario": {
        "name_pt": "Dicionário",
        "name_en": "Dictionary",
        "name_es": "Diccionario",
        "description_pt": (
            "Registro dos valores assumidos pelas colunas codificadas da tabela "
            "seds_consumption, com o significado de cada código e a cobertura temporal em que "
            "ele aparece. Inclui as descrições completas dos nomes mnemônicos de série (MSN), "
            "os códigos de estado e agregado, os tipos de medida e as safras dos dados"
        ),
        "description_en": (
            "A register of the values taken by the coded columns of the seds_consumption "
            "table, with the meaning of each code and the years over which it appears. It "
            "includes the full descriptions of the mnemonic series names (MSN), the state and "
            "aggregate codes, the measure types and the data vintages"
        ),
        "description_es": (
            "Registro de los valores que toman las columnas codificadas de la tabla "
            "seds_consumption, con el significado de cada código y la cobertura temporal en "
            "que aparece. Incluye las descripciones completas de los nombres mnemónicos de "
            "serie (MSN), los códigos de estado y agregado, los tipos de medida y las "
            "cosechas de los datos"
        ),
        "observation_levels": [],
        "level_columns": {},
    },
}
