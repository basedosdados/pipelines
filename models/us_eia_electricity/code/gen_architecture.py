"""Write the architecture CSVs for us_eia_electricity — the schema source of truth.

    python gen_architecture.py

One CSV per table under ``architecture/``, in Data Basis architecture-sheet
column order. Everything downstream reads these files: the cleaning transform
takes column order and ``bigquery_type`` from them, ``gen_dbt.py`` writes the
dbt models and ``schema.yml`` from them, and ``gen_columns_json.py`` writes the
backend column payloads from them. Edit the column definitions **here**, never
the CSVs.

``original_name`` is the **PUDL canonical** column name the value is read from,
not a raw spreadsheet header: the raw header changes almost every year and the
vendored column maps under ``pudl/`` are what resolve it. A name starting with
``@`` marks a column this repository derives rather than reads.
"""

import csv
import json
from dataclasses import dataclass
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
OUT = CODE_DIR / "architecture"
# Written by verify_parquet.py from the parquet footers. When it is present, the
# first and last year each column is actually non-null overrides the coverage
# declared by hand below, so the published temporal_coverage is what the data
# shows rather than what the source's documentation claims. Absent on a first
# run, before any parquet exists.
MEASURED = CODE_DIR / "measured.json"

HEADER = [
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


@dataclass
class C:
    """One architecture row. ``d`` is the trilingual description, ``o`` the notes."""

    name: str
    type: str
    d: tuple[str, str, str]
    original: str
    unit: str = ""
    dictionary: bool = False
    directory: str = ""
    coverage: str = ""
    o: tuple[str, str, str] = ("", "", "")
    sensitive: bool = False

    def row(self) -> dict:
        return {
            "name": self.name,
            "bigquery_type": self.type,
            "description_pt": self.d[0],
            "description_en": self.d[1],
            "description_es": self.d[2],
            "temporal_coverage": self.coverage,
            "covered_by_dictionary": "yes" if self.dictionary else "no",
            "directory_column": self.directory,
            "measurement_unit": self.unit,
            "has_sensitive_data": "yes" if self.sensitive else "no",
            "observations_pt": self.o[0],
            "observations_en": self.o[1],
            "observations_es": self.o[2],
            "original_name": self.original,
        }


# --------------------------------------------------------------------------
# Column fragments shared by more than one table
# --------------------------------------------------------------------------

# The unit of a physical fuel quantity is not fixed: EIA reports coal in short
# tons, gas in thousand cubic feet and oil in barrels, all in the same column,
# with the unit named per row. Those columns therefore carry no measurement_unit
# and say why; the MMBtu columns beside them are the ones that can be summed
# across fuels.
MIXED_UNIT = (
    "A unidade física varia por combustível (toneladas curtas para carvão, "
    "mil pés cúbicos para gás, barris para líquidos) e é indicada em fuel_unit. "
    "Some as colunas em MMBtu, não esta, para agregar entre combustíveis",
    "The physical unit varies by fuel — short tons for coal, thousand cubic "
    "feet for gas, barrels for liquids — and is named per row in fuel_unit. Sum "
    "the MMBtu columns, not this one, to aggregate across fuels",
    "La unidad física varía según el combustible (toneladas cortas para carbón, "
    "miles de pies cúbicos para gas, barriles para líquidos) y se indica en "
    "fuel_unit. Sume las columnas en MMBtu, no esta, para agregar entre "
    "combustibles",
)

YEAR_MONTH_DAY_ONE = (
    "O formulário publica apenas mês e ano; o dia é fixado em 01",
    "The form publishes only a month and a year; the day is fixed at 01",
    "El formulario publica solo mes y año; el día se fija en 01",
)

COUNTY_ID_NOTE = (
    "Resolvido a partir do nome do condado e da sigla do estado contra "
    "br_bd_diretorios_us.county — o formulário publica o nome, nunca o código "
    "FIPS. Nulo quando o nome não resolve, sobretudo nos condados históricos de "
    "Connecticut, que o formulário ainda usa e que o diretório substituiu pelas "
    "regiões de planejamento de 2022",
    "Resolved from the published county name and state abbreviation against "
    "br_bd_diretorios_us.county — the form publishes a name, never a FIPS code. "
    "Null where the name does not resolve, chiefly the historical Connecticut "
    "counties, which the form still uses and which the directory replaced with "
    "the 2022 planning regions",
    "Resuelto a partir del nombre del condado y la sigla del estado contra "
    "br_bd_diretorios_us.county — el formulario publica el nombre, nunca el "
    "código FIPS. Nulo cuando el nombre no resuelve, sobre todo en los condados "
    "históricos de Connecticut, que el formulario aún usa y que el directorio "
    "reemplazó por las regiones de planificación de 2022",
)

DATA_MATURITY_NOTE = (
    "final = arquivo definitivo; provisional = early release, substituído por "
    "uma revisão final posterior; incremental_ytd = arquivo mensal do ano "
    "corrente, substituído a cada nova publicação. Derivado do nome do arquivo "
    "publicado, único lugar onde a EIA registra isso",
    "final = definitive file; provisional = an early release, later superseded "
    "by a final revision; incremental_ytd = a within-year monthly file, "
    "superseded by every later release. Derived from the published file name, "
    "the only place EIA records it",
    "final = archivo definitivo; provisional = early release, sustituido por "
    "una revisión final posterior; incremental_ytd = archivo mensual del año "
    "corriente, sustituido en cada nueva publicación. Derivado del nombre del "
    "archivo publicado, único lugar donde la EIA lo registra",
)

PLANT_ID_NOTE = (
    "Identificador de usina atribuído pela EIA. É a chave de junção estável "
    "entre todas as tabelas deste conjunto e entre os formulários EIA-860 e "
    "EIA-923; permanece com a usina através de mudanças de proprietário e de "
    "nome. STRING porque é um identificador, não uma quantidade",
    "EIA-assigned plant identifier. It is the stable join key across every "
    "table in this dataset and between Forms EIA-860 and EIA-923, and it stays "
    "with the plant through changes of owner and of name. STRING because it is "
    "an identifier, not a quantity",
    "Identificador de planta asignado por la EIA. Es la clave de unión estable "
    "entre todas las tablas de este conjunto y entre los formularios EIA-860 y "
    "EIA-923; permanece con la planta a través de cambios de propietario y de "
    "nombre. STRING porque es un identificador, no una cantidad",
)

UTILITY_ID_NOTE = (
    "Identificador de empresa atribuído pela EIA, estável entre formulários e "
    "anos. STRING porque é um identificador, não uma quantidade",
    "EIA-assigned utility identifier, stable across forms and years. STRING "
    "because it is an identifier, not a quantity",
    "Identificador de empresa asignado por la EIA, estable entre formularios y "
    "años. STRING porque es un identificador, no una cantidad",
)


def year_col(monthly: bool) -> C:
    return C(
        "year",
        "INT64",
        (
            "Ano-calendário do relatório"
            if monthly
            else "Ano-calendário do inventário",
            "Report calendar year" if monthly else "Inventory calendar year",
            "Año calendario del informe"
            if monthly
            else "Año calendario del inventario",
        ),
        "@partition",
        unit="year",
        directory="diretorios_data_tempo.ano:ano",
        o=(
            "Coluna de particionamento; corresponde ao ano do arquivo anual de origem",
            "Partition column; matches the year of the source annual file",
            "Columna de particionamiento; coincide con el año del archivo anual de origen",
        ),
    )


def month_col() -> C:
    return C(
        "month",
        "INT64",
        (
            "Mês-calendário a que a observação se refere",
            "Calendar month the observation refers to",
            "Mes calendario al que se refiere la observación",
        ),
        "@month",
        unit="month",
        directory="diretorios_data_tempo.mes:mes",
    )


def plant_id_col() -> C:
    return C(
        "plant_id",
        "STRING",
        (
            "Código da usina geradora atribuído pela EIA",
            "EIA-assigned identifier of the generating plant",
            "Código de la planta generadora asignado por la EIA",
        ),
        "plant_id_eia",
        o=PLANT_ID_NOTE,
    )


def geography_cols(prefix: str = "", from_form: str = "state") -> list[C]:
    return [
        C(
            f"{prefix}state_abbreviation",
            "STRING",
            (
                "Sigla postal de duas letras do estado",
                "Two-letter postal abbreviation of the state",
                "Sigla postal de dos letras del estado",
            ),
            from_form,
        ),
        C(
            f"{prefix}state_id",
            "STRING",
            (
                "Código FIPS de duas posições do estado",
                "Two-digit FIPS code of the state",
                "Código FIPS de dos dígitos del estado",
            ),
            "@state_id",
            directory="br_bd_diretorios_us.state:id_state",
            o=(
                "Resolvido a partir da sigla postal publicada",
                "Resolved from the published postal abbreviation",
                "Resuelto a partir de la sigla postal publicada",
            ),
        ),
    ]


def data_maturity_col() -> C:
    return C(
        "data_maturity",
        "STRING",
        (
            "Grau de consolidação dos valores da linha",
            "How settled the values in the row are",
            "Grado de consolidación de los valores de la fila",
        ),
        "@data_maturity",
        dictionary=True,
        o=DATA_MATURITY_NOTE,
    )


# --------------------------------------------------------------------------
# plant — EIA-860 Schedule 2
# --------------------------------------------------------------------------

PLANT = [
    year_col(monthly=False),
    plant_id_col(),
    C(
        "plant_name",
        "STRING",
        (
            "Nome da usina tal como declarado",
            "Plant name as reported",
            "Nombre de la planta tal como se declara",
        ),
        "plant_name_eia",
    ),
    C(
        "utility_id",
        "STRING",
        (
            "Código da empresa operadora atribuído pela EIA",
            "EIA-assigned identifier of the operating utility",
            "Código de la empresa operadora asignado por la EIA",
        ),
        "utility_id_eia",
        o=UTILITY_ID_NOTE,
    ),
    C(
        "utility_name",
        "STRING",
        (
            "Nome da empresa operadora",
            "Name of the operating utility",
            "Nombre de la empresa operadora",
        ),
        "utility_name_eia",
        coverage="2013(1)",
    ),
    *geography_cols(),
    C(
        "county_name",
        "STRING",
        (
            "Nome do condado onde a usina está localizada",
            "Name of the county the plant is located in",
            "Nombre del condado donde se ubica la planta",
        ),
        "county",
    ),
    C(
        "county_id",
        "STRING",
        (
            "Código FIPS de cinco posições do condado",
            "Five-digit FIPS code of the county",
            "Código FIPS de cinco dígitos del condado",
        ),
        "@county_id",
        directory="br_bd_diretorios_us.county:id_county",
        o=COUNTY_ID_NOTE,
    ),
    C(
        "city",
        "STRING",
        (
            "Cidade do endereço da usina",
            "City of the plant address",
            "Ciudad de la dirección de la planta",
        ),
        "city",
        coverage="2007(1)",
    ),
    C(
        "street_address",
        "STRING",
        (
            "Logradouro do endereço da usina",
            "Street address of the plant",
            "Dirección de la planta",
        ),
        "street_address",
        coverage="2007(1)",
    ),
    C(
        "zip_code",
        "STRING",
        (
            "CEP do endereço da usina",
            "ZIP code of the plant address",
            "Código postal de la planta",
        ),
        "zip_code",
    ),
    C(
        "latitude",
        "FLOAT64",
        (
            "Latitude da usina em graus decimais",
            "Plant latitude in decimal degrees",
            "Latitud de la planta en grados decimales",
        ),
        "latitude",
        unit="degree",
        coverage="2009(1)",
    ),
    C(
        "longitude",
        "FLOAT64",
        (
            "Longitude da usina em graus decimais",
            "Plant longitude in decimal degrees",
            "Longitud de la planta en grados decimales",
        ),
        "longitude",
        unit="degree",
        coverage="2009(1)",
    ),
    C(
        "balancing_authority_code",
        "STRING",
        (
            "Código da autoridade de balanceamento que despacha a usina",
            "Code of the balancing authority that dispatches the plant",
            "Código de la autoridad de balance que despacha la planta",
        ),
        "balancing_authority_code_eia",
        dictionary=True,
        coverage="2013(1)",
    ),
    C(
        "balancing_authority_name",
        "STRING",
        (
            "Nome da autoridade de balanceamento",
            "Name of the balancing authority",
            "Nombre de la autoridad de balance",
        ),
        "balancing_authority_name_eia",
        coverage="2013(1)",
    ),
    C(
        "nerc_region",
        "STRING",
        (
            "Região de confiabilidade da NERC",
            "NERC reliability region",
            "Región de confiabilidad de la NERC",
        ),
        "nerc_region",
    ),
    C(
        "iso_rto_code",
        "STRING",
        (
            "Código do operador independente do sistema ou organização regional de transmissão",
            "Code of the independent system operator or regional transmission organization",
            "Código del operador independiente del sistema u organización regional de transmisión",
        ),
        "iso_rto_code",
        coverage="2009(1)2012",
    ),
    C(
        "sector_id",
        "STRING",
        (
            "Código do setor econômico da usina",
            "Code of the plant's economic sector",
            "Código del sector económico de la planta",
        ),
        "sector_id_eia",
        dictionary=True,
        coverage="2009(1)",
    ),
    C(
        "sector_name",
        "STRING",
        (
            "Nome do setor econômico da usina",
            "Name of the plant's economic sector",
            "Nombre del sector económico de la planta",
        ),
        "sector_name_eia",
        coverage="2009(1)",
    ),
    C(
        "primary_purpose_naics_code",
        "STRING",
        (
            "Código NAICS da atividade econômica principal do estabelecimento",
            "NAICS code of the establishment's primary economic activity",
            "Código NAICS de la actividad económica principal del establecimiento",
        ),
        "primary_purpose_id_naics",
        o=(
            "Sem vínculo com o diretório NAICS porque a vintagem do código não é publicada",
            "Not linked to the NAICS directory because the code's vintage is not published",
            "Sin vínculo con el directorio NAICS porque la añada del código no se publica",
        ),
    ),
    C(
        "regulatory_status_code",
        "STRING",
        (
            "Situação regulatória da usina",
            "Regulatory status of the plant",
            "Situación regulatoria de la planta",
        ),
        "regulatory_status_code",
        dictionary=True,
        coverage="2006(1)",
    ),
    C(
        "service_area",
        "STRING",
        (
            "Área de concessão declarada",
            "Reported service area",
            "Área de concesión declarada",
        ),
        "service_area",
        coverage="2001(1)2006",
    ),
    C(
        "water_source",
        "STRING",
        (
            "Fonte de água de resfriamento declarada",
            "Reported source of cooling water",
            "Fuente de agua de enfriamiento declarada",
        ),
        "water_source",
    ),
    C(
        "grid_voltage_1_kv",
        "FLOAT64",
        (
            "Tensão do primeiro ponto de conexão à rede",
            "Voltage of the first grid interconnection point",
            "Tensión del primer punto de conexión a la red",
        ),
        "grid_voltage_1_kv",
        unit="kilovolt",
        coverage="2013(1)",
    ),
    C(
        "grid_voltage_2_kv",
        "FLOAT64",
        (
            "Tensão do segundo ponto de conexão à rede",
            "Voltage of the second grid interconnection point",
            "Tensión del segundo punto de conexión a la red",
        ),
        "grid_voltage_2_kv",
        unit="kilovolt",
        coverage="2013(1)",
    ),
    C(
        "grid_voltage_3_kv",
        "FLOAT64",
        (
            "Tensão do terceiro ponto de conexão à rede",
            "Voltage of the third grid interconnection point",
            "Tensión del tercer punto de conexión a la red",
        ),
        "grid_voltage_3_kv",
        unit="kilovolt",
        coverage="2013(1)",
    ),
    C(
        "ferc_cogen_status",
        "STRING",
        (
            "Indica se a usina é cogeradora qualificada junto à FERC",
            "Whether the plant is a FERC qualifying cogeneration facility",
            "Indica si la planta es cogeneradora calificada ante la FERC",
        ),
        "ferc_cogen_status",
        dictionary=True,
        coverage="2007(1)",
    ),
    C(
        "ferc_small_power_producer",
        "STRING",
        (
            "Indica se a usina é pequena produtora qualificada junto à FERC",
            "Whether the plant is a FERC qualifying small power producer",
            "Indica si la planta es pequeña productora calificada ante la FERC",
        ),
        "ferc_small_power_producer",
        dictionary=True,
        coverage="2007(1)",
    ),
    C(
        "ferc_exempt_wholesale_generator",
        "STRING",
        (
            "Indica se a usina é geradora atacadista isenta junto à FERC",
            "Whether the plant is a FERC exempt wholesale generator",
            "Indica si la planta es generadora mayorista exenta ante la FERC",
        ),
        "ferc_exempt_wholesale_generator",
        dictionary=True,
        coverage="2007(1)",
    ),
    C(
        "transmission_distribution_owner_id",
        "STRING",
        (
            "Código da empresa dona da rede de transmissão ou distribuição de conexão",
            "Identifier of the utility owning the interconnecting transmission or distribution system",
            "Código de la empresa dueña de la red de transmisión o distribución de conexión",
        ),
        "transmission_distribution_owner_id",
        coverage="2007(1)",
    ),
    C(
        "transmission_distribution_owner_name",
        "STRING",
        (
            "Nome da empresa dona da rede de conexão",
            "Name of the utility owning the interconnecting system",
            "Nombre de la empresa dueña de la red de conexión",
        ),
        "transmission_distribution_owner_name",
        coverage="2007(1)",
    ),
    C(
        "transmission_distribution_owner_state",
        "STRING",
        (
            "Sigla do estado da empresa dona da rede de conexão",
            "State abbreviation of the utility owning the interconnecting system",
            "Sigla del estado de la empresa dueña de la red de conexión",
        ),
        "transmission_distribution_owner_state",
        coverage="2007(1)",
    ),
    C(
        "natural_gas_pipeline_name_1",
        "STRING",
        (
            "Nome do primeiro gasoduto que abastece a usina",
            "Name of the first natural gas pipeline serving the plant",
            "Nombre del primer gasoducto que abastece la planta",
        ),
        "natural_gas_pipeline_name_1",
        coverage="2013(1)",
    ),
    C(
        "natural_gas_pipeline_name_2",
        "STRING",
        (
            "Nome do segundo gasoduto que abastece a usina",
            "Name of the second natural gas pipeline serving the plant",
            "Nombre del segundo gasoducto que abastece la planta",
        ),
        "natural_gas_pipeline_name_2",
        coverage="2013(1)",
    ),
    C(
        "natural_gas_pipeline_name_3",
        "STRING",
        (
            "Nome do terceiro gasoduto que abastece a usina",
            "Name of the third natural gas pipeline serving the plant",
            "Nombre del tercer gasoducto que abastece la planta",
        ),
        "natural_gas_pipeline_name_3",
        coverage="2013(1)",
    ),
    C(
        "natural_gas_local_distribution_company",
        "STRING",
        (
            "Distribuidora local de gás natural que atende a usina",
            "Local natural gas distribution company serving the plant",
            "Distribuidora local de gas natural que atiende la planta",
        ),
        "natural_gas_local_distribution_company",
        coverage="2013(1)",
    ),
    C(
        "natural_gas_storage",
        "STRING",
        (
            "Indica se a usina dispõe de estocagem de gás natural",
            "Whether the plant has on-site natural gas storage",
            "Indica si la planta dispone de almacenamiento de gas natural",
        ),
        "natural_gas_storage",
        dictionary=True,
        coverage="2013(1)",
    ),
    C(
        "liquefied_natural_gas_storage",
        "STRING",
        (
            "Indica se a usina dispõe de estocagem de gás natural liquefeito",
            "Whether the plant has on-site liquefied natural gas storage",
            "Indica si la planta dispone de almacenamiento de gas natural licuado",
        ),
        "liquefied_natural_gas_storage",
        dictionary=True,
        coverage="2013(1)",
    ),
    C(
        "ash_impoundment",
        "STRING",
        (
            "Indica se a usina opera bacia de cinzas",
            "Whether the plant operates an ash impoundment",
            "Indica si la planta opera una laguna de cenizas",
        ),
        "ash_impoundment",
        dictionary=True,
        coverage="2013(1)",
    ),
    C(
        "ash_impoundment_lined",
        "STRING",
        (
            "Indica se a bacia de cinzas é impermeabilizada",
            "Whether the ash impoundment is lined",
            "Indica si la laguna de cenizas está impermeabilizada",
        ),
        "ash_impoundment_lined",
        dictionary=True,
        coverage="2013(1)",
    ),
    C(
        "ash_impoundment_status",
        "STRING",
        (
            "Situação operacional da bacia de cinzas",
            "Operating status of the ash impoundment",
            "Situación operativa de la laguna de cenizas",
        ),
        "ash_impoundment_status",
        dictionary=True,
        coverage="2013(1)",
    ),
    C(
        "energy_storage",
        "STRING",
        (
            "Indica se a usina opera armazenamento de energia",
            "Whether the plant operates energy storage",
            "Indica si la planta opera almacenamiento de energía",
        ),
        "energy_storage",
        dictionary=True,
        coverage="2013(1)",
    ),
    C(
        "has_net_metering",
        "STRING",
        (
            "Indica se a usina opera sob medição líquida",
            "Whether the plant operates under net metering",
            "Indica si la planta opera bajo medición neta",
        ),
        "has_net_metering",
        dictionary=True,
        coverage="2013(1)2015",
    ),
    C(
        "datum",
        "STRING",
        (
            "Datum geodésico das coordenadas declaradas",
            "Geodetic datum of the reported coordinates",
            "Datum geodésico de las coordenadas declaradas",
        ),
        "datum",
        coverage="2013(1)",
    ),
    data_maturity_col(),
]


# --------------------------------------------------------------------------
# generator — EIA-860 Schedule 3
# --------------------------------------------------------------------------

YN_NOTE = (
    "Indicador Y/N tal como publicado; mantido como STRING porque a fonte também "
    "usa códigos e brancos que um booleano descartaria",
    "A Y/N indicator as published; kept as STRING because the source also uses "
    "other codes and blanks that a boolean would discard",
    "Indicador Y/N tal como se publica; se mantiene como STRING porque la fuente "
    "también usa códigos y blancos que un booleano descartaría",
)

ENERGY_SOURCE_NOTE = (
    "Combustíveis são declarados em até seis colunas por ordem de importância; "
    "as posições não usadas ficam nulas",
    "Fuels are reported in up to six columns in order of importance; unused "
    "positions are null",
    "Los combustibles se declaran en hasta seis columnas por orden de "
    "importancia; las posiciones no usadas quedan nulas",
)


def energy_source_col(n: int) -> C:
    ordinal_pt = {
        1: "primeiro",
        2: "segundo",
        3: "terceiro",
        4: "quarto",
        5: "quinto",
        6: "sexto",
    }[n]
    ordinal_en = {
        1: "first",
        2: "second",
        3: "third",
        4: "fourth",
        5: "fifth",
        6: "sixth",
    }[n]
    ordinal_es = {
        1: "primero",
        2: "segundo",
        3: "tercero",
        4: "cuarto",
        5: "quinto",
        6: "sexto",
    }[n]
    return C(
        f"energy_source_code_{n}",
        "STRING",
        (
            f"Código do {ordinal_pt} combustível ou fonte de energia do gerador",
            f"Code of the generator's {ordinal_en} fuel or energy source",
            f"Código del {ordinal_es} combustible o fuente de energía del generador",
        ),
        f"energy_source_code_{n}",
        dictionary=True,
        o=ENERGY_SOURCE_NOTE,
    )


def tech_flag(name: str, pt: str, en: str, es: str, coverage: str) -> C:
    return C(
        name,
        "STRING",
        (pt, en, es),
        name,
        dictionary=True,
        coverage=coverage,
        o=YN_NOTE,
    )


GENERATOR = [
    year_col(monthly=False),
    plant_id_col(),
    C(
        "generator_id",
        "STRING",
        (
            "Identificador do gerador dentro da usina",
            "Identifier of the generator within the plant",
            "Identificador del generador dentro de la planta",
        ),
        "generator_id",
        o=(
            "Único apenas dentro de uma usina: a chave da tabela é (year, plant_id, generator_id)",
            "Unique only within a plant: the table's key is (year, plant_id, generator_id)",
            "Único solo dentro de una planta: la clave de la tabla es (year, plant_id, generator_id)",
        ),
    ),
    C(
        "plant_name",
        "STRING",
        (
            "Nome da usina tal como declarado",
            "Plant name as reported",
            "Nombre de la planta tal como se declara",
        ),
        "plant_name_eia",
        coverage="2004(1)",
    ),
    C(
        "utility_id",
        "STRING",
        (
            "Código da empresa operadora atribuído pela EIA",
            "EIA-assigned identifier of the operating utility",
            "Código de la empresa operadora asignado por la EIA",
        ),
        "utility_id_eia",
        o=UTILITY_ID_NOTE,
    ),
    C(
        "utility_name",
        "STRING",
        (
            "Nome da empresa operadora",
            "Name of the operating utility",
            "Nombre de la empresa operadora",
        ),
        "utility_name_eia",
        coverage="2004(1)",
    ),
    *geography_cols(),
    C(
        "county_name",
        "STRING",
        (
            "Nome do condado onde o gerador está localizado",
            "Name of the county the generator is located in",
            "Nombre del condado donde se ubica el generador",
        ),
        "county",
        coverage="2009(1)",
    ),
    C(
        "county_id",
        "STRING",
        (
            "Código FIPS de cinco posições do condado",
            "Five-digit FIPS code of the county",
            "Código FIPS de cinco dígitos del condado",
        ),
        "@county_id",
        directory="br_bd_diretorios_us.county:id_county",
        coverage="2009(1)",
        o=COUNTY_ID_NOTE,
    ),
    C(
        "generator_status_group",
        "STRING",
        (
            "Grupo do gerador no formulário: em operação, proposto ou aposentado",
            "Which group of the form the generator was reported in: existing, proposed or retired",
            "Grupo del generador en el formulario: en operación, propuesto o retirado",
        ),
        "@generator_status_group",
        dictionary=True,
        o=(
            "A partir de 2009 o formulário separa os três grupos em planilhas distintas e o valor é a "
            "planilha de origem. Antes disso operantes e aposentados vêm em uma planilha só, e o grupo é "
            "derivado de operational_status_code pelo próprio vocabulário da EIA",
            "From 2009 the form splits the three groups into separate sheets and the value is the sheet "
            "the row came from. Before that operable and retired generators share one sheet, and the "
            "group is derived from operational_status_code using EIA's own vocabulary",
            "Desde 2009 el formulario separa los tres grupos en hojas distintas y el valor es la hoja de "
            "origen. Antes de eso operantes y retirados vienen en una sola hoja, y el grupo se deriva de "
            "operational_status_code por el propio vocabulario de la EIA",
        ),
    ),
    C(
        "operational_status_code",
        "STRING",
        (
            "Código da situação operacional do gerador",
            "Code of the generator's operational status",
            "Código de la situación operativa del generador",
        ),
        "operational_status_code",
        dictionary=True,
    ),
    C(
        "unit_id",
        "STRING",
        (
            "Identificador da unidade a que o gerador pertence",
            "Identifier of the unit the generator belongs to",
            "Identificador de la unidad a la que pertenece el generador",
        ),
        "unit_id_eia",
    ),
    C(
        "ownership_code",
        "STRING",
        (
            "Código da forma de propriedade do gerador",
            "Code of the generator's form of ownership",
            "Código de la forma de propiedad del generador",
        ),
        "ownership_code",
        dictionary=True,
    ),
    C(
        "sector_id",
        "STRING",
        (
            "Código do setor econômico da usina",
            "Code of the plant's economic sector",
            "Código del sector económico de la planta",
        ),
        "sector_id_eia",
        dictionary=True,
        coverage="2009(1)",
    ),
    C(
        "sector_name",
        "STRING",
        (
            "Nome do setor econômico da usina",
            "Name of the plant's economic sector",
            "Nombre del sector económico de la planta",
        ),
        "sector_name_eia",
        coverage="2009(1)",
    ),
    C(
        "prime_mover_code",
        "STRING",
        (
            "Código da máquina primária do gerador",
            "Code of the generator's prime mover",
            "Código de la máquina primaria del generador",
        ),
        "prime_mover_code",
        dictionary=True,
    ),
    C(
        "technology_description",
        "STRING",
        (
            "Descrição da tecnologia de geração",
            "Description of the generating technology",
            "Descripción de la tecnología de generación",
        ),
        "technology_description",
        dictionary=True,
        coverage="2014(1)",
    ),
    C(
        "capacity_mw",
        "FLOAT64",
        (
            "Capacidade nominal de placa do gerador",
            "Nameplate capacity of the generator",
            "Capacidad nominal de placa del generador",
        ),
        "capacity_mw",
        unit="megawatt",
    ),
    C(
        "summer_capacity_mw",
        "FLOAT64",
        (
            "Capacidade líquida do gerador em condições de verão",
            "Net capacity of the generator under summer conditions",
            "Capacidad neta del generador en condiciones de verano",
        ),
        "summer_capacity_mw",
        unit="megawatt",
    ),
    C(
        "winter_capacity_mw",
        "FLOAT64",
        (
            "Capacidade líquida do gerador em condições de inverno",
            "Net capacity of the generator under winter conditions",
            "Capacidad neta del generador en condiciones de invierno",
        ),
        "winter_capacity_mw",
        unit="megawatt",
    ),
    C(
        "minimum_load_mw",
        "FLOAT64",
        (
            "Carga mínima de operação estável do gerador",
            "Minimum stable operating load of the generator",
            "Carga mínima de operación estable del generador",
        ),
        "minimum_load_mw",
        unit="megawatt",
        coverage="2013(1)",
    ),
    C(
        "planned_new_capacity_mw",
        "FLOAT64",
        (
            "Capacidade de placa prevista para o gerador proposto",
            "Nameplate capacity planned for the proposed generator",
            "Capacidad de placa prevista para el generador propuesto",
        ),
        "planned_new_capacity_mw",
        unit="megawatt",
        coverage="2013(1)",
    ),
    C(
        "turbines_num",
        "INT64",
        (
            "Número de turbinas do gerador",
            "Number of turbines in the generator",
            "Número de turbinas del generador",
        ),
        "turbines_num",
        unit="unit",
        coverage="2001(1)2012",
    ),
    C(
        "turbines_inverters_hydrokinetics",
        "INT64",
        (
            "Número de turbinas, inversores ou dispositivos hidrocinéticos",
            "Number of turbines, inverters or hydrokinetic devices",
            "Número de turbinas, inversores o dispositivos hidrocinéticos",
        ),
        "turbines_inverters_hydrokinetics",
        unit="unit",
        coverage="2013(1)",
    ),
    *[energy_source_col(n) for n in range(1, 7)],
    C(
        "can_burn_multiple_fuels",
        "STRING",
        (
            "Indica se o gerador pode queimar mais de um combustível",
            "Whether the generator can burn more than one fuel",
            "Indica si el generador puede quemar más de un combustible",
        ),
        "can_burn_multiple_fuels",
        dictionary=True,
        coverage="2006(1)",
        o=YN_NOTE,
    ),
    C(
        "can_cofire_fuels",
        "STRING",
        (
            "Indica se o gerador pode co-queimar combustíveis",
            "Whether the generator can co-fire fuels",
            "Indica si el generador puede co-quemar combustibles",
        ),
        "can_cofire_fuels",
        dictionary=True,
        coverage="2013(1)",
        o=YN_NOTE,
    ),
    C(
        "switch_oil_gas",
        "STRING",
        (
            "Indica se o gerador pode alternar entre óleo e gás",
            "Whether the generator can switch between oil and gas",
            "Indica si el generador puede alternar entre petróleo y gas",
        ),
        "switch_oil_gas",
        dictionary=True,
        coverage="2013(1)",
        o=YN_NOTE,
    ),
    C(
        "operating_date",
        "DATE",
        (
            "Data em que o gerador entrou em operação comercial",
            "Date the generator entered commercial operation",
            "Fecha en que el generador entró en operación comercial",
        ),
        "@operating_date",
        o=YEAR_MONTH_DAY_ONE,
    ),
    C(
        "retirement_date",
        "DATE",
        (
            "Data em que o gerador foi aposentado",
            "Date the generator was retired",
            "Fecha en que el generador fue retirado",
        ),
        "@retirement_date",
        o=YEAR_MONTH_DAY_ONE,
    ),
    C(
        "planned_retirement_date",
        "DATE",
        (
            "Data prevista de aposentadoria do gerador",
            "Date the generator is planned to retire",
            "Fecha prevista de retiro del generador",
        ),
        "@planned_retirement_date",
        coverage="2007(1)",
        o=YEAR_MONTH_DAY_ONE,
    ),
    C(
        "planned_operating_date",
        "DATE",
        (
            "Data prevista de entrada em operação do gerador proposto",
            "Date the proposed generator is planned to enter operation",
            "Fecha prevista de entrada en operación del generador propuesto",
        ),
        "@planned_operating_date",
        o=YEAR_MONTH_DAY_ONE,
    ),
    tech_flag(
        "carbon_capture",
        "Indica se o gerador possui captura de carbono",
        "Whether the generator has carbon capture",
        "Indica si el generador cuenta con captura de carbono",
        "2009(1)",
    ),
    tech_flag(
        "solid_fuel_gasification",
        "Indica se o gerador usa gaseificação de combustível sólido",
        "Whether the generator uses solid fuel gasification",
        "Indica si el generador usa gasificación de combustible sólido",
        "2004(1)",
    ),
    tech_flag(
        "pulverized_coal_tech",
        "Indica tecnologia de carvão pulverizado",
        "Whether the generator uses pulverized coal technology",
        "Indica tecnología de carbón pulverizado",
        "2009(1)",
    ),
    tech_flag(
        "fluidized_bed_tech",
        "Indica tecnologia de leito fluidizado",
        "Whether the generator uses fluidized bed technology",
        "Indica tecnología de lecho fluidizado",
        "2009(1)",
    ),
    tech_flag(
        "stoker_tech",
        "Indica tecnologia de grelha alimentadora",
        "Whether the generator uses stoker technology",
        "Indica tecnología de parrilla alimentadora",
        "2013(1)",
    ),
    tech_flag(
        "other_combustion_tech",
        "Indica outra tecnologia de combustão",
        "Whether the generator uses another combustion technology",
        "Indica otra tecnología de combustión",
        "2013(1)",
    ),
    tech_flag(
        "subcritical_tech",
        "Indica ciclo a vapor subcrítico",
        "Whether the generator uses a subcritical steam cycle",
        "Indica ciclo de vapor subcrítico",
        "2009(1)",
    ),
    tech_flag(
        "supercritical_tech",
        "Indica ciclo a vapor supercrítico",
        "Whether the generator uses a supercritical steam cycle",
        "Indica ciclo de vapor supercrítico",
        "2009(1)",
    ),
    tech_flag(
        "ultrasupercritical_tech",
        "Indica ciclo a vapor ultrassupercrítico",
        "Whether the generator uses an ultrasupercritical steam cycle",
        "Indica ciclo de vapor ultrasupercrítico",
        "2009(1)",
    ),
    tech_flag(
        "duct_burners",
        "Indica presença de queimadores de duto",
        "Whether the generator has duct burners",
        "Indica presencia de quemadores de ducto",
        "2007(1)",
    ),
    tech_flag(
        "bypass_heat_recovery",
        "Indica se a recuperação de calor pode ser contornada",
        "Whether heat recovery can be bypassed",
        "Indica si la recuperación de calor puede ser evitada",
        "2013(1)",
    ),
    tech_flag(
        "associated_combined_heat_power",
        "Indica se o gerador integra unidade de cogeração",
        "Whether the generator is part of a combined heat and power unit",
        "Indica si el generador integra una unidad de cogeneración",
        "2001(1)",
    ),
    C(
        "topping_bottoming_code",
        "STRING",
        (
            "Posição do gerador no ciclo de cogeração",
            "Position of the generator in the cogeneration cycle",
            "Posición del generador en el ciclo de cogeneración",
        ),
        "topping_bottoming_code",
        dictionary=True,
        coverage="2009(1)",
    ),
    tech_flag(
        "synchronized_transmission_grid",
        "Indica se o gerador é sincronizado com a rede de transmissão",
        "Whether the generator is synchronized with the transmission grid",
        "Indica si el generador está sincronizado con la red de transmisión",
        "2004(1)",
    ),
    C(
        "rto_iso_lmp_node_id",
        "STRING",
        (
            "Identificador do nó de preço marginal local no ISO ou RTO",
            "Identifier of the locational marginal price node in the ISO or RTO",
            "Identificador del nodo de precio marginal local en el ISO o RTO",
        ),
        "rto_iso_lmp_node_id",
        coverage="2013(1)",
    ),
    data_maturity_col(),
]


# --------------------------------------------------------------------------
# generation_fuel — EIA-923 page 1, melted to long
# --------------------------------------------------------------------------

GENERATION_FUEL = [
    year_col(monthly=True),
    month_col(),
    plant_id_col(),
    C(
        "plant_name",
        "STRING",
        (
            "Nome da usina tal como declarado",
            "Plant name as reported",
            "Nombre de la planta tal como se declara",
        ),
        "plant_name_eia",
    ),
    C(
        "operator_id",
        "STRING",
        (
            "Código do operador da usina atribuído pela EIA",
            "EIA-assigned identifier of the plant operator",
            "Código del operador de la planta asignado por la EIA",
        ),
        "operator_id",
        o=UTILITY_ID_NOTE,
    ),
    C(
        "operator_name",
        "STRING",
        (
            "Nome do operador da usina",
            "Name of the plant operator",
            "Nombre del operador de la planta",
        ),
        "operator_name",
    ),
    *geography_cols(from_form="plant_state"),
    C(
        "census_region",
        "STRING",
        (
            "Região censitária dos Estados Unidos",
            "United States census region",
            "Región censal de los Estados Unidos",
        ),
        "census_region",
        dictionary=True,
    ),
    C(
        "nerc_region",
        "STRING",
        (
            "Região de confiabilidade da NERC",
            "NERC reliability region",
            "Región de confiabilidad de la NERC",
        ),
        "nerc_region",
    ),
    C(
        "balancing_authority_code",
        "STRING",
        (
            "Código da autoridade de balanceamento que despacha a usina",
            "Code of the balancing authority that dispatches the plant",
            "Código de la autoridad de balance que despacha la planta",
        ),
        "balancing_authority_code_eia",
        dictionary=True,
        coverage="2018(1)",
    ),
    C(
        "sector_id",
        "STRING",
        (
            "Código do setor econômico da usina",
            "Code of the plant's economic sector",
            "Código del sector económico de la planta",
        ),
        "sector_id_eia",
        dictionary=True,
    ),
    C(
        "sector_name",
        "STRING",
        (
            "Nome do setor econômico da usina",
            "Name of the plant's economic sector",
            "Nombre del sector económico de la planta",
        ),
        "sector_name_eia",
    ),
    C(
        "naics_code",
        "STRING",
        (
            "Código NAICS da atividade econômica da usina",
            "NAICS code of the plant's economic activity",
            "Código NAICS de la actividad económica de la planta",
        ),
        "naics_code",
        o=(
            "Sem vínculo com o diretório NAICS porque a vintagem do código não é publicada",
            "Not linked to the NAICS directory because the code's vintage is not published",
            "Sin vínculo con el directorio NAICS porque la añada del código no se publica",
        ),
    ),
    C(
        "prime_mover_code",
        "STRING",
        (
            "Código da máquina primária",
            "Code of the prime mover",
            "Código de la máquina primaria",
        ),
        "prime_mover_code",
        dictionary=True,
        o=(
            "Nulo em 2001 e 2002: o formulário só passou a coletar a máquina primária a partir de 2003. "
            "Mantido nulo em vez de imputado a partir de outros anos, porque esta tabela publica o "
            "declarado",
            "Null in 2001 and 2002: the form only began collecting the prime mover in 2003. Left null "
            "rather than imputed from other years, because this table publishes what was reported",
            "Nulo en 2001 y 2002: el formulario solo comenzó a recoger la máquina primaria a partir de "
            "2003. Se mantiene nulo en vez de imputado desde otros años, porque esta tabla publica lo "
            "declarado",
        ),
    ),
    C(
        "energy_source_code",
        "STRING",
        (
            "Código do combustível ou fonte de energia consumida",
            "Code of the fuel or energy source consumed",
            "Código del combustible o fuente de energía consumida",
        ),
        "energy_source_code",
        dictionary=True,
    ),
    C(
        "fuel_type_code_agg",
        "STRING",
        (
            "Código agregado de combustível usado nas publicações da EIA",
            "Aggregated fuel code used in EIA's own published series",
            "Código agregado de combustible usado en las publicaciones de la EIA",
        ),
        "fuel_type_code_agg",
        dictionary=True,
    ),
    C(
        "nuclear_unit_id",
        "STRING",
        (
            "Identificador da unidade nuclear",
            "Identifier of the nuclear unit",
            "Identificador de la unidad nuclear",
        ),
        "nuclear_unit_id",
        o=(
            "Preenchido apenas em usinas nucleares, que declaram uma linha por reator; nulo nas demais",
            "Populated only at nuclear plants, which report one row per reactor; null elsewhere",
            "Poblado solo en plantas nucleares, que declaran una fila por reactor; nulo en las demás",
        ),
    ),
    C(
        "associated_combined_heat_power",
        "STRING",
        (
            "Indica se a geração provém de unidade de cogeração",
            "Whether the generation comes from a combined heat and power unit",
            "Indica si la generación proviene de una unidad de cogeneración",
        ),
        "associated_combined_heat_power",
        dictionary=True,
    ),
    C(
        "reporting_frequency_code",
        "STRING",
        (
            "Frequência com que o respondente declara o formulário",
            "Frequency at which the respondent files the form",
            "Frecuencia con que el encuestado declara el formulario",
        ),
        "reporting_frequency_code",
        dictionary=True,
        coverage="2018(1)",
        o=(
            "Respondentes anuais declaram o total do ano na coluna de dezembro; ler um único mês de um "
            "respondente anual não é a geração daquele mês",
            "Annual respondents report the whole year in the December column; reading a single month "
            "from an annual respondent is not that month's generation",
            "Los encuestados anuales declaran el total del año en la columna de diciembre; leer un solo "
            "mes de un encuestado anual no es la generación de ese mes",
        ),
    ),
    C(
        "fuel_unit",
        "STRING",
        (
            "Unidade física em que o combustível é declarado",
            "Physical unit the fuel quantity is reported in",
            "Unidad física en que se declara el combustible",
        ),
        "fuel_unit",
        dictionary=True,
    ),
    C(
        "fuel_consumed_units",
        "FLOAT64",
        (
            "Quantidade física total de combustível consumida no mês",
            "Total physical quantity of fuel consumed in the month",
            "Cantidad física total de combustible consumida en el mes",
        ),
        "fuel_consumed_units",
        o=MIXED_UNIT,
    ),
    C(
        "fuel_consumed_for_electricity_units",
        "FLOAT64",
        (
            "Quantidade física de combustível consumida para geração de eletricidade no mês",
            "Physical quantity of fuel consumed for electricity generation in the month",
            "Cantidad física de combustible consumida para generación eléctrica en el mes",
        ),
        "fuel_consumed_for_electricity_units",
        o=MIXED_UNIT,
    ),
    C(
        "fuel_mmbtu_per_unit",
        "FLOAT64",
        (
            "Conteúdo energético médio por unidade física de combustível",
            "Average heat content per physical unit of fuel",
            "Contenido energético medio por unidad física de combustible",
        ),
        "fuel_mmbtu_per_unit",
        o=(
            "Sem unidade de medida declarada porque o denominador varia por combustível, conforme "
            "fuel_unit: MMBtu por tonelada curta de carvão, por mil pés cúbicos de gás, por barril de "
            "líquidos",
            "No measurement unit is declared because the denominator varies by fuel, as given in "
            "fuel_unit: MMBtu per short ton of coal, per thousand cubic feet of gas, per barrel of "
            "liquids",
            "Sin unidad de medida declarada porque el denominador varía según el combustible, conforme "
            "fuel_unit: MMBtu por tonelada corta de carbón, por mil pies cúbicos de gas, por barril de "
            "líquidos",
        ),
    ),
    C(
        "fuel_consumed_mmbtu",
        "FLOAT64",
        (
            "Conteúdo energético total do combustível consumido no mês",
            "Total heat content of the fuel consumed in the month",
            "Contenido energético total del combustible consumido en el mes",
        ),
        "fuel_consumed_mmbtu",
        unit="mmbtu",
    ),
    C(
        "fuel_consumed_for_electricity_mmbtu",
        "FLOAT64",
        (
            "Conteúdo energético do combustível consumido para geração de eletricidade no mês",
            "Heat content of the fuel consumed for electricity generation in the month",
            "Contenido energético del combustible consumido para generación eléctrica en el mes",
        ),
        "fuel_consumed_for_electricity_mmbtu",
        unit="mmbtu",
    ),
    C(
        "net_generation_mwh",
        "FLOAT64",
        (
            "Geração líquida de eletricidade no mês",
            "Net electricity generation in the month",
            "Generación neta de electricidad en el mes",
        ),
        "net_generation_mwh",
        unit="megawatt_hour",
        o=(
            "Pode ser negativa: unidades de armazenamento e usinas em partida consomem mais eletricidade "
            "do que entregam à rede no período",
            "Can be negative: storage units and plants that are starting up consume more electricity "
            "than they deliver to the grid over the period",
            "Puede ser negativa: unidades de almacenamiento y plantas en arranque consumen más "
            "electricidad de la que entregan a la red en el período",
        ),
    ),
    data_maturity_col(),
]


# --------------------------------------------------------------------------
# fuel_receipts_costs — EIA-923 Schedule 2, Part C
# --------------------------------------------------------------------------

FUEL_RECEIPTS_COSTS = [
    year_col(monthly=True),
    month_col(),
    plant_id_col(),
    C(
        "plant_name",
        "STRING",
        (
            "Nome da usina que recebeu o combustível",
            "Name of the plant that received the fuel",
            "Nombre de la planta que recibió el combustible",
        ),
        "plant_name_eia",
    ),
    *geography_cols(from_form="plant_state"),
    C(
        "operator_id",
        "STRING",
        (
            "Código do operador da usina atribuído pela EIA",
            "EIA-assigned identifier of the plant operator",
            "Código del operador de la planta asignado por la EIA",
        ),
        "operator_id",
        o=UTILITY_ID_NOTE,
    ),
    C(
        "operator_name",
        "STRING",
        (
            "Nome do operador da usina",
            "Name of the plant operator",
            "Nombre del operador de la planta",
        ),
        "operator_name",
    ),
    C(
        "balancing_authority_code",
        "STRING",
        (
            "Código da autoridade de balanceamento que despacha a usina",
            "Code of the balancing authority that dispatches the plant",
            "Código de la autoridad de balance que despacha la planta",
        ),
        "balancing_authority_code_eia",
        dictionary=True,
        coverage="2018(1)",
    ),
    C(
        "energy_source_code",
        "STRING",
        (
            "Código do combustível recebido",
            "Code of the fuel received",
            "Código del combustible recibido",
        ),
        "energy_source_code",
        dictionary=True,
    ),
    C(
        "fuel_group_code",
        "STRING",
        (
            "Grupo de combustível: carvão, gás natural, petróleo ou outro",
            "Fuel group: coal, natural gas, petroleum or other",
            "Grupo de combustible: carbón, gas natural, petróleo u otro",
        ),
        "fuel_group_code",
        dictionary=True,
    ),
    C(
        "contract_type_code",
        "STRING",
        (
            "Código do tipo de contrato de suprimento",
            "Code of the supply contract type",
            "Código del tipo de contrato de suministro",
        ),
        "contract_type_code",
        dictionary=True,
    ),
    C(
        "contract_expiration_date",
        "DATE",
        (
            "Data de expiração do contrato de suprimento",
            "Expiry date of the supply contract",
            "Fecha de expiración del contrato de suministro",
        ),
        "@contract_expiration_date",
        o=(
            "Publicada como mês e ano de quatro dígitos (MMAA), com o zero à esquerda removido pela "
            "planilha; o dia é fixado em 01. Preenchida apenas em entregas contratadas",
            "Published as a four-digit month and year (MMYY) with the leading zero stripped by the "
            "spreadsheet; the day is fixed at 01. Populated only for contracted deliveries",
            "Publicada como mes y año de cuatro dígitos (MMAA), con el cero inicial eliminado por la "
            "hoja de cálculo; el día se fija en 01. Poblada solo en entregas contratadas",
        ),
    ),
    C(
        "supplier_name",
        "STRING",
        (
            "Nome do fornecedor do combustível",
            "Name of the fuel supplier",
            "Nombre del proveedor del combustible",
        ),
        "supplier_name",
    ),
    C(
        "mine_name",
        "STRING",
        (
            "Nome da mina de origem do carvão",
            "Name of the coal mine of origin",
            "Nombre de la mina de origen del carbón",
        ),
        "mine_name",
    ),
    C(
        "mine_id_msha",
        "STRING",
        (
            "Código da mina na Mine Safety and Health Administration",
            "Mine Safety and Health Administration identifier of the mine",
            "Código de la mina en la Mine Safety and Health Administration",
        ),
        "mine_id_msha",
    ),
    C(
        "mine_type_code",
        "STRING",
        (
            "Código do tipo de lavra da mina",
            "Code of the mine's extraction type",
            "Código del tipo de explotación de la mina",
        ),
        "mine_type_code",
        dictionary=True,
    ),
    C(
        "mine_state_abbreviation",
        "STRING",
        (
            "Sigla postal do estado da mina de origem",
            "Postal abbreviation of the mine's state",
            "Sigla postal del estado de la mina de origen",
        ),
        "state",
        o=(
            "Também assume códigos de país nas importações de carvão, que não resolvem contra o "
            "diretório de estados",
            "Also takes country codes on imported coal, which do not resolve against the state directory",
            "También toma códigos de país en las importaciones de carbón, que no resuelven contra el "
            "directorio de estados",
        ),
    ),
    C(
        "mine_county_id",
        "STRING",
        (
            "Código FIPS de cinco posições do condado da mina",
            "Five-digit FIPS code of the mine's county",
            "Código FIPS de cinco dígitos del condado de la mina",
        ),
        "@mine_county_id",
        directory="br_bd_diretorios_us.county:id_county",
        o=(
            "Montado a partir do FIPS do estado da mina e do código de condado de três posições "
            "publicado, cujo zero à esquerda a planilha remove",
            "Assembled from the mine state's FIPS code and the published three-digit county code, whose "
            "leading zero the spreadsheet strips",
            "Armado a partir del FIPS del estado de la mina y del código de condado de tres dígitos "
            "publicado, cuyo cero inicial la hoja de cálculo elimina",
        ),
    ),
    C(
        "primary_transportation_mode_code",
        "STRING",
        (
            "Código do modo principal de transporte do combustível",
            "Code of the fuel's primary transportation mode",
            "Código del modo principal de transporte del combustible",
        ),
        "primary_transportation_mode_code",
        dictionary=True,
    ),
    C(
        "secondary_transportation_mode_code",
        "STRING",
        (
            "Código do modo secundário de transporte do combustível",
            "Code of the fuel's secondary transportation mode",
            "Código del modo secundario de transporte del combustible",
        ),
        "secondary_transportation_mode_code",
        dictionary=True,
    ),
    C(
        "natural_gas_transport_code",
        "STRING",
        (
            "Código do regime de transporte do gás natural: firme ou interruptível",
            "Code of the natural gas transportation service: firm or interruptible",
            "Código del régimen de transporte del gas natural: firme o interrumpible",
        ),
        "natural_gas_transport_code",
        dictionary=True,
    ),
    C(
        "natural_gas_delivery_contract_type_code",
        "STRING",
        (
            "Código do tipo de contrato de entrega de gás natural",
            "Code of the natural gas delivery contract type",
            "Código del tipo de contrato de entrega de gas natural",
        ),
        "natural_gas_delivery_contract_type_code",
        dictionary=True,
        coverage="2014(1)",
    ),
    C(
        "fuel_received_units",
        "FLOAT64",
        (
            "Quantidade física de combustível recebida na entrega",
            "Physical quantity of fuel received in the delivery",
            "Cantidad física de combustible recibida en la entrega",
        ),
        "fuel_received_units",
        o=(
            "A unidade física varia por combustível: toneladas curtas para carvão e coque, mil pés "
            "cúbicos para gás natural, barris para líquidos. Some fuel_received_units apenas dentro de "
            "um mesmo energy_source_code",
            "The physical unit varies by fuel: short tons for coal and petroleum coke, thousand cubic "
            "feet for natural gas, barrels for liquids. Sum fuel_received_units only within a single "
            "energy_source_code",
            "La unidad física varía según el combustible: toneladas cortas para carbón y coque, miles de "
            "pies cúbicos para gas natural, barriles para líquidos. Sume fuel_received_units solo dentro "
            "de un mismo energy_source_code",
        ),
    ),
    C(
        "fuel_mmbtu_per_unit",
        "FLOAT64",
        (
            "Conteúdo energético por unidade física do combustível recebido",
            "Heat content per physical unit of the fuel received",
            "Contenido energético por unidad física del combustible recibido",
        ),
        "fuel_mmbtu_per_unit",
        o=(
            "Sem unidade de medida declarada porque o denominador varia por combustível, como em "
            "fuel_received_units",
            "No measurement unit is declared because the denominator varies by fuel, as in "
            "fuel_received_units",
            "Sin unidad de medida declarada porque el denominador varía según el combustible, como en "
            "fuel_received_units",
        ),
    ),
    C(
        "fuel_cost_per_mmbtu",
        "FLOAT64",
        (
            "Custo do combustível entregue por milhão de BTU",
            "Delivered cost of the fuel per million BTU",
            "Costo del combustible entregado por millón de BTU",
        ),
        "fuel_cost_per_mmbtu",
        unit="usd",
        o=(
            "A fonte publica este valor em centavos de dólar por MMBtu; aqui está convertido para "
            "dólares. É o custo entregue na usina, incluindo transporte. Ausente onde a EIA suprime o "
            "valor por confidencialidade do respondente",
            "The source publishes this value in cents per MMBtu; it is converted to dollars here. It is "
            "the cost delivered to the plant, transport included. Absent where EIA suppresses the value "
            "for respondent confidentiality",
            "La fuente publica este valor en centavos de dólar por MMBtu; aquí está convertido a "
            "dólares. Es el costo entregado en la planta, transporte incluido. Ausente donde la EIA "
            "suprime el valor por confidencialidad del encuestado",
        ),
    ),
    C(
        "sulfur_content_pct",
        "FLOAT64",
        (
            "Teor de enxofre do combustível recebido",
            "Sulfur content of the fuel received",
            "Contenido de azufre del combustible recibido",
        ),
        "sulfur_content_pct",
        unit="percent",
    ),
    C(
        "ash_content_pct",
        "FLOAT64",
        (
            "Teor de cinzas do combustível recebido",
            "Ash content of the fuel received",
            "Contenido de cenizas del combustible recibido",
        ),
        "ash_content_pct",
        unit="percent",
    ),
    C(
        "moisture_content_pct",
        "FLOAT64",
        (
            "Teor de umidade do combustível recebido",
            "Moisture content of the fuel received",
            "Contenido de humedad del combustible recibido",
        ),
        "moisture_content_pct",
        unit="percent",
        coverage="2014(1)",
    ),
    C(
        "mercury_content_ppm",
        "FLOAT64",
        (
            "Teor de mercúrio do combustível recebido",
            "Mercury content of the fuel received",
            "Contenido de mercurio del combustible recibido",
        ),
        "mercury_content_ppm",
        unit="ppm",
        coverage="2012(1)",
    ),
    C(
        "chlorine_content_ppm",
        "FLOAT64",
        (
            "Teor de cloro do combustível recebido",
            "Chlorine content of the fuel received",
            "Contenido de cloro del combustible recibido",
        ),
        "chlorine_content_ppm",
        unit="ppm",
        coverage="2014(1)",
    ),
    C(
        "regulated",
        "STRING",
        (
            "Indica se a entrega se destina a usina de tarifa regulada",
            "Whether the delivery is to a rate-regulated plant",
            "Indica si la entrega se destina a una planta de tarifa regulada",
        ),
        "regulated",
        dictionary=True,
    ),
    C(
        "reporting_frequency_code",
        "STRING",
        (
            "Frequência com que o respondente declara o formulário",
            "Frequency at which the respondent files the form",
            "Frecuencia con que el encuestado declara el formulario",
        ),
        "reporting_frequency_code",
        dictionary=True,
    ),
    data_maturity_col(),
]


TABLES = {
    "plant": PLANT,
    "generator": GENERATOR,
    "generation_fuel": GENERATION_FUEL,
    "fuel_receipts_costs": FUEL_RECEIPTS_COSTS,
}


def check(table: str, cols: list[C]) -> None:
    names = [c.name for c in cols]
    duplicates = {n for n in names if names.count(n) > 1}
    if duplicates:
        raise SystemExit(
            f"{table}: duplicate column names {sorted(duplicates)}"
        )
    for col in cols:
        if (
            col.type in ("INT64", "FLOAT64")
            and not col.unit
            and not any(col.o)
        ):
            raise SystemExit(
                f"{table}.{col.name}: numeric column with no measurement_unit must say why "
                "in its observations"
            )
        for text in col.d:
            if not text or text[0] != text[0].upper():
                raise SystemExit(
                    f"{table}.{col.name}: description must start with a capital"
                )
            if text.endswith("."):
                raise SystemExit(
                    f"{table}.{col.name}: description must not end with a period"
                )


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    measured = json.loads(MEASURED.read_text()) if MEASURED.exists() else {}
    if not measured:
        print(
            "measured.json absent — temporal_coverage falls back to the hand-declared values"
        )
    for table, cols in TABLES.items():
        check(table, cols)
        for col in cols:
            span = measured.get(table, {}).get("coverage", {}).get(col.name)
            if span:
                col.coverage = span
        path = OUT / f"sheet_{table}.csv"
        with open(path, "w", encoding="utf-8", newline="") as fh:
            # lineterminator="\n": csv defaults to CRLF, which the repo's
            # `mixed line ending` pre-commit hook then rewrites to LF. Without
            # this the generator and the hook fight, and re-running the
            # generator shows every line of every sheet as changed.
            writer = csv.DictWriter(fh, fieldnames=HEADER, lineterminator="\n")
            writer.writeheader()
            for col in cols:
                writer.writerow(col.row())
        print(f"{path.name}: {len(cols)} columns")


if __name__ == "__main__":
    main()
