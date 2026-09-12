"""Generate the us_census_trade architecture CSVs.

One column spec per table, written to ``<table>.csv`` in this directory. These
CSVs are the source of truth for the cleaning transform, the dbt models and the
backend column registration -- nothing downstream re-declares a type or an order.

Run: ``python models/us_census_trade/code/architecture/build_architecture.py``
"""

from __future__ import annotations

import csv
from pathlib import Path

HERE = Path(__file__).resolve().parent

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

# --------------------------------------------------------------------------- #
# Directories and units
# --------------------------------------------------------------------------- #
DIR_YEAR = "br_bd_diretorios_data_tempo.ano:ano"
DIR_MONTH = "br_bd_diretorios_data_tempo.mes:mes"
# The backend column names differ from the BigQuery ones: the directory table
# is `sigla_pais_iso3` in the backend and `sigla_iso3` in BigQuery. The backend
# link needs the backend name, and it must target the directory's PRIMARY KEY,
# which is ISO3 -- a link to ISO2 is silently dropped on write.
DIR_COUNTRY = "br_bd_diretorios_mundo.pais:sigla_pais_iso3"
DIR_STATE = "br_bd_diretorios_us.state:id_state"
DIR_HS6 = "br_bd_diretorios_comercio_internacional.sistema_harmonizado:id_sh6"

USD = "USD"
KG = "kilogram"


def col(
    name,
    btype,
    pt,
    en,
    es,
    *,
    dic="no",
    directory="",
    unit="",
    obs_pt="",
    obs_en="",
    obs_es="",
    original="",
):
    """Build one architecture row. Descriptions carry no trailing period."""
    for text in (pt, en, es):
        assert text and text[0] == text[0].upper(), (
            f"{name}: description must be capitalised"
        )
        assert not text.endswith("."), (
            f"{name}: description must not end with a period"
        )
    return {
        "name": name,
        "bigquery_type": btype,
        "description_pt": pt,
        "description_en": en,
        "description_es": es,
        "temporal_coverage": "",
        "covered_by_dictionary": dic,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations_pt": obs_pt,
        "observations_en": obs_en,
        "observations_es": obs_es,
        "original_name": original,
    }


# --------------------------------------------------------------------------- #
# Shared key blocks
# --------------------------------------------------------------------------- #
def time_keys():
    return [
        col(
            "year",
            "INT64",
            "Ano de referência da estatística mensal",
            "Reference year of the monthly statistic",
            "Año de referencia de la estadística mensual",
            directory=DIR_YEAR,
            unit="year",
            obs_pt="Coluna de partição",
            obs_en="Partition column",
            obs_es="Columna de partición",
            original="YEAR",
        ),
        col(
            "month",
            "INT64",
            "Mês de referência da estatística, de 1 a 12",
            "Reference month of the statistic, from 1 to 12",
            "Mes de referencia de la estadística, de 1 a 12",
            directory=DIR_MONTH,
            unit="month",
            original="MONTH",
        ),
    ]


def country_keys():
    return [
        col(
            "country_code",
            "STRING",
            "Código do país parceiro na Schedule C do Census Bureau",
            "Partner country code in the Census Bureau Schedule C",
            "Código del país socio en la Schedule C del Census Bureau",
            dic="yes",
            obs_pt="Código de 4 dígitos com zeros à esquerda significativos. Apenas linhas de detalhe (SUMMARY_LVL='DET') são carregadas, de modo que agrupamentos de países como OPEP e União Europeia estão ausentes e os países não se somam duas vezes.",
            obs_en="Four-digit code whose leading zeros are meaningful. Only detail rows (SUMMARY_LVL='DET') are loaded, so country groupings such as OPEC and the European Union are absent and countries are not double counted.",
            obs_es="Código de cuatro dígitos cuyos ceros a la izquierda son significativos. Solo se cargan filas de detalle (SUMMARY_LVL='DET'), por lo que las agrupaciones de países como la OPEP y la Unión Europea están ausentes y los países no se cuentan dos veces.",
            original="CTY_CODE",
        ),
        col(
            "country_iso3_code",
            "STRING",
            "Sigla ISO 3166-1 alfa-3 do país parceiro",
            "ISO 3166-1 alpha-3 code of the partner country",
            "Código ISO 3166-1 alfa-3 del país socio",
            directory=DIR_COUNTRY,
            obs_pt="Derivada do código da Schedule C. Cobre 238 dos 241 códigos do Census; fica nula para Kosovo, Faixa de Gaza e Cisjordânia, que não são países no ISO 3166-1.",
            obs_en="Derived from the Schedule C code. Covers 238 of the 241 Census codes; null for Kosovo, the Gaza Strip and the West Bank, which are not countries in ISO 3166-1.",
            obs_es="Derivada del código de la Schedule C. Cubre 238 de los 241 códigos del Census; queda nula para Kosovo, la Franja de Gaza y Cisjordania, que no son países en la ISO 3166-1.",
            original="CTY_CODE",
        ),
        col(
            "country_iso2_code",
            "STRING",
            "Sigla ISO 3166-1 alfa-2 do país parceiro",
            "ISO 3166-1 alpha-2 code of the partner country",
            "Código ISO 3166-1 alfa-2 del país socio",
            obs_pt="Valor publicado na própria Schedule C, mantido como veio da fonte. A ligação ao diretório de países é feita por country_iso3_code, que é a chave primária do diretório.",
            obs_en="The value Schedule C itself publishes, kept as the source gives it. The link to the country directory is made through country_iso3_code, which is the directory's primary key.",
            obs_es="El valor publicado en la propia Schedule C, mantenido tal como viene de la fuente. El enlace al directorio de países se hace por country_iso3_code, que es la clave primaria del directorio.",
            original="CTY_CODE",
        ),
    ]


def hs_keys(side):
    """side: 'I' for imports, 'E' for exports -- fixes the source column name."""
    src = "I_COMMODITY" if side == "I" else "E_COMMODITY"
    return [
        col(
            "hs6_code",
            "STRING",
            "Código de 6 dígitos da mercadoria no Sistema Harmonizado",
            "Six-digit Harmonized System commodity code",
            "Código de seis dígitos de la mercancía en el Sistema Armonizado",
            directory=DIR_HS6,
            obs_pt="Zeros à esquerda são significativos, portanto o código é STRING e nunca INT64. O espaço de códigos não é contínuo ao longo do tempo: consulte hs_revision.",
            obs_en="Leading zeros are meaningful, so the code is STRING and never INT64. The code space is not continuous over time: see hs_revision.",
            obs_es="Los ceros a la izquierda son significativos, por lo que el código es STRING y nunca INT64. El espacio de códigos no es continuo en el tiempo: véase hs_revision.",
            original=src,
        ),
        col(
            "hs4_code",
            "STRING",
            "Código de 4 dígitos da posição do Sistema Harmonizado",
            "Four-digit Harmonized System heading code",
            "Código de cuatro dígitos de la partida del Sistema Armonizado",
            obs_pt="Primeiros 4 dígitos de hs6_code",
            obs_en="First four digits of hs6_code",
            obs_es="Primeros cuatro dígitos de hs6_code",
            original=src,
        ),
        col(
            "hs2_code",
            "STRING",
            "Código de 2 dígitos do capítulo do Sistema Harmonizado",
            "Two-digit Harmonized System chapter code",
            "Código de dos dígitos del capítulo del Sistema Armonizado",
            obs_pt="Primeiros 2 dígitos de hs6_code",
            obs_en="First two digits of hs6_code",
            obs_es="Primeros dos dígitos de hs6_code",
            original=src,
        ),
        col(
            "hs_revision",
            "STRING",
            "Revisão do Sistema Harmonizado vigente no ano de referência",
            "Harmonized System revision in force in the reference year",
            "Revisión del Sistema Armonizado vigente en el año de referencia",
            dic="yes",
            obs_pt="A OMA revisa o Sistema Harmonizado a cada cinco anos e os Estados Unidos adotam cada revisão, de modo que os códigos não são comparáveis sem ajuste entre revisões. Derivada do ano: HS2007 para 2010-2011, HS2012 para 2012-2016, HS2017 para 2017-2021 e HS2022 a partir de 2022.",
            obs_en="The WCO revises the Harmonized System every five years and the United States adopts each revision, so codes are not comparable across revisions without adjustment. Derived from the year: HS2007 for 2010-2011, HS2012 for 2012-2016, HS2017 for 2017-2021 and HS2022 from 2022 onwards.",
            obs_es="La OMA revisa el Sistema Armonizado cada cinco años y los Estados Unidos adoptan cada revisión, por lo que los códigos no son comparables entre revisiones sin ajuste. Derivada del año: HS2007 para 2010-2011, HS2012 para 2012-2016, HS2017 para 2017-2021 y HS2022 desde 2022.",
            original="",
        ),
    ]


DISTRICT = col(
    "district_code",
    "STRING",
    "Código do distrito aduaneiro na Schedule D do Census Bureau",
    "Customs district code in the Census Bureau Schedule D",
    "Código del distrito aduanero en la Schedule D del Census Bureau",
    dic="yes",
    obs_pt="Código de 2 dígitos com zeros à esquerda significativos. Nas importações é o distrito de entrada; nas exportações, o distrito de saída. Um distrito pode abranger mais de um estado.",
    obs_en="Two-digit code whose leading zeros are meaningful. On imports it is the district of entry; on exports, the district of exit. A district may span more than one state.",
    obs_es="Código de dos dígitos cuyos ceros a la izquierda son significativos. En las importaciones es el distrito de entrada; en las exportaciones, el distrito de salida. Un distrito puede abarcar más de un estado.",
    original="DISTRICT",
)

PORT = col(
    "port_code",
    "STRING",
    "Código do porto na Schedule D do Census Bureau",
    "Port code in the Census Bureau Schedule D",
    "Código del puerto en la Schedule D del Census Bureau",
    dic="yes",
    obs_pt="Código de 4 dígitos cujos 2 primeiros dígitos são o distrito aduaneiro. Zeros à esquerda são significativos.",
    obs_en="Four-digit code whose first two digits are the customs district. Leading zeros are meaningful.",
    obs_es="Código de cuatro dígitos cuyos dos primeros dígitos son el distrito aduanero. Los ceros a la izquierda son significativos.",
    original="PORT",
)

STATE_COLS = [
    col(
        "state_abbreviation",
        "STRING",
        "Sigla de duas letras do estado dos Estados Unidos",
        "Two-letter United States state abbreviation",
        "Sigla de dos letras del estado de los Estados Unidos",
        obs_pt="Nas importações é o estado de destino declarado; nas exportações, o estado de origem do movimento, que é a localização do exportador e não necessariamente onde a mercadoria foi produzida. Inclui códigos que não são estados, como distritos e territórios.",
        obs_en="On imports it is the declared state of destination; on exports, the state of origin of movement, which is the exporter's location and not necessarily where the goods were produced. Includes codes that are not states, such as districts and territories.",
        obs_es="En las importaciones es el estado de destino declarado; en las exportaciones, el estado de origen del movimiento, que es la ubicación del exportador y no necesariamente donde se produjeron los bienes. Incluye códigos que no son estados, como distritos y territorios.",
        original="STATE",
    ),
    col(
        "state_id",
        "STRING",
        "Código FIPS do estado dos Estados Unidos",
        "FIPS code of the United States state",
        "Código FIPS del estado de los Estados Unidos",
        directory=DIR_STATE,
        obs_pt="Derivado de state_abbreviation. Nulo para códigos que não correspondem a um estado ou território com código FIPS.",
        obs_en="Derived from state_abbreviation. Null for codes that do not correspond to a state or territory with a FIPS code.",
        obs_es="Derivado de state_abbreviation. Nulo para códigos que no corresponden a un estado o territorio con código FIPS.",
        original="STATE",
    ),
]

DF_COL = col(
    "domestic_foreign_code",
    "STRING",
    "Indicador de exportação doméstica ou reexportação",
    "Indicator of domestic export or re-export",
    "Indicador de exportación doméstica o reexportación",
    dic="yes",
    obs_pt="1 identifica exportações de mercadorias produzidas nos Estados Unidos e 2 identifica reexportações de mercadorias estrangeiras. É uma dimensão da linha, não um filtro: somar as duas categorias produz as exportações totais.",
    obs_en="1 identifies exports of goods produced in the United States and 2 identifies re-exports of foreign goods. It is a row dimension, not a filter: summing the two categories gives total exports.",
    obs_es="1 identifica exportaciones de bienes producidos en los Estados Unidos y 2 identifica reexportaciones de bienes extranjeros. Es una dimensión de la fila, no un filtro: sumar ambas categorías da las exportaciones totales.",
    original="DF",
)


# --------------------------------------------------------------------------- #
# Measure blocks
# --------------------------------------------------------------------------- #
def mode_cols(with_weight=True):
    """Air / vessel / containerised-vessel value and shipping weight.

    Census publishes transport mode as parallel measure columns, not as a row
    dimension, so these stay wide. Shipping weight exists for air and vessel
    only -- land-mode shipments carry no weight, so air plus vessel weight is
    not a total.
    """
    out = [
        col(
            "air_value",
            "FLOAT64",
            "Valor transportado por via aérea no mês",
            "Value shipped by air in the month",
            "Valor transportado por vía aérea en el mes",
            unit=USD,
            original="AIR_VAL_MO",
        ),
    ]
    if with_weight:
        out.append(
            col(
                "air_shipping_weight",
                "FLOAT64",
                "Peso embarcado por via aérea no mês",
                "Shipping weight moved by air in the month",
                "Peso embarcado por vía aérea en el mes",
                unit=KG,
                original="AIR_WGT_MO",
            )
        )
    out.append(
        col(
            "vessel_value",
            "FLOAT64",
            "Valor transportado por via marítima no mês",
            "Value shipped by vessel in the month",
            "Valor transportado por vía marítima en el mes",
            unit=USD,
            original="VES_VAL_MO",
        )
    )
    if with_weight:
        out.append(
            col(
                "vessel_shipping_weight",
                "FLOAT64",
                "Peso embarcado por via marítima no mês",
                "Shipping weight moved by vessel in the month",
                "Peso embarcado por vía marítima en el mes",
                unit=KG,
                original="VES_WGT_MO",
            )
        )
    out.append(
        col(
            "containerized_vessel_value",
            "FLOAT64",
            "Valor transportado em contêineres por via marítima no mês",
            "Value shipped in containers by vessel in the month",
            "Valor transportado en contenedores por vía marítima en el mes",
            unit=USD,
            obs_pt="Subconjunto de vessel_value",
            obs_en="Subset of vessel_value",
            obs_es="Subconjunto de vessel_value",
            original="CNT_VAL_MO",
        )
    )
    if with_weight:
        out.append(
            col(
                "containerized_vessel_shipping_weight",
                "FLOAT64",
                "Peso embarcado em contêineres por via marítima no mês",
                "Shipping weight moved in containers by vessel in the month",
                "Peso embarcado en contenedores por vía marítima en el mes",
                unit=KG,
                obs_pt="Subconjunto de vessel_shipping_weight",
                obs_en="Subset of vessel_shipping_weight",
                obs_es="Subconjunto de vessel_shipping_weight",
                original="CNT_WGT_MO",
            )
        )
    return out


QTY_OBS_PT = "A unidade varia por mercadoria e é dada pela coluna de unidade correspondente, portanto não há unidade de medida fixa e valores de mercadorias diferentes não devem ser somados."
QTY_OBS_EN = "The unit varies by commodity and is given by the corresponding unit column, so there is no fixed measurement unit and values for different commodities must not be summed."
QTY_OBS_ES = "La unidad varía según la mercancía y viene dada por la columna de unidad correspondiente, por lo que no hay unidad de medida fija y no deben sumarse valores de mercancías distintas."


def unit_col(n):
    return col(
        f"quantity_{n}_unit",
        "STRING",
        f"Unidade de medida da quantidade {n}",
        f"Unit of measure of quantity {n}",
        f"Unidad de medida de la cantidad {n}",
        obs_pt="Abreviatura de unidade usada pelo Census, como KG para quilogramas e NO para número de unidades. Não é coberta pelo dicionário porque o Census não publica a lista de códigos em formato legível por máquina e os valores armazenados já são a abreviatura legível.",
        obs_en="Census unit abbreviation, such as KG for kilograms and NO for number of units. Not dictionary-covered because Census publishes no machine-readable list of these codes and the stored values are already the readable abbreviation.",
        obs_es="Abreviatura de unidad usada por el Census, como KG para kilogramos y NO para número de unidades. No está cubierta por el diccionario porque el Census no publica la lista de códigos en formato legible por máquina y los valores almacenados ya son la abreviatura legible.",
        original=f"UNIT_QY{n}",
    )


CARD_COUNT = col(
    "card_count",
    "INT64",
    "Número de registros administrativos agregados na linha",
    "Number of administrative records aggregated into the row",
    "Número de registros administrativos agregados en la fila",
    unit="record",
    original="CC_MO",
)


# --------------------------------------------------------------------------- #
# Tables
# --------------------------------------------------------------------------- #
def import_district():
    return [
        *time_keys(),
        *country_keys(),
        DISTRICT,
        *hs_keys("I"),
        col(
            "general_value",
            "FLOAT64",
            "Valor das importações gerais no mês",
            "General imports value in the month",
            "Valor de las importaciones generales en el mes",
            unit=USD,
            obs_pt="Importações gerais medem o total de mercadorias que entram no país, somando as que vão para consumo imediato e as que entram em entreposto aduaneiro.",
            obs_en="General imports measure total goods entering the country, combining those going to immediate consumption and those entering bonded warehouses.",
            obs_es="Las importaciones generales miden el total de bienes que entran al país, sumando las que van a consumo inmediato y las que entran en depósito aduanero.",
            original="GEN_VAL_MO",
        ),
        col(
            "general_cif_value",
            "FLOAT64",
            "Valor CIF das importações gerais no mês",
            "General imports CIF value in the month",
            "Valor CIF de las importaciones generales en el mes",
            unit=USD,
            obs_pt="Inclui custo, seguro e frete",
            obs_en="Includes cost, insurance and freight",
            obs_es="Incluye costo, seguro y flete",
            original="GEN_CIF_MO",
        ),
        col(
            "general_charges",
            "FLOAT64",
            "Despesas de seguro e frete das importações gerais no mês",
            "Insurance and freight charges on general imports in the month",
            "Gastos de seguro y flete de las importaciones generales en el mes",
            unit=USD,
            original="GEN_CHA_MO",
        ),
        col(
            "general_quantity_1",
            "FLOAT64",
            "Primeira quantidade das importações gerais no mês",
            "First quantity of general imports in the month",
            "Primera cantidad de las importaciones generales en el mes",
            obs_pt=QTY_OBS_PT,
            obs_en=QTY_OBS_EN,
            obs_es=QTY_OBS_ES,
            original="GEN_QY1_MO",
        ),
        col(
            "general_quantity_2",
            "FLOAT64",
            "Segunda quantidade das importações gerais no mês",
            "Second quantity of general imports in the month",
            "Segunda cantidad de las importaciones generales en el mes",
            obs_pt=QTY_OBS_PT,
            obs_en=QTY_OBS_EN,
            obs_es=QTY_OBS_ES,
            original="GEN_QY2_MO",
        ),
        col(
            "consumption_value",
            "FLOAT64",
            "Valor das importações para consumo no mês",
            "Imports for consumption value in the month",
            "Valor de las importaciones para consumo en el mes",
            unit=USD,
            obs_pt="Importações para consumo medem as mercadorias que entram no fluxo do comércio interno, somando a entrada direta e a retirada de entreposto aduaneiro. Não somar com general_value: as duas medidas se sobrepõem.",
            obs_en="Imports for consumption measure goods entering the domestic stream of commerce, combining direct entry and withdrawal from bonded warehouse. Do not add to general_value: the two measures overlap.",
            obs_es="Las importaciones para consumo miden los bienes que entran en el flujo del comercio interno, sumando la entrada directa y el retiro de depósito aduanero. No sumar con general_value: ambas medidas se solapan.",
            original="CON_VAL_MO",
        ),
        col(
            "consumption_cif_value",
            "FLOAT64",
            "Valor CIF das importações para consumo no mês",
            "Imports for consumption CIF value in the month",
            "Valor CIF de las importaciones para consumo en el mes",
            unit=USD,
            original="CON_CIF_MO",
        ),
        col(
            "consumption_charges",
            "FLOAT64",
            "Despesas de seguro e frete das importações para consumo no mês",
            "Insurance and freight charges on imports for consumption in the month",
            "Gastos de seguro y flete de las importaciones para consumo en el mes",
            unit=USD,
            original="CON_CHA_MO",
        ),
        col(
            "consumption_quantity_1",
            "FLOAT64",
            "Primeira quantidade das importações para consumo no mês",
            "First quantity of imports for consumption in the month",
            "Primera cantidad de las importaciones para consumo en el mes",
            obs_pt=QTY_OBS_PT,
            obs_en=QTY_OBS_EN,
            obs_es=QTY_OBS_ES,
            original="CON_QY1_MO",
        ),
        col(
            "consumption_quantity_2",
            "FLOAT64",
            "Segunda quantidade das importações para consumo no mês",
            "Second quantity of imports for consumption in the month",
            "Segunda cantidad de las importaciones para consumo en el mes",
            obs_pt=QTY_OBS_PT,
            obs_en=QTY_OBS_EN,
            obs_es=QTY_OBS_ES,
            original="CON_QY2_MO",
        ),
        unit_col(1),
        unit_col(2),
        col(
            "dutiable_value",
            "FLOAT64",
            "Valor tributável das importações para consumo no mês",
            "Dutiable value of imports for consumption in the month",
            "Valor imponible de las importaciones para consumo en el mes",
            unit=USD,
            original="DUT_VAL_MO",
        ),
        col(
            "calculated_duty",
            "FLOAT64",
            "Imposto de importação calculado sobre as importações para consumo no mês",
            "Calculated duty on imports for consumption in the month",
            "Arancel calculado sobre las importaciones para consumo en el mes",
            unit=USD,
            obs_pt="É o imposto calculado a partir das alíquotas vigentes e não o valor efetivamente arrecadado",
            obs_en="This is duty computed from the rates in force, not duty actually collected",
            obs_es="Es el arancel calculado a partir de las tasas vigentes y no el efectivamente recaudado",
            original="CAL_DUT_MO",
        ),
        *mode_cols(),
        CARD_COUNT,
    ]


def export_district():
    return [
        *time_keys(),
        *country_keys(),
        DISTRICT,
        *hs_keys("E"),
        DF_COL,
        col(
            "total_value",
            "FLOAT64",
            "Valor total das exportações no mês",
            "Total exports value in the month",
            "Valor total de las exportaciones en el mes",
            unit=USD,
            obs_pt="Valor FAS, isto é, o valor das mercadorias no porto de saída dos Estados Unidos, excluindo frete e seguro internacionais",
            obs_en="FAS value, that is the value of the goods at the United States port of exit, excluding international freight and insurance",
            obs_es="Valor FAS, es decir, el valor de las mercancías en el puerto de salida de los Estados Unidos, excluyendo flete y seguro internacionales",
            original="ALL_VAL_MO",
        ),
        col(
            "quantity_1",
            "FLOAT64",
            "Primeira quantidade exportada no mês",
            "First quantity exported in the month",
            "Primera cantidad exportada en el mes",
            obs_pt=QTY_OBS_PT,
            obs_en=QTY_OBS_EN,
            obs_es=QTY_OBS_ES,
            original="QTY_1_MO",
        ),
        col(
            "quantity_2",
            "FLOAT64",
            "Segunda quantidade exportada no mês",
            "Second quantity exported in the month",
            "Segunda cantidad exportada en el mes",
            obs_pt=QTY_OBS_PT,
            obs_en=QTY_OBS_EN,
            obs_es=QTY_OBS_ES,
            original="QTY_2_MO",
        ),
        unit_col(1),
        unit_col(2),
        *mode_cols(),
        CARD_COUNT,
    ]


def import_port():
    return [
        *time_keys(),
        *country_keys(),
        PORT,
        *hs_keys("I"),
        col(
            "general_value",
            "FLOAT64",
            "Valor das importações gerais no mês",
            "General imports value in the month",
            "Valor de las importaciones generales en el mes",
            unit=USD,
            obs_pt="O endpoint por porto publica apenas as importações gerais: valor para consumo, imposto e quantidade existem somente na tabela por distrito aduaneiro.",
            obs_en="The port endpoint publishes general imports only: consumption value, duty and quantity exist only in the customs district table.",
            obs_es="El endpoint por puerto publica solo las importaciones generales: valor para consumo, arancel y cantidad existen solo en la tabla por distrito aduanero.",
            original="GEN_VAL_MO",
        ),
        *mode_cols(),
    ]


def export_port():
    return [
        *time_keys(),
        *country_keys(),
        PORT,
        *hs_keys("E"),
        col(
            "total_value",
            "FLOAT64",
            "Valor total das exportações no mês",
            "Total exports value in the month",
            "Valor total de las exportaciones en el mes",
            unit=USD,
            obs_pt="Valor FAS. O endpoint por porto não publica quantidade nem a separação entre exportação doméstica e reexportação, que existem somente na tabela por distrito aduaneiro.",
            obs_en="FAS value. The port endpoint publishes neither quantity nor the domestic export versus re-export split, which exist only in the customs district table.",
            obs_es="Valor FAS. El endpoint por puerto no publica cantidad ni la separación entre exportación doméstica y reexportación, que existen solo en la tabla por distrito aduanero.",
            original="ALL_VAL_MO",
        ),
        *mode_cols(),
    ]


def import_state():
    return [
        *time_keys(),
        *country_keys(),
        *STATE_COLS,
        *hs_keys("I"),
        col(
            "general_value",
            "FLOAT64",
            "Valor das importações gerais no mês",
            "General imports value in the month",
            "Valor de las importaciones generales en el mes",
            unit=USD,
            original="GEN_VAL_MO",
        ),
        col(
            "consumption_value",
            "FLOAT64",
            "Valor das importações para consumo no mês",
            "Imports for consumption value in the month",
            "Valor de las importaciones para consumo en el mes",
            unit=USD,
            obs_pt="Não somar com general_value: as duas medidas se sobrepõem",
            obs_en="Do not add to general_value: the two measures overlap",
            obs_es="No sumar con general_value: ambas medidas se solapan",
            original="CON_VAL_MO",
        ),
        *mode_cols(),
    ]


def export_state():
    return [
        *time_keys(),
        *country_keys(),
        *STATE_COLS,
        *hs_keys("E"),
        col(
            "total_value",
            "FLOAT64",
            "Valor total das exportações no mês",
            "Total exports value in the month",
            "Valor total de las exportaciones en el mes",
            unit=USD,
            obs_pt="Valor FAS. O endpoint por estado não publica quantidade.",
            obs_en="FAS value. The state endpoint does not publish quantity.",
            obs_es="Valor FAS. El endpoint por estado no publica cantidad.",
            original="ALL_VAL_MO",
        ),
        *mode_cols(),
    ]


def dicionario():
    return [
        col(
            "id_tabela",
            "STRING",
            "Nome da tabela à qual a coluna pertence",
            "Name of the table the column belongs to",
            "Nombre de la tabla a la que pertenece la columna",
        ),
        col(
            "nome_coluna",
            "STRING",
            "Nome da coluna codificada",
            "Name of the coded column",
            "Nombre de la columna codificada",
        ),
        col(
            "chave",
            "STRING",
            "Valor codificado",
            "Coded value",
            "Valor codificado",
        ),
        col(
            "cobertura_temporal",
            "STRING",
            "Cobertura temporal do valor codificado",
            "Temporal coverage of the coded value",
            "Cobertura temporal del valor codificado",
        ),
        col(
            "valor",
            "STRING",
            "Valor traduzido do código",
            "Translated value of the code",
            "Valor traducido del código",
        ),
    ]


TABLES = {
    "import": import_district,
    "export": export_district,
    "import_port": import_port,
    "export_port": export_port,
    "import_state": import_state,
    "export_state": export_state,
    "dicionario": dicionario,
}


def main():
    for table, fn in TABLES.items():
        rows = fn()
        names = [r["name"] for r in rows]
        assert len(names) == len(set(names)), (
            f"{table}: duplicate column names"
        )
        for r in rows:
            if r["bigquery_type"] in ("INT64", "FLOAT64"):
                has_unit = bool(r["measurement_unit"])
                is_quantity = (
                    r["name"].startswith("quantity_")
                    or "_quantity_" in r["name"]
                )
                assert has_unit or is_quantity, (
                    f"{table}.{r['name']}: numeric column needs a measurement_unit "
                    "(or must be a variable-unit quantity)"
                )
            if r["directory_column"]:
                assert r["covered_by_dictionary"] == "no", (
                    f"{table}.{r['name']}: directory-referenced columns are never "
                    "covered_by_dictionary"
                )
        path = HERE / f"{table}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            # lineterminator="\n": csv defaults to CRLF, which the repo's
            # mixed-line-ending pre-commit hook rewrites on every regeneration.
            w = csv.DictWriter(fh, fieldnames=HEADER, lineterminator="\n")
            w.writeheader()
            w.writerows(rows)
        print(f"{table:14s} {len(rows):3d} columns -> {path.name}")


if __name__ == "__main__":
    main()
