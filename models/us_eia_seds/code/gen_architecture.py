"""Write the architecture CSV for us_eia_seds.

    python gen_architecture.py

The architecture CSV is the schema source of truth: column order, bigquery_type,
directory links, units and the trilingual descriptions the dbt models and the
backend column payloads are both generated from. This dataset has a single data
table, so the column definitions are declared here directly (there is no PUDL map
to derive them from, unlike us_eia_electricity).
"""

import csv

from common import ARCHITECTURE_DIR

# Architecture CSV columns, in the order the backend and the style manual expect.
FIELDS = [
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

# One dict per column. Missing keys default to empty. Column ORDER here is the
# table's column order: partition (year) first, then the geographic id, then the
# identifiers, then the descriptive columns.
SEDS_CONSUMPTION = [
    {
        "name": "year",
        "bigquery_type": "INT64",
        "description_pt": "Ano-calendário a que o valor se refere",
        "description_en": "Calendar year the value refers to",
        "description_es": "Año calendario al que se refiere el valor",
        "directory_column": "diretorios_data_tempo.ano:ano",
        "measurement_unit": "year",
        "original_name": "Year",
    },
    {
        "name": "state_id",
        "bigquery_type": "STRING",
        "description_pt": "Código FIPS do estado dos Estados Unidos; nulo para os agregados que não são estados (total nacional e áreas federais offshore)",
        "description_en": "FIPS code of the U.S. state; null for the non-state aggregates (national total and federal offshore areas)",
        "description_es": "Código FIPS del estado de los Estados Unidos; nulo para los agregados que no son estados (total nacional y áreas federales costa afuera)",
        "directory_column": "br_bd_diretorios_us.state:id_state",
        "original_name": "StateCode",
    },
    {
        "name": "state_code",
        "bigquery_type": "STRING",
        "description_pt": "Código de dois caracteres da fonte para o estado ou agregado: as siglas postais dos 50 estados e do Distrito de Columbia, US para o total nacional, e X3 e X5 para as áreas federais offshore do Golfo e da Costa Oeste",
        "description_en": "Source two-character code for the state or aggregate: the postal abbreviations of the 50 states and the District of Columbia, US for the national total, and X3 and X5 for the federal offshore Gulf and West Coast areas",
        "description_es": "Código de dos caracteres de la fuente para el estado o agregado: las siglas postales de los 50 estados y el Distrito de Columbia, US para el total nacional, y X3 y X5 para las áreas federales costa afuera del Golfo y la Costa Oeste",
        "covered_by_dictionary": "yes",
        "original_name": "StateCode",
    },
    {
        "name": "msn",
        "bigquery_type": "STRING",
        "description_pt": "Nome mnemônico da série (MSN), o código de cinco caracteres da EIA em que os dois primeiros identificam a fonte de energia, o terceiro e o quarto o setor, e o quinto a medida; o significado completo de cada código está na tabela dicionario",
        "description_en": "Mnemonic Series Name (MSN), EIA's five-character code in which the first two characters identify the energy source, the third and fourth the sector, and the fifth the measure; the full meaning of each code is in the dicionario table",
        "description_es": "Nombre mnemónico de la serie (MSN), el código de cinco caracteres de la EIA en el que los dos primeros identifican la fuente de energía, el tercero y el cuarto el sector, y el quinto la medida; el significado completo de cada código está en la tabla dicionario",
        "covered_by_dictionary": "yes",
        "original_name": "MSN",
    },
    {
        "name": "measure_type",
        "bigquery_type": "STRING",
        "description_pt": "Tipo de medida da série, derivado do quinto caractere do MSN: consumo em Btu, consumo em unidades físicas, preço, despesa, emissões de CO2, eletricidade, capacidade, fator de conversão, contagem ou outro",
        "description_en": "Measure type of the series, derived from the fifth character of the MSN: consumption in Btu, consumption in physical units, price, expenditure, CO2 emissions, electricity, capacity, conversion factor, count or other",
        "description_es": "Tipo de medida de la serie, derivado del quinto carácter del MSN: consumo en Btu, consumo en unidades físicas, precio, gasto, emisiones de CO2, electricidad, capacidad, factor de conversión, recuento u otro",
        "covered_by_dictionary": "yes",
        "original_name": "MSN",
    },
    {
        "name": "value",
        "bigquery_type": "FLOAT64",
        "description_pt": "Valor da série para o estado e o ano",
        "description_en": "Value of the series for the state and year",
        "description_es": "Valor de la serie para el estado y el año",
        "observations_pt": "A unidade varia conforme o MSN — consumo em Btu ou em unidades físicas, preço, despesa, emissões, capacidade — de modo que a coluna não carrega uma unidade única; a unidade de cada linha está em measurement_unit",
        "observations_en": "The unit varies by MSN — consumption in Btu or physical units, price, expenditure, emissions, capacity — so the column carries no single unit; each row's unit is in measurement_unit",
        "observations_es": "La unidad varía según el MSN — consumo en Btu o en unidades físicas, precio, gasto, emisiones, capacidad — de modo que la columna no lleva una unidad única; la unidad de cada fila está en measurement_unit",
        "original_name": "Data",
    },
    {
        "name": "measurement_unit",
        "bigquery_type": "STRING",
        "description_pt": "Unidade de medida do valor desta linha, conforme a EIA publica para o MSN (por exemplo, bilhões de Btu, milhares de barris, dólares por milhão de Btu, milhões de dólares)",
        "description_en": "Unit of measurement of this row's value, as EIA publishes it for the MSN (for example, billion Btu, thousand barrels, dollars per million Btu, million dollars)",
        "description_es": "Unidad de medida del valor de esta fila, tal como la EIA la publica para el MSN (por ejemplo, miles de millones de Btu, miles de barriles, dólares por millón de Btu, millones de dólares)",
        "original_name": "MSN",
    },
    {
        "name": "data_status",
        "bigquery_type": "STRING",
        "description_pt": "Situação e safra da estimativa, conforme publicado pela fonte (por exemplo, 2024F para as estimativas finais de 2024)",
        "description_en": "Status and vintage of the estimate, as published by the source (for example, 2024F for the 2024 final estimates)",
        "description_es": "Situación y cosecha de la estimación, tal como la publica la fuente (por ejemplo, 2024F para las estimaciones finales de 2024)",
        "covered_by_dictionary": "yes",
        "original_name": "Data_Status",
    },
]

TABLES = {"seds_consumption": SEDS_CONSUMPTION}


def write_table(table: str, cols: list[dict]) -> None:
    ARCHITECTURE_DIR.mkdir(parents=True, exist_ok=True)
    path = ARCHITECTURE_DIR / f"sheet_{table}.csv"
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()
        for col in cols:
            writer.writerow({k: col.get(k, "") for k in FIELDS})
    print(f"wrote {path} ({len(cols)} columns)")


def main() -> None:
    for table, cols in TABLES.items():
        write_table(table, cols)


if __name__ == "__main__":
    main()
