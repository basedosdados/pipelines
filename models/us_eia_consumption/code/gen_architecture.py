"""Write the architecture CSVs for us_eia_consumption.

    python gen_architecture.py

The architecture CSVs are the schema source of truth: column order,
bigquery_type, directory links, units and the trilingual descriptions the dbt
models and the backend column payloads are both generated from.
"""

import csv

from common import ARCHITECTURE_DIR

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


def col(name, t, pt, en, es, **kw):
    d = {
        "name": name,
        "bigquery_type": t,
        "description_pt": pt,
        "description_en": en,
        "description_es": es,
    }
    d.update(kw)
    return d


YEAR = col(
    "year",
    "INT64",
    "Ano-calendário de referência do formulário",
    "Report calendar year of the form",
    "Año calendario de referencia del formulario",
    directory_column="diretorios_data_tempo.ano:ano",
    measurement_unit="year",
    original_name="Year",
)
UTILITY_ID = col(
    "utility_id",
    "STRING",
    "Identificador da empresa (Utility Number) atribuído pela EIA; é a chave de junção estável com o conjunto us_eia_electricity e entre os formulários da EIA",
    "Utility Number assigned by EIA; the stable join key with the us_eia_electricity dataset and across EIA forms",
    "Número de empresa (Utility Number) asignado por la EIA; es la clave de unión estable con el conjunto us_eia_electricity y entre los formularios de la EIA",
    original_name="Utility Number",
    observations_pt="Junta-se a us_eia_electricity pelo mesmo identificador de empresa da EIA",
    observations_en="Joins to us_eia_electricity on the same EIA utility identifier",
    observations_es="Se une a us_eia_electricity por el mismo identificador de empresa de la EIA",
)
UTILITY_NAME = col(
    "utility_name",
    "STRING",
    "Nome da empresa conforme declarado no formulário",
    "Utility name as reported on the form",
    "Nombre de la empresa según lo declarado en el formulario",
    original_name="Utility Name",
)
STATE_ID = col(
    "state_id",
    "STRING",
    "Código FIPS do estado dos Estados Unidos",
    "FIPS code of the U.S. state",
    "Código FIPS del estado de los Estados Unidos",
    directory_column="br_bd_diretorios_us.state:id_state",
    original_name="State",
)
OWNERSHIP = col(
    "ownership_type",
    "STRING",
    "Tipo de propriedade da empresa, conforme a EIA (por exemplo, Investor Owned, Municipal, Cooperative, Federal, Political Subdivision, Power Marketer)",
    "Ownership type of the utility, as classified by EIA (for example Investor Owned, Municipal, Cooperative, Federal, Political Subdivision, Power Marketer)",
    "Tipo de propiedad de la empresa, según la EIA (por ejemplo Investor Owned, Municipal, Cooperative, Federal, Political Subdivision, Power Marketer)",
    original_name="Ownership",
)
NERC = col(
    "nerc_region",
    "STRING",
    "Região de confiabilidade da NERC em que a empresa opera (por exemplo, WECC, SERC, RFC)",
    "NERC reliability region in which the utility operates (for example WECC, SERC, RFC)",
    "Región de confiabilidad de la NERC en la que opera la empresa (por ejemplo WECC, SERC, RFC)",
    original_name="NERC Region",
)

TABLES = {
    "utility": [YEAR, UTILITY_ID, UTILITY_NAME, STATE_ID, OWNERSHIP, NERC],
    "retail_sales": [
        YEAR,
        UTILITY_ID,
        UTILITY_NAME,
        STATE_ID,
        OWNERSHIP,
        col(
            "ba_code",
            "STRING",
            "Código da autoridade de balanceamento (balancing authority) da empresa; coletado a partir de 2012",
            "Balancing authority code of the utility; collected from 2012 on",
            "Código de la autoridad de balance de la empresa; recopilado desde 2012",
            original_name="BA Code",
        ),
        col(
            "part",
            "STRING",
            "Parte do Anexo 4 do formulário em que a linha foi declarada, indicando o tipo de arranjo de serviço",
            "Part of the form's Schedule 4 the row was reported under, indicating the type of service arrangement",
            "Parte del Anexo 4 del formulario en la que se declaró la fila, que indica el tipo de arreglo de servicio",
            covered_by_dictionary="yes",
            original_name="Part",
        ),
        col(
            "service_type",
            "STRING",
            "Tipo de serviço prestado: Bundled (energia e distribuição), Energy (só energia) ou Delivery (só distribuição)",
            "Type of service provided: Bundled (energy and delivery), Energy (energy only) or Delivery (delivery only)",
            "Tipo de servicio prestado: Bundled (energía y distribución), Energy (solo energía) o Delivery (solo distribución)",
            original_name="Service Type",
        ),
        col(
            "data_type",
            "STRING",
            "Indica se o valor foi observado (declarado pelo respondente) ou imputado pela EIA",
            "Whether the value was observed (reported by the respondent) or imputed by EIA",
            "Indica si el valor fue observado (declarado por el encuestado) o imputado por la EIA",
            covered_by_dictionary="yes",
            original_name="Data Type",
        ),
        col(
            "customer_sector",
            "STRING",
            "Setor de consumo final: residencial, comercial, industrial ou transporte",
            "End-use customer sector: residential, commercial, industrial or transportation",
            "Sector de consumo final: residencial, comercial, industrial o transporte",
            original_name="(sector banner)",
        ),
        col(
            "sales_mwh",
            "FLOAT64",
            "Vendas de eletricidade a consumidores finais no setor",
            "Electricity sales to end-use customers in the sector",
            "Ventas de electricidad a consumidores finales en el sector",
            measurement_unit="megawatt_hour",
            original_name="Sales (Megawatthours)",
        ),
        col(
            "revenue_usd",
            "FLOAT64",
            "Receita com vendas a consumidores finais no setor, convertida de milhares de dólares para dólares",
            "Revenue from sales to end-use customers in the sector, converted from thousands of dollars to dollars",
            "Ingresos por ventas a consumidores finales en el sector, convertidos de miles de dólares a dólares",
            measurement_unit="usd",
            original_name="Revenues (Thousand Dollars)",
        ),
        col(
            "customer_count",
            "INT64",
            "Número de consumidores finais atendidos no setor",
            "Number of end-use customers served in the sector",
            "Número de consumidores finales atendidos en el sector",
            measurement_unit="unit",
            original_name="Customers (Count)",
        ),
        col(
            "average_price_cents_kwh",
            "FLOAT64",
            "Preço médio ao consumidor final no setor, derivado como receita dividida por vendas",
            "Average price to the end-use customer in the sector, derived as revenue divided by sales",
            "Precio medio al consumidor final en el sector, derivado como ingresos divididos por ventas",
            measurement_unit="cents_per_kilowatt_hour",
            original_name="(derived)",
            observations_pt="Derivado: receita em milhares de dólares x 100 dividida por vendas em MWh; nulo quando não há vendas",
            observations_en="Derived: revenue in thousands of dollars times 100 divided by sales in MWh; null when there are no sales",
            observations_es="Derivado: ingresos en miles de dólares por 100 divididos por ventas en MWh; nulo cuando no hay ventas",
        ),
    ],
    "service_territory": [
        YEAR,
        UTILITY_ID,
        UTILITY_NAME,
        STATE_ID,
        col(
            "county_id",
            "STRING",
            "Código FIPS do condado atendido pela empresa",
            "FIPS code of the county served by the utility",
            "Código FIPS del condado atendido por la empresa",
            directory_column="br_bd_diretorios_us.county:id_county",
            original_name="County",
        ),
        col(
            "county_name",
            "STRING",
            "Nome do condado atendido, conforme declarado",
            "Name of the county served, as reported",
            "Nombre del condado atendido, según lo declarado",
            original_name="County",
        ),
        col(
            "short_form",
            "STRING",
            "Indica se a empresa preencheu o formulário simplificado (EIA-861S)",
            "Whether the utility filed the short form (EIA-861S)",
            "Indica si la empresa presentó el formulario simplificado (EIA-861S)",
            covered_by_dictionary="yes",
            original_name="Short Form",
        ),
    ],
    "eia861m": [
        YEAR,
        col(
            "month",
            "INT64",
            "Mês de referência (1 a 12)",
            "Report month (1 to 12)",
            "Mes de referencia (1 a 12)",
            directory_column="diretorios_data_tempo.mes:mes",
            measurement_unit="month",
            original_name="Month",
        ),
        STATE_ID,
        col(
            "state_code",
            "STRING",
            "Código postal de dois caracteres do estado ou território conforme a fonte",
            "Two-character postal code of the state or territory as published by the source",
            "Código postal de dos caracteres del estado o territorio según la fuente",
            original_name="State",
        ),
        col(
            "customer_sector",
            "STRING",
            "Setor de consumo final: residencial, comercial, industrial ou transporte",
            "End-use customer sector: residential, commercial, industrial or transportation",
            "Sector de consumo final: residencial, comercial, industrial o transporte",
            original_name="(sector banner)",
        ),
        col(
            "data_status",
            "STRING",
            "Situação do dado do mês: preliminar ou final",
            "Status of the month's data: preliminary or final",
            "Situación del dato del mes: preliminar o final",
            original_name="Data Status",
        ),
        col(
            "sales_mwh",
            "FLOAT64",
            "Vendas de eletricidade a consumidores finais no setor no mês",
            "Electricity sales to end-use customers in the sector in the month",
            "Ventas de electricidad a consumidores finales en el sector en el mes",
            measurement_unit="megawatt_hour",
            original_name="Sales (Megawatthours)",
        ),
        col(
            "revenue_usd",
            "FLOAT64",
            "Receita com vendas a consumidores finais no setor no mês, convertida de milhares de dólares para dólares",
            "Revenue from sales to end-use customers in the sector in the month, converted from thousands of dollars to dollars",
            "Ingresos por ventas a consumidores finales en el sector en el mes, convertidos de miles de dólares a dólares",
            measurement_unit="usd",
            original_name="Revenue (Thousand Dollars)",
        ),
        col(
            "customer_count",
            "INT64",
            "Número de consumidores finais atendidos no setor no mês",
            "Number of end-use customers served in the sector in the month",
            "Número de consumidores finales atendidos en el sector en el mes",
            measurement_unit="unit",
            original_name="Customers (Count)",
        ),
        col(
            "average_price_cents_kwh",
            "FLOAT64",
            "Preço médio ao consumidor final no setor no mês, conforme publicado pela fonte",
            "Average price to the end-use customer in the sector in the month, as published by the source",
            "Precio medio al consumidor final en el sector en el mes, según lo publicado por la fuente",
            measurement_unit="cents_per_kilowatt_hour",
            original_name="Price (Cents/kWh)",
        ),
    ],
}


def main() -> None:
    ARCHITECTURE_DIR.mkdir(parents=True, exist_ok=True)
    for table, cols in TABLES.items():
        path = ARCHITECTURE_DIR / f"sheet_{table}.csv"
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS)
            w.writeheader()
            for c in cols:
                w.writerow({k: c.get(k, "") for k in FIELDS})
        print(f"wrote {path} ({len(cols)} columns)")


if __name__ == "__main__":
    main()
