"""Generate architecture CSVs for world_wb_wdi (World Bank WDI).

The architecture CSVs are the single schema source of truth: column order and
bigquery_type feed both the one-shot bootstrap (models/.../code/clean.py) and the
recurring pipeline (pipelines/datasets/world_wb_wdi/utils.py). Run this to
regenerate architecture/*.csv.

Six tables mirror the six files inside WDI_CSV.zip plus a derived dictionary:
  data              <- WDICSV.csv          (wide -> long, drop nulls)
  indicators        <- WDISeries.csv       (drop Indicator Name)
  country_indicator <- WDIcountry-series.csv
  footnote          <- WDIfootnote.csv
  indicator_time    <- WDIseries-time.csv
  dicionario        <- derived (indicator_id -> Indicator Name)
"""

import csv
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
ARCH.mkdir(parents=True, exist_ok=True)

FIELDS = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
]

# Each column: (name, type, pt_description, covered_by_dictionary, measurement_unit,
#               original_name, observations)
TABLES = {
    "data": [
        ("year", "INT64", "Ano de referência da observação", "no", "", "", ""),
        (
            "country_id",
            "STRING",
            "Código do país ou agregado regional, no padrão do Banco Mundial",
            "no",
            "",
            "Country Code",
            "Inclui países e agregados regionais/de renda (ex.: WLD, ARB, AFE)",
        ),
        (
            "indicator_id",
            "STRING",
            "Código do indicador WDI",
            "yes",
            "",
            "Indicator Code",
            "",
        ),
        (
            "value",
            "FLOAT64",
            "Valor observado do indicador",
            "no",
            "",
            "value",
            "A unidade depende do indicador; ver measurement_unit na tabela indicators",
        ),
    ],
    "indicators": [
        (
            "indicator_id",
            "STRING",
            "Código do indicador",
            "no",
            "",
            "Series Code",
            "",
        ),
        ("topic", "STRING", "Tema do indicador", "no", "", "Topic", ""),
        (
            "short_definition",
            "STRING",
            "Definição curta do indicador",
            "no",
            "",
            "Short definition",
            "",
        ),
        (
            "long_definition",
            "STRING",
            "Definição longa do indicador",
            "no",
            "",
            "Long definition",
            "",
        ),
        (
            "measurement_unit",
            "STRING",
            "Unidade de medida do indicador",
            "no",
            "",
            "Unit of measure",
            "",
        ),
        (
            "periodicity",
            "STRING",
            "Periodicidade do indicador",
            "no",
            "",
            "Periodicity",
            "",
        ),
        (
            "base_period",
            "STRING",
            "Período base do indicador",
            "no",
            "",
            "Base Period",
            "",
        ),
        (
            "other_notes",
            "STRING",
            "Outras notas sobre o indicador",
            "no",
            "",
            "Other notes",
            "",
        ),
        (
            "aggregation_method",
            "STRING",
            "Método de agregação do indicador",
            "no",
            "",
            "Aggregation method",
            "",
        ),
        (
            "limitations_exceptions",
            "STRING",
            "Limitações e exceções do indicador",
            "no",
            "",
            "Limitations and exceptions",
            "",
        ),
        (
            "notes_from_original_source",
            "STRING",
            "Notas da fonte original",
            "no",
            "",
            "Notes from original source",
            "",
        ),
        (
            "general_comments",
            "STRING",
            "Comentários gerais sobre o indicador",
            "no",
            "",
            "General comments",
            "",
        ),
        ("source", "STRING", "Fonte do indicador", "no", "", "Source", ""),
        (
            "statistical_concept_methodology",
            "STRING",
            "Conceito estatístico e metodologia",
            "no",
            "",
            "Statistical concept and methodology",
            "",
        ),
        (
            "development_relevance",
            "STRING",
            "Relevância do indicador para o desenvolvimento",
            "no",
            "",
            "Development relevance",
            "",
        ),
        (
            "related_source_links",
            "STRING",
            "Links de fontes relacionadas",
            "no",
            "",
            "Related source links",
            "",
        ),
        (
            "other_web_links",
            "STRING",
            "Outros links da web",
            "no",
            "",
            "Other web links",
            "",
        ),
        (
            "related_indicators",
            "STRING",
            "Indicadores relacionados",
            "no",
            "",
            "Related indicators",
            "",
        ),
        (
            "license_type",
            "STRING",
            "Tipo de licença do indicador",
            "no",
            "",
            "License Type",
            "",
        ),
    ],
    "country_indicator": [
        (
            "country_id",
            "STRING",
            "Código do país ou agregado regional",
            "no",
            "",
            "CountryCode",
            "",
        ),
        (
            "indicator_id",
            "STRING",
            "Código do indicador WDI",
            "no",
            "",
            "SeriesCode",
            "",
        ),
        (
            "description",
            "STRING",
            "Observação sobre a série do indicador para o país",
            "no",
            "",
            "DESCRIPTION",
            "",
        ),
    ],
    "footnote": [
        (
            "year",
            "INT64",
            "Ano de referência da nota de rodapé",
            "no",
            "",
            "Year",
            "",
        ),
        (
            "country_id",
            "STRING",
            "Código do país ou agregado regional",
            "no",
            "",
            "CountryCode",
            "",
        ),
        (
            "indicator_id",
            "STRING",
            "Código do indicador WDI",
            "no",
            "",
            "SeriesCode",
            "",
        ),
        (
            "description",
            "STRING",
            "Nota de rodapé associada à observação",
            "no",
            "",
            "DESCRIPTION",
            "",
        ),
    ],
    "indicator_time": [
        (
            "year",
            "INT64",
            "Ano de referência da nota temporal",
            "no",
            "",
            "Year",
            "",
        ),
        (
            "indicator_id",
            "STRING",
            "Código do indicador WDI",
            "no",
            "",
            "SeriesCode",
            "",
        ),
        (
            "description",
            "STRING",
            "Nota temporal associada ao indicador",
            "no",
            "",
            "DESCRIPTION",
            "",
        ),
    ],
    "dicionario": [
        (
            "id_tabela",
            "STRING",
            "Nome da tabela à qual a chave se refere",
            "no",
            "",
            "",
            "",
        ),
        (
            "nome_coluna",
            "STRING",
            "Nome da coluna à qual a chave se refere",
            "no",
            "",
            "",
            "",
        ),
        (
            "chave",
            "STRING",
            "Chave (valor codificado) presente na coluna",
            "no",
            "",
            "",
            "",
        ),
        (
            "cobertura_temporal",
            "STRING",
            "Cobertura temporal da chave",
            "no",
            "",
            "",
            "",
        ),
        (
            "valor",
            "STRING",
            "Valor (rótulo legível) correspondente à chave",
            "no",
            "",
            "",
            "",
        ),
    ],
}

# Per-table temporal coverage annotation on the year column.
YEAR_COVERAGE = {"data": "1960(1)2025", "footnote": "", "indicator_time": ""}


def main():
    for table, cols in TABLES.items():
        path = ARCH / f"{table}.csv"
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS, lineterminator="\n")
            w.writeheader()
            for name, typ, desc, dic, unit, orig, obs in cols:
                w.writerow(
                    {
                        "name": name,
                        "bigquery_type": typ,
                        "description": desc,
                        "temporal_coverage": (
                            YEAR_COVERAGE.get(table, "")
                            if name == "year"
                            else ""
                        ),
                        "covered_by_dictionary": dic,
                        "directory_column": "",
                        "measurement_unit": unit,
                        "has_sensitive_data": "no",
                        "observations": obs,
                        "original_name": orig,
                    }
                )
        print(f"wrote {path.relative_to(HERE)}  ({len(cols)} cols)")


if __name__ == "__main__":
    main()
