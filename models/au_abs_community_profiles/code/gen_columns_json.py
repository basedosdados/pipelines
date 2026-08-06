"""Generate trilingual columns_json per table for au_abs_community_profiles,
for mcp__databasis__bulk_upsert_columns(columns_json=...).

Column NAMES + types + flags come from the architecture CSVs; trilingual
descriptions are supplied here (the architecture `description` is PT-only).
Emits one JSON file per table under ./columns_json/.

Run:  python gen_columns_json.py
"""

import csv
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ARCH = os.path.join(HERE, "architecture")
OUT = os.path.join(HERE, "columns_json")

# column name -> (pt, en, es)
TRANS = {
    "census_year": (
        "Ano do Censo (2011, 2016 ou 2021)",
        "Census year (2011, 2016 or 2021)",
        "Año del Censo (2011, 2016 o 2021)",
    ),
    "profile": (
        "Perfil do Censo de origem (GCP = General Community Profile; TSP = Time Series Profile)",
        "Source Census profile (GCP = General Community Profile; TSP = Time Series Profile)",
        "Perfil del Censo de origen (GCP = General Community Profile; TSP = Time Series Profile)",
    ),
    "table_code": (
        "Código da tabela do perfil (por exemplo G01, B01, T01)",
        "Profile table code (e.g. G01, B01, T01)",
        "Código de la tabla del perfil (por ejemplo G01, B01, T01)",
    ),
    "cell_code": (
        "Código da célula (variável) do perfil; ver auxiliary_info",
        "Profile cell (variable) code; see auxiliary_info",
        "Código de la celda (variable) del perfil; ver auxiliary_info",
    ),
    "value": (
        "Valor da célula; a unidade varia por célula (contagem, mediana ou média — ver auxiliary_info)",
        "Cell value; the unit varies by cell (count, median or average — see auxiliary_info)",
        "Valor de la celda; la unidad varía por celda (conteo, mediana o media — ver auxiliary_info)",
    ),
    "table_name": (
        "Nome da tabela do perfil",
        "Profile table name",
        "Nombre de la tabla del perfil",
    ),
    "table_population": (
        "População da tabela (Persons, Families ou Dwellings)",
        "Table population (Persons, Families or Dwellings)",
        "Población de la tabla (Persons, Families o Dwellings)",
    ),
    "long_description": (
        "Descrição longa da célula fornecida pela ABS",
        "Long cell description provided by the ABS",
        "Descripción larga de la celda provista por la ABS",
    ),
    "heading": (
        "Rótulo da coluna da célula no perfil",
        "Cell column heading in the profile",
        "Etiqueta de la columna de la celda en el perfil",
    ),
    "datapack_part": (
        "Arquivo CSV do DataPack em que a célula aparece",
        "DataPack CSV file where the cell appears",
        "Archivo CSV del DataPack donde aparece la celda",
    ),
    "statistic_type": (
        "Tipo de estatística da célula (count, median ou average)",
        "Cell statistic type (count, median or average)",
        "Tipo de estadística de la celda (count, median o average)",
    ),
    "measurement_unit": (
        "Unidade de medida da célula (por exemplo persons, dwellings, $/weekly)",
        "Cell measurement unit (e.g. persons, dwellings, $/weekly)",
        "Unidad de medida de la celda (por ejemplo persons, dwellings, $/weekly)",
    ),
}

# id_<level> -> (pt, en, es) unit label
LEVEL_LABEL = {
    "id_state": (
        "estado ou território",
        "state or territory",
        "estado o territorio",
    ),
    "id_sa1": ("Statistical Area Level 1 (SA1)",) * 3,
    "id_sa2": ("Statistical Area Level 2 (SA2)",) * 3,
    "id_sa3": ("Statistical Area Level 3 (SA3)",) * 3,
    "id_sa4": ("Statistical Area Level 4 (SA4)",) * 3,
    "id_gccsa": ("Greater Capital City Statistical Area (GCCSA)",) * 3,
    "id_lga": (
        "área de governo local (LGA)",
        "local government area (LGA)",
        "área de gobierno local (LGA)",
    ),
    "id_suburb": (
        "subúrbio ou localidade",
        "suburb or locality",
        "suburbio o localidad",
    ),
    "id_postal_area": (
        "área postal (POA)",
        "postal area (POA)",
        "área postal (POA)",
    ),
    "id_commonwealth_electoral_division": (
        "divisão eleitoral federal (CED)",
        "Commonwealth electoral division (CED)",
        "división electoral federal (CED)",
    ),
    "id_state_electoral_division": (
        "divisão eleitoral estadual (SED)",
        "state electoral division (SED)",
        "división electoral estatal (SED)",
    ),
}


def descs(name):
    if name in TRANS:
        return TRANS[name]
    if name in LEVEL_LABEL:
        pt, en, es = LEVEL_LABEL[name]
        return (
            f"Código identificador da unidade geográfica ({pt})",
            f"Geographic unit identifier code ({en})",
            f"Código identificador de la unidad geográfica ({es})",
        )
    raise ValueError(f"no trilingual description for column {name!r}")


def build(table):
    with open(os.path.join(ARCH, f"{table}.csv")) as f:
        rows = list(csv.DictReader(f))
    cols = []
    for r in rows:
        pt, en, es = descs(r["name"])
        c = {
            "name": r["name"],
            "bigquery_type": r["bigquery_type"],
            "description_pt": pt,
            "description_en": en,
            "description_es": es,
            "covered_by_dictionary": r["covered_by_dictionary"].strip().lower()
            == "yes",
            "has_sensitive_data": r["has_sensitive_data"].strip().lower()
            == "yes",
        }
        if r["measurement_unit"].strip():
            c["measurement_unit"] = r["measurement_unit"].strip()
        cols.append(c)
    return cols


def main():
    os.makedirs(OUT, exist_ok=True)
    tables = sorted(f[:-4] for f in os.listdir(ARCH) if f.endswith(".csv"))
    for t in tables:
        with open(os.path.join(OUT, f"{t}.json"), "w") as f:
            json.dump(build(t), f, ensure_ascii=False, indent=2)
    print(f"wrote {len(tables)} columns_json files to {OUT}")


if __name__ == "__main__":
    main()
