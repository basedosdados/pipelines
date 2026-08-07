"""Generate trilingual columns_json per table for br_bd_diretorios_au, for
mcp__databasis__bulk_upsert_columns(columns_json=...).

The PT descriptions in the architecture CSVs are highly regular, so EN/ES are
derived by ordered rule-based transforms (specific strings first, then the
generic "Código da/do X" / "Nome da/do X" forms). Emits one JSON file per table
under ./columns_json/.

Run:  python gen_columns_json.py
"""

import csv
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
ARCH = os.path.join(HERE, "architecture")
OUT = os.path.join(HERE, "columns_json")

AREA_PT = "Área em quilômetros quadrados, calculada na projeção Albers de áreas iguais"
AREA_EN = (
    "Area in square kilometres, computed in the Albers equal-area projection"
)
AREA_ES = "Área en kilómetros cuadrados, calculada en la proyección Albers de áreas iguales"

# exact PT -> (EN, ES) for non-generic descriptions
EXACT = {
    "Código do estado ou território australiano (1 a 9)": (
        "State or territory code (1 to 9)",
        "Código del estado o territorio australiano (1 a 9)",
    ),
    "Sigla do estado ou território australiano (NSW, VIC, QLD, SA, WA, TAS, NT, ACT, OT)": (
        "State or territory abbreviation (NSW, VIC, QLD, SA, WA, TAS, NT, ACT, OT)",
        "Sigla del estado o territorio australiano (NSW, VIC, QLD, SA, WA, TAS, NT, ACT, OT)",
    ),
    "Sigla do estado ou território australiano": (
        "State or territory abbreviation",
        "Sigla del estado o territorio australiano",
    ),
    "Nome do estado ou território australiano": (
        "State or territory name",
        "Nombre del estado o territorio australiano",
    ),
    AREA_PT: (AREA_EN, AREA_ES),
    "Proporção da população da região de origem (2016) alocada à região de destino (2021), de 0 a 1": (
        "Share of the source region (2016) population allocated to the destination region (2021), from 0 to 1",
        "Proporción de la población de la región de origen (2016) asignada a la región de destino (2021), de 0 a 1",
    ),
    "Indicador geral de qualidade da correspondência (Good, Acceptable, Poor)": (
        "Overall quality indicator of the correspondence (Good, Acceptable, Poor)",
        "Indicador general de calidad de la correspondencia (Good, Acceptable, Poor)",
    ),
}

# regex rules (checked in order) -> (en_template, es_template) with \1 group
RULES = [
    # legacy short codes
    (
        r"^Código curto de (\d+) dígitos da (.+) na edição de (\d+) \(removido na edição de 2021\)$",
        r"\1-digit short code of the \2 in the \3 edition (removed in the 2021 edition)",
        r"Código corto de \1 dígitos de la \2 en la edición de \3 (eliminado en la edición de 2021)",
    ),
    # crosswalk code/name with edition
    (
        r"^Código d[ao] (.+) na edição de (\d+)$",
        r"\1 code in the \2 edition",
        r"Código de \1 en la edición de \2",
    ),
    (
        r"^Nome d[ao] (.+) na edição de (\d+)$",
        r"Name of the \1 in the \2 edition",
        r"Nombre de \1 en la edición de \2",
    ),
    # generic code with optional digits
    (
        r"^Código d[ao] (.+?), (\d+) dígitos$",
        r"\1 code, \2 digits",
        r"Código de \1, \2 dígitos",
    ),
    (r"^Código d[ao] (.+)$", r"\1 code", r"Código de \1"),
    # generic name
    (r"^Nome d[ao] (.+)$", r"Name of the \1", r"Nombre de \1"),
]


def translate(pt):
    if pt in EXACT:
        return EXACT[pt]
    for pat, en_t, es_t in RULES:
        m = re.match(pat, pt)
        if m:
            return re.sub(pat, en_t, pt), re.sub(pat, es_t, pt)
    raise ValueError(f"No translation rule for: {pt!r}")


def build(table):
    with open(os.path.join(ARCH, f"{table}.csv")) as f:
        rows = list(csv.DictReader(f))
    cols = []
    for r in rows:
        en, es = translate(r["description"])
        c = {
            "name": r["name"],
            "bigquery_type": r["bigquery_type"],
            "description_pt": r["description"],
            "description_en": en,
            "description_es": es,
            "covered_by_dictionary": r["covered_by_dictionary"].strip().lower()
            == "yes",
            "has_sensitive_data": r["has_sensitive_data"].strip().lower()
            == "yes",
        }
        if r["measurement_unit"].strip():
            c["measurement_unit"] = r["measurement_unit"].strip()
        if r["observations"].strip():
            c["observations"] = r["observations"].strip()
        cols.append(c)
    return cols


def main():
    os.makedirs(OUT, exist_ok=True)
    tables = sorted(f[:-4] for f in os.listdir(ARCH) if f.endswith(".csv"))
    for t in tables:
        cols = build(t)
        with open(os.path.join(OUT, f"{t}.json"), "w") as f:
            json.dump(cols, f, ensure_ascii=False, indent=2)
    print(f"wrote {len(tables)} columns_json files to {OUT}")


if __name__ == "__main__":
    main()
