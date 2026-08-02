"""Generate dbt models (.sql) + schema.yml for br_bd_diretorios_au from the
architecture CSVs. One model per table; every column safe_cast; parent id_*
columns get `relationships` tests to their own directory table; the PK gets a
[diretorio]-tagged uniqueness test.

Run:  python gen_dbt.py
"""

import csv
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ARCH = os.path.join(HERE, "architecture")
MODELS = os.path.dirname(HERE)  # models/br_bd_diretorios_au
DATASET = "br_bd_diretorios_au"

FLOAT_COLS = {"area_albers_sqkm", "ratio"}

TABLES = [
    "state",
    *[
        f"{u}_{y}"
        for y in ("2021", "2016")
        for u in (
            "sa1",
            "sa2",
            "sa3",
            "sa4",
            "gccsa",
            "lga",
            "postal_area",
            "suburb",
            "commonwealth_electoral_division",
            "state_electoral_division",
        )
    ],
    "correspondence_sa2_2016_2021",
    "correspondence_lga_2016_2021",
]

# unit -> PT label for the table description
UNIT_DESC = {
    "state": "de estados e territórios",
    "sa1": "das Statistical Areas Level 1 (SA1)",
    "sa2": "das Statistical Areas Level 2 (SA2)",
    "sa3": "das Statistical Areas Level 3 (SA3)",
    "sa4": "das Statistical Areas Level 4 (SA4)",
    "gccsa": "das Greater Capital City Statistical Areas (GCCSA)",
    "lga": "das Local Government Areas (LGA)",
    "postal_area": "das áreas postais (POA)",
    "suburb": "dos subúrbios e localidades",
    "commonwealth_electoral_division": "das divisões eleitorais federais (CED)",
    "state_electoral_division": "das divisões eleitorais estaduais (SED)",
}


def read_arch(table):
    with open(os.path.join(ARCH, f"{table}.csv")) as f:
        return list(csv.DictReader(f))


def parse(table):
    """Return (unit, year) for a table name."""
    if table == "state":
        return "state", None
    if table.startswith("correspondence_"):
        return "correspondence", None
    for y in ("2021", "2016"):
        if table.endswith("_" + y):
            return table[: -(len(y) + 1)], y
    raise ValueError(table)


def parent_table(colname, year):
    """Directory table a parent id_* column references, or None."""
    if colname == "id_state":
        return "state"
    for lvl in ("sa1", "sa2", "sa3", "sa4", "gccsa"):
        if colname == f"id_{lvl}":
            return f"{lvl}_{year}"
    return None


def corr_refs(table):
    """relationships targets for correspondence tables: {col: (table, pk_field)}."""
    unit = "sa2" if "sa2" in table else "lga"
    pk = f"id_{unit}"
    return {
        f"id_{unit}_2016": (f"{unit}_2016", pk),
        f"id_{unit}_2021": (f"{unit}_2021", pk),
    }


def gen_sql(table):
    rows = read_arch(table)
    lines = [
        "{{",
        "    config(",
        f'        alias="{table}",',
        f'        schema="{DATASET}",',
        '        materialized="table",',
        "    )",
        "}}",
        "select",
    ]
    casts = []
    for r in rows:
        c = r["name"]
        t = "float64" if c in FLOAT_COLS else "string"
        casts.append(f"    safe_cast({c} as {t}) {c}")
    lines.append(",\n".join(casts))
    lines.append(
        f'from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t'
    )
    return "\n".join(lines) + "\n"


def table_description(table):
    unit, year = parse(table)
    if unit == "correspondence":
        u = "SA2" if "sa2" in table else "LGA"
        return (
            f"Correspondência (crosswalk) entre as {u} do ASGS de 2016 e 2021, "
            f"com a razão de aporcionamento populacional (RATIO_FROM_TO) e o "
            f"indicador de qualidade da Australian Bureau of Statistics."
        )
    edition = f" na edição de {year} do ASGS" if year else ""
    return (
        f"Diretório {UNIT_DESC[unit]} da Austrália{edition}, "
        f"com código, nome, hierarquia superior e área."
    )


def gen_schema_model(table):
    rows = read_arch(table)
    unit, year = parse(table)
    pk_cols = (
        [rows[0]["name"], rows[2]["name"]]
        if unit == "correspondence"
        else [rows[0]["name"]]
    )

    out = [
        f"  - name: {DATASET}__{table}",
        "    description: >",
        f"      {table_description(table)}",
        "    tests:",
        "      - dbt_utils.unique_combination_of_columns:",
        "          tags: [diretorio]",
        f"          combination_of_columns: {pk_cols}",
        "    columns:",
    ]

    refs = corr_refs(table) if unit == "correspondence" else {}
    for r in rows:
        c = r["name"]
        out.append(f"      - name: {c}")
        out.append(f"        description: {r['description']}")
        tests = []
        if c in pk_cols:
            tests.append("not_null")
        # relationships
        if unit == "correspondence":
            ref = refs.get(c)
            ref_tbl, ref_fld = ref if ref else (None, None)
        elif c.startswith("id_") and c not in pk_cols:
            ref_tbl = parent_table(c, year)
            ref_fld = ref_field(c, ref_tbl) if ref_tbl else None
        else:
            ref_tbl = ref_fld = None
        if ref_tbl:
            out.append("        tests:")
            for t in tests:
                out.append(f"          - {t}")
            out.append("          - relationships:")
            out.append(f"              to: ref('{DATASET}__{ref_tbl}')")
            out.append(f"              field: {ref_fld}")
        elif tests:
            out.append("        tests: [" + ", ".join(tests) + "]")
    return "\n".join(out)


def ref_field(colname, ref_tbl):
    """The PK column name in the referenced directory table."""
    if ref_tbl == "state":
        return "id_state"
    return colname  # id_sa3 -> sa3 table's PK is id_sa3


def main():
    # SQL models
    for t in TABLES:
        with open(os.path.join(MODELS, f"{DATASET}__{t}.sql"), "w") as f:
            f.write(gen_sql(t))
    # schema.yml
    parts = ["version: 2", "", "models:"]
    for t in TABLES:
        parts.append(gen_schema_model(t))
    with open(os.path.join(MODELS, "schema.yml"), "w") as f:
        f.write("---\n" + "\n".join(parts) + "\n")
    print(f"wrote {len(TABLES)} models + schema.yml to {MODELS}")


if __name__ == "__main__":
    main()
