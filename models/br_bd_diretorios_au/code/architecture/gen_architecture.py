"""Generate architecture CSVs for br_bd_diretorios_au (23 tables).

Source of truth for column order, types, PT descriptions, and directory FKs.
Emits one CSV per table under this directory. Descriptions follow the Data Basis
style manual: capitalized first letter, NO trailing period on column descriptions.

Design decisions (user-approved 2026-07-31):
- Directory dataset -> directory_column is BLANK on every column (directory tables
  never fill directoryPrimaryKey; cross-table integrity is enforced by dbt
  relationships tests, not backend FKs).
- Drop AUS_CODE/AUS_NAME (constant), ASGS_LOCI_URI, CHANGE_FLAG/CHANGE_LABEL.
- Add derived `abbreviation` (state) and `abbreviation_state` (children).
- Keep area_albers_sqkm (FLOAT64, km2) on every directory table.
- All codes STRING. Aggregate structures: one row/unit. Non-ABS (lga/poa/sal/ced/sed):
  mesh-block source deduped to one row/unit, area summed.
"""

import csv
import os

HERE = os.path.dirname(os.path.abspath(__file__))

# Architecture sheet header (Data Basis standard order)
HEADER = [
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

# ---- reusable column builders ----------------------------------------------


def col(name, btype, desc, unit="", obs="", original=""):
    return {
        "name": name,
        "bigquery_type": btype,
        "description": desc,
        "temporal_coverage": "",
        "covered_by_dictionary": "no",
        "directory_column": "",
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations": obs,
        "original_name": original,
    }


def area_col():
    return col(
        "area_albers_sqkm",
        "FLOAT64",
        "Área em quilômetros quadrados, calculada na projeção Albers de áreas iguais",
        unit="km2",
        original="AREA_ALBERS_SQKM",
    )


# state tail carried by child tables (order matters)
def state_tail(year):
    y = year
    return [
        col(
            "id_state",
            "STRING",
            "Código do estado ou território australiano (1 a 9)",
            original=f"STATE_CODE_{y}",
        ),
        col(
            "abbreviation_state",
            "STRING",
            "Sigla do estado ou território australiano",
            obs="Derivada do código do estado",
            original="(derivada)",
        ),
        col(
            "name_state",
            "STRING",
            "Nome do estado ou território australiano",
            original=f"STATE_NAME_{y}",
        ),
    ]


# unit metadata: level abbrev -> (pt long name, code digits text)
UNIT_PT = {
    "sa1": "Statistical Area Level 1 (SA1)",
    "sa2": "Statistical Area Level 2 (SA2)",
    "sa3": "Statistical Area Level 3 (SA3)",
    "sa4": "Statistical Area Level 4 (SA4)",
    "gccsa": "Greater Capital City Statistical Area (GCCSA)",
    "lga": "Local Government Area (LGA)",
    "postal_area": "área postal (POA)",
    "suburb": "subúrbio ou localidade (SAL)",
    "commonwealth_electoral_division": "divisão eleitoral federal (CED)",
    "state_electoral_division": "divisão eleitoral estadual (SED)",
}
CODE_DIGITS = {
    "sa1": ", 11 dígitos",
    "sa2": ", 9 dígitos",
    "sa3": ", 5 dígitos",
    "sa4": ", 3 dígitos",
    "gccsa": "",
    "lga": ", 5 dígitos",
    "postal_area": ", 4 dígitos",
    "suburb": "",
    "commonwealth_electoral_division": "",
    "state_electoral_division": "",
}
# Portuguese article by unit gender (subúrbio is masculine -> "do")
ART = {u: ("do" if u == "suburb" else "da") for u in UNIT_PT}


# original source code/name column stems per edition
def src_code(u, y):
    if u == "sa2" and y in ("2016", "2011"):
        return f"SA2_MAINCODE_{y}"
    if u == "sa1" and y in ("2016", "2011"):
        return f"SA1_MAINCODE_{y}"
    if u == "suburb" and y in ("2016", "2011"):
        return f"SSC_CODE_{y}"  # 2016/2011 suburbs are SSC (State Suburbs), renamed SAL in 2021
    return {
        "sa1": f"SA1_CODE_{y}",
        "sa2": f"SA2_CODE_{y}",
        "sa3": f"SA3_CODE_{y}",
        "sa4": f"SA4_CODE_{y}",
        "gccsa": f"GCCSA_CODE_{y}",
        "lga": f"LGA_CODE_{y}",
        "postal_area": f"POA_CODE_{y}",
        "suburb": f"SAL_CODE_{y}",
        "commonwealth_electoral_division": f"CED_CODE_{y}",
        "state_electoral_division": f"SED_CODE_{y}",
    }[u]


def src_name(u, y):
    if u == "suburb" and y in ("2016", "2011"):
        return f"SSC_NAME_{y}"
    return {
        "sa1": None,
        "sa2": f"SA2_NAME_{y}",
        "sa3": f"SA3_NAME_{y}",
        "sa4": f"SA4_NAME_{y}",
        "gccsa": f"GCCSA_NAME_{y}",
        "lga": f"LGA_NAME_{y}",
        "postal_area": f"POA_NAME_{y}",
        "suburb": f"SAL_NAME_{y}",
        "commonwealth_electoral_division": f"CED_NAME_{y}",
        "state_electoral_division": f"SED_NAME_{y}",
    }[u]


def id_col(u, y):
    return col(
        f"id_{u}",
        "STRING",
        f"Código {ART[u]} {UNIT_PT[u]}{CODE_DIGITS[u]}",
        original=src_code(u, y),
    )


def name_col_own(u, y):
    return col(
        "name",
        "STRING",
        f"Nome {ART[u]} {UNIT_PT[u]}",
        original=src_name(u, y),
    )


def parent_cols(u, y):
    """parent id+name pairs above unit u (excluding state tail)."""
    order = {
        "sa1": ["sa2", "sa3", "sa4", "gccsa"],
        "sa2": ["sa3", "sa4", "gccsa"],
        "sa3": ["sa4", "gccsa"],
        "sa4": ["gccsa"],
        "gccsa": [],
        "lga": [],
        "postal_area": [],
        "suburb": [],
        "commonwealth_electoral_division": [],
        "state_electoral_division": [],
    }[u]
    out = []
    for p in order:
        out.append(
            col(
                f"id_{p}",
                "STRING",
                f"Código {ART[p]} {UNIT_PT[p]}",
                original=src_code(p, y),
            )
        )
        out.append(
            col(
                f"name_{p}",
                "STRING",
                f"Nome {ART[p]} {UNIT_PT[p]}",
                original=src_name(p, y),
            )
        )
    return out


# tables that have NO state parent (postal_area crosses state borders)
NO_STATE = {"postal_area"}


def build_unit_table(u, year):
    cols = [id_col(u, year)]
    if u != "sa1":  # SA1 has no name
        cols.append(name_col_own(u, year))
    cols += parent_cols(u, year)
    if u not in NO_STATE:
        cols += state_tail(year)
    # legacy short codes in 2016 and 2011 SA1/SA2 (dropped in 2021)
    if year == "2016" and u == "sa2":
        cols.insert(
            1,
            col(
                "id_sa2_short",
                "STRING",
                "Código curto de 5 dígitos da SA2 na edição de 2016 (removido na edição de 2021)",
                obs="Confirmar presença na fonte 2016",
                original="SA2_5DIGITCODE_2016",
            ),
        )
    if year == "2016" and u == "sa1":
        cols.insert(
            1,
            col(
                "id_sa1_short",
                "STRING",
                "Código curto de 7 dígitos da SA1 na edição de 2016 (removido na edição de 2021)",
                obs="Confirmar presença na fonte 2016",
                original="SA1_7DIGITCODE_2016",
            ),
        )
    if year == "2011" and u == "sa2":
        cols.insert(
            1,
            col(
                "id_sa2_short",
                "STRING",
                "Código curto de 5 dígitos da SA2 na edição de 2011 (removido na edição de 2021)",
                original="SA2_5DIGITCODE_2011",
            ),
        )
    if year == "2011" and u == "sa1":
        cols.insert(
            1,
            col(
                "id_sa1_short",
                "STRING",
                "Código curto de 7 dígitos da SA1 na edição de 2011 (removido na edição de 2021)",
                original="SA1_7DIGITCODE_2011",
            ),
        )
    cols.append(area_col())
    return cols


def build_state():
    return [
        col(
            "id_state",
            "STRING",
            "Código do estado ou território australiano (1 a 9)",
            original="STATE_CODE_2021",
        ),
        col(
            "abbreviation",
            "STRING",
            "Sigla do estado ou território australiano (NSW, VIC, QLD, SA, WA, TAS, NT, ACT, OT)",
            obs="Derivada do código do estado",
            original="(derivada)",
        ),
        col(
            "name",
            "STRING",
            "Nome do estado ou território australiano",
            original="STATE_NAME_2021",
        ),
        area_col(),
    ]


def build_corr(u):
    up = {"sa2": "SA2", "lga": "LGA"}[u]
    a = ART[u]
    return [
        col(
            f"id_{u}_2016",
            "STRING",
            f"Código {a} {UNIT_PT[u]} na edição de 2016",
            original=f"{up}_{'MAINCODE' if u == 'sa2' else 'CODE'}_2016",
        ),
        col(
            f"name_{u}_2016",
            "STRING",
            f"Nome {a} {UNIT_PT[u]} na edição de 2016",
            original=f"{up}_NAME_2016",
        ),
        col(
            f"id_{u}_2021",
            "STRING",
            f"Código {a} {UNIT_PT[u]} na edição de 2021",
            original=f"{up}_CODE_2021",
        ),
        col(
            f"name_{u}_2021",
            "STRING",
            f"Nome {a} {UNIT_PT[u]} na edição de 2021",
            original=f"{up}_NAME_2021",
        ),
        col(
            "ratio",
            "FLOAT64",
            "Proporção da população da região de origem (2016) alocada à região de destino (2021), de 0 a 1",
            obs="Peso de apporcionamento adimensional; some 1 por região de origem",
            original="RATIO_FROM_TO",
        ),
        col(
            "quality_indicator",
            "STRING",
            "Indicador geral de qualidade da correspondência (Good, Acceptable, Poor)",
            original="OVERALL_QUALITY_INDICATOR",
        ),
    ]


# ---- table registry ---------------------------------------------------------

UNITS = [
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
]


def all_tables():
    tables = {"state": build_state()}
    for y in ("2021", "2016", "2011"):
        for u in UNITS:
            tables[f"{u}_{y}"] = build_unit_table(u, y)
    tables["correspondence_sa2_2016_2021"] = build_corr("sa2")
    tables["correspondence_lga_2016_2021"] = build_corr("lga")
    return tables


def main():
    tables = all_tables()
    for tname, cols in tables.items():
        path = os.path.join(HERE, f"{tname}.csv")
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=HEADER, lineterminator="\n")
            w.writeheader()
            for c in cols:
                w.writerow(c)
    print(f"wrote {len(tables)} architecture CSVs to {HERE}")
    for tname, cols in tables.items():
        print(f"  {tname:42} {len(cols):2} cols: {[c['name'] for c in cols]}")


if __name__ == "__main__":
    main()
