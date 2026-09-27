"""Generate models/world_noaa_ghcn/schema.yml from the architecture CSVs."""

import csv
from pathlib import Path

DATASET = "world_noaa_ghcn"
ROOT = Path(__file__).resolve().parents[1]
ARCH = Path(__file__).parent / "architecture"

DESCRIPTIONS = {
    "station": (
        "Estações de superfície do GHCN-Daily, uma linha por estação, com "
        "coordenadas, altitude, país, estado e identificador da Organização "
        "Meteorológica Mundial."
    ),
    "station_element_inventory": (
        "Inventário de cobertura por estação e elemento, indicando o primeiro "
        "e o último ano com dados não sinalizados. Cobre os mesmos 144 "
        "elementos da tabela observation, permitindo descobrir a cobertura de "
        "uma estação sem varrer a tabela de observações."
    ),
    "observation": (
        "Observações diárias por estação e elemento, em formato longo, de 1763 "
        "em diante. Cobre os 144 elementos do GHCN-Daily, dos quais os cinco "
        "principais (TMAX, TMIN, PRCP, SNOW, SNWD) concentram 84,6% das "
        "linhas. Os valores já estão convertidos para unidades padrão, "
        "indicadas por linha em measurement_unit; em 28 elementos o valor não "
        "é uma grandeza mensurável (horários em HHMM e indicadores de "
        "ocorrência) e measurement_unit é nulo. Observações que reprovaram no "
        "controle de qualidade foram mantidas e estão marcadas em "
        "quality_flag: filtre por quality_flag IS NULL para usar apenas dados "
        "aprovados."
    ),
    "dicionario": (
        "Dicionário de códigos das colunas categóricas das demais tabelas: "
        "elementos meteorológicos, sinalizadores de medição, qualidade e "
        "fonte, e códigos de rede das estações."
    ),
}

# Columns that are legitimately sparse and must not trip the proportion test.
SPARSE = {
    "observation": ["measurement_flag", "quality_flag", "observation_time"],
    "station": [
        "state_code",
        "state_name",
        "gsn_flag",
        "hcn_crn_flag",
        "wmo_id",
        "elevation",
    ],
}

NOT_NULL = {
    "observation": ["year", "station_id", "date", "element", "value"],
    "station": ["station_id"],
    "station_element_inventory": [
        "station_id",
        "element",
        "first_year",
        "last_year",
    ],
    "dicionario": ["id_tabela", "nome_coluna", "chave", "valor"],
}

UNIQUE = {
    "observation": ["year", "station_id", "date", "element"],
    "station": ["station_id"],
    "station_element_inventory": ["station_id", "element"],
}

# Only `observation` is large enough to need scoped tests.
SCOPED = {"observation"}


def block(text: str, indent: int) -> str:
    """Wrap text to fit a YAML block scalar at a given indent.

    Args:
        text: Text to wrap.
        indent: Number of leading spaces per line.

    Returns:
        The wrapped, indented text.
    """
    pad = " " * indent
    words, lines, cur = text.split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 > 74:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    lines.append(cur)
    return "\n".join(pad + ln for ln in lines)


def main() -> None:
    """Generate ``schema.yml`` from the architecture CSVs."""
    out = ["---", "version: 2", "models:"]
    for table in (
        "dicionario",
        "observation",
        "station",
        "station_element_inventory",
    ):
        with open(ARCH / f"{table}.csv", encoding="utf-8") as fh:
            cols = list(csv.DictReader(fh))
        scoped = table in SCOPED
        out.append(f"  - name: {DATASET}__{table}")
        out.append("    description: >")
        out.append(block(DESCRIPTIONS[table], 6))
        out.append("    tests:")
        if table in UNIQUE:
            out.append("      - dbt_utils.unique_combination_of_columns:")
            out.append(
                f"          combination_of_columns: [{', '.join(UNIQUE[table])}]"
            )
            if scoped:
                out.append("          config:")
                out.append("            where: __most_recent_year_en__")
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        if table in SPARSE:
            out.append("          ignore_values:")
            for c in SPARSE[table]:
                out.append(f"            - {c}")
        if scoped:
            out.append("          config:")
            out.append("            where: __most_recent_year_en__")
        out.append("    columns:")
        for c in cols:
            out.append(f"      - name: {c['name']}")
            out.append("        description: >")
            out.append(block(c["description"], 10))
            tests = []
            if c["name"] in NOT_NULL.get(table, []):
                tests.append("not_null")
            if tests:
                if scoped:
                    # An unscoped not_null on a 3.19bn-row table full-scans that
                    # column on every CI run and every table-approve. Scope it to
                    # the most recent year: nullness is guaranteed at write time
                    # by clean.py and re-checked by the per-year row-count gate,
                    # and the recurring pipeline only ever rewrites recent
                    # partitions, so that is where a regression would appear.
                    # Safe here because the newest partition is a real calendar
                    # year holding 20.9M rows, not a stub -- see
                    # `reference_most_recent_year_scope_assumes_a_calendar`.
                    out.append("        tests:")
                    for t in tests:
                        out.append(f"          - {t}:")
                        out.append("              config:")
                        out.append(
                            "                where: __most_recent_year_en__"
                        )
                else:
                    out.append(f"        tests: [{', '.join(tests)}]")
    (ROOT / "schema.yml").write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"wrote {ROOT / 'schema.yml'}")


if __name__ == "__main__":
    main()
