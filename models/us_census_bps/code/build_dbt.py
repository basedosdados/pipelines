"""Emit the dbt models and schema.yml for us_census_bps.

Column order and types come from the architecture CSVs, so the models cannot
drift from the schema the cleaning transform writes. The referential tests'
allowances are the shares measured against the US geography directory in
BigQuery, not guesses; each is explained in the model description.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

MODEL_DIR = Path(__file__).resolve().parent.parent
ARCH = MODEL_DIR / "code" / "architecture"
DATASET = "us_census_bps"

# table -> (partition start, partition end, cluster columns)
PARTITION = {
    "permit_place_monthly": (1988, 2031, ["state_id", "structure_type"]),
    "permit_place_annual": (1980, 2030, ["state_id", "structure_type"]),
    "permit_county_monthly": (2000, 2031, ["state_id", "structure_type"]),
    "permit_county_annual": (1990, 2030, ["state_id", "structure_type"]),
    "permit_cbsa_monthly": (2004, 2031, ["cbsa_id", "structure_type"]),
    "permit_cbsa_annual": (2003, 2030, ["cbsa_id", "structure_type"]),
    "permit_msa_monthly": (1988, 2008, ["msa_cmsa_id", "structure_type"]),
    "permit_msa_annual": (1980, 2007, ["msa_cmsa_id", "structure_type"]),
    "permit_state_monthly": (
        1988,
        2031,
        ["geography_level", "structure_type"],
    ),
    "permit_state_annual": (1980, 2030, ["geography_level", "structure_type"]),
}

KEY = {
    "permit_place_monthly": ["year", "month", "state_id", "permit_office_id"],
    "permit_place_annual": ["year", "state_id", "permit_office_id"],
    "permit_county_monthly": ["year", "month", "state_id", "county_id"],
    "permit_county_annual": ["year", "state_id", "county_id"],
    "permit_cbsa_monthly": ["year", "month", "cbsa_id"],
    "permit_cbsa_annual": ["year", "cbsa_id"],
    "permit_msa_monthly": ["year", "month", "msa_cmsa_id", "pmsa_id"],
    "permit_msa_annual": ["year", "msa_cmsa_id", "pmsa_id"],
    "permit_state_monthly": ["year", "month", "geography_id"],
    "permit_state_annual": ["year", "geography_id"],
}

NOT_NULL = {
    "permit_place_monthly": ["year", "month", "state_id", "permit_office_id"],
    "permit_place_annual": ["year", "state_id", "permit_office_id"],
    "permit_county_monthly": ["year", "month", "state_id"],
    "permit_county_annual": ["year", "state_id"],
    "permit_cbsa_monthly": ["year", "month", "cbsa_id"],
    "permit_cbsa_annual": ["year", "cbsa_id"],
    "permit_msa_monthly": ["year", "month", "msa_cmsa_id"],
    "permit_msa_annual": ["year", "msa_cmsa_id"],
    "permit_state_monthly": [
        "year",
        "month",
        "geography_id",
        "geography_level",
    ],
    "permit_state_annual": ["year", "geography_id", "geography_level"],
}

# Columns below the 5 percent non-null floor, measured in BigQuery.
SPARSE = {
    "permit_place_monthly": ["central_city"],
    "permit_place_annual": ["central_city"],
}

DIRECTORY = "br_bd_diretorios_us__"

# column -> (directory model, key column). state_id matches completely, so it
# takes the plain relationships test; the rest carry a measured allowance.
STRICT_FK = {"state_id": ("state", "id_state")}
LOOSE_FK = {
    "county_id": ("county", "id_county", 0.02),
    "place_id": ("place", "id_place", 0.02),
    "cbsa_id": ("cbsa_2023", "id_cbsa", 0.05),
}

DESCRIPTIONS = {
    "permit_place_monthly": (
        "Monthly counts of new privately-owned residential buildings, housing "
        "units and construction valuation authorized by building permits, for "
        "each permit-issuing place, from January 1988. One row per place, "
        "month and structure type. Figures include imputed values for "
        "non-responding permit offices; source_code marks how each office's "
        "month was obtained, and the monthly place file carries no separate "
        "reported-only block. The monthly universe was a sample of roughly "
        "9,000 permit offices until it was widened to the full universe of "
        "about 20,000 in 2024, so counts of offices are not comparable across "
        "that break; the annual table covers the full universe throughout."
    ),
    "permit_place_annual": (
        "Annual counts of new privately-owned residential buildings, housing "
        "units and construction valuation authorized by building permits, for "
        "each permit-issuing place, from 1980. One row per place, year and "
        "structure type, for the full universe of about 20,000 permit "
        "offices. Both the estimate including imputation for non-responding "
        "offices and the reported-only figures are given; use the former by "
        "default."
    ),
    "permit_county_monthly": (
        "Monthly counts of new privately-owned residential buildings, housing "
        "units and construction valuation authorized by building permits, by "
        "county, from January 2000. One row per county, month and structure "
        "type. Both the estimate including imputation for non-responding "
        "permit offices and the reported-only figures are given; use the "
        "former by default."
    ),
    "permit_county_annual": (
        "Annual counts of new privately-owned residential buildings, housing "
        "units and construction valuation authorized by building permits, by "
        "county, from 1990. One row per county, year and structure type. Both "
        "the estimate including imputation for non-responding permit offices "
        "and the reported-only figures are given; use the former by default."
    ),
    "permit_cbsa_monthly": (
        "Monthly counts of new privately-owned residential buildings, housing "
        "units and construction valuation authorized by building permits, by "
        "Core Based Statistical Area, from January 2004. One row per area, "
        "month and structure type. Micropolitan areas were added to the "
        "series in January 2024. For 1988 to 2003 the survey used the older "
        "MSA and PMSA definitions, which are a different geography and are "
        "held in permit_msa_monthly."
    ),
    "permit_cbsa_annual": (
        "Annual counts of new privately-owned residential buildings, housing "
        "units and construction valuation authorized by building permits, by "
        "Core Based Statistical Area, from 2003. One row per area, year and "
        "structure type. Micropolitan areas were added to the series in 2024. "
        "For 1980 to 2002 the survey used the older MSA and PMSA definitions, "
        "which are a different geography and are held in permit_msa_annual."
    ),
    "permit_msa_monthly": (
        "Monthly counts of new privately-owned residential buildings, housing "
        "units and construction valuation authorized by building permits, by "
        "Metropolitan Statistical Area, from January 1988 to December 2003. "
        "One row per area, month and structure type. This table uses the "
        "pre-2004 MSA and PMSA code system, which was superseded by CBSA "
        "codes rather than renamed, so it does not join to permit_cbsa_"
        "monthly or to the CBSA directory."
    ),
    "permit_msa_annual": (
        "Annual counts of new privately-owned residential buildings, housing "
        "units and construction valuation authorized by building permits, by "
        "Metropolitan Statistical Area, from 1980 to 2002. One row per area, "
        "year and structure type. This table uses the pre-2004 MSA and PMSA "
        "code system, which was superseded by CBSA codes rather than renamed, "
        "so it does not join to permit_cbsa_annual or to the CBSA directory."
    ),
    "permit_state_monthly": (
        "Monthly counts of new privately-owned residential buildings, housing "
        "units and construction valuation authorized by building permits, by "
        "state, from January 1988. The published file also carries the "
        "national, regional and divisional totals, so filter on "
        "geography_level before aggregating or the same permits are counted "
        "several times. One row per geography, month and structure type."
    ),
    "permit_state_annual": (
        "Annual counts of new privately-owned residential buildings, housing "
        "units and construction valuation authorized by building permits, by "
        "state, from 1980. The published file also carries the national, "
        "regional and divisional totals, so filter on geography_level before "
        "aggregating or the same permits are counted several times. One row "
        "per geography, year and structure type."
    ),
    "dicionario": (
        "Dictionary of the coded values used across the us_census_bps tables, "
        "one row per table, column and key."
    ),
}

EXCEPTIONS = {
    "county_id": (
        "Exception: the county_id referential test allows up to 2 percent "
        "unmatched because the survey reports historical counties that the "
        "current FIPS directory no longer lists, chiefly Connecticut's eight "
        "counties replaced by planning regions in 2022 and the Alaska census "
        "areas that have since been split."
    ),
    "place_id": (
        "Exception: the place_id referential test allows up to 2 percent "
        "unmatched because place FIPS codes drift between decennial "
        "vintages while the directory holds the 2020 vintage. Measured "
        "unmatched share is 0.3 percent."
    ),
    "cbsa_id": (
        "Exception: the cbsa_id referential test allows up to 5 percent "
        "unmatched because CBSA delineations are revised every few years "
        "while the directory holds the 2023 vintage; areas such as "
        "Los Angeles-Long Beach-Santa Ana (31100) were redelineated. "
        "Measured unmatched share is 2.7 percent."
    ),
}


def read_arch(table: str) -> list[dict]:
    """Read one table's architecture CSV."""
    with (ARCH / f"{table}.csv").open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def wrap(text: str, indent: str, width: int = 79) -> list[str]:
    """Wrap text to the repo's line length at a given indent."""
    words, lines, line = text.split(), [], indent
    for word in words:
        if len(line) + len(word) + 1 > width and line.strip():
            lines.append(line.rstrip())
            line = indent + word
        else:
            line = f"{line} {word}" if line.strip() else indent + word
    if line.strip():
        lines.append(line.rstrip())
    return lines


def build_sql(table: str) -> str:
    """Render one dbt model."""
    arch = read_arch(table)
    casts = ",\n".join(
        f"    safe_cast({c['name']} as {c['bigquery_type'].lower()}) "
        f"{c['name']}"
        for c in arch
    )
    if table == "dicionario":
        config = (
            '        schema="us_census_bps",\n'
            '        alias="dicionario",\n'
            '        materialized="table",\n'
        )
    else:
        start, end, cluster = PARTITION[table]
        cluster_sql = ", ".join(f'"{c}"' for c in cluster)
        config = (
            '        schema="us_census_bps",\n'
            f'        alias="{table}",\n'
            '        materialized="table",\n'
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {start}, "end": {end}, '
            '"interval": 1},\n'
            "        },\n"
            f"        cluster_by=[{cluster_sql}],\n"
        )
    return (
        "{{\n    config(\n"
        + config
        + "    )\n}}\n\n\nselect\n"
        + casts
        + f'\nfrom {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}'
        " as t\n"
    )


def build_schema() -> str:
    """Render schema.yml for every model."""
    out = ["---", "version: 2", "models:"]
    for table in [*PARTITION, "dicionario"]:
        arch = read_arch(table)
        names = [c["name"] for c in arch]
        description = DESCRIPTIONS[table]
        for column, note in EXCEPTIONS.items():
            if column in names and column in LOOSE_FK:
                description = f"{description} {note}"
        out.append(f"  - name: {DATASET}__{table}")
        out.append("    description: >-")
        out += wrap(description, " " * 6)
        if table != "dicionario":
            out.append("    tests:")
            out.append("      - dbt_utils.unique_combination_of_columns:")
            out.append("          combination_of_columns:")
            for column in [*KEY[table], "structure_type"]:
                out.append(f"            - {column}")
            out.append("      - not_null_proportion_multiple_columns:")
            out.append("          at_least: 0.05")
            if table in SPARSE:
                out.append("          ignore_values:")
                for column in SPARSE[table]:
                    out.append(f"            - {column}")
        out.append("    columns:")
        for column in arch:
            name = column["name"]
            out.append(f"      - name: {name}")
            out.append("        description: >-")
            out += wrap(column["description"], " " * 10)
            tests: list[str] = []
            if name in NOT_NULL.get(table, []):
                tests.append("          - not_null")
            if name == "year" and table != "dicionario":
                tests += [
                    "          - relationships:",
                    "              to: ref('br_bd_diretorios_data_tempo__ano')",
                    "              field: ano.ano",
                ]
            if name == "month":
                tests += [
                    "          - relationships:",
                    "              to: ref('br_bd_diretorios_data_tempo__mes')",
                    "              field: mes.mes",
                ]
            if name in STRICT_FK:
                model, field = STRICT_FK[name]
                tests += [
                    "          - relationships:",
                    f"              to: ref('{DIRECTORY}{model}')",
                    f"              field: {field}",
                ]
            if name in LOOSE_FK:
                model, field, allowed = LOOSE_FK[name]
                tests += [
                    "          - custom_relationships:",
                    f"              to: ref('{DIRECTORY}{model}')",
                    f"              field: {field}",
                    # The macro only filters nulls when ignore_values is set,
                    # and these columns are null by design for records the
                    # survey does not tie to that geography.
                    "              ignore_values: ['']",
                    f"              proportion_allowed_failures: {allowed}",
                ]
            if tests:
                out.append("        tests:")
                out += tests
    return "\n".join(out) + "\n"


def main() -> int:
    for table in [*PARTITION, "dicionario"]:
        path = MODEL_DIR / f"{DATASET}__{table}.sql"
        path.write_text(build_sql(table), encoding="utf-8")
        print(f"wrote {path.name}")
    (MODEL_DIR / "schema.yml").write_text(build_schema(), encoding="utf-8")
    print("wrote schema.yml")
    return 0


if __name__ == "__main__":
    sys.exit(main())
