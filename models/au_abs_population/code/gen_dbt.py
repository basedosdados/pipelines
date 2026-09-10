"""Generate the au_abs_population dbt models and schema.yml.

The architecture CSVs are the source of truth for column names, order, types
and descriptions; this script derives the SQL and the tests from them so the
three cannot drift apart. Re-run after any architecture change.

schema.yml is written with a plain YAML dump, never folded (">") scalars: a
folded scalar appends a trailing newline, which makes the description stored in
BigQuery differ from the one in the API. yamlfix reflows the file afterwards
without changing its meaning, so the workflow is regenerate, then run
pre-commit:

    python gen_dbt.py && uv run pre-commit run --files models/au_abs_population/schema.yml

Usage:
    python gen_dbt.py
"""

import csv
import os

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
ARCH = os.path.join(HERE, "architecture")
MODELS = os.path.abspath(os.path.join(HERE, ".."))
DATASET = "au_abs_population"

# table -> (partition start, partition end) or None when unpartitioned
PARTITION = {
    "national_state": (1981, 2030),
    "erp_age_sex": (1971, 2030),
    "projection": (2022, 2076),
    "regional_sa2": (2001, 2030),
    "regional_lga": (2001, 2030),
    "series": None,
}

KEYS = {
    "national_state": ["year", "quarter", "region_name", "sex", "measure"],
    "erp_age_sex": ["year", "region_name", "sex", "age"],
    "projection": ["year", "series", "region_name", "sex", "age"],
    "regional_sa2": ["year", "sa2_id"],
    "regional_lga": ["year", "lga_id"],
    "series": ["series_id"],
}

# Columns that carry a not_null test: the partition column and the identifying
# columns of each table's logical key.
NOT_NULL = {
    "national_state": [
        "year",
        "quarter",
        "geography_level",
        "region_name",
        "sex",
        "measure",
        "series_id",
    ],
    "erp_age_sex": [
        "year",
        "geography_level",
        "region_name",
        "sex",
        "age",
        "series_id",
        "erp",
    ],
    "projection": [
        "year",
        "series",
        "geography_level",
        "region_name",
        "sex",
        "age",
        "series_id",
        "projected_population",
    ],
    "regional_sa2": [
        "year",
        "sa2_id",
        "sa3_id",
        "sa4_id",
        "gccsa_id",
        "state_id",
        "erp",
    ],
    "regional_lga": ["year", "lga_id", "state_id", "erp"],
    "series": ["series_id", "description", "unit", "frequency"],
}

# Measured, not guessed: population_density is published by ABS for one year in
# the series, so it sits below the 5% floor of the proportion test.
SPARSE = {
    "regional_sa2": ["population_density"],
    "regional_lga": ["population_density"],
}

# LGA boundaries move ahead of the ASGS 2021 directory between releases.
LGA_IGNORE = ["24700", "71500", "71700"]

DESCRIPTIONS = {
    "national_state": (
        "Quarterly estimated resident population and components of population change for "
        "Australia and for each state and territory (ABS, former catalogue 3101.0), from the "
        "June quarter 1981. One row per quarter, region, sex and measure, keyed on the ABS "
        "Series ID. Measures cover the estimated resident population, births, deaths, natural "
        "increase, interstate and overseas arrivals and departures, and net interstate and net "
        "overseas migration. The unit varies by row and is given by the unit column: ABS "
        "publishes the Australia-only summary series in thousands and the state series in "
        "persons. Where a measure was published in both, the persons figure is kept, which was "
        "verified to differ from the thousands figure by at most 50 persons, exactly half of "
        "the rounding unit."
    ),
    "erp_age_sex": (
        "Estimated resident population by single year of age and sex, at 30 June, for Australia "
        "and for each state and territory (ABS, former catalogue 3101.0), from 1971. One row per "
        "year, region, sex and single year of age. This is the historical actual population; the "
        "projection table holds the projected population on the same grain."
    ),
    "projection": (
        "Population projections by single year of age and sex, at 30 June, for Australia and for "
        "each state and territory (ABS, former catalogue 3222.0). One row per projected year, "
        "series, region, sex and age, covering the three published series: high (ABS series "
        "1(A)), medium (29(B)) and low (45(C)). These are projections, not estimates: they are "
        "the arithmetic consequence of the stated fertility, mortality and migration "
        "assumptions, not a forecast. The historical actual population on the same grain is in "
        "the erp_age_sex table."
    ),
    "regional_sa2": (
        "Estimated resident population and components of population change by Statistical Area "
        "Level 2 (ABS, former catalogue 3218.0), from 2001. One row per SA2 and year, carrying "
        "the full ASGS hierarchy so that SA3, SA4, GCCSA and state totals can be recomputed by "
        "summing: every aggregate ABS publishes was verified to equal the sum of its SA2s "
        "exactly, at every level and in every year, so the pre-aggregated levels are not stored. "
        "Components of change are published for the four most recent financial years only. "
        "Exception: population_density is excluded from the non-null proportion test because ABS "
        "publishes it for a single year of the series."
    ),
    "regional_lga": (
        "Estimated resident population and components of population change by Local Government "
        "Area (ABS, former catalogue 3218.0), from 2001. One row per LGA and year. Components of "
        "change are published for the four most recent financial years only. Exceptions: three "
        "LGA codes are ignored in the directory relationship test because ABS restates the "
        "series onto boundaries newer than the ASGS 2021 LGA directory (24700 Merri-bek, renamed "
        "from Moreland in 2022; 71500 East Arnhem and 71700 Groote Archipelago, split in 2023); "
        "and population_density is excluded from the non-null proportion test because ABS "
        "publishes it for a single year of the series."
    ),
    "series": (
        "Dimension table of the ABS time series that feed this dataset, one row per ABS Series "
        "ID, covering the national and state population estimates (former catalogue 3101.0) and "
        "the population projections (3222.0). Carries the series description, unit, frequency, "
        "source table and observation span. The Series ID is stable across ABS releases and "
        "products, so it is the key for joining these tables to other ABS output."
    ),
}


def read_arch(table):
    with open(os.path.join(ARCH, f"{table}.csv"), encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def sql_for(table, cols):
    part = PARTITION[table]
    cfg = [
        f'        schema="{DATASET}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    if part:
        cfg.append(
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {part[0]}, "end": {part[1]}, "interval": 1}},\n'
            "        },"
        )
    selects = []
    for c in cols:
        name, typ = c["name"], c["bigquery_type"].lower()
        selects.append(f"    safe_cast({name} as {typ}) {name},")
    selects[-1] = selects[-1].rstrip(",")
    return (
        "{{\n    config(\n"
        + "\n".join(cfg)
        + "\n    )\n}}\n\n\nselect\n"
        + "\n".join(selects)
        + "\nfrom "
        + f'{{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t\n'
    )


def schema_for(table, cols):
    model = {"name": f"{DATASET}__{table}", "description": DESCRIPTIONS[table]}
    tests = [
        {
            "dbt_utils.unique_combination_of_columns": {
                "combination_of_columns": KEYS[table]
            }
        }
    ]
    proportion = {"at_least": 0.05}
    if table in SPARSE:
        proportion["ignore_values"] = SPARSE[table]
    tests.append({"not_null_proportion_multiple_columns": proportion})
    model["tests"] = tests

    out_cols = []
    for c in cols:
        col = {"name": c["name"], "description": c["description"]}
        ctests = []
        if c["name"] in NOT_NULL[table]:
            ctests.append("not_null")
        dirc = c["directory_column"].strip()
        if dirc:
            ds_tbl, field = dirc.split(":")
            ds, tbl = ds_tbl.split(".")
            # The time directory's model exposes `ano` as a row struct to the
            # relationships test, so the field must be qualified.
            fld = "ano.ano" if tbl == "ano" else field
            rel = {"to": f"ref('{ds}__{tbl}')", "field": fld}
            if table == "regional_lga" and c["name"] == "lga_id":
                ctests.append(
                    {
                        "custom_relationships": {
                            **rel,
                            "ignore_values": LGA_IGNORE,
                            "proportion_allowed_failures": 0,
                        }
                    }
                )
            else:
                ctests.append({"relationships": rel})
        if c["name"] == "series_id" and table != "series":
            ctests.append(
                {
                    "relationships": {
                        "to": f"ref('{DATASET}__series')",
                        "field": "series_id",
                    }
                }
            )
        if ctests:
            col["tests"] = ctests
        out_cols.append(col)
    model["columns"] = out_cols
    return model


def main():
    models = []
    for table in PARTITION:
        cols = read_arch(table)
        path = os.path.join(MODELS, f"{DATASET}__{table}.sql")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(sql_for(table, cols))
        models.append(schema_for(table, cols))
        print(f"wrote {os.path.basename(path)} ({len(cols)} columns)")

    with open(os.path.join(MODELS, "schema.yml"), "w", encoding="utf-8") as fh:
        fh.write("---\n")
        yaml.safe_dump(
            {"version": 2, "models": models},
            fh,
            sort_keys=False,
            allow_unicode=True,
            width=88,
            default_flow_style=False,
        )
    print("wrote schema.yml")


if __name__ == "__main__":
    main()
