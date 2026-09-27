"""Generate architecture CSVs for us_bls_oes.

One CSV per table (`area`, `industry`) plus the standard `dicionario`. The header
follows the Data Basis architecture schema (see .claude/rules/data-basis-style.md).

OEWS is a US dataset, so column names are English and descriptions are written in
English, first letter capitalized, no trailing period. Types follow the
"arithmetic meaning" rule: only genuine quantities are numeric, and every numeric
column carries a measurement_unit. Codes and flags are STRING.

Run: uv run python models/us_bls_oes/code/build_architecture.py
"""

import csv
from pathlib import Path

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


def col(
    name,
    bigquery_type,
    description,
    *,
    dictionary="no",
    directory="",
    unit="",
    observations="",
    original="",
    coverage="",
):
    """Build one architecture row as a dict keyed by HEADER."""
    return dict(
        zip(
            HEADER,
            [
                name,
                bigquery_type,
                description,
                coverage,
                dictionary,
                directory,
                unit,
                "no",
                observations,
                original,
            ],
            strict=True,
        )
    )


# ── shared blocks ───────────────────────────────────────────────────────────

YEAR = col(
    "year",
    "INT64",
    "Reference year of the estimate; OEWS estimates refer to May of this year",
    directory="br_bd_diretorios_data_tempo.ano:ano",
    unit="year",
    observations=(
        "Partition column. The 2003 and 2004 November surveys are not included "
        "so that the whole panel is a single May series"
    ),
    original="(derived from the release)",
)

OCCUPATION = [
    col(
        "occupation_id",
        "STRING",
        "Standard Occupational Classification (SOC) code, or an OEWS-specific "
        "code where OEWS publishes a combination of SOC occupations",
        observations=(
            "SOC vintage changes within the panel: SOC 2000 for 2003-2009, "
            "SOC 2010 for 2010-2018, SOC 2018 for 2019 onward. Codes from 2019 "
            "align with br_bd_diretorios_us.soc_2018:id_soc. No directory link "
            "is declared because the older vintages and the OEWS-specific codes "
            "(including the 00-0000 all-occupations total) would not resolve"
        ),
        original="occ_code",
    ),
    col(
        "occupation_group",
        "STRING",
        "SOC aggregation level of the occupation: total, major, minor, broad or "
        "detailed",
        observations=(
            "Where OEWS no longer publishes below a broad occupation, the broad "
            "row is published a second time tagged 'detailed'. Both rows are "
            "kept, so this column is part of the table key"
        ),
        original="o_group (2017 onward), group (2003-2016)",
    ),
]

OCCUPATION_NAME = col(
    "occupation_name",
    "STRING",
    "SOC title, or the OEWS-specific title where OEWS publishes a combination "
    "of SOC occupations",
    original="occ_title",
)

EMPLOYMENT = [
    col(
        "employment",
        "INT64",
        "Estimated total employment, rounded to the nearest 10 and excluding "
        "the self-employed",
        unit="worker",
        observations="Null where the source reported '**' (estimate not available)",
        original="tot_emp",
    ),
    col(
        "employment_prse",
        "FLOAT64",
        "Percent relative standard error of the employment estimate",
        unit="percent",
        original="emp_prse",
    ),
]

WAGES = [
    col(
        "hourly_wage_mean",
        "FLOAT64",
        "Mean hourly wage",
        unit="USD",
        original="h_mean",
    ),
    col(
        "annual_wage_mean",
        "FLOAT64",
        "Mean annual wage",
        unit="USD",
        original="a_mean",
    ),
    col(
        "wage_mean_prse",
        "FLOAT64",
        "Percent relative standard error of the mean wage estimate",
        unit="percent",
        original="mean_prse",
    ),
    col(
        "hourly_wage_percentile_10",
        "FLOAT64",
        "Hourly wage at the 10th percentile",
        unit="USD",
        original="h_pct10",
    ),
    col(
        "hourly_wage_percentile_25",
        "FLOAT64",
        "Hourly wage at the 25th percentile",
        unit="USD",
        original="h_pct25",
    ),
    col(
        "hourly_wage_median",
        "FLOAT64",
        "Median hourly wage, the 50th percentile",
        unit="USD",
        original="h_median",
    ),
    col(
        "hourly_wage_percentile_75",
        "FLOAT64",
        "Hourly wage at the 75th percentile",
        unit="USD",
        original="h_pct75",
    ),
    col(
        "hourly_wage_percentile_90",
        "FLOAT64",
        "Hourly wage at the 90th percentile",
        unit="USD",
        original="h_pct90",
    ),
    col(
        "annual_wage_percentile_10",
        "FLOAT64",
        "Annual wage at the 10th percentile",
        unit="USD",
        original="a_pct10",
    ),
    col(
        "annual_wage_percentile_25",
        "FLOAT64",
        "Annual wage at the 25th percentile",
        unit="USD",
        original="a_pct25",
    ),
    col(
        "annual_wage_median",
        "FLOAT64",
        "Median annual wage, the 50th percentile",
        unit="USD",
        original="a_median",
    ),
    col(
        "annual_wage_percentile_75",
        "FLOAT64",
        "Annual wage at the 75th percentile",
        unit="USD",
        original="a_pct75",
    ),
    col(
        "annual_wage_percentile_90",
        "FLOAT64",
        "Annual wage at the 90th percentile",
        unit="USD",
        original="a_pct90",
    ),
]

FLAGS = [
    col(
        "annual_wage_only",
        "STRING",
        "TRUE when the source releases only annual wages for the occupation, "
        "which applies to occupations paid annually but typically working fewer "
        "than 2,080 hours per year, such as teachers, pilots and athletes",
        original="annual",
    ),
    col(
        "hourly_wage_only",
        "STRING",
        "TRUE when the source releases only hourly wages for the occupation, "
        "which applies to occupations paid hourly but typically working fewer "
        "than 2,080 hours per year, such as actors, dancers and musicians",
        observations="Absent from the 2003 source files, where it is null",
        original="hourly",
    ),
    col(
        "wage_top_coded",
        "STRING",
        "TRUE when at least one wage estimate in the row is at or above $115.00 "
        "per hour or $239,200 per year and was therefore withheld by the source",
        observations=(
            "Derived from the source sentinel '#'. The wage columns it applies "
            "to are null, so this flag distinguishes a top-coded wage from a "
            "suppressed one. Top-coding always covers a contiguous upper run of "
            "the wage distribution, and masks the hourly and annual ladders at "
            "the same positions"
        ),
        original="(derived from '#')",
    ),
]


# ── area ────────────────────────────────────────────────────────────────────

AREA = [
    YEAR,
    col(
        "area_id",
        "STRING",
        "Area code: 99 for the United States, the state FIPS code for states and "
        "territories, the Metropolitan Statistical Area code, or the "
        "OEWS-specific nonmetropolitan area code",
        observations=(
            "One column holds four code systems, so no directory link is "
            "declared. Rows with area_type 2 carry a state FIPS code "
            "(br_bd_diretorios_us.state:id_state); rows with area_type 4 carry a "
            "CBSA code (br_bd_diretorios_us.cbsa_2023:id_cbsa), subject to the "
            "delineation vintage of the reference year. Metropolitan areas in "
            "2003 and 2004 follow the pre-2003 OMB definitions and carry 4-digit "
            "MSA/PMSA codes; from 2005 they carry 5-digit CBSA codes. The two "
            "code systems are not comparable, and nonmetropolitan estimates "
            "start in 2006"
        ),
        original="area",
    ),
    col(
        "area_type",
        "STRING",
        "Type of area the estimate covers: nation, state, territory, "
        "metropolitan statistical area, metropolitan division or "
        "nonmetropolitan area",
        dictionary="yes",
        observations=(
            "Taken from the source field from 2011 onward. For 2003-2010 the "
            "source files do not carry it and it is reconstructed from the "
            "file's geographic level and, for the 2005-2010 metropolitan "
            "files, from the 2011-2013 area-code lookup, with unmatched codes "
            "falling back to 4. The 2003-2004 metropolitan rows are all 4: those "
            "releases predate the CBSA delineations, so metropolitan divisions "
            "cannot be told apart from metropolitan statistical areas"
        ),
        original="area_type",
    ),
    col(
        "state_abbreviation",
        "STRING",
        "Two-letter abbreviation of the primary state of the area, or US for the "
        "national estimates",
        observations=(
            "Null for 2011-2019: the source publishes it in the 2003-2010 state "
            "and metropolitan files and again from 2020, but not in between"
        ),
        original="prim_state (2020 onward), prim_state or st (2003-2010)",
    ),
    col(
        "ownership_id",
        "STRING",
        "Ownership of the establishments covered by the estimate",
        dictionary="yes",
        observations=(
            "OEWS encodes cross-industry ownership splits as pseudo-NAICS codes "
            "(000000, 000001, 999001, 999101, 999201, 999301). Those map one to "
            "one onto this column, which is why the area table carries no "
            "industry code. On those rows the value is taken from the "
            "pseudo-NAICS code rather than from the source's own_code field, "
            "because the May 2012 release publishes own_code 5 (Private) on all "
            "of them; every other release agrees with the derived value"
        ),
        original="own_code",
    ),
    *OCCUPATION,
    col(
        "area_name",
        "STRING",
        "Name of the area the estimate covers",
        original="area_title (2011 onward), area_name or state (2003-2010)",
    ),
    OCCUPATION_NAME,
    *EMPLOYMENT,
    col(
        "jobs_per_1000",
        "FLOAT64",
        "Jobs in the occupation per 1,000 jobs in the area",
        unit="ratio",
        observations=(
            "Published for state and metropolitan estimates only; null "
            "elsewhere. Also null for 2003-2008 and for 2019, where the source "
            "does not publish the field at all"
        ),
        original="jobs_1000",
    ),
    col(
        "location_quotient",
        "FLOAT64",
        "Ratio of the occupation's share of area employment to its share of "
        "national employment",
        unit="ratio",
        observations=(
            "Published for state and metropolitan estimates only; null "
            "elsewhere. Also null for 2003-2009, where the source does not "
            "publish the field at all"
        ),
        original="loc_quotient (2012 onward), loc_q (2011), loc quotient (2010)",
    ),
    *WAGES,
    *FLAGS,
]


# ── industry ────────────────────────────────────────────────────────────────

INDUSTRY = [
    YEAR,
    col(
        "industry_id",
        "STRING",
        "North American Industry Classification System (NAICS) code, or an "
        "OEWS-specific code where OEWS publishes a combination of industries",
        observations=(
            "NAICS vintage changes within the panel, so no directory link is "
            "declared. Real codes align with the br_bd_diretorios_us.naics_* "
            "table matching the reference year's vintage. Codes 999000, 999100, "
            "999200 and 999300 are OEWS pseudo-codes for government excluding "
            "schools and hospitals"
        ),
        original="naics",
    ),
    col(
        "industry_group",
        "STRING",
        "NAICS aggregation level of the industry: sector, 3-digit, 4-digit, "
        "5-digit or 6-digit, with or without an ownership split",
        observations=(
            "Absent from the source before 2017, where it is null. Where OEWS "
            "no longer publishes an industry at the 4-digit level, the "
            "'4-digit' tag marks the most detailed breakdown available"
        ),
        original="i_group",
    ),
    col(
        "ownership_id",
        "STRING",
        "Ownership of the establishments covered by the estimate",
        dictionary="yes",
        observations=(
            "For 2003-2008 the source publishes national industry estimates "
            "without an ownership split; those rows carry the all-ownership "
            "code. On the government pseudo-NAICS codes (999000, 999100, "
            "999200, 999300) the value is taken from the industry code rather "
            "than from the source's own_code field, because the May 2012 release "
            "publishes own_code 5 (Private) on them; every other release agrees "
            "with the derived value"
        ),
        original="own_code (2011 onward), ownership title (2009-2010)",
    ),
    *OCCUPATION,
    col(
        "industry_name",
        "STRING",
        "NAICS title, or the OEWS-specific title where OEWS publishes a "
        "combination of industries",
        original="naics_title",
    ),
    OCCUPATION_NAME,
    *EMPLOYMENT,
    col(
        "percent_total_employment",
        "FLOAT64",
        "Percent of the industry's employment accounted for by the occupation",
        unit="percent",
        observations=(
            "Percents may not sum to 100 because industry totals include "
            "occupations that could not be published separately"
        ),
        original="pct_total",
    ),
    col(
        "percent_establishments_reporting",
        "FLOAT64",
        "Percent of establishments reporting the occupation for the cell",
        unit="percent",
        observations=(
            "Null where the source reported '~', meaning below 0.5 percent; see "
            "establishments_reporting_below_threshold. Also null for 2011-2020 "
            "and for the 2009-2010 by-ownership rows, where the source does not "
            "publish the field at all"
        ),
        original="pct_rpt",
    ),
    col(
        "establishments_reporting_below_threshold",
        "STRING",
        "TRUE when the percent of establishments reporting the occupation is "
        "below 0.5 percent, which the source reports as a symbol rather than a "
        "value",
        observations=(
            "Derived from the source sentinel '~'. "
            "percent_establishments_reporting is null on these rows, so this "
            "flag distinguishes a below-threshold value from a missing one"
        ),
        original="(derived from '~')",
    ),
    *WAGES,
    *FLAGS,
]


# ── dicionario ──────────────────────────────────────────────────────────────

DICIONARIO = [
    col(
        "id_tabela",
        "STRING",
        "Slug of the us_bls_oes table the dictionary entry describes",
        original="id_tabela",
    ),
    col(
        "nome_coluna",
        "STRING",
        "Name of the column the dictionary entry describes",
        original="nome_coluna",
    ),
    col(
        "chave",
        "STRING",
        "Code stored in the column",
        original="chave",
    ),
    col(
        "cobertura_temporal",
        "STRING",
        "Temporal coverage of the dictionary entry",
        original="cobertura_temporal",
    ),
    col(
        "valor",
        "STRING",
        "Label the code stands for",
        original="valor",
    ),
]


TABLES = {"area": AREA, "industry": INDUSTRY, "dicionario": DICIONARIO}


def main():
    out = Path(__file__).resolve().parent / "architecture"
    out.mkdir(parents=True, exist_ok=True)
    for table, rows in TABLES.items():
        path = out / f"{table}.csv"
        with open(path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=HEADER)
            w.writeheader()
            w.writerows(rows)
        names = [r["name"] for r in rows]
        assert len(names) == len(set(names)), (
            f"{table}: duplicate column names"
        )
        for r in rows:
            if r["bigquery_type"] in ("INT64", "FLOAT64"):
                assert r["measurement_unit"], (
                    f"{table}.{r['name']}: numeric without unit"
                )
            if r["covered_by_dictionary"] == "yes":
                assert r["bigquery_type"] == "STRING", (
                    f"{table}.{r['name']}: dict on non-STRING"
                )
            assert not r["description"].endswith("."), (
                f"{table}.{r['name']}: trailing period"
            )
            assert r["description"][:1].isupper(), (
                f"{table}.{r['name']}: not capitalized"
            )
        print(f"{path.name}: {len(rows)} columns")


if __name__ == "__main__":
    main()
