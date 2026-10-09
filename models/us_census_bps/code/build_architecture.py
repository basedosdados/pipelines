"""Emit the architecture CSVs for us_census_bps.

The CSVs under ``architecture/`` are the single source of truth for column
names, order and BigQuery types. The cleaning transform in
``pipelines/datasets/us_census_bps/utils.py`` reads them, and the dbt models
mirror them.
"""

from __future__ import annotations

import csv
from pathlib import Path

ARCH_DIR = Path(__file__).parent / "architecture"

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

DIR_YEAR = "br_bd_diretorios_data_tempo.ano:ano"
DIR_MONTH = "br_bd_diretorios_data_tempo.mes:mes"
DIR_STATE = "diretorios_us.state:id_state"
DIR_COUNTY = "diretorios_us.county:id_county"
DIR_PLACE = "diretorios_us.place:id_place"
DIR_CBSA = "diretorios_us.cbsa_2023:id_cbsa"

IMPUTED_NOTE = (
    "Estimate with imputation: reported data for responding permit offices "
    "plus imputed data for non-respondents. This is the series to use by "
    "default"
)
REPORTED_NOTE = (
    "Reported only: counts data from responding permit offices and makes no "
    "imputation for non-respondents. Lower than the estimate with imputation"
)


def col(
    name: str,
    btype: str,
    description: str,
    *,
    coverage: str = "",
    dictionary: str = "no",
    directory: str = "",
    unit: str = "",
    observations: str = "",
    original: str = "",
) -> list[str]:
    """Build one architecture row."""
    return [
        name,
        btype,
        description,
        coverage,
        dictionary,
        directory,
        unit,
        "no",
        observations,
        original,
    ]


def year_col(coverage: str) -> list[str]:
    return col(
        "year",
        "INT64",
        "Reference year of the survey period",
        coverage=coverage,
        directory=DIR_YEAR,
        unit="year",
        observations="Partition column",
        original="Survey Date",
    )


def month_col(coverage: str) -> list[str]:
    return col(
        "month",
        "INT64",
        "Reference month of the survey period, from 1 to 12",
        coverage=coverage,
        directory=DIR_MONTH,
        unit="month",
        original="Survey Date",
    )


def structure_type_col() -> list[str]:
    return col(
        "structure_type",
        "STRING",
        "Type of residential structure, by the number of housing units in "
        "the building",
        dictionary="yes",
        observations=(
            "Census structure type codes: 101 single-family, 103 two-family, "
            "104 three- and four-family, 105 five-or-more-family. Summing the "
            "four codes reproduces the published total for the geography"
        ),
        original="column block",
    )


def measures(*, reported: bool) -> list[list[str]]:
    """Buildings, units and valuation, optionally with the reported-only block."""
    rows = [
        col(
            "buildings",
            "INT64",
            "Number of buildings authorized by building permits",
            unit="building",
            observations=IMPUTED_NOTE,
            original="Bldgs",
        ),
        col(
            "units",
            "INT64",
            "Number of housing units authorized by building permits",
            unit="housing_unit",
            observations=IMPUTED_NOTE,
            original="Units",
        ),
        col(
            "valuation",
            "INT64",
            "Construction valuation of the buildings authorized, in US dollars",
            unit="usd",
            observations=IMPUTED_NOTE,
            original="Value",
        ),
    ]
    if not reported:
        return rows
    rows += [
        col(
            "buildings_reported",
            "INT64",
            "Number of buildings authorized, counting reporting permit "
            "offices only",
            unit="building",
            observations=REPORTED_NOTE,
            original="Bldgs rep",
        ),
        col(
            "units_reported",
            "INT64",
            "Number of housing units authorized, counting reporting permit "
            "offices only",
            unit="housing_unit",
            observations=REPORTED_NOTE,
            original="Units rep",
        ),
        col(
            "valuation_reported",
            "INT64",
            "Construction valuation of the buildings authorized, in US "
            "dollars, counting reporting permit offices only",
            unit="usd",
            observations=REPORTED_NOTE,
            original="Value rep",
        ),
    ]
    return rows


def region_division(coverage: str = "") -> list[list[str]]:
    return [
        col(
            "region_id",
            "STRING",
            "Census region code, from 1 to 4",
            coverage=coverage,
            dictionary="yes",
            original="Region Code",
        ),
        col(
            "division_id",
            "STRING",
            "Census division code, from 1 to 9",
            coverage=coverage,
            dictionary="yes",
            original="Division Code",
        ),
    ]


def state_table(monthly: bool) -> list[list[str]]:
    cov = "1988(1)2026" if monthly else "1980(1)2025"
    rows = [year_col(cov)]
    if monthly:
        rows.append(month_col(cov))
    rows += [
        col(
            "geography_level",
            "STRING",
            "Level of the geography the row describes",
            dictionary="yes",
            observations=(
                "The published state file also carries the national, "
                "regional and divisional totals. Filter on this column before "
                "aggregating, or the same permits are counted several times"
            ),
            original="FIPS State",
        ),
        col(
            "geography_id",
            "STRING",
            "Code identifying the geography, as published",
            observations=(
                "Two-digit FIPS state code for states and territories, "
                "R1-R4 for regions, D1-D9 for divisions, US for the national "
                "total"
            ),
            original="FIPS State",
        ),
        col(
            "state_id",
            "STRING",
            "Two-digit FIPS state code",
            directory=DIR_STATE,
            observations=(
                "Null on the national, regional and divisional total rows. "
                "Until 2021 the survey identified the territories by their "
                "old Census codes rather than by FIPS, so Puerto Rico is "
                "published as 43 through 2021 and as 72 from 2022; this "
                "column carries the FIPS code throughout, and geography_id "
                "carries the code as published"
            ),
            original="FIPS State",
        ),
    ]
    rows += region_division()
    rows += [
        structure_type_col(),
        col(
            "geography_name",
            "STRING",
            "Name of the geography, as published",
            original="State Name",
        ),
    ]
    rows += measures(reported=True)
    return rows


def county_table(monthly: bool) -> list[list[str]]:
    cov = "2000(1)2026" if monthly else "1990(1)2025"
    rows = [year_col(cov)]
    if monthly:
        rows.append(month_col(cov))
    rows += [
        col(
            "county_id",
            "STRING",
            "Five-digit FIPS county code, state code followed by county code",
            directory=DIR_COUNTY,
            original="FIPS State + FIPS County",
        ),
        col(
            "state_id",
            "STRING",
            "Two-digit FIPS state code",
            directory=DIR_STATE,
            original="FIPS State",
        ),
    ]
    rows += region_division()
    rows += [
        structure_type_col(),
        col(
            "county_name",
            "STRING",
            "Name of the county, as published",
            original="County Name",
        ),
    ]
    rows += measures(reported=True)
    return rows


def cbsa_table(monthly: bool) -> list[list[str]]:
    cov = "2004(1)2026" if monthly else "2003(1)2025"
    rows = [year_col(cov)]
    if monthly:
        rows.append(month_col(cov))
    rows += [
        col(
            "cbsa_id",
            "STRING",
            "Five-digit Core Based Statistical Area code",
            directory=DIR_CBSA,
            original="CBSA Code",
        ),
        col(
            "csa_id",
            "STRING",
            "Three-digit Combined Statistical Area code the CBSA belongs to",
            observations=(
                "Null where the source publishes 999, meaning the CBSA is not "
                "part of a CSA"
            ),
            original="CSA Code",
        ),
        col(
            "cbsa_type",
            "STRING",
            "Header code distinguishing metropolitan from micropolitan areas",
            coverage="2024(1)2025",
            dictionary="yes",
            observations=(
                "Published from January 2024 only, when micropolitan areas "
                "were added to the file. Null before that"
            ),
            original="HHEADER",
        ),
        col(
            "full_monthly_coverage",
            "STRING",
            "Whether the area is completely covered by monthly reporting "
            "permit-issuing places",
            coverage="2003(1)2023",
            dictionary="yes",
            observations=(
                "Published up to 2023 only, in the field replaced by "
                "cbsa_type from January 2024. Null from 2024 on"
            ),
            original="MONCOV",
        ),
        structure_type_col(),
        col(
            "cbsa_name",
            "STRING",
            "Name of the Core Based Statistical Area, as published",
            original="CBSA Name",
        ),
    ]
    rows += measures(reported=True)
    return rows


def msa_table(monthly: bool) -> list[list[str]]:
    cov = "1988(1)2003" if monthly else "1980(1)2002"
    rows = [year_col(cov)]
    if monthly:
        rows.append(month_col(cov))
    rows += [
        col(
            "msa_cmsa_id",
            "STRING",
            "Four-digit Metropolitan or Consolidated Metropolitan Statistical "
            "Area code",
            observations=(
                "The pre-2004 MSA/CMSA code system, superseded by CBSA codes. "
                "The two are different geographies, not a renaming, so this "
                "table does not join to the CBSA tables or to the CBSA "
                "directory"
            ),
            original="MSA/CMSA Code",
        ),
        col(
            "pmsa_id",
            "STRING",
            "Four-digit Primary Metropolitan Statistical Area code",
            observations=(
                "Null where the source publishes 9999, meaning the area is "
                "not divided into PMSAs"
            ),
            original="PMSA Code",
        ),
        structure_type_col(),
        col(
            "msa_name",
            "STRING",
            "Name of the metropolitan area, as published",
            original="MA Name",
        ),
    ]
    rows += measures(reported=True)
    return rows


PLACE_GEO = [
    col(
        "state_id",
        "STRING",
        "Two-digit FIPS state code",
        directory=DIR_STATE,
        original="State Code",
    ),
    col(
        "county_id",
        "STRING",
        "Five-digit FIPS county code, state code followed by county code",
        directory=DIR_COUNTY,
        original="State Code + County Code",
    ),
    col(
        "place_id",
        "STRING",
        "Seven-digit FIPS place code, state code followed by place code",
        coverage="2008(1)2026",
        directory=DIR_PLACE,
        observations=(
            "Published from 2008 only. Null for the roughly 28 percent of "
            "permit offices that are minor civil divisions or county-part "
            "records rather than places, and for the balance-of-state "
            "sentinel the source writes as 99990. Where present it matches "
            "the 2020-vintage place directory for 99.6 percent of offices"
        ),
        original="FIPS Place Code",
    ),
    col(
        "mcd_id",
        "STRING",
        "Seven-digit FIPS minor civil division code, state code followed by "
        "MCD code",
        coverage="2008(1)2026",
        observations=(
            "Published from 2008 only. Identifies township and other minor "
            "civil division permit offices, which have no FIPS place code"
        ),
        original="FIPS MCD Code",
    ),
    col(
        "cbsa_id",
        "STRING",
        "Five-digit Core Based Statistical Area code",
        coverage="2005(1)2026",
        directory=DIR_CBSA,
        observations=(
            "Published from 2005 only, when CBSA codes replaced MSA/PMSA "
            "codes. Null where the source publishes 99999"
        ),
        original="CBSA Code",
    ),
    col(
        "csa_id",
        "STRING",
        "Three-digit Combined Statistical Area code",
        coverage="2005(1)2026",
        observations=(
            "Published from 2005 only. Null where the source publishes 999"
        ),
        original="CSA Code",
    ),
    col(
        "msa_cmsa_id",
        "STRING",
        "Four-digit Metropolitan or Consolidated Metropolitan Statistical "
        "Area code",
        coverage="1988(1)2004",
        observations=(
            "The pre-2005 code system, superseded by cbsa_id. Null from 2005 "
            "on"
        ),
        original="MSA/CMSA Code",
    ),
    col(
        "pmsa_id",
        "STRING",
        "Four-digit Primary Metropolitan Statistical Area code",
        coverage="1988(1)2004",
        observations=(
            "The pre-2005 code system, superseded by cbsa_id. Null from 2005 "
            "on and where the source publishes 9999"
        ),
        original="PMSA Code",
    ),
    col(
        "permit_office_id",
        "STRING",
        "Six-digit Building Permit Survey identifier of the permit-issuing "
        "office, unique within a state and survey period",
        observations=(
            "The Census Bureau assigns this code to sort offices "
            "alphabetically within a state, so it is reassigned when offices "
            "are added and is not stable over time. Addison village, "
            "Illinois, is 001000 in 1988 and 002800 from 1995. Do not use it "
            "to build a panel; use place_id or mcd_id"
        ),
        original="6-Digit ID",
    ),
    col(
        "census_place_id",
        "STRING",
        "Four-digit Census place code",
        coverage="2000(1)2026",
        observations=(
            "Published from 2000 only. A Census Bureau internal place code, "
            "not the FIPS place code"
        ),
        original="Census Place Code",
    ),
]

PLACE_DESCRIPTIVE = [
    col(
        "place_name",
        "STRING",
        "Name of the permit-issuing place or office, as published",
        original="Place Name",
    ),
    col(
        "central_city",
        "STRING",
        "Whether the place is a central city of a metropolitan area",
        coverage="2000(1)2026",
        dictionary="yes",
        original="Central City",
    ),
    col(
        "footnote_code",
        "STRING",
        "Whether the place carries an explanatory footnote in the source "
        "release",
        coverage="2005(1)2026",
        dictionary="yes",
        observations=(
            "The footnote text itself is published in a separate monthly "
            "file, not loaded here"
        ),
        original="Footnote Code",
    ),
    col(
        "zip_code",
        "STRING",
        "ZIP code of the permit office or official",
        coverage="2000(1)2026",
        original="Zip Code",
    ),
    col(
        "population",
        "INT64",
        "Population of the place as carried in the source file",
        coverage="2008(1)2026",
        unit="person",
        observations=(
            "The Census layout document labels this the 2000 population, but "
            "the values change from release to release, so it is a current "
            "population estimate. Published from 2008 only"
        ),
        original="Pop",
    ),
]


def place_table(monthly: bool) -> list[list[str]]:
    cov = "1988(1)2026" if monthly else "1980(1)2025"
    rows = [year_col(cov)]
    if monthly:
        rows.append(month_col(cov))
    rows += PLACE_GEO
    rows += region_division()
    rows.append(structure_type_col())
    rows += PLACE_DESCRIPTIVE
    if monthly:
        rows.append(
            col(
                "source_code",
                "STRING",
                "How the data for the permit office was obtained for the "
                "month",
                dictionary="yes",
                observations=(
                    "Codes 1 to 4 mark reported data, 5 marks imputed data "
                    "and 9 marks a month with no report and no imputation. "
                    "The monthly place file carries no separate reported-only "
                    "block, so this column is how a reported subset is "
                    "selected"
                ),
                original="Source Code",
            )
        )
    else:
        rows.append(
            col(
                "months_reported",
                "INT64",
                "Number of months of the year for which the permit office "
                "reported residential data",
                unit="month",
                original="Number of Months Rep",
            )
        )
    rows += measures(reported=not monthly)
    return rows


DICIONARIO = [
    col(
        "id_tabela",
        "STRING",
        "Slug of the us_census_bps table the dictionary entry describes",
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
        "Coded value (key) exactly as stored in the data",
        original="chave",
    ),
    col(
        "cobertura_temporal",
        "STRING",
        "Temporal coverage of the key",
        original="cobertura_temporal",
    ),
    col(
        "valor",
        "STRING",
        "Human-readable label corresponding to the coded value",
        original="valor",
    ),
]

TABLES = {
    "permit_place_monthly": place_table(monthly=True),
    "permit_place_annual": place_table(monthly=False),
    "permit_county_monthly": county_table(monthly=True),
    "permit_county_annual": county_table(monthly=False),
    "permit_cbsa_monthly": cbsa_table(monthly=True),
    "permit_cbsa_annual": cbsa_table(monthly=False),
    "permit_msa_monthly": msa_table(monthly=True),
    "permit_msa_annual": msa_table(monthly=False),
    "permit_state_monthly": state_table(monthly=True),
    "permit_state_annual": state_table(monthly=False),
    "dicionario": DICIONARIO,
}


def main() -> None:
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    for table, rows in TABLES.items():
        path = ARCH_DIR / f"{table}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh, lineterminator="\n")
            writer.writerow(HEADER)
            writer.writerows(rows)
        print(f"{table}: {len(rows)} columns -> {path.name}")


if __name__ == "__main__":
    main()
