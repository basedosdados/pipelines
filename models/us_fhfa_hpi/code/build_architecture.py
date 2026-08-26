"""Write the us_fhfa_hpi architecture CSVs.

One CSV per table, in `architecture/`. The architecture is the single source of
truth for column order, BigQuery type and description; `clean_data.py`,
`build_dbt.py` and the metadata step all read it back from here.

Run:  uv run python models/us_fhfa_hpi/code/build_architecture.py
"""

import csv
from pathlib import Path

ARCH_DIR = Path(__file__).parent / "architecture"

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

DIR_YEAR = "br_bd_diretorios_data_tempo.ano:ano"
DIR_MONTH = "br_bd_diretorios_data_tempo.mes:mes"
DIR_STATE_ABBR = "br_bd_diretorios_us.state:abbreviation"
DIR_STATE_FIPS = "br_bd_diretorios_us.state:id_state"
DIR_COUNTY = "br_bd_diretorios_us.county:id_county"


def col(
    name,
    bigquery_type,
    description,
    *,
    dictionary="no",
    directory="",
    unit="",
    observations="",
    original_name="",
):
    return {
        "name": name,
        "bigquery_type": bigquery_type,
        "description": description,
        "temporal_coverage": "",
        "covered_by_dictionary": dictionary,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations": observations,
        "original_name": original_name,
    }


# ---------------------------------------------------------------- master file

YEAR_MASTER = col(
    "year",
    "INT64",
    "Reference year of the index value",
    directory=DIR_YEAR,
    unit="year",
    observations="Partition column",
    original_name="yr",
)

INDEX_TYPE = col(
    "index_type",
    "STRING",
    "Index variant: traditional, developmental, distress-free, non-metro or manufactured",
    dictionary="yes",
    observations=(
        "The traditional purchase-only seasonally adjusted index is the series FHFA reports in its "
        "press releases. non-metro covers the nonmetropolitan remainder of a state rather than the "
        "state as a whole; manufactured covers manufactured homes only."
    ),
    original_name="hpi_type",
)

INDEX_FLAVOR = col(
    "index_flavor",
    "STRING",
    "Underlying transaction data: purchase-only, all-transactions or expanded-data",
    dictionary="yes",
    observations=(
        "The three flavours differ by the data feeding the repeat-sales regression, not by the "
        "statistical method. Only all-transactions runs before 1991."
    ),
    original_name="hpi_flavor",
)

INDEX_NSA = col(
    "index_nsa",
    "FLOAT64",
    "House price index, not seasonally adjusted",
    unit="index",
    observations="The first period of each series is set to 100",
    original_name="index_nsa",
)

INDEX_SA = col(
    "index_sa",
    "FLOAT64",
    "House price index, seasonally adjusted",
    unit="index",
    observations="Null wherever FHFA publishes no seasonally adjusted variant of the series",
    original_name="index_sa",
)


def master_period(kind):
    if kind == "month":
        return col(
            "month",
            "INT64",
            "Reference month of the index value",
            directory=DIR_MONTH,
            unit="month",
            original_name="period",
        )
    return col(
        "quarter",
        "INT64",
        "Reference quarter of the index value",
        unit="quarter",
        original_name="period",
    )


TABLES = {}

TABLES["monthly_national"] = [
    YEAR_MASTER,
    master_period("month"),
    col(
        "place_id",
        "STRING",
        "Place code: USA for the United States, or DV_ plus the Census division abbreviation",
        original_name="place_id",
    ),
    col(
        "place_name",
        "STRING",
        "Name of the United States or of the Census division",
        original_name="place_name",
    ),
    INDEX_TYPE,
    INDEX_FLAVOR,
    INDEX_NSA,
    INDEX_SA,
]

TABLES["quarterly_national"] = [
    YEAR_MASTER,
    master_period("quarter"),
    col(
        "place_id",
        "STRING",
        "Place code: USA for the United States, or DV_ plus the Census division abbreviation",
        original_name="place_id",
    ),
    col(
        "place_name",
        "STRING",
        "Name of the United States or of the Census division",
        original_name="place_name",
    ),
    INDEX_TYPE,
    INDEX_FLAVOR,
    INDEX_NSA,
    INDEX_SA,
]

TABLES["quarterly_state"] = [
    YEAR_MASTER,
    master_period("quarter"),
    col(
        "state_abbreviation",
        "STRING",
        "Two-letter abbreviation of the state, the District of Columbia or Puerto Rico",
        directory=DIR_STATE_ABBR,
        original_name="place_id",
    ),
    col(
        "state_name",
        "STRING",
        "Name of the state, the District of Columbia or Puerto Rico",
        original_name="place_name",
    ),
    INDEX_TYPE,
    INDEX_FLAVOR,
    INDEX_NSA,
    INDEX_SA,
]

TABLES["quarterly_metro"] = [
    YEAR_MASTER,
    master_period("quarter"),
    col(
        "cbsa_id",
        "STRING",
        "Five-digit code of the Metropolitan Statistical Area or Metropolitan Division",
        observations=(
            "No directory link is declared: 37 of the 410 codes are Metropolitan Division (MSAD) codes, "
            "which br_bd_diretorios_us.cbsa_2023 does not carry. The remaining 373 resolve against "
            "br_bd_diretorios_us.cbsa_2023:id_cbsa."
        ),
        original_name="place_id",
    ),
    col(
        "cbsa_name",
        "STRING",
        "Name of the Metropolitan Statistical Area or Metropolitan Division",
        original_name="place_name",
    ),
    INDEX_TYPE,
    INDEX_FLAVOR,
    INDEX_NSA,
    INDEX_SA,
    col(
        "relative_standard_error",
        "FLOAT64",
        "Relative standard error of the index value",
        unit="percent",
        observations="Published for the expanded-data metropolitan series only; null elsewhere",
        original_name="rstderr",
    ),
    col(
        "note",
        "STRING",
        "Footnote FHFA attaches to the index value",
        observations=(
            "Flags quarters with fewer than 1,000 accumulated repeat-sales records, or a value "
            "suppressed for too few records. Null on every other row; the source's literal tab "
            "placeholder is cleaned to null."
        ),
        original_name="note",
    ),
]

# ----------------------------------------------------- annual developmental

YEAR_ANNUAL = col(
    "year",
    "INT64",
    "Reference year of the index value",
    directory=DIR_YEAR,
    unit="year",
    observations="Partition column",
    original_name="Year",
)

ANNUAL_CHANGE = col(
    "annual_change_percent",
    "FLOAT64",
    "Percentage change in the index from the previous year",
    unit="percent",
    observations="Null in the first year of each series, which has no previous year",
    original_name="Annual Change (%)",
)

ANNUAL_INDEX = col(
    "index_nsa",
    "FLOAT64",
    "Annual house price index, not seasonally adjusted, with the first year of the series set to 100",
    unit="index",
    original_name="HPI",
)

ANNUAL_1990 = col(
    "index_nsa_1990_base",
    "FLOAT64",
    "Annual house price index rescaled so that 1990 equals 100",
    unit="index",
    observations="Null where the series does not reach 1990",
    original_name="HPI with 1990 base",
)

ANNUAL_2000 = col(
    "index_nsa_2000_base",
    "FLOAT64",
    "Annual house price index rescaled so that 2000 equals 100",
    unit="index",
    observations="Null where the series does not reach 2000",
    original_name="HPI with 2000 base",
)

ANNUAL_TAIL = [ANNUAL_CHANGE, ANNUAL_INDEX, ANNUAL_1990, ANNUAL_2000]

TABLES["annual_national"] = [YEAR_ANNUAL, *ANNUAL_TAIL]

TABLES["annual_state"] = [
    YEAR_ANNUAL,
    col(
        "state_id",
        "STRING",
        "Two-digit FIPS code of the state or the District of Columbia",
        directory=DIR_STATE_FIPS,
        original_name="FIPS",
    ),
    col(
        "state_abbreviation",
        "STRING",
        "Two-letter abbreviation of the state or the District of Columbia",
        original_name="Abbreviation",
    ),
    col(
        "state_name",
        "STRING",
        "Name of the state or the District of Columbia",
        original_name="State",
    ),
    *ANNUAL_TAIL,
]

TABLES["annual_cbsa"] = [
    YEAR_ANNUAL,
    col(
        "cbsa_id",
        "STRING",
        "Five-digit Core Based Statistical Area code, or a two-digit state FIPS code for that state's non-CBSA remainder",
        observations=(
            "No directory link is declared: 922 of the 966 codes are five-digit CBSA codes that resolve "
            "against br_bd_diretorios_us.cbsa_2023:id_cbsa, but the other 44 are two-digit state FIPS "
            "codes standing for the state's areas outside any CBSA."
        ),
        original_name="CBSA",
    ),
    col(
        "cbsa_name",
        "STRING",
        "Name of the Core Based Statistical Area or of the state's non-CBSA remainder",
        original_name="Name",
    ),
    *ANNUAL_TAIL,
]

TABLES["annual_county"] = [
    YEAR_ANNUAL,
    col(
        "county_id",
        "STRING",
        "Five-digit FIPS code of the county or county equivalent",
        directory=DIR_COUNTY,
        original_name="FIPS code",
    ),
    col(
        "county_name",
        "STRING",
        "Name of the county or county equivalent",
        original_name="County",
    ),
    col(
        "state_abbreviation",
        "STRING",
        "Two-letter abbreviation of the state the county belongs to",
        original_name="State",
    ),
    *ANNUAL_TAIL,
]

TABLES["annual_zip3"] = [
    YEAR_ANNUAL,
    col(
        "zip_code_3",
        "STRING",
        "Three-digit USPS ZIP code prefix",
        observations=(
            "USPS ZIP prefixes are not ZCTAs, so no link to br_bd_diretorios_us.zcta_2020 is declared"
        ),
        original_name="Three-Digit ZIP Code",
    ),
    *ANNUAL_TAIL,
]

TABLES["annual_zip5"] = [
    YEAR_ANNUAL,
    col(
        "zip_code_5",
        "STRING",
        "Five-digit USPS ZIP code",
        observations=(
            "USPS ZIP codes are not ZCTAs, so no link to br_bd_diretorios_us.zcta_2020 is declared"
        ),
        original_name="Five-Digit ZIP Code",
    ),
    *ANNUAL_TAIL,
]

TABLES["annual_tract"] = [
    YEAR_ANNUAL,
    col(
        "census_tract_id",
        "STRING",
        "Eleven-digit census tract GEOID",
        observations=(
            "FHFA builds the tract index on 2010 census tract boundaries (working paper 16-04), so no "
            "link to br_bd_diretorios_us.census_tract_2020 is declared"
        ),
        original_name="tract",
    ),
    col(
        "state_abbreviation",
        "STRING",
        "Two-letter abbreviation of the state the tract belongs to",
        original_name="state_abbr",
    ),
    *ANNUAL_TAIL,
]

TABLES["dicionario"] = [
    col(
        "id_tabela",
        "STRING",
        "Slug of the us_fhfa_hpi table the dictionary entry describes",
        original_name="id_tabela",
    ),
    col(
        "nome_coluna",
        "STRING",
        "Name of the column the dictionary entry describes",
        original_name="nome_coluna",
    ),
    col("chave", "STRING", "Code stored in the column", original_name="chave"),
    col(
        "cobertura_temporal",
        "STRING",
        "Temporal coverage of the dictionary entry",
        original_name="cobertura_temporal",
    ),
    col("valor", "STRING", "Label the code stands for", original_name="valor"),
]


def main():
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    for table, cols in TABLES.items():
        path = ARCH_DIR / f"{table}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(cols)
        print(f"{path.name}: {len(cols)} columns")


if __name__ == "__main__":
    main()
