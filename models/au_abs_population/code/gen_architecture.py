"""Generate the six architecture CSVs from one declarative spec."""

import csv
import os
import sys

sys.path.insert(0, os.getcwd())
from pipelines.datasets.au_abs_population.constants import constants

OUT = "models/au_abs_population/code/architecture"
HDR = [
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
    name, typ, desc, *, cov="", dic="no", dirc="", unit="", obs="", orig=""
):
    """One architecture row, in the canonical column order."""
    values = [name, typ, desc, cov, dic, dirc, unit, "no", obs, orig]
    return dict(zip(HDR, values, strict=True))


YEAR_DIR = "br_bd_diretorios_data_tempo.ano:ano"
STATE_DIR = "br_bd_diretorios_au.state:id_state"

ASGS_NOTE = (
    "The entire series is published on the ASGS Edition 3 (2021) boundaries: ABS "
    "restates history onto current boundaries at each release, so codes are "
    "comparable across every year of this table but NOT against figures published "
    "in releases before the 2021 edition, which used ASGS 2016. Use "
    "br_bd_diretorios_au.correspondence_sa2_2016_2021 to bridge that break"
)

GEO_LEVEL = col(
    "geography_level",
    "STRING",
    "Geographic level of the row, either state or australia",
    obs=(
        "australia marks the national aggregate, which is not a state and therefore "
        "carries a null state_id; state marks one of the eight states and territories"
    ),
)


def state_col():
    return col(
        "state_id",
        "STRING",
        "Code of the Australian state or territory",
        dirc=STATE_DIR,
        obs="Null on national (australia) rows, which are not a state",
    )


def region_col():
    return col(
        "region_name",
        "STRING",
        "Name of the state, territory or national aggregate as published by the ABS",
        orig="Data Item Description",
    )


def sex_col(obs):
    return col(
        "sex",
        "STRING",
        "Sex of the population counted",
        obs=obs,
        orig="Data Item Description",
    )


def series_id_col(desc):
    return col(
        "series_id",
        "STRING",
        desc,
        obs="Foreign key to the au_abs_population series table",
        orig="Series ID",
    )


COMPONENTS = [
    (
        "births",
        "Number of births registered to usual residents of the area during the financial year",
    ),
    (
        "deaths",
        "Number of deaths of usual residents of the area during the financial year",
    ),
    ("natural_increase", "Births less deaths during the financial year"),
    (
        "internal_arrivals",
        "Number of people who moved into the area from elsewhere in Australia",
    ),
    (
        "internal_departures",
        "Number of people who moved out of the area to elsewhere in Australia",
    ),
    ("net_internal_migration", "Internal arrivals less internal departures"),
    (
        "overseas_arrivals",
        "Number of people who moved into the area from overseas",
    ),
    (
        "overseas_departures",
        "Number of people who moved out of the area to overseas",
    ),
    ("net_overseas_migration", "Overseas arrivals less overseas departures"),
]
COMP_NOTE = (
    "Published by ABS only for the four most recent financial years, so this column "
    "is null for earlier years of the ERP series"
)


def components():
    return [
        col(n, "INT64", d, unit="person", obs=COMP_NOTE) for n, d in COMPONENTS
    ]


def area_density(level):
    return [
        col(
            "area_sqkm",
            "FLOAT64",
            "Area of the region in square kilometres",
            unit="km2",
            obs=(
                "Published with the latest release only and carried across every year, because "
                "it is a property of the boundary and the whole series is published on a single "
                "boundary vintage"
            ),
            orig="Area",
        ),
        col(
            "population_density",
            "FLOAT64",
            "Estimated resident population per square kilometre, as published by the ABS",
            unit="person / km2",
            obs=(
                "Populated only for the latest reference year, the one year ABS publishes it for. "
                "It is deliberately not recomputed for earlier years: ABS derives density from the "
                "unrounded area while publishing area rounded to 0.1 km2, so erp / area_sqkm "
                "reproduced only 35% of SA2 values within 0.1 and erred by up to 2769"
            ),
            orig="Population density",
        ),
    ]


SPEC = {
    "national_state": [
        col(
            "year",
            "INT64",
            "Reference year of the quarterly observation",
            cov="1981(1)2025",
            dirc=YEAR_DIR,
            unit="year",
            obs="Partition column",
        ),
        col(
            "quarter",
            "INT64",
            "Reference quarter of the observation, from 1 to 4",
            unit="quarter",
            obs="ABS labels each quarter by its final month (Q1 March, Q2 June, Q3 September, Q4 December)",
        ),
        GEO_LEVEL,
        state_col(),
        region_col(),
        sex_col(
            "Male, Female or Persons; series that ABS does not break down by sex are reported as Persons, which is what they count"
        ),
        col(
            "measure",
            "STRING",
            "Demographic measure reported by the series",
            obs=(
                "Includes estimated_resident_population, natural_increase, net_overseas_migration, "
                "net_interstate_migration, births, deaths and the interstate and overseas movement flows"
            ),
            orig="Data Item Description",
        ),
        col(
            "unit",
            "STRING",
            "Unit the value is published in",
            obs=(
                "Persons or Number for the state-level series, 000 for the Australia-only series ABS "
                "publishes in thousands, and Percent for the percentage change series. Where the same "
                "measure was published in both thousands and persons, the persons figure is kept"
            ),
            orig="Unit",
        ),
        series_id_col("ABS series identifier of the observation"),
        col(
            "value",
            "FLOAT64",
            "Observed value of the series for the quarter",
            obs="Unit varies by row and is given by the unit column, so no fixed measurement unit is set here",
        ),
    ],
    "erp_age_sex": [
        col(
            "year",
            "INT64",
            "Reference year of the estimate, at 30 June",
            cov="1971(1)2025",
            dirc=YEAR_DIR,
            unit="year",
            obs="Partition column",
        ),
        GEO_LEVEL,
        state_col(),
        region_col(),
        sex_col(
            "Male, Female or Persons, where Persons is the ABS-published total"
        ),
        col(
            "age",
            "STRING",
            "Single year of age of the population counted",
            obs=(
                "Single years 0 to 99 plus the open-ended group 100 and over; stored as a string "
                "because the top group is not a number"
            ),
            orig="Data Item Description",
        ),
        series_id_col("ABS series identifier of the observation"),
        col(
            "erp",
            "INT64",
            "Estimated resident population at 30 June",
            unit="person",
            orig="value",
        ),
    ],
    "projection": [
        col(
            "year",
            "INT64",
            "Year the population is projected for, at 30 June",
            cov="2022(1)2071",
            dirc=YEAR_DIR,
            unit="year",
            obs="Partition column",
        ),
        col(
            "projection_base_year",
            "INT64",
            "Year of the estimated resident population the projection is based on",
            unit="year",
            obs="2022 for the current release; ABS re-bases the projections every few years",
        ),
        col(
            "series",
            "STRING",
            "Projection series, high, medium or low",
            obs=(
                "ABS series 1(A) high, 29(B) medium and 45(C) low, which differ in their assumed "
                "fertility, life expectancy, net overseas migration and interstate migration. The "
                "medium series is the one ABS describes as the most likely"
            ),
        ),
        GEO_LEVEL,
        state_col(),
        region_col(),
        sex_col(
            "Male, Female or Persons, where Persons is the ABS-published total"
        ),
        col(
            "age",
            "STRING",
            "Single year of age of the projected population",
            obs=(
                "Single years 0 to 84 plus 85 and over for the states, and 0 to 99 plus 100 and over "
                "for Australia; stored as a string because the top group is not a number"
            ),
            orig="Data Item Description",
        ),
        series_id_col("ABS series identifier of the projected series"),
        col(
            "projected_population",
            "INT64",
            "Projected population at 30 June",
            unit="person",
            orig="value",
        ),
    ],
    "regional_sa2": [
        col(
            "year",
            "INT64",
            "Reference year, at 30 June for the population and the financial year ending "
            "30 June for the components",
            cov="2001(1)2025",
            dirc=YEAR_DIR,
            unit="year",
            obs="Partition column",
        ),
        col(
            "sa2_id",
            "STRING",
            "Code of the Statistical Area Level 2",
            dirc="br_bd_diretorios_au.sa2_2021:id_sa2",
            obs=ASGS_NOTE,
            orig="SA2 code",
        ),
        col(
            "sa2_name",
            "STRING",
            "Name of the Statistical Area Level 2",
            orig="SA2 name",
        ),
        col(
            "sa3_id",
            "STRING",
            "Code of the Statistical Area Level 3 containing the SA2",
            dirc="br_bd_diretorios_au.sa3_2021:id_sa3",
            orig="SA3 code",
        ),
        col(
            "sa4_id",
            "STRING",
            "Code of the Statistical Area Level 4 containing the SA2",
            dirc="br_bd_diretorios_au.sa4_2021:id_sa4",
            orig="SA4 code",
        ),
        col(
            "gccsa_id",
            "STRING",
            "Code of the Greater Capital City Statistical Area containing the SA2",
            dirc="br_bd_diretorios_au.gccsa_2021:id_gccsa",
            orig="GCCSA code",
        ),
        col(
            "state_id",
            "STRING",
            "Code of the Australian state or territory containing the SA2",
            dirc=STATE_DIR,
            orig="S/T code",
        ),
        col(
            "erp",
            "INT64",
            "Estimated resident population at 30 June",
            unit="person",
            orig="ERP at 30 June",
        ),
        *components(),
        *area_density("sa2"),
    ],
    "regional_lga": [
        col(
            "year",
            "INT64",
            "Reference year, at 30 June for the population and the financial year ending "
            "30 June for the components",
            cov="2001(1)2025",
            dirc=YEAR_DIR,
            unit="year",
            obs="Partition column",
        ),
        col(
            "lga_id",
            "STRING",
            "Code of the Local Government Area",
            dirc="br_bd_diretorios_au.lga_2021:id_lga",
            obs=(
                "ABS restates the whole series onto the LGA boundaries current at the release, which run "
                "ahead of the ASGS 2021 LGA directory: 24700 Merri-bek (renamed from Moreland in 2022) "
                "and 71500 East Arnhem / 71700 Groote Archipelago (split in 2023) have no 2021 entry"
            ),
            orig="LGA code",
        ),
        col(
            "lga_name",
            "STRING",
            "Name of the Local Government Area",
            orig="LGA name",
        ),
        col(
            "state_id",
            "STRING",
            "Code of the Australian state or territory containing the LGA",
            dirc=STATE_DIR,
            obs=(
                "Taken from the components data cube; where absent it is the leading digit of the LGA "
                "code, which is the state digit in the ASGS LGA coding scheme"
            ),
            orig="S/T code",
        ),
        col(
            "erp",
            "INT64",
            "Estimated resident population at 30 June",
            unit="person",
            orig="ERP at 30 June",
        ),
        *components(),
        *area_density("lga"),
    ],
    "series": [
        col(
            "series_id",
            "STRING",
            "ABS series identifier uniquely identifying a time series across ABS releases",
            obs="Primary key; stable ABS-wide identifier such as A2133244X",
            orig="Series ID",
        ),
        col(
            "description",
            "STRING",
            "Full description of the data item as published by the ABS",
            orig="Data Item Description",
        ),
        col(
            "unit",
            "STRING",
            "Unit of measure of the series as published by the ABS",
            obs="Persons, Number, 000 or Percent",
            orig="Unit",
        ),
        col(
            "frequency",
            "STRING",
            "Publication frequency of the series, Quarter or Annual",
            orig="Frequency",
        ),
        col(
            "source_catalogue",
            "STRING",
            "ABS catalogue number the series is published under",
            obs="3101.0 for the population estimates and 3222.0 for the projections",
            orig="Index",
        ),
        col(
            "source_table",
            "STRING",
            "Title of the source ABS table the series is published in",
            orig="Index",
        ),
        col(
            "series_start",
            "DATE",
            "Date of the first observation of the series",
            orig="Series Start",
        ),
        col(
            "series_end",
            "DATE",
            "Date of the last observation of the series",
            orig="Series End",
        ),
    ],
}

os.makedirs(OUT, exist_ok=True)
for table, cols in SPEC.items():
    declared = [c["name"] for c in cols]
    expected = constants.COLUMNS.value[table]
    assert declared == expected, (
        f"{table}: architecture {declared} != COLUMNS {expected}"
    )
    with open(f"{OUT}/{table}.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=HDR, lineterminator="\n")
        w.writeheader()
        w.writerows(cols)
    print(f"{table}.csv: {len(cols)} columns")
