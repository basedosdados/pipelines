"""Write the architecture CSVs for us_meta_sci (one per table).

Usage:
    uv run --no-sync python models/us_meta_sci/code/build_architecture.py

The CSVs under ``architecture/`` are the source of truth for column order, types
and descriptions. The dbt models and the cleaning code follow them.
"""

import csv
from pathlib import Path

OUT = Path(__file__).parent / "architecture"
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
PAIS = "br_bd_diretorios_mundo.pais:sigla_iso2"
COUNTRY_OBS = (
    "ISO 3166-1 alpha-2 code as published by Meta. Kosovo is coded XK, a "
    "user-assigned code outside ISO 3166-1. Namibia is coded NA"
)
SCI_OBS = (
    "Dimensionless relative index: SCI(i,j) = friendships(i,j) / (users(i) x "
    "users(j)), rescaled by Meta to lie between 1 and 1,000,000,000 within each "
    "source file. Ratios are comparable within a table (and within a level), not "
    "across tables. Symmetric: SCI(i,j) = SCI(j,i)"
)


def sci(scope: str, pair: str | None = None, obs: str = SCI_OBS) -> list:
    pair = pair or f"the user {scope} and the friend {scope}"
    return [
        "scaled_sci",
        "INT64",
        f"Scaled Social Connectedness Index between {pair}, proportional to the "
        "probability that a Facebook user in the first is friends with a Facebook "
        "user in the second",
        "",
        "no",
        "",
        "index",
        "no",
        obs,
        "scaled_sci",
    ]


def country(side: str) -> list:
    return [
        f"{side}_country_id",
        "STRING",
        f"ISO 3166-1 alpha-2 code of the {side}'s country",
        "",
        "no",
        PAIS,
        "",
        "no",
        COUNTRY_OBS,
        f"{side}_country",
    ]


def region(side: str, what: str, obs: str, directory: str = "") -> list:
    return [
        f"{side}_region_id",
        "STRING",
        f"Identifier of the {side}'s {what}",
        "",
        "no",
        directory,
        "",
        "no",
        obs,
        f"{side}_region",
    ]


GADM = lambda lvl: (  # noqa: E731
    f"GADM level-{lvl} GID as published by Meta (e.g. "
    + ("KOR.15_1" if lvl == 1 else "BRA.25.564_1")
    + "). Data Basis has no GADM directory yet"
)
GEOB = lambda lvl: (  # noqa: E731
    f"geoBoundaries ADM{lvl} shapeID (e.g. 66186276B64762166704956). Data Basis "
    "has no geoBoundaries directory yet"
)
ZCTA_OBS = (
    "10 of the 20,195 ZCTAs are absent from the 2020 ZCTA directory: 10281, 23806, "
    "30320, 40506, 83601, 84132, 85144, 85288, 92878, 98205"
)
NUTS_OBS = (
    "Eurostat NUTS 2024 code (e.g. FRC, EL5). Codes for EU candidate and EFTA "
    "countries follow the NUTS 2024 statistical regions. Data Basis has no NUTS "
    "directory yet"
)

TABLES = {
    "country": [country("user"), country("friend"), sci("country")],
    "gadm1": [
        country("user"),
        country("friend"),
        region("user", "GADM level-1 region", GADM(1)),
        region("friend", "GADM level-1 region", GADM(1)),
        sci("region"),
    ],
    "gadm2": [
        country("user"),
        country("friend"),
        region("user", "GADM level-2 region", GADM(2)),
        region("friend", "GADM level-2 region", GADM(2)),
        sci("region"),
    ],
    "geoboundaries_adm1": [
        country("user"),
        country("friend"),
        region("user", "geoBoundaries ADM1 region", GEOB(1)),
        region("friend", "geoBoundaries ADM1 region", GEOB(1)),
        sci("region"),
    ],
    "geoboundaries_adm2": [
        country("user"),
        country("friend"),
        region("user", "geoBoundaries ADM2 region", GEOB(2)),
        region("friend", "geoBoundaries ADM2 region", GEOB(2)),
        sci("region"),
    ],
    "us_county": [
        [
            "user_county_id",
            "STRING",
            "Five-digit FIPS code of the user's US county",
            "",
            "no",
            "br_bd_diretorios_us.county:id_county",
            "",
            "no",
            "All rows are US-US pairs, so the source's constant country columns are dropped",
            "user_region",
        ],
        [
            "friend_county_id",
            "STRING",
            "Five-digit FIPS code of the friend's US county",
            "",
            "no",
            "br_bd_diretorios_us.county:id_county",
            "",
            "no",
            "",
            "friend_region",
        ],
        sci("county"),
    ],
    "us_zcta": [
        [
            "user_zcta_id",
            "STRING",
            "Five-digit code of the user's US ZIP Code Tabulation Area (ZCTA)",
            "",
            "no",
            "br_bd_diretorios_us.zcta_2020:id_zcta",
            "",
            "no",
            "All rows are US-US pairs, so the source's constant country columns are dropped. "
            + ZCTA_OBS,
            "user_region",
        ],
        [
            "friend_zcta_id",
            "STRING",
            "Five-digit code of the friend's US ZIP Code Tabulation Area (ZCTA)",
            "",
            "no",
            "br_bd_diretorios_us.zcta_2020:id_zcta",
            "",
            "no",
            ZCTA_OBS,
            "friend_region",
        ],
        sci("ZCTA"),
    ],
    "nuts_2024": [
        [
            "nuts_level",
            "STRING",
            "NUTS 2024 level of both regions in the pair: nuts1, nuts2 or nuts3",
            "",
            "no",
            "",
            "",
            "no",
            "Derived from the source file name (nuts1_2024.csv, nuts2_2024.csv, "
            "nuts3_2024.csv). Pairs never mix levels. scaled_sci is rescaled "
            "separately within each level",
            "",
        ],
        country("user"),
        country("friend"),
        region("user", "NUTS 2024 region", NUTS_OBS),
        region("friend", "NUTS 2024 region", NUTS_OBS),
        sci("region"),
    ],
    "region_to_country": [
        [
            "region_level",
            "STRING",
            "Geographic classification of the user's region: gadm1, gadm2, "
            "geoboundaries_adm1, geoboundaries_adm2, nuts1_2024, nuts2_2024, "
            "nuts3_2024, us_county or us_zcta",
            "",
            "no",
            "",
            "",
            "no",
            "Derived from the source file name (<level>_to_country.csv). Values match "
            "the slug of the region-to-region table with the same geography. "
            "scaled_sci is rescaled separately within each level",
            "",
        ],
        country("user"),
        [
            "user_region_id",
            "STRING",
            "Identifier of the user's region in the classification given by region_level",
            "",
            "no",
            "",
            "",
            "no",
            "GADM GID, geoBoundaries shapeID, NUTS 2024 code, county FIPS or ZCTA, "
            "depending on region_level. Not linked to a directory because the code "
            "system varies by row",
            "user_region",
        ],
        country("friend"),
        sci(
            "",
            "the user region and the friend country",
            SCI_OBS.split(". Symmetric")[0]
            + ". Directed region-to-country pairs only; the reverse direction is not "
            "published",
        ),
    ],
}

if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    for t, rows in TABLES.items():
        with open(OUT / f"{t}.csv", "w", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(HEADER)
            w.writerows(rows)
    print(f"wrote {len(TABLES)} architecture CSVs to {OUT}")
