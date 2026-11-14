"""Paths, source files and lookup tables for us_stanford_open_policing.

Scratch data never lives in the repo or under Dropbox. It defaults to
``~/Library/Caches/us_stanford_open_policing_data`` and can be moved with
``US_STANFORD_OPEN_POLICING_DATA_DIR``.
"""

import os
from pathlib import Path

DATASET_ID = "us_stanford_open_policing"
BILLING_PROJECT = "basedosdados-dev"

DATA_DIR = Path(
    os.environ.get(
        "US_STANFORD_OPEN_POLICING_DATA_DIR",
        Path.home() / "Library" / "Caches" / "us_stanford_open_policing_data",
    )
)
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

CODE_DIR = Path(__file__).resolve().parent
ARCHITECTURE_DIR = CODE_DIR / "architecture"
SOURCE_URLS = [
    line.strip()
    for line in (CODE_DIR / "source_urls.txt").read_text().splitlines()
    if line.strip()
]

# File slug -> (place name as the project's README names it, release date).
# "Statewide" files come from the state patrol; every other file from a city
# police department. The 2023 release (druid wb225bk3255) refreshed 12
# locations; the rest are the 2020-04-01 release (druid yg821jf8611). No
# location appears in both.
JURISDICTIONS = {
    "az_mesa": ("Mesa", "2023-01-26"),
    "ca_statewide": ("Statewide", "2023-01-26"),
    "co_aurora": ("Aurora", "2023-01-26"),
    "il_chicago": ("Chicago", "2023-01-26"),
    "ks_wichita": ("Wichita", "2023-01-26"),
    "ky_louisville": ("Louisville", "2023-01-26"),
    "mt_statewide": ("Statewide", "2023-01-26"),
    "ok_oklahoma_city": ("Oklahoma City", "2023-01-26"),
    "tx_houston": ("Houston", "2023-01-26"),
    "tx_san_antonio": ("San Antonio", "2023-01-26"),
    "vt_burlington": ("Burlington", "2023-01-26"),
    "wi_madison": ("Madison", "2023-01-26"),
    "ar_little_rock": ("Little Rock", "2020-04-01"),
    "az_gilbert": ("Gilbert", "2020-04-01"),
    "az_statewide": ("Statewide", "2020-04-01"),
    "ca_anaheim": ("Anaheim", "2020-04-01"),
    "ca_bakersfield": ("Bakersfield", "2020-04-01"),
    "ca_long_beach": ("Long Beach", "2020-04-01"),
    "ca_los_angeles": ("Los Angeles", "2020-04-01"),
    "ca_oakland": ("Oakland", "2020-04-01"),
    "ca_san_bernardino": ("San Bernardino", "2020-04-01"),
    "ca_san_diego": ("San Diego", "2020-04-01"),
    "ca_san_francisco": ("San Francisco", "2020-04-01"),
    "ca_san_jose": ("San Jose", "2020-04-01"),
    "ca_santa_ana": ("Santa Ana", "2020-04-01"),
    "ca_stockton": ("Stockton", "2020-04-01"),
    "co_denver": ("Denver", "2020-04-01"),
    "co_statewide": ("Statewide", "2020-04-01"),
    "ct_hartford": ("Hartford", "2020-04-01"),
    "ct_statewide": ("Statewide", "2020-04-01"),
    "fl_saint_petersburg": ("Saint Petersburg", "2020-04-01"),
    "fl_statewide": ("Statewide", "2020-04-01"),
    "fl_tampa": ("Tampa", "2020-04-01"),
    "ga_statewide": ("Statewide", "2020-04-01"),
    "ia_statewide": ("Statewide", "2020-04-01"),
    "id_idaho_falls": ("Idaho Falls", "2020-04-01"),
    "il_statewide": ("Statewide", "2020-04-01"),
    "in_fort_wayne": ("Fort Wayne", "2020-04-01"),
    "ky_owensboro": ("Owensboro", "2020-04-01"),
    "la_new_orleans": ("New Orleans", "2020-04-01"),
    "ma_statewide": ("Statewide", "2020-04-01"),
    "md_baltimore": ("Baltimore", "2020-04-01"),
    "md_statewide": ("Statewide", "2020-04-01"),
    "mi_statewide": ("Statewide", "2020-04-01"),
    "mn_saint_paul": ("Saint Paul", "2020-04-01"),
    "mo_statewide": ("Statewide", "2020-04-01"),
    "ms_statewide": ("Statewide", "2020-04-01"),
    "nc_charlotte": ("Charlotte", "2020-04-01"),
    "nc_durham": ("Durham", "2020-04-01"),
    "nc_fayetteville": ("Fayetteville", "2020-04-01"),
    "nc_greensboro": ("Greensboro", "2020-04-01"),
    "nc_raleigh": ("Raleigh", "2020-04-01"),
    "nc_statewide": ("Statewide", "2020-04-01"),
    "nc_winston-salem": ("Winston-Salem", "2020-04-01"),
    "nd_grand_forks": ("Grand Forks", "2020-04-01"),
    "nd_statewide": ("Statewide", "2020-04-01"),
    "ne_statewide": ("Statewide", "2020-04-01"),
    "nh_statewide": ("Statewide", "2020-04-01"),
    "nj_camden": ("Camden", "2020-04-01"),
    "nj_statewide": ("Statewide", "2020-04-01"),
    "nv_henderson": ("Henderson", "2020-04-01"),
    "nv_statewide": ("Statewide", "2020-04-01"),
    "ny_albany": ("Albany", "2020-04-01"),
    "ny_statewide": ("Statewide", "2020-04-01"),
    "oh_cincinnati": ("Cincinnati", "2020-04-01"),
    "oh_columbus": ("Columbus", "2020-04-01"),
    "oh_statewide": ("Statewide", "2020-04-01"),
    "ok_tulsa": ("Tulsa", "2020-04-01"),
    "or_statewide": ("Statewide", "2020-04-01"),
    "pa_philadelphia": ("Philadelphia", "2020-04-01"),
    "ri_statewide": ("Statewide", "2020-04-01"),
    "sc_statewide": ("Statewide", "2020-04-01"),
    "sd_statewide": ("Statewide", "2020-04-01"),
    "tn_nashville": ("Nashville", "2020-04-01"),
    "tn_statewide": ("Statewide", "2020-04-01"),
    "tx_arlington": ("Arlington", "2020-04-01"),
    "tx_austin": ("Austin", "2020-04-01"),
    "tx_garland": ("Garland", "2020-04-01"),
    "tx_lubbock": ("Lubbock", "2020-04-01"),
    "tx_plano": ("Plano", "2020-04-01"),
    "tx_statewide": ("Statewide", "2020-04-01"),
    "va_statewide": ("Statewide", "2020-04-01"),
    "vt_statewide": ("Statewide", "2020-04-01"),
    "wa_seattle": ("Seattle", "2020-04-01"),
    "wa_statewide": ("Statewide", "2020-04-01"),
    "wa_tacoma": ("Tacoma", "2020-04-01"),
    "wi_statewide": ("Statewide", "2020-04-01"),
    "wy_statewide": ("Statewide", "2020-04-01"),
}

STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "DC": "11", "FL": "12", "GA": "13", "HI": "15",
    "ID": "16", "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21",
    "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
    "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46",
    "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
    "WV": "54", "WI": "55", "WY": "56",
}  # fmt: skip


def source_file(slug: str) -> Path:
    """Local path of the downloaded CSV archive for one jurisdiction."""
    for url in SOURCE_URLS:
        name = url.rsplit("/", 1)[1]
        if f"_{slug}_" in name:
            return INPUT_DIR / name
    raise KeyError(slug)


def source_url(slug: str) -> str:
    return next(u for u in SOURCE_URLS if f"_{slug}_" in u.rsplit("/", 1)[1])
