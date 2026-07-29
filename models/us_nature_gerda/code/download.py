#!/usr/bin/env python3
"""Download phase-1 GERDA CSV files from the awiedem/german_election_data repo.

The CSVs are Git-LFS tracked; raw.githubusercontent returns only the pointer, so
we fetch through the `github.com/.../raw/...?download=` media URL, which 302s to
media.githubusercontent. Files land in ../input/<name>.csv.

Phase-1 scope (core vote-result modules): federal, state, municipal,
county-council (Kreistag), European. Plus the two crosswalk files used to build
the br_bd_diretorios_de geography directories.

Run:
    cd models/us_nature_gerda/code && python3 download.py [name ...]
(no args = download everything in the manifest)
"""

import os
import sys
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
INPUT = os.path.join(HERE, "..", "input")
BASE = (
    "https://github.com/awiedem/german_election_data/raw/refs/heads/main/data"
)

# name -> repo path under data/ (without .csv)
MANIFEST = {
    # --- Federal (Bundestag) ---
    "federal_muni_unharm": "federal_elections/municipality_level/final/federal_muni_unharm",
    "federal_muni_harm_21": "federal_elections/municipality_level/final/federal_muni_harm_21",
    "federal_muni_harm_25": "federal_elections/municipality_level/final/federal_muni_harm_25",
    "federal_cty_unharm": "federal_elections/county_level/final/federal_cty_unharm",
    "federal_cty_harm": "federal_elections/county_level/final/federal_cty_harm",
    "federal_wkr_unharm_long": "federal_elections/wahlkreis_level/final/federal_wkr_unharm_long",
    "federal_wkr_2021_on_2025": "federal_elections/wahlkreis_level/final/federal_wkr_2021_on_2025",
    # --- State (Landtag) ---
    "state_unharm": "state_elections/final/state_unharm",
    "state_harm_21": "state_elections/final/state_harm_21",
    "state_harm_23": "state_elections/final/state_harm_23",
    "state_harm_25": "state_elections/final/state_harm_25",
    "ltw_wkr_unharm_long": "state_elections/final/ltw_wkr_unharm_long",
    # --- Municipal (Gemeinderat) ---
    "municipal_unharm": "municipal_elections/final/municipal_unharm",
    "municipal_harm": "municipal_elections/final/municipal_harm",
    "municipal_harm_25": "municipal_elections/final/municipal_harm_25",
    # --- County council (Kreistag) ---
    "county_elec_unharm": "county_elections/final/county_elec_unharm",
    "county_elec_harm_21_muni": "county_elections/final/county_elec_harm_21_muni",
    "county_elec_harm_21_cty": "county_elections/final/county_elec_harm_21_cty",
    "county_council_seats": "county_elections/final/county_council_seats",
    # --- European (Europawahl) ---
    "european_muni_unharm": "european_elections/final/european_muni_unharm",
    "european_muni_harm": "european_elections/final/european_muni_harm",
    # --- Crosswalks (source for br_bd_diretorios_de geography) ---
    "ags_crosswalks": "crosswalks/final/ags_crosswalks",
    "cty_crosswalks": "crosswalks/final/cty_crosswalks",
}


def download(name, path):
    url = f"{BASE}/{path}.csv?download="
    dest = os.path.join(INPUT, f"{name}.csv")
    req = urllib.request.Request(url, headers={"User-Agent": "curl/8"})
    with urllib.request.urlopen(req) as resp, open(dest, "wb") as fh:
        total = 0
        while True:
            chunk = resp.read(1 << 20)
            if not chunk:
                break
            fh.write(chunk)
            total += len(chunk)
    # quick line count (rows = lines - 1 header)
    with open(dest, "rb") as fh:
        rows = sum(1 for _ in fh) - 1
    print(f"  {name:32s} {total / 1e6:8.1f} MB  {rows:>9,} rows")
    return total, rows


def main():
    os.makedirs(INPUT, exist_ok=True)
    names = sys.argv[1:] or list(MANIFEST)
    print(f"Downloading {len(names)} file(s) to {os.path.relpath(INPUT)} ...")
    for name in names:
        if name not in MANIFEST:
            print(f"  !! unknown: {name}")
            continue
        download(name, MANIFEST[name])
    print("done.")


if __name__ == "__main__":
    main()
