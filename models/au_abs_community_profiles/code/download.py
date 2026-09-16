"""Download ABS Census DataPacks (short-header) and extract to input/packs/.

Products: GCP 2021, GCP 2016, BCP 2011 (the 2011 GCP-equivalent), TSP 2021.
Geographies given on the command line (tokens); missing (profile, geo) combos
404 and are skipped.

Run:  python download.py STE SA4 SA3 GCCSA LGA CED SED AUS   # small subset
      python download.py SA2 SAL POA                          # medium
      python download.py SA1                                  # the big one
      python download.py ALL
"""

import io
import os
import sys
import zipfile

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
DS = os.path.dirname(HERE)
PACKS = os.path.join(DS, "input", "packs")
BASE = "https://www.abs.gov.au/census/find-census-data/datapacks/download/"
UA = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

# (year, profile token, region token) — region is part of the zip filename
PRODUCTS = [
    (2021, "GCP", "AUS"),
    (2016, "GCP", "AUS"),
    (2011, "BCP", "AUST"),
    (2021, "TSP", "AUS"),
]
# geographies each product publishes (superset; 404s are skipped)
GEO_GCP = [
    "AUS",
    "STE",
    "SA1",
    "SA2",
    "SA3",
    "SA4",
    "GCCSA",
    "LGA",
    "SAL",
    "POA",
    "CED",
    "SED",
]
GEO_TSP = ["AUS", "STE", "SA2", "SA3", "SA4", "GCCSA", "LGA"]
GEO_BY_PROFILE = {"GCP": GEO_GCP, "BCP": GEO_GCP, "TSP": GEO_TSP}
ALL_GEOS = GEO_GCP


def fetch(year, prof, region, geo):
    name = f"{year}_{prof}_{geo}_for_{region}_short-header"
    dest = os.path.join(PACKS, name)
    if os.path.isdir(dest) and any(
        f.endswith(".csv") for _r, _d, fs in os.walk(dest) for f in fs
    ):
        print(f"SKIP {name} (already extracted)")
        return True
    url = BASE + name + ".zip"
    r = requests.get(url, headers=UA, timeout=600)
    if r.status_code == 404:
        print(f"404  {name} (not published)")
        return False
    r.raise_for_status()
    if r.content[:2] != b"PK":
        print(f"BAD  {name}: not a zip ({len(r.content)} bytes)")
        return False
    os.makedirs(dest, exist_ok=True)
    zipfile.ZipFile(io.BytesIO(r.content)).extractall(dest)
    mb = len(r.content) / 1e6
    print(f"OK   {name}  ({mb:.1f} MB)")
    return True


def main():
    args = [a.upper() for a in sys.argv[1:]] or ["STE"]
    geos = ALL_GEOS if args == ["ALL"] else args
    os.makedirs(PACKS, exist_ok=True)
    got = 0
    for year, prof, region in PRODUCTS:
        for geo in GEO_BY_PROFILE[prof]:
            if geo not in geos:
                continue
            if fetch(year, prof, region, geo):
                got += 1
    print(f"\n{got} packs present under {os.path.relpath(PACKS, DS)}")


if __name__ == "__main__":
    main()
