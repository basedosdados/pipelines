"""Build the per-table auxiliary-file bundles for world_noaa_ghcn.

    python models/world_noaa_ghcn/code/build_auxiliary_files.py

GHCN-Daily is unusable without its codebook: the value column is meaningless
until you know that TMAX is tenths of a degree and SNOW is whole millimetres,
and the three flag columns are opaque single characters. That content is also
encoded in the `dicionario` table, which is the primary route for a user; these
bundles carry the source documents themselves.

Per .claude/rules/auxiliary-files.md, bundles are per TABLE and hold only what a
user of that table needs. `dicionario` gets no bundle — it is itself the decoded
form of the documentation.
"""

from __future__ import annotations

import os
import zipfile
from datetime import date
from pathlib import Path

DATA = Path(
    os.environ.get(
        "GHCN_DATA_DIR", os.path.expanduser("~/Downloads/world_noaa_ghcn_data")
    )
)
INPUT = DATA / "input"
BUNDLES = DATA / "auxiliary_files"
BASE = "https://www.ncei.noaa.gov/pub/data/ghcn/daily"

CITATION = (
    "Menne, M.J., I. Durre, B. Korzeniewski, S. McNeill, K. Thomas, X. Yin, "
    "S. Anthony, R. Ray, R.S. Vose, B.E. Gleason and T.G. Houston, 2012: "
    "Global Historical Climatology Network - Daily (GHCN-Daily), Version 3.34. "
    "NOAA National Centers for Environmental Information. "
    "doi:10.7289/V5D21VHZ. Accessed {accessed}.\n\n"
    "Publications using this dataset should also cite:\n"
    "Menne, M.J., I. Durre, R.S. Vose, B.E. Gleason and T.G. Houston, 2012: "
    "An Overview of the Global Historical Climatology Network-Daily Database. "
    "Journal of Atmospheric and Oceanic Technology, 29, 897-910. "
    "doi:10.1175/JTECH-D-11-00103.1"
)

# file -> (bundled name, what it is)
DOCS = {
    "readme.txt": (
        "ghcnd_readme.txt",
        "The GHCN-Daily readme, version 3.34. Section III defines every element "
        "code and its units and the measurement, quality and source flag "
        "tables; section IV defines the station metadata format.",
    ),
    "readme-by_year.txt": (
        "ghcnd_readme_by_year.txt",
        "Field definitions for the by_year CSV files this table was built from.",
    ),
    "status.txt": (
        "ghcnd_status.txt",
        "NCEI's version history for GHCN-Daily, recording what each version "
        "added and when.",
    ),
    "ghcnd-countries.txt": (
        "ghcnd_countries.txt",
        "FIPS country code to country name, used to populate country_name.",
    ),
    "ghcnd-states.txt": (
        "ghcnd_states.txt",
        "US and Canadian state or province postal code to name, used to "
        "populate state_name.",
    ),
}

BUNDLE_CONTENTS = {
    "observation": ["readme.txt", "readme-by_year.txt", "status.txt"],
    "station": ["readme.txt", "ghcnd-countries.txt", "ghcnd-states.txt"],
    "station_element_inventory": ["readme.txt", "status.txt"],
}

NOTES = {
    "observation": (
        "Values in this table have already been converted to the standard unit "
        "named per row in `measurement_unit`, so the scaling factors in section "
        "III of the readme have been applied and must not be applied again. "
        "PRCP arrives from the source in tenths of a millimetre and TMAX/TMIN "
        "in tenths of a degree Celsius, but SNOW and SNWD already arrive in "
        "whole millimetres.\n\n"
        "The source's -9999 missing sentinel was dropped rather than loaded.\n\n"
        "Rows that failed NCEI's quality assurance were KEPT. A non-null "
        "`quality_flag` names the check the value failed; filter on "
        "`quality_flag IS NULL` to use only values that passed.\n\n"
        "For 28 of the 144 elements the stored value is not a measurable "
        "quantity and `measurement_unit` is null: FMTM and PGTM hold a clock "
        "time in HHMM, and every WT** and WV** element is an occurrence "
        "indicator whose value is always 1."
    ),
    "station": (
        "The source's -999.9 elevation sentinel was converted to null, which "
        "affects 4,619 of the 132,501 stations.\n\n"
        "`country_code` and `network_code` are derived from the station "
        "identifier itself: characters 1-2 and character 3 respectively."
    ),
    "station_element_inventory": (
        "This table covers all 144 elements GHCN-Daily records. `first_year` "
        "and `last_year` are the first and last year of UNFLAGGED data, so a "
        "station may hold flagged observations outside that range."
    ),
}


def build(table: str) -> Path:
    """Build one table's documentation bundle.

    Args:
        table: Table slug to build a bundle for.

    Returns:
        Path to the written ``auxiliary_files.zip``.
    """
    files = BUNDLE_CONTENTS[table]
    accessed = date.today().isoformat()
    readme = [
        f"# Auxiliary files - world_noaa_ghcn.{table}",
        "",
        "## How to cite",
        "",
        CITATION.format(accessed=accessed),
        "",
        "## Contents",
        "",
    ]
    for src in files:
        name, what = DOCS[src]
        readme += [
            f"### {name}",
            "",
            what,
            "",
            f"- Source: {BASE}/{src}",
            f"- Downloaded: {accessed}",
            "",
        ]
    readme += [
        "## What was changed when loading this table",
        "",
        NOTES[table],
        "",
        "## Not bundled",
        "",
        "The full station-by-station `.dly` archive "
        f"({BASE}/ghcnd_all.tar.gz, several gigabytes) is left at the "
        "publisher. The element, flag and network code tables from the readme "
        "are also available decoded in this dataset's `dicionario` table, "
        "which is the easier route for most users.",
        "",
    ]

    BUNDLES.mkdir(parents=True, exist_ok=True)
    out = BUNDLES / table / "auxiliary_files.zip"
    out.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("README.md", "\n".join(readme))
        for src in files:
            z.write(INPUT / src, DOCS[src][0])
    return out


def main() -> None:
    """Build every table's auxiliary-file bundle."""
    for table in BUNDLE_CONTENTS:
        p = build(table)
        print(f"{table}: {p} ({p.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
