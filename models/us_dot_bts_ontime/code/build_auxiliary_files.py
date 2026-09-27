"""Build the per-table auxiliary-file bundles for us_dot_bts_ontime.

Per `auxiliary-files`, a bundle holds the documents a user of *that table* needs
in hand. Here that is the BTS record layout (which defines all 109 published
fields) and the lookup tables that decode the categorical columns.

    uv run --no-project python models/us_dot_bts_ontime/code/build_auxiliary_files.py

Upload with:
    gsutil cp <bundle> gs://basedosdados/auxiliary_files/us_dot_bts_ontime/<table>/auxiliary_files.zip
"""

from __future__ import annotations

import os
import shutil
import sys
import zipfile
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_dot_bts_ontime.utils import (
    DICIONARIO_SOURCES,
    read_readme,
)

DATA = Path(
    os.environ.get(
        "BTS_DATA_DIR", Path.home() / "Downloads" / "us_dot_bts_ontime_data"
    )
)
LOOKUPS = DATA / "input" / "lookups"
RAW = DATA / "input" / "monthly"
OUT = DATA / "auxiliary_files"

SOURCE = "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGJ"
LOOKUP_URL = "https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72={key}"

CITATION = (
    "Bureau of Transportation Statistics, United States Department of Transportation. "
    '"Reporting Carrier On-Time Performance (1987-present)", TranStats. '
    "Public domain (17 U.S.C. Sec. 105)."
)

# Which lookups belong in which table's bundle.
FLIGHT_LOOKUPS = sorted(set(DICIONARIO_SOURCES.values()))
BUNDLES = {
    "flight": {"readme": True, "lookups": FLIGHT_LOOKUPS},
    "airport": {
        "readme": False,
        "lookups": ["L_AIRPORT_ID", "L_AIRPORT", "L_STATE_ABR_AVIATION"],
    },
    "dicionario": {"readme": False, "lookups": sorted(set(FLIGHT_LOOKUPS))},
}


def bundle_readme(table: str, files: list[str], has_layout: bool) -> str:
    today = date.today().isoformat()
    lines = [
        f"# Auxiliary files — us_dot_bts_ontime.{table}",
        "",
        "## Citation",
        "",
        CITATION,
        "",
        "## Contents",
        "",
    ]
    if has_layout:
        lines += [
            "- `record_layout.html` — the BTS record layout shipped inside every monthly",
            "  archive. Defines all 109 published fields in the order they appear, and is",
            f"  the authoritative field documentation for this table. Downloaded {today}",
            f"  from {SOURCE}",
            "",
        ]
    lines.append("Lookup tables, each a `Code,Description` CSV:")
    lines.append("")
    for name in files:
        lines.append(f"- `{name}.csv` — downloaded {today} from")
        lines.append(f"  `{LOOKUP_URL.format(key='<rot13 of ' + name + '>')}`")
    lines += [
        "",
        "## Reading the table",
        "",
        "Three things are not obvious from the data alone:",
        "",
        "1. **Clock fields are STRING, not numbers.** `scheduled_departure_time` and the",
        "   other HHMM fields are clock labels kept exactly as published, so `0937` keeps",
        "   its leading zero. Derived `*_local` TIME columns are provided for arithmetic.",
        "   `2400` in the source means midnight ending the day and becomes `00:00:00`.",
        "2. **Only scheduled departure has a datetime.** The flight date is the scheduled",
        "   departure date in *origin* local time; arrival clocks are in *destination*",
        "   local time and may fall on the next day. The source carries no timezone, so",
        "   no arrival datetime is derived.",
        "3. **Empty is not zero.** Delay attribution (carrier, weather, NAS, security,",
        "   late aircraft) exists only from June 2003, and the diversion columns only",
        "   from 2008. Before those dates the columns are null because the field was not",
        "   collected, not because there was no delay.",
        "",
        "## Link only",
        "",
        "- BTS on-time performance landing page and field descriptions:",
        f"  {SOURCE}",
        "- Airline On-Time Statistics documentation:",
        "  https://www.bts.gov/topics/airlines-and-airports/understanding-reporting-causes-flight-delays-and-cancellations",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    layout = None
    for p in sorted(RAW.glob("ontime_*.zip"), reverse=True):
        layout = read_readme(p)
        if layout:
            break
    if layout is None:
        raise SystemExit("no record layout found in any monthly archive")

    for table, spec in BUNDLES.items():
        staging = OUT / table
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir(parents=True)
        if spec["readme"]:
            (staging / "record_layout.html").write_bytes(layout)
        present = []
        for name in spec["lookups"]:
            src = LOOKUPS / f"{name}.csv"
            if src.exists():
                shutil.copy(src, staging / f"{name}.csv")
                present.append(name)
        (staging / "README.md").write_text(
            bundle_readme(table, present, spec["readme"]),
            encoding="utf-8",
        )
        zpath = OUT / f"{table}_auxiliary_files.zip"
        with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as z:
            for f in sorted(staging.iterdir()):
                z.write(f, f.name)
        print(
            f"{table}: {zpath.name} ({zpath.stat().st_size / 1e3:.0f} kB, "
            f"{len(list(staging.iterdir()))} files)"
        )


if __name__ == "__main__":
    main()
