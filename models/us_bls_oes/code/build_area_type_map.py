"""Build the area code to area type lookup used to clean the 2005-2010 releases.

Those releases split the metropolitan estimates across `MSA_*`, `aMSA_*` and
`BOS_*` workbooks without the `area_type` field, so metropolitan divisions are
indistinguishable from metropolitan statistical areas by code alone. Divisions
nest inside their parent MSA, so conflating them would double-count any sum over
the area type. The 2011-2013 releases do carry `area_type` and use the same CBSA
delineations, so they are pooled into a static lookup.

The 2003-2004 releases use the pre-2003 OMB 4-digit MSA/PMSA codes, a different
code system entirely; they are not covered here and fall back to area_type 4.

Run: uv run python models/us_bls_oes/code/build_area_type_map.py
"""

import csv
import os
from pathlib import Path

from pipelines.datasets.us_bls_oes.constants import constants
from pipelines.datasets.us_bls_oes.utils import _code, _members, _read_sheet

INPUT_DIR = Path(
    os.environ.get(
        "OES_INPUT_DIR", Path.home() / "Downloads/us_bls_oes_data/input"
    )
)
SOURCE_YEARS = [2011, 2012, 2013]


def main():
    out = constants.AREA_TYPE_MAP.value
    area_type, area_name = {}, {}
    for year in SOURCE_YEARS:
        path = INPUT_DIR / f"oesm{year % 100:02d}all.zip"
        df = _read_sheet(path, _members(path)[0])
        ids = _code(df["area_id"])
        types = _code(df["area_type"])
        for a, t, n in zip(ids, types, df["area_name"], strict=True):
            if a is not None and a not in area_type:
                area_type[a], area_name[a] = t, str(n)
    with open(out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["area_id", "area_type", "area_name"])
        for a in sorted(area_type):
            w.writerow([a, area_type[a], area_name[a]])
    print(f"{out}: {len(area_type)} areas from {SOURCE_YEARS}")


if __name__ == "__main__":
    main()
