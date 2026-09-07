"""Report the distinct values taken by each WMO-coded SYNOP column."""

import collections
import glob
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from schema_map import SYNOP_COLUMNS

INPUT = os.path.expanduser(
    os.environ.get("MF_INPUT", "~/Downloads/fr_meteofrance_data/input")
)
CODED = [
    (src, tgt) for src, tgt, _t, _u, is_dict, _d in SYNOP_COLUMNS if is_dict
]

seen = collections.defaultdict(collections.Counter)
total = 0
for path in sorted(glob.glob(os.path.join(INPUT, "synop_*.csv.gz"))):
    df = pd.read_csv(
        path,
        sep=";",
        dtype=str,
        usecols=[c for c, _ in CODED],
        low_memory=False,
    )
    total += len(df)
    for src, tgt in CODED:
        seen[tgt].update(
            df[src].dropna().str.strip().replace("", pd.NA).dropna()
        )

print("rows scanned:", total)
for _src, tgt in CODED:
    vals = seen[tgt]
    keys = sorted(vals, key=lambda k: (len(k), k))
    print(f"\n{tgt}: {len(keys)} distinct, {sum(vals.values())} non-null")
    print("  ", keys)
