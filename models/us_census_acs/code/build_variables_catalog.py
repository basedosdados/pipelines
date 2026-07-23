#!/usr/bin/env python3
"""Build the `variables` catalog for ACS data profiles from vars_*.json.gz.

One row per estimate code (DPxx_NNNNE and DPxx_NNNNPE) that the melt emits.
Columns (all STRING): variable_code, profile, profile_name, line_number, label,
concept, universe, unit, is_percent. Most-recent year's label/concept wins.
Output: output/variables/data.parquet  (unpartitioned reference table).
"""

import glob
import gzip
import json
import os
import re

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PROF = "/Users/rdahis/acs_data/profiles"
OUT = "/Users/rdahis/acs_data/output/variables"
EST_RE = re.compile(r"^DP\d{2}_\d{3,4}P?E$")
PROFILE_NAME = {
    "DP02": "Social characteristics",
    "DP03": "Economic characteristics",
    "DP04": "Housing characteristics",
    "DP05": "Demographic and housing estimates",
}


def clean_label(lbl):
    parts = [p for p in lbl.split("!!") if p]
    if parts and parts[0] in (
        "Estimate",
        "Percent",
        "Number",
        "Percent Margin of Error",
        "Margin of Error",
    ):
        parts = parts[1:]
    return " - ".join(parts)


def unit_of(label, is_pct):
    low = label.lower()
    if is_pct:
        return "percent"
    if "(dollars)" in low or ("income" in low and "dollars" in low):
        return "USD"
    if "median age" in low:
        return "years"
    if "ratio" in low:
        return "ratio"
    return "count"


def line_no(code):
    m = re.search(r"_(\d{3,4})P?E$", code)
    return m.group(1) if m else ""


rows = {}
years_seen = {}
for path in sorted(glob.glob(f"{PROF}/vars_*.json.gz")):
    yr = int(re.search(r"_(\d{4})\.json", path).group(1))
    with gzip.open(path, "rt") as fh:
        v = json.load(fh)["variables"]
    for code, info in v.items():
        if not EST_RE.match(code):
            continue
        # keep the most-recent year's metadata
        if code in years_seen and years_seen[code] >= yr:
            continue
        years_seen[code] = yr
        label = info.get("label", "")
        is_pct = (
            "yes"
            if (code.endswith("PE") or label.startswith("Percent"))
            else "no"
        )
        rows[code] = {
            "variable_code": code,
            "profile": info.get("group", code.split("_")[0]),
            "profile_name": PROFILE_NAME.get(info.get("group", ""), ""),
            "line_number": line_no(code),
            "label": clean_label(label),
            "concept": info.get("concept", ""),
            "universe": "",
            "unit": unit_of(label, is_pct == "yes"),
            "is_percent": is_pct,
        }

df = pd.DataFrame(
    sorted(rows.values(), key=lambda r: r["variable_code"])
).astype("string")
os.makedirs(OUT, exist_ok=True)
schema = pa.schema([(c, pa.string()) for c in df.columns])
pq.write_table(
    pa.Table.from_pandas(df, schema=schema, preserve_index=False),
    f"{OUT}/data.parquet",
    compression="snappy",
)
print(f"variables catalog: {len(df)} codes -> {OUT}/data.parquet")
print("by profile:", df.profile.value_counts().to_dict())
print("by unit:", df.unit.value_counts().to_dict())
print(df.head(4).to_string())
