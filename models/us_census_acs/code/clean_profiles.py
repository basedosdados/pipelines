#!/usr/bin/env python3
"""Melt ACS Data Profile JSON -> long parquet, one table per geography level.

Each Census group(DPxx) response is [header, *rows]. We melt every estimate
column (DPxx_NNNNE and DPxx_NNNNPE) into a row with its MOE partner
(DPxx_NNNNM / DPxx_NNNNPM), dropping *EA/*MA/*PEA/*PMA annotation columns.
MOE/est sentinels (-555555555 etc.) -> null.

Output: output/data_profile_<level>/ano=<y>/<period>.parquet
Usage: python3 clean_profiles.py <level> [<level> ...]   (default: all levels)
"""

import glob
import gzip
import json
import os
import re
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PROF = "/Users/rdahis/acs_data/profiles"
OUTROOT = "/Users/rdahis/acs_data/output"
EST_RE = re.compile(r"^DP\d{2}_\d{3,4}P?E$")  # DPxx_NNNNE or DPxx_NNNNPE
SENTINEL = {
    "-555555555",
    "-666666666",
    "-222222222",
    "-333333333",
    "-888888888",
    "-999999999",
    "-111111111",
    "*",
    "**",
    "-",
    "N",
    "(X)",
    "null",
    "",
}
LEVELS = [
    "nation",
    "state",
    "county",
    "place",
    "puma",
    "cbsa",
    "congressional_district",
    "zcta",
    "school_district",
]


def num(v):
    if v is None or v in SENTINEL:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f <= -100000000 else f


def geo_ids(level, g):
    st = g.get("state")
    if level == "nation":
        return {"id_pais": "US"}
    if level == "state":
        return {"id_state": st}
    if level == "county":
        return {"id_state": st, "id_county": st + g["county"]}
    if level == "place":
        return {"id_state": st, "id_place": st + g["place"]}
    if level == "puma":
        return {"id_state": st, "puma": g["public use microdata area"]}
    if level == "cbsa":
        return {
            "id_cbsa": g[
                "metropolitan statistical area/micropolitan statistical area"
            ]
        }
    if level == "congressional_district":
        return {
            "id_state": st,
            "id_congressional_district": st + g["congressional district"],
        }
    if level == "zcta":
        return {"id_zcta": g["zip code tabulation area"]}
    if level == "school_district":
        return {
            "id_state": st,
            "id_school_district": st + g["school district (unified)"],
        }
    return {}


def id_cols(level):
    return list(
        geo_ids(
            level,
            {
                "state": "",
                "county": "",
                "place": "",
                "public use microdata area": "",
                "metropolitan statistical area/micropolitan statistical area": "",
                "congressional district": "",
                "zip code tabulation area": "",
                "school district (unified)": "",
            },
        ).keys()
    )


def melt_file(path, level, ano, period, rows_out):
    with gzip.open(path, "rt") as f:
        data = json.load(f)
    header = data[0]
    idx = {c: i for i, c in enumerate(header)}
    est_cols = [c for c in header if EST_RE.match(c)]
    geo_names = [
        c
        for c in header
        if c == "state"
        or c
        in (
            "county",
            "place",
            "public use microdata area",
            "metropolitan statistical area/micropolitan statistical area",
            "congressional district",
            "zip code tabulation area",
            "school district (unified)",
        )
    ]
    for row in data[1:]:
        g = {gn: row[idx[gn]] for gn in geo_names}
        ids = geo_ids(level, g)
        for c in est_cols:
            mcol = c[:-1] + "M"
            est = num(row[idx[c]])
            moe = num(row[idx[mcol]]) if mcol in idx else None
            rec = {
                "period": period,
                **ids,  # ano is path-only (partition)
                "variable_code": c,
                "estimate": est,
                "margin_of_error": moe,
            }
            rows_out.append(rec)


def schema_for(level):
    # ano is the hive partition (path-only), not a column in the file
    fields = [("period", pa.string())]
    for c in id_cols(level):
        fields.append((c, pa.string()))
    fields += [
        ("variable_code", pa.string()),
        ("estimate", pa.float64()),
        ("margin_of_error", pa.float64()),
    ]
    return pa.schema(fields)


def process_level(level):
    schema = schema_for(level)
    # (period, year) -> list of source files (school_district has nationwide + per-state)
    tasks = {}
    for period_dir, plabel in [("acs1", "1-year"), ("acs5", "5-year")]:
        for yeardir in sorted(glob.glob(f"{PROF}/{period_dir}/*")):
            y = int(os.path.basename(yeardir))
            files = glob.glob(f"{yeardir}/{level}_DP0*.json.gz")
            if level == "school_district":
                files += glob.glob(f"{yeardir}/{level}-st*_DP0*.json.gz")
            if files:
                tasks[(plabel, y)] = sorted(set(files))
    names = [f.name for f in schema]
    for (plabel, y), files in sorted(tasks.items()):
        outdir = f"{OUTROOT}/data_profile_{level}/ano={y}"
        os.makedirs(outdir, exist_ok=True)
        outfile = f"{outdir}/{plabel}.parquet"
        if os.path.exists(outfile):
            print(f"  SKIP {level} {plabel} {y}")
            continue
        tmp = outfile + ".tmp"
        writer = None  # created lazily on first non-empty batch
        total = 0
        for fp in files:  # one row-group per source file -> bounded memory
            rows = []
            try:
                melt_file(fp, level, y, plabel, rows)
            except Exception as e:
                print(f"  !! {fp}: {e}")
                continue
            if not rows:
                continue
            df = pd.DataFrame(rows)
            for c in id_cols(level):
                df[c] = df[c].astype("string")
            df = df.reindex(columns=names)
            if writer is None:
                writer = pq.ParquetWriter(tmp, schema, compression="snappy")
            writer.write_table(
                pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            )
            total += len(df)
        if writer is not None:
            writer.close()
            os.replace(tmp, outfile)
            print(
                f"  OK   {level} {plabel} {y}: {total:,} rows ({len(files)} files)"
            )


if __name__ == "__main__":
    for lvl in sys.argv[1:] or LEVELS:
        print(f"[{lvl}]")
        process_level(lvl)
