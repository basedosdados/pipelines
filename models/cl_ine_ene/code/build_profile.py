#!/usr/bin/env python3
"""Per-column value profile, read from the cleaned parquet.

Reproduces what an earlier pass measured by re-reading all 197 source CSVs, in
about thirty seconds instead of forty-five minutes: the parquet holds the same
values, column-projected and compressed. build_architecture.py consumes the
result to decide types from evidence.

    python models/cl_ine_ene/code/build_profile.py
"""

from __future__ import annotations

import argparse
import collections
import gc
import json
import os
import pathlib
import re

import pyarrow.compute as pc
import pyarrow.parquet as pq

DATA = pathlib.Path(
    os.environ.get(
        "CL_INE_ENE_DATA", pathlib.Path.home() / "Downloads/cl_ine_ene_data"
    )
)
TABLE_DIR = DATA / "output" / "microdato"
HERE = pathlib.Path(__file__).resolve().parent

INTEGER = re.compile(r"^-?\d+$")
DECIMAL = re.compile(r"^-?\d+[.,]\d+$")
#: Beyond this many distinct values a column is a free-text or identifier field
#: and its value list stops being useful.
CAP = 400


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(HERE / "column_profile.json"))
    args = parser.parse_args()

    files = sorted(TABLE_DIR.glob("ano=*/mes=*/data.parquet"))
    if not files:
        raise SystemExit(
            f"no parquet under {TABLE_DIR} — run cl_ine_ene_clean.py --clean"
        )

    stats: dict[str, dict] = collections.defaultdict(
        lambda: {"rows": 0, "nonnull": 0, "values": set(), "over_cap": False}
    )
    for path in files:
        table = pq.read_table(path)
        for name in table.column_names:
            column = table[name]
            entry = stats[name]
            entry["rows"] += len(column)
            entry["nonnull"] += len(column) - column.null_count
            if not entry["over_cap"]:
                # pyarrow ships no type stub for its compute module.
                # pyrefly: ignore [missing-attribute]
                distinct = pc.unique(column).to_pylist()
                entry["values"].update(v for v in distinct if v is not None)
                if len(entry["values"]) > CAP:
                    entry["over_cap"] = True
                    entry["values"] = set()
        del table
        gc.collect()

    # The partition keys live in the path, not the file.
    for name, width in (("ano", 4), ("mes", 2)):
        values = {
            p.parts[-3 if name == "ano" else -2].split("=")[1] for p in files
        }
        stats[name] = {
            "rows": stats["id_comuna"]["rows"],
            "nonnull": stats["id_comuna"]["rows"],
            "values": {v.zfill(width).lstrip("0") or "0" for v in values},
            "over_cap": False,
        }

    out = {}
    for name, entry in stats.items():
        values = entry["values"]
        integers = [int(v) for v in values if INTEGER.match(v)]
        out[name] = {
            "rows": entry["rows"],
            "nonnull": entry["nonnull"],
            "n_distinct": None if entry["over_cap"] else len(values),
            "int": sum(1 for v in values if INTEGER.match(v)),
            "dec": sum(1 for v in values if DECIMAL.match(v)),
            "other": sum(
                1
                for v in values
                if not INTEGER.match(v) and not DECIMAL.match(v)
            ),
            "min": min(integers) if integers else None,
            "max": max(integers) if integers else None,
            "over_cap": entry["over_cap"],
            # Every distinct value when there are few — the sentinel check needs
            # the whole set, not a sample of the most frequent.
            "values": sorted(values) if not entry["over_cap"] else [],
        }
    pathlib.Path(args.out).write_text(json.dumps(out, ensure_ascii=False))
    print(
        f"profiled {len(out)} columns over {len(files)} partitions -> {args.out}"
    )


if __name__ == "__main__":
    main()
