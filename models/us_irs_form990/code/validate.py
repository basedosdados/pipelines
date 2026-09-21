"""Validate the cleaned e-file output before upload.

Reads ``output/efile_results/<batch>.json`` (written by ``clean.py efile``) and
the IRS index CSVs and reports:

* returns and people per batch and per tax year, and the form types skipped;
* concordance variables that never matched (a schema version the concordance
  does not cover would show up here as a silently-null column);
* parsed 990/990-EZ counts per release year against the IRS index files;
* duplicate (ein, year, form_type) groups the dbt model will collapse.

    python models/us_irs_form990/code/validate.py
"""

import csv
import glob
import json
import os
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq

from pipelines.datasets.us_irs_form990 import utils
from pipelines.datasets.us_irs_form990.variables import ALL_VARIABLES

DATA = Path(
    os.environ.get(
        "FORM990_DATA_DIR", Path.home() / "Downloads/us_irs_form990_data"
    )
)
INPUT = DATA / "input"
OUTPUT = DATA / "output"
RESULTS = OUTPUT / "efile_results"


def main() -> None:
    results = [
        json.loads(p.read_text()) for p in sorted(RESULTS.glob("*.json"))
    ]
    errors = [r for r in results if "error" in r]
    print(f"{len(results)} batches, {len(errors)} failed")
    for r in errors:
        print("  FAILED", r["batch"], r["error"])

    by_year_ret: Counter = Counter()
    by_year_ppl: Counter = Counter()
    skipped: Counter = Counter()
    hits: Counter = Counter()
    by_release: Counter = Counter()
    bad = 0
    for r in results:
        if "error" in r:
            continue
        for y, n in r["return_financial"].items():
            by_year_ret[y] += n
        for y, n in r["compensation"].items():
            by_year_ppl[y] += n
        skipped.update(r["skipped"])
        hits.update(r.get("xpath_hits", {}))
        bad += len(r["unparseable"])
        release = (
            r["batch"][:4]
            if r["batch"][:4].isdigit()
            else r["batch"].split("_")[1]
        )
        by_release[release] += sum(r["return_financial"].values())
    print(
        f"\nreturns: {sum(by_year_ret.values()):,}  people: {sum(by_year_ppl.values()):,}"
    )
    print("skipped form types:", dict(skipped), "unparseable:", bad)
    print("\nby tax year (returns / people):")
    for y in sorted(by_year_ret):
        print(f"  {y}: {by_year_ret[y]:>10,} / {by_year_ppl[y]:>10,}")

    # --- concordance coverage: which variables never matched -------------
    conc = utils.concordance()
    var_hits: Counter = Counter()
    for xp, n in hits.items():
        var = conc.scalar.get(xp)
        if var is None:
            # group xpaths were recorded as parent/rel
            for gp, table in conc.group.items():
                if xp.startswith(gp + "/") and xp[len(gp) + 1 :] in table:
                    var = table[xp[len(gp) + 1 :]]
                    break
        if var:
            var_hits[var] += n
    never = [v for v in ALL_VARIABLES if var_hits[v] == 0]
    print(
        f"\nconcordance variables used: {len(ALL_VARIABLES)}, never matched: {never}"
    )
    print("distinct xpaths matched:", len(hits))

    # --- against the IRS index files --------------------------------------
    print("\nrelease year: parsed 990+990EZ vs IRS index 990+990EZ")
    for idx in sorted(glob.glob(str(INPUT / "efile" / "index_*.csv"))):
        year = Path(idx).stem.split("_")[1]
        c: Counter = Counter()
        with open(idx, newline="", encoding="latin-1") as fh:
            for row in csv.DictReader(fh):
                c[row["RETURN_TYPE"]] += 1
        want = c["990"] + c["990EZ"]
        print(
            f"  {year}: {by_release[year]:>9,} parsed vs {want:>9,} indexed  (index all types {sum(c.values()):,}; 990PF {c['990PF']:,}, 990T {c['990T']:,})"
        )

    # --- duplicates the dbt model collapses -------------------------------
    files = [
        f
        for f in glob.glob(
            str(OUTPUT / "return_financial" / "**" / "*.parquet"),
            recursive=True,
        )
        if "00_header" not in f
    ]
    keys: Counter = Counter()
    versions: Counter = Counter()
    types: Counter = Counter()
    for f in files:
        t = pq.read_table(
            f, columns=["ein", "form_type", "return_version", "object_id"]
        )
        year = Path(f).parent.name.split("=")[1]
        for ein, ft in zip(
            t["ein"].to_pylist(), t["form_type"].to_pylist(), strict=True
        ):
            keys[(ein, year, ft)] += 1
        versions.update(t["return_version"].to_pylist())
        types.update(t["form_type"].to_pylist())
    dup_groups = sum(1 for v in keys.values() if v > 1)
    dup_rows = sum(v - 1 for v in keys.values() if v > 1)
    print(
        f"\n(ein, year, form_type) groups: {len(keys):,}; groups with >1 return: {dup_groups:,} ({dup_rows:,} rows dropped by the model)"
    )
    print("form types:", dict(types))
    print("schema versions:", sorted(versions.items()))


if __name__ == "__main__":
    main()
