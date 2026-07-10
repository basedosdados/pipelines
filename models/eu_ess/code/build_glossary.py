#!/usr/bin/env python3
"""Build the cross-round glossary from all available variables_r*.json.
- glossary_names.json: {mnemonic: english_name} for every mnemonic across rounds.
  R11 is assigned FIRST in its codebook order (reproducing the approved R11 names
  exactly); new mnemonics from other rounds (newest-first) get names afterwards.
- r11_attr.json: {mnemonic: [bq_type, covered_by_dictionary, directory_column]} taken
  from the final approved R11 architecture, so a variable's type/dict/dir stays
  consistent with R11 in every round it appears."""

import collections
import csv
import glob
import importlib.util
import json
import re

spec = importlib.util.spec_from_file_location("ng", "name_gen.py")
ng = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ng)

avail = sorted(
    int(m.group(1))
    for f in glob.glob("variables_r*.json")
    if (m := re.fullmatch(r"variables_r(\d+)\.json", f))
)  # excludes variables_r11_new.json
has_r11 = bool(glob.glob("variables_r11_new.json"))
order = ([11] if has_r11 else []) + [
    r for r in sorted(avail, reverse=True) if r != 11
]

names, used = {}, collections.Counter()


def assign(mn, label):
    if mn in names:
        return
    nm = ng.base_name(mn, label)
    if len(nm) > 48:
        nm = "_".join(nm.split("_")[:6])
    if nm in used:
        used[nm] += 1
        nm = f"{nm}_{used[nm]}"
    else:
        used[nm] = 1
    names[mn] = nm


def _load_json(path):
    with open(path) as fh:
        return json.load(fh)


def _dump_json(obj, path):
    with open(path, "w") as fh:
        json.dump(obj, fh, ensure_ascii=False)


def _read_tsv(path):
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def load(r):
    fn = f"variables_r{r}.json"
    if r == 11:
        fn = "variables_r11_new.json"  # identical to variables.json
    return _load_json(fn)


for r in order:
    if r == 11 and not glob.glob("variables_r11_new.json"):
        continue
    for v in load(r):
        assign(v["mnemonic"], v["label"])
_dump_json(names, "glossary_names.json")

# final approved R11 attributes, keyed by mnemonic (original_name)
r11_rows = _read_tsv("eu_ess__round_11.tsv")
r11attr = {}
for row in r11_rows:
    on = row["original_name"]
    if on and "(" not in on and " " not in on:
        r11attr[on] = [
            row["bigquery_type"],
            row["covered_by_dictionary"],
            row["directory_column"],
        ]
_dump_json(r11attr, "r11_attr.json")

print(
    f"glossary_names: {len(names)} | r11_attr: {len(r11attr)} | rounds available: {avail}"
)
# verify R11 names reproduce the approved sheet exactly
sheet = {
    r["original_name"]: r["name"]
    for r in r11_rows
    if r["original_name"]
    and "(" not in r["original_name"]
    and " " not in r["original_name"]
}
mism = [m for m, nm in sheet.items() if m in names and names[m] != nm]
print(f"R11 name mismatches vs approved sheet: {len(mism)} {mism[:6]}")
