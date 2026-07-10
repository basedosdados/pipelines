#!/usr/bin/env python3
"""Build the cross-round glossary from the .dta-sourced variables_r*.json.
- glossary_names.json: {mnemonic: english_name}. SEEDED from the existing glossary
  so every already-assigned name (the approved R11 names and all prior rounds) is
  frozen; only genuinely new mnemonics get a deterministic base_name(), resolving
  each from the most recent round it appears in (rounds processed newest-first).
- r11_attr.json: {mnemonic: [bq_type, covered_by_dictionary, directory_column]} taken
  from the approved R11 architecture (eu_ess__round_11.tsv), so a variable's
  type/dict/dir stays consistent with the approved R11 in every round it appears."""

import collections
import csv
import glob
import importlib.util
import json
import re

spec = importlib.util.spec_from_file_location("ng", "name_gen.py")
ng = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ng)


def _load_json(path):
    with open(path) as fh:
        return json.load(fh)


def _dump_json(obj, path):
    with open(path, "w") as fh:
        json.dump(obj, fh, ensure_ascii=False)


def _read_tsv(path):
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


# seed from the existing approved glossary so prior names never change
try:
    names = _load_json("glossary_names.json")
except FileNotFoundError:
    names = {}
used = collections.Counter(names.values())


def assign(mn, label):
    if mn in names:
        return
    base = ng.base_name(mn, label)
    if len(base) > 48:
        base = "_".join(base.split("_")[:6])
    nm, k = base, 1
    while nm in used:
        k += 1
        nm = f"{base}_{k}"
    used[nm] += 1
    names[mn] = nm


avail = sorted(
    (
        int(m.group(1))
        for f in glob.glob("variables_r*.json")
        if (m := re.fullmatch(r"variables_r(\d+)\.json", f))
    ),
    reverse=True,  # newest first: new mnemonics resolve from the latest wording
)
for r in avail:
    for v in _load_json(f"variables_r{r}.json"):
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
