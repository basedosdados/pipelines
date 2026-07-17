#!/usr/bin/env python3
"""Parse all PUMS data dictionaries + compute person/housing column unions.

Outputs (to WORK dir):
  pums_vardict.json     var -> {type, width, desc, has_labels, n_labels, years:[...]}
  pums_person_cols.txt  ordered union of person columns across all vintages
  pums_housing_cols.txt ordered union of housing columns across all vintages

Dictionary CSV rows:
  NAME,<VAR>,<C|N>,<width>,"<description>"
  VAL,<VAR>,<C|N>,<width>,"<min>","<max>","<label>"
"""

import csv
import glob
import io
import json
import os
import re
import zipfile

PUMS = "/Users/rdahis/acs_data/pums"
WORK = "/Users/rdahis/acs_data/work"
os.makedirs(WORK, exist_ok=True)

# High-confidence pure identity renames (old -> canonical); applied before unioning
# so the old name never becomes a separate canonical column. Keep this list minimal
# and identical across parse/build/clean. Do NOT add recoded vars (WKW->WKWN,
# REL->RELSHIPP) — those stay separate historical columns.
RENAME = {"ST": "STATE", "BDS": "BDSP", "RMS": "RMSP", "VAL": "VALP"}

# ---- 1. parse dictionaries (csv format only; recent years cover all current vars) ----
vardict = {}  # var -> dict


def year_of(path):
    m = re.search(r"Dictionary_(\d{4})(?:-(\d{4}))?\.csv$", path)
    return m.group(2) or m.group(1) if m else "?"


for path in sorted(glob.glob(f"{PUMS}/dict/PUMS_Data_Dictionary_*.csv")):
    yr = year_of(path)
    with open(path, newline="", encoding="latin-1") as f:
        for row in csv.reader(f):
            if not row:
                continue
            rt = row[0].strip()
            if rt == "NAME" and len(row) >= 5:
                var, typ, width, desc = (
                    row[1].strip().upper(),
                    row[2].strip(),
                    row[3].strip(),
                    row[4].strip(),
                )
                d = vardict.setdefault(
                    var,
                    {
                        "type": typ,
                        "width": width,
                        "desc": desc,
                        "n_labels": 0,
                        "years": set(),
                    },
                )
                d["years"].add(yr)
                # prefer the most recent (max year) description/type
                if yr >= max(d["years"]):
                    d["type"], d["width"], d["desc"] = typ, width, desc
            elif rt == "VAL" and len(row) >= 2:
                var = row[1].strip().upper()
                if var in vardict:
                    vardict[var]["n_labels"] += 1

for _v, d in vardict.items():
    d["has_labels"] = d["n_labels"] > 0
    d["years"] = sorted(d["years"])


# ---- 2. column unions from actual CSV headers across vintages ----
def first_csv_header(zip_path):
    try:
        with zipfile.ZipFile(zip_path) as z:
            members = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if not members:
                return []
            with z.open(sorted(members)[0]) as m:
                line = io.TextIOWrapper(m, encoding="latin-1").readline()
                cols, seen_local = [], set()
                for c in line.rstrip("\n").split(","):
                    u = RENAME.get(c.strip().upper(), c.strip().upper())
                    if (
                        u not in seen_local
                    ):  # drop dup if both old+new present in a file
                        cols.append(u)
                        seen_local.add(u)
                return cols
    except Exception as e:
        print(f"  ! {zip_path}: {e}")
        return []


def union_cols(kind):  # kind in {"pus","hus"}
    base_order, seen, extras = [], set(), []
    dirs = sorted(glob.glob(f"{PUMS}/*_1yr")) + sorted(
        glob.glob(f"{PUMS}/*_5yr")
    )
    # canonical order = most recent 1yr header
    recent = f"{PUMS}/2024_1yr/csv_{kind}.zip"
    for c in first_csv_header(recent):
        if c not in seen:
            base_order.append(c)
            seen.add(c)
    for d in dirs:
        for zp in (f"{d}/csv_{kind}.zip", f"{d}/csv_{kind}a.zip"):
            if os.path.exists(zp):
                for c in first_csv_header(zp):
                    if c not in seen:
                        extras.append(c)
                        seen.add(c)
                break
    return base_order + extras


person_cols = union_cols("pus")
housing_cols = union_cols("hus")

with open(f"{WORK}/pums_person_cols.txt", "w") as f:
    f.write("\n".join(person_cols))
with open(f"{WORK}/pums_housing_cols.txt", "w") as f:
    f.write("\n".join(housing_cols))
with open(f"{WORK}/pums_vardict.json", "w") as f:
    json.dump(vardict, f, indent=1)


# ---- 3. summary ----
def classify(cols):
    known = [c for c in cols if c in vardict]
    unknown = [c for c in cols if c not in vardict]
    n = sum(1 for c in known if vardict[c]["type"] == "N")
    cc = sum(1 for c in known if vardict[c]["type"] == "C")
    return len(cols), len(known), len(unknown), n, cc, unknown


print(
    f"dictionaries parsed: {len(glob.glob(f'{PUMS}/dict/PUMS_Data_Dictionary_*.csv'))} csv files"
)
print(f"total distinct vars in dict: {len(vardict)}")
for kind, cols in [("PERSON", person_cols), ("HOUSING", housing_cols)]:
    tot, kn, unk, n, cc, unkl = classify(cols)
    print(
        f"\n{kind}: {tot} union cols | in-dict {kn} (N={n}, C={cc}) | not-in-dict {unk}"
    )
    if unkl:
        print(f"  not-in-dict: {unkl}")
