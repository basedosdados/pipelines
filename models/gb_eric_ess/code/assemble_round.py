#!/usr/bin/env python3
"""Assemble one round's architecture CSV using the global glossary.
Usage: python assemble_round.py <round_int>
Names come from glossary_names.json; type/dict/directory come from the approved R11
attributes where the mnemonic exists in R11, else inferred from this round's data
with the same rules (0-10 scale -> INT64; coded -> STRING+dict; directory entities)."""

import csv
import json
import re
import sys
from collections import Counter

ROUND = int(sys.argv[1])
YEAR = {
    1: 2002,
    2: 2004,
    3: 2006,
    4: 2008,
    5: 2010,
    6: 2012,
    7: 2014,
    8: 2016,
    9: 2018,
    10: 2020,
    11: 2023,
}[ROUND]

ARCH_COLS = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
]
NUMERIC_KEEP = {"wkhct", "edagegb", "edagepgb", "edagefgb", "edagemgb"}
DIRECTORY_COL = {
    "cntbrthd": "br_bd_diretorios_mundo.pais:sigla_iso2",
    "fbrncntc": "br_bd_diretorios_mundo.pais:sigla_iso2",
    "mbrncntc": "br_bd_diretorios_mundo.pais:sigla_iso2",
}
SENSITIVE_MODULES = {"Health and Inequality"}
SENSITIVE_HINT = re.compile(
    r"\b(health|income|religio|ethnic|disab|sexual|mental)\b", re.I
)


def _load_json(path):
    with open(path) as fh:
        return json.load(fh)


names = _load_json("glossary_names.json")
r11attr = _load_json("r11_attr.json")
rows = _load_json(f"variables_r{ROUND}.json")

DERIVED = [
    dict(
        name="year",
        bigquery_type="INT64",
        description="Reference year of the ESS round (round start year).",
        directory_column="br_bd_diretorios_data_tempo.ano:ano",
        measurement_unit="year",
        original_name="essround (derived)",
    ),
    dict(
        name="round",
        bigquery_type="INT64",
        description="ESS round number (1-11).",
        original_name="essround",
    ),
    dict(
        name="country_id",
        bigquery_type="STRING",
        description="Country ISO 3166-1 alpha-2 code (native ESS country identifier).",
        directory_column="br_bd_diretorios_mundo.pais:sigla_iso2",
        original_name="cntry",
    ),
    dict(
        name="respondent_id",
        bigquery_type="STRING",
        description="Respondent identification number, unique within country and round.",
        original_name="idno",
    ),
]
PLACED = {"essround", "cntry", "idno"}


def blank():
    return {c: "" for c in ARCH_COLS}


def infer(v):
    mn = v["mnemonic"]
    codes = v.get("int_codes", [])
    in_010 = {c for c in codes if 0 <= c <= 10}
    out_010 = [c for c in codes if c < 0 or c > 10]
    is_010 = in_010 == set(range(0, 11)) and all(c >= 55 for c in out_010)
    coded = v["n_cats"] > 0 and mn not in NUMERIC_KEEP and not is_010
    bt = "STRING" if coded else v["bq_type"]
    cbd = "yes" if coded else "no"
    dirc = ""
    if mn in DIRECTORY_COL:
        bt, cbd, dirc = "STRING", "no", DIRECTORY_COL[mn]
    return bt, cbd, dirc


out = []
for d in DERIVED:
    r = blank()
    r.update(d)
    r["covered_by_dictionary"] = r.get("covered_by_dictionary", "no") or "no"
    out.append(r)

missing_name = 0
for v in rows:
    mn = v["mnemonic"]
    if mn in PLACED:
        continue
    r = blank()
    r["name"] = names.get(mn) or mn
    if mn not in names:
        missing_name += 1
    if mn in r11attr:  # consistent with approved R11
        (
            r["bigquery_type"],
            r["covered_by_dictionary"],
            r["directory_column"],
        ) = r11attr[mn]
    else:
        (
            r["bigquery_type"],
            r["covered_by_dictionary"],
            r["directory_column"],
        ) = infer(v)
    r["description"] = v["label"]
    sens = (v["module"] in SENSITIVE_MODULES) or bool(
        SENSITIVE_HINT.search(v["label"])
    )
    r["has_sensitive_data"] = "yes" if sens else "no"
    r["observations"] = v["module"]
    r["original_name"] = mn
    out.append(r)

# de-dup any name collisions introduced within this round (safety net)
seen = {}
for r in out:
    nm = r["name"]
    if nm in seen:
        seen[nm] += 1
        r["name"] = f"{nm}_{seen[nm]}"
    else:
        seen[nm] = 1

tsv = f"gb_eric_ess__round_{ROUND:02d}.tsv"
with open(tsv, "w", encoding="utf-8", newline="") as f:
    w = csv.DictWriter(f, fieldnames=ARCH_COLS, delimiter="\t")
    w.writeheader()
    for r in out:
        w.writerow(r)
with open(tsv.replace(".tsv", ".csv"), "w", encoding="utf-8", newline="") as f:
    w = csv.writer(f)
    w.writerow(ARCH_COLS)
    for r in out:
        w.writerow([r[c] for c in ARCH_COLS])

print(
    f"round {ROUND:02d} (year {YEAR}): {len(out)} rows | types {dict(Counter(r['bigquery_type'] for r in out))} "
    f"| dict=yes {sum(1 for r in out if r['covered_by_dictionary'] == 'yes')} | missing-name {missing_name}"
)
