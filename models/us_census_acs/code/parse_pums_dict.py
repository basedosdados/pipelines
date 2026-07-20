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

# ---- 1. parse dictionaries (CSV 2013+ and TXT 2009-2016) ----
# The CSV dictionaries only exist from 2013 on and carry an explicit C|N type.
# The TXT dictionaries (2009-2016, incl. the single-year 13/14/15/16 files) are the
# ONLY machine-readable definition of the vintage/legacy variables (OCCP02, PUMA00,
# DSL, HANDHELD, MODEM, ...) that the older PUMS files still carry. Parsing them is
# what keeps those columns from falling back to a hand-written description.
# Pre-2009 dictionaries are published as PDF only, so a residue stays undefined.
#
# TXT layout:
#   VARNAME     <width>
#   <description>                 <- next non-empty line
#               <val> .<label>    <- indented label lines
#
# TXT carries no C|N type. Records parsed from TXT therefore contribute the
# description/labels only; the type comes from a CSV record when one exists and
# otherwise defaults to "C" (categorical -> STRING), which is what
# build_pums_architecture.py already assumed for these columns. Keeping that
# default means adding TXT parsing changes descriptions WITHOUT changing any
# BigQuery type (no re-materialization).
records = {}  # var -> list of {year, source, type, width, desc, n_labels}

TXT_NAME_RE = re.compile(r"^([A-Z][A-Z0-9]{0,12})\s+(\d+)\s*$")
# A value-label line is "<value> .<label>". The published .txt dictionaries indent
# these, but the PDF-rendered ones (fetch_legacy_dicts.py) emit them at column 0, so
# the leading indent must NOT be required — demanding it silently counted zero labels
# and flipped covered_by_dictionary to "no" on label-bearing vintage columns.
TXT_LABEL_RE = re.compile(r"^\s*\S+\s+\.\S")
# pypdf renders page numbers as their own line; never mistake one for a description.
TXT_PAGENO_RE = re.compile(r"^\d{1,4}$")
# ...but a description often wraps onto a bare year ("...for data collected prior to"
# / "2012"), which the page-number guard would otherwise swallow. These dictionaries
# run to ~150 pages, so a standalone 19xx/20xx is always a wrapped year.
TXT_YEAR_RE = re.compile(r"^(19|20)\d{2}$")
# A NOTE: block follows the description but is commentary, not part of it.
TXT_NOTE_RE = re.compile(r"^\s*NOTE\b", re.I)
# Descriptions wrap across physical lines, e.g.
#     POWSP05   3
#         Place of work for data collected prior to 2012 - State or foreign country
#         recode
#             bbb .N/A ...
# Reading only the first line silently truncates mid-sentence, so continuation
# lines are accumulated until a value-label line, a NOTE:, or the next variable.
MAX_DESC_LINES = 6


def clean_desc(s: str) -> str:
    """Normalize a dictionary description.

    Repairs double-encoded UTF-8 (the Census ships a few dictionaries whose
    correct UTF-8 bytes were themselves decoded as latin-1 and re-encoded, so
    an en dash arrives as 'â\x80\x93'), collapses tabs/newlines/runs of spaces
    introduced by wrapping, and drops a trailing period per the repo convention.
    """
    try:
        repaired = s.encode("latin-1").decode("utf-8")
        s = repaired
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass  # genuinely latin-1 (or already clean) — leave as-is
    s = re.sub(r"\s+", " ", s).strip()
    return s[:-1].rstrip() if s.endswith(".") else s


def year_of(path, ext):
    m = re.search(rf"_(\d{{4}})(?:-(\d{{4}}))?\.{ext}$", path)
    if m:
        return m.group(2) or m.group(1)
    m = re.search(rf"Dict(\d{{2}})\.{ext}$", path)  # PUMSDataDict13.txt
    return f"20{m.group(1)}" if m else "?"


_seq = [0]


def add(var, year, source, typ, width, desc, n_labels):
    _seq[0] += 1
    records.setdefault(var, []).append(
        {
            "year": year,
            "source": source,
            "type": typ,
            "width": width,
            "desc": desc,
            "n_labels": n_labels,
            "seq": _seq[0],
        }
    )


# --- CSV dictionaries (authoritative types) ---
for path in sorted(glob.glob(f"{PUMS}/dict/*.csv")):
    yr = year_of(path, "csv")
    pending = {}  # var -> record index, to attach VAL counts
    with open(path, newline="", encoding="latin-1") as f:
        for row in csv.reader(f):
            if not row:
                continue
            rt = row[0].strip()
            if rt == "NAME" and len(row) >= 5:
                var = row[1].strip().upper()
                add(
                    var,
                    yr,
                    "csv",
                    row[2].strip(),
                    row[3].strip(),
                    clean_desc(row[4]),
                    0,
                )
                pending[var] = records[var][-1]
            elif rt == "VAL" and len(row) >= 2:
                var = row[1].strip().upper()
                if var in pending:
                    pending[var]["n_labels"] += 1

# --- TXT dictionaries (only source for the legacy/vintage variables) ---
for path in sorted(glob.glob(f"{PUMS}/dict/*.txt")):
    yr = year_of(path, "txt")
    with open(path, encoding="latin-1") as f:
        lines = [ln.rstrip("\n") for ln in f]
    i = 0
    while i < len(lines):
        m = TXT_NAME_RE.match(lines[i])
        if not m:
            i += 1
            continue
        var, width = m.group(1).upper(), m.group(2)
        j = i + 1
        while j < len(lines) and (
            not lines[j].strip() or TXT_PAGENO_RE.match(lines[j].strip())
        ):
            j += 1
        # accumulate the (possibly wrapped) description
        parts = []
        while (
            j < len(lines)
            and len(parts) < MAX_DESC_LINES
            and not TXT_NAME_RE.match(lines[j])
            and not TXT_LABEL_RE.match(lines[j])
            and not TXT_NOTE_RE.match(lines[j])
        ):
            chunk = lines[j].strip()
            if chunk and (
                not TXT_PAGENO_RE.match(chunk) or TXT_YEAR_RE.match(chunk)
            ):
                parts.append(chunk)
            j += 1
        desc = clean_desc(" ".join(parts)) if parts else ""
        n_labels = 0
        while j < len(lines) and not TXT_NAME_RE.match(lines[j]):
            if TXT_LABEL_RE.match(lines[j]):
                n_labels += 1
            j += 1
        if desc:
            add(var, yr, "txt", None, width, desc, n_labels)
        i = j

# --- merge: newest year wins; CSV beats TXT on a tie; last-parsed wins a full tie
# (the seq tiebreak reproduces the previous CSV-only precedence exactly, so adding
# TXT parsing cannot silently reword an already-published CSV-sourced description) ---
vardict = {}
for var, recs in records.items():
    best = max(recs, key=lambda r: (r["year"], r["source"] == "csv", r["seq"]))
    csv_types = [r["type"] for r in recs if r["source"] == "csv" and r["type"]]
    vardict[var] = {
        "type": csv_types[-1] if csv_types else "C",
        "width": best["width"],
        "desc": best["desc"],
        "n_labels": max(r["n_labels"] for r in recs),
        "has_labels": max(r["n_labels"] for r in recs) > 0,
        "years": sorted({r["year"] for r in recs}),
        "desc_source": best["source"],
        "typed_from_csv": bool(csv_types),
    }


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


n_csv = len(glob.glob(f"{PUMS}/dict/*.csv"))
n_txt = len(glob.glob(f"{PUMS}/dict/*.txt"))
print(f"dictionaries parsed: {n_csv} csv + {n_txt} txt")
print(f"total distinct vars in dict: {len(vardict)}")
from_txt = [v for v, d in vardict.items() if d["desc_source"] == "txt"]
print(f"  vars whose description comes from a TXT dict: {len(from_txt)}")
print(
    f"  vars with no CSV type (defaulted to C/STRING): {sum(1 for d in vardict.values() if not d['typed_from_csv'])}"
)
for kind, cols in [("PERSON", person_cols), ("HOUSING", housing_cols)]:
    tot, kn, unk, n, cc, unkl = classify(cols)
    print(
        f"\n{kind}: {tot} union cols | in-dict {kn} (N={n}, C={cc}) | not-in-dict {unk}"
    )
    if unkl:
        print(f"  not-in-dict: {unkl}")
