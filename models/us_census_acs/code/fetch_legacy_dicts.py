#!/usr/bin/env python3
"""Fetch the pre-2009 PUMS data dictionaries and render them to text.

The Census publishes machine-readable PUMS dictionaries (.csv from 2013, .txt from
2009) only for recent years. For 2005-2008 the dictionary exists as **PDF only**, so
the variables that appear exclusively in those vintages (ADJUST, DS, DWRK, MILY, REL,
SSPA, UWRK, MLPC/D/F/G, INDP02, NAICSP02, ...) have no parseable definition and fall
back to a hand-written placeholder in the architecture.

The PDF text layout is identical to the .txt dictionary layout:

    VARNAME     <width>
    <description>
                <val> .<label>

so rendering each PDF to `PUMSDataDict<YY>.txt` lets parse_pums_dict.py pick them up
with no special-casing. Extraction uses pypdf (pure Python — no poppler dependency).

Idempotent: skips a download or a render whose output already exists, and never
overwrites a dictionary that the Census actually publishes as .txt (2009+).

Usage:  python3 code/fetch_legacy_dicts.py
"""

import os
import urllib.request

BASE = "https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict"
DICT = "/Users/rdahis/acs_data/pums/dict"

# Single-year dictionaries that exist as PDF only (the Census ships real .txt from
# 2013 on — never render those from PDF or we would clobber the authoritative source).
# 2012 matters specifically for SSPA ("Same sex spouse recode"), which appears in the
# 2012 1-year file and in no .txt dictionary.
PDF_ONLY = ["05", "06", "07", "08", "09", "10", "11", "12"]

# Multi-year dictionaries needed for variables that exist only in a 5-year file.
# 2006-2010 spans the 2002->2007 industry code change and is the only published
# definition of INDP02/INDP07/NAICSP02/NAICSP07 (carried by the 2010 and 2011
# 5-year PUMS files).
PDF_RANGES = ["PUMS_Data_Dictionary_2006-2010"]

UA = "Mozilla/5.0 (compatible; basedosdados-pipelines/1.0)"


def fetch(name: str) -> str:
    path = os.path.join(DICT, name)
    if os.path.exists(path):
        print(f"  have {name}")
        return path
    req = urllib.request.Request(f"{BASE}/{name}", headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=120) as r, open(path, "wb") as f:
        f.write(r.read())
    print(f"  downloaded {name} ({os.path.getsize(path):,} bytes)")
    return path


def render(pdf_path: str, txt_path: str) -> None:
    if os.path.exists(txt_path):
        print(f"  have {os.path.basename(txt_path)}")
        return
    import pypdf

    reader = pypdf.PdfReader(pdf_path)
    pages = [p.extract_text() or "" for p in reader.pages]
    text = "\n".join(pages)
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(text)
    print(
        f"  rendered {os.path.basename(txt_path)} ({len(reader.pages)} pages)"
    )


def main() -> None:
    os.makedirs(DICT, exist_ok=True)
    for yy in PDF_ONLY:
        print(f"[20{yy}]")
        render(
            fetch(f"PUMSDataDict{yy}.pdf"),
            os.path.join(DICT, f"PUMSDataDict{yy}.txt"),
        )
    for stem in PDF_RANGES:
        print(f"[{stem}]")
        render(fetch(f"{stem}.pdf"), os.path.join(DICT, f"{stem}.txt"))
    print("\nDone. Re-run parse_pums_dict.py to pick up the new dictionaries.")


if __name__ == "__main__":
    main()
