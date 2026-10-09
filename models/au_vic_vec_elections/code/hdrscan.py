"""Read the print date / election / stage header out of every VEC report workbook."""

import os
import re
import sys
import warnings

import pandas as pd

warnings.filterwarnings("ignore")
S = os.path.expanduser("~/Downloads/au_vic_vec_elections_data")
IN = f"{S}/input"


def header(path):
    try:
        xl = pd.ExcelFile(path)
    except Exception as e:
        return {"error": str(e)[:80]}
    sh = xl.sheet_names[0]
    df = xl.parse(sh, header=None, nrows=12)
    flat = [str(v).strip() for v in df.to_numpy().ravel() if not pd.isna(v)]
    txt = " | ".join(flat)
    pd_m = re.search(r"Print Date/Time:?\s*([0-9/]{8,10}[^|]*)", txt)
    el_m = re.search(
        r"((?:State Election|[A-Z][A-Za-z\- ]+District By-election)\s*\d{4})",
        txt,
    )
    stage = (
        "Recheck"
        if re.search(r"\bRecheck\b", txt)
        else ("Primary" if re.search(r"\bPrimary\b", txt) else "")
    )
    return {
        "sheets": len(xl.sheet_names),
        "print": (pd_m.group(1).strip() if pd_m else ""),
        "election": (el_m.group(1) if el_m else ""),
        "stage": stage,
        "kind": flat[0][:44] if flat else "",
    }


targets = sys.argv[1:]
for t in targets:
    full = os.path.join(IN, t)
    h = header(full)
    print(f"{t[:78]:80s} {h}")
