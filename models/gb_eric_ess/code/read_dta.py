#!/usr/bin/env python3
"""Read an ESS integrated Stata (.dta) file's metadata into the pipeline's
variables_r<N>.json format (same schema parse_codebook.py emits).

The .dta metadata gives exact variable labels and value labels, so this is a
drop-in, higher-fidelity replacement for the PDF codebook parser used for
rounds 8-11. Value-label keys that are integers are the substantive codes;
string keys ('a','b','c') are Stata extended-missing markers (No answer /
Refusal / Don't know) and are ignored for int_codes.

Usage: python read_dta.py <input.dta> <round_int> <output.json>
"""

import json
import re
import sys
from collections import Counter

# pyrefly: ignore [missing-import]
import pyreadstat

DTA = sys.argv[1]
ROUND = int(sys.argv[2])
OUT = sys.argv[3]

_, meta = pyreadstat.read_dta(DTA, metadataonly=True)

labels = dict(zip(meta.column_names, meta.column_labels, strict=False))
rvt = (
    meta.readstat_variable_types
)  # name -> int8/int16/int32/float/double/string
vvl = meta.variable_value_labels  # name -> {code: label}


def bq_of(t):
    t = (t or "").lower()
    if "string" in t:
        return "STRING"
    if "float" in t or "double" in t:
        return "FLOAT64"
    return "INT64"  # int8/int16/int32/int64


def is_int_key(k):
    if isinstance(k, bool):
        return False
    if isinstance(k, int):
        return True
    if isinstance(k, float):
        return k.is_integer()
    if isinstance(k, str):
        return bool(re.fullmatch(r"-?\d+", k))
    return False


rows = []
for name in meta.column_names:
    vl = vvl.get(name, {}) or {}
    int_codes = sorted({int(k) for k in vl if is_int_key(k)})
    has_str_cat = any(
        isinstance(k, str)
        and not re.fullmatch(r"-?\d+", k)
        and k not in ("a", "b", "c", "r", "d", "n")
        for k in vl
    )
    t = rvt.get(name, "")
    ck = "str" if has_str_cat else ("int" if int_codes else "none")
    rows.append(
        {
            "module": "",
            "mnemonic": name,
            "label": (labels.get(name) or "").strip(),
            "ess_type": t,
            "bq_type": bq_of(t),
            "code_kind": ck,
            "n_cats": len(vl),
            "int_codes": int_codes,
        }
    )

with open(OUT, "w") as fh:
    json.dump(rows, fh, indent=1, ensure_ascii=False)

print(f"round {ROUND}: {len(rows)} vars -> {OUT}")
print(
    "  bq types:",
    dict(Counter(r["bq_type"] for r in rows)),
    "| coded (n_cats>0):",
    # pyrefly: ignore [unsupported-operation]
    sum(1 for r in rows if r["n_cats"] > 0),
)
