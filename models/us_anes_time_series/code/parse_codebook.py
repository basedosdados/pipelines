# ruff: noqa: E741
"""Parse the ANES CDF variable codebook (pdftotext -layout output) into a
structured JSON of per-variable entries: label, question, Valid/Missing
value-label blocks, and a proposed BigQuery type.

Run: uv run python models/us_anes_time_series/code/parse_codebook.py
Outputs: code/build/parsed.json  (+ prints a review sample)
"""

import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
BUILD = HERE / "build"
TXT = BUILD / "var_codebook.txt"
CSV_COLS = BUILD / "csv_cols.txt"

csv_cols = [c.strip() for c in CSV_COLS.read_text().splitlines() if c.strip()]
csv_set = set(csv_cols)

lines = TXT.read_text(errors="replace").splitlines()

# ---- entry header detection -------------------------------------------------
# A header looks like:  " VCF0104 RESPONDENT - GENDER"  (label mostly UPPERCASE)
# Index lines look like " 31   VCF0107   Respondent - Hispanic Origin Type" (Title Case)
HDR = re.compile(r"^\s?(VCF\d{4}[a-z]?)\s+(\S.*\S)\s*$")


def is_upper_label(lbl: str) -> bool:
    letters = [c for c in lbl if c.isalpha()]
    if not letters:
        return False
    up = sum(1 for c in letters if c.isupper())
    return up / len(letters) >= 0.8


# page-artifact / banner lines to drop inside an entry body
ARTIFACT = re.compile(
    r"(VARIABLE DESCRIPTION|GENERAL INTRODUCTION|^\s*\d{1,4}\s{6,}\S|^\s*\d{1,4}\s*$)"
)
VAL = re.compile(r"^\s*([0-9]+(?:-[0-9]+)?)\.\s+(.*\S)\s*$")
KEYWORD = re.compile(
    r"^\s*(Question|Valid|Missing|Notes|Weight|Source|Table)\b"
)

# find header line indices. A real entry header has the VCF code at (near) the
# start of the line; index lines have a sequence number BEFORE the code, so they
# never match HDR. We take the FIRST such line per code as its entry.
CROSSREF = re.compile(r"^(See |Note |Collapsed |Recoded |Derived )", re.I)


def is_upperish(s: str) -> bool:
    letters = [c for c in s if c.isalpha()]
    return (
        bool(letters)
        and sum(c.isupper() for c in letters) / len(letters) >= 0.8
    )


def label_prefix(i: int) -> str:
    """Wrapped labels put their first part on the indented line(s) directly
    above the code line. Collect contiguous uppercase, indented, non-keyword
    lines immediately above header line i."""
    parts = []
    j = i - 1
    while j >= 0:
        ln = lines[j]
        if not ln.strip():
            break
        if (
            len(ln) - len(ln.lstrip())
        ) < 5:  # left-aligned body text, not a label
            break
        if KEYWORD.match(ln) or VAL.match(ln) or ARTIFACT.search(ln):
            break
        if not is_upperish(ln):
            break
        parts.append(ln.strip())
        j -= 1
    return " ".join(reversed(parts))


headers = []  # (line_idx, code, label)
for i, ln in enumerate(lines):
    m = HDR.match(ln)
    if not m:
        continue
    code, frag = m.group(1), m.group(2)
    if code not in csv_set:
        continue
    if CROSSREF.match(frag):  # body cross-reference, not a header
        continue
    lbl = (label_prefix(i) + " " + frag).strip()
    headers.append((i, code, lbl))

# keep the FIRST header per code (entries appear once; indexes were filtered)
seen = set()
uniq = []
for i, code, lbl in headers:
    if code in seen:
        continue
    seen.add(code)
    uniq.append((i, code, lbl))
headers = uniq

# entry boundaries
bounds = {}
for k, (i, code, lbl) in enumerate(headers):
    end = headers[k + 1][0] if k + 1 < len(headers) else len(lines)
    bounds[code] = (i, end, lbl)


def parse_block(body, start_kw):
    """Return list of (key,label) for the Valid/Missing block named start_kw."""
    out = []
    in_block = False
    cur = None
    for ln in body:
        if ARTIFACT.search(ln):
            continue
        kw = KEYWORD.match(ln)
        if kw:
            name = kw.group(1)
            if name == start_kw:
                in_block = True
                # strip the leading "Valid"/"Missing" keyword, keep remainder
                ln = ln[kw.end() :]
            elif in_block:
                break  # next keyword ends the block
            else:
                continue
        if not in_block:
            continue
        if re.match(r"\s*INAP\b", ln):
            cur = None  # INAP = blank/NULL in data; not a dict value, ends any label
            continue
        vm = VAL.match(ln)
        if vm:
            cur = [vm.group(1), vm.group(2).strip()]
            out.append(cur)
        elif cur is not None and ln.strip():
            # continuation of previous label
            cur[1] += " " + ln.strip()
    return out


def parse_question(body):
    q = []
    in_q = False
    for ln in body:
        if ARTIFACT.search(ln):
            continue
        kw = KEYWORD.match(ln)
        if kw:
            if kw.group(1) == "Question":
                in_q = True
                ln = ln[kw.end() :]
            elif in_q:
                break
            else:
                continue
        if in_q and ln.strip():
            q.append(ln.strip())
    return " ".join(q).strip()


RANGE = re.compile(r"^(\d+)-(\d+)$")
NUM_HINT = re.compile(
    r"(as coded|degrees|age|dollars?|amount|years old|number of|percent)", re.I
)

entries = {}
for code, (i, end, lbl) in bounds.items():
    body = lines[i + 1 : end]
    valid = parse_block(body, "Valid")
    missing = parse_block(body, "Missing")
    question = parse_question(body)

    # ---- type proposal ----------------------------------------------------
    # Policy (locked): numeric ONLY for unambiguous quantities where arithmetic
    # is meaningful and a unit exists — year, weights, age, feeling thermometers.
    # Everything else -> STRING + dictionary (coded categoricals, Likerts, counts,
    # nominal geo/religion/ethnicity codes) so missing sentinels are preserved.
    btype = "STRING"
    numeric_reason = ""
    valid_labels = " ".join(l for _, l in valid)
    is_therm = "THERMOMETER" in lbl.upper() or (
        valid and valid[0][1].lower().startswith("degrees")
    )
    if code == "VCF0004":
        btype, numeric_reason = "INT64", "year/partition"
    elif (
        re.match(r"VCF00(09|10|11)$|VCF00(09|10|11)[a-z]$", code)
        or code == "VCF9999"
    ):
        btype, numeric_reason = "FLOAT64", "weight (dimensionless)"
    elif code in ("VCF0006", "VCF0006a"):
        btype, numeric_reason = "STRING", "identifier"
    elif code == "VCF0101":
        btype, numeric_reason = "INT64", "age (years)"
    elif is_therm:
        btype, numeric_reason = "INT64", "feeling thermometer (degrees 0-100)"
    # description: header label, but if the label is a bare year / year-range
    # (a coverage annotation, e.g. "VCF0120 1984"), fall back to the question.
    bare_year = re.fullmatch(r"[0-9]{4}([-,][0-9]{4})*", lbl.strip())
    if bare_year and question:
        desc = re.split(r"[.?]", question)[0].strip()
    else:
        desc = lbl.title()
    desc = re.sub(r"\s+", " ", desc).strip()

    entries[code] = {
        "code": code,
        "label": lbl,
        "label_title": desc,
        "question": question[:500],
        "valid": valid,
        "missing": missing,
        "type": btype,
        "numeric_reason": numeric_reason,
    }

(BUILD / "parsed.json").write_text(json.dumps(entries, indent=1))

# ---- coverage + summary -----------------------------------------------------
missing_cols = [c for c in csv_cols if c not in entries and c != "Version"]
by_type = {}
for e in entries.values():
    by_type[e["type"]] = by_type.get(e["type"], 0) + 1

print(f"columns in CSV: {len(csv_cols)}  parsed entries: {len(entries)}")
print(
    f"columns without an entry (excl Version): {len(missing_cols)} -> {missing_cols[:20]}"
)
print(f"type counts: {by_type}")

numerics = [
    (c, e["type"], e["numeric_reason"])
    for c, e in entries.items()
    if e["type"] in ("INT64", "FLOAT64")
]
print(f"\n=== PROPOSED NUMERIC COLUMNS ({len(numerics)}) ===")
for c, t, r in sorted(numerics):
    print(f"  {c:10} {t:8} {r}")

print("\n=== SAMPLE ENTRIES ===")
sample = [
    "VCF0004",
    "VCF0006",
    "VCF0006a",
    "VCF0009x",
    "VCF0101",
    "VCF0102",
    "VCF0104",
    "VCF0201",
    "VCF0301",
    "VCF0112",
    "VCF0114",
    "VCF0128",
]
for c in sample:
    e = entries.get(c)
    if not e:
        print(f"  {c}: (no entry)")
        continue
    print(f"\n  {c}  [{e['type']}]  EN='{e['label_title']}'")
    if e["valid"]:
        preview = "; ".join(f"{k}={v}" for k, v in e["valid"][:5])
        print(f"     valid: {preview}{' ...' if len(e['valid']) > 5 else ''}")
    if e["missing"]:
        preview = "; ".join(f"{k}={v}" for k, v in e["missing"][:4])
        print(f"     missing: {preview}")
