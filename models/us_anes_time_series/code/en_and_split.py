# ruff: noqa: SIM115
"""Finalize EN descriptions from parsed.json and split into chunks for parallel
translation. Writes build/en_final.json (code->EN) and build/chunks/en_XX.tsv."""

import csv
import json
import re
from pathlib import Path

B = Path(__file__).resolve().parent / "build"
e = json.load(open(B / "parsed.json"))
csv_cols = [
    c.strip()
    for c in open(B / "csv_cols.txt")
    if c.strip() and c.strip() != "Version"
]

ACR = {
    "ftf": "FTF",
    "dk": "DK",
    "na": "NA",
    "rf": "RF",
    "sei": "SEI",
    "gop": "GOP",
    "tv": "TV",
    "iw": "IW",
    "pid": "PID",
    "aca": "ACA",
    "wwii": "WWII",
    "vcf": "VCF",
    "id": "ID",
}


def sentence(label: str) -> str:
    s = re.sub(r"\s+", " ", label.strip())
    s = re.sub(r"respondent-\s*", "respondent - ", s, flags=re.I)
    s = s.lower()

    # acronym / token restore
    def fix(m):
        w = m.group(0)
        if re.fullmatch(r"(u\.s\.?|u\.s\.a\.?)", w):
            return "U.S.A." if w.rstrip(".").endswith("a") else "U.S."
        return ACR.get(w, w)

    s = re.sub(r"[a-z]+(?:\.[a-z]+)*\.?", fix, s)
    s = s[:1].upper() + s[1:]
    return s.strip().rstrip(".")  # no trailing period (BD column-desc rule)


en = {c: sentence(e[c]["label_title"]) for c in csv_cols if c in e}
json.dump(en, open(B / "en_final.json", "w"), ensure_ascii=False, indent=0)

# split into N chunks
N = 8
codes = list(en.keys())
chunks_dir = B / "chunks"
chunks_dir.mkdir(exist_ok=True)
size = (len(codes) + N - 1) // N
for k in range(N):
    part = codes[k * size : (k + 1) * size]
    if not part:
        continue
    with open(chunks_dir / f"en_{k:02d}.tsv", "w") as f:
        w = csv.writer(f, delimiter="\t")
        for c in part:
            w.writerow([c, en[c]])
print(
    f"EN finalized: {len(en)} descriptions; {N} chunks of ~{size} in {chunks_dir}"
)
print("sample:", list(en.items())[:3])
