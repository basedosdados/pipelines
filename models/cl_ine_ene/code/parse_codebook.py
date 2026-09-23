"""Codebook parser bounded to the variable-table sections, first occurrence wins."""

import json
import pathlib
import re

universe = [
    r["name"] for r in json.loads(pathlib.Path("universe.json").read_text())
]
# the PDF wraps long names at underscores
spellings = {c: c for c in universe}
for c in universe:
    for i, ch in enumerate(c):
        if ch == "_":
            spellings.setdefault(c[:i] + "_ " + c[i + 1 :], c)

# Longest spelling first, so `b14_rev4cl_caenes` matches before `b14`.
_ALTERNATIVES: list[str] = sorted(spellings, key=lambda name: -len(name))
PATTERN = re.compile(
    r"^("
    + "|".join(re.escape(name) for name in _ALTERNATIVES)
    + r")(?=[\s\d]|$)",
    re.M,
)
CODE = re.compile(
    r"^\s*(\d{1,5})\s*[:\-\u2013]\s*(.+?)\s*$"
)  # the PDF uses ":" or an en dash
INLINE_FIRST_CODE = re.compile(r"^(.*?\S)\s+(\d{1,5}):\s*(\S.*)$")
OBS = re.compile(
    r"vigente|descontinuad|dej[oó] de|se mantiene|reemplaz|a partir de|"
    r"producida entre|equivale a|ver detalle|anexo|corresponde|seg[uú]n |"
    r"variable construida|v[eé]ase|incorporada",
    re.I,
)
NOISE = re.compile(
    r"^\s*(\d+\s*)?$|^variable descripci|^\d+\s+[A-ZÁÉÍÓÚ ]{6,}$"
)

# line ranges holding the variable tables (1-indexed, inclusive)
BOUNDS = {"cod2020.txt": [(560, 3300)], "cod2019.txt": [(120, 1610)]}


def parse(path):
    lines = pathlib.Path(path).read_text().splitlines()
    out = {}
    for lo, hi in BOUNDS[path]:
        text = "\n".join(lines[lo - 1 : hi])
        hits = [
            (m.start(), m.end(), spellings[m.group(1)])
            for m in PATTERN.finditer(text)
        ]
        for i, (_start, end_of_name, name) in enumerate(hits):
            if name in out:  # first occurrence wins
                continue
            stop = hits[i + 1][0] if i + 1 < len(hits) else len(text)
            desc, cats, obs = [], [], []
            for raw in text[end_of_name:stop].splitlines():
                line = raw.strip()
                if not line or NOISE.match(line.lower()):
                    continue
                m = CODE.match(line)
                if m:
                    cats.append(f"{m.group(1)}: {m.group(2)}")
                elif OBS.search(line) or cats:
                    obs.append(line)
                else:
                    desc.append(line)
            # The PDF prints the first category on the same line as the
            # description ("sexo Sexo 1: Hombre"), so it lands in `desc` and the
            # category list silently starts at the second code.
            if desc:
                m = INLINE_FIRST_CODE.match(desc[0])
                if m:
                    desc[0] = m.group(1).strip()
                    cats.insert(0, f"{m.group(2)}: {m.group(3).strip()}")
            out[name] = {
                "desc": " ".join(d for d in desc if d).strip(),
                "cats": cats,
                "obs": " ".join(obs).strip(),
            }
    return out


merged = {}
for year in (2020, 2019):
    p = parse(f"cod{year}.txt")
    n = sum(1 for c in universe if p.get(c, {}).get("desc"))
    print(f"{year}: {n}/{len(universe)} with a description")
    for c in universe:
        if p.get(c, {}).get("desc") and c not in merged:
            merged[c] = dict(p[c], src=year)
pathlib.Path("cb_final.json").write_text(
    json.dumps(merged, ensure_ascii=False, indent=1)
)
miss = [c for c in universe if c not in merged]
print(f"MERGED {len(merged)}/{len(universe)}; missing ({len(miss)}): {miss}")
