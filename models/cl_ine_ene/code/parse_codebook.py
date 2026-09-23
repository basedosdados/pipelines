"""Extract the variable tables of the two INE ENE codebooks.

Anchors on the known column names inside the bounded section-5 line ranges, so
prose from the surrounding methodology cannot be mistaken for a variable entry,
and the first occurrence of each name wins. The classification annexes at the
back of the PDFs are handled separately by parse_annexes.py.

    python models/cl_ine_ene/code/parse_codebook.py
"""

from __future__ import annotations

import json
import os
import pathlib
import re

import pypdf

HERE = pathlib.Path(__file__).resolve().parent
DOCS = (
    pathlib.Path(
        os.environ.get(
            "CL_INE_ENE_DATA",
            pathlib.Path.home() / "Downloads/cl_ine_ene_data",
        )
    )
    / "docs"
)

universe = [
    r["name"] for r in json.loads((HERE / "column_universe.json").read_text())
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
    r"^\s*(\d{1,5})\s*[:.\-\u2013]\s*(.+?)\s*$"
)  # ":" mostly, but the PDF also has "1." and an en dash
INLINE_FIRST_CODE = re.compile(r"^(.*?\S)\s+(\d{1,5}):\s*(\S.*)$")
# A value RANGE ("1 - 168", "2010 - 2026") flattens to look exactly like a
# code/label pair once the en dash is lost. Checked AFTER the Observaciones tail
# is trimmed, so "11 Variable construida..." reduces to a bare "11" and is
# rejected, while a genuine label that happens to start with a digit
# ("15 a 19 años", "10 Chango") is kept.
RANGE_NOT_LABEL = re.compile(r"^\d+$")
# Where the Observaciones column bleeds into the label text.
LABEL_TAIL = re.compile(
    r"\s+(?=Descontinuad|Vigente|Variable |Incorporad|V[eé]ase|Se[nñ]ala|Corresponde|"
    r"Equivale|Ver detalle|Seg[uú]n |Pregunta abierta)"
)
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
#: Line ranges holding the variable tables, 1-indexed and inclusive. Outside them
#: the documents are prose, and a bare word like "nivel" or "edad" is an ordinary
#: noun rather than a variable name.
BOUNDS = {2020: (560, 3300), 2019: (120, 1610)}


def text_lines(year: int) -> list[str]:
    """The codebook's flattened text, extracted the same way every run."""
    reader = pypdf.PdfReader(str(DOCS / f"codigos-ene-{year}.pdf"))
    return "\n".join(
        page.extract_text() or "" for page in reader.pages
    ).splitlines()


def parse(year: int):
    lines = text_lines(year)
    out = {}
    for lo, hi in [BOUNDS[year]]:
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
                    label = LABEL_TAIL.split(m.group(2), maxsplit=1)[0].strip()
                    if label and not RANGE_NOT_LABEL.match(label):
                        cats.append(f"{m.group(1)}: {label}")
                    else:
                        obs.append(line)
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
                    label = LABEL_TAIL.split(m.group(3), maxsplit=1)[0].strip()
                    if label and not RANGE_NOT_LABEL.match(label):
                        desc[0] = m.group(1).strip()
                        cats.insert(0, f"{m.group(2)}: {label}")
            out[name] = {
                "desc": " ".join(d for d in desc if d).strip(),
                "cats": cats,
                "obs": " ".join(obs).strip(),
            }
    return out


merged = {}
for year in (2020, 2019):
    p = parse(year)
    n = sum(1 for c in universe if p.get(c, {}).get("desc"))
    print(f"{year}: {n}/{len(universe)} with a description")
    for c in universe:
        if p.get(c, {}).get("desc") and c not in merged:
            merged[c] = dict(p[c], src=year)
(HERE / "codebook_parsed.json").write_text(
    json.dumps(merged, ensure_ascii=False, indent=1)
)
miss = [c for c in universe if c not in merged]
print(f"MERGED {len(merged)}/{len(universe)}; missing ({len(miss)}): {miss}")
