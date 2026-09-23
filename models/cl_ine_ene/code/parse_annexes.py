#!/usr/bin/env python3
"""Extract the classification annexes of the current ENE codebook.

The variable tables in section 5 give only the sentinel codes for the columns that
carry a standard classification — `b1` lists "999: Sin información" and nothing
else, because the real labels sit in the annexes at the back of the PDF. Those
are the columns an economist actually reads (occupation, industry, nationality),
so without the annexes the dicionario documents everything except what matters.

  Anexo 2  UN M49 country codes        -> nacionalidad, mig3_cod, mig6_cod
  Anexo 3  CIUO-08 and CIUO-88 majors  -> b1, e16_ciuo08 / b1_ciuo88, e16_ciuo88
  Anexo 5  CAENES sections             -> b13/b14/e18_rev4cl_caenes, r_p_rev4cl_caenes

Anexo 1 (regions, provinces, communes) is deliberately NOT extracted: those codes
resolve through br_bd_diretorios_cl, which is the source of truth for them.

Run against the text dump produced by parse_codebook.py's pypdf extraction.
"""

from __future__ import annotations

import json
import pathlib
import re
import sys

import pypdf

HERE = pathlib.Path(__file__).resolve().parent

# "Afganistán 4 30001" — name, M49 code, retired ENE code. Territories added
# after the old ENE classifier was retired carry "n/a" (and Chile carries "0")
# in the third column, so it is not always five digits.
COUNTRY = re.compile(r"^(.+?)\s+(\d{1,3})\s+(\d+|n/a)(?:\s|$)")
# "1 Directores y gerentes"
MAJOR_GROUP = re.compile(r"^(\d{1,2})\s+(\D.+)$")
# "1 A Agricultura, ganadería, silvicultura y pesca"
CAENES = re.compile(r"^(\d{1,2})\s+([A-U])\s+(\D.+)$")

#: Lines that look like table rows but are page furniture: the repeated column
#: header, the page number, and the footnotes carrying URLs.
FURNITURE = re.compile(
    r"^Nombre del país|^C[oó]digo (M49|Gran grupo|Secci[oó]n)|^\d{1,3}\s*$"
    r"|^\d*\s*(Para mayores antecedentes|En 19|n/a: No aplica)"
)
#: Footnote bodies run across several lines and can be joined onto a real row.
FOOTNOTE = re.compile(
    r"https?://|Para mayores antecedentes|Naciones Unidas\) public"
)

#: Spacing artefacts pypdf introduces inside words, corrected by hand after
#: reading the rendered page. Keyed by the extracted text so a re-run either
#: applies the fix or reports that it is no longer needed.
ANNEX_FIXES = {
    "pode r ejecutivo": "poder ejecutivo",
    # CAENES section G, one of the most frequent industry codes.
    "Comerci o al por mayor": "Comercio al por mayor",
}

APPLIES_TO = {
    "countries": ["nacionalidad", "mig3_cod", "mig6_cod"],
    "ciuo08": ["b1", "e16_ciuo08"],
    "ciuo88": ["b1_ciuo88", "e16_ciuo88"],
    "caenes": [
        "b13_rev4cl_caenes",
        "b14_rev4cl_caenes",
        "e18_rev4cl_caenes",
        "r_p_rev4cl_caenes",
    ],
}


def text_of(pdf_path: pathlib.Path) -> list[str]:
    reader = pypdf.PdfReader(str(pdf_path))
    return "\n".join(
        page.extract_text() or "" for page in reader.pages
    ).splitlines()


def section(lines: list[str], start: str, end: str) -> list[str]:
    # The heading also appears in the table of contents, so take its LAST
    # occurrence — the annex itself — not the first.
    first = max(
        i for i, line in enumerate(lines) if line.strip().startswith(start)
    )
    last = next(
        i
        for i, line in enumerate(lines[first + 1 :], first + 1)
        if line.strip().startswith(end)
    )
    return lines[first:last]


def repair(text: str) -> str:
    for broken, fixed in ANNEX_FIXES.items():
        text = text.replace(broken, fixed)
    return " ".join(text.split())


def join_wrapped(lines: list[str], pattern: re.Pattern) -> list[str]:
    """Rejoin rows the PDF wrapped onto a second line, dropping page furniture."""
    out: list[str] = []
    for raw in lines:
        line = " ".join(raw.split())
        if not line or FURNITURE.match(line) or FOOTNOTE.search(line):
            continue
        if pattern.match(line) or not out:
            out.append(line)
        else:
            out[-1] = f"{out[-1]} {line}"
    return out


def parse_countries(lines: list[str]) -> dict[str, str]:
    """One country per line — deliberately NOT join-wrapped.

    The annex sits beside a multi-line footnote, and joining continuation lines
    welds that footnote onto whichever country row precedes it. Every country
    name fits on one line, so matching line by line is both simpler and safer.
    """
    labels = {}
    for raw in lines:
        line = " ".join(raw.split())
        if not line or FURNITURE.match(line) or FOOTNOTE.search(line):
            continue
        match = COUNTRY.match(line)
        if match:
            labels[match.group(2)] = repair(match.group(1))
    return labels


def parse_major_groups(lines: list[str]) -> dict[str, str]:
    labels = {}
    for line in join_wrapped(lines, MAJOR_GROUP):
        match = MAJOR_GROUP.match(line)
        if match and match.group(1) not in labels:
            labels[match.group(1)] = repair(match.group(2))
    return labels


def parse_caenes(lines: list[str]) -> dict[str, str]:
    labels = {}
    for line in join_wrapped(lines, CAENES):
        match = CAENES.match(line)
        if match:
            labels[match.group(1)] = (
                f"{match.group(2)} {repair(match.group(3))}"
            )
    return labels


def main():
    pdf = (
        pathlib.Path(sys.argv[1])
        if len(sys.argv) > 1
        else (
            pathlib.Path.home()
            / "Downloads/cl_ine_ene_data/docs/codigos-ene-2020.pdf"
        )
    )
    lines = text_of(pdf)

    countries = parse_countries(
        section(
            lines,
            "Anexo 2. Clasificación de países",
            "Anexo 3. Clasificación de grupo ocupacional",
        )
    )
    occupation = section(
        lines,
        "Anexo 3. Clasificación de grupo ocupacional",
        "Anexo 4. Modificación",
    )
    # The annex prints CIUO-08 first, then CIUO-88 after a paragraph naming the
    # older variables; split on that sentence rather than on a line count.
    split_at = next(
        i for i, line in enumerate(occupation) if "versión 88" in line
    )
    ciuo08 = parse_major_groups(occupation[:split_at])
    ciuo88 = parse_major_groups(occupation[split_at:])
    caenes = parse_caenes(
        section(
            lines,
            "Anexo 5. Clasificación de rama",
            "Anexo 6. Variables descontinuadas",
        )
    )

    tables = {
        "countries": countries,
        "ciuo08": ciuo08,
        "ciuo88": ciuo88,
        "caenes": caenes,
    }
    for name, table in tables.items():
        print(f"{name}: {len(table)} codes  e.g. {list(table.items())[:2]}")

    if (
        len(countries) < 240
        or len(ciuo08) != 10
        or len(ciuo88) != 10
        or len(caenes) != 21
    ):
        raise SystemExit(
            "annex extraction looks wrong — expected 240+ countries, 10 CIUO-08, "
            "10 CIUO-88 and 21 CAENES sections"
        )

    out = {
        "_comment": __doc__.strip(),
        "applies_to": APPLIES_TO,
        "tables": tables,
    }
    path = HERE / "codebook_annexes.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=1) + "\n")
    print(f"\nwrote {path}")


if __name__ == "__main__":
    main()
