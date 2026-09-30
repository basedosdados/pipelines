"""Parse INDEC's 'Diseno de registros' PDF into per-column type, description
and value labels.

Why this exists. The Stata era (2003 Q3 - 2015 Q2) ships variable labels and
value labels inside the .dta files, so those columns describe themselves. The
TXT era (2016 Q2 on) ships none, and the 2023 Q4 questionnaire redesign added
around 70 columns that consequently have no description or code list anywhere
in the data. The record-layout PDF is the only machine-readable source for them.

Layout of the PDF, after `pdftotext -layout`:

    NAME        N (1)      First line of the description
                           continuation of the description
                           1 = Si
                           2 = No

The type column is occasionally absent (PP03K). Descriptions wrap over several
lines. Value codes appear as `<code> = <label>` lines beneath their column.

One naming quirk is handled here: the household receipt indicators are called
V5_1 / V5_2 / V5_3 / V11_1 / V11_2 in the PDF but V5_01 / V5_02 / V5_03 /
V11_01 / V11_02 in the data. Lookup therefore also tries the zero-padded and
unpadded spellings of a trailing numeric suffix.
"""

import json
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import CODE_DIR, DATA_DIR

PDF = DATA_DIR / "docs" / "EPH_registro_1T2026.pdf"
PDF_FALLBACK = DATA_DIR / "docs" / "EPH_registro_4T2024.pdf"

# A column row: NAME, optional "N (8)" / "C (6)" / "date", then the description.
ROW = re.compile(
    r"^(\s{0,24})([A-Z][A-Z0-9_]{1,18})"
    r"(?:\s{2,}(?:([NC])\s*\(\s*(\d+)\s*\)|(date)))?"
    r"\s{2,}(\S.*)$"
)
# A value-label line: "1 = Si", "99 = Ns./Nr."
VALUE = re.compile(r"^\s*(-?\d{1,3})\s*=\s*(\S.*?)\s*$")
FOOTER = re.compile(
    r"Direcci.n de Encuesta Permanente de Hogares|^\s*\d+\s*/\s*\d+\s*$"
)
# A type declaration sitting alone on its own line, or trailing a description.
# PP03K is laid out as "PP03K  <desc>" with "N (1)" on the following line.
LONE_TYPE = re.compile(r"^\s*(?:([NC])\s*\(\s*(\d+)\s*\)|(date))\s*$")
TRAILING_TYPE = re.compile(r"\s*(?:[NC]\s*\(\s*\d+\s*\)|date)\s*$")
# A description stops at a section heading. Without this the last column of a
# section absorbs the prose that follows it: p_adeccf, last in the Personas
# section, swallowed the whole of "Anexo I. Recomendaciones tecnicas para el uso
# de la informacion de ingresos" and reached 2115 characters, over BigQuery's
# 1024-character limit for a column description.
SECTION_STOP = re.compile(
    r"^\s*(Anexo\b|Recomendaciones\s+t.cnicas|Montos\s+de\s+ingresos\b"
    r"|Dise.o\s+de\s+registros\b|Informaci.n\s+para\s+las\s+personas\s+usuarias)",
    re.I,
)
# A single column description never legitimately runs this long in this document;
# anything beyond it means continuation lines are being mis-attributed.
MAX_DESCRIPTION = 400

HOGAR_HEAD = re.compile(r"Dise.o de registros de la base Hogar", re.I)
PERSON_HEAD = re.compile(r"Dise.o de registros de la base Personas", re.I)


def pdf_lines(path: Path) -> list[str]:
    out = subprocess.run(
        ["pdftotext", "-layout", str(path), "-"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [
        line for line in out.stdout.splitlines() if not FOOTER.search(line)
    ]


def clean(text: str) -> str:
    text = re.sub(r"\s{2,}", " ", text).strip()
    text = text.replace("“", '"').replace("”", '"')
    # pdftotext sometimes places the type column after the description, which
    # would otherwise leave "...porque N (1)" as the description (PP03K).
    text = TRAILING_TYPE.sub("", text)
    return text.strip().rstrip(".")


def parse_section(body: list[str]) -> dict[str, dict]:
    cols: dict[str, dict] = {}
    current: str | None = None
    last_value: str | None = None
    indent = 0
    for raw in body:
        line = raw.rstrip()
        if not line.strip():
            continue
        m = ROW.match(line)
        vm = VALUE.match(line)
        # A value line must be attributed to the column above it, and must not
        # be mistaken for a new column.
        if vm and current:
            code, label = vm.group(1), clean(vm.group(2))
            cols[current]["values"].setdefault(str(int(code)), label)
            last_value = str(int(code))
            continue
        # A type declaration alone on its own line belongs to the column above.
        if SECTION_STOP.match(line):
            current = None
            last_value = None
            continue
        lt = LONE_TYPE.match(line)
        if lt and current:
            kind, width, is_date = lt.groups()
            if not cols[current]["declared_type"]:
                cols[current]["declared_type"] = "date" if is_date else kind
            if width and not cols[current]["width"]:
                cols[current]["width"] = int(width)
            continue
        if m:
            _lead, name, kind, width, is_date, desc = m.groups()
            desc = clean(desc)
            current = name.upper()
            last_value = None
            # Some columns (EMPLEO, SECTOR) carry no description at all: their
            # first code sits where the description would be. Record it as a
            # value and leave the description empty for overrides.py to supply.
            inline_value = VALUE.match(desc)
            entry = cols.setdefault(
                current,
                {
                    "declared_type": "date" if is_date else (kind or None),
                    "width": int(width) if width else None,
                    "description": "",
                    "values": {},
                },
            )
            if inline_value:
                last_value = str(int(inline_value.group(1)))
                entry["values"].setdefault(
                    last_value, clean(inline_value.group(2))
                )
            elif len(desc) >= 3 and len(desc) > len(entry["description"]):
                entry["description"] = desc
            if kind and not entry["declared_type"]:
                entry["declared_type"] = "date" if is_date else kind
            indent = len(raw) - len(raw.lstrip())
            continue
        # Continuation of the current description: deeper indent, no name, and
        # only before any value line -- text after the codes belongs to them.
        if current and (len(raw) - len(raw.lstrip())) > indent:
            extra = clean(line)
            if not extra:
                continue
            if cols[current]["values"]:
                # Text after the codes began is the wrapped tail of the last
                # code's label (PP02A code 2 wraps onto a second line).
                if last_value is not None:
                    cols[current]["values"][last_value] = clean(
                        cols[current]["values"][last_value] + " " + extra
                    )
            elif len(cols[current]["description"]) < MAX_DESCRIPTION:
                cols[current]["description"] = clean(
                    (cols[current]["description"] + " " + extra).strip()
                )
    return cols


def parse(path: Path = PDF) -> dict[str, dict[str, dict]]:
    lines = pdf_lines(path)
    h_idx = [i for i, line in enumerate(lines) if HOGAR_HEAD.search(line)]
    p_idx = [i for i, line in enumerate(lines) if PERSON_HEAD.search(line)]
    if not h_idx or not p_idx:
        raise RuntimeError(
            f"{path.name}: no Hogar / Personas section headings"
        )
    h_start, p_start = h_idx[-1], p_idx[-1]
    return {
        "microdatos_hogar": parse_section(lines[h_start:p_start]),
        "microdatos_individuo": parse_section(lines[p_start:]),
    }


def suffix_aliases(name: str) -> list[str]:
    """V5_01 <-> V5_1, PP04B_COD -> itself. Handles the PDF/data suffix mismatch."""
    out = [name]
    m = re.match(r"^(.*_)0(\d)$", name)
    if m:
        out.append(f"{m.group(1)}{m.group(2)}")
    m = re.match(r"^(.*_)(\d)$", name)
    if m:
        out.append(f"{m.group(1)}0{m.group(2)}")
    # A hogar indicator may only be documented via its _M amount counterpart.
    out.append(f"{name}_M")
    m = re.match(r"^(.*_)0(\d)$", name)
    if m:
        out.append(f"{m.group(1)}{m.group(2)}_M")
    return out


def lookup(parsed_table: dict[str, dict], name: str) -> dict | None:
    for alias in suffix_aliases(name):
        if alias in parsed_table:
            return parsed_table[alias]
    return None


def main() -> int:
    parsed = parse()
    fallback = parse(PDF_FALLBACK) if PDF_FALLBACK.exists() else {}
    for table, cols in parsed.items():
        for name, entry in (fallback.get(table) or {}).items():
            if name not in cols:
                cols[name] = entry
            elif len(entry["description"]) > len(cols[name]["description"]):
                cols[name]["description"] = entry["description"]

    (CODE_DIR / "registro_parsed.json").write_text(
        json.dumps(parsed, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    universe = json.loads(
        (CODE_DIR / "column_universe.json").read_text(encoding="utf-8")
    )
    labels = json.loads(
        (CODE_DIR / "source_labels.json").read_text(encoding="utf-8")
    )
    for table, cols in parsed.items():
        n_vals = sum(1 for e in cols.values() if e["values"])
        need = {c for c in universe[table] if c and c not in labels[table]}
        got = {c for c in need if lookup(cols, c)}
        print(
            f"{table}: {len(cols)} columns parsed, {n_vals} with value labels"
        )
        print(
            f"   unlabelled in data: {len(need)}   recovered from PDF: {len(got)}"
        )
        missing = sorted(need - got)
        if missing:
            print(f"   still missing ({len(missing)}): {missing}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
