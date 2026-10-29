"""Parse the BLS hierarchical grouping (stub) files into the ``ucc`` table.

Source: ``docs/stubs.zip`` -> ``stubs/CE-HG-{Integ,Inter,Diary}-YYYY.txt``
(Integ 1996-2024, Inter and Diary 1997-2024).

The files are fixed-width, but the layout is not constant across years:

* Every ``1`` (row) line has the record type in column 1, the level in columns
  2-5, the title in columns 7-69 and the UCC or mnemonic in columns 70-79.
* Layout A (most years): type at column 80, then factor and section as
  whitespace-separated tokens.
* Layout B (Integ 1998-2000; all three groupings 2013-2020): one extra
  one-character field at column 80 (``-``, ``G``, ``I`` or blank, undocumented
  and not in the architecture), and the type at column 83.
  The layout is detected per line: B when column 83 holds a type letter.
* A ``2`` line continues the title of the previous row; its text is appended.
* ``*`` lines exist only up to 2003. ``*  *  Text:`` lines are untyped section
  headings without code or level (later files turn them into ``T`` rows); they
  are kept with ``row_type = '*'`` and NULL level, ucc, factor and section.
  Other ``*`` lines are free-text change notes ("NEW UCC IN COLLECTION Q961")
  and are dropped, along with any continuation line that follows them.

``parent_ucc`` is the code of the nearest preceding row whose level is one
less; headings are not levelled and take part in neither side of that link.

Usage:
    uv run models/us_bls_cex/code/clean_ucc.py [--years 1996 2024]
"""

import argparse
import json
import logging
import re
import time
import zipfile
from collections import Counter

import pyarrow as pa

from pipelines.datasets.us_bls_cex.pumd_files import (
    DATA_DIR,
    DOCS_DIR,
    OUTPUT_DIR,
)
from pipelines.datasets.us_bls_cex.utils import write_table

STUBS_ZIP = DOCS_DIR / "stubs.zip"
GROUPING = {"Integ": "integrated", "Inter": "interview", "Diary": "diary"}
SECTIONS = {"CUCHARS", "EXPEND", "INCOME", "ASSETS", "ADDENDA", "FOOD"}
TYPES = "HTGSID"
_NAME = re.compile(r"stubs/CE-HG-(Integ|Inter|Diary)-(\d{4})\.txt$")
# Fallback for a row whose code column is empty and shifted (Integ 2012 has one).
_LOOSE = re.compile(
    r"^(?P<title>.*?)\s+(?:(?P<code>[0-9A-Z]{3,})\s+)?(?P<type>[HTGSID])\s+"
    r"(?P<factor>-?\d+)\s+(?P<section>[A-Z]+)\s*(?P<extra>.*)$"
)

log = logging.getLogger("clean_ucc")


def parse_row(line: str) -> dict:
    """Parse one ``1`` line into its fields; raises on an unknown layout."""
    pad = line.ljust(90)
    level = pad[1:6].strip()
    if not level.isdigit():
        raise ValueError(f"bad level: {line!r}")
    if pad[82] in TYPES and pad[80:82] == "  ":  # layout B
        code, src, rtype, rest = pad[69:79], pad[79].strip(), pad[82], pad[83:]
        layout = "B"
    else:
        code, src, rtype, rest = pad[69:79], "", pad[79], pad[80:]
        layout = "A"
    tokens = rest.split()
    ok = (
        pad[68] == " "
        and rtype in TYPES
        and len(tokens) >= 2
        and re.fullmatch(r"-?\d+", tokens[0])
        and tokens[1] in SECTIONS
    )
    if ok:
        return {
            "level": level,
            "title": pad[6:69].strip(),
            "ucc": code.strip() or None,
            "row_type": rtype,
            "factor": tokens[0],
            "section": tokens[1],
            "_src": src,
            "_extra": " ".join(tokens[2:]),
            "_layout": layout,
        }
    m = _LOOSE.match(pad[6:].rstrip())
    if not m or m.group("section") not in SECTIONS:
        raise ValueError(f"unparseable stub row: {line!r}")
    return {
        "level": level,
        "title": m.group("title").strip(),
        "ucc": m.group("code"),
        "row_type": m.group("type"),
        "factor": m.group("factor"),
        "section": m.group("section"),
        "_src": "",
        "_extra": m.group("extra"),
        "_layout": "loose",
    }


def parse_file(
    text: str, year: int, grouping: str, stats: Counter
) -> list[dict]:
    rows: list[dict] = []
    prev_kind = None  # what the previous non-blank line produced
    for raw in text.split("\n"):
        line = raw.rstrip("\r")
        if not line.strip():
            continue
        kind = line[0]
        if kind == "1":
            r = parse_row(line)
            stats[f"layout_{r['_layout']}"] += 1
            if r["_src"]:
                stats[f"extra_field_{r['_src']}"] += 1
            if r["_extra"]:
                stats["trailing_tokens"] += 1
                log.warning(
                    f"{grouping} {year}: trailing {r['_extra']!r}: {line.strip()}"
                )
            if r["ucc"] is None:
                stats["rows_without_code"] += 1
                log.warning(
                    f"{grouping} {year}: row without code: {line.strip()}"
                )
            rows.append(r)
            prev_kind = "row"
        elif kind == "2":
            text2 = line[6:69].strip() if len(line) > 6 else ""
            if prev_kind in ("row", "heading") and rows:
                t = rows[-1]["title"]
                sep = "" if t.endswith("-") else " "
                rows[-1]["title"] = f"{t}{sep}{text2}".strip()
                stats["continuations_merged"] += 1
            else:
                stats["continuations_dropped"] += 1
        elif kind == "*":
            if line[1:3] == "  " and line[3:4] == "*":
                rows.append(
                    {
                        "level": None,
                        "title": line[6:].strip(),
                        "ucc": None,
                        "row_type": "*",
                        "factor": None,
                        "section": None,
                    }
                )
                stats["headings_kept"] += 1
                prev_kind = "heading"
            else:
                stats["comments_dropped"] += 1
                prev_kind = "comment"
        else:
            raise ValueError(
                f"{grouping} {year}: unknown record type {line!r}"
            )

    # parent_ucc from levels; headings are skipped
    last_at_level: dict[int, str | None] = {}
    for i, r in enumerate(rows, start=1):
        r["year"] = str(year)
        r["hierarchy"] = grouping
        r["line_number"] = str(i)
        if r["level"] is None:
            r["parent_ucc"] = None
            continue
        lv = int(r["level"])
        r["parent_ucc"] = last_at_level.get(lv - 1) if lv > 1 else None
        if lv > 1 and (lv - 1) not in last_at_level:
            stats["orphan_rows"] += 1
        last_at_level[lv] = r["ucc"]
        for deeper in [k for k in last_at_level if k > lv]:
            del last_at_level[deeper]
    return rows


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", nargs="*", type=int)
    args = ap.parse_args()
    t0 = time.time()
    stats: Counter = Counter()
    rows: list[dict] = []
    with zipfile.ZipFile(STUBS_ZIP) as zf:
        members = sorted(n for n in zf.namelist() if _NAME.search(n))
        for name in members:
            m = _NAME.search(name)
            assert m is not None
            g, y = m.groups()
            if args.years and int(y) not in args.years:
                continue
            text = zf.read(name).decode("latin-1")
            rows.extend(parse_file(text, int(y), GROUPING[g], stats))
            stats["files"] += 1
    at = pa.Table.from_pylist(
        [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]
    )
    write_table(at, "ucc", OUTPUT_DIR, partition="year")
    stats["rows"] = len(rows)
    (DATA_DIR / "logs").mkdir(parents=True, exist_ok=True)
    with open(DATA_DIR / "logs" / "ucc.json", "w") as fh:
        json.dump(dict(stats), fh, indent=1)
    log.info(f"ucc: {dict(stats)} in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
