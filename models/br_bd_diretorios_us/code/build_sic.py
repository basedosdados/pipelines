"""Build the br_bd_diretorios_us.sic directory: the 1987 SIC classification.

SIC is a recurring entity in US data (SEC EDGAR filer classification, QCEW's
pre-2001 series, older Census products), so the code-to-label mapping belongs in
a directory rather than in each dataset's dictionary.

Two public-domain federal sources are combined:

- **OSHA's SIC manual** (https://www.osha.gov/data/sic-manual) — the full 1987
  hierarchy: division, major group (2 digits), industry group (3 digits) and
  industry (4 digits).
- **SEC's EDGAR SIC code list**
  (https://www.sec.gov/search-filings/standard-industrial-classification-sic-code-list)
  — the subset EDGAR assigns to filers, with the SEC's own industry title and
  the reviewing office.

One row per code at every level of the hierarchy, keyed by a 4-digit `id_sic`
and distinguished by `level`, because filers are classified at whichever level
fits:

| level | shape | meaning | example |
|---|---|---|---|
| 2 | `NN00` | major group | `2000` Food And Kindred Products |
| 3 | `NNN0` | industry group | `2020` Dairy Products |
| 4 | `NNNN` | industry | `2021` Creamery Butter |

The padding is unambiguous: no 1987 four-digit industry code ends in 0 (verified
against the manual — 1,004 industries, none ending in 0), so `NN00` and `NNN0`
can only mean the coarser level.

`id_sec_office` is non-null exactly for the codes EDGAR uses. A few EDGAR codes
have no 1987 counterpart at any level (e.g. 6770 blank checks, 8880 American
depositary receipts); they are kept, so the directory covers every value that
appears in `us_sec_edgar.submission`.

OSHA's index lists major group 99 under Division J; the 1987 standard puts it in
Division K, Nonclassifiable Establishments, and that correction is applied here.

    uv run python models/br_bd_diretorios_us/code/build_sic.py

Writes `<outdir>/sic/data.parquet` (all-STRING staging) plus a CSV copy beside
it for review. Both go to the scratch directory, never into the repo.
"""

import argparse
import csv
import html
import os
import re
import time

import pyarrow as pa
import pyarrow.parquet as pq
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
USER_AGENT = "Data Basis (Base dos Dados) rdahis@basedosdados.org"
OSHA_INDEX = "https://www.osha.gov/data/sic-manual"
OSHA_MAJOR_GROUP = "https://www.osha.gov/data/sic-manual/major-group-{code}"
SEC_LIST = (
    "https://www.sec.gov/search-filings/"
    "standard-industrial-classification-sic-code-list"
)
REQUEST_INTERVAL_SECONDS = 0.5

# OSHA's index nests major group 99 under Division J's list; in the 1987
# standard it is its own division.
DIVISION_OVERRIDE = {"99": ("K", "Nonclassifiable Establishments")}

COLUMNS = [
    "id_sic",
    "name",
    "level",
    "id_industry_group",
    "name_industry_group",
    "id_major_group",
    "name_major_group",
    "id_division",
    "name_division",
    "name_sec",
    "id_sec_office",
]


def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def _get(session: requests.Session, url: str) -> str:
    time.sleep(REQUEST_INTERVAL_SECONDS)
    response = session.get(url, timeout=120)
    response.raise_for_status()
    return response.text


def _clean(text: str) -> str:
    return re.sub(
        r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", text))
    ).strip()


def parse_index(page: str):
    """Divisions and major groups, in document order.

    The index lists each division heading followed by its major-group links, so
    a single ordered scan assigns every major group to the division above it.
    """
    divisions, major_groups = {}, {}
    pattern = re.compile(
        r'href="/data/sic-manual/division-([a-z])"[^>]*>([^<]*)'
        r'|href="/data/sic-manual/major-group-(\d{2})"[^>]*>([^<]*)'
    )
    current = None
    for match in pattern.finditer(page):
        letter, division_name, code, group_name = match.groups()
        if letter:
            current = letter.upper()
            name = _clean(division_name)
            name = re.sub(r"^Division\s+[A-Z]:\s*", "", name)
            if name:
                divisions[current] = name
        else:
            name = _clean(group_name)
            name = re.sub(r"^Major Group\s+\d{2}:\s*", "", name)
            major_groups[code] = (name, current)
    return divisions, major_groups


def parse_major_group(page: str):
    """Industry groups and their 4-digit industries from one major-group page."""
    out = []
    blocks = re.split(r"<strong>\s*Industry Group\s*", page)[1:]
    for block in blocks:
        header = re.match(r"(\d{3}):\s*(.*?)</strong>", block, flags=re.S)
        if not header:
            continue
        group_code, group_name = header.group(1), _clean(header.group(2))
        for code, name in re.findall(
            r"<li>\s*(\d{4})\s*<a[^>]*>(.*?)</a>", block, flags=re.S
        ):
            out.append((code, _clean(name), group_code, group_name))
    return out


def parse_sec(page: str):
    """EDGAR's SIC subset: code -> (office, industry title)."""
    out = {}
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", page, flags=re.S):
        cells = [
            _clean(c)
            for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, flags=re.S)
        ]
        if len(cells) == 3 and cells[0].isdigit():
            out[cells[0].zfill(4)] = (cells[1], cells[2])
    return out


def build(session: requests.Session):
    divisions, major_groups = parse_index(_get(session, OSHA_INDEX))
    print(f"divisions: {len(divisions)}  major groups: {len(major_groups)}")

    def hierarchy(major: str):
        group_name, division = major_groups.get(major, (None, None))
        if major in DIVISION_OVERRIDE:
            division, division_name = DIVISION_OVERRIDE[major]
        else:
            division_name = divisions.get(division) if division else None
        return group_name, division, division_name

    rows = {}

    def put(sic, name, level, industry_group=None, industry_group_name=None):
        major = sic[:2]
        group_name, division, division_name = hierarchy(major)
        rows[sic] = {
            "id_sic": sic,
            "name": name,
            "level": str(level),
            "id_industry_group": industry_group,
            "name_industry_group": industry_group_name,
            "id_major_group": major,
            "name_major_group": group_name,
            "id_division": division,
            "name_division": division_name,
            "name_sec": None,
            "id_sec_office": None,
        }

    for code in sorted(major_groups):
        group_name, _ = major_groups[code]
        put(code + "00", group_name, 2)
        page = _get(session, OSHA_MAJOR_GROUP.format(code=code))
        for sic, name, ig_code, ig_name in parse_major_group(page):
            if ig_code + "0" not in rows:
                put(ig_code + "0", ig_name, 3, ig_code, ig_name)
            put(sic, name, 4, ig_code, ig_name)

    levels = {}
    for row in rows.values():
        levels[row["level"]] = levels.get(row["level"], 0) + 1
    print(f"1987 SIC rows by level: {dict(sorted(levels.items()))}")

    sec = parse_sec(_get(session, SEC_LIST))
    print(f"EDGAR SIC codes: {len(sec)}")
    edgar_only = []
    for sic, (office, title) in sec.items():
        if sic not in rows:
            level = 2 if sic.endswith("00") else 3 if sic.endswith("0") else 4
            put(
                sic,
                title,
                level,
                sic[:3] if level >= 3 else None,
                None,
            )
            edgar_only.append(sic)
        rows[sic]["name_sec"] = title
        rows[sic]["id_sec_office"] = office
    print(
        f"EDGAR codes with no 1987 counterpart ({len(edgar_only)}): "
        + ", ".join(edgar_only)
    )
    return [rows[k] for k in sorted(rows)]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--outdir",
        default=os.path.expanduser(
            "~/Downloads/br_bd_diretorios_us_data/output"
        ),
    )
    args = parser.parse_args()

    rows = build(_session())

    os.makedirs(args.outdir, exist_ok=True)
    csv_path = os.path.join(args.outdir, "sic.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    table = pa.Table.from_arrays(
        [pa.array([r[c] for r in rows], type=pa.string()) for c in COLUMNS],
        names=COLUMNS,
    )
    part_dir = os.path.join(args.outdir, "sic")
    os.makedirs(part_dir, exist_ok=True)
    pq.write_table(
        table, os.path.join(part_dir, "data.parquet"), compression="snappy"
    )
    print(f"{len(rows)} rows -> {part_dir}/data.parquet and {csv_path}")


if __name__ == "__main__":
    main()
