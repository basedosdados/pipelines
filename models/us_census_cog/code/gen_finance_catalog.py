"""Extract the finance variable catalogue from the source User Guide.

``UserGuide.xls`` ships inside the 1967-2012 archive and is the only place the
Census publishes the mapping from the wide file's column labels to its finance
item codes. The wide file's header carries labels ("Property Tax"); the long
2013-2018 files carry codes ("T01"); joining the two eras needs this table.

The catalogue is written to ``architecture/finance_item_catalog.csv`` and
committed, so neither the cleaning transform nor the recurring pipeline has to
read an .xls at run time.

    python gen_finance_catalog.py
"""

import csv
import re
import zipfile
from pathlib import Path

from common import INPUT

ARCH = Path(__file__).resolve().parent / "architecture"
# The wide file's first 24 columns are reference fields, not finance items; the
# catalogue's numbering starts at "Total Revenue".
FIRST_FINANCE_VARIABLE = 25


def read_user_guide() -> list[dict]:
    """Return one row per numbered variable in the User Guide."""
    import xlrd

    archive = INPUT / "fin" / "IndFin_1967_2012.zip"
    guide = Path("/tmp/cog_user_guide.xls")
    with zipfile.ZipFile(archive) as zf:
        guide.write_bytes(zf.read("UserGuide.xls"))
    sheet = xlrd.open_workbook(guide).sheet_by_name("2. Variables")

    def cell(row: int, column: int) -> str:
        value = sheet.cell_value(row, column)
        if isinstance(value, float):
            return str(int(value)) if value == int(value) else str(value)
        return str(value).strip()

    rows = []
    for row in range(sheet.nrows):
        if re.fullmatch(r"\d+", cell(row, 1)):
            rows.append(
                {
                    "number": int(cell(row, 1)),
                    "item_code": cell(row, 2),
                    "variable_name": cell(row, 4),
                    "sas_name": cell(row, 5),
                    "description": cell(row, 7).rstrip(". "),
                }
            )
    guide.unlink(missing_ok=True)
    return rows


def wide_header() -> list[str]:
    """Return the wide file's column labels in file order, a then b then c.

    Files b and c repeat the three key columns, which are dropped so the
    sequence lines up one-to-one with the catalogue.
    """
    archive = INPUT / "fin" / "IndFin_1967_2012.zip"
    header: list[str] = []
    with zipfile.ZipFile(archive) as zf:
        for part in "abc":
            with zf.open(f"IndFin12{part}.Txt") as fh:
                line = fh.readline().decode("latin-1")
            names = next(csv.reader([line]))
            header += names if part == "a" else names[3:]
    return header


def item_type(code: str) -> str:
    """Classify a catalogue entry by what its code means.

    A three-character code is an item the Census collects. A code containing a
    dash is a subtotal over a family of codes. Everything else is an aggregate
    the Census derives, and summing derived rows double-counts.
    """
    if code and "-" not in code and code != "DDD" and len(code) == 3:
        return "item"
    if code and "-" in code:
        return "subtotal"
    return "derived"


def main() -> None:
    """Write the finance item catalogue."""
    catalog = read_user_guide()
    header = wide_header()
    if len(header) != len(catalog) + 1:
        raise SystemExit(
            f"catalogue and header disagree: {len(catalog)} vs {len(header)}"
        )
    # The header's first column, SortCode, carries no catalogue row, so label
    # i lines up with catalogue entry i-1 from there on.
    rows = []
    for index, label in enumerate(header):
        if index < FIRST_FINANCE_VARIABLE:
            continue
        entry = catalog[index - 1]
        code = entry["item_code"]
        rows.append(
            {
                "column_label": label.strip(),
                "item_code": (
                    code
                    if item_type(code) == "item"
                    else entry["variable_name"]
                ),
                "source_item_code": code,
                "item_type": item_type(code),
                "variable_name": entry["variable_name"],
                "description": entry["description"],
            }
        )

    # Eight item codes are printed against two columns each, and the User Guide
    # gives four of those pairs the same variable name too. They are not
    # redundant: measured on 30,000 records of fiscal 2012, three of the four
    # pairs disagree on 64 to 637 records, so both columns carry real and
    # different values. Whenever a key would collide, every column in the
    # colliding group falls back to a slug of its own label, which is unique by
    # construction. The Census code stays in source_item_code so users can still
    # group by it.
    def slug(label: str) -> str:
        return re.sub(
            r"_+", "_", re.sub(r"[^a-z0-9]+", "_", label.lower())
        ).strip("_")

    by_key: dict[str, list[dict]] = {}
    for row in rows:
        by_key.setdefault(row["item_code"], []).append(row)
    for group in by_key.values():
        if len(group) > 1:
            for row in group:
                row["item_code"] = slug(row["column_label"])

    path = ARCH / "finance_item_catalog.csv"
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=list(rows[0]), lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)
    kinds: dict[str, int] = {}
    for row in rows:
        kinds[row["item_type"]] = kinds.get(row["item_type"], 0) + 1
    collisions = len(rows) - len({r["item_code"] for r in rows})
    print(
        f"{path.name}: {len(rows)} variables {kinds} collisions={collisions}"
    )
    if collisions:
        raise SystemExit("item_code is not unique across columns")


if __name__ == "__main__":
    main()
