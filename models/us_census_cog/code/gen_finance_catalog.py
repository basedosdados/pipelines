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
import subprocess
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


# No single annual technical document lists every code the long files use: the
# 2018 edition is missing four that the 2013 edition carries, so both are read.
ANNUAL_TECH_DOCS = {
    2018: "2018 S&L Public Use Files Technical Documentation.pdf",
    2013: "2013 S&L Indiv Unit Data File Tech Doc.pdf",
}
CODE_LINE = re.compile(r"^\s*([A-Z0-9]{3})\s{2,}(\S.*?)\s*$")


def read_annual_codes() -> dict[str, str]:
    """Return the item code list published with the 2018 annual finance file.

    The historical User Guide documents the variables of the wide file, which is
    not the same set of codes the long 2013-2018 files use: 129 of the 326 codes
    seen in those years appear nowhere in it. The annual technical documentation
    carries the missing labels, as a plain two-column list in its PDF.
    """
    codes: dict[str, str] = {}
    pdf = Path("/tmp/cog_annual_tech_doc.pdf")
    text = Path("/tmp/cog_annual_tech_doc.txt")
    for year, member in ANNUAL_TECH_DOCS.items():
        archive = next((INPUT / "fin").glob(f"{year}_*.zip"))
        with zipfile.ZipFile(archive) as zf:
            pdf.write_bytes(zf.read(member))
        subprocess.run(
            ["pdftotext", "-layout", str(pdf), str(text)], check=True
        )
        for line in text.read_text().splitlines():
            match = CODE_LINE.match(line)
            if match and not match.group(2).startswith(
                ("Description", "Value")
            ):
                codes.setdefault(match.group(1), match.group(2))
    pdf.unlink(missing_ok=True)
    text.unlink(missing_ok=True)
    return codes


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
                "source": "historical_user_guide",
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

    # Codes used only by the long 2013-2018 files, labelled from the annual
    # technical documentation rather than the historical User Guide.
    # Match on the emitted key only. The six codes whose historical columns
    # collided are keyed under slugs there, but the long files use the plain
    # code, so each still needs a row of its own.
    known = {r["item_code"] for r in rows}
    for code, description in sorted(read_annual_codes().items()):
        if code in known:
            continue
        rows.append(
            {
                "column_label": "",
                "item_code": code,
                "source_item_code": code,
                "item_type": item_type(code),
                "variable_name": "",
                "description": description,
                "source": "annual_tech_doc",
            }
        )

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
