"""Write the architecture CSVs for us_fdic_bankfind.

Four tables:

    institution           one row per FDIC-insured institution ever registered
    financials            wide, ~300 headline line items per institution-quarter
    financials_indicator  long, all 2,378 line items per institution-quarter
    indicator             the line-item dictionary that decodes the long table

Column names are English snake_case per the house style for English-language
data; the FDIC's own mnemonic (ASSET, LNLSNET, ...) is preserved in
`original_name` so the mapping back to the source is never lost.

Dollar amounts are converted from the FDIC's thousands-of-dollars to plain USD
at cleaning time, so `measurement_unit` is the vocabulary's `USD`.
"""

from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_fdic_bankfind.institution_spec import (
    SPEC,
)

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"

HEADER = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
]

# FDIC reports dollar amounts in thousands; cleaning multiplies by 1,000.
UNIT_MAP = {
    "USD_thousand": "USD",
    "percent": "percent",
    "unit": "unit",
    "": "",
}
SCALED_NOTE = (
    "Reported by the FDIC in thousands of dollars and multiplied by 1,000 here, "
    "so the value is in whole dollars"
)

RESERVED = {"year", "quarter", "report_date", "cert", "rssd_id"}


# Long FDIC titles make unreadable column names; these shorten the worst repeat
# offenders before the title is snake_cased.
ABBREVIATE = [
    (
        "estimated uninsured deposits in domestic offices and in insured branches"
        " in us territories and possessions",
        "estimated uninsured deposits",
    ),
    (
        "total allowable exclusions including foreign deposits",
        "total allowable exclusions",
    ),
    ("charge-off", "chargeoff"),
]


def snake(text: str) -> str:
    lowered = text.lower()
    for long_form, short in ABBREVIATE:
        lowered = lowered.replace(long_form, short)
    text = lowered
    text = (
        text.replace("&", " and ")
        .replace("%", " share ")
        .replace("/", " per ")
    )
    text = re.sub(r"[^0-9A-Za-z]+", "_", text).strip("_").lower()
    text = re.sub(r"_+", "_", text)
    if text and text[0].isdigit():
        text = f"n_{text}"
    return text


def column_names(codes: list[str], catalog: dict) -> dict[str, str]:
    """Map FDIC mnemonics to unique English snake_case column names."""
    names, taken = {}, set(RESERVED)
    # Claim names for base amounts first.  Processed in source order, the ratio
    # IDNTCIR ("COMMERCIAL & INDUSTRIAL LOANS", despite being a ratio) took the
    # clean name and pushed the actual C&I balance to a suffixed one.
    ordered = sorted(
        codes,
        key=lambda c: (
            catalog[c]["is_ratio"] == "yes",
            catalog[c]["is_quarterly"] == "yes",
            codes.index(c),
        ),
    )
    for code in ordered:
        base = snake(catalog[code]["name"]) or code.lower()
        if len(base) > 45:  # truncate on a word boundary, never mid-word
            base = base[:45].rsplit("_", 1)[0]
        base = base.rstrip("_")
        if catalog[code]["is_ratio"] == "yes" and not base.endswith("_ratio"):
            base = f"{base}_ratio"
        name = base
        if name in taken:  # disambiguate with the FDIC mnemonic
            name = f"{base}_{code.lower()}"[:63].rstrip("_")
        while name in taken:
            name = f"{name}_x"
        taken.add(name)
        names[code] = name
    return names


def row(name, btype, description, **kw) -> list[str]:
    return [
        name,
        btype,
        description,
        kw.get("temporal_coverage", ""),
        kw.get("covered_by_dictionary", "no"),
        kw.get("directory_column", ""),
        kw.get("measurement_unit", ""),
        kw.get("has_sensitive_data", "no"),
        kw.get("observations", ""),
        kw.get("original_name", ""),
    ]


def write(table: str, rows: list[list[str]]) -> None:
    ARCH.mkdir(parents=True, exist_ok=True)
    with (ARCH / f"{table}.csv").open("w", newline="") as handle:
        # csv.writer defaults to CRLF; the repo's mixed-line-ending hook
        # rewrites that to LF, so pre-commit.ci reformatted all four files
        # after the first push. Emitting LF here keeps regeneration stable.
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(HEADER)
        writer.writerows(rows)
    print(f"{table:<22} {len(rows):>4} columns")


def keys(include_rssd: bool = True) -> list[list[str]]:
    """The institution-quarter key shared by both financial tables."""
    out = [
        row(
            "year",
            "INT64",
            "Calendar year of the quarterly report",
            directory_column="br_bd_diretorios_data_tempo.ano:ano",
            measurement_unit="year",
            observations="Partition column, derived from the report date",
            original_name="REPDTE",
        ),
        row(
            "quarter",
            "INT64",
            "Calendar quarter of the report, 1 to 4",
            measurement_unit="quarter",
            observations="Derived from the report date",
            original_name="REPDTE",
        ),
        row(
            "report_date",
            "DATE",
            "Last day of the quarter the report covers",
            observations=(
                "Call Reports are filed as of 31 March, 30 June, 30 September "
                "and 31 December"
            ),
            original_name="REPDTE",
        ),
        row(
            "cert",
            "STRING",
            "FDIC certificate number identifying the institution",
            observations=(
                "Stable across name changes; the primary institution key. "
                "Joins to institution.cert, enforced as a dbt relationships test"
            ),
            original_name="CERT",
        ),
    ]
    if include_rssd:
        out.append(
            row(
                "rssd_id",
                "STRING",
                "Federal Reserve RSSD identifier of the institution",
                observations=(
                    "Second identifier carried by the FDIC, used to join to "
                    "Federal Reserve and FFIEC sources"
                ),
                original_name="RSSDID",
            )
        )
    return out


def build_financials(catalog: dict, wide: list[str]) -> None:
    names = column_names(wide, catalog)
    rows = keys()
    for code in wide:
        record = catalog[code]
        unit = UNIT_MAP[record["unit_of_measure"]]
        rows.append(
            row(
                names[code],
                "FLOAT64",
                record["description"] or record["name"],
                measurement_unit=unit,
                observations=SCALED_NOTE if unit == "USD" else "",
                original_name=code,
            )
        )
    write("financials", rows)
    (HERE / "wide_column_names.json").write_text(json.dumps(names, indent=0))


def build_financials_indicator() -> None:
    rows = keys(include_rssd=False)
    rows += [
        row(
            "indicator_id",
            "STRING",
            "FDIC mnemonic of the reported line item",
            observations=(
                "Decoded by the indicator table, which also carries the unit "
                "each value is expressed in. Joins to indicator.indicator_id, "
                "enforced as a dbt relationships test"
            ),
            original_name="(field name)",
        ),
        row(
            "value",
            "FLOAT64",
            "Reported value of the line item",
            observations=(
                "The unit varies by indicator and is given by "
                "indicator.measurement_unit: dollar amounts are in whole dollars "
                "after conversion from the FDIC's thousands, ratios are in "
                "percent, and counts are dimensionless"
            ),
            original_name="(field value)",
        ),
    ]
    write("financials_indicator", rows)


def build_institution() -> None:
    rows = [
        row(
            "extraction_date",
            "DATE",
            "Date the institution directory was extracted from the FDIC API",
            observations=(
                "The directory is a current-state snapshot: one row per "
                "institution, carrying its latest known attributes rather than "
                "a history of them"
            ),
            original_name="(derived)",
        )
    ]
    for code, name, btype, description, opts in SPEC:
        rows.append(
            row(
                name,
                btype,
                description,
                covered_by_dictionary="yes" if opts.get("dict") else "no",
                directory_column=opts.get("dir", ""),
                measurement_unit=opts.get("unit", ""),
                observations=SCALED_NOTE
                if opts.get("scaled")
                else opts.get("obs", ""),
                original_name=code,
            )
        )
    write("institution", rows)


def build_indicator(catalog: dict, wide: list[str]) -> None:
    names = json.loads((HERE / "wide_column_names.json").read_text())
    rows = [
        row(
            "indicator_id",
            "STRING",
            "FDIC mnemonic of the line item",
            observations="Primary key; the value stored in financials_indicator",
            original_name="(field name)",
        ),
        row(
            "name",
            "STRING",
            "Readable name of the line item",
            observations="Derived from the FDIC title, which is published in capitals",
            original_name="title",
        ),
        row(
            "description",
            "STRING",
            "Definition of the line item as published by the FDIC",
            observations="Empty for the line items the FDIC documents by title only",
            original_name="description",
        ),
        row(
            "measurement_unit",
            "STRING",
            "Unit the value is expressed in: USD, percent or unit",
            covered_by_dictionary="yes",
            observations=(
                "Classified from the FDIC title and description. Dollar amounts "
                "are converted from thousands to whole dollars at cleaning time"
            ),
            original_name="(derived)",
        ),
        row(
            "is_ratio",
            "STRING",
            "Whether the line item is a ratio computed by the FDIC",
            covered_by_dictionary="yes",
            original_name="(derived)",
        ),
        row(
            "is_quarterly",
            "STRING",
            "Whether the line item covers the quarter rather than the year to date",
            covered_by_dictionary="yes",
            observations=(
                "Income-statement items are year-to-date unless flagged here; "
                "the FDIC marks the quarterly variants with a Q suffix"
            ),
            original_name="(derived)",
        ),
        row(
            "is_flag",
            "STRING",
            "Whether the line item is a binary flag rather than a measure",
            covered_by_dictionary="yes",
            observations=(
                "Flags are excluded from the financials table and appear in "
                "financials_indicator as 0 or 1"
            ),
            original_name="(derived)",
        ),
        row(
            "financials_column",
            "STRING",
            "Name of the matching column in the financials table",
            observations=(
                "Empty for the line items that are only available in "
                "financials_indicator"
            ),
            original_name="(derived)",
        ),
    ]
    write("indicator", rows)
    assert set(names) <= set(catalog), (
        "wide columns must all exist in the catalog"
    )


if __name__ == "__main__":
    catalog = json.loads((HERE / "indicator_catalog.json").read_text())
    # the dictionary decodes the long table, which carries numeric line items only
    catalog = {
        k: v for k, v in catalog.items() if v["source_type"] == "number"
    }
    wide = json.loads((HERE / "wide_fields.json").read_text())
    wide = [c for c in wide if catalog[c]["is_flag"] == "no"]
    build_institution()
    build_financials(catalog, wide)
    build_financials_indicator()
    build_indicator(catalog, wide)
    print(f"\nwide line items after dropping flags: {len(wide)}")
