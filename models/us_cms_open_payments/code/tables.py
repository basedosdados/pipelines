"""Table definitions: which source files feed which table, and in what shape.

Six detail tables, four entity tables, twelve summary tables and a dictionary.

The era split follows CMS: program years 2013-2015 use the legacy schema and
2016 onwards the modern one, so ``general``/``research`` carry the modern years
and ``general_legacy``/``research_legacy`` the legacy ones. Two exceptions,
both cases where the legacy columns are a strict subset of the modern ones and
a union loses nothing:

* ``ownership`` spans 2013-2025 (PY 2013-2014 simply lack ``Physician_NPI``).
* ``research_principal_investigator`` spans 2013-2025. It is a reshape in any
  case -- the five repeated investigator blocks become rows -- so splitting it
  by era would add a table without adding information. Legacy rows leave the
  modern-only fields (``covered_recipient_type``, ``primary_type_2``..``_6``,
  ``specialty_2``..``_6``) null.
"""

import json
from pathlib import Path

import constants as c
import naming

with open(Path(__file__).resolve().parent / "headers.json") as _fh:
    HEADERS = json.load(_fh)

PI_TABLE = "research_principal_investigator"

# Columns placed first, in this order, when they exist in a table: the
# partition column, then identifiers, then everything else in source order.
LEAD = [
    "year",
    "record_id",
    "covered_recipient_profile_id",
    "physician_profile_id",
    "covered_recipient_npi",
    "physician_npi",
    "teaching_hospital_ccn",
    "teaching_hospital_id",
    "reporting_entity_id",
    "principal_investigator_number",
]


def _union(keys: list[str]) -> list[str]:
    """Ordered union of several source headers, longest header first.

    PY 2015 is a superset of PY 2013-2014, and the modern years are identical
    to one another, so taking the longest header as the spine and appending
    anything unseen preserves the published column order.
    """
    ordered = sorted(
        keys, key=lambda k: len(HEADERS["detail"][k]), reverse=True
    )
    out: list[str] = []
    for key in ordered:
        for col in HEADERS["detail"][key]:
            if col not in out:
                out.append(col)
    return out


def order(cols: list[str]) -> list[str]:
    lead = [c_ for c_ in LEAD if c_ in cols]
    return lead + [c_ for c_ in cols if c_ not in lead]


def detail_columns(kind: str, years: list[int]) -> list[str]:
    """Data Basis column names for a detail table, excluding investigator blocks."""
    source = _union([f"{kind}_{y}" for y in years])
    mapped = [
        naming.rename(s)
        for s in source
        if not naming.split_principal_investigator(s)
    ]
    return order(mapped)


def principal_investigator_columns() -> list[str]:
    """Data Basis column names for the investigator child table."""
    fields: list[str] = []
    for year in c.ALL_YEARS:
        for src in HEADERS["detail"][f"research_{year}"]:
            split = naming.split_principal_investigator(src)
            if split and split[1] not in fields:
                fields.append(split[1])
    return order(
        ["year", "record_id", "principal_investigator_number", *fields]
    )


DETAIL_TABLES = {
    "general": dict(kind="general", years=c.MODERN_YEARS),
    "general_legacy": dict(kind="general", years=c.LEGACY_YEARS),
    "research": dict(kind="research", years=c.MODERN_YEARS),
    "research_legacy": dict(kind="research", years=c.LEGACY_YEARS),
    "ownership": dict(kind="ownership", years=c.ALL_YEARS),
}


def all_tables() -> dict[str, list[str]]:
    out = {
        name: detail_columns(**spec) for name, spec in DETAIL_TABLES.items()
    }
    out[PI_TABLE] = principal_investigator_columns()
    return out


if __name__ == "__main__":
    tables = all_tables()
    for name, cols in tables.items():
        print(f"{name:38s} {len(cols):4d} cols   lead={cols[:5]}")
    distinct = sorted({c_ for cols in tables.values() for c_ in cols})
    print(f"\ndistinct detail column names: {len(distinct)}")
