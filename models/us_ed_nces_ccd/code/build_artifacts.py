#!/usr/bin/env python3
"""Generate the architecture CSVs and the value dictionary for us_ed_nces_ccd.

Reads the column specs in ``schema.py`` and the Urban Institute variable lists,
and writes:

    code/architecture/<table>.csv   one per table, Data Basis architecture format
    code/architecture/dicionario_values.csv   code -> label for every coded column
    code/columns.json               trilingual payload for the backend

Usage:
    uv run --no-project python models/us_ed_nces_ccd/code/build_artifacts.py
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# pyrefly: ignore [missing-import]
import schema

ROOT = Path(__file__).resolve().parent
ARCH = ROOT / "architecture"
DATA = Path(
    os.environ.get(
        "CCD_DATA_DIR", Path.home() / "Downloads" / "us_ed_nces_ccd_data"
    )
)
INPUT = DATA / "input"

ARCH_HEADER = [
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

#: Urban endpoint ids carrying the variable lists we need.
VARLIST_ENDPOINTS = {
    "school": 24,
    "school_district": 54,
    "school_enrollment": 28,
    "district_finance": 29,
}


def fetch_varlist(endpoint_id: int) -> list[dict]:
    """Variable list for one Urban endpoint, cached under code/."""
    cache = ROOT / f"varlist_{endpoint_id}.json"
    if cache.exists():
        return json.loads(cache.read_text())
    url = (
        "https://educationdata.urban.org/api/v1/api-endpoint-varlist/"
        f"?endpoint_id={endpoint_id}&mode=R&limit=1000"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=120) as fh:
        results = json.load(fh)["results"]
    cache.write_text(json.dumps(results, indent=1))
    return results


_VALUE_RE = re.compile(r'"([^"]*)"\s*:\s*"([^"]*)"')


def parse_values(raw: str | None) -> list[tuple[str, str]]:
    """Parse Urban's ``"code" : "label",...`` value string, order preserved."""
    if not raw:
        return []
    return [(k, schema._unescape(v)) for k, v in _VALUE_RE.findall(raw)]


def write_architecture(table: schema.Table) -> None:
    ARCH.mkdir(parents=True, exist_ok=True)
    path = ARCH / f"{table.slug}.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(ARCH_HEADER)
        for c in table.columns:
            w.writerow(
                [
                    c.name,
                    c.type,
                    c.desc_en,
                    c.coverage,
                    "yes" if c.dictionary else "no",
                    c.directory,
                    c.unit,
                    "no",
                    c.observations,
                    c.original_name,
                ]
            )
    print(
        f"  wrote {path.relative_to(ROOT.parents[2])} ({len(table.columns)} columns)"
    )


def build_dictionary(
    tables: list[schema.Table], varlists: dict[str, dict[str, dict]]
) -> list[dict]:
    """One row per (table, coded column, code) with the label it stands for."""
    rows: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    for table in tables:
        vl = varlists.get(table.slug, {})
        for col in table.columns:
            if not col.dictionary:
                continue
            if col.name == "staff_category":
                pairs = [
                    (code, en)
                    for code, _src, en, _pt, _es in schema.STAFF_CATEGORIES
                ]
            else:
                src = col.source or col.name
                pairs = parse_values((vl.get(src) or {}).get("values"))
            if not pairs:
                print(f"  !! no value labels for {table.slug}.{col.name}")
                continue
            for code, label in pairs:
                # Sentinels are mapped to NULL on load and never reach the data,
                # so they are not dictionary entries. `grade` is the exception:
                # there -1 is prekindergarten, a real category.
                if col.name not in schema.SENTINEL_EXEMPT and code in (
                    "-1",
                    "-2",
                    "-3",
                ):
                    continue
                # The dictionary key must be the form actually stored. A
                # padded identifier (state_id -> "06") does not match the
                # source's bare label key ("6"), and the coverage test fails
                # on every single-digit state.
                width = schema.PAD.get(col.name)
                if width:
                    code = code.zfill(width)
                key = (table.slug, col.name, code)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    {
                        "id_tabela": table.slug,
                        "nome_coluna": col.name,
                        "chave": code,
                        "cobertura_temporal": "",
                        "valor": label,
                    }
                )
    return rows


def main() -> None:
    finance_header = next(
        csv.reader(
            (INPUT / "districts_ccd_finance.csv").open(encoding="utf-8")
        )
    )
    finance_labels = {x["variable"]: x["label"] for x in fetch_varlist(29)}
    tables = [
        *schema.STATIC_TABLES,
        schema.finance_table(finance_header, finance_labels),
    ]

    varlists: dict[str, dict[str, dict]] = {}
    for slug, eid in VARLIST_ENDPOINTS.items():
        varlists[slug] = {x["variable"]: x for x in fetch_varlist(eid)}
    # staff inherits the agency directory's variable list
    varlists["staff"] = varlists["school_district"]

    # The glossary composes rather than understands, so an unseen phrase passes
    # through as English and reads plausibly. Fail the build instead.
    print("translation check:")
    for table in [*tables, schema.TABLE_DICIONARIO]:
        schema.assert_translated(table.columns, table.slug)
    print(f"  all descriptions translated across {len(tables) + 1} tables")

    print("architecture:")
    for table in tables:
        write_architecture(table)
    write_architecture(schema.TABLE_DICIONARIO)

    print("dictionary:")
    rows = build_dictionary(tables, varlists)
    dict_path = ARCH / "dicionario_values.csv"
    with dict_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=[
                "id_tabela",
                "nome_coluna",
                "chave",
                "cobertura_temporal",
                "valor",
            ],
        )
        w.writeheader()
        w.writerows(rows)
    print(f"  {len(rows)} dictionary entries -> {dict_path.name}")

    print("backend column payload:")
    payload = {}
    for table in [*tables, schema.TABLE_DICIONARIO]:
        payload[table.slug] = [
            {
                "name": c.name,
                "bigquery_type": c.type,
                "description_pt": c.desc_pt,
                "description_en": c.desc_en,
                "description_es": c.desc_es,
                "covered_by_dictionary": c.dictionary,
                "directory_column": c.directory,
                "measurement_unit": c.unit,
                "has_sensitive_data": False,
                "observations_en": c.observations,
                "observations_pt": c.observations_pt,
                "observations_es": c.observations_es,
                "is_partition": c.name == table.partition,
            }
            for c in table.columns
        ]
    (ROOT / "columns.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=1)
    )
    total = sum(len(v) for v in payload.values())
    print(f"  {total} columns across {len(payload)} tables -> columns.json")


if __name__ == "__main__":
    main()
