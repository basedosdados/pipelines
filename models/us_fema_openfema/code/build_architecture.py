"""Generate the architecture CSVs and columns_json for us_fema_openfema.

Inputs, all in this directory:
  source_metadata.json  OpenFEMA's own catalog and field dictionary (cached)
  tables.py             which sets become which tables, renames, keys, order
  glossary.py           trilingual descriptions, units, coded columns, FKs

Outputs:
  architecture/<table>.csv    the architecture table (source of truth)
  columns_json/<table>.json   the same, trilingual, for bulk_upsert_columns

The build refuses to emit anything unless every column resolves: a missing
glossary entry, a numeric column with no declared unit, or a coded column that
did not end up STRING all raise. Silence there would ship a column with an
empty description or a meaningless unit into the catalog.

    uv run python build_architecture.py
"""

from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import glossary  # noqa: E402
import tables as spec  # noqa: E402

ARCH_DIR = HERE / "architecture"
JSON_DIR = HERE / "columns_json"

CSV_HEADER = [
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

# OpenFEMA field type -> BigQuery type, before the CODED/IDENTIFIER overrides.
TYPE_MAP = {
    "text": "STRING",
    "uuid": "STRING",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "datetime": "DATETIME",
    "datetimez": "DATETIME",
    "smallint": "INT64",
    "integer": "INT64",
    "bigint": "INT64",
}

NUMERIC = {"INT64", "FLOAT64"}


def snake(name: str) -> str:
    """camelCase -> snake_case, keeping acronym runs together."""
    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


def bq_type(fema_type: str, name: str, table: str) -> str:
    override = spec.TYPE_OVERRIDE.get((table, name))
    if override:
        return override
    if name in glossary.CODED or name in glossary.IDENTIFIER:
        # Codes and identifiers are STRING however the source stores them.
        return "STRING"
    if fema_type.startswith("decimal"):
        return "FLOAT64"
    if fema_type not in TYPE_MAP:
        raise SystemExit(f"unmapped OpenFEMA type {fema_type!r} on {name!r}")
    return TYPE_MAP[fema_type]


def build_table(table: str, cfg: dict, fields: list[dict]) -> list[dict]:
    rename = {**spec.HARMONISE, **cfg["rename"]}
    partition_source = cfg["partition_source"]

    rows: list[dict] = []
    seen: set[str] = set()

    # The partition column always comes first.
    rows.append(
        {
            "name": cfg["partition"],
            "bigquery_type": "INT64",
            "original_name": _source_name(fields, partition_source, rename),
            "derived": f"year of {partition_source}",
        }
    )
    seen.add(cfg["partition"])

    for field in sorted(fields, key=lambda f: f["sortOrder"]):
        raw = snake(field["name"])
        if raw in spec.DROP:
            continue
        name = rename.get(raw, raw)
        # The column the partition is derived from is kept only if it is not
        # itself the year (a bare yearOfLoss would duplicate the partition).
        if name in seen:
            raise SystemExit(
                f"{table}: duplicate column {name!r} after rename"
            )
        if raw == partition_source and field["type"] == "smallint":
            continue  # yearOfLoss becomes `year`; do not carry both
        rows.append(
            {
                "name": name,
                "bigquery_type": bq_type(field["type"], name, table),
                "original_name": field["name"],
                "derived": "",
            }
        )
        seen.add(name)

    for name, typ, how in spec.DERIVED.get(table, []):
        if name in seen:
            raise SystemExit(
                f"{table}: derived column {name!r} already exists"
            )
        rows.append(
            {
                "name": name,
                "bigquery_type": typ,
                "original_name": "",
                "derived": how,
            }
        )
        seen.add(name)

    # Order: the partition column, then the declared lead columns (identifiers
    # and geography), then everything else in the source's own field order.
    lead = [cfg["partition"], *cfg["lead"]]
    unknown = [c for c in cfg["lead"] if c not in seen]
    if unknown:
        raise SystemExit(f"{table}: lead names not in the table: {unknown}")
    source_order = {r["name"]: i for i, r in enumerate(rows)}
    ordered = sorted(
        rows,
        key=lambda r: (
            (0, lead.index(r["name"]))
            if r["name"] in lead
            else (1, source_order[r["name"]])
        ),
    )
    return _finalise(table, ordered)


def _source_name(fields: list[dict], target: str, rename: dict) -> str:
    for f in fields:
        if snake(f["name"]) == target:
            return f["name"]
    raise SystemExit(
        f"partition source {target!r} not found in the source set"
    )


def _finalise(table: str, rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        name = r["name"]
        if name not in glossary.COLUMNS:
            raise SystemExit(f"{table}.{name}: no glossary entry")
        pt, en, es = glossary.COLUMNS[name]
        typ = r["bigquery_type"]
        coded = name in glossary.CODED
        if coded and typ != "STRING":
            raise SystemExit(
                f"{table}.{name}: coded column is {typ}, not STRING"
            )
        if typ in NUMERIC and name not in glossary.UNIT:
            raise SystemExit(
                f"{table}.{name}: {typ} column with no entry in glossary.UNIT"
            )
        obs = glossary.OBSERVATIONS.get(name, ("", "", ""))
        if r["derived"]:
            note = f"Derived during cleaning: {r['derived']}"
            obs = tuple(f"{o}. {note}" if o else note for o in obs)
        out.append(
            {
                "name": name,
                "bigquery_type": typ,
                "description_pt": pt,
                "description_en": en,
                "description_es": es,
                "covered_by_dictionary": coded,
                "directory_column": glossary.DIRECTORY.get(name, ""),
                "measurement_unit": glossary.UNIT.get(name, ""),
                "observations_pt": obs[0],
                "observations_en": obs[1],
                "observations_es": obs[2],
                "original_name": r["original_name"],
            }
        )
    return out


# The dictionary table is the same five columns in every Data Basis dataset.
DICIONARIO = [
    (
        "id_tabela",
        "Nome da tabela a que o código se aplica",
        "Name of the table the code applies to",
        "Nombre de la tabla a la que se aplica el código",
    ),
    (
        "nome_coluna",
        "Nome da coluna a que o código se aplica",
        "Name of the column the code applies to",
        "Nombre de la columna a la que se aplica el código",
    ),
    (
        "chave",
        "Código, tal como armazenado na coluna",
        "Code, as stored in the column",
        "Código, tal como se almacena en la columna",
    ),
    (
        "cobertura_temporal",
        "Cobertura temporal do código",
        "Temporal coverage of the code",
        "Cobertura temporal del código",
    ),
    (
        "valor",
        "Rótulo do código, em inglês, a língua da fonte",
        "Label for the code, in English, the language of the source",
        "Etiqueta del código, en inglés, la lengua de la fuente",
    ),
]


def write_dicionario() -> None:
    cols = [
        {
            "name": name,
            "bigquery_type": "STRING",
            "description_pt": pt,
            "description_en": en,
            "description_es": es,
            "covered_by_dictionary": False,
            "directory_column": "",
            "measurement_unit": "",
            "observations_pt": "",
            "observations_en": "",
            "observations_es": "",
            "original_name": "",
        }
        for name, pt, en, es in DICIONARIO
    ]
    (JSON_DIR / "dicionario.json").write_text(
        json.dumps(cols, indent=1, ensure_ascii=False) + "\n"
    )
    with (ARCH_DIR / "dicionario.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_HEADER, lineterminator="\n")
        w.writeheader()
        for c in cols:
            w.writerow(
                {
                    "name": c["name"],
                    "bigquery_type": "STRING",
                    "description": c["description_pt"],
                    "temporal_coverage": "",
                    "covered_by_dictionary": "no",
                    "directory_column": "",
                    "measurement_unit": "",
                    "has_sensitive_data": "no",
                    "observations": "",
                    "original_name": "",
                }
            )
    print(f"{'dicionario':<28} {len(cols):>3} columns")


def main() -> None:
    meta = json.loads((HERE / "source_metadata.json").read_text())
    ARCH_DIR.mkdir(exist_ok=True)
    JSON_DIR.mkdir(exist_ok=True)

    for table, cfg in spec.TABLES.items():
        set_name, version = cfg["source"]
        fields = [
            f
            for f in meta["fields"]
            if f["openFemaDataSet"] == set_name
            and f["datasetVersion"] == version
        ]
        if not fields:
            raise SystemExit(
                f"{table}: no fields cached for {set_name} v{version}"
            )
        cols = build_table(table, cfg, fields)

        for key in cfg["primary_key"]:
            if key not in {c["name"] for c in cols}:
                raise SystemExit(
                    f"{table}: primary key {key!r} is not a column"
                )

        (JSON_DIR / f"{table}.json").write_text(
            json.dumps(cols, indent=1, ensure_ascii=False) + "\n"
        )
        with (ARCH_DIR / f"{table}.csv").open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=CSV_HEADER, lineterminator="\n")
            w.writeheader()
            for c in cols:
                w.writerow(
                    {
                        "name": c["name"],
                        "bigquery_type": c["bigquery_type"],
                        "description": c["description_pt"],
                        "temporal_coverage": "",
                        "covered_by_dictionary": "yes"
                        if c["covered_by_dictionary"]
                        else "no",
                        "directory_column": c["directory_column"],
                        "measurement_unit": c["measurement_unit"],
                        "has_sensitive_data": "no",
                        "observations": c["observations_pt"],
                        "original_name": c["original_name"],
                    }
                )
        n_coded = sum(c["covered_by_dictionary"] for c in cols)
        print(
            f"{table:<28} {len(cols):>3} columns  "
            f"{n_coded:>2} dictionary-covered  "
            f"pk={'+'.join(cfg['primary_key'])}"
        )

    write_dicionario()


if __name__ == "__main__":
    main()
