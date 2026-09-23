#!/usr/bin/env python3
"""Generate the cl_ine_ene architecture CSV from the codebook and the value profile.

Three inputs, one output:

  column_universe.json   every column the 197 published periods carry, with the
                         first and last period each one appears in
  codebook_parsed.json   descriptions and category lists scraped from the two INE
                         codebooks (see parse_codebook.py)
  overrides.json         hand-written entries for what the PDFs would not yield
  colprofile.json        observed values per column across all 197 files, used to
                         decide types from evidence rather than from the name

Types default to STRING. A column is promoted to INT64/FLOAT64 only if it is on
NUMERIC and the profile confirms it: every non-null value parses as a number, and
no value is one of the codebook's "no sabe / no responde" sentinels. A sentinel
found in a NUMERIC column is a hard error, not a warning -- casting it away is how
a refusal code silently becomes a real measurement.
"""

from __future__ import annotations

import argparse
import csv
import json
import pathlib
import re

HERE = pathlib.Path(__file__).resolve().parent
OVERRIDES = json.loads((HERE / "overrides.json").read_text())
RENAMES = OVERRIDES["renames"]

#: The only columns where arithmetic across rows is meaningful. Everything else --
#: questionnaire codes, flags, identifiers, sequence numbers -- stays STRING with
#: `covered_by_dictionary = yes` when the codebook labels its values.
NUMERIC = {
    "ano": ("INT64", "year"),
    "mes": ("INT64", "month"),
    "edad": ("INT64", "year"),
    # A survey weight is dimensionless -- the one case that takes no unit.
    "fact": ("FLOAT64", ""),
    "fact_cal": ("FLOAT64", ""),
    "fact_anual": ("FLOAT64", ""),
}

#: Values the codebook uses for "does not know" / "does not answer" / "no
#: information". Any of these inside a NUMERIC column means the column is really
#: categorical and the type is wrong.
SENTINELS = {"77", "88", "99", "888", "999", "8888", "9999", "88888", "99999"}

DIRECTORY = {
    "ano": "br_bd_diretorios_data_tempo.ano:ano",
    "mes": "br_bd_diretorios_data_tempo.mes:mes",
    "id_region": "br_bd_diretorios_cl.region:id_region",
    "id_provincia": "br_bd_diretorios_cl.provincia:id_provincia",
    "id_comuna": "br_bd_diretorios_cl.comuna:id_comuna",
}

SERIES_FIRST, SERIES_LAST = "2010-02", "2026-06"

#: Fragments the flattened PDF drags into the Observaciones column: numbered
#: section headings, the table header repeated on each page, and pointers to
#: annexes that are not part of this table's documentation.
OBS_NOISE = re.compile(
    r"\s*\d+(\.\d+)+\s+[A-ZÁÉÍÓÚÑ][^.]*"  # "5.1.2 Información muestral ..."
    r"|\s*Variable Etiqueta Valores Observaciones"
    r"|\s*Variable Descripción Categorías observadas Observaciones"
    r"|\s*Ver detalle[^.]*",
)


def clean_observation(text: str) -> str:
    text = OBS_NOISE.sub(" ", text)
    text = " ".join(text.split()).strip(" .;")
    # The flattened table often repeats a sentence across the Descripción and
    # Observaciones cells; keep the first occurrence of each.
    seen, kept = set(), []
    for sentence in re.split(r"(?<=\.)\s+", text):
        key = sentence.strip().lower()
        if key and key not in seen:
            seen.add(key)
            kept.append(sentence.strip())
    return " ".join(kept).strip(" .;")


def temporal_coverage(first: str, last: str) -> str:
    """BD notation, empty when the column spans the whole table."""
    if (first, last) == (SERIES_FIRST, SERIES_LAST):
        return ""
    start = first.replace("-", "-")
    end = "" if last == SERIES_LAST else last
    return f"{start}(1){end}"


def build(universe, codebook, profile):
    rows, problems = [], []
    for entry in universe:
        source_name = entry["name"]
        name = RENAMES.get(source_name, source_name)

        book = codebook.get(source_name, {})
        categories = (
            OVERRIDES["categories"].get(name) or book.get("cats") or []
        )
        description = (
            OVERRIDES["descriptions"].get(name) or book.get("desc") or ""
        )
        description = description.strip().rstrip(".")
        if description:
            description = description[0].upper() + description[1:]

        stats = profile.get(source_name, {})
        observed = {str(value) for value, _ in stats.get("top", [])}

        if name in NUMERIC:
            bq_type, unit = NUMERIC[name]
            if stats:
                if stats.get("other", 0):
                    problems.append(
                        f"{name}: {stats['other']} non-numeric values but typed {bq_type}"
                    )
                hit = sorted(observed & SENTINELS)
                if hit:
                    problems.append(
                        f"{name}: typed {bq_type} but carries sentinel codes {hit}"
                    )
        else:
            bq_type, unit = "STRING", ""

        covered = (
            "yes"
            if (bq_type == "STRING" and categories and name not in DIRECTORY)
            else "no"
        )

        observation = OVERRIDES["observations"].get(name, "")
        book_obs = clean_observation(book.get("obs", ""))
        if book_obs and not observation:
            observation = book_obs
        if entry["last_period"] != SERIES_LAST:
            retired = f"Descontinuada: publicada entre {entry['first_period']} y {entry['last_period']}"
            observation = (
                f"{retired}. {observation}".strip() if observation else retired
            )

        rows.append(
            {
                "name": name,
                "bigquery_type": bq_type,
                "description": description,
                "temporal_coverage": temporal_coverage(
                    entry["first_period"], entry["last_period"]
                ),
                "covered_by_dictionary": covered,
                "directory_column": DIRECTORY.get(name, ""),
                "measurement_unit": unit,
                "has_sensitive_data": "no",
                "observations": observation,
                "original_name": source_name if source_name != name else "",
            }
        )
    return rows, problems


FIELDS = [
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True, help="colprofile.json")
    parser.add_argument(
        "--out", default=str(HERE / "architecture" / "microdato.csv")
    )
    args = parser.parse_args()

    universe = json.loads((HERE / "column_universe.json").read_text())
    codebook = json.loads((HERE / "codebook_parsed.json").read_text())
    profile = json.loads(pathlib.Path(args.profile).read_text())

    rows, problems = build(universe, codebook, profile)

    missing = [r["name"] for r in rows if not r["description"]]
    print(f"{len(rows)} columns; {len(missing)} without a description")
    if missing:
        print("  MISSING DESCRIPTION:", ", ".join(missing))
    types = {}
    for r in rows:
        types[r["bigquery_type"]] = types.get(r["bigquery_type"], 0) + 1
    print("  types:", types)
    print(
        f"  covered_by_dictionary=yes: {sum(1 for r in rows if r['covered_by_dictionary'] == 'yes')}"
    )
    if problems:
        print("\nTYPE PROBLEMS (fix before writing):")
        for p in problems:
            print("  -", p)
        raise SystemExit(1)

    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
