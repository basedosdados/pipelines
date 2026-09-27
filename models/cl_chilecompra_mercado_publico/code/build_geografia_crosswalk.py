"""Build the name-to-CUT crosswalk used to link ChileCompra geography to the directory.

ChileCompra publishes región and comuna as free text only, never as a código único
territorial, so linking to br_bd_diretorios_cl needs a name lookup. Most names match the
directory once accents and case are folded; the rest are a short, enumerable list of
spelling variants, which is why the crosswalk is a checked-in file rather than fuzzy
matching at load time. Fuzzy matching would silently produce a different answer as the
data changes; a table produces the same answer or none.

Run against the directory in BigQuery to refresh the snapshot:

    uv run python models/cl_chilecompra_mercado_publico/code/build_geografia_crosswalk.py

Writes code/geografia_crosswalk.csv, which the cleaning transform reads at load time.
"""

from __future__ import annotations

import argparse
import csv
import os
import unicodedata
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

OUT = Path(__file__).resolve().parent / "geografia_crosswalk.csv"

# ChileCompra spellings that do not fold onto the directory name. Every one was found by
# diffing the actual distinct values against the directory, not guessed. Three classes:
# genuine alternative names (Puerto Natales / Natales), orthographic variants
# (Llay-Llay / Llaillay), and values truncated by the source at 35 characters, which is
# how region_proveedor arrives.
COMUNA_ALIASES = {
    "alto bio bio": "Alto Biobío",
    "chol chol": "Cholchol",
    "la calera": "Calera",
    "las guaitecas": "Guaitecas",
    "llay-llay": "Llaillay",
    "marchigue": "Marchihue",
    "o higgins": "O'Higgins",
    "puerto natales": "Natales",
    "puerto saavedra": "Saavedra",
    "san vicente de tagua tagua": "San Vicente",
    "santiago centro": "Santiago",
    "til til": "Tiltil",
    "torres del payne": "Torres del Paine",
}

# "Arica 1" is not a comuna -- it is a branch/postal designator that leaks into the
# comuna field. It is deliberately left unresolved rather than forced onto Arica.
COMUNA_UNRESOLVED = {"arica 1"}

REGION_ALIASES = {
    # The source truncates region_proveedor at 35 characters.
    "aysen del general carlos iba": "11",
    "magallanes y de la antart": "12",
    "libertador general berna": "06",
    "magallanes y de la antartica": "12",
    # Acute accent U+00B4 instead of an apostrophe.
    "libertador general bernardo o'higgins": "06",
}

# Deliberately no "region de la " entry -- see utils.REGION_PREFIXES.
REGION_PREFIXES = ("region del ", "region de ", "region ")


def fold(value: object) -> str:
    """Lower-case, strip accents, normalise the acute-accent apostrophe, collapse space."""
    text = unicodedata.normalize("NFD", str(value))
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower().replace("\u00b4", "'").replace(".", "")
    return " ".join(text.split())


def strip_region_prefix(value: str) -> str:
    folded = fold(value)
    for prefix in REGION_PREFIXES:
        if folded.startswith(prefix):
            return folded[len(prefix) :].strip()
    return folded


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--billing-project", default="basedosdados-dev")
    args = parser.parse_args()

    os.environ.setdefault(
        "GOOGLE_APPLICATION_CREDENTIALS",
        os.path.expanduser("~/.basedosdados/credentials/staging.json"),
    )
    from google.cloud import bigquery

    client = bigquery.Client(project=args.billing_project)
    project = args.billing_project

    regions = {
        r.id_region: r.nombre
        for r in client.query(
            f"select id_region, nombre from `{project}.br_bd_diretorios_cl.region`"
        ).result()
    }
    comunas = {
        r.id_comuna: r.nombre
        for r in client.query(
            f"select id_comuna, nombre from `{project}.br_bd_diretorios_cl.comuna`"
        ).result()
    }
    if len({fold(v) for v in comunas.values()}) != len(comunas):
        raise SystemExit(
            "comuna names are not unique; a name lookup would be ambiguous"
        )

    rows = []
    for cut, name in sorted(regions.items()):
        rows.append(("region", strip_region_prefix(name), cut, name))
    for folded, cut in sorted(REGION_ALIASES.items()):
        rows.append(("region", folded, cut, regions.get(cut, "")))

    by_folded = {fold(v): k for k, v in comunas.items()}
    for cut, name in sorted(comunas.items()):
        rows.append(("comuna", fold(name), cut, name))
    for alias, target in sorted(COMUNA_ALIASES.items()):
        cut = by_folded.get(fold(target))
        if cut is None:
            raise SystemExit(f"alias target not in directory: {target!r}")
        rows.append(("comuna", alias, cut, target))

    seen, deduped = set(), []
    for kind, folded, cut, name in rows:
        if (kind, folded) in seen:
            continue
        seen.add((kind, folded))
        deduped.append((kind, folded, cut, name))

    with open(OUT, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(
            ["tipo", "nombre_normalizado", "id", "nombre_directorio"]
        )
        writer.writerows(deduped)

    n_region = sum(1 for r in deduped if r[0] == "region")
    n_comuna = sum(1 for r in deduped if r[0] == "comuna")
    print(f"wrote {OUT}")
    print(f"  region entries: {n_region} ({len(regions)} directory + aliases)")
    print(
        f"  comuna entries: {n_comuna} ({len(comunas)} directory + {len(COMUNA_ALIASES)} aliases)"
    )
    print(f"  deliberately unresolved: {sorted(COMUNA_UNRESOLVED)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
