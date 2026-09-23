"""Emit the per-table column payloads for bulk_upsert_columns, with translations.

The architecture CSVs carry Spanish descriptions (the dataset's own language).
The backend wants pt/en/es, so each Spanish string is looked up whole in
``translations.TRANSLATIONS``.

The build FAILS on any description without a translation, and on any translation
that still carries Spanish-only residue. Without a gate, an untranslated string
is silently written into all three language fields and reads plausibly enough to
survive a skim - the failure mode that nearly shipped on us_ed_nces_ccd.
"""

from __future__ import annotations

import csv
import json
import re
import sys
import unicodedata

from constants import ARCHITECTURE_DIR, DATA_ROOT, TABLES
from translations import TRANSLATIONS

PAYLOAD_DIR = DATA_ROOT / "metadata_payloads"

# Spanish function words that should never survive into a Portuguese or English
# translation. Chosen because they are unambiguous: each either does not exist in
# the target language or is spelled differently there.
SPANISH_RESIDUE = {
    "pt": {
        "y",
        "del",
        "los",
        "las",
        "una",
        "vivienda",
        "viviendas",
        "hogares",
        "personas",
        "anos_es",
        "mas",
        "segun",
        "esta",
        "cual",
        "quien",
    },
    "en": {
        "y",
        "de",
        "del",
        "la",
        "el",
        "los",
        "las",
        "un",
        "una",
        "con",
        "sin",
        "por",
        "para",
        "vivienda",
        "viviendas",
        "hogares",
        "personas",
    },
}

# Tokens exempt from the residue check: proper nouns, classification acronyms and
# codes that are identical across languages by design.
PROTECTED = {
    "ine",
    "chile",
    "cut",
    "caenes",
    "ciuo",
    "cine11",
    "isco",
    "isced",
    "cise",
    "icse",
    "urbano",
    "aldea",
    "parcela-hijuela",
    "wc",
    "p28_autoid_pueblo",
    "p32a",
    "p32f",
    "pirca",
    "coiron",
    "totora",
    "fonolita",
    "tabique",
    "mediagua",
    "adobe",
    "quincha",
    "cortico",
}


def tokens(text: str) -> set[str]:
    folded = "".join(
        c
        for c in unicodedata.normalize("NFD", text.lower())
        if unicodedata.category(c) != "Mn"
    )
    return {t for t in re.findall(r"[a-z0-9_\-]+", folded) if t}


def residue(text: str, language: str) -> set[str]:
    """Spanish-only words left in a supposedly translated string."""
    return (tokens(text) & SPANISH_RESIDUE[language]) - PROTECTED


def build(table: str) -> tuple[list[dict], list[str]]:
    with (ARCHITECTURE_DIR / f"{table}.csv").open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    columns, problems = [], []
    for row in rows:
        spanish = row["description"]
        if spanish not in TRANSLATIONS:
            problems.append(
                f"{table}.{row['name']}: no translation for {spanish!r}"
            )
            continue
        portuguese, english = TRANSLATIONS[spanish]

        for language, text in (("pt", portuguese), ("en", english)):
            left = residue(text, language)
            if left:
                problems.append(
                    f"{table}.{row['name']}: {language} translation keeps "
                    f"Spanish {sorted(left)} -> {text!r}"
                )

        entry = {
            "name": row["name"],
            "bigquery_type": row["bigquery_type"],
            "description_es": spanish,
            "description_pt": portuguese,
            "description_en": english,
            "covered_by_dictionary": row["covered_by_dictionary"] == "yes",
            "is_partition": row["name"] in ("ano", "id_region"),
        }
        if row["directory_column"]:
            entry["directory_column"] = row["directory_column"]
        if row["measurement_unit"]:
            entry["measurement_unit"] = row["measurement_unit"]
        if row["observations"]:
            entry["observations"] = row["observations"]
        columns.append(entry)
    return columns, problems


def main() -> None:
    PAYLOAD_DIR.mkdir(parents=True, exist_ok=True)
    all_problems: list[str] = []
    for table in TABLES:
        columns, problems = build(table)
        all_problems += problems
        path = PAYLOAD_DIR / f"{table}.json"
        path.write_text(
            json.dumps(columns, ensure_ascii=False), encoding="utf-8"
        )
        print(
            f"  {table:18s} {len(columns):4d} columns  "
            f"{path.stat().st_size / 1024:7.1f} KB"
        )

    if all_problems:
        print(
            f"\n{len(all_problems)} TRANSLATION PROBLEM(S):", file=sys.stderr
        )
        for problem in all_problems[:40]:
            print(f"  {problem}", file=sys.stderr)
        raise SystemExit(1)
    print(
        f"\ntranslation gate passed: {len(TRANSLATIONS)} strings, no residue"
    )


if __name__ == "__main__":
    main()
