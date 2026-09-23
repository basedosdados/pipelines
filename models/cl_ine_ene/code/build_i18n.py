#!/usr/bin/env python3
"""Render every column's description and observations in Portuguese, English and Spanish.

The architecture CSV is written in Spanish, the language of the data and of its
documentation. The backend needs all three, so this script assembles them from
two different sources depending on where the text came from:

* Boilerplate this repo authors — "discontinued, published between X and Y", the
  note about values INE never defines — is GENERATED from a template per
  language. Translating a rendered sentence back out of Spanish would be both
  wasteful and lossy when the sentence was ours to begin with.
* Text that comes from the INE codebook is translated once per distinct string
  in translations.json, keyed by the Spanish.

The build gate is the point: a missing translation raises rather than silently
shipping a Spanish string into the English field, which is how a column set ends
up looking trilingual while being anything but.

    python models/cl_ine_ene/code/build_i18n.py
"""

from __future__ import annotations

import csv
import json
import pathlib

HERE = pathlib.Path(__file__).resolve().parent
LANGS = ("pt", "en", "es")

#: Sentences this repo writes, per language. `{first}`/`{last}` are periods and
#: `{values}` a comma-separated list of codes.
TEMPLATES = {
    "retired": {
        "pt": "Descontinuada: publicada entre {first} e {last}",
        "en": "Discontinued: published between {first} and {last}",
        "es": "Descontinuada: publicada entre {first} y {last}",
    },
    "undocumented": {
        "pt": (
            "Os valores {values} aparecem nos dados mas o livro de códigos do INE "
            "não os define; são publicados tal como vêm, sem etiqueta"
        ),
        "en": (
            "The values {values} occur in the data but INE's codebook does not "
            "define them; they are published as they come, unlabelled"
        ),
        "es": (
            "Los valores {values} aparecen en los datos pero el libro de códigos "
            "del INE no los define; se publican tal cual, sin etiqueta"
        ),
    },
    "undocumented_one": {
        "pt": (
            "O valor {values} aparece nos dados mas o livro de códigos do INE "
            "não o define; é publicado tal como vem, sem etiqueta"
        ),
        "en": (
            "The value {values} occurs in the data but INE's codebook does not "
            "define it; it is published as it comes, unlabelled"
        ),
        "es": (
            "El valor {values} aparece en los datos pero el libro de códigos "
            "del INE no lo define; se publica tal cual, sin etiqueta"
        ),
    },
}


def render_undocumented(values: list[str], lang: str) -> str:
    shown = ", ".join(values[:12]) + (" ..." if len(values) > 12 else "")
    key = "undocumented_one" if len(values) == 1 else "undocumented"
    return TEMPLATES[key][lang].format(values=shown)


def join(sentences: list[str]) -> str:
    return ". ".join(
        s.strip(" .") for s in sentences if s and s.strip()
    ).strip()


def main():
    with open(HERE / "architecture/microdato.csv", encoding="utf-8") as handle:
        arch = list(csv.DictReader(handle))
    parts = json.loads((HERE / "observation_parts.json").read_text())
    translations = json.loads((HERE / "translations.json").read_text())

    text = translations["text"]  # spanish -> {"pt": ..., "en": ...}
    authored = translations["authored"]  # column -> {field -> {lang -> ...}}

    missing: list[str] = []

    def in_lang(spanish: str, lang: str, where: str) -> str:
        if lang == "es":
            return spanish
        entry = text.get(spanish)
        if not entry or not entry.get(lang):
            missing.append(f"{where} [{lang}]: {spanish[:70]}")
            return ""
        return entry[lang]

    columns = []
    for row in arch:
        name = row["name"]
        part = parts[name]
        record: dict[str, object] = {"name": name}

        for lang in LANGS:
            if part["description_is_mine"]:
                # Spanish is already written in the architecture; only pt/en are authored.
                value = (
                    row["description"]
                    if lang == "es"
                    else authored.get(name, {})
                    .get("description", {})
                    .get(lang, "")
                )
                if not value:
                    missing.append(f"{name}.description [{lang}]")
            else:
                value = in_lang(
                    row["description"], lang, f"{name}.description"
                )
            record[f"description_{lang}"] = value

            sentences = []
            if part["retired"]:
                first, last = part["retired"]
                sentences.append(
                    TEMPLATES["retired"][lang].format(first=first, last=last)
                )
            if part["override"]:
                own = (
                    part["override"]
                    if lang == "es"
                    else authored.get(name, {})
                    .get("observations", {})
                    .get(lang, "")
                )
                if not own:
                    missing.append(f"{name}.observations [{lang}]")
                sentences.append(own)
            elif part["source"]:
                sentences.append(
                    in_lang(part["source"], lang, f"{name}.observations")
                )
            if part["undocumented"]:
                sentences.append(
                    render_undocumented(part["undocumented"], lang)
                )
            record[f"observations_{lang}"] = join(sentences)

        record["bigquery_type"] = row["bigquery_type"]
        record["covered_by_dictionary"] = row["covered_by_dictionary"] == "yes"
        record["has_sensitive_data"] = row["has_sensitive_data"] == "yes"
        if row["directory_column"]:
            record["directory_column"] = row["directory_column"]
        if row["measurement_unit"]:
            record["measurement_unit"] = row["measurement_unit"]
        if row["temporal_coverage"]:
            record["temporal_coverage"] = row["temporal_coverage"]
        columns.append(record)

    if missing:
        print(f"{len(missing)} missing translations; first 25:")
        for item in missing[:25]:
            print("  -", item)
        raise SystemExit(
            "refusing to emit: a missing translation would ship Spanish text in the "
            "Portuguese or English field. Add the entries to translations.json."
        )

    out = HERE / "columns_microdato.json"
    out.write_text(json.dumps(columns, ensure_ascii=False, indent=1) + "\n")
    print(f"wrote {out} ({len(columns)} columns, all three languages)")

    unused = (
        set(text)
        - {row["description"] for row in arch}
        - {p["source"] for p in parts.values() if p["source"]}
    )
    if unused:
        print(
            f"note: {len(unused)} translations in the file are no longer referenced"
        )


if __name__ == "__main__":
    main()
