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
ANNEXES = json.loads((HERE / "codebook_annexes.json").read_text())

#: {column: {code}} for the columns whose labels live in a codebook annex rather
#: than in its section-5 variable table — occupation, industry and nationality.
ANNEX_CODES = {
    column: set(ANNEXES["tables"][table])
    for table, columns in ANNEXES["applies_to"].items()
    for column in columns
}
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

#: Codes the codebook uses for "does not know" / "does not answer" / "does not
#: apply" / "no information", recognised by their LABEL rather than their value.
#: The value alone cannot decide it: 77, 88 and 99 are non-response codes in
#: `habituales` and ordinary ages in `edad`, and the codebook says which is which.
NON_RESPONSE = re.compile(
    r"no\s+(sabe|responde|aplica|corresponde|informa)|sin\s+(informaci[oó]n|clasificaci[oó]n)"
    r"|zona no especificada",
    re.IGNORECASE,
)


def declared_sentinels(categories: list[str]) -> set[str]:
    """The codes this column's own codebook entry marks as non-response."""
    out = set()
    for entry in categories:
        code, _, label = entry.partition(":")
        if NON_RESPONSE.search(label):
            out.add(code.strip())
    return out


#: Values that are a non-response code in SOME column. Used only to decide
#: dictionary coverage, never to decide a type.
SENTINELS = {"77", "88", "99", "888", "999", "8888", "9999", "88888", "99999"}

#: Columns whose codes are resolved by a directory rather than by this dataset's
#: dicionario. The comuna and region annexes of the codebook restate
#: br_bd_diretorios_cl, which is their source of truth.
DIRECTORY_CODED = {
    "id_region",
    "id_provincia",
    "id_comuna",
    "mig2_cod",
    "mig5_cod",
    "b18_codigo",
    "b18_region",
}


#: A dicionario must explain most of what the column actually holds. Below this
#: share the "categories" are an artefact: a line number whose 1 and 2 happen to
#: be documented, or a free-text field with a couple of coded answers among
#: thousands of written ones.
COVERAGE_FLOOR = 0.5


def is_dictionary_covered(
    name: str,
    bq_type: str,
    categories: list[str],
    observed: set[str],
    over_cap: bool,
) -> bool:
    """True only when the dicionario interprets the column's stored VALUES.

    Three ways a column fails, all of them seen in this dataset:

    * documented solely by its non-response codes — `turno_h` holds hours, and
      888/999 are sentinels sitting beside them, not a vocabulary;
    * resolved by a directory instead — `id_comuna`, `mig2_cod` and the rest use
      the DPA, whose source of truth is br_bd_diretorios_cl;
    * free text or an identifier that happens to carry a couple of coded answers
      — `e19_otro` has 20,196 distinct written reasons.

    Marking any of these `yes` asserts a value->label map that can never be
    complete, and custom_dictionary_coverage then fails on every real value.
    """
    annex = ANNEX_CODES.get(name, set())
    if (
        bq_type != "STRING"
        or name in DIRECTORY_CODED
        or not (categories or annex)
    ):
        return False
    # A section-5 entry listing only non-response codes is not a vocabulary — but
    # it is for the classification columns, whose real labels sit in an annex.
    substantive = {
        code.split(":", 1)[0].strip() for code in categories
    } - SENTINELS
    if not substantive and not annex:
        return False
    if over_cap or not observed:
        return False
    labelled = {code.split(":", 1)[0].strip() for code in categories} | annex
    return len(observed & labelled) / len(observed) >= COVERAGE_FLOOR


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


#: A well-formed observation starts with a capital. The codebook's Observaciones
#: column sits beside two others, and when pypdf flattens a page it sometimes
#: interleaves them into a mid-sentence jumble ("adentro afuera Descontinuada.",
#: "cuenta propia pronto permanentes estudios actividad"). Such a fragment cannot
#: be repaired, and publishing it in three languages would be worse than silence.
GARBLED_START = re.compile(r"^[a-záéíóúñ¿]|^[)\]]")
#: Sentences that ARE well-formed can be salvaged from the tail of a jumble.
SALVAGEABLE = re.compile(
    r"((?:Vigente desde|Descontinuada|Producida entre|Incorporada|Variable incorporada|"
    r"Solo responden|Responden|Corresponde)[^.]*\.?)\s*$"
)


def salvage(text: str) -> str:
    """Keep a trailing well-formed sentence out of a flattening jumble, else nothing."""
    match = SALVAGEABLE.search(text)
    return match.group(1).strip(" .") if match else ""


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
    text = " ".join(kept).strip(" .;")
    if text and GARBLED_START.match(text):
        return salvage(text)
    return text


def temporal_coverage(first: str, last: str) -> str:
    """BD notation, empty when the column spans the whole table."""
    if (first, last) == (SERIES_FIRST, SERIES_LAST):
        return ""
    start = first.replace("-", "-")
    end = "" if last == SERIES_LAST else last
    return f"{start}(1){end}"


def undocumented(
    categories: list[str], name: str, observed: set[str]
) -> list[str]:
    """Values present in the data that the codebook does not label.

    INE's codebook does not document every value its own microdata contains —
    `ocup_form` carries a 0 it never defines, `b17_mes` a stray 1997. Nothing is
    invented for these: they are listed in the column's observations so a reader
    meets them knowingly, and they keep the column out of the completeness test.
    """
    labelled = {code.split(":", 1)[0].strip() for code in categories}
    labelled |= ANNEX_CODES.get(name, set())
    return sorted(observed - labelled, key=lambda v: (len(v), v))


def build(universe, codebook, profile):
    rows, problems, complete = [], [], []
    observation_parts: dict[str, dict] = {}
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
        observed = set(stats.get("values", []))

        if name in NUMERIC:
            bq_type, unit = NUMERIC[name]
            if stats:
                if stats.get("other", 0):
                    problems.append(
                        f"{name}: {stats['other']} non-numeric values but typed {bq_type}"
                    )
                hit = sorted(observed & declared_sentinels(categories))
                if hit:
                    problems.append(
                        f"{name}: typed {bq_type} but carries the non-response "
                        f"codes {hit} its codebook entry declares"
                    )
        else:
            bq_type, unit = "STRING", ""

        covered = (
            "yes"
            if is_dictionary_covered(
                name,
                bq_type,
                categories,
                observed,
                stats.get("over_cap", False),
            )
            else "no"
        )

        missing = (
            undocumented(categories, name, observed)
            if covered == "yes"
            else []
        )
        if covered == "yes" and not missing:
            complete.append(name)

        observation = OVERRIDES["observations"].get(name, "")
        book_obs = clean_observation(book.get("obs", ""))
        if book_obs and not observation:
            observation = book_obs

        # Kept apart from the rendered Spanish so build_i18n.py can regenerate the
        # boilerplate from per-language templates instead of translating it back.
        observation_parts[name] = {
            "override": OVERRIDES["observations"].get(name, ""),
            "source": "" if OVERRIDES["observations"].get(name) else book_obs,
            "undocumented": [],
            "retired": [],
            "description_is_mine": name in OVERRIDES["descriptions"],
        }
        if missing:
            shown = ", ".join(missing[:12]) + (
                " ..." if len(missing) > 12 else ""
            )
            note = (
                f"Los valores {shown} aparecen en los datos pero el libro de códigos "
                "del INE no los define; se publican tal cual, sin etiqueta"
            )
            observation = (
                f"{observation}. {note}".strip(". ") if observation else note
            )

        observation_parts[name]["undocumented"] = missing

        if entry["last_period"] != SERIES_LAST:
            observation_parts[name]["retired"] = [
                entry["first_period"],
                entry["last_period"],
            ]
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
    return rows, problems, complete, observation_parts


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

    rows, problems, complete, observation_parts = build(
        universe, codebook, profile
    )

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

    # Columns whose dicionario is demonstrably complete against every value in
    # the data. schema.yml scopes custom_dictionary_coverage to exactly these, so
    # the test asserts something true instead of failing on the source's own gaps.
    parts_path = HERE / "observation_parts.json"
    parts_path.write_text(
        json.dumps(observation_parts, ensure_ascii=False, indent=1) + "\n"
    )
    print(f"wrote {parts_path}")

    listing = HERE / "dictionary_complete.json"
    listing.write_text(json.dumps(complete, indent=1) + "\n")
    print(
        f"wrote {listing} ({len(complete)} columns with a complete dicionario)"
    )


if __name__ == "__main__":
    main()
