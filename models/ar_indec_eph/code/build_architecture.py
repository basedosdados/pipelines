"""Build the architecture CSVs for ar_indec_eph from the harvested artefacts.

Inputs (all produced by earlier steps in code/):
  column_universe.json  which columns exist, and in which of the 87 waves
  source_labels.json    Spanish variable labels, from the 2003-2015 Stata files
  registro_parsed.json  descriptions + code lists, from the record-layout PDF
  column_profile.json   real value distributions, used to choose the type
  overrides.json        the few columns INDEC documents nowhere, and drops

Type assignment follows arithmetic meaning, per
.claude/rules/bigquery-conventions.md: INT64/FLOAT64 only where summing or
averaging the values means something and a measurement unit can be named.
Questionnaire codes, flags, decile groups, classifier codes and identifiers are
STRING even when the source stores digits.

Naming follows br_ibge_pnadc, the house precedent for large survey microdata:
descriptive Spanish snake_case for the structural head, and the questionnaire's
own code (lowercased) for the question items, whose recognisability is the whole
point -- CH04, P21, ITF and PONDERA are the shared vocabulary of every EPH user.
"""

import csv
import json

from models.ar_indec_eph.code.constants import ARCH_DIR, CODE_DIR, TABLES
from models.ar_indec_eph.code.i18n import LANGS, fragment
from models.ar_indec_eph.code.parse_registro import lookup

N_WAVES = 87
# BigQuery's hard limit on a column description.
MAX_BQ_DESCRIPTION = 1024

# Populated by main() from the JSON artefacts, and read by build(). Declared here
# so they are module-level names rather than bare `global` assignments.
TRANSLATIONS: dict[str, dict[str, str]] = {}
VALUE_LABEL_COLS: dict[str, set[str]] = {}
DICT_OVERRIDE: set[str] = set()

# --- naming -----------------------------------------------------------------
STRUCTURAL_RENAME = {
    "ANO4": "ano",
    "TRIMESTRE": "trimestre",
    "CODUSU": "id_vivienda",
    "NRO_HOGAR": "nro_hogar",
    "COMPONENTE": "componente",
    "REGION": "id_region",
    "AGLOMERADO": "id_aglomerado",
    "MAS_500": "mas_500",
}
PARTITION = ["ano", "trimestre"]
IDENTIFIERS = [
    "id_vivienda",
    "nro_hogar",
    "componente",
    "id_region",
    "id_aglomerado",
    "mas_500",
]
WEIGHTS = {"PONDERA", "PONDIH", "PONDII", "PONDIIO"}

# --- types ------------------------------------------------------------------
# Monetary amounts, in nominal Argentine pesos of the reference month.
MONEY = {
    "P21",
    "P47T",
    "TOT_P12",
    "T_VI",
    "ITF",
    "IPCF",
    "PP06C",
    "PP06D",
    "PP08D1",
    "PP08D4",
    "PP08F1",
    "PP08F2",
    "PP08J1",
    "PP08J2",
    "PP08J3",
}
# Every *_M column is an amount, plus V19_AM.
MONEY_SUFFIX = "_M"
MONEY_EXTRA = {"V19_AM"}

# INT64 quantities: column -> measurement unit ("" where the backend has no
# matching unit slug; recorded in observations instead).
INT_UNITS = {
    "CH06": "year",
    "PP3E_TOT": "hour",
    "PP3F_TOT": "hour",
    "PP06L": "hour",
    "PP08H": "hour",
    "PP06K_SEM": "day",
    "PP06K_MES": "day",
    "PP08G_DSEM": "day",
    "PP08G_DMES": "day",
    "PP04B3_ANO": "year",
    "PP04B3_MES": "month",
    "PP04B3_DIA": "day",
    "PP05B2_ANO": "year",
    "PP05B2_MES": "month",
    "PP05B2_DIA": "day",
    "PP11B2_ANO": "year",
    "PP11B2_MES": "month",
    "PP11B2_DIA": "day",
    "PP11G_ANO": "year",
    "PP11G_MES": "month",
    "PP11G_DIA": "day",
    "IX_TOT": "person",
    "IX_MEN10": "person",
    "IX_MAYEQ10": "person",
    "PP04B2": "household",
    # Room and occupation counts are genuine quantities, but the backend
    # measurement-unit vocabulary has no room/occupation slug, so the unit is
    # named in observations instead of being forced onto a wrong slug.
    "IV2": "",
    "II1": "",
    "II2": "",
    "II3_1": "",
    "II5_1": "",
    "II6_1": "",
    "PP03D": "",
}
# The quantity each unitless count measures, as an i18n.VALUES key.
UNITLESS_NOTE = {
    "IV2": "rooms",
    "II1": "rooms",
    "II2": "rooms",
    "II3_1": "rooms",
    "II5_1": "rooms",
    "II6_1": "rooms",
    "PP03D": "occupations",
}
# Classifier codes resolved against external INDEC classifiers (CNO for
# occupation, CAES/CLANAE for activity), not against this dataset's dicionario.
CLASSIFIER_CODES = {
    "PP04B_COD": "caes",
    "PP04D_COD": "cno",
    "PP11B_COD": "caes",
    "PP11D_COD": "cno",
    "PP04B_CAES": "caes10",
    "PP11B_CAES": "caes10",
    "CH15_COD": "birthplace",
    "CH16_COD": "prior_residence",
}
# Pure identifiers are never dictionary-covered, whatever the sources suggest.
# NRO_HOGAR is listed here deliberately: the record-layout PDF prints the codes
# 51 = Servicio domestico and 71 = Pensionistas underneath NRO_HOGAR, but the
# Stata value labels attach them to COMPONENTE, where they belong. The PDF
# misplaces them by one row, so they are ignored for NRO_HOGAR.
NEVER_DICTIONARY = {"CODUSU", "NRO_HOGAR"}

# Decile-group columns. INDEC declares them as C (2) -- character with leading
# zeros -- and a decile number is a group label, not a quantity, so they are
# STRING. Values outside 1-10 occur (0 and 12 were observed); INDEC discusses
# their treatment in "Anexo I. Recomendaciones tecnicas para el uso de la
# informacion de ingresos" of the record layout rather than in a code table, so
# no label set is asserted for them here.
DECILE_NOTE = (
    "Numero de grupo decilico, no una cantidad: se preserva como cadena con el "
    "cero a la izquierda tal como lo publica la fuente. Se observan valores "
    "fuera del rango 1 a 10 (0 y 12); su tratamiento se describe en el Anexo I "
    "del diseno de registros de INDEC"
)

# CH15_COD and CH16_COD do not merely change padding across eras: the three 2016
# waves encode them as three-letter abbreviations ("tuc", "bol", "par", in
# inconsistent case) while every other wave uses INDEC's numeric province and
# country codes. The two encodings are not comparable without a crosswalk, so the
# raw values are preserved and the break is documented. See analyse_geo_codes.py.
GEO_CODE_NOTE = (
    "Atencion: la codificacion cambia entre ondas. Las tres ondas de 2016 "
    "(2016 Q2 a 2016 Q4) usan abreviaturas de tres letras, con mayusculas y "
    "minusculas mezcladas, mientras que el resto de la serie usa los codigos "
    "numericos de provincia y pais de INDEC. Se preservan los valores originales, "
    "por lo que comparar esas tres ondas con el resto exige una tabla de "
    "equivalencias"
)

SENTINEL_MINUS_NINE = (
    "El valor -9 indica Ns./Nr. y no un monto negativo; debe excluirse antes de "
    "cualquier calculo."
)


def money(col: str) -> bool:
    return col in MONEY or col in MONEY_EXTRA or col.endswith(MONEY_SUFFIX)


def bq_type(col: str, prof: dict, has_values: bool) -> str:
    if col in ("ANO4", "TRIMESTRE"):
        return "INT64"
    if col in WEIGHTS:
        return "INT64"
    if money(col):
        return "FLOAT64"
    if col in INT_UNITS:
        return "INT64"
    return "STRING"


def _appender(obs: dict[str, list[str]]):
    """Return an add(key, **kwargs) that appends one fragment in every language."""

    def add(key: str, **kwargs) -> None:
        for lang in LANGS:
            obs[lang].append(fragment(key, lang, **kwargs))

    return add


def temporal_coverage(entry: dict) -> str:
    """Empty when the column is in every wave; otherwise its own span."""
    if entry["n_waves"] == N_WAVES:
        return ""
    first, last = entry["first"], entry["last"]
    fy, fq = first.split("Q")
    ly, lq = last.split("Q")
    return f"{fy}-{int(fq):02d}(3){ly}-{int(lq):02d}"


def build(
    table: str, universe, labels, registro, profile, overrides
) -> list[dict]:
    drops = set(overrides.get("_drop", {}).get(table) or {})
    desc_over = overrides.get("description", {}).get(table) or {}
    rows = []
    for col, entry in universe[table].items():
        if not col or col in drops:
            continue
        prof = profile[table].get(col, {})
        reg = lookup(registro[table], col) or {}
        stata_label = labels[table].get(col) or ""
        pdf_desc = reg.get("description") or ""
        over = desc_over.get(col) or {}

        description = over.get("description") or stata_label or pdf_desc
        if not description:
            raise RuntimeError(
                f"{table}.{col}: no description from any source"
            )
        description = description[0].upper() + description[1:]
        description = description.rstrip(".")
        # BigQuery rejects a column description over 1024 characters, and dbt
        # persists these, so the whole model fails on one long description.
        # Enforce the limit here rather than discovering it during a dbt run.
        # Translations are keyed by the Spanish, which is the source language:
        # every description comes from a Stata variable label or INDEC's record
        # layout. A missing translation raises rather than writing the Spanish
        # into the English field, which is how a column set ends up looking
        # trilingual while being anything but.
        translation = TRANSLATIONS.get(description)
        if translation is None:
            raise RuntimeError(
                f"{table}.{col}: no translation for {description!r}. Add it to "
                f"translations.json under 'descriptions'."
            )
        for lang in ("pt", "en"):
            if not translation.get(lang):
                raise RuntimeError(
                    f"{table}.{col}: translations.json has no {lang} for "
                    f"{description!r}"
                )
        if len(description) > MAX_BQ_DESCRIPTION:
            raise RuntimeError(
                f"{table}.{col}: description is {len(description)} characters, "
                f"over BigQuery's {MAX_BQ_DESCRIPTION}-character limit. Fix the "
                f"source of the description rather than truncating it here: "
                f"{description[:120]}..."
            )

        has_values = bool(reg.get("values"))
        btype = bq_type(col, prof, has_values)
        name = STRUCTURAL_RENAME.get(col, col.lower())

        # Dictionary coverage: a coded categorical whose labels we hold.
        covered = "no"
        if (
            btype == "STRING"
            and col not in CLASSIFIER_CODES
            and col not in NEVER_DICTIONARY
            and (
                has_values
                or col in VALUE_LABEL_COLS[table]
                or col in DICT_OVERRIDE
            )
        ):
            covered = "yes"

        unit = ""
        if btype == "FLOAT64" and money(col):
            unit = "ars"
        elif btype == "INT64" and col in INT_UNITS:
            unit = INT_UNITS[col]
        elif col == "ANO4":
            unit = "year"
        elif col == "TRIMESTRE":
            unit = "quarter"

        directory = ""
        if name == "ano":
            directory = "br_bd_diretorios_data_tempo.ano:ano"
        elif name == "trimestre":
            directory = "br_bd_diretorios_data_tempo.trimestre:trimestre"

        # Observations are assembled per language from i18n.FRAGMENTS, not
        # written once in Spanish and translated: every sentence here is one this
        # repo authors, so generating it per language is lossless.
        obs: dict[str, list[str]] = {lang: [] for lang in LANGS}
        add = _appender(obs)

        if name in PARTITION:
            add("partition")
        if col in WEIGHTS:
            add("weight")
        if col in UNITLESS_NOTE:
            add("no_unit_slug", what=UNITLESS_NOTE[col])
        if col in CLASSIFIER_CODES:
            add("classifier", classifier=CLASSIFIER_CODES[col])
        if (
            "DEC" in col
            and btype == "STRING"
            and col.endswith(("IFR", "CFR", "CUR", "NDR", "CCF"))
        ):
            add("decile")
        if col == "AGLOMERADO":
            add("eph_geography_not_directory")
        if col == "REGION":
            add("eph_region_not_directory")
        if col in ("CH15_COD", "CH16_COD"):
            add("geo_code_encoding")
        if prof.get("min") is not None and prof["min"] == -9:
            add("sentinel_minus_nine")
        if col == "CH06":
            add("age_sentinels")
        if col == "CODUSU":
            add("codusu")
        if col == "MAS_500":
            add("mas_500")
        if col == "CH05":
            add("ch05_date")
        if entry["n_waves"] != N_WAVES:
            add(
                "partial_waves",
                n=entry["n_waves"],
                total=N_WAVES,
                first=entry["first"],
                last=entry["last"],
            )
        if not stata_label and pdf_desc and not over:
            add("from_pdf")
        if over.get("reason"):
            for lang in LANGS:
                reason = over.get(f"reason_{lang}") or over["reason"]
                obs[lang].append(reason)
        if name != col.lower():
            add("source_name", name=col)

        rows.append(
            {
                "name": name,
                "bigquery_type": btype,
                "description": description,
                "description_pt": translation["pt"],
                "description_en": translation["en"],
                "description_es": description,
                "temporal_coverage": temporal_coverage(entry),
                "covered_by_dictionary": covered,
                "directory_column": directory,
                "measurement_unit": unit,
                "has_sensitive_data": "no",
                "observations": ". ".join(obs["es"]),
                "observations_pt": ". ".join(obs["pt"]),
                "observations_en": ". ".join(obs["en"]),
                "observations_es": ". ".join(obs["es"]),
                "original_name": col,
            }
        )

    order = {n: i for i, n in enumerate(PARTITION + IDENTIFIERS)}
    weights_lower = {w.lower() for w in WEIGHTS}

    def sort_key(row):
        n = row["name"]
        if n in order:
            return (0, order[n], 0)
        if n in weights_lower:
            return (1, 0, list(universe[table]).index(row["original_name"]))
        return (2, 0, list(universe[table]).index(row["original_name"]))

    rows.sort(key=sort_key)
    return rows


# description/observations repeat the Spanish so the CSV stays readable as the
# source-language artefact, while the _pt/_en/_es columns are what the backend
# registration reads. upload_columns_from_sheet writes a bare `description` to
# descriptionPt, which would be wrong here; bulk_upsert_columns understands the
# per-language keys and is what code/columns_json.py targets.
FIELDS = [
    "name",
    "bigquery_type",
    "description",
    "description_pt",
    "description_en",
    "description_es",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "observations_pt",
    "observations_en",
    "observations_es",
    "original_name",
]


def main() -> int:
    load = lambda f: json.loads((CODE_DIR / f).read_text(encoding="utf-8"))  # noqa: E731
    universe = load("column_universe.json")
    labels = load("source_labels.json")
    registro = load("registro_parsed.json")
    profile = load("column_profile.json")
    overrides = load("overrides.json")
    value_labels = load("value_labels.json")
    global TRANSLATIONS
    TRANSLATIONS = load("translations.json")["descriptions"]
    global VALUE_LABEL_COLS, DICT_OVERRIDE
    VALUE_LABEL_COLS = {t: set(value_labels[t]) for t in TABLES}
    DICT_OVERRIDE = set(overrides.get("value_labels") or {})

    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    for table in TABLES:
        rows = build(table, universe, labels, registro, profile, overrides)
        path = ARCH_DIR / f"{table}.csv"
        # lineterminator is explicit: csv defaults to CRLF, which the
        # mixed-line-ending hook then rewrites, so every regeneration would
        # otherwise show the whole file as changed.
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle, fieldnames=FIELDS, lineterminator="\n"
            )
            writer.writeheader()
            writer.writerows(rows)
        from collections import Counter

        types = Counter(r["bigquery_type"] for r in rows)
        dict_cov = sum(1 for r in rows if r["covered_by_dictionary"] == "yes")
        partial = sum(1 for r in rows if r["temporal_coverage"])
        print(f"{table}: {len(rows)} columns -> {path.name}")
        print(f"   types: {dict(types)}")
        print(
            f"   dictionary-covered: {dict_cov}   partial coverage: {partial}"
        )
        missing_unit = [
            r["name"]
            for r in rows
            if r["bigquery_type"] in ("INT64", "FLOAT64")
            and not r["measurement_unit"]
            and r["original_name"] not in WEIGHTS
            and r["original_name"] not in UNITLESS_NOTE
        ]
        if missing_unit:
            print(
                f"   !! numeric without unit and without a note: {missing_unit}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
