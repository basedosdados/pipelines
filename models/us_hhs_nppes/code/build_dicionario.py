"""Build ``dicionario.csv`` from the source's own Code Values document.

Run once per NPPES code-values revision:

    pdftotext -layout NPPES_Data_Dissemination_CodeValues.pdf codevalues.txt
    python build_dicionario.py --codevalues codevalues.txt

The country list (236 entries) is parsed out of that text; the small code sets
are transcribed here because they are stable and short. The resulting CSV is
committed — the cleaning transform only converts it to parquet.

Taxonomy codes are deliberately absent: the code set is maintained by NUCC and
licensed by the AMA for commercial use, so its labels are not redistributed.
"""

import argparse
import csv
import re
from pathlib import Path

HERE = Path(__file__).parent
OUT = HERE / "dicionario.csv"

# Words that must not be title-cased when prettifying the source's ALL-CAPS names.
_KEEP_UPPER = {"US", "UK", "USA", "UAE"}
_KEEP_LOWER = {"and", "of", "the", "d'"}


def prettify(name: str) -> str:
    parts = []
    for i, word in enumerate(name.split()):
        bare = re.sub(r"[^^\w']|\d|_", "", word)
        if bare.upper() in _KEEP_UPPER:
            parts.append(re.sub(r"[^\W\d_]+", bare.upper(), word, count=1))
        elif i and bare.lower() in _KEEP_LOWER:
            parts.append(word.lower())
        else:
            # capitalize each alphabetic run, so "TIMOR-LESTE" and "(VATICAN"
            # come out right
            parts.append(
                re.sub(r"[^\W\d_]+", lambda m: m.group(0).capitalize(), word)
            )
    out = " ".join(parts)
    # "Bonaire, Sint Eustatius And Saba" -> lower the conjunction after a comma too
    return re.sub(r"\bAnd\b", "and", out)


# (column, {code: label}) applied to every table that carries the column.
ENTITY_TYPE = {"1": "Individual", "2": "Organization"}
YES_NO_NA = {"X": "Not answered", "Y": "Yes", "N": "No"}
SEX = {"M": "Male", "F": "Female", "U": "Undisclosed", "X": "Undisclosed"}
NAME_TYPE = {
    "1": "Former name (individual)",
    "2": "Professional name (individual)",
    "3": "Doing business as (organization)",
    "4": "Former legal business name (organization)",
    "5": "Other name (individual or organization)",
    "6": "Organization has other names, listed in the other_name table",
}
IDENTIFIER_TYPE = {"01": "Other", "05": "Medicaid"}
TAXONOMY_GROUP = {
    "193200000X": "Multi-specialty group: a business group of one or more "
    "individual practitioners who practice in different areas "
    "of specialization",
    "193400000X": "Single specialty group: a business group of one or more "
    "individual practitioners who all practice in the same area "
    "of specialization",
}
ENDPOINT_TYPE = {
    "DIRECT": "Direct messaging address",
    "CONNECT": "CONNECT URL",
    "SOAP": "SOAP URL",
    "FHIR": "FHIR URL",
    "REST": "RESTful URL",
    "OTHERS": "Other URL",
}
USE_CODE = {
    "DIRECT": "Direct",
    "HIE": "Health information exchange (HIE)",
    "OTHER": "Other",
}
CONTENT_TYPE = {"CSV": "CSV", "OTHER": "Other"}
AFFILIATION = {"Y": "Yes", "N": "No"}

# (table, column, mapping)
STATIC = [
    ("provider", "entity_type_code", ENTITY_TYPE),
    ("provider", "sex_code", SEX),
    ("provider", "is_sole_proprietor", YES_NO_NA),
    ("provider", "is_organization_subpart", YES_NO_NA),
    ("provider", "other_organization_name_type_code", NAME_TYPE),
    (
        "provider",
        "other_last_name_type_code",
        {k: v for k, v in NAME_TYPE.items() if k != "6"},
    ),
    ("taxonomy", "is_primary_taxonomy", YES_NO_NA),
    ("taxonomy", "taxonomy_group_code", TAXONOMY_GROUP),
    ("other_identifier", "other_identifier_type_code", IDENTIFIER_TYPE),
    (
        "other_name",
        "other_organization_name_type_code",
        {k: v for k, v in NAME_TYPE.items() if k != "6"},
    ),
    ("endpoint", "endpoint_type", ENDPOINT_TYPE),
    ("endpoint", "use_code", USE_CODE),
    ("endpoint", "content_type", CONTENT_TYPE),
    ("endpoint", "affiliation", AFFILIATION),
]

# Columns that take the ISO country list, per table.
COUNTRY_COLUMNS = [
    ("provider", "mailing_address_country_code"),
    ("provider", "practice_address_country_code"),
    ("practice_location", "address_country_code"),
    ("endpoint", "affiliation_address_country_code"),
]


def parse_countries(codevalues_txt: Path) -> dict[str, str]:
    text = codevalues_txt.read_text(encoding="utf-8", errors="replace")
    block = re.search(
        r"^1\.10\s+Country Codes.*?Values in text format:(.*?)^1\.11",
        text,
        re.S | re.M,
    )
    if not block:
        raise SystemExit("Could not locate section 1.10 Country Codes")
    out = {}
    for line in block.group(1).splitlines():
        m = re.match(r"^([A-Z]{2}), (.+?)\s*$", line)
        if m:
            out[m.group(1)] = prettify(m.group(2))
    if len(out) < 200:
        raise SystemExit(
            f"Only parsed {len(out)} country codes; expected ~236"
        )
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--codevalues", required=True, type=Path)
    args = ap.parse_args()
    countries = parse_countries(args.codevalues)

    rows = []
    for table, column, mapping in STATIC:
        for key, value in mapping.items():
            rows.append([table, column, key, "", value])
    for table, column in COUNTRY_COLUMNS:
        for key, value in sorted(countries.items()):
            rows.append([table, column, key, "", value])

    with open(OUT, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(
            [
                "id_tabela",
                "nome_coluna",
                "chave",
                "cobertura_temporal",
                "valor",
            ]
        )
        w.writerows(rows)
    print(f"{OUT}: {len(rows)} rows ({len(countries)} country codes)")


if __name__ == "__main__":
    main()
