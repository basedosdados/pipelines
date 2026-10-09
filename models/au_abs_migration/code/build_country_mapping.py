"""Resolve ABS SACC country-of-birth codes to ISO 3166-1 alpha-3.

The Overseas Migration spreadsheets identify country of birth by its SACC 2016
code and an abbreviated name ("PNG", "S & E Afr, nec"). To link the fact tables
to ``br_bd_diretorios_mundo.pais`` — whose primary key is ``sigla_iso3`` — each
SACC code has to be resolved to an ISO3 code.

Two inputs, both under ``$AU_ABS_MIGRATION_DATA/input``:

* ``CL_ERP_COB.json`` — the ABS codelist, which carries the full SACC names
  (downloaded by ``download.py``);
* ``pais.csv`` — ``sigla_iso3, nome_en`` from the directory, produced by

      select sigla_iso3, nome_en
      from `basedosdados.br_bd_diretorios_mundo.pais`
      where sigla_iso3 is not null

  (249 of the directory's 277 rows carry an ISO3 code; the other 28 cannot be
  joined against and are irrelevant here).

Matching is **exact on the normalised name only** — no fuzzy matching, which
would happily confuse Niger with Nigeria, Samoa with American Samoa, and the two
Congos with each other. Everything the exact pass does not resolve is listed
explicitly in ``ALIASES`` below, so every one of the 250 published codes is
either mapped by an exact name match or by a reviewed hand-written decision.

Output: ``sacc_iso3.csv`` next to this file, read by ``clean.py``.
"""

from __future__ import annotations

import csv
import json
import os
import re
import unicodedata
from pathlib import Path

import openpyxl

DATA_DIR = Path(
    os.environ.get(
        "AU_ABS_MIGRATION_DATA",
        Path.home() / "Downloads" / "au_abs_migration_data",
    )
)
INPUT_DIR = DATA_DIR / "input"
OUT_PATH = Path(__file__).resolve().parent / "sacc_iso3.csv"

# SACC codes the exact name match cannot resolve. A value of "" means the code
# has no ISO 3166-1 counterpart and the fact tables carry a null there.
ALIASES: dict[str, tuple[str, str]] = {
    # residual categories, not countries
    "0000": ("", "Inadequately described origin, not a country"),
    "1199": ("", "Residual 'not elsewhere classified' category"),
    "1599": ("", "Residual 'not elsewhere classified' category"),
    "8299": ("", "Residual 'not elsewhere classified' category"),
    "9299": ("", "Residual 'not elsewhere classified' category"),
    # aggregates broader than any single ISO3 entity
    "2100": (
        "GBR",
        "SACC groups the United Kingdom with the Channel Islands and the Isle of "
        "Man, which ISO 3166-1 splits into GBR, GGY, JEY and IMN",
    ),
    # territorial claims in Antarctica, all inside ISO 3166-1 AQ/ATA
    "1601": ("ATA", "Antarctic territorial claim"),
    "1602": ("ATA", "Antarctic territorial claim"),
    "1603": ("ATA", "Antarctic territorial claim"),
    "1604": ("ATA", "Antarctic territorial claim"),
    "1605": ("ATA", "Antarctic territorial claim"),
    "1606": ("ATA", "Antarctic territorial claim"),
    "1607": ("ATA", "Antarctic territorial claim"),
    # entities with no ISO 3166-1 alpha-3 code
    "3216": ("", "Kosovo has no ISO 3166-1 code"),
    "4108": (
        "",
        "Spanish North Africa (Ceuta and Melilla) has no ISO 3166-1 code",
    ),
    # naming variants
    "1404": ("FSM", "Micronesia, Federated States of"),
    "1506": ("ASM", "American Samoa"),
    "1513": ("PCN", "Pitcairn"),
    "4202": ("PSE", "Palestine, State of"),
    "4214": ("SYR", "Syrian Arab Republic"),
    "4215": ("TUR", "Turkey"),
    "5101": ("MMR", "Myanmar"),
    "5103": ("LAO", "Lao People's Democratic Republic"),
    "5105": ("VNM", "Viet Nam"),
    "6103": ("MAC", "Macao"),
    "6202": ("PRK", "Korea, Democratic People's Republic of"),
    "6203": ("KOR", "Korea, Republic of"),
    "8103": ("SPM", "Saint Pierre and Miquelon"),
    "8202": ("BOL", "Bolivia"),
    "8216": ("VEN", "Venezuela"),
    "8422": ("KNA", "Saint Kitts and Nevis"),
    "8423": ("LCA", "Saint Lucia"),
    "8424": ("VCT", "Saint Vincent and the Grenadines"),
    "8427": ("VGB", "Virgin Islands, British"),
    "8428": ("VIR", "Virgin Islands, U.S."),
    "8431": ("BLM", "Saint Barthélemy"),
    "8432": ("MAF", "Saint Martin, French part"),
    "9107": ("COG", "Congo, Republic of the"),
    "9108": ("COD", "Congo, Democratic Republic of the"),
    "9222": ("SHN", "Saint Helena, Ascension and Tristan da Cunha"),
    "9227": ("TZA", "Tanzania, the United Republic of"),
}


def normalise(name: str) -> str:
    ascii_name = (
        unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    )
    ascii_name = re.sub(r"\(.*?\)", " ", ascii_name.lower())
    return re.sub(r"\s+", " ", re.sub(r"[^a-z ]", " ", ascii_name)).strip()


def load_sacc_names(input_dir: Path = INPUT_DIR) -> dict[str, str]:
    payload = json.loads(
        (input_dir / "CL_ERP_COB.json").read_text(encoding="utf-8")
    )
    for codelist in payload["data"]["codelists"]:
        if codelist["id"] == "CL_ERP_COB":
            return {code["id"]: code["name"] for code in codelist["codes"]}
    raise RuntimeError("CL_ERP_COB not present in the downloaded structure")


def load_directory(input_dir: Path = INPUT_DIR) -> dict[str, list[str]]:
    names: dict[str, list[str]] = {}
    with (input_dir / "pais.csv").open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            names.setdefault(normalise(row["nome_en"]), []).append(
                row["sigla_iso3"]
            )
    return names


def build(
    codes: list[str], input_dir: Path = INPUT_DIR
) -> list[dict[str, str]]:
    sacc_names = load_sacc_names(input_dir)
    directory = load_directory(input_dir)
    rows = []
    for code in codes:
        name = sacc_names[code]
        if code in ALIASES:
            iso3, note = ALIASES[code]
        else:
            matches = directory.get(normalise(name), [])
            if len(matches) != 1:
                raise RuntimeError(
                    f"SACC {code} ({name}) has {len(matches)} exact matches in the "
                    "country directory; add it to ALIASES"
                )
            iso3, note = matches[0], ""
        rows.append(
            {
                "country_of_birth_id": code,
                "sacc_name": name,
                "country_iso3_code": iso3,
                "note": note,
            }
        )
    return rows


def published_codes(input_dir: Path = INPUT_DIR) -> list[str]:
    """The SACC codes the release actually publishes, in source order.

    Read straight from the spreadsheet rather than from ``clean.py``, which
    consumes this script's output and must not be imported back into it. SACC
    codes are exactly four digits, which also excludes the sheet's own title
    ("34070DO001_202425 Overseas Migration, 2024-25") from the first column.
    """
    workbook = openpyxl.load_workbook(
        input_dir / "34070DO001_202425.xlsx", read_only=True, data_only=True
    )
    try:
        codes = [
            str(row[0]).strip()
            for row in workbook["Table 1.1"].iter_rows(
                max_col=1, values_only=True
            )
            if row[0] is not None
            and re.fullmatch(r"\d{4}", str(row[0]).strip())
        ]
    finally:
        workbook.close()
    return list(dict.fromkeys(codes))


def main() -> None:
    rows = build(published_codes())
    with OUT_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "country_of_birth_id",
                "sacc_name",
                "country_iso3_code",
                "note",
            ],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    mapped = sum(1 for row in rows if row["country_iso3_code"])
    print(
        f"{OUT_PATH.name}: {len(rows)} SACC codes, {mapped} with an ISO3 code"
    )


if __name__ == "__main__":
    main()
