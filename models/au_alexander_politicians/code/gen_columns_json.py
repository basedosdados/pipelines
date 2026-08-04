"""Generate trilingual columns_json per table for au_alexander_politicians,
for mcp__databasis__bulk_upsert_columns(columns_json=...).

PT descriptions come from the architecture CSVs (source of truth); EN/ES are
declared here per (table, column). directory_column, covered_by_dictionary,
measurement_unit and observations are carried through from the architecture.
"""

import csv
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
OUT = HERE / "columns_json"

# (EN, ES) per (table, column). Keyed by table then column name.
TRANS = {
    "politician": {
        "id_politician": (
            "Unique identifier of the politician, usually surname followed by birth year",
            "Identificador único del político, generalmente apellido seguido del año de nacimiento",
        ),
        "id_wikidata": (
            "Wikidata identifier of the politician (Q code)",
            "Identificador del político en Wikidata (código Q)",
        ),
        "id_aph": (
            "Identifier of the politician in the Parliament of Australia Parliamentary Handbook (APH)",
            "Identificador del político en el Parliamentary Handbook del Parlamento de Australia (APH)",
        ),
        "surname": ("Surname of the politician", "Apellido del político"),
        "all_other_names": (
            "All other names of the politician",
            "Demás nombres del político",
        ),
        "first_name": (
            "First name of the politician",
            "Primer nombre del político",
        ),
        "common_name": (
            "Name by which the politician was most commonly known, when different from the first name",
            "Nombre por el cual el político era más conocido, cuando difiere del primer nombre",
        ),
        "display_name": (
            "Display name, composed of surname and common name or first name",
            "Nombre de visualización, compuesto por apellido y nombre común o primer nombre",
        ),
        "earlier_or_later_names": (
            "Earlier or later names, used mainly to record name changes after marriage",
            "Nombres anteriores o posteriores, usados sobre todo para registrar cambios de nombre tras el matrimonio",
        ),
        "title": (
            "Honorific title of the politician (Dr, Sir, Dame), used inconsistently in the source",
            "Título honorífico del político (Dr, Sir, Dame), usado de forma inconsistente en la fuente",
        ),
        "gender": ("Gender of the politician", "Género del político"),
        "birth_date": (
            "Birth date of the politician",
            "Fecha de nacimiento del político",
        ),
        "birth_year": (
            "Birth year of the politician, filled only when the full date is unknown",
            "Año de nacimiento del político, completado solo cuando se desconoce la fecha completa",
        ),
        "birth_place": (
            "Birth place of the politician, mostly taken from Wikidata",
            "Lugar de nacimiento del político, mayormente extraído de Wikidata",
        ),
        "death_date": (
            "Death date of the politician",
            "Fecha de fallecimiento del político",
        ),
        "indicator_member": (
            "Indicates whether the politician was a member of the House of Representatives (lower house)",
            "Indica si el político fue miembro de la Cámara de Representantes (cámara baja)",
        ),
        "indicator_senator": (
            "Indicates whether the politician was a member of the Senate (upper house)",
            "Indica si el político fue miembro del Senado (cámara alta)",
        ),
        "indicator_prime_minister": (
            "Indicates whether the politician was Prime Minister of Australia",
            "Indica si el político fue Primer Ministro de Australia",
        ),
        "url_wikipedia": (
            "URL of the politician's Wikipedia page",
            "URL de la página del político en Wikipedia",
        ),
        "url_adb": (
            "URL of the politician's entry in the Australian Dictionary of Biography (ADB)",
            "URL de la entrada del político en el Australian Dictionary of Biography (ADB)",
        ),
        "comments": (
            "Comments and caveats about the politician's record",
            "Comentarios y advertencias sobre el registro del político",
        ),
    },
    "party_affiliation": {
        "id_politician": (
            "Unique identifier of the politician",
            "Identificador único del político",
        ),
        "party_abbreviation": (
            "Party abbreviation as recorded in the Parliamentary Handbook",
            "Sigla del partido según el Parliamentary Handbook",
        ),
        "party_name": (
            "Party name as recorded in the Parliamentary Handbook",
            "Nombre del partido según el Parliamentary Handbook",
        ),
        "party_simplified_name": (
            "Simplified party name, aggregating name changes over time",
            "Nombre simplificado del partido, agregando cambios de nombre a lo largo del tiempo",
        ),
        "date_start": (
            "Start date of the party affiliation during the term",
            "Fecha de inicio de la afiliación partidaria durante el mandato",
        ),
        "date_end": (
            "End date of the party affiliation during the term",
            "Fecha de término de la afiliación partidaria durante el mandato",
        ),
        "indicator_party_changed_name": (
            "Indicates whether the change of record is due to the party changing its name rather than the politician changing party",
            "Indica si el cambio de registro se debe a un cambio de nombre del partido y no a un cambio de partido",
        ),
        "indicator_specific_date_inputted": (
            "Indicates whether the date was specifically inputted rather than inherited from the term",
            "Indica si la fecha fue ingresada específicamente y no heredada del mandato",
        ),
        "comments": (
            "Comments about the party affiliation",
            "Comentarios sobre la afiliación partidaria",
        ),
    },
    "house_member": {
        "id_politician": (
            "Unique identifier of the politician",
            "Identificador único del político",
        ),
        "id_state": (
            "Code of the state or territory of the electoral division",
            "Código del estado o territorio de la división electoral",
        ),
        "abbreviation_state": (
            "Abbreviation of the state or territory of the electoral division",
            "Sigla del estado o territorio de la división electoral",
        ),
        "division": (
            "Name of the electoral division (seat) in the House of Representatives",
            "Nombre de la división electoral (escaño) de la Cámara de Representantes",
        ),
        "date_start": (
            "Start date of the term in the division",
            "Fecha de inicio del mandato en la división",
        ),
        "date_end": (
            "End date of the term in the division",
            "Fecha de término del mandato en la división",
        ),
        "end_reason": (
            "Reason for the end of the term in the division",
            "Motivo del término del mandato en la división",
        ),
        "indicator_entered_at_by_election": (
            "Indicates whether the politician entered the division at a by-election",
            "Indica si el político asumió la división en una elección parcial (by-election)",
        ),
        "indicator_changed_seat": (
            "Indicates whether the politician left the division by changing seat rather than by defeat or retirement",
            "Indica si el político dejó la división por cambio de escaño y no por derrota o retiro",
        ),
        "comments": (
            "Comments about the term in the House of Representatives",
            "Comentarios sobre el mandato en la Cámara de Representantes",
        ),
    },
    "senator": {
        "id_politician": (
            "Unique identifier of the politician",
            "Identificador único del político",
        ),
        "id_state": (
            "Code of the state or territory represented by the senator",
            "Código del estado o territorio representado por el senador",
        ),
        "abbreviation_state": (
            "Abbreviation of the state or territory represented by the senator",
            "Sigla del estado o territorio representado por el senador",
        ),
        "date_start": (
            "Start date of the term in the Senate",
            "Fecha de inicio del mandato en el Senado",
        ),
        "date_end": (
            "End date of the term in the Senate",
            "Fecha de término del mandato en el Senado",
        ),
        "end_reason": (
            "Reason for the end of the term in the Senate",
            "Motivo del término del mandato en el Senado",
        ),
        "indicator_section_15_selection": (
            "Indicates whether the senator was appointed (section 15) rather than elected",
            "Indica si el senador fue designado (sección 15) y no elegido",
        ),
        "comments": (
            "Comments about the term in the Senate",
            "Comentarios sobre el mandato en el Senado",
        ),
    },
    "ministry": {
        "id_politician": (
            "Unique identifier of the politician",
            "Identificador único del político",
        ),
        "ministry": (
            "Name of the ministry (government), usually the surname of the Prime Minister",
            "Nombre del ministerio (gobierno), generalmente el apellido del Primer Ministro",
        ),
        "ministry_number": (
            "Sequential number of the ministry (government)",
            "Número secuencial del ministerio (gobierno)",
        ),
        "ministry_party": (
            "Party or coalition that formed the ministry",
            "Partido o coalición que formó el ministerio",
        ),
        "ministry_title": (
            "Title of the portfolio held by the politician in the ministry",
            "Título de la cartera ocupada por el político en el ministerio",
        ),
        "display_name": (
            "Display name of the politician in the ministry record",
            "Nombre de visualización del político en el registro del ministerio",
        ),
        "date_start": (
            "Start date in the portfolio",
            "Fecha de inicio en la cartera",
        ),
        "date_end": (
            "End date in the portfolio",
            "Fecha de término en la cartera",
        ),
        "indicator_assistant_or_secretary": (
            "Indicates whether the position is assistant minister or parliamentary secretary",
            "Indica si el cargo es de ministro asistente o secretario parlamentario",
        ),
        "comments": (
            "Comments about the ministry record",
            "Comentarios sobre el registro del ministerio",
        ),
    },
}


def build(table):
    with open(ARCH / f"{table}.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    cols = []
    for r in rows:
        name = r["name"]
        en, es = TRANS[table][name]
        c = {
            "name": name,
            "bigquery_type": r["bigquery_type"],
            "description_pt": r["description"],
            "description_en": en,
            "description_es": es,
            "covered_by_dictionary": r["covered_by_dictionary"].strip().lower()
            == "yes",
            "has_sensitive_data": r["has_sensitive_data"].strip().lower()
            == "yes",
        }
        if r["directory_column"].strip():
            c["directory_column"] = r["directory_column"].strip()
        if r["measurement_unit"].strip():
            c["measurement_unit"] = r["measurement_unit"].strip()
        if r["observations"].strip():
            c["observations"] = r["observations"].strip()
        cols.append(c)
    return cols


def main():
    OUT.mkdir(exist_ok=True)
    for table in TRANS:
        cols = build(table)
        (OUT / f"{table}.json").write_text(
            json.dumps(cols, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"wrote columns_json/{table}.json ({len(cols)} cols)")


if __name__ == "__main__":
    main()
