#!/usr/bin/env python3
"""Emit per-table columns_json for mcp bulk_upsert_columns, from the architecture
CSVs, with PT/EN/ES descriptions. Writes metadata/<dataset>/<table>.columns.json.

Run: cd models/us_nature_gerda/code && python3 build_columns_json.py
"""

import csv
import glob
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..", "..")  # models/
OUTDIR = os.path.join(ROOT, "us_nature_gerda", "metadata")

# EN description -> (PT, ES). Keyed by the exact English text in the architecture
# CSVs so per-table homonyms (e.g. `name`) translate independently.
TR = {
    "Ballot type: first_vote (Erststimme, candidate) or second_vote (Zweitstimme, party list)": (
        "Tipo de voto: first_vote (Erststimme, candidato) ou second_vote (Zweitstimme, lista partidária)",
        "Tipo de voto: first_vote (Erststimme, candidato) o second_vote (Zweitstimme, lista de partido)",
    ),
    "2025 constituency boundary status relative to 2021: unchanged, redrawn, or new": (
        "Situação do limite do distrito de 2025 em relação a 2021: inalterado, redesenhado ou novo",
        "Situación del límite de la circunscripción de 2025 respecto a 2021: sin cambios, redibujado o nuevo",
    ),
    "Row category: party, residual_other, local_voter_groups, or independents": (
        "Categoria da linha: party, residual_other, local_voter_groups ou independents",
        "Categoría de la fila: party, residual_other, local_voter_groups o independents",
    ),
    "Electoral constituency (Wahlkreis) name": (
        "Nome do distrito eleitoral (Wahlkreis)",
        "Nombre de la circunscripción electoral (Wahlkreis)",
    ),
    "Level of the constituency: federal or state": (
        "Nível do distrito: federal ou estadual",
        "Nivel de la circunscripción: federal o estatal",
    ),
    "County name as reported by the source": (
        "Nome do condado conforme informado pela fonte",
        "Nombre del condado según lo informado por la fuente",
    ),
    "County type: Landkreis or kreisfreie Stadt": (
        "Tipo de condado: Landkreis ou kreisfreie Stadt",
        "Tipo de condado: Landkreis o kreisfreie Stadt",
    ),
    "Date of the election": ("Data da eleição", "Fecha de la elección"),
    "Type of council election": (
        "Tipo de eleição para o conselho",
        "Tipo de elección para el consejo",
    ),
    "Number of eligible voters (Wahlberechtigte)": (
        "Número de eleitores aptos (Wahlberechtigte)",
        "Número de votantes habilitados (Wahlberechtigte)",
    ),
    "Party family (ParlGov)": (
        "Família partidária (ParlGov)",
        "Familia de partidos (ParlGov)",
    ),
    "1 for county-level mail-in aggregate rows (1994 and 1998 only), else 0": (
        "1 para linhas agregadas de voto postal em nível de condado (apenas 1994 e 1998), caso contrário 0",
        "1 para filas agregadas de voto por correo a nivel de condado (solo 1994 y 1998), de lo contrario 0",
    ),
    "1 if the row is a mail-in-only voting district (eligible_voters 0, valid_votes above 0), else 0": (
        "1 se a linha é um distrito eleitoral apenas de voto postal (eligible_voters 0, valid_votes acima de 0), caso contrário 0",
        "1 si la fila es un distrito de voto solo por correo (eligible_voters 0, valid_votes por encima de 0), de lo contrario 0",
    ),
    "1 if uncapped turnout exceeded 1 after harmonization, else 0": (
        "1 se o comparecimento sem limite excedeu 1 após a harmonização, caso contrário 0",
        "1 si la participación sin tope superó 1 tras la armonización, de lo contrario 0",
    ),
    "1 if uncapped turnout exceeded 1, a mail-in allocation artifact, else 0": (
        "1 se o comparecimento sem limite excedeu 1, um artefato da alocação de voto postal, caso contrário 0",
        "1 si la participación sin tope superó 1, un artefacto de la asignación de voto por correo, de lo contrario 0",
    ),
    "1 if the row reports no valid votes, else 0": (
        "1 se a linha não registra votos válidos, caso contrário 0",
        "1 si la fila no registra votos válidos, de lo contrario 0",
    ),
    "1 if the other-party share was derived as a residual rather than reported, else 0": (
        "1 se a parcela de outros partidos foi derivada como resíduo em vez de informada, caso contrário 0",
        "1 si la proporción de otros partidos se derivó como residuo en lugar de informarse, de lo contrario 0",
    ),
    "1 if seats_total does not equal the sum of the party seat columns, else 0": (
        "1 se seats_total difere da soma das colunas de cadeiras por partido, caso contrário 0",
        "1 si seats_total no es igual a la suma de las columnas de escaños por partido, de lo contrario 0",
    ),
    "1 if summed party votes do not match valid_votes, else 0": (
        "1 se a soma dos votos dos partidos não coincide com valid_votes, caso contrário 0",
        "1 si la suma de los votos de los partidos no coincide con valid_votes, de lo contrario 0",
    ),
    "1 if turnout was capped at 1 (European elections), else 0": (
        "1 se o comparecimento foi limitado a 1 (eleições europeias), caso contrário 0",
        "1 si la participación se limitó a 1 (elecciones europeas), de lo contrario 0",
    ),
    "1 if the boundary-harmonization merge fell back to an alternative match, else 0": (
        "1 se a fusão de harmonização de limites recorreu a uma correspondência alternativa, caso contrário 0",
        "1 si la fusión de armonización de límites recurrió a una coincidencia alternativa, de lo contrario 0",
    ),
    "Party of the county executive (Landrat or Oberbürgermeister); parteilos denotes an independent": (
        "Partido do executivo do condado (Landrat ou Oberbürgermeister); parteilos indica um independente",
        "Partido del ejecutivo del condado (Landrat u Oberbürgermeister); parteilos indica un independiente",
    ),
    "Electoral constituency (Wahlkreis) identifier": (
        "Identificador do distrito eleitoral (Wahlkreis)",
        "Identificador de la circunscripción electoral (Wahlkreis)",
    ),
    "County identifier, 5-digit Kreisschlüssel": (
        "Identificador do condado, Kreisschlüssel de 5 dígitos",
        "Identificador del condado, Kreisschlüssel de 5 dígitos",
    ),
    "Municipality identifier, 8-digit Amtlicher Gemeindeschlüssel (AGS)": (
        "Identificador do município, Amtlicher Gemeindeschlüssel (AGS) de 8 dígitos",
        "Identificador del municipio, Amtlicher Gemeindeschlüssel (AGS) de 8 dígitos",
    ),
    "Party or list, GERDA normalized name": (
        "Partido ou lista, nome normalizado do GERDA",
        "Partido o lista, nombre normalizado de GERDA",
    ),
    "State identifier, 2-digit Land code": (
        "Identificador do estado, código Land de 2 dígitos",
        "Identificador del estado, código Land de 2 dígitos",
    ),
    "Number of invalid votes (ungültige Stimmen)": (
        "Número de votos inválidos (ungültige Stimmen)",
        "Número de votos inválidos (ungültige Stimmen)",
    ),
    "1 if the party is CDU or CSU, else 0": (
        "1 se o partido é CDU ou CSU, caso contrário 0",
        "1 si el partido es CDU o CSU, de lo contrario 0",
    ),
    "1 if classified far left by GERDA (excluding Die Linke/PDS), else 0": (
        "1 se classificado como extrema-esquerda pelo GERDA (excluindo Die Linke/PDS), caso contrário 0",
        "1 si GERDA lo clasifica como extrema izquierda (excluyendo Die Linke/PDS), de lo contrario 0",
    ),
    "1 if classified far right by GERDA, else 0": (
        "1 se classificado como extrema-direita pelo GERDA, caso contrário 0",
        "1 si GERDA lo clasifica como extrema derecha, de lo contrario 0",
    ),
    "Left-right ideology score from ParlGov (0 left to 10 right)": (
        "Índice de ideologia esquerda-direita do ParlGov (0 esquerda a 10 direita)",
        "Índice de ideología izquierda-derecha de ParlGov (0 izquierda a 10 derecha)",
    ),
    "Municipality name as reported by the source": (
        "Nome do município conforme informado pela fonte",
        "Nombre del municipio según lo informado por la fuente",
    ),
    "Constituency (Wahlkreis) name": (
        "Nome do distrito (Wahlkreis)",
        "Nombre de la circunscripción (Wahlkreis)",
    ),
    "State name in English": (
        "Nome do estado em inglês",
        "Nombre del estado en inglés",
    ),
    "Short party name (ParlGov)": (
        "Nome curto do partido (ParlGov)",
        "Nombre corto del partido (ParlGov)",
    ),
    "ParlGov party identifier": (
        "Identificador do partido no ParlGov",
        "Identificador del partido en ParlGov",
    ),
    "Council seats won by the party": (
        "Cadeiras conquistadas pelo partido no conselho",
        "Escaños obtenidos por el partido en el consejo",
    ),
    "Council seats held outside the six major parties (freie_wahler plus regional plus other), comparable across years": (
        "Cadeiras fora dos seis principais partidos (freie_wahler mais regional mais outros), comparáveis entre anos",
        "Escaños fuera de los seis partidos principales (freie_wahler más regional más otros), comparables entre años",
    ),
    "Council seats won by all other parties combined": (
        "Cadeiras conquistadas por todos os demais partidos combinados",
        "Escaños obtenidos por todos los demás partidos combinados",
    ),
    "Council seats won by regional parties": (
        "Cadeiras conquistadas por partidos regionais",
        "Escaños obtenidos por partidos regionales",
    ),
    "Total council size (all parties)": (
        "Tamanho total do conselho (todos os partidos)",
        "Tamaño total del consejo (todos los partidos)",
    ),
    "Official state abbreviation (e.g. BY, NW)": (
        "Sigla oficial do estado (por exemplo, BY, NW)",
        "Abreviatura oficial del estado (por ejemplo, BY, NW)",
    ),
    "Voter turnout as a percentage (0-100), number_voters divided by eligible_voters times 100, capped at 100": (
        "Comparecimento eleitoral como percentual (0-100), number_voters dividido por eligible_voters vezes 100, limitado a 100",
        "Participación electoral como porcentaje (0-100), number_voters dividido por eligible_voters por 100, limitada a 100",
    ),
    "Number of valid votes (gültige Stimmen); counts votes not ballots under multi-vote systems": (
        "Número de votos válidos (gültige Stimmen); conta votos, não cédulas, em sistemas de múltiplos votos",
        "Número de votos válidos (gültige Stimmen); cuenta votos, no papeletas, en sistemas de voto múltiple",
    ),
    "Party vote share as a percentage (0-100); denominator is number_voters for federal and European, valid_votes otherwise": (
        "Percentual de votos do partido (0-100); o denominador é number_voters para eleições federais e europeias, valid_votes nas demais",
        "Porcentaje de votos del partido (0-100); el denominador es number_voters para federales y europeas, valid_votes en las demás",
    ),
    "Number of voters including invalid ballots (Wähler)": (
        "Número de votantes, incluindo cédulas inválidas (Wähler)",
        "Número de votantes, incluidas las papeletas inválidas (Wähler)",
    ),
    "Votes cast for the party": (
        "Votos dados ao partido",
        "Votos emitidos por el partido",
    ),
    "Election year": ("Ano da eleição", "Año de la elección"),
    "Constituency identifier: federal_<nr> or state_<state>_<nr>": (
        "Identificador do distrito eleitoral: federal_<nr> ou state_<estado>_<nr>",
        "Identificador de la circunscripción: federal_<nr> o state_<estado>_<nr>",
    ),
    "County name in German": (
        "Nome do condado em alemão",
        "Nombre del condado en alemán",
    ),
    "Municipality name in German": (
        "Nome do município em alemão",
        "Nombre del municipio en alemán",
    ),
    "Party name in English (ParlGov) or a readable form of the GERDA name": (
        "Nome do partido em inglês (ParlGov) ou uma forma legível do nome do GERDA",
        "Nombre del partido en inglés (ParlGov) o una forma legible del nombre de GERDA",
    ),
    "State name in German": (
        "Nome do estado em alemão",
        "Nombre del estado en alemán",
    ),
}


def cols_for(arch_csv):
    out = []
    with open(arch_csv) as fh:
        for r in csv.DictReader(fh):
            en = r["description"]
            pt, es = TR.get(en, (en, en))
            c = {
                "name": r["name"],
                "bigquery_type": r["bigquery_type"],
                "description_pt": pt,
                "description_en": en,
                "description_es": es,
                "covered_by_dictionary": r.get("covered_by_dictionary", "no")
                .strip()
                .lower()
                == "yes",
                "has_sensitive_data": r.get("has_sensitive_data", "no")
                .strip()
                .lower()
                == "yes",
            }
            if r.get("directory_column", "").strip():
                c["directory_column"] = r["directory_column"].strip()
            if r.get("measurement_unit", "").strip():
                c["measurement_unit"] = r["measurement_unit"].strip()
            if r.get("observations", "").strip():
                c["observations"] = r["observations"].strip()
            out.append(c)
    return out


def main():
    missing = set()
    for ds, arch_glob in [
        ("us_nature_gerda", "us_nature_gerda/code/architecture/*.csv"),
        ("br_bd_diretorios_de", "br_bd_diretorios_de/code/architecture/*.csv"),
    ]:
        d = os.path.join(ROOT, ds, "metadata")
        os.makedirs(d, exist_ok=True)
        for f in sorted(glob.glob(os.path.join(ROOT, arch_glob))):
            tbl = os.path.basename(f)[:-4]
            cols = cols_for(f)
            with open(f) as fh:
                for r in csv.DictReader(fh):
                    if r["description"] not in TR:
                        missing.add(r["description"])
            with open(os.path.join(d, f"{tbl}.columns.json"), "w") as fh:
                json.dump(cols, fh, ensure_ascii=False, indent=0)
        print(
            f"{ds}: wrote {len(glob.glob(os.path.join(ROOT, arch_glob)))} columns.json files"
        )
    if missing:
        print("\nUNTRANSLATED descriptions (fell back to EN):")
        for m in sorted(missing):
            print("  -", m)


if __name__ == "__main__":
    main()
