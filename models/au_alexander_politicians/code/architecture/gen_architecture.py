"""Generate architecture CSVs for au_alexander_politicians (source of truth).

One CSV per table, following the Data Basis architecture schema:
name, bigquery_type, description, temporal_coverage, covered_by_dictionary,
directory_column, measurement_unit, has_sensitive_data, observations, original_name

Descriptions are in Portuguese (house standard); EN/ES are generated at the
metadata step. Column names are English (English-language dataset).

Dataset: Rohan Alexander & Paul Hodgetts, "AustralianPoliticians" (MIT).
Australian federal politicians, 1901-2021. No publishing organization.
"""

import csv
from pathlib import Path

HERE = Path(__file__).resolve().parent

ARCH_HEADER = [
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

# Shared FK targets
STATE_DIR = "br_bd_diretorios_au.state:id_state"


# Each column: (name, type, description_pt, covered_by_dictionary,
#               directory_column, measurement_unit, observations, original_name)
def col(name, typ, desc, dict_="no", directory="", unit="", obs="", orig=""):
    return {
        "name": name,
        "bigquery_type": typ,
        "description": desc,
        "temporal_coverage": "",
        "covered_by_dictionary": dict_,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations": obs,
        "original_name": orig,
    }


TABLES = {}

# ------------------------------------------------------------------ politician
TABLES["politician"] = [
    col(
        "id_politician",
        "STRING",
        "Identificador único do político, geralmente sobrenome seguido do ano de nascimento",
        orig="uniqueID",
    ),
    col(
        "id_wikidata",
        "STRING",
        "Identificador do político na Wikidata (código Q)",
        orig="wikidataID",
    ),
    col(
        "id_aph",
        "STRING",
        "Identificador do político no Parliamentary Handbook do Parlamento da Austrália (APH)",
        orig="aphID",
        obs="Incorporado a partir do arquivo uniqueID_to_aphID.csv (relação 1:1); 5 identificadores do arquivo de origem sem correspondência na tabela principal foram descartados",
    ),
    col("surname", "STRING", "Sobrenome do político", orig="surname"),
    col(
        "all_other_names",
        "STRING",
        "Demais nomes do político",
        orig="allOtherNames",
    ),
    col("first_name", "STRING", "Primeiro nome do político", orig="firstName"),
    col(
        "common_name",
        "STRING",
        "Nome pelo qual o político era mais conhecido, quando diferente do primeiro nome",
        orig="commonName",
    ),
    col(
        "display_name",
        "STRING",
        "Nome de exibição, composto por sobrenome e nome comum ou primeiro nome",
        orig="displayName",
    ),
    col(
        "earlier_or_later_names",
        "STRING",
        "Nomes anteriores ou posteriores, usados sobretudo para registrar mudança de nome após casamento",
        orig="earlierOrLaterNames",
    ),
    col(
        "title",
        "STRING",
        "Título honorífico do político (Dr, Sir, Dame), usado de forma inconsistente na fonte",
        orig="title",
    ),
    col("gender", "STRING", "Gênero do político", orig="gender"),
    col(
        "birth_date",
        "DATE",
        "Data de nascimento do político",
        orig="birthDate",
        obs="Vazia quando apenas o ano de nascimento é conhecido; ver birth_year",
    ),
    col(
        "birth_year",
        "INT64",
        "Ano de nascimento do político, preenchido apenas quando a data completa é desconhecida",
        unit="year",
        orig="birthYear",
    ),
    col(
        "birth_place",
        "STRING",
        "Local de nascimento do político, majoritariamente extraído da Wikidata",
        orig="birthPlace",
    ),
    col(
        "death_date",
        "DATE",
        "Data de falecimento do político",
        orig="deathDate",
    ),
    col(
        "indicator_member",
        "STRING",
        "Indica se o político foi membro da Câmara dos Representantes (câmara baixa)",
        dict_="yes",
        orig="member",
    ),
    col(
        "indicator_senator",
        "STRING",
        "Indica se o político foi membro do Senado (câmara alta)",
        dict_="yes",
        orig="senator",
    ),
    col(
        "indicator_prime_minister",
        "STRING",
        "Indica se o político foi Primeiro-Ministro da Austrália",
        dict_="yes",
        orig="wasPrimeMinister",
        obs="Na fonte, apenas o valor 1 aparece; ausência foi preenchida como 0",
    ),
    col(
        "url_wikipedia",
        "STRING",
        "URL da página do político na Wikipédia",
        orig="wikipedia",
    ),
    col(
        "url_adb",
        "STRING",
        "URL da entrada do político no Australian Dictionary of Biography (ADB)",
        orig="adb",
    ),
    col(
        "comments",
        "STRING",
        "Comentários e ressalvas sobre o registro do político",
        orig="comments",
    ),
]

# ------------------------------------------------------------- party_affiliation
TABLES["party_affiliation"] = [
    col(
        "id_politician",
        "STRING",
        "Identificador único do político",
        orig="uniqueID",
    ),
    col(
        "party_abbreviation",
        "STRING",
        "Sigla do partido conforme o Parliamentary Handbook",
        orig="partyAbbrev",
    ),
    col(
        "party_name",
        "STRING",
        "Nome do partido conforme o Parliamentary Handbook",
        orig="partyName",
    ),
    col(
        "party_simplified_name",
        "STRING",
        "Nome simplificado do partido, agregando mudanças de nome ao longo do tempo",
        orig="partySimplifiedName",
    ),
    col(
        "date_start",
        "DATE",
        "Data de início da filiação partidária no mandato",
        orig="partyFrom",
        obs="Vazia quando a filiação vigora desde o início do primeiro mandato do político",
    ),
    col(
        "date_end",
        "DATE",
        "Data de término da filiação partidária no mandato",
        orig="partyTo",
    ),
    col(
        "indicator_party_changed_name",
        "STRING",
        "Indica se a mudança de registro decorre de mudança de nome do partido, e não de troca de partido",
        dict_="yes",
        orig="partyChangedName",
        obs="Na fonte, apenas o valor 1 aparece; ausência foi preenchida como 0",
    ),
    col(
        "indicator_specific_date_inputted",
        "STRING",
        "Indica se a data foi inputada especificamente, e não herdada do mandato",
        dict_="yes",
        orig="partySpecificDateInputted",
        obs="Na fonte, apenas o valor 1 aparece; ausência foi preenchida como 0",
    ),
    col(
        "comments",
        "STRING",
        "Comentários sobre a filiação partidária",
        orig="partyComments",
    ),
]

# ------------------------------------------------------------------ house_member
TABLES["house_member"] = [
    col(
        "id_politician",
        "STRING",
        "Identificador único do político",
        orig="uniqueID",
    ),
    col(
        "id_state",
        "STRING",
        "Código do estado ou território da divisão eleitoral",
        directory=STATE_DIR,
        orig="stateOfDivision",
        obs="Derivado da sigla do estado da fonte (NSW, VIC, QLD, SA, WA, TAS, NT, ACT)",
    ),
    col(
        "abbreviation_state",
        "STRING",
        "Sigla do estado ou território da divisão eleitoral",
        orig="stateOfDivision",
    ),
    col(
        "division",
        "STRING",
        "Nome da divisão eleitoral (assento) da Câmara dos Representantes",
        orig="division",
        obs="Sem chave estrangeira formal: inclui divisões históricas, extintas ou renomeadas, ausentes dos recortes 2016/2021 do diretório commonwealth_electoral_division",
    ),
    col(
        "date_start",
        "DATE",
        "Data de início do mandato na divisão",
        orig="mpFrom",
    ),
    col(
        "date_end",
        "DATE",
        "Data de término do mandato na divisão",
        orig="mpTo",
    ),
    col(
        "end_reason",
        "STRING",
        "Motivo do término do mandato na divisão",
        orig="mpEndReason",
        obs="Rótulos livres da fonte, com pequenas inconsistências de grafia",
    ),
    col(
        "indicator_entered_at_by_election",
        "STRING",
        "Indica se o político assumiu a divisão em uma eleição suplementar (by-election)",
        dict_="yes",
        orig="enteredAtByElection",
        obs="Valores originais inconsistentes (1, Yes, No) normalizados para 0 e 1",
    ),
    col(
        "indicator_changed_seat",
        "STRING",
        "Indica se o político deixou a divisão por mudança de assento, e não por derrota ou aposentadoria",
        dict_="yes",
        orig="mpChangedSeat",
        obs="Na fonte, apenas o valor 1 aparece; ausência foi preenchida como 0",
    ),
    col(
        "comments",
        "STRING",
        "Comentários sobre o mandato na Câmara dos Representantes",
        orig="mpComments",
    ),
]

# ----------------------------------------------------------------------- senator
TABLES["senator"] = [
    col(
        "id_politician",
        "STRING",
        "Identificador único do político",
        orig="uniqueID",
    ),
    col(
        "id_state",
        "STRING",
        "Código do estado ou território representado pelo senador",
        directory=STATE_DIR,
        orig="senatorsState",
        obs="Derivado da sigla do estado da fonte (NSW, VIC, QLD, SA, WA, TAS, NT, ACT)",
    ),
    col(
        "abbreviation_state",
        "STRING",
        "Sigla do estado ou território representado pelo senador",
        orig="senatorsState",
    ),
    col(
        "date_start",
        "DATE",
        "Data de início do mandato no Senado",
        orig="senatorFrom",
    ),
    col(
        "date_end",
        "DATE",
        "Data de término do mandato no Senado",
        orig="senatorTo",
    ),
    col(
        "end_reason",
        "STRING",
        "Motivo do término do mandato no Senado",
        orig="senatorEndReason",
        obs="Rótulos livres da fonte, com pequenas inconsistências de grafia",
    ),
    col(
        "indicator_section_15_selection",
        "STRING",
        "Indica se o senador foi nomeado (seção 15), e não eleito",
        dict_="yes",
        orig="sec15Sel",
    ),
    col(
        "comments",
        "STRING",
        "Comentários sobre o mandato no Senado",
        orig="senatorComments",
    ),
]

# ---------------------------------------------------------------------- ministry
TABLES["ministry"] = [
    col(
        "id_politician",
        "STRING",
        "Identificador único do político",
        orig="uniqueID",
        obs="Ausente em 2 registros da fonte sem correspondência",
    ),
    col(
        "ministry",
        "STRING",
        "Nome do ministério (governo), geralmente o sobrenome do Primeiro-Ministro",
        orig="ministry",
    ),
    col(
        "ministry_number",
        "STRING",
        "Número sequencial do ministério (governo)",
        orig="ministry_number",
    ),
    col(
        "ministry_party",
        "STRING",
        "Partido ou coalizão que formou o ministério",
        orig="ministry_party",
    ),
    col(
        "ministry_title",
        "STRING",
        "Título da pasta ocupada pelo político no ministério",
        orig="ministry_title",
    ),
    col(
        "display_name",
        "STRING",
        "Nome de exibição do político no registro do ministério",
        orig="ministry_name",
    ),
    col("date_start", "DATE", "Data de início na pasta", orig="ministry_from"),
    col("date_end", "DATE", "Data de término na pasta", orig="ministry_to"),
    col(
        "indicator_assistant_or_secretary",
        "STRING",
        "Indica se o cargo é de ministro assistente ou secretário parlamentar",
        dict_="yes",
        orig="ministry_assistant_minister_or_parliamentary_secretary",
        obs="Na fonte, apenas o valor 1 aparece; ausência foi preenchida como 0",
    ),
    col(
        "comments",
        "STRING",
        "Comentários sobre o registro do ministério",
        orig="ministry_comment",
    ),
]

# -------------------------------------------------------------------- dicionario
TABLES["dicionario"] = [
    col("id_tabela", "STRING", "Nome da tabela à qual a coluna pertence"),
    col("nome_coluna", "STRING", "Nome da coluna coberta pelo dicionário"),
    col("chave", "STRING", "Chave (valor armazenado) da coluna"),
    col("cobertura_temporal", "STRING", "Cobertura temporal da chave"),
    col("valor", "STRING", "Valor (rótulo legível) correspondente à chave"),
]


def main():
    for table, cols in TABLES.items():
        out = HERE / f"{table}.csv"
        with open(out, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=ARCH_HEADER, lineterminator="\n")
            w.writeheader()
            for c in cols:
                w.writerow(c)
        print(f"wrote {out.name:24s} ({len(cols)} cols)")


if __name__ == "__main__":
    main()
