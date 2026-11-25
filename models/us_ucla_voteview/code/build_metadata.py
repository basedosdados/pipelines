"""Build the trilingual column payloads for metadata registration.

Usage:
    uv run --no-sync python models/us_ucla_voteview/code/build_metadata.py OUT_DIR

Writes ``OUT_DIR/columns_<table>.json``, the ``columns_json`` argument of the
databasis MCP ``bulk_upsert_columns`` tool, for every table. English text comes
from the architecture CSVs; Portuguese and Spanish come from ``PT_ES`` below,
keyed by the English string. A string without a translation stops the build.
"""

import csv
import json
import sys
from pathlib import Path

ARCH = Path(__file__).parent / "architecture"
TABLES = ["members", "parties", "rollcalls", "votes", "dictionary"]

NOM_PT, NOM_ES = "NOMINATE (DW-NOMINATE)", "NOMINATE (DW-NOMINATE)"
SCALE = (
    "Ideal-point coordinate, roughly between -1 and 1. The first dimension is "
    "usually read as economic liberal (negative) to conservative (positive). "
    "Re-estimated over the whole history when new votes arrive, so past values "
    "can change between releases"
)

PT_ES = {
    # Shared columns
    "Number of the Congress, e.g. 115 for the 115th Congress (2017-2019)": (
        "Número do Congresso, por exemplo 115 para o 115º Congresso (2017-2019)",
        "Número del Congreso, por ejemplo 115 para el 115.º Congreso (2017-2019)",
    ),
    "Ordinal Congress number, starting at 1 in 1789. Each Congress spans two years. Partition column of the partitioned tables": (
        "Número ordinal do Congresso, começando em 1 em 1789. Cada Congresso dura dois anos. Coluna de partição das tabelas particionadas",
        "Número ordinal del Congreso, que comienza en 1 en 1789. Cada Congreso abarca dos años. Columna de partición de las tablas particionadas",
    ),
    "Chamber: House, Senate or President": (
        "Câmara: House (Câmara dos Representantes), Senate (Senado) ou President (Presidente)",
        "Cámara: House (Cámara de Representantes), Senate (Senado) o President (Presidente)",
    ),
    "Sequential number of the roll call within the Congress and chamber": (
        "Número sequencial da votação nominal dentro do Congresso e da câmara",
        "Número secuencial de la votación nominal dentro del Congreso y la cámara",
    ),
    "Starts at 1 in the first roll call of each Congress. Excludes quorum calls and vacated votes, so it does not match clerk_rollnumber. Stored as text: cast to INT64 to sort numerically": (
        "Começa em 1 na primeira votação nominal de cada Congresso. Exclui verificações de quórum e votações anuladas, por isso não coincide com clerk_rollnumber. Armazenado como texto: converta para INT64 para ordenar numericamente",
        "Comienza en 1 en la primera votación nominal de cada Congreso. Excluye las verificaciones de quórum y las votaciones anuladas, por lo que no coincide con clerk_rollnumber. Almacenado como texto: convierta a INT64 para ordenar numéricamente",
    ),
    "ICPSR identifier of the member": (
        "Identificador ICPSR do membro",
        "Identificador ICPSR del miembro",
    ),
    "In general one identifier per member for their whole career. Members who switched parties, and members who later became president, receive a new identifier so that a separate NOMINATE estimate can be produced. Use bioguide_id to identify the person": (
        "Em geral, um identificador por membro em toda a carreira. Membros que trocaram de partido, e membros que depois se tornaram presidentes, recebem um novo identificador para que se produza uma estimativa NOMINATE separada. Use bioguide_id para identificar a pessoa",
        "En general, un identificador por miembro durante toda su carrera. Los miembros que cambiaron de partido, y los que después llegaron a la presidencia, reciben un nuevo identificador para producir una estimación NOMINATE separada. Use bioguide_id para identificar a la persona",
    ),
    "Date of the roll call": (
        "Data da votação nominal",
        "Fecha de la votación nominal",
    ),
    "Dimensionless fit statistic": (
        "Estatística de ajuste adimensional",
        "Estadístico de ajuste adimensional",
    ),
    SCALE: (
        "Coordenada de ponto ideal, aproximadamente entre -1 e 1. A primeira dimensão costuma ser lida como o eixo econômico de liberal (negativo) a conservador (positivo). Reestimada sobre toda a série histórica quando chegam novas votações, por isso valores passados podem mudar entre versões",
        "Coordenada de punto ideal, aproximadamente entre -1 y 1. La primera dimensión suele leerse como el eje económico de liberal (negativo) a conservador (positivo). Se reestima sobre toda la serie histórica cuando llegan nuevas votaciones, por lo que los valores pasados pueden cambiar entre versiones",
    ),
    # members
    "ICPSR code of the state the member represents": (
        "Código ICPSR do estado que o membro representa",
        "Código ICPSR del estado que representa el miembro",
    ),
    "ICPSR state codes. 92-96 are territories (PR, AS, GU, MP, VI) and 99 is the whole country (presidents). The state directory has no ICPSR code for territories, so the directory link is on state_id": (
        "Códigos de estado do ICPSR. 92-96 são territórios (PR, AS, GU, MP, VI) e 99 é o país inteiro (presidentes). O diretório de estados não tem código ICPSR para territórios, por isso a ligação com o diretório é feita por state_id",
        "Códigos de estado del ICPSR. 92-96 son territorios (PR, AS, GU, MP, VI) y 99 es el país entero (presidentes). El directorio de estados no tiene código ICPSR para los territorios, por lo que el vínculo con el directorio se hace por state_id",
    ),
    "Congressional district the member represents within their state": (
        "Distrito congressional que o membro representa dentro do estado",
        "Distrito congresional que representa el miembro dentro de su estado",
    ),
    "0 for senators and presidents. Historical at-large districts are coded 99, 98 or 1": (
        "0 para senadores e presidentes. Distritos históricos at-large (eleição por todo o estado) são codificados como 99, 98 ou 1",
        "0 para senadores y presidentes. Los distritos históricos at-large (elección en todo el estado) se codifican como 99, 98 o 1",
    ),
    "Two-letter postal abbreviation of the state the member represents": (
        "Sigla postal de duas letras do estado que o membro representa",
        "Abreviatura postal de dos letras del estado que representa el miembro",
    ),
    "USA for presidents": ("USA para presidentes", "USA para presidentes"),
    "FIPS code of the state the member represents": (
        "Código FIPS do estado que o membro representa",
        "Código FIPS del estado que representa el miembro",
    ),
    "Not in the Voteview members file. Added by Data Basis from state_abbrev. Empty for presidents": (
        "Não está no arquivo de membros do Voteview. Adicionado pela Base dos Dados a partir de state_abbrev. Vazio para presidentes",
        "No está en el archivo de miembros de Voteview. Agregado por Data Basis a partir de state_abbrev. Vacío para presidentes",
    ),
    "Voteview code of the member's party": (
        "Código Voteview do partido do membro",
        "Código Voteview del partido del miembro",
    ),
    "Party names are in the parties table (congress, chamber, party_code)": (
        "Os nomes dos partidos estão na tabela parties (congress, chamber, party_code)",
        "Los nombres de los partidos están en la tabla parties (congress, chamber, party_code)",
    ),
    "ICPSR occupancy code: order of the member among the occupants of the seat": (
        "Código de ocupação do ICPSR: ordem do membro entre os ocupantes da cadeira",
        "Código de ocupación del ICPSR: orden del miembro entre los ocupantes del escaño",
    ),
    "0 if the only occupant, 1 if the first, 2 if the second, and so on. Legacy ICPSR field, not verified by Voteview": (
        "0 se for o único ocupante, 1 se for o primeiro, 2 se for o segundo, e assim por diante. Campo legado do ICPSR, não verificado pelo Voteview",
        "0 si es el único ocupante, 1 si es el primero, 2 si es el segundo, y así sucesivamente. Campo heredado del ICPSR, no verificado por Voteview",
    ),
    "ICPSR attain-office code: the member's last means of attaining office": (
        "Código de forma de acesso ao cargo do ICPSR: última forma pela qual o membro chegou ao cargo",
        "Código de forma de acceso al cargo del ICPSR: última forma en que el miembro llegó al cargo",
    ),
    "Legacy ICPSR field, not verified by Voteview": (
        "Campo legado do ICPSR, não verificado pelo Voteview",
        "Campo heredado del ICPSR, no verificado por Voteview",
    ),
    "Name of the member, surname first": (
        "Nome do membro, sobrenome primeiro",
        "Nombre del miembro, apellido primero",
    ),
    "Identifier of the member in the Biographical Directory of the US Congress": (
        "Identificador do membro no Biographical Directory of the United States Congress",
        "Identificador del miembro en el Biographical Directory of the United States Congress",
    ),
    "Empty for most presidents": (
        "Vazio para a maioria dos presidentes",
        "Vacío para la mayoría de los presidentes",
    ),
    "Year of the member's birth": (
        "Ano de nascimento do membro",
        "Año de nacimiento del miembro",
    ),
    "Year of the member's death": (
        "Ano de falecimento do membro",
        "Año de fallecimiento del miembro",
    ),
    "NOMINATE (DW-NOMINATE) first-dimension estimate": (
        f"Estimativa {NOM_PT} da primeira dimensão",
        f"Estimación {NOM_ES} de la primera dimensión",
    ),
    "NOMINATE (DW-NOMINATE) second-dimension estimate": (
        f"Estimativa {NOM_PT} da segunda dimensão",
        f"Estimación {NOM_ES} de la segunda dimensión",
    ),
    "Log-likelihood of the member's NOMINATE (DW-NOMINATE) estimate": (
        f"Log-verossimilhança da estimativa {NOM_PT} do membro",
        f"Log-verosimilitud de la estimación {NOM_ES} del miembro",
    ),
    "Geometric mean probability of the member's NOMINATE (DW-NOMINATE) estimate": (
        f"Probabilidade média geométrica da estimativa {NOM_PT} do membro",
        f"Probabilidad media geométrica de la estimación {NOM_ES} del miembro",
    ),
    "Number of votes cast by the member in the Congress": (
        "Número de votos dados pelo membro no Congresso",
        "Número de votos emitidos por el miembro en el Congreso",
    ),
    "Number of the member's votes misclassified by the NOMINATE (DW-NOMINATE) estimate": (
        f"Número de votos do membro classificados incorretamente pela estimativa {NOM_PT}",
        f"Número de votos del miembro mal clasificados por la estimación {NOM_ES}",
    ),
    "Whether the NOMINATE (DW-NOMINATE) estimate was produced conditionally": (
        f"Se a estimativa {NOM_PT} foi produzida de forma condicional",
        f"Si la estimación {NOM_ES} se produjo de forma condicional",
    ),
    "Conditional estimates are provisional and are updated at the next full estimation. Empty in every row of the current release": (
        "Estimativas condicionais são provisórias e são atualizadas na próxima estimação completa. Vazio em todas as linhas da versão atual",
        "Las estimaciones condicionales son provisionales y se actualizan en la siguiente estimación completa. Vacío en todas las filas de la versión actual",
    ),
    "Nokken-Poole first-dimension estimate": (
        "Estimativa Nokken-Poole da primeira dimensão",
        "Estimación Nokken-Poole de la primera dimensión",
    ),
    "Nokken-Poole second-dimension estimate": (
        "Estimativa Nokken-Poole da segunda dimensão",
        "Estimación Nokken-Poole de la segunda dimensión",
    ),
    "Estimated separately for each Congress, unlike nominate_dim1": (
        "Estimada separadamente para cada Congresso, ao contrário de nominate_dim1",
        "Estimada por separado para cada Congreso, a diferencia de nominate_dim1",
    ),
    "Estimated separately for each Congress, unlike nominate_dim2": (
        "Estimada separadamente para cada Congresso, ao contrário de nominate_dim2",
        "Estimada por separado para cada Congreso, a diferencia de nominate_dim2",
    ),
    # rollcalls
    "Session of the Congress in which the roll call took place": (
        "Sessão do Congresso em que a votação nominal ocorreu",
        "Sesión del Congreso en que tuvo lugar la votación nominal",
    ),
    "Empty for most historical roll calls": (
        "Vazio na maioria das votações nominais históricas",
        "Vacío en la mayoría de las votaciones nominales históricas",
    ),
    "Roll call number assigned by the clerk of the chamber": (
        "Número da votação nominal atribuído pelo secretário da câmara",
        "Número de la votación nominal asignado por el secretario de la cámara",
    ),
    "Includes quorum calls and vacated votes and restarts at 1 every session, not every Congress": (
        "Inclui verificações de quórum e votações anuladas e recomeça em 1 a cada sessão, não a cada Congresso",
        "Incluye las verificaciones de quórum y las votaciones anuladas y vuelve a 1 en cada sesión, no en cada Congreso",
    ),
    "Fraction of votes required to pass, e.g. 1/2, 2/3 or 3/5": (
        "Fração dos votos necessária para aprovação, por exemplo 1/2, 2/3 ou 3/5",
        "Fracción de los votos necesaria para aprobar, por ejemplo 1/2, 2/3 o 3/5",
    ),
    "Number of Yea votes": (
        "Número de votos Sim (Yea)",
        "Número de votos Sí (Yea)",
    ),
    "Number of Nay votes": (
        "Número de votos Não (Nay)",
        "Número de votos No (Nay)",
    ),
    "NOMINATE (DW-NOMINATE) first-dimension midpoint estimate of the roll call": (
        f"Estimativa {NOM_PT} do ponto médio da votação nominal na primeira dimensão",
        f"Estimación {NOM_ES} del punto medio de la votación nominal en la primera dimensión",
    ),
    "NOMINATE (DW-NOMINATE) second-dimension midpoint estimate of the roll call": (
        f"Estimativa {NOM_PT} do ponto médio da votação nominal na segunda dimensão",
        f"Estimación {NOM_ES} del punto medio de la votación nominal en la segunda dimensión",
    ),
    "NOMINATE (DW-NOMINATE) first-dimension spread estimate of the roll call": (
        f"Estimativa {NOM_PT} da dispersão (spread) da votação nominal na primeira dimensão",
        f"Estimación {NOM_ES} de la dispersión (spread) de la votación nominal en la primera dimensión",
    ),
    "NOMINATE (DW-NOMINATE) second-dimension spread estimate of the roll call": (
        f"Estimativa {NOM_PT} da dispersão (spread) da votação nominal na segunda dimensão",
        f"Estimación {NOM_ES} de la dispersión (spread) de la votación nominal en la segunda dimensión",
    ),
    "Log-likelihood of the roll call's NOMINATE (DW-NOMINATE) estimate": (
        f"Log-verossimilhança da estimativa {NOM_PT} da votação nominal",
        f"Log-verosimilitud de la estimación {NOM_ES} de la votación nominal",
    ),
    "Number of the bill on which the roll call was held": (
        "Número do projeto de lei sobre o qual a votação nominal foi feita",
        "Número del proyecto de ley sobre el que se realizó la votación nominal",
    ),
    "Official result of the roll call": (
        "Resultado oficial da votação nominal",
        "Resultado oficial de la votación nominal",
    ),
    "Description of the roll call assigned by congressional staff": (
        "Descrição da votação nominal atribuída pela equipe do Congresso",
        "Descripción de la votación nominal asignada por el personal del Congreso",
    ),
    "Question put to the vote, e.g. On Agreeing to the Amendment": (
        "Questão submetida à votação, por exemplo On Agreeing to the Amendment",
        "Cuestión sometida a votación, por ejemplo On Agreeing to the Amendment",
    ),
    "Description of the roll call from historical sources": (
        "Descrição da votação nominal a partir de fontes históricas",
        "Descripción de la votación nominal a partir de fuentes históricas",
    ),
    # votes
    "Not in the Voteview votes file. Added by Data Basis from the rollcalls table (congress, chamber, rollnumber)": (
        "Não está no arquivo de votos do Voteview. Adicionada pela Base dos Dados a partir da tabela rollcalls (congress, chamber, rollnumber)",
        "No está en el archivo de votos de Voteview. Agregada por Data Basis a partir de la tabla rollcalls (congress, chamber, rollnumber)",
    ),
    "How the member voted": ("Como o membro votou", "Cómo votó el miembro"),
    "Estimated probability, from NOMINATE (DW-NOMINATE), of the member casting the vote as recorded": (
        f"Probabilidade estimada, pelo {NOM_PT}, de o membro votar como registrado",
        f"Probabilidad estimada, según {NOM_ES}, de que el miembro vote como se registró",
    ),
    # parties
    "Voteview code of the party": (
        "Código Voteview do partido",
        "Código Voteview del partido",
    ),
    "Name of the party": ("Nome do partido", "Nombre del partido"),
    "Number of party members in the chamber in the Congress": (
        "Número de membros do partido na câmara no Congresso",
        "Número de miembros del partido en la cámara en el Congreso",
    ),
    "NOMINATE (DW-NOMINATE) first-dimension median of the party's members": (
        f"Mediana {NOM_PT} da primeira dimensão entre os membros do partido",
        f"Mediana {NOM_ES} de la primera dimensión entre los miembros del partido",
    ),
    "NOMINATE (DW-NOMINATE) second-dimension median of the party's members": (
        f"Mediana {NOM_PT} da segunda dimensão entre os membros do partido",
        f"Mediana {NOM_ES} de la segunda dimensión entre los miembros del partido",
    ),
    "NOMINATE (DW-NOMINATE) first-dimension mean of the party's members": (
        f"Média {NOM_PT} da primeira dimensão entre os membros do partido",
        f"Media {NOM_ES} de la primera dimensión entre los miembros del partido",
    ),
    "NOMINATE (DW-NOMINATE) second-dimension mean of the party's members": (
        f"Média {NOM_PT} da segunda dimensão entre os membros do partido",
        f"Media {NOM_ES} de la segunda dimensión entre los miembros del partido",
    ),
    # dictionary
    "Table identifier": (
        "Identificador da tabela",
        "Identificador de la tabla",
    ),
    "Column name": ("Nome da coluna", "Nombre de la columna"),
    "Code": ("Código", "Código"),
    "Temporal coverage of the code": (
        "Cobertura temporal do código",
        "Cobertura temporal del código",
    ),
    "Meaning of the code": ("Significado do código", "Significado del código"),
}


def tr(text: str) -> tuple[str, str]:
    if not text:
        return "", ""
    if text not in PT_ES:
        raise KeyError(f"no translation for: {text!r}")
    return PT_ES[text]


def columns(table: str) -> list[dict]:
    out = []
    with open(ARCH / f"{table}.csv") as f:
        for c in csv.DictReader(f):
            d_pt, d_es = tr(c["description"])
            o_pt, o_es = tr(c["observations"])
            out.append(
                {
                    "name": c["name"],
                    "bigquery_type": c["bigquery_type"],
                    "description_pt": d_pt,
                    "description_en": c["description"],
                    "description_es": d_es,
                    "observations_pt": o_pt,
                    "observations_en": c["observations"],
                    "observations_es": o_es,
                    "covered_by_dictionary": c["covered_by_dictionary"]
                    == "yes",
                    "directory_column": c["directory_column"],
                    "measurement_unit": c["measurement_unit"],
                    "has_sensitive_data": c["has_sensitive_data"] == "yes",
                }
            )
    return out


def main():
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    for table in TABLES:
        cols = columns(table)
        (out / f"columns_{table}.json").write_text(
            json.dumps(cols, ensure_ascii=False)
        )
        print(f"{table}: {len(cols)} columns")


if __name__ == "__main__":
    main()
