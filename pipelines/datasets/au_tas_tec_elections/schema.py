"""Column specification for every au_tas_tec_elections table.

Single source of truth: the architecture CSVs, the cleaning transform and the dbt
models are all generated from (or validated against) the ``TABLES`` mapping below.

Typing follows the house rule — INT64/FLOAT64 only where arithmetic is meaningful and
a measurement unit exists; every code, flag, sequence number and identifier is STRING.

Two Tasmania-specific design decisions are recorded here rather than in a comment
somewhere downstream:

1. **The inherited ``state_electoral_division_id`` cannot be populated.** The ABS
   layer ``state_electoral_division_2021`` for Tasmania is neither chamber's
   divisions — it is their *intersection*, 22 rows named "Bass (Launceston)",
   "Clark (Elwick)" and so on, because the ABS needs one non-overlapping layer and
   Tasmania has two overlapping systems. The column is kept for cross-state union
   compatibility and left NULL, because pointing it at a different directory would
   make the column name assert something false.
2. **House of Assembly divisions link to the Commonwealth directory instead.** The
   TEC states that the five divisions "have the same boundaries as the five
   Commonwealth House of Representatives divisions for Tasmania", so
   ``commonwealth_electoral_division_id`` is a real, storable foreign key.
   Legislative Council divisions get no directory link at all: the 15 of them appear
   in the ABS layer only as parentheticals, at no clean grain of their own.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Directory foreign keys use the BACKEND slug (diretorios_au, diretorios_data_tempo),
# not the BigQuery dataset id (br_bd_diretorios_*). An unresolved directory_column
# makes bulk_upsert_columns drop the whole column.
DIR_YEAR = "diretorios_data_tempo.ano:ano"
DIR_SED = (
    "diretorios_au.state_electoral_division_2021:id_state_electoral_division"
)
DIR_CED = (
    "diretorios_au.commonwealth_electoral_division_2021"
    ":id_commonwealth_electoral_division"
)


@dataclass(frozen=True)
class Column:
    name: str
    bigquery_type: str
    description: str  # Portuguese
    description_en: str
    description_es: str
    measurement_unit: str = ""
    covered_by_dictionary: str = "no"
    directory_column: str = ""
    has_sensitive_data: str = "no"
    observations: str = ""
    original_name: str = ""
    temporal_coverage: str = ""


C = Column


def votes(
    name: str, pt: str, en: str, es: str, original: str = "", obs: str = ""
) -> Column:
    return C(
        name,
        "INT64",
        pt,
        en,
        es,
        measurement_unit="vote",
        original_name=original,
        observations=obs,
    )


def pct(
    name: str, pt: str, en: str, es: str, original: str = "", obs: str = ""
) -> Column:
    return C(
        name,
        "FLOAT64",
        pt,
        en,
        es,
        measurement_unit="percent",
        original_name=original,
        observations=obs,
    )


# --------------------------------------------------------------------------------------
# Shared columns
# --------------------------------------------------------------------------------------

YEAR = C(
    "year",
    "INT64",
    "Ano do evento eleitoral",
    "Year of the electoral event",
    "Año del evento electoral",
    measurement_unit="year",
    directory_column=DIR_YEAR,
    observations="Coluna de particionamento. Vários eventos eleitorais podem compartilhar o mesmo ano: em 2021 e em 2022 houve eleição da Casa de Assembleia ou eleição suplementar no mesmo ano das eleições periódicas do Conselho Legislativo.",
)
ELECTION_ID = C(
    "election_id",
    "STRING",
    "Identificador do evento eleitoral",
    "Identifier of the electoral event",
    "Identificador del evento electoral",
    observations="Chave estrangeira para a tabela election, presente em todas as tabelas de resultados. Assume valores como hoa2025, lc2024 e lc2022pembroke.",
)
CONTEST_ID = C(
    "contest_id",
    "STRING",
    "Identificador da disputa por uma divisão dentro do evento eleitoral",
    "Identifier of the contest for a single division within the electoral event",
    "Identificador de la disputa por una división dentro del evento electoral",
    observations="Concatenação de election_id e do slug da divisão, como em hoa2025-bass. É único dentro do conjunto.",
)
CHAMBER = C(
    "chamber",
    "STRING",
    "Câmara do parlamento da Tasmânia à qual a disputa se refere",
    "Chamber of the Tasmanian parliament the contest refers to",
    "Cámara del parlamento de Tasmania a la que se refiere la disputa",
    covered_by_dictionary="yes",
    observations="Assume os valores house_of_assembly e legislative_council. As duas câmaras usam sistemas eleitorais diferentes e divisões geográficas que se sobrepõem sem coincidir, por isso a coluna é definida por disputa.",
)
GOVERNMENT_LEVEL = C(
    "government_level",
    "STRING",
    "Esfera de governo da disputa",
    "Level of government of the contest",
    "Nivel de gobierno de la disputa",
    covered_by_dictionary="yes",
    observations="Assume o valor state. A coluna é definida por disputa, e não como constante do conjunto, para que as eleições de governo local, também administradas pela TEC e ainda não incluídas, possam ser acrescentadas a estas mesmas tabelas sem reestruturá-las.",
)
CONTEST_TYPE = C(
    "contest_type",
    "STRING",
    "Tipo de divisão disputada",
    "Type of division contested",
    "Tipo de división disputada",
    covered_by_dictionary="yes",
    observations="Assume os valores house_of_assembly_division e legislative_council_division.",
)
VOTING_SYSTEM = C(
    "voting_system",
    "STRING",
    "Sistema de votação aplicado à disputa",
    "Voting system applied to the contest",
    "Sistema de votación aplicado a la disputa",
    covered_by_dictionary="yes",
    observations="Assume os valores hare_clark e preferential. A Casa de Assembleia usa Hare-Clark, o voto único transferível em divisões de múltiplas cadeiras com rotação Robson; o Conselho Legislativo usa voto preferencial em divisões de cadeira única.",
)
DISTRICT_NAME = C(
    "district_name",
    "STRING",
    "Nome da divisão disputada",
    "Name of the division contested",
    "Nombre de la división disputada",
    observations="Divisão da Casa de Assembleia ou divisão do Conselho Legislativo. Denison é a mesma divisão que Clark: o nome mudou por emenda ao Constitution Act sancionada em 28 de setembro de 2018, depois da eleição de 2018 e antes da de 2021.",
)
CED_ID = C(
    "commonwealth_electoral_division_id",
    "STRING",
    "Código da divisão eleitoral federal correspondente, no padrão ASGS 2021 do ABS",
    "Australian Statistical Geography Standard 2021 code of the corresponding Commonwealth electoral division",
    "Código de la división electoral federal correspondiente, en el estándar ASGS 2021 del ABS",
    directory_column=DIR_CED,
    observations="As cinco divisões da Casa de Assembleia coincidem com as cinco divisões federais da Tasmânia, conforme a própria TEC: as divisões estaduais têm as mesmas fronteiras das divisões federais da Câmara dos Representantes. Denison, usada até 2018, corresponde a Clark. Nulo em toda disputa do Conselho Legislativo, cujas 15 divisões não têm correspondente federal.",
)
SED_ID = C(
    "state_electoral_division_id",
    "STRING",
    "Código do distrito eleitoral estadual no padrão ASGS 2021 do ABS",
    "Australian Statistical Geography Standard 2021 code of the state electoral division",
    "Código del distrito electoral estatal en el estándar ASGS 2021 del ABS",
    directory_column=DIR_SED,
    observations="Nulo em todas as linhas. A camada state_electoral_division do ABS para a Tasmânia não é a de nenhuma das duas câmaras, e sim a interseção das duas, com 22 registros de nomes como Bass (Launceston) e Clark (Elwick): o ABS precisa de uma camada sem sobreposição e a Tasmânia tem dois sistemas que se sobrepõem. A coluna é mantida para compatibilidade com os demais estados, onde ela é preenchida; aqui o vínculo geográfico está em commonwealth_electoral_division_id.",
)

CONTEST_BLOCK = [
    YEAR,
    ELECTION_ID,
    CONTEST_ID,
    CHAMBER,
    GOVERNMENT_LEVEL,
    CONTEST_TYPE,
    VOTING_SYSTEM,
    DISTRICT_NAME,
    CED_ID,
    SED_ID,
]

BALLOT_NAME = C(
    "ballot_name",
    "STRING",
    "Nome da pessoa candidata tal como publicado pela TEC",
    "Name of the candidate as published by the TEC",
    "Nombre de la persona candidata tal como lo publica la TEC",
    observations='Formato "SOBRENOME, Prenomes". A Casa de Assembleia usa rotação Robson, que embaralha a ordem das candidaturas em cada cédula, de modo que não existe posição na cédula a publicar.',
)
CANDIDATE_SURNAME = C(
    "candidate_surname",
    "STRING",
    "Sobrenome da pessoa candidata",
    "Surname of the candidate",
    "Apellido de la persona candidata",
)
CANDIDATE_GIVEN_NAMES = C(
    "candidate_given_names",
    "STRING",
    "Prenomes da pessoa candidata",
    "Given names of the candidate",
    "Nombres de pila de la persona candidata",
)
PARTY_NAME = C(
    "party_name",
    "STRING",
    "Nome do partido ou grupo da pessoa candidata",
    "Name of the candidate's party or group",
    "Nombre del partido o grupo de la persona candidata",
    observations="Reproduz o rótulo publicado pela TEC, incluindo Independent para candidaturas sem partido.",
)
VOTING_CENTRE_NAME = C(
    "voting_centre_name",
    "STRING",
    "Nome do local de votação",
    "Name of the voting centre",
    "Nombre del local de votación",
    observations="Inclui, além dos locais de votação do dia da eleição, as categorias de voto especial publicadas pela TEC na mesma tabela, como voto postal, voto antecipado e voto fora da divisão.",
)
COUNT_NUMBER = C(
    "count_number",
    "STRING",
    "Número da contagem dentro da apuração da disputa",
    "Number of the count within the contest's scrutiny",
    "Número del escrutinio dentro del recuento de la disputa",
    observations="Número de ordem; aritmética sobre ele não tem sentido, por isso é publicado como texto. A folha de escrutínio da Casa de Assembleia agrupa contagens consecutivas de exclusão em intervalos, publicados como 4 to 6.",
)


# --------------------------------------------------------------------------------------
# Tables
# --------------------------------------------------------------------------------------

TABLES: dict[str, list[Column]] = {}

TABLES["election"] = [
    YEAR,
    C(
        "election_id",
        "STRING",
        "Identificador do evento eleitoral",
        "Identifier of the electoral event",
        "Identificador del evento electoral",
        observations="Chave da tabela. Assume valores como hoa2025, lc2024 e lc2022pembroke.",
    ),
    C(
        "election_name",
        "STRING",
        "Nome do evento eleitoral",
        "Name of the electoral event",
        "Nombre del evento electoral",
    ),
    CHAMBER,
    C(
        "election_type",
        "STRING",
        "Tipo do evento eleitoral",
        "Type of the electoral event",
        "Tipo del evento electoral",
        covered_by_dictionary="yes",
        observations="Assume os valores state_general, state_periodic e state_by_election. As eleições do Conselho Legislativo são periódicas e escalonadas: duas ou três divisões vão às urnas a cada ano, e não a câmara inteira.",
    ),
    C(
        "election_date",
        "DATE",
        "Data da votação",
        "Date of the poll",
        "Fecha de la votación",
    ),
    C(
        "seats_per_division",
        "INT64",
        "Número de cadeiras preenchidas em cada divisão do evento",
        "Number of seats filled in each division of the event",
        "Número de escaños ocupados en cada división del evento",
        measurement_unit="seat",
        observations="A Casa de Assembleia passou de 25 para 35 cadeiras na eleição de 2024: as mesmas cinco divisões passaram a eleger 7 pessoas em vez de 5. É uma ruptura de série, não uma continuidade, e altera a quota de Hare-Clark. O Conselho Legislativo elege uma pessoa por divisão.",
    ),
    C(
        "results_index_url",
        "STRING",
        "Endereço da página de resultados publicada pela TEC para o evento",
        "URL of the results index page published by the TEC for the event",
        "Dirección de la página de resultados publicada por la TEC para el evento",
    ),
]

TABLES["candidate"] = [
    *CONTEST_BLOCK,
    BALLOT_NAME,
    CANDIDATE_SURNAME,
    CANDIDATE_GIVEN_NAMES,
    PARTY_NAME,
    C(
        "is_elected",
        "STRING",
        "Indica se a pessoa candidata foi eleita na disputa",
        "Whether the candidate was elected in the contest",
        "Indica si la persona candidata fue electa en la disputa",
        covered_by_dictionary="yes",
        observations="Assume os valores yes e no.",
    ),
    C(
        "election_order",
        "STRING",
        "Ordem em que a pessoa candidata foi eleita dentro da disputa",
        "Order in which the candidate was elected within the contest",
        "Orden en que la persona candidata fue electa dentro de la disputa",
        observations="Número de ordem; aritmética sobre ele não tem sentido. Vai de 1 até o número de cadeiras da divisão e é nulo para quem não foi eleito.",
    ),
    C(
        "candidate_status",
        "STRING",
        "Situação final da pessoa candidata ao término da apuração",
        "Final status of the candidate at the end of the scrutiny",
        "Situación final de la persona candidata al término del recuento",
        covered_by_dictionary="yes",
        observations="Assume os valores elected, excluded e continuing. Continuing marca quem permanecia na contagem no encerramento sem ter atingido a quota, situação normal para a última cadeira de Hare-Clark.",
    ),
]

TABLES["enrolment_turnout"] = [
    *CONTEST_BLOCK,
    C(
        "enrolment",
        "INT64",
        "Número de pessoas inscritas na divisão",
        "Number of electors enrolled in the division",
        "Número de personas inscritas en la división",
        measurement_unit="person",
    ),
    votes(
        "votes_total",
        "Total de cédulas apuradas",
        "Total ballot papers counted",
        "Total de boletas escrutadas",
    ),
    votes(
        "votes_formal",
        "Total de votos válidos",
        "Total formal votes",
        "Total de votos válidos",
    ),
    votes(
        "votes_informal",
        "Total de votos inválidos",
        "Total informal votes",
        "Total de votos inválidos",
    ),
    pct(
        "percentage_turnout",
        "Cédulas apuradas como percentual das pessoas inscritas",
        "Ballot papers counted as a share of enrolled electors",
        "Boletas escrutadas como porcentaje de las personas inscritas",
    ),
    pct(
        "percentage_informal",
        "Votos inválidos como percentual das cédulas apuradas",
        "Informal votes as a share of ballot papers counted",
        "Votos inválidos como porcentaje de las boletas escrutadas",
    ),
    votes(
        "quota",
        "Quota necessária para a eleição na disputa",
        "Quota required for election in the contest",
        "Cuota necesaria para la elección en la disputa",
        obs="Quota de Droop, igual à parte inteira dos votos válidos divididos pelo número de cadeiras mais um, somada de um. No Conselho Legislativo, com uma cadeira, equivale à maioria absoluta.",
    ),
    C(
        "seats_to_elect",
        "INT64",
        "Número de cadeiras a preencher na disputa",
        "Number of seats to be filled in the contest",
        "Número de escaños a ocupar en la disputa",
        measurement_unit="seat",
    ),
    C(
        "candidates_count",
        "INT64",
        "Número de pessoas candidatas na disputa",
        "Number of candidates in the contest",
        "Número de personas candidatas en la disputa",
        measurement_unit="candidate",
    ),
]

TABLES["result_district"] = [
    *CONTEST_BLOCK,
    C(
        "count_type",
        "STRING",
        "Tipo de apuração ao qual os votos se referem",
        "Type of count the votes refer to",
        "Tipo de escrutinio al que se refieren los votos",
        covered_by_dictionary="yes",
        observations="Assume os valores first_preference e final_distribution. As duas não são recortes redundantes do mesmo número: a primeira traz os votos de primeira preferência e a segunda os votos após a distribuição completa de preferências, portanto filtre sempre por count_type.",
    ),
    BALLOT_NAME,
    PARTY_NAME,
    votes(
        "votes",
        "Votos atribuídos à pessoa candidata",
        "Votes credited to the candidate",
        "Votos atribuidos a la persona candidata",
    ),
    pct(
        "percentage_formal_votes",
        "Votos da pessoa candidata como percentual dos votos válidos da disputa",
        "Candidate votes as a share of the formal votes in the contest",
        "Votos de la persona candidata como porcentaje de los votos válidos de la disputa",
    ),
    C(
        "quotas",
        "FLOAT64",
        "Votos da pessoa candidata expressos em quotas",
        "Candidate votes expressed in quotas",
        "Votos de la persona candidata expresados en cuotas",
        measurement_unit="quota",
        observations="Votos divididos pela quota da disputa. Publicado apenas para a Casa de Assembleia, cujo sistema Hare-Clark elege quem atinge a quota; nulo no Conselho Legislativo.",
    ),
    C(
        "candidate_status",
        "STRING",
        "Situação da pessoa candidata na apuração a que a linha se refere",
        "Status of the candidate at the count the row refers to",
        "Situación de la persona candidata en el escrutinio al que se refiere la fila",
        covered_by_dictionary="yes",
        observations="Assume os valores elected, excluded e continuing. Preenchido apenas nas linhas de count_type igual a final_distribution.",
    ),
]

TABLES["result_voting_centre"] = [
    *CONTEST_BLOCK,
    VOTING_CENTRE_NAME,
    BALLOT_NAME,
    PARTY_NAME,
    votes(
        "votes",
        "Votos de primeira preferência da pessoa candidata no local de votação",
        "First preference votes for the candidate at the voting centre",
        "Votos de primera preferencia de la persona candidata en el local de votación",
    ),
    votes(
        "votes_formal",
        "Total de votos válidos no local de votação",
        "Total formal votes at the voting centre",
        "Total de votos válidos en el local de votación",
        obs="Total do local de votação, repetido em cada linha de pessoa candidata.",
    ),
    votes(
        "votes_informal",
        "Total de votos inválidos no local de votação",
        "Total informal votes at the voting centre",
        "Total de votos inválidos en el local de votación",
        obs="Total do local de votação, repetido em cada linha de pessoa candidata.",
    ),
    votes(
        "votes_total",
        "Total de cédulas apuradas no local de votação",
        "Total ballot papers counted at the voting centre",
        "Total de boletas escrutadas en el local de votación",
        obs="Total do local de votação, repetido em cada linha de pessoa candidata.",
    ),
]

TABLES["distribution_of_preferences"] = [
    *CONTEST_BLOCK,
    COUNT_NUMBER,
    BALLOT_NAME,
    PARTY_NAME,
    votes(
        "votes_transferred",
        "Votos transferidos para a pessoa candidata na contagem",
        "Votes transferred to the candidate at this count",
        "Votos transferidos a la persona candidata en el escrutinio",
        obs="Negativo na linha da pessoa candidata excluída ou eleita cujos votos estão sendo distribuídos.",
    ),
    votes(
        "votes_progressive_total",
        "Total acumulado de votos da pessoa candidata ao fim da contagem",
        "Running total of votes for the candidate at the end of the count",
        "Total acumulado de votos de la persona candidata al final del escrutinio",
    ),
    votes(
        "votes_exhausted",
        "Votos esgotados acumulados ao fim da contagem, sem preferência seguinte válida",
        "Votes exhausted in total at the end of the count, with no valid next preference",
        "Votos agotados acumulados al final del escrutinio, sin preferencia siguiente válida",
        obs="Total da contagem, repetido em cada linha de pessoa candidata.",
    ),
    C(
        "remarks",
        "STRING",
        "Observação publicada pela TEC sobre o que ocorreu na contagem",
        "Remark published by the TEC on what happened at this count",
        "Observación publicada por la TEC sobre lo ocurrido en el escrutinio",
        observations="Texto livre da fonte, como PETERSEN excluded ou GLADE-WRIGHT elected. Total da contagem, repetido em cada linha de pessoa candidata.",
    ),
]

TABLES["voting_centre"] = [
    YEAR,
    ELECTION_ID,
    VOTING_CENTRE_NAME,
    C(
        "locality",
        "STRING",
        "Localidade do local de votação",
        "Locality of the voting centre",
        "Localidad del local de votación",
    ),
    C(
        "premise_name",
        "STRING",
        "Nome do estabelecimento que sedia o local de votação",
        "Name of the premise hosting the voting centre",
        "Nombre del establecimiento que alberga el local de votación",
    ),
    C(
        "premise_address",
        "STRING",
        "Endereço do estabelecimento que sedia o local de votação",
        "Address of the premise hosting the voting centre",
        "Dirección del establecimiento que alberga el local de votación",
    ),
    C(
        "postcode",
        "STRING",
        "Código postal do local de votação",
        "Postcode of the voting centre",
        "Código postal del local de votación",
        observations="Código postal, não uma quantidade; publicado como texto.",
    ),
    C(
        "state_abbreviation",
        "STRING",
        "Sigla do estado do local de votação",
        "State abbreviation of the voting centre",
        "Sigla del estado del local de votación",
        observations="Constante em TAS. O vínculo com o diretório de estados é real, mas o backend só aceita chave estrangeira para a coluna marcada como chave primária do diretório, e a chave de diretorios_au.state é id_state, não abbreviation.",
    ),
    C(
        "location_within_premise",
        "STRING",
        "Localização do local de votação dentro do estabelecimento",
        "Location of the voting centre within the premise",
        "Ubicación del local de votación dentro del establecimiento",
    ),
    C(
        "disabled_access",
        "STRING",
        "Nível de acessibilidade do local de votação",
        "Level of accessibility of the voting centre",
        "Nivel de accesibilidad del local de votación",
        covered_by_dictionary="yes",
        observations="Assume os valores publicados pela TEC, entre eles Full, Assistance e None.",
    ),
    C(
        "district_name",
        "STRING",
        "Nome da divisão atendida pelo local de votação",
        "Name of the division served by the voting centre",
        "Nombre de la división atendida por el local de votación",
        observations="O grão da tabela é uma linha por evento eleitoral, local de votação e divisão atendida: um local que atende duas divisões aparece duas vezes.",
    ),
]

TABLES["dicionario"] = [
    C(
        "id_tabela",
        "STRING",
        "Slug da tabela descrita pela entrada do dicionário",
        "Slug of the table described by the dictionary entry",
        "Slug de la tabla descrita por la entrada del diccionario",
    ),
    C(
        "nome_coluna",
        "STRING",
        "Nome da coluna descrita pela entrada do dicionário",
        "Name of the column described by the dictionary entry",
        "Nombre de la columna descrita por la entrada del diccionario",
    ),
    C(
        "chave",
        "STRING",
        "Valor da chave codificada na coluna",
        "Coded key value stored in the column",
        "Valor de la clave codificada en la columna",
    ),
    C(
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal da entrada do dicionário",
        "Temporal coverage of the dictionary entry",
        "Cobertura temporal de la entrada del diccionario",
    ),
    C(
        "valor",
        "STRING",
        "Significado da chave codificada",
        "Meaning of the coded key",
        "Significado de la clave codificada",
    ),
]


PARTITION_COLUMNS: dict[str, list[str]] = {
    name: ([] if name == "dicionario" else ["year"]) for name in TABLES
}

PARTITION_RANGE = {"start": 2017, "end": 2035, "interval": 1}


def column_names(table: str) -> list[str]:
    return [c.name for c in TABLES[table]]


def column_types(table: str) -> dict[str, str]:
    return {c.name: c.bigquery_type for c in TABLES[table]}


# --------------------------------------------------------------------------------------
# Table-level metadata (dbt schema.yml and the Data Basis backend both read this)
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class TableMeta:
    name_pt: str
    name_en: str
    name_es: str
    description_pt: str
    description_en: str
    description_es: str
    unique_key: list[str] = field(default_factory=list)
    ignore_null_proportion: list[str] = field(default_factory=list)
    # Key columns the source allows to be NULL — part of the uniqueness key, but no
    # not_null test is emitted for them.
    nullable_key: list[str] = field(default_factory=list)


_NULL_GEO = [
    "state_electoral_division_id",
    "commonwealth_electoral_division_id",
]

TABLE_META: dict[str, TableMeta] = {
    "election": TableMeta(
        "Eventos eleitorais",
        "Electoral events",
        "Eventos electorales",
        "Catálogo dos eventos eleitorais estaduais da Tasmânia cobertos pelo conjunto: "
        "eleições gerais da Casa de Assembleia e eleições periódicas e suplementares do "
        "Conselho Legislativo. Serve de chave para todas as demais tabelas por "
        "election_id.",
        "Catalogue of the Tasmanian state electoral events covered by the dataset: House "
        "of Assembly general elections and Legislative Council periodic and by-elections. "
        "Acts as the key for every other table through election_id.",
        "Catálogo de los eventos electorales estatales de Tasmania cubiertos por el "
        "conjunto: elecciones generales de la Casa de Asamblea y elecciones periódicas y "
        "parciales del Consejo Legislativo. Sirve de clave para las demás tablas mediante "
        "election_id.",
        unique_key=["election_id"],
    ),
    "candidate": TableMeta(
        "Candidaturas",
        "Candidates",
        "Candidaturas",
        "Pessoas candidatas em cada disputa, com partido, situação final e ordem de "
        "eleição. A Casa de Assembleia usa rotação Robson, que embaralha a ordem das "
        "candidaturas em cada cédula, por isso não há posição na cédula a publicar.",
        "Candidates in each contest, with party, final status and order of election. The "
        "House of Assembly uses Robson Rotation, which reorders candidates on every "
        "ballot paper, so there is no ballot position to publish.",
        "Personas candidatas en cada disputa, con partido, situación final y orden de "
        "elección. La Casa de Asamblea usa rotación Robson, que altera el orden de las "
        "candidaturas en cada boleta, por lo que no hay posición en la boleta que "
        "publicar.",
        unique_key=["year", "election_id", "contest_id", "ballot_name"],
        ignore_null_proportion=[*_NULL_GEO, "election_order"],
    ),
    "enrolment_turnout": TableMeta(
        "Inscrições e comparecimento",
        "Enrolment and turnout",
        "Inscripciones y participación",
        "Uma linha por disputa, com pessoas inscritas, cédulas apuradas, votos válidos e "
        "inválidos, quota e número de cadeiras. O voto é obrigatório nas duas câmaras em "
        "todo o período coberto.",
        "One row per contest, with enrolled electors, ballot papers counted, formal and "
        "informal votes, quota and number of seats. Voting is compulsory in both chambers "
        "throughout the period covered.",
        "Una fila por disputa, con personas inscritas, boletas escrutadas, votos válidos e "
        "inválidos, cuota y número de escaños. El voto es obligatorio en ambas cámaras "
        "durante todo el período cubierto.",
        unique_key=["year", "election_id", "contest_id"],
        ignore_null_proportion=[
            *_NULL_GEO,
            "percentage_turnout",
            "percentage_informal",
            "votes_total",
            "votes_informal",
            "enrolment",
            "quota",
        ],
    ),
    "result_district": TableMeta(
        "Resultados por divisão",
        "Results by division",
        "Resultados por división",
        "Votos de cada pessoa candidata no total da divisão, por tipo de apuração. As "
        "linhas de primeira preferência e de distribuição final não são recortes "
        "redundantes do mesmo número, portanto filtre sempre por count_type.",
        "Votes for each candidate across the whole division, by count type. The first "
        "preference and final distribution rows are not redundant snapshots of the same "
        "number, so always filter on count_type.",
        "Votos de cada persona candidata en el total de la división, por tipo de "
        "escrutinio. Las filas de primera preferencia y de distribución final no son "
        "recortes redundantes del mismo número, por lo que conviene filtrar siempre por "
        "count_type.",
        unique_key=[
            "year",
            "election_id",
            "contest_id",
            "count_type",
            "ballot_name",
        ],
        ignore_null_proportion=[
            *_NULL_GEO,
            "quotas",
            "candidate_status",
            "percentage_formal_votes",
        ],
    ),
    "result_voting_centre": TableMeta(
        "Resultados por local de votação",
        "Results by voting centre",
        "Resultados por local de votación",
        "Votos de primeira preferência de cada pessoa candidata em cada local de votação, "
        "com os totais do local repetidos em cada linha de candidatura. Inclui as "
        "categorias de voto especial publicadas pela TEC na mesma tabela.",
        "First preference votes for each candidate at each voting centre, with the centre "
        "totals repeated on every candidate row. Includes the special vote categories the "
        "TEC publishes in the same table.",
        "Votos de primera preferencia de cada persona candidata en cada local de votación, "
        "con los totales del local repetidos en cada fila de candidatura. Incluye las "
        "categorías de voto especial que la TEC publica en la misma tabla.",
        unique_key=[
            "year",
            "election_id",
            "contest_id",
            "voting_centre_name",
            "ballot_name",
        ],
        ignore_null_proportion=[
            *_NULL_GEO,
            "votes_informal",
            "votes_total",
            "votes_formal",
        ],
    ),
    "distribution_of_preferences": TableMeta(
        "Distribuição de preferências",
        "Distribution of preferences",
        "Distribución de preferencias",
        "Transferências de voto contagem a contagem, com o total acumulado de cada pessoa "
        "candidata ao fim de cada contagem. Cobre a Casa de Assembleia a partir de 2024, "
        "quando a TEC passou a publicar a folha de escrutínio em planilha, e o Conselho "
        "Legislativo em todo o período.",
        "Vote transfers count by count, with each candidate's running total at the end of "
        "each count. Covers the House of Assembly from 2024, when the TEC began publishing "
        "the scrutiny sheet as a spreadsheet, and the Legislative Council throughout.",
        "Transferencias de voto escrutinio a escrutinio, con el total acumulado de cada "
        "persona candidata al final de cada uno. Cubre la Casa de Asamblea desde 2024, "
        "cuando la TEC empezó a publicar la hoja de escrutinio en planilla, y el Consejo "
        "Legislativo en todo el período.",
        unique_key=[
            "year",
            "election_id",
            "contest_id",
            "count_number",
            "ballot_name",
        ],
        ignore_null_proportion=[
            *_NULL_GEO,
            "votes_exhausted",
            "remarks",
            "votes_transferred",
            "votes_progressive_total",
        ],
    ),
    "voting_centre": TableMeta(
        "Locais de votação",
        "Voting centres",
        "Locales de votación",
        "Locais de votação de cada evento eleitoral, com endereço, código postal e nível "
        "de acessibilidade. O grão é uma linha por evento, local e divisão atendida.",
        "Voting centres for each electoral event, with address, postcode and accessibility "
        "level. The grain is one row per event, centre and division served.",
        "Locales de votación de cada evento electoral, con dirección, código postal y "
        "nivel de accesibilidad. El grano es una fila por evento, local y división "
        "atendida.",
        unique_key=[
            "year",
            "election_id",
            "voting_centre_name",
            "district_name",
        ],
        ignore_null_proportion=[
            "location_within_premise",
            "premise_address",
            "postcode",
            "premise_name",
            "disabled_access",
            "locality",
        ],
    ),
    "dicionario": TableMeta(
        "Dicionário",
        "Dictionary",
        "Diccionario",
        "Correspondência entre as chaves codificadas das colunas do conjunto e o seu "
        "significado.",
        "Mapping between the coded keys used in the dataset's columns and their meaning.",
        "Correspondencia entre las claves codificadas de las columnas del conjunto y su "
        "significado.",
        unique_key=["id_tabela", "nome_coluna", "chave"],
        # Never populated: no dictionary key in this dataset changes meaning over
        # time, so the column exists for schema conformity only.
        ignore_null_proportion=["cobertura_temporal"],
    ),
}

assert set(TABLE_META) == set(TABLES), (
    f"TABLE_META and TABLES disagree: {set(TABLE_META) ^ set(TABLES)}"
)


def validate() -> None:
    """Fail loudly on a duplicated column, an untyped quantity, or a bad description."""
    for table, cols in TABLES.items():
        names = [c.name for c in cols]
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise ValueError(f"{table}: duplicated columns {sorted(dupes)}")
        for c in cols:
            if (
                c.bigquery_type in ("INT64", "FLOAT64")
                and not c.measurement_unit
            ):
                raise ValueError(
                    f"{table}.{c.name}: numeric column with no unit"
                )
            for lang, text in (
                ("pt", c.description),
                ("en", c.description_en),
                ("es", c.description_es),
            ):
                if not text:
                    raise ValueError(
                        f"{table}.{c.name}: empty {lang} description"
                    )
                if text.endswith("."):
                    raise ValueError(
                        f"{table}.{c.name}: {lang} description ends with a period"
                    )
                if not text[0].isupper():
                    raise ValueError(
                        f"{table}.{c.name}: {lang} description is not capitalised"
                    )
            if c.name.startswith("id_") and table != "dicionario":
                raise ValueError(
                    f"{table}.{c.name}: English datasets take the _id suffix"
                )
            if (
                c.observations
                and c.observations not in OBSERVATION_TRANSLATIONS
            ):
                raise ValueError(
                    f"{table}.{c.name}: observation missing from "
                    "OBSERVATION_TRANSLATIONS"
                )


# Every distinct `observations` note, with its English and Spanish rendering.
# bulk_upsert_columns writes a bare `observations` key to Portuguese only, so
# EN and ES have to be carried explicitly or the column ends up PT-only in the
# backend. validate() fails on any note missing from this table.
OBSERVATION_TRANSLATIONS: dict[str, tuple[str, str]] = {
    "Coluna de particionamento. Vários eventos eleitorais podem "
    "compartilhar o mesmo ano: em 2021 e em 2022 houve eleição da Casa de "
    "Assembleia ou eleição suplementar no mesmo ano das eleições "
    "periódicas do Conselho Legislativo.": (
        "Partition column. Several electoral events can share the same "
        "year: in 2021 and in 2022 there was a House of Assembly election "
        "or a by-election in the same year as the periodic Legislative "
        "Council elections.",
        "Columna de particionamiento. Varios eventos electorales pueden "
        "compartir el mismo año: en 2021 y en 2022 hubo una elección de la "
        "Casa de la Asamblea o una elección suplementaria en el mismo año "
        "que las elecciones periódicas del Consejo Legislativo.",
    ),
    "Chave da tabela. Assume valores como hoa2025, lc2024 e lc2022pembroke.": (
        "Table key. Takes values such as hoa2025, lc2024 and lc2022pembroke.",
        "Clave de la tabla. Toma valores como hoa2025, lc2024 y "
        "lc2022pembroke.",
    ),
    "Assume os valores house_of_assembly e legislative_council. As duas "
    "câmaras usam sistemas eleitorais diferentes e divisões geográficas "
    "que se sobrepõem sem coincidir, por isso a coluna é definida por "
    "disputa.": (
        "Takes the values house_of_assembly and legislative_council. The "
        "two chambers use different electoral systems and geographic "
        "divisions that overlap without coinciding, so the column is "
        "defined per contest.",
        "Toma los valores house_of_assembly y legislative_council. Las dos "
        "cámaras usan sistemas electorales diferentes y divisiones "
        "geográficas que se superponen sin coincidir, por lo que la "
        "columna se define por contienda.",
    ),
    "Assume os valores state_general, state_periodic e state_by_election. "
    "As eleições do Conselho Legislativo são periódicas e escalonadas: "
    "duas ou três divisões vão às urnas a cada ano, e não a câmara "
    "inteira.": (
        "Takes the values state_general, state_periodic and "
        "state_by_election. Legislative Council elections are periodic and "
        "staggered: two or three divisions go to the polls each year, and "
        "not the whole chamber.",
        "Toma los valores state_general, state_periodic y "
        "state_by_election. Las elecciones del Consejo Legislativo son "
        "periódicas y escalonadas: dos o tres divisiones acuden a las "
        "urnas cada año, y no la cámara entera.",
    ),
    "A Casa de Assembleia passou de 25 para 35 cadeiras na eleição de "
    "2024: as mesmas cinco divisões passaram a eleger 7 pessoas em vez de "
    "5. É uma ruptura de série, não uma continuidade, e altera a quota de "
    "Hare-Clark. O Conselho Legislativo elege uma pessoa por divisão.": (
        "The House of Assembly went from 25 to 35 seats at the 2024 "
        "election: the same five divisions began electing 7 people instead "
        "of 5. This is a break in the series, not a continuity, and it "
        "changes the Hare-Clark quota. The Legislative Council elects one "
        "person per division.",
        "La Casa de la Asamblea pasó de 25 a 35 escaños en la elección de "
        "2024: las mismas cinco divisiones pasaron a elegir 7 personas en "
        "lugar de 5. Es una ruptura de serie, no una continuidad, y altera "
        "la cuota de Hare-Clark. El Consejo Legislativo elige una persona "
        "por división.",
    ),
    "Chave estrangeira para a tabela election, presente em todas as "
    "tabelas de resultados. Assume valores como hoa2025, lc2024 e "
    "lc2022pembroke.": (
        "Foreign key to the election table, present in every results "
        "table. Takes values such as hoa2025, lc2024 and lc2022pembroke.",
        "Clave foránea de la tabla election, presente en todas las tablas "
        "de resultados. Toma valores como hoa2025, lc2024 y "
        "lc2022pembroke.",
    ),
    "Concatenação de election_id e do slug da divisão, como em "
    "hoa2025-bass. É único dentro do conjunto.": (
        "Concatenation of election_id and the division slug, as in "
        "hoa2025-bass. It is unique within the dataset.",
        "Concatenación de election_id y del slug de la división, como en "
        "hoa2025-bass. Es único dentro del conjunto.",
    ),
    "Assume o valor state. A coluna é definida por disputa, e não como "
    "constante do conjunto, para que as eleições de governo local, também "
    "administradas pela TEC e ainda não incluídas, possam ser "
    "acrescentadas a estas mesmas tabelas sem reestruturá-las.": (
        "Takes the value state. The column is defined per contest, and not "
        "as a constant of the dataset, so that local government elections, "
        "also administered by the TEC and not yet included, can be added "
        "to these same tables without restructuring them.",
        "Toma el valor state. La columna se define por contienda, y no "
        "como constante del conjunto, para que las elecciones de gobierno "
        "local, también administradas por la TEC y aún no incluidas, "
        "puedan añadirse a estas mismas tablas sin reestructurarlas.",
    ),
    "Assume os valores house_of_assembly_division e "
    "legislative_council_division.": (
        "Takes the values house_of_assembly_division and "
        "legislative_council_division.",
        "Toma los valores house_of_assembly_division y "
        "legislative_council_division.",
    ),
    "Assume os valores hare_clark e preferential. A Casa de Assembleia usa "
    "Hare-Clark, o voto único transferível em divisões de múltiplas "
    "cadeiras com rotação Robson; o Conselho Legislativo usa voto "
    "preferencial em divisões de cadeira única.": (
        "Takes the values hare_clark and preferential. The House of "
        "Assembly uses Hare-Clark, the single transferable vote in multi- "
        "member divisions with Robson Rotation; the Legislative Council "
        "uses preferential voting in single-member divisions.",
        "Toma los valores hare_clark y preferential. La Casa de la "
        "Asamblea usa Hare-Clark, el voto único transferible en divisiones "
        "plurinominales con rotación Robson; el Consejo Legislativo usa "
        "voto preferencial en divisiones uninominales.",
    ),
    "Divisão da Casa de Assembleia ou divisão do Conselho Legislativo. "
    "Denison é a mesma divisão que Clark: o nome mudou por emenda ao "
    "Constitution Act sancionada em 28 de setembro de 2018, depois da "
    "eleição de 2018 e antes da de 2021.": (
        "House of Assembly division or Legislative Council division. "
        "Denison is the same division as Clark: the name changed by an "
        "amendment to the Constitution Act assented to on 28 September "
        "2018, after the 2018 election and before the 2021 one.",
        "División de la Casa de la Asamblea o división del Consejo "
        "Legislativo. Denison es la misma división que Clark: el nombre "
        "cambió por una enmienda al Constitution Act sancionada el 28 de "
        "septiembre de 2018, después de la elección de 2018 y antes de la "
        "de 2021.",
    ),
    "As cinco divisões da Casa de Assembleia coincidem com as cinco "
    "divisões federais da Tasmânia, conforme a própria TEC: as divisões "
    "estaduais têm as mesmas fronteiras das divisões federais da Câmara "
    "dos Representantes. Denison, usada até 2018, corresponde a Clark. "
    "Nulo em toda disputa do Conselho Legislativo, cujas 15 divisões não "
    "têm correspondente federal.": (
        "The five House of Assembly divisions coincide with Tasmania's "
        "five federal divisions, according to the TEC itself: the state "
        "divisions have the same boundaries as the federal House of "
        "Representatives divisions. Denison, used until 2018, corresponds "
        "to Clark. Null in every Legislative Council contest, whose 15 "
        "divisions have no federal counterpart.",
        "Las cinco divisiones de la Casa de la Asamblea coinciden con las "
        "cinco divisiones federales de Tasmania, según la propia TEC: las "
        "divisiones estatales tienen las mismas fronteras que las "
        "divisiones federales de la Cámara de Representantes. Denison, "
        "usada hasta 2018, corresponde a Clark. Nulo en toda contienda del "
        "Consejo Legislativo, cuyas 15 divisiones no tienen "
        "correspondiente federal.",
    ),
    "Nulo em todas as linhas. A camada state_electoral_division do ABS "
    "para a Tasmânia não é a de nenhuma das duas câmaras, e sim a "
    "interseção das duas, com 22 registros de nomes como Bass (Launceston) "
    "e Clark (Elwick): o ABS precisa de uma camada sem sobreposição e a "
    "Tasmânia tem dois sistemas que se sobrepõem. A coluna é mantida para "
    "compatibilidade com os demais estados, onde ela é preenchida; aqui o "
    "vínculo geográfico está em commonwealth_electoral_division_id.": (
        "Null in every row. The ABS state_electoral_division layer for "
        "Tasmania is neither chamber's, but rather the intersection of the "
        "two, with 22 records named like Bass (Launceston) and Clark "
        "(Elwick): the ABS needs a layer with no overlap and Tasmania has "
        "two systems that overlap. The column is kept for compatibility "
        "with the other states, where it is populated; here the geographic "
        "link is in commonwealth_electoral_division_id.",
        "Nulo en todas las filas. La capa state_electoral_division del ABS "
        "para Tasmania no es la de ninguna de las dos cámaras, sino la "
        "intersección de ambas, con 22 registros de nombres como Bass "
        "(Launceston) y Clark (Elwick): el ABS necesita una capa sin "
        "superposición y Tasmania tiene dos sistemas que se superponen. La "
        "columna se mantiene por compatibilidad con los demás estados, "
        "donde sí se rellena; aquí el vínculo geográfico está en "
        "commonwealth_electoral_division_id.",
    ),
    'Formato "SOBRENOME, Prenomes". A Casa de Assembleia usa rotação '
    "Robson, que embaralha a ordem das candidaturas em cada cédula, de "
    "modo que não existe posição na cédula a publicar.": (
        'Format "SURNAME, Given names". The House of Assembly uses '
        "Robson Rotation, which shuffles the order of the candidates on "
        "each ballot paper, so there is no ballot paper position to "
        "publish.",
        'Formato "APELLIDO, Nombres". La Casa de la Asamblea usa la '
        "rotación Robson, que baraja el orden de las candidaturas en cada "
        "boleta, de modo que no existe posición en la boleta que publicar.",
    ),
    "Reproduz o rótulo publicado pela TEC, incluindo Independent para "
    "candidaturas sem partido.": (
        "Reproduces the label published by the TEC, including Independent "
        "for candidacies with no party.",
        "Reproduce la etiqueta publicada por la TEC, incluido Independent "
        "para candidaturas sin partido.",
    ),
    "Assume os valores yes e no.": (
        "Takes the values yes and no.",
        "Toma los valores yes y no.",
    ),
    "Número de ordem; aritmética sobre ele não tem sentido. Vai de 1 até o "
    "número de cadeiras da divisão e é nulo para quem não foi eleito.": (
        "Sequence number; arithmetic on it is meaningless. It runs from 1 "
        "to the number of seats of the division and is null for those who "
        "were not elected.",
        "Número de orden; la aritmética sobre él no tiene sentido. Va de 1 "
        "hasta el número de escaños de la división y es nulo para quienes "
        "no fueron elegidos.",
    ),
    "Assume os valores elected, excluded e continuing. Continuing marca "
    "quem permanecia na contagem no encerramento sem ter atingido a quota, "
    "situação normal para a última cadeira de Hare-Clark.": (
        "Takes the values elected, excluded and continuing. Continuing "
        "marks those who were still in the count at its close without "
        "having reached the quota, a normal situation for the last Hare- "
        "Clark seat.",
        "Toma los valores elected, excluded y continuing. Continuing marca "
        "a quienes permanecían en el escrutinio al cierre sin haber "
        "alcanzado la cuota, situación normal para el último escaño de "
        "Hare-Clark.",
    ),
    "Quota de Droop, igual à parte inteira dos votos válidos divididos "
    "pelo número de cadeiras mais um, somada de um. No Conselho "
    "Legislativo, com uma cadeira, equivale à maioria absoluta.": (
        "Droop quota, equal to the integer part of the formal votes "
        "divided by the number of seats plus one, plus one. In the "
        "Legislative Council, with one seat, it is equivalent to an "
        "absolute majority.",
        "Cuota de Droop, igual a la parte entera de los votos válidos "
        "divididos por el número de escaños más uno, sumada de uno. En el "
        "Consejo Legislativo, con un escaño, equivale a la mayoría "
        "absoluta.",
    ),
    "Assume os valores first_preference e final_distribution. As duas não "
    "são recortes redundantes do mesmo número: a primeira traz os votos de "
    "primeira preferência e a segunda os votos após a distribuição "
    "completa de preferências, portanto filtre sempre por count_type.": (
        "Takes the values first_preference and final_distribution. The two "
        "are not redundant slices of the same number: the first carries "
        "the first preference votes and the second the votes after the "
        "full distribution of preferences, so always filter by count_type.",
        "Toma los valores first_preference y final_distribution. Las dos "
        "no son recortes redundantes del mismo número: la primera trae los "
        "votos de primera preferencia y la segunda los votos tras la "
        "distribución completa de preferencias, por lo tanto filtre "
        "siempre por count_type.",
    ),
    "Votos divididos pela quota da disputa. Publicado apenas para a Casa "
    "de Assembleia, cujo sistema Hare-Clark elege quem atinge a quota; "
    "nulo no Conselho Legislativo.": (
        "Votes divided by the quota of the contest. Published only for the "
        "House of Assembly, whose Hare-Clark system elects those who reach "
        "the quota; null in the Legislative Council.",
        "Votos divididos por la cuota de la contienda. Publicado solo para "
        "la Casa de la Asamblea, cuyo sistema Hare-Clark elige a quienes "
        "alcanzan la cuota; nulo en el Consejo Legislativo.",
    ),
    "Assume os valores elected, excluded e continuing. Preenchido apenas "
    "nas linhas de count_type igual a final_distribution.": (
        "Takes the values elected, excluded and continuing. Populated only "
        "in the rows whose count_type equals final_distribution.",
        "Toma los valores elected, excluded y continuing. Rellenado solo "
        "en las filas cuyo count_type es igual a final_distribution.",
    ),
    "Inclui, além dos locais de votação do dia da eleição, as categorias "
    "de voto especial publicadas pela TEC na mesma tabela, como voto "
    "postal, voto antecipado e voto fora da divisão.": (
        "Includes, besides the election day voting centres, the special "
        "vote categories published by the TEC in the same table, such as "
        "postal votes, early votes and out of division votes.",
        "Incluye, además de los centros de votación del día de la "
        "elección, las categorías de voto especial publicadas por la TEC "
        "en la misma tabla, como el voto postal, el voto anticipado y el "
        "voto fuera de la división.",
    ),
    "Total do local de votação, repetido em cada linha de pessoa candidata.": (
        "Total for the voting centre, repeated on each candidate row.",
        "Total del centro de votación, repetido en cada fila de persona "
        "candidata.",
    ),
    "Número de ordem; aritmética sobre ele não tem sentido, por isso é "
    "publicado como texto. A folha de escrutínio da Casa de Assembleia "
    "agrupa contagens consecutivas de exclusão em intervalos, publicados "
    "como 4 to 6.": (
        "Sequence number; arithmetic on it is meaningless, which is why it "
        "is published as text. The House of Assembly scrutiny sheet groups "
        "consecutive exclusion counts into ranges, published as 4 to 6.",
        "Número de orden; la aritmética sobre él no tiene sentido, por eso "
        "se publica como texto. La hoja de escrutinio de la Casa de la "
        "Asamblea agrupa escrutinios consecutivos de exclusión en "
        "intervalos, publicados como 4 to 6.",
    ),
    "Negativo na linha da pessoa candidata excluída ou eleita cujos votos "
    "estão sendo distribuídos.": (
        "Negative on the row of the excluded or elected candidate whose "
        "votes are being distributed.",
        "Negativo en la fila de la persona candidata excluida o elegida "
        "cuyos votos se están distribuyendo.",
    ),
    "Total da contagem, repetido em cada linha de pessoa candidata.": (
        "Total for the count, repeated on each candidate row.",
        "Total del escrutinio, repetido en cada fila de persona candidata.",
    ),
    "Texto livre da fonte, como PETERSEN excluded ou GLADE-WRIGHT elected. "
    "Total da contagem, repetido em cada linha de pessoa candidata.": (
        "Free text from the source, such as PETERSEN excluded or GLADE- "
        "WRIGHT elected. Total for the count, repeated on each candidate "
        "row.",
        "Texto libre de la fuente, como PETERSEN excluded o GLADE-WRIGHT "
        "elected. Total del escrutinio, repetido en cada fila de persona "
        "candidata.",
    ),
    "Código postal, não uma quantidade; publicado como texto.": (
        "Postcode, not a quantity; published as text.",
        "Código postal, no una cantidad; publicado como texto.",
    ),
    "Constante em TAS. O vínculo com o diretório de estados é real, mas o "
    "backend só aceita chave estrangeira para a coluna marcada como chave "
    "primária do diretório, e a chave de diretorios_au.state é id_state, "
    "não abbreviation.": (
        "Constant at TAS. The link to the state directory is real, but the "
        "backend only accepts a foreign key to the column marked as the "
        "directory's primary key, and the key of diretorios_au.state is "
        "id_state, not abbreviation.",
        "Constante en TAS. El vínculo con el directorio de estados es "
        "real, pero el backend solo acepta clave foránea a la columna "
        "marcada como clave primaria del directorio, y la clave de "
        "diretorios_au.state es id_state, no abbreviation.",
    ),
    "Assume os valores publicados pela TEC, entre eles Full, Assistance e "
    "None.": (
        "Takes the values published by the TEC, among them Full, "
        "Assistance and None.",
        "Toma los valores publicados por la TEC, entre ellos Full, "
        "Assistance y None.",
    ),
    "O grão da tabela é uma linha por evento eleitoral, local de votação e "
    "divisão atendida: um local que atende duas divisões aparece duas "
    "vezes.": (
        "The grain of the table is one row per electoral event, voting "
        "centre and division served: a centre serving two divisions "
        "appears twice.",
        "El grano de la tabla es una fila por evento electoral, centro de "
        "votación y división atendida: un centro que atiende dos "
        "divisiones aparece dos veces.",
    ),
}


validate()
