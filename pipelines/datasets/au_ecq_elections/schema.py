"""Column specification for every au_ecq_elections table.

Single source of truth: the architecture CSVs, the cleaning transform and the dbt
models are all generated from (or validated against) the ``TABLES`` mapping below.

Typing follows the house rule — INT64/FLOAT64 only where arithmetic is meaningful and
a measurement unit exists; every code, flag, ballot position, sequence number and
identifier is STRING.
"""

from __future__ import annotations

from dataclasses import dataclass, field

DIR_YEAR = "br_bd_diretorios_data_tempo.ano:ano"
DIR_LGA = "br_bd_diretorios_au.lga_2021:id_lga"
DIR_SED = "br_bd_diretorios_au.state_electoral_division_2021:id_state_electoral_division"
DIR_STATE = "br_bd_diretorios_au.state:abbreviation"


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


# --------------------------------------------------------------------------------------
# Numeric helpers
# --------------------------------------------------------------------------------------


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


def aud(
    name: str, pt: str, en: str, es: str, original: str = "", obs: str = ""
) -> Column:
    return C(
        name,
        "FLOAT64",
        pt,
        en,
        es,
        measurement_unit="AUD",
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
    observations=(
        "Coluna de particionamento. Vários eventos eleitorais podem compartilhar "
        "o mesmo ano."
    ),
)
ELECTION_ID = C(
    "election_id",
    "STRING",
    "Identificador do evento eleitoral atribuído pela ECQ",
    "ECQ identifier of the electoral event",
    "Identificador del evento electoral asignado por la ECQ",
    original_name="electionId",
    observations=(
        "Chave estrangeira para a tabela election, presente em todas as tabelas "
        "de resultados."
    ),
)
CONTEST_ID = C(
    "contest_id",
    "STRING",
    "Identificador da disputa por uma cadeira dentro do evento eleitoral",
    "Identifier of the contest for a single seat within the electoral event",
    "Identificador de la disputa por un escaño dentro del evento electoral",
    original_name="electorateId",
    observations=(
        "Campo electorateId da ECQ. É único dentro de cada evento eleitoral e "
        "identifica uma única disputa por cadeira."
    ),
)
LGA_ID = C(
    "lga_id",
    "STRING",
    "Código da área de governo local no padrão ABS 2021",
    "Australian Bureau of Statistics 2021 code of the local government area",
    "Código del área de gobierno local en el estándar ABS 2021",
    directory_column=DIR_LGA,
    observations=(
        "Código LGA 2021 do ABS, derivado por cruzamento validado de nomes: a ECQ "
        "mantém o sufixo legal (Shire, Regional, City) que o nome do ABS omite. "
        "Nulo nas disputas estaduais."
    ),
)
LGA_CODE = C(
    "lga_code",
    "STRING",
    "Código da área de governo local atribuído pela própria ECQ",
    "Local government area code assigned by the ECQ itself",
    "Código del área de gobierno local asignado por la propia ECQ",
    original_name="areaCode",
    observations=(
        "Campo areaCode da ECQ. Não é o código do ABS, que está em lga_id."
    ),
)
SED_ID = C(
    "state_electoral_division_id",
    "STRING",
    "Código do distrito eleitoral estadual no padrão ASGS 2021 do ABS",
    "Australian Statistical Geography Standard 2021 code of the state electoral division",
    "Código del distrito electoral estatal en el estándar ASGS 2021 del ABS",
    directory_column=DIR_SED,
    observations=(
        "Os 93 distritos estaduais de Queensland presentes na cobertura de 2020 a "
        "2026 correspondem exatamente à vintage 2021 do ASGS. Nulo nas disputas "
        "de governo local."
    ),
)
GOVERNMENT_LEVEL = C(
    "government_level",
    "STRING",
    "Esfera de governo da disputa: estadual ou local",
    "Level of government of the contest: state or local",
    "Nivel de gobierno de la disputa: estatal o local",
    covered_by_dictionary="yes",
    observations=(
        "Assume os valores state e local. É definido por disputa, nunca uma "
        "constante do conjunto: disputas estaduais e locais convivem nas mesmas "
        "tabelas."
    ),
)
CONTEST_TYPE = C(
    "contest_type",
    "STRING",
    "Tipo de cadeira disputada",
    "Type of seat contested",
    "Tipo de escaño disputado",
    covered_by_dictionary="yes",
    observations="Assume os valores state_district, councillor e mayor.",
)
VOTING_SYSTEM = C(
    "voting_system",
    "STRING",
    "Sistema de votação aplicado à disputa",
    "Voting system applied to the contest",
    "Sistema de votación aplicado a la disputa",
    covered_by_dictionary="yes",
    observations=(
        "Assume os valores compulsory_preferential, optional_preferential e "
        "first_past_the_post. É definido por disputa: os três coexistem dentro do "
        "evento de 2024."
    ),
)
LGA_NAME = C(
    "lga_name",
    "STRING",
    "Nome da área de governo local tal como publicado pela ECQ",
    "Name of the local government area as published by the ECQ",
    "Nombre del área de gobierno local tal como lo publica la ECQ",
    observations="Nulo nas disputas estaduais.",
)
DISTRICT_NAME = C(
    "district_name",
    "STRING",
    "Nome da disputa: distrito estadual, divisão de conselho ou cargo de prefeitura",
    "Name of the contest: state district, council division or mayoral office",
    "Nombre de la disputa: distrito estatal, división de concejo o cargo de alcaldía",
)

# Spliced verbatim into the five contest-level result tables.
CONTEST_BLOCK = [
    YEAR,
    ELECTION_ID,
    CONTEST_ID,
    LGA_ID,
    LGA_CODE,
    SED_ID,
    GOVERNMENT_LEVEL,
    CONTEST_TYPE,
    VOTING_SYSTEM,
    LGA_NAME,
    DISTRICT_NAME,
]

COUNT_STATUS = C(
    "count_status",
    "STRING",
    "Estágio da apuração ao qual a linha se refere",
    "Stage of the count the row refers to",
    "Etapa del escrutinio a la que se refiere la fila",
    covered_by_dictionary="yes",
    observations=(
        "Assume os valores preliminary_unofficial, indicative_unofficial, "
        "first_preference_official, distribution_of_preferences_official e "
        "declared_unopposed. As apurações preliminar e oficial de primeira "
        "preferência divergem nos 93 distritos da eleição geral de 2024: não são "
        "recortes redundantes do mesmo número, portanto filtre sempre por "
        "count_status."
    ),
)
BALLOT_ORDER_NUMBER = C(
    "ballot_order_number",
    "STRING",
    "Posição da pessoa candidata na cédula",
    "Position of the candidate on the ballot paper",
    "Posición de la persona candidata en la boleta",
    original_name="ballotOrderNumber",
    observations=(
        "Número de ordem na cédula; aritmética sobre ele não tem sentido, por isso "
        "é publicado como texto."
    ),
)
BALLOT_NAME = C(
    "ballot_name",
    "STRING",
    "Nome da pessoa candidata tal como impresso na cédula",
    "Name of the candidate as printed on the ballot paper",
    "Nombre de la persona candidata tal como aparece impreso en la boleta",
    original_name="ballotName",
    observations='Formato "SOBRENOME, Prenomes".',
)
PARTY_CODE = C(
    "party_code",
    "STRING",
    "Sigla do partido da pessoa candidata",
    "Abbreviation of the candidate's party",
    "Sigla del partido de la persona candidata",
    original_name="partyCode",
)
PARTY_NAME = C(
    "party_name",
    "STRING",
    "Nome do partido da pessoa candidata",
    "Name of the candidate's party",
    "Nombre del partido de la persona candidata",
    original_name="partyName",
)
VOTING_CENTRE_ID = C(
    "voting_centre_id",
    "STRING",
    "Identificador do local de votação atribuído pela ECQ",
    "ECQ identifier of the voting centre",
    "Identificador del local de votación asignado por la ECQ",
    original_name="venueId",
)
VOTING_CENTRE_NAME = C(
    "voting_centre_name",
    "STRING",
    "Nome do local de votação",
    "Name of the voting centre",
    "Nombre del local de votación",
    original_name="venueName",
)
ELECTION_NAME = C(
    "election_name",
    "STRING",
    "Nome do evento eleitoral",
    "Name of the electoral event",
    "Nombre del evento electoral",
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
        "Identificador do evento eleitoral atribuído pela ECQ",
        "ECQ identifier of the electoral event",
        "Identificador del evento electoral asignado por la ECQ",
        original_name="electionId",
        observations=(
            "Chave da tabela. A eleição suplementar de conselheiro do Mapoon "
            "Aboriginal Shire Council de 2023 (stub MASC23) está excluída do "
            "conjunto porque a ECQ não publica arquivo de resultados para ela."
        ),
    ),
    C(
        "election_stub",
        "STRING",
        "Sigla curta do evento eleitoral usada nas URLs da ECQ",
        "Short stub of the electoral event used in the ECQ URLs",
        "Sigla corta del evento electoral usada en las URL de la ECQ",
        original_name="stub",
    ),
    ELECTION_NAME,
    C(
        "election_type",
        "STRING",
        "Tipo do evento eleitoral",
        "Type of the electoral event",
        "Tipo del evento electoral",
        covered_by_dictionary="yes",
        observations=(
            "Assume os valores Local Quadrennial, State General, State By-election, "
            "Local Councillor By-election e Local Mayoral By-election."
        ),
    ),
    C(
        "government_level",
        "STRING",
        "Esfera de governo do evento eleitoral: estadual ou local",
        "Level of government of the electoral event: state or local",
        "Nivel de gobierno del evento electoral: estatal o local",
        covered_by_dictionary="yes",
        observations="Assume os valores state e local.",
    ),
    C(
        "election_date",
        "DATE",
        "Data da votação",
        "Date of the poll",
        "Fecha de la votación",
    ),
    C(
        "results_archive_url",
        "STRING",
        "Endereço do arquivo de resultados publicado pela ECQ para o evento",
        "URL of the results archive published by the ECQ for the event",
        "Dirección del archivo de resultados publicado por la ECQ para el evento",
        original_name="archiveXML",
    ),
]

TABLES["candidate"] = [
    *CONTEST_BLOCK,
    BALLOT_ORDER_NUMBER,
    BALLOT_NAME,
    C(
        "candidate_surname",
        "STRING",
        "Sobrenome da pessoa candidata",
        "Surname of the candidate",
        "Apellido de la persona candidata",
        original_name="surname",
    ),
    C(
        "candidate_given_names",
        "STRING",
        "Prenomes da pessoa candidata",
        "Given names of the candidate",
        "Nombres de pila de la persona candidata",
        original_name="givenNames",
    ),
    PARTY_CODE,
    PARTY_NAME,
    C(
        "is_declared_elected",
        "STRING",
        "Indica se a pessoa candidata foi declarada eleita na disputa",
        "Whether the candidate was declared elected in the contest",
        "Indica si la persona candidata fue declarada electa en la disputa",
        covered_by_dictionary="yes",
        observations="Assume os valores yes e no.",
    ),
]

TABLES["enrolment_turnout"] = [
    *CONTEST_BLOCK,
    COUNT_STATUS,
    C(
        "count_round_number",
        "STRING",
        "Número da rodada de apuração dentro do estágio",
        "Number of the count round within the stage",
        "Número de la ronda de escrutinio dentro de la etapa",
        original_name="countNumber",
        observations="Número de ordem; aritmética sobre ele não tem sentido.",
    ),
    C(
        "number_to_elect",
        "INT64",
        "Número de cadeiras a preencher na disputa",
        "Number of seats to be filled in the contest",
        "Número de escaños a ocupar en la disputa",
        measurement_unit="seat",
        original_name="numberToElect",
    ),
    C(
        "candidates_count",
        "INT64",
        "Número de pessoas candidatas na disputa",
        "Number of candidates in the contest",
        "Número de personas candidatas en la disputa",
        measurement_unit="candidate",
        original_name="candidateCount",
    ),
    C(
        "enrolment",
        "INT64",
        "Número de pessoas inscritas na disputa",
        "Number of electors enrolled in the contest",
        "Número de personas inscritas en la disputa",
        measurement_unit="person",
        original_name="enrolment",
    ),
    votes(
        "votes_total",
        "Total de votos apurados",
        "Total votes counted",
        "Total de votos escrutados",
        "totalVotes",
    ),
    votes(
        "votes_formal",
        "Total de votos válidos",
        "Total formal votes",
        "Total de votos válidos",
        "formalVotes",
    ),
    votes(
        "votes_informal",
        "Total de votos inválidos",
        "Total informal votes",
        "Total de votos inválidos",
        "informalVotes",
    ),
    pct(
        "percentage_formal",
        "Votos válidos como percentual do total apurado",
        "Formal votes as a share of the total count",
        "Votos válidos como porcentaje del total escrutado",
        "formalPercentage",
    ),
    pct(
        "percentage_informal",
        "Votos inválidos como percentual do total apurado",
        "Informal votes as a share of the total count",
        "Votos inválidos como porcentaje del total escrutado",
        "informalPercentage",
    ),
    pct(
        "percentage_roll_counted",
        "Votos apurados como percentual das pessoas inscritas",
        "Votes counted as a share of enrolled electors",
        "Votos escrutados como porcentaje de las personas inscritas",
        "percentageRollCounted",
    ),
    C(
        "voting_method",
        "STRING",
        "Método de votação ao qual os totais se referem",
        "Voting method the totals refer to",
        "Método de votación al que se refieren los totales",
        covered_by_dictionary="yes",
        original_name="votingMethod",
    ),
    C(
        "is_final",
        "STRING",
        "Indica se a rodada de apuração é a final do estágio",
        "Whether the count round is the final one of the stage",
        "Indica si la ronda de escrutinio es la final de la etapa",
        covered_by_dictionary="yes",
        observations="Assume os valores yes e no.",
    ),
    C(
        "last_updated",
        "DATETIME",
        "Momento da última atualização da rodada de apuração pela ECQ",
        "Time the count round was last updated by the ECQ",
        "Momento de la última actualización de la ronda de escrutinio por la ECQ",
        original_name="lastUpdated",
        observations=(
            "As 64 disputas decididas sem apuração, com count_status igual a "
            "declared_unopposed, trazem uma única linha com as colunas de votos "
            "nulas."
        ),
    ),
]

TABLES["result_district"] = [
    *CONTEST_BLOCK,
    COUNT_STATUS,
    C(
        "count_type",
        "STRING",
        "Tipo de apuração ao qual os votos se referem",
        "Type of count the votes refer to",
        "Tipo de escrutinio al que se refieren los votos",
        covered_by_dictionary="yes",
        observations=(
            "Assume os valores first_preference e two_candidate_preferred. A ECQ "
            "não publica apuração de preferência entre dois partidos, ao contrário "
            "da AEC."
        ),
    ),
    BALLOT_ORDER_NUMBER,
    BALLOT_NAME,
    PARTY_CODE,
    PARTY_NAME,
    votes(
        "votes",
        "Votos recebidos pela pessoa candidata na disputa",
        "Votes received by the candidate in the contest",
        "Votos recibidos por la persona candidata en la disputa",
        "votes",
    ),
    pct(
        "percentage",
        "Votos da pessoa candidata como percentual dos votos válidos da disputa",
        "Candidate votes as a share of the formal votes in the contest",
        "Votos de la persona candidata como porcentaje de los votos válidos de la disputa",
        "percentage",
    ),
]

TABLES["result_voting_centre"] = [
    *CONTEST_BLOCK,
    VOTING_CENTRE_ID,
    COUNT_STATUS,
    C(
        "count_type",
        "STRING",
        "Tipo de apuração ao qual os votos se referem",
        "Type of count the votes refer to",
        "Tipo de escrutinio al que se refieren los votos",
        covered_by_dictionary="yes",
        observations=(
            "Assume os valores first_preference e two_candidate_preferred. A ECQ "
            "não publica apuração de preferência entre dois partidos, ao contrário "
            "da AEC."
        ),
    ),
    BALLOT_ORDER_NUMBER,
    VOTING_CENTRE_NAME,
    C(
        "voting_centre_type_code",
        "STRING",
        "Código do tipo de local de votação",
        "Code for the type of voting centre",
        "Código del tipo de local de votación",
        covered_by_dictionary="yes",
        original_name="typeCode",
        observations=(
            "Assume os valores PB, EV, DV1, DV2, AB1 e AB2. O campo typeDescription "
            "da própria ECQ é inconfiável, pois em parte dos locais repete o nome do "
            "local em vez do rótulo do tipo; por isso apenas o código é publicado "
            "aqui e os rótulos ficam na tabela dicionario."
        ),
    ),
    C(
        "voting_centre_district_name",
        "STRING",
        "Nome do distrito de apuração ao qual o local de votação pertence",
        "Name of the reporting district the voting centre falls in",
        "Nombre del distrito de escrutinio al que pertenece el local de votación",
        observations=(
            "Igual a district_name nas disputas estaduais e nas divisões de "
            "conselho; nas disputas de conselho indiviso e de prefeitura é uma "
            "sub-área da disputa."
        ),
    ),
    BALLOT_NAME,
    PARTY_CODE,
    PARTY_NAME,
    votes(
        "votes",
        "Votos recebidos pela pessoa candidata no local de votação",
        "Votes received by the candidate at the voting centre",
        "Votos recibidos por la persona candidata en el local de votación",
        "votes",
    ),
    pct(
        "percentage",
        "Votos da pessoa candidata como percentual dos votos válidos do local de votação",
        "Candidate votes as a share of the formal votes at the voting centre",
        "Votos de la persona candidata como porcentaje de los votos válidos del local de votación",
        "percentage",
    ),
    votes(
        "votes_total",
        "Total de votos apurados no local de votação",
        "Total votes counted at the voting centre",
        "Total de votos escrutados en el local de votación",
        "totalVotes",
        obs="Total do local de votação, repetido em cada linha de pessoa candidata.",
    ),
    votes(
        "votes_formal",
        "Total de votos válidos no local de votação",
        "Total formal votes at the voting centre",
        "Total de votos válidos en el local de votación",
        "formalVotes",
        obs="Total do local de votação, repetido em cada linha de pessoa candidata.",
    ),
    votes(
        "votes_informal",
        "Total de votos inválidos no local de votação",
        "Total informal votes at the voting centre",
        "Total de votos inválidos en el local de votación",
        "informalVotes",
        obs="Total do local de votação, repetido em cada linha de pessoa candidata.",
    ),
]

TABLES["distribution_of_preferences"] = [
    *CONTEST_BLOCK,
    COUNT_STATUS,
    C(
        "distribution_number",
        "STRING",
        "Número da etapa de distribuição de preferências",
        "Number of the preference distribution step",
        "Número de la etapa de distribución de preferencias",
        original_name="distributionNumber",
        observations=(
            "Número de ordem; aritmética sobre ele não tem sentido. A ECQ também "
            "publica esta distribuição por local de votação (146.223 linhas); esse "
            "grão mais fino não está incluído nesta tabela."
        ),
    ),
    C(
        "excluded_ballot_order_number",
        "STRING",
        "Posição na cédula da pessoa candidata excluída na etapa",
        "Ballot position of the candidate excluded at this step",
        "Posición en la boleta de la persona candidata excluida en la etapa",
        original_name="excludedBallotOrderNumber",
        observations="Número de ordem na cédula; aritmética sobre ele não tem sentido.",
    ),
    C(
        "ballot_order_number",
        "STRING",
        "Posição na cédula da pessoa candidata que recebeu os votos transferidos",
        "Ballot position of the candidate receiving the transferred votes",
        "Posición en la boleta de la persona candidata que recibió los votos transferidos",
        original_name="ballotOrderNumber",
        observations="Número de ordem na cédula; aritmética sobre ele não tem sentido.",
    ),
    C(
        "excluded_ballot_name",
        "STRING",
        "Nome na cédula da pessoa candidata excluída na etapa",
        "Ballot name of the candidate excluded at this step",
        "Nombre en la boleta de la persona candidata excluida en la etapa",
        original_name="excludedBallotName",
    ),
    C(
        "ballot_name",
        "STRING",
        "Nome na cédula da pessoa candidata que recebeu os votos transferidos",
        "Ballot name of the candidate receiving the transferred votes",
        "Nombre en la boleta de la persona candidata que recibió los votos transferidos",
        original_name="ballotName",
    ),
    PARTY_CODE,
    PARTY_NAME,
    votes(
        "votes_transferred",
        "Votos transferidos da pessoa candidata excluída para a pessoa candidata receptora",
        "Votes transferred from the excluded candidate to the receiving candidate",
        "Votos transferidos de la persona candidata excluida a la persona candidata receptora",
        "votesTransferred",
    ),
    pct(
        "percentage_transferred",
        "Votos transferidos como percentual dos votos distribuídos na etapa",
        "Transferred votes as a share of the votes distributed at this step",
        "Votos transferidos como porcentaje de los votos distribuidos en la etapa",
        "percentageTransferred",
    ),
    votes(
        "votes_distributed",
        "Total de votos distribuídos na etapa",
        "Total votes distributed at this step",
        "Total de votos distribuidos en la etapa",
        "votesDistributed",
        obs="Total da etapa de distribuição, repetido em cada linha de pessoa candidata receptora.",
    ),
    votes(
        "votes_exhausted",
        "Votos esgotados na etapa, sem preferência seguinte válida",
        "Votes exhausted at this step, with no valid next preference",
        "Votos agotados en la etapa, sin preferencia siguiente válida",
        "votesExhausted",
        obs="Total da etapa de distribuição, repetido em cada linha de pessoa candidata receptora.",
    ),
    pct(
        "percentage_exhausted",
        "Votos esgotados como percentual dos votos distribuídos na etapa",
        "Exhausted votes as a share of the votes distributed at this step",
        "Votos agotados como porcentaje de los votos distribuidos en la etapa",
        "percentageExhausted",
        obs="Total da etapa de distribuição, repetido em cada linha de pessoa candidata receptora.",
    ),
    votes(
        "votes_remaining_in_count",
        "Votos que permanecem na apuração após a etapa",
        "Votes remaining in the count after this step",
        "Votos que permanecen en el escrutinio tras la etapa",
        "votesRemainingInCount",
        obs="Total da etapa de distribuição, repetido em cada linha de pessoa candidata receptora.",
    ),
]

TABLES["voting_centre"] = [
    YEAR,
    ELECTION_ID,
    VOTING_CENTRE_ID,
    SED_ID,
    C(
        "district_name",
        "STRING",
        "Nome do distrito atendido pelo local de votação",
        "Name of the district served by the voting centre",
        "Nombre del distrito atendido por el local de votación",
        observations=(
            "O grão da tabela é uma linha por evento eleitoral, local de votação e "
            "distrito atendido: um local que atende dois distritos aparece duas vezes."
        ),
    ),
    VOTING_CENTRE_NAME,
    C(
        "building_name",
        "STRING",
        "Nome do estabelecimento que sedia o local de votação",
        "Name of the building hosting the voting centre",
        "Nombre del establecimiento que alberga el local de votación",
        original_name="buildingName",
    ),
    C(
        "street_number",
        "STRING",
        "Número do endereço do local de votação",
        "Street number of the voting centre address",
        "Número de la dirección del local de votación",
        original_name="streetNumber",
        observations="Publicado como texto: pode conter letras e intervalos.",
    ),
    C(
        "street_name",
        "STRING",
        "Logradouro do local de votação",
        "Street name of the voting centre address",
        "Calle del local de votación",
        original_name="streetName",
    ),
    C(
        "locality",
        "STRING",
        "Localidade ou bairro do local de votação",
        "Locality or suburb of the voting centre",
        "Localidad o barrio del local de votación",
        original_name="locality",
    ),
    C(
        "postcode",
        "STRING",
        "Código postal do local de votação",
        "Postcode of the voting centre",
        "Código postal del local de votación",
        original_name="postcode",
        observations="Código postal, não uma quantidade; publicado como texto.",
    ),
    C(
        "state_abbreviation",
        "STRING",
        "Sigla do estado ou território do local de votação",
        "State or territory abbreviation of the voting centre",
        "Sigla del estado o territorio del local de votación",
        directory_column=DIR_STATE,
        observations=(
            "O vínculo com o diretório é real e é testado no dbt, mas o backend não "
            "consegue registrá-lo: ele só aceita chave estrangeira para a coluna marcada "
            "como chave primária do diretório, e a chave de br_bd_diretorios_au.state é "
            "id_state, não abbreviation."
        ),
        original_name="state",
    ),
    C(
        "latitude",
        "FLOAT64",
        "Latitude do local de votação",
        "Latitude of the voting centre",
        "Latitud del local de votación",
        measurement_unit="degree",
        original_name="latitude",
    ),
    C(
        "longitude",
        "FLOAT64",
        "Longitude do local de votação",
        "Longitude of the voting centre",
        "Longitud del local de votación",
        measurement_unit="degree",
        original_name="longitude",
    ),
    C(
        "joint_type",
        "STRING",
        "Papel do local de votação quando ele é compartilhado entre distritos",
        "Role of the voting centre when it is shared between districts",
        "Papel del local de votación cuando se comparte entre distritos",
        covered_by_dictionary="yes",
        original_name="jointType",
        observations=(
            "Assume os valores Host e Guest, que marcam locais compartilhados entre "
            "distritos. Nulo quando o local não é compartilhado."
        ),
    ),
    C(
        "is_abolished",
        "STRING",
        "Indica se o local de votação foi extinto",
        "Whether the voting centre has been abolished",
        "Indica si el local de votación fue suprimido",
        covered_by_dictionary="yes",
        original_name="abolished",
        observations="Assume os valores yes e no.",
    ),
]

TABLES["disclosure_gift"] = [
    C(
        "year",
        "INT64",
        "Ano da doação declarada",
        "Year of the disclosed gift",
        "Año de la donación declarada",
        measurement_unit="year",
        directory_column=DIR_YEAR,
        observations=(
            "Coluna de particionamento, derivada de date_gift_made. 23 linhas não "
            "trazem data da doação nem evento eleitoral, de modo que a coluna de "
            "particionamento não está integralmente preenchida."
        ),
    ),
    C(
        "government_level",
        "STRING",
        "Esfera de governo à qual a doação se refere: estadual ou local",
        "Level of government the gift relates to: state or local",
        "Nivel de gobierno al que se refiere la donación: estatal o local",
        covered_by_dictionary="yes",
        observations=(
            "Nenhuma coluna da fonte codifica esta informação: ela é derivada de "
            "qual das duas exportações do mapa (State ou Local) originou a linha, "
            "e essa é a única forma de recuperá-la."
        ),
    ),
    C(
        "date_gift_made",
        "DATE",
        "Data em que a doação foi feita",
        "Date the gift was made",
        "Fecha en que se realizó la donación",
        observations=(
            "Ausente em 23 linhas, que também não trazem evento eleitoral associado."
        ),
    ),
    C(
        "donor_name",
        "STRING",
        "Nome de quem fez a doação",
        "Name of the donor",
        "Nombre de quien hizo la donación",
        observations=(
            "Linhas inteiramente duplicadas são frequentes (502 na esfera estadual e "
            "54 na local) e não são necessariamente erros: duas doações idênticas do "
            "mesmo doador na mesma data são genuinamente indistinguíveis, por isso a "
            "tabela não publica chave primária."
        ),
    ),
    C(
        "recipient_name",
        "STRING",
        "Nome de quem recebeu a doação",
        "Name of the recipient",
        "Nombre de quien recibió la donación",
        observations=(
            "Linhas inteiramente duplicadas são frequentes e não são necessariamente "
            "erros; a tabela não publica chave primária."
        ),
    ),
    aud(
        "gift_value",
        "Valor da doação em dólares australianos",
        "Value of the gift in Australian dollars",
        "Valor de la donación en dólares australianos",
    ),
    C(
        "election_name",
        "STRING",
        "Nome do evento eleitoral ao qual a doação se refere",
        "Name of the electoral event the gift relates to",
        "Nombre del evento electoral al que se refiere la donación",
        observations=(
            "Majoritariamente vazio (preenchido em 2,7% das doações estaduais) e em "
            "texto livre. Não corresponde de forma confiável à tabela election, pois "
            "também nomeia eventos de 2012 e 2016, anteriores à cobertura de "
            "resultados."
        ),
    ),
    C(
        "is_political_donation",
        "STRING",
        "Indica se a doação foi declarada como doação política",
        "Whether the gift was reported as a political donation",
        "Indica si la donación fue declarada como donación política",
        covered_by_dictionary="yes",
        observations=(
            "Assume os valores Yes, No, Unknown e -. O traço é um terceiro estado "
            "real, que significa não aplicável e não um vazio: o campo existe apenas "
            "na esfera estadual, de modo que toda linha local traz -."
        ),
    ),
    C(
        "has_electoral_committee",
        "STRING",
        "Indica se quem recebeu a doação possui comitê eleitoral registrado",
        "Whether the recipient has a registered electoral committee",
        "Indica si quien recibió la donación tiene comité electoral registrado",
        covered_by_dictionary="yes",
        observations=(
            "Campo exclusivo da esfera estadual; vazio em todas as linhas locais."
        ),
    ),
    C(
        "electoral_committee_name",
        "STRING",
        "Nome do comitê eleitoral de quem recebeu a doação",
        "Name of the recipient's electoral committee",
        "Nombre del comité electoral de quien recibió la donación",
        observations=(
            "Campo exclusivo da esfera estadual; vazio em todas as linhas locais."
        ),
    ),
]

TABLES["disclosure_expenditure"] = [
    C(
        "year",
        "INT64",
        "Ano em que a despesa foi incorrida",
        "Year the expenditure was incurred",
        "Año en que se incurrió en el gasto",
        measurement_unit="year",
        directory_column=DIR_YEAR,
        observations=(
            "Coluna de particionamento, derivada de date_incurred. A cobertura é "
            "concentrada nos ciclos eleitorais e não é uniforme: na prática começa "
            "em 2019, com apenas 1.997 linhas somando 2016, 2017 e 2018."
        ),
    ),
    C(
        "date_incurred",
        "DATE",
        "Data em que a despesa foi incorrida",
        "Date the expenditure was incurred",
        "Fecha en que se incurrió en el gasto",
        observations=(
            "Duas linhas datadas de 1924 são erros de digitação para 2024, ambas "
            "ligadas às eleições municipais de 2024, e foram corrigidas para 2024 "
            "para que o intervalo de particionamento não recue um século."
        ),
    ),
    C(
        "incurred_by_name",
        "STRING",
        "Nome de quem incorreu na despesa",
        "Name of the entity that incurred the expenditure",
        "Nombre de quien incurrió en el gasto",
    ),
    aud(
        "expenditure_value",
        "Valor da despesa em dólares australianos",
        "Value of the expenditure in Australian dollars",
        "Valor del gasto en dólares australianos",
    ),
    C(
        "candidate_type",
        "STRING",
        "Categoria da candidatura que incorreu na despesa",
        "Category of the candidacy that incurred the expenditure",
        "Categoría de la candidatura que incurrió en el gasto",
        covered_by_dictionary="yes",
        observations=(
            "Assume os valores Councillor, Mayor e Announced Candidate, além de vazio."
        ),
    ),
    C(
        "local_electorate_name",
        "STRING",
        "Nome da área eleitoral local à qual a despesa se refere",
        "Name of the local electorate the expenditure relates to",
        "Nombre del área electoral local a la que se refiere el gasto",
    ),
    C(
        "election_name",
        "STRING",
        "Nome do evento eleitoral ao qual a despesa se refere",
        "Name of the electoral event the expenditure relates to",
        "Nombre del evento electoral al que se refiere el gasto",
        observations=(
            "Texto livre; não corresponde de forma confiável à tabela election."
        ),
    ),
    C(
        "goods_or_services_description",
        "STRING",
        "Descrição dos bens ou serviços adquiridos",
        "Description of the goods or services purchased",
        "Descripción de los bienes o servicios adquiridos",
    ),
    C(
        "expenditure_purpose",
        "STRING",
        "Finalidade declarada da despesa",
        "Declared purpose of the expenditure",
        "Finalidad declarada del gasto",
    ),
]

TABLES["disclosure_return"] = [
    C(
        "year",
        "INT64",
        "Ano de criação da declaração periódica",
        "Year the periodic return was created",
        "Año de creación de la declaración periódica",
        measurement_unit="year",
        directory_column=DIR_YEAR,
        observations="Coluna de particionamento, derivada de date_created.",
    ),
    C(
        "date_created",
        "DATE",
        "Data de criação da declaração periódica",
        "Date the periodic return was created",
        "Fecha de creación de la declaración periódica",
        observations=(
            "As exportações do Electronic Disclosure System não trazem coluna de "
            "situação da declaração: a interface do sistema filtra por ela, mas não "
            "a exporta, de modo que a situação de entrega é uma lacuna conhecida."
        ),
    ),
    C(
        "submitter_name",
        "STRING",
        "Nome de quem apresentou a declaração",
        "Name of the party that submitted the return",
        "Nombre de quien presentó la declaración",
    ),
    C(
        "return_for_name",
        "STRING",
        "Nome da entidade à qual a declaração se refere",
        "Name of the entity the return is lodged for",
        "Nombre de la entidad a la que se refiere la declaración",
    ),
    C(
        "period_start_date",
        "DATE",
        "Data inicial do período de referência da declaração",
        "Start date of the reporting period of the return",
        "Fecha inicial del período de referencia de la declaración",
    ),
    C(
        "period_end_date",
        "DATE",
        "Data final do período de referência da declaração",
        "End date of the reporting period of the return",
        "Fecha final del período de referencia de la declaración",
    ),
    C(
        "period_label",
        "STRING",
        "Rótulo do período de referência publicado pela ECQ",
        "Label of the reporting period published by the ECQ",
        "Etiqueta del período de referencia publicada por la ECQ",
        observations=(
            "São 20 períodos semestrais de calendário, do segundo semestre de 2016 "
            "ao primeiro semestre de 2026. A declaração é única por return_for_name, "
            "period_start_date e period_end_date."
        ),
    ),
    aud(
        "amount_received",
        "Total recebido no período de referência, em dólares australianos",
        "Total received in the reporting period, in Australian dollars",
        "Total recibido en el período de referencia, en dólares australianos",
    ),
    aud(
        "amount_paid",
        "Total pago no período de referência, em dólares australianos",
        "Total paid in the reporting period, in Australian dollars",
        "Total pagado en el período de referencia, en dólares australianos",
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


# --------------------------------------------------------------------------------------
# Partitioning
# --------------------------------------------------------------------------------------

# Every table except the dictionary is partitioned by year.
PARTITION_COLUMNS: dict[str, list[str]] = {
    name: ([] if name == "dicionario" else ["year"]) for name in TABLES
}

# Starts at 2013, not at the 2016 start of the disclosure regime: 23 gift rows are
# dated 2013-2015. Outside a start=2016 range they land in BigQuery's
# __UNPARTITIONED__ bucket, which is not a load error and is therefore easy to miss.
PARTITION_RANGE = {"start": 2013, "end": 2035, "interval": 1}


def column_names(table: str) -> list[str]:
    return [c.name for c in TABLES[table]]


def column_types(table: str) -> dict[str, str]:
    return {c.name: c.bigquery_type for c in TABLES[table]}


# --------------------------------------------------------------------------------------
# Observation translations
# --------------------------------------------------------------------------------------

# Column ``observations`` are authored in Portuguese; the backend stores one field per
# language and a bare ``observations`` key is written to Portuguese only, which is how
# 3,022 production columns ended up PT-only. Every distinct note is translated here and
# ``validate()`` fails on any note that is not.
OBSERVATION_TRANSLATIONS: dict[str, tuple[str, str]] = {
    "Coluna de particionamento. Vários eventos eleitorais podem compartilhar o mesmo ano.": (
        "Partition column. Several electoral events can share the same year.",
        "Columna de particionamiento. Varios eventos electorales pueden compartir el mismo año.",
    ),
    "Chave estrangeira para a tabela election, presente em todas as tabelas de resultados.": (
        "Foreign key to the election table, present in every results table.",
        "Clave foránea hacia la tabla election, presente en todas las tablas de resultados.",
    ),
    "Os 93 distritos estaduais de Queensland presentes na cobertura de 2020 a 2026 correspondem exatamente à vintage 2021 do ASGS. Nulo nas disputas de governo local.": (
        "The 93 Queensland state districts in the 2020 to 2026 coverage match the ASGS 2021 vintage exactly. Null in local government contests.",
        "Los 93 distritos estatales de Queensland presentes en la cobertura de 2020 a 2026 corresponden exactamente a la vintage 2021 del ASGS. Nulo en las disputas de gobierno local.",
    ),
    "Campo electorateId da ECQ. É único dentro de cada evento eleitoral e identifica uma única disputa por cadeira.": (
        "The ECQ electorateId field. Unique within each electoral event, identifying a single seat race.",
        "Campo electorateId de la ECQ. Es único dentro de cada evento electoral e identifica una única disputa por escaño.",
    ),
    "Código LGA 2021 do ABS, derivado por cruzamento validado de nomes: a ECQ mantém o sufixo legal (Shire, Regional, City) que o nome do ABS omite. Nulo nas disputas estaduais.": (
        "ABS 2021 LGA code, derived by a validated name crosswalk: the ECQ keeps the legal suffix (Shire, Regional, City) that the ABS name drops. Null in state contests.",
        "Código LGA 2021 del ABS, derivado por cruce validado de nombres: la ECQ mantiene el sufijo legal (Shire, Regional, City) que el nombre del ABS omite. Nulo en las disputas estatales.",
    ),
    "Campo areaCode da ECQ. Não é o código do ABS, que está em lga_id.": (
        "The ECQ areaCode field. Not the ABS code, which is in lga_id.",
        "Campo areaCode de la ECQ. No es el código del ABS, que está en lga_id.",
    ),
    "Assume os valores state e local. É definido por disputa, nunca uma constante do conjunto: disputas estaduais e locais convivem nas mesmas tabelas.": (
        "Takes the values state and local. Set per contest, never a constant of the dataset: state and local contests coexist in the same tables.",
        "Toma los valores state y local. Se define por disputa, nunca es una constante del conjunto: disputas estatales y locales conviven en las mismas tablas.",
    ),
    "Assume os valores state_district, councillor e mayor.": (
        "Takes the values state_district, councillor and mayor.",
        "Toma los valores state_district, councillor y mayor.",
    ),
    "Assume os valores compulsory_preferential, optional_preferential e first_past_the_post. É definido por disputa: os três coexistem dentro do evento de 2024.": (
        "Takes the values compulsory_preferential, optional_preferential and first_past_the_post. Set per contest: all three coexist within the 2024 event.",
        "Toma los valores compulsory_preferential, optional_preferential y first_past_the_post. Se define por disputa: los tres coexisten dentro del evento de 2024.",
    ),
    "Nulo nas disputas estaduais.": (
        "Null in state contests.",
        "Nulo en las disputas estatales.",
    ),
    "Assume os valores preliminary_unofficial, indicative_unofficial, first_preference_official, distribution_of_preferences_official e declared_unopposed. As apurações preliminar e oficial de primeira preferência divergem nos 93 distritos da eleição geral de 2024: não são recortes redundantes do mesmo número, portanto filtre sempre por count_status.": (
        "Takes the values preliminary_unofficial, indicative_unofficial, first_preference_official, distribution_of_preferences_official and declared_unopposed. The preliminary and official first preference counts differ in all 93 districts of the 2024 general election: they are not redundant snapshots of the same number, so always filter on count_status.",
        "Toma los valores preliminary_unofficial, indicative_unofficial, first_preference_official, distribution_of_preferences_official y declared_unopposed. Los escrutinios preliminar y oficial de primera preferencia divergen en los 93 distritos de la elección general de 2024: no son recortes redundantes del mismo número, por lo que conviene filtrar siempre por count_status.",
    ),
    "Total da etapa de distribuição, repetido em cada linha de pessoa candidata receptora.": (
        "Total for the distribution stage, repeated on every receiving candidate row.",
        "Total de la etapa de distribución, repetido en cada fila de persona candidata receptora.",
    ),
    "Número de ordem na cédula; aritmética sobre ele não tem sentido, por isso é publicado como texto.": (
        "Ballot order number; arithmetic on it is meaningless, so it is published as text.",
        "Número de orden en la boleta; la aritmética sobre él no tiene sentido, por eso se publica como texto.",
    ),
    'Formato "SOBRENOME, Prenomes".': (
        'Format "SURNAME, Given names".',
        'Formato "APELLIDO, Nombres".',
    ),
    "Assume os valores yes e no.": (
        "Takes the values yes and no.",
        "Toma los valores yes y no.",
    ),
    "Total do local de votação, repetido em cada linha de pessoa candidata.": (
        "Voting centre total, repeated on every candidate row.",
        "Total del local de votación, repetido en cada fila de persona candidata.",
    ),
    "Assume os valores first_preference e two_candidate_preferred. A ECQ não publica apuração de preferência entre dois partidos, ao contrário da AEC.": (
        "Takes the values first_preference and two_candidate_preferred. Unlike the AEC, the ECQ publishes no two party preferred count.",
        "Toma los valores first_preference y two_candidate_preferred. A diferencia de la AEC, la ECQ no publica escrutinio de preferencia entre dos partidos.",
    ),
    "Número de ordem na cédula; aritmética sobre ele não tem sentido.": (
        "Ballot order number; arithmetic on it is meaningless.",
        "Número de orden en la boleta; la aritmética sobre él no tiene sentido.",
    ),
    "Campo exclusivo da esfera estadual; vazio em todas as linhas locais.": (
        "A state-only field; empty on every local row.",
        "Campo exclusivo del ámbito estatal; vacío en todas las filas locales.",
    ),
    "Chave da tabela. A eleição suplementar de conselheiro do Mapoon Aboriginal Shire Council de 2023 (stub MASC23) está excluída do conjunto porque a ECQ não publica arquivo de resultados para ela.": (
        "Key of the table. The 2023 Mapoon Aboriginal Shire Council councillor by-election (stub MASC23) is excluded from the dataset because the ECQ publishes no results archive for it.",
        "Clave de la tabla. La elección parcial de concejal del Mapoon Aboriginal Shire Council de 2023 (stub MASC23) está excluida del conjunto porque la ECQ no publica archivo de resultados para ella.",
    ),
    "Assume os valores Local Quadrennial, State General, State By-election, Local Councillor By-election e Local Mayoral By-election.": (
        "Takes the values Local Quadrennial, State General, State By-election, Local Councillor By-election and Local Mayoral By-election.",
        "Toma los valores Local Quadrennial, State General, State By-election, Local Councillor By-election y Local Mayoral By-election.",
    ),
    "Assume os valores state e local.": (
        "Takes the values state and local.",
        "Toma los valores state y local.",
    ),
    "Número de ordem; aritmética sobre ele não tem sentido.": (
        "Sequence number; arithmetic on it is meaningless.",
        "Número de orden; la aritmética sobre él no tiene sentido.",
    ),
    "As 64 disputas decididas sem apuração, com count_status igual a declared_unopposed, trazem uma única linha com as colunas de votos nulas.": (
        "The 64 contests decided without a count, with count_status equal to declared_unopposed, carry a single row whose vote columns are null.",
        "Las 64 disputas decididas sin escrutinio, con count_status igual a declared_unopposed, traen una única fila con las columnas de votos nulas.",
    ),
    "Assume os valores PB, EV, DV1, DV2, AB1 e AB2. O campo typeDescription da própria ECQ é inconfiável, pois em parte dos locais repete o nome do local em vez do rótulo do tipo; por isso apenas o código é publicado aqui e os rótulos ficam na tabela dicionario.": (
        "Takes the values PB, EV, DV1, DV2, AB1 and AB2. The ECQ's own typeDescription field is unreliable, because for some centres it repeats the centre name instead of the type label; only the code is published here and the labels live in the dicionario table.",
        "Toma los valores PB, EV, DV1, DV2, AB1 y AB2. El campo typeDescription de la propia ECQ no es confiable, pues en parte de los locales repite el nombre del local en vez de la etiqueta del tipo; por eso aquí solo se publica el código y las etiquetas quedan en la tabla dicionario.",
    ),
    "Igual a district_name nas disputas estaduais e nas divisões de conselho; nas disputas de conselho indiviso e de prefeitura é uma sub-área da disputa.": (
        "Equal to district_name in state contests and in council divisions; in undivided council and mayoral contests it is a reporting sub-area of the contest.",
        "Igual a district_name en las disputas estatales y en las divisiones de concejo; en las disputas de concejo indiviso y de alcaldía es una subárea de la disputa.",
    ),
    "Número de ordem; aritmética sobre ele não tem sentido. A ECQ também publica esta distribuição por local de votação (146.223 linhas); esse grão mais fino não está incluído nesta tabela.": (
        "Sequence number; arithmetic on it is meaningless. The ECQ also publishes this distribution by voting centre (146,223 rows); that finer grain is not included in this table.",
        "Número de orden; la aritmética sobre él no tiene sentido. La ECQ también publica esta distribución por local de votación (146.223 filas); ese grano más fino no está incluido en esta tabla.",
    ),
    "O grão da tabela é uma linha por evento eleitoral, local de votação e distrito atendido: um local que atende dois distritos aparece duas vezes.": (
        "The grain of the table is one row per electoral event, voting centre and district served: a centre serving two districts appears twice.",
        "El grano de la tabla es una fila por evento electoral, local de votación y distrito atendido: un local que atiende dos distritos aparece dos veces.",
    ),
    "Publicado como texto: pode conter letras e intervalos.": (
        "Published as text: it can contain letters and ranges.",
        "Publicado como texto: puede contener letras e intervalos.",
    ),
    "Código postal, não uma quantidade; publicado como texto.": (
        "A postcode, not a quantity; published as text.",
        "Código postal, no una cantidad; publicado como texto.",
    ),
    "O vínculo com o diretório é real e é testado no dbt, mas o backend não consegue registrá-lo: ele só aceita chave estrangeira para a coluna marcada como chave primária do diretório, e a chave de br_bd_diretorios_au.state é id_state, não abbreviation.": (
        "The directory link is real and is tested in dbt, but the backend cannot record it: it only accepts a foreign key to the column flagged as the directory's primary key, and the key of br_bd_diretorios_au.state is id_state, not abbreviation.",
        "El vínculo con el directorio es real y se prueba en dbt, pero el backend no puede registrarlo: solo acepta clave foránea hacia la columna marcada como clave primaria del directorio, y la clave de br_bd_diretorios_au.state es id_state, no abbreviation.",
    ),
    "Assume os valores Host e Guest, que marcam locais compartilhados entre distritos. Nulo quando o local não é compartilhado.": (
        "Takes the values Host and Guest, which mark centres shared between districts. Null when the centre is not shared.",
        "Toma los valores Host y Guest, que marcan locales compartidos entre distritos. Nulo cuando el local no es compartido.",
    ),
    "Coluna de particionamento, derivada de date_gift_made. 23 linhas não trazem data da doação nem evento eleitoral, de modo que a coluna de particionamento não está integralmente preenchida.": (
        "Partition column, derived from date_gift_made. 23 rows carry neither a gift date nor an electoral event, so the partition column is not fully populated.",
        "Columna de particionamiento, derivada de date_gift_made. 23 filas no traen fecha de la donación ni evento electoral, de modo que la columna de particionamiento no está íntegramente poblada.",
    ),
    "Nenhuma coluna da fonte codifica esta informação: ela é derivada de qual das duas exportações do mapa (State ou Local) originou a linha, e essa é a única forma de recuperá-la.": (
        "No column in the source encodes this: it is derived from which of the two map exports (State or Local) the row came from, and that is the only way to recover it.",
        "Ninguna columna de la fuente codifica esta información: se deriva de cuál de las dos exportaciones del mapa (State o Local) originó la fila, y esa es la única forma de recuperarla.",
    ),
    "Ausente em 23 linhas, que também não trazem evento eleitoral associado.": (
        "Absent on 23 rows, which also carry no associated electoral event.",
        "Ausente en 23 filas, que tampoco traen evento electoral asociado.",
    ),
    "Linhas inteiramente duplicadas são frequentes (502 na esfera estadual e 54 na local) e não são necessariamente erros: duas doações idênticas do mesmo doador na mesma data são genuinamente indistinguíveis, por isso a tabela não publica chave primária.": (
        "Fully duplicated rows are pervasive (502 in the state export and 54 in the local one, 557 once pooled and cleaned) and are not necessarily errors: two identical gifts from the same donor on the same date are genuinely indistinguishable, so the table publishes no primary key.",
        "Las filas totalmente duplicadas son frecuentes (502 en la exportación estatal y 54 en la local, 557 una vez reunidas y limpiadas) y no son necesariamente errores: dos donaciones idénticas del mismo donante en la misma fecha son genuinamente indistinguibles, por lo que la tabla no publica clave primaria.",
    ),
    "Linhas inteiramente duplicadas são frequentes e não são necessariamente erros; a tabela não publica chave primária.": (
        "Fully duplicated rows are pervasive (446 in the source export, 454 once cleaned) and are not necessarily errors; the table publishes no primary key.",
        "Las filas totalmente duplicadas son frecuentes (446 en la exportación de origen, 454 una vez limpiadas) y no son necesariamente errores; la tabla no publica clave primaria.",
    ),
    "Majoritariamente vazio (preenchido em 2,7% das doações estaduais) e em texto livre. Não corresponde de forma confiável à tabela election, pois também nomeia eventos de 2012 e 2016, anteriores à cobertura de resultados.": (
        "Mostly empty (populated on 2.7% of state gifts, 13.6% once pooled) and free text. It does not join reliably to the election table, since it also names 2012 and 2016 events that precede the results coverage.",
        "Mayoritariamente vacío (poblado en el 2,7% de las donaciones estatales, 13,6% una vez reunidas) y en texto libre. No corresponde de forma confiable a la tabla election, pues también nombra eventos de 2012 y 2016, anteriores a la cobertura de resultados.",
    ),
    "Assume os valores Yes, No, Unknown e -. O traço é um terceiro estado real, que significa não aplicável e não um vazio: o campo existe apenas na esfera estadual, de modo que toda linha local traz -.": (
        "Takes the values Yes, No, Unknown and -. The dash is a real third state meaning not applicable, not a blank: the field exists only in the state sphere, so every local row carries -.",
        "Toma los valores Yes, No, Unknown y -. El guion es un tercer estado real, que significa no aplicable y no un vacío: el campo existe solo en el ámbito estatal, de modo que toda fila local trae -.",
    ),
    "Coluna de particionamento, derivada de date_incurred. A cobertura é concentrada nos ciclos eleitorais e não é uniforme: na prática começa em 2019, com apenas 1.997 linhas somando 2016, 2017 e 2018.": (
        "Partition column, derived from date_incurred. Coverage is concentrated in election cycles and is not uniform: in practice it starts in 2019, with only 1,997 rows across 2016, 2017 and 2018 combined.",
        "Columna de particionamiento, derivada de date_incurred. La cobertura se concentra en los ciclos electorales y no es uniforme: en la práctica comienza en 2019, con solo 1.997 filas sumando 2016, 2017 y 2018.",
    ),
    "Duas linhas datadas de 1924 são erros de digitação para 2024, ambas ligadas às eleições municipais de 2024, e foram corrigidas para 2024 para que o intervalo de particionamento não recue um século.": (
        "Two rows dated 1924 are data-entry typos for 2024, both attached to the 2024 local government elections, and were repaired to 2024 so the partition range is not dragged back a century.",
        "Dos filas fechadas en 1924 son errores de digitación por 2024, ambas ligadas a las elecciones municipales de 2024, y fueron corregidas a 2024 para que el intervalo de particionamiento no retroceda un siglo.",
    ),
    "Assume os valores Councillor, Mayor e Announced Candidate, além de vazio.": (
        "Takes the values Councillor, Mayor and Announced Candidate, as well as blank.",
        "Toma los valores Councillor, Mayor y Announced Candidate, además de vacío.",
    ),
    "Texto livre; não corresponde de forma confiável à tabela election.": (
        "Free text; it does not join reliably to the election table.",
        "Texto libre; no corresponde de forma confiable a la tabla election.",
    ),
    "Coluna de particionamento, derivada de date_created.": (
        "Partition column, derived from date_created.",
        "Columna de particionamiento, derivada de date_created.",
    ),
    "As exportações do Electronic Disclosure System não trazem coluna de situação da declaração: a interface do sistema filtra por ela, mas não a exporta, de modo que a situação de entrega é uma lacuna conhecida.": (
        "The Electronic Disclosure System exports carry no return status column: the system's interface filters on it but does not export it, so lodgement status is a known gap.",
        "Las exportaciones del Electronic Disclosure System no traen columna de situación de la declaración: la interfaz del sistema filtra por ella, pero no la exporta, de modo que la situación de entrega es una laguna conocida.",
    ),
    "São 20 períodos semestrais de calendário, do segundo semestre de 2016 ao primeiro semestre de 2026. A declaração é única por return_for_name, period_start_date e period_end_date.": (
        "There are 20 half-year calendar periods, from the second half of 2016 to the first half of 2026. The return is unique on return_for_name, period_start_date and period_end_date.",
        "Son 20 períodos semestrales de calendario, del segundo semestre de 2016 al primer semestre de 2026. La declaración es única por return_for_name, period_start_date y period_end_date.",
    ),
}


def validate() -> None:
    """Fail loudly on a duplicated column name within a table, or a typed quantity
    published without a measurement unit."""
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
                    f"{table}.{c.name}: observation note has no EN/ES translation"
                )


validate()


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


TABLE_META: dict[str, TableMeta] = {
    "election": TableMeta(
        "Eventos eleitorais",
        "Electoral events",
        "Eventos electorales",
        "Catálogo dos 54 eventos eleitorais estaduais e municipais de Queensland cobertos "
        "pelo conjunto: eleições gerais estaduais, eleições quadrienais municipais e "
        "eleições suplementares. Serve de chave para todas as demais tabelas de "
        "resultados por election_id.",
        "Catalogue of the 54 Queensland state and local electoral events covered by the "
        "dataset: state general elections, local quadrennial elections and by-elections. "
        "Acts as the key for every other results table through election_id.",
        "Catálogo de los 54 eventos electorales estatales y municipales de Queensland "
        "cubiertos por el conjunto: elecciones generales estatales, elecciones "
        "cuatrienales municipales y elecciones parciales. Sirve de clave para las demás "
        "tablas de resultados mediante election_id.",
        unique_key=["election_id"],
    ),
    "candidate": TableMeta(
        "Candidaturas",
        "Candidates",
        "Candidaturas",
        "Pessoas candidatas em cada disputa, com posição na cédula, partido e indicação "
        "de eleição. Reúne disputas estaduais e municipais, distinguidas por "
        "government_level.",
        "Candidates in each contest, with ballot position, party and declared elected "
        "status. Pools state and local contests, told apart by government_level.",
        "Personas candidatas en cada disputa, con posición en la boleta, partido e "
        "indicación de elección. Reúne disputas estatales y municipales, distinguidas "
        "por government_level.",
        unique_key=[
            "year",
            "election_id",
            "contest_id",
            "contest_type",
            "ballot_order_number",
        ],
    ),
    "enrolment_turnout": TableMeta(
        "Inscrições e comparecimento",
        "Enrolment and turnout",
        "Inscripciones y participación",
        "Uma linha por disputa e rodada de apuração, com pessoas inscritas, votos totais, "
        "válidos e inválidos e os percentuais correspondentes. As apurações preliminar e "
        "oficial não são recortes redundantes do mesmo número, portanto filtre sempre por "
        "count_status.",
        "One row per contest and count round, with enrolled electors, total, formal and "
        "informal votes and the corresponding shares. The preliminary and official counts "
        "are not redundant snapshots of the same number, so always filter on count_status.",
        "Una fila por disputa y ronda de escrutinio, con personas inscritas, votos "
        "totales, válidos e inválidos y los porcentajes correspondientes. Los escrutinios "
        "preliminar y oficial no son recortes redundantes del mismo número, por lo que "
        "conviene filtrar siempre por count_status.",
        unique_key=[
            "year",
            "election_id",
            "contest_id",
            "contest_type",
            "count_status",
        ],
    ),
    "result_district": TableMeta(
        "Resultados por disputa",
        "Results by contest",
        "Resultados por disputa",
        "Votos de cada pessoa candidata no total da disputa, por estágio e tipo de "
        "apuração. A ECQ não publica apuração de preferência entre dois partidos, "
        "apenas primeira preferência e preferência entre dois candidatos.",
        "Votes for each candidate across the whole contest, by count stage and count "
        "type. The ECQ publishes no two party preferred count, only first preference and "
        "two candidate preferred.",
        "Votos de cada persona candidata en el total de la disputa, por etapa y tipo de "
        "escrutinio. La ECQ no publica escrutinio de preferencia entre dos partidos, solo "
        "primera preferencia y preferencia entre dos candidatos.",
        unique_key=[
            "year",
            "election_id",
            "contest_id",
            "contest_type",
            "count_status",
            "count_type",
            "ballot_order_number",
        ],
    ),
    "result_voting_centre": TableMeta(
        "Resultados por local de votação",
        "Results by voting centre",
        "Resultados por local de votación",
        "Votos de cada pessoa candidata em cada local de votação, por estágio e tipo de "
        "apuração, com os totais do local repetidos em cada linha de candidatura.",
        "Votes for each candidate at each voting centre, by count stage and count type, "
        "with the voting centre totals repeated on every candidate row.",
        "Votos de cada persona candidata en cada local de votación, por etapa y tipo de "
        "escrutinio, con los totales del local repetidos en cada fila de candidatura.",
        unique_key=[
            "year",
            "election_id",
            "contest_id",
            "contest_type",
            "count_status",
            "count_type",
            "voting_centre_id",
            "voting_centre_district_name",
            "ballot_order_number",
        ],
    ),
    "distribution_of_preferences": TableMeta(
        "Distribuição de preferências",
        "Distribution of preferences",
        "Distribución de preferencias",
        "Transferências de voto na distribuição de preferências, no nível da disputa: uma "
        "linha por pessoa candidata excluída e pessoa candidata receptora. A ECQ também "
        "publica a mesma distribuição por local de votação, grão não incluído aqui.",
        "Vote transfers in the distribution of preferences, at contest level: one row per "
        "excluded candidate and receiving candidate. The ECQ also publishes the same "
        "distribution by voting centre, a grain not included here.",
        "Transferencias de voto en la distribución de preferencias, a nivel de disputa: "
        "una fila por persona candidata excluida y persona candidata receptora. La ECQ "
        "también publica la misma distribución por local de votación, grano no incluido "
        "aquí.",
        unique_key=[
            "year",
            "election_id",
            "contest_id",
            "contest_type",
            "count_status",
            "distribution_number",
            "excluded_ballot_order_number",
            "ballot_order_number",
        ],
    ),
    "voting_centre": TableMeta(
        "Locais de votação",
        "Voting centres",
        "Locales de votación",
        "Locais de votação de cada evento eleitoral, com endereço e coordenadas. O grão é "
        "uma linha por evento, local e distrito atendido: um local que atende dois "
        "distritos aparece duas vezes.",
        "Voting centres for each electoral event, with address and coordinates. The grain "
        "is one row per event, centre and district served: a centre serving two districts "
        "appears twice.",
        "Locales de votación de cada evento electoral, con dirección y coordenadas. El "
        "grano es una fila por evento, local y distrito atendido: un local que atiende dos "
        "distritos aparece dos veces.",
        unique_key=[
            "year",
            "election_id",
            "voting_centre_id",
            "district_name",
        ],
        # 11.3% populated: only the centres in a shared host/guest arrangement
        # carry one. Measured, not assumed.
        ignore_null_proportion=["joint_type"],
    ),
    "disclosure_gift": TableMeta(
        "Doações declaradas",
        "Disclosed gifts",
        "Donaciones declaradas",
        "Doações individuais declaradas ao Electronic Disclosure System da ECQ, reunindo "
        "as exportações estadual e local. Linhas inteiramente duplicadas são frequentes e "
        "não são necessariamente erros, de modo que a tabela não publica chave primária.",
        "Individual gifts disclosed to the ECQ Electronic Disclosure System, pooling the "
        "state and local exports. Fully duplicated rows are pervasive and are not "
        "necessarily errors, so the table publishes no primary key.",
        "Donaciones individuales declaradas al Electronic Disclosure System de la ECQ, "
        "reuniendo las exportaciones estatal y local. Las filas totalmente duplicadas son "
        "frecuentes y no son necesariamente errores, por lo que la tabla no publica clave "
        "primaria.",
        # No key: fully duplicated rows are pervasive (28,328 rows, 27,771
        # distinct, largest identical group 11) and are not necessarily errors,
        # so no uniqueness test is emitted and no synthetic key is invented.
        # Sparsity is measured on the pooled export and is time-dependent —
        # election_name 13.6%, has_electoral_committee 11.5%,
        # electoral_committee_name 7.5%. The latter two are state-only fields.
        ignore_null_proportion=[
            "election_name",
            "has_electoral_committee",
            "electoral_committee_name",
        ],
        # 23 gifts carry neither a gift date nor an election, so no year can be
        # inferred; year is nullable here and gets no not_null test.
        nullable_key=["year"],
    ),
    "disclosure_expenditure": TableMeta(
        "Despesas eleitorais declaradas",
        "Disclosed electoral expenditure",
        "Gastos electorales declarados",
        "Despesas eleitorais individuais declaradas ao Electronic Disclosure System da "
        "ECQ, com bem ou serviço adquirido e finalidade. A cobertura é concentrada nos "
        "ciclos eleitorais e começa na prática em 2019.",
        "Individual items of electoral expenditure disclosed to the ECQ Electronic "
        "Disclosure System, with the goods or services purchased and their purpose. "
        "Coverage is concentrated in election cycles and starts in practice in 2019.",
        "Gastos electorales individuales declarados al Electronic Disclosure System de la "
        "ECQ, con el bien o servicio adquirido y su finalidad. La cobertura se concentra "
        "en los ciclos electorales y comienza en la práctica en 2019.",
        # No key, for the same reason as the gifts: 27,225 rows, 26,771 distinct,
        # largest identical group 29. All four sparse columns measure above 50%
        # once pooled, so none is ignored in the null-proportion test.
    ),
    "disclosure_return": TableMeta(
        "Declarações periódicas",
        "Periodic returns",
        "Declaraciones periódicas",
        "Totais recebidos e pagos nas declarações periódicas semestrais apresentadas ao "
        "Electronic Disclosure System da ECQ. As exportações do sistema não trazem coluna "
        "de situação da declaração, que é uma lacuna conhecida.",
        "Totals received and paid in the half-yearly periodic returns lodged with the ECQ "
        "Electronic Disclosure System. The system's exports carry no return status column, "
        "which is a known gap.",
        "Totales recibidos y pagados en las declaraciones periódicas semestrales "
        "presentadas al Electronic Disclosure System de la ECQ. Las exportaciones del "
        "sistema no incluyen columna de situación de la declaración, lo que es una laguna "
        "conocida.",
        unique_key=["return_for_name", "period_start_date", "period_end_date"],
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
