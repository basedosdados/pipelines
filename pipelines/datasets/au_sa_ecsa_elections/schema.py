"""Column specification for every au_sa_ecsa_elections table.

Single source of truth: the architecture CSVs, the cleaning transform, the dbt
models and the backend column payloads are all generated from the ``TABLES``
mapping below, so the four can never drift.

Typing follows the house rule — INT64/FLOAT64 only where arithmetic is meaningful
and a measurement unit exists; every code, flag, ballot position, round number and
identifier is STRING.

The table skeleton and the column vocabulary are shared with
``au_qld_ecq_elections`` (Queensland) and ``au_nsw_nswec_elections`` (New South
Wales) so a later cross-jurisdiction union is a dbt model rather than another
re-onboarding. South Australia is bicameral like New South Wales, so ``chamber``
is a column on every contest rather than a split of the tables.

Two things are specific to South Australia and shape the schema:

``count_type`` carries vote components, not just measures
    The API publishes the ordinary and declaration halves of the first-preference
    count separately, and a declaration-only figure for both preferred counts.
    Keeping the table long on ``count_type`` publishes all of it without widening
    the row.

no Legislative Council candidates
    The API exposes the Legislative Council at ballot-group grain only. There is
    no candidate list and no candidate-level count, so the ``candidate`` table
    covers the House of Assembly alone rather than shipping a winners-only list
    that would read as complete.
"""

from __future__ import annotations

from dataclasses import dataclass, field

DIR_YEAR = "br_bd_diretorios_data_tempo.ano:ano"
DIR_SED = "br_bd_diretorios_au.state_electoral_division_2021:id_state_electoral_division"


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


def votes(name, pt, en, es, original="", obs="") -> Column:
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


def pct(name, pt, en, es, original="", obs="") -> Column:
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


def aud(name, pt, en, es, original="", obs="") -> Column:
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
# Observation notes, authored in Portuguese and translated below
# --------------------------------------------------------------------------------------

OBS_PARTITION = "Coluna de particionamento. Cada evento eleitoral estadual ocupa um único ano."
OBS_ELECTION_FK = (
    "Chave estrangeira para a tabela election, presente em todas as tabelas de "
    "resultados."
)
OBS_CONTEST_ID = (
    "Formado pelo prefixo da câmara e pelo slug do distrito, por exemplo ha-adelaide "
    "para a Assembleia e lc-state para o Conselho Legislativo, que é uma única "
    "disputa estadual. É único dentro de cada evento eleitoral."
)
OBS_SED = (
    "Código ASGS 2021 do ABS, atribuído por cruzamento de nomes sem distinção de "
    "maiúsculas. Cobre 47 dos 47 distritos de 2022 e 46 dos 47 de 2026: Ngadjuri, "
    "criado na redistribuição que substituiu Frome, é posterior à vintage 2021 e "
    "fica nulo. Nula no Conselho Legislativo, que é uma circunscrição estadual "
    "única."
)
OBS_CHAMBER = (
    "Assume os valores house_of_assembly e legislative_council. A Austrália "
    "Meridional é bicameral, portanto a câmara é uma coluna da disputa e nunca uma "
    "constante do conjunto."
)
OBS_GOV_LEVEL = (
    "Assume o valor state em todas as linhas. As eleições de governo local são "
    "servidas pela mesma API e estão deliberadamente fora deste conjunto."
)
OBS_CONTEST_TYPE = (
    "Assume os valores state_district, uma cadeira por distrito na Assembleia, e "
    "state_at_large, a eleição estadual única de 11 cadeiras do Conselho "
    "Legislativo."
)
OBS_VOTING_SYSTEM = (
    "Assume os valores compulsory_preferential, usado na Assembleia, e "
    "single_transferable_vote, usado no Conselho Legislativo com voto acima e "
    "abaixo da linha."
)
OBS_DISTRICT_NAME = (
    "Nome do distrito na Assembleia; assume o valor State no Conselho Legislativo. "
    "Frome foi substituído por Ngadjuri entre 2022 e 2026, e é a única mudança de "
    "nome entre os dois pleitos gerais."
)
OBS_BALLOT_ORDER = (
    "Posição da pessoa candidata na cédula do distrito, publicada pela ECSA no "
    "campo candidateId. Aritmética sobre ela não tem sentido, por isso é publicada "
    "como texto. Nula nas linhas do Conselho Legislativo, cujo grão é o grupo da "
    "cédula."
)
OBS_BALLOT_NAME = (
    'Formato "SOBRENOME, Prenomes", como publicado no arquivo de candidaturas. A '
    'própria ECSA publica o mesmo nome como "Prenomes SOBRENOME" nos blocos de '
    "distribuição e de declaração; ambos são normalizados para este formato."
)
OBS_PARTY_CODE = (
    "Sigla do partido. Em parte dos blocos de voto por declaração de 2026 a fonte "
    "grava a sigla no campo de nome do partido; o cruzamento é feito pela posição "
    "na cédula, não pelo texto."
)
OBS_GROUP_CODE = (
    "Letra da coluna do grupo na cédula do Conselho Legislativo. Nula na "
    "Assembleia. Publicada apenas de 2026 em diante: em 2022 a API do Conselho "
    "traz somente totais por partido, sem letra de grupo."
)
OBS_GROUP_NAME = (
    "Nome do grupo ou partido impresso na cédula do Conselho Legislativo. Nulo na "
    "Assembleia."
)
OBS_COUNT_TYPE_DISTRICT = (
    "Mantém a tabela longa. Assume first_preference, o total de primeiras "
    "preferências; first_preference_ordinary e first_preference_declaration, as "
    "suas duas parcelas; two_candidate_preferred e two_party_preferred; e as "
    "parcelas por declaração dessas duas. Em 2026 a parcela de declaração exclui "
    "os votos por ausência, que a fonte publica em bloco separado, portanto as "
    "parcelas não somam ao total nesse pleito."
)
OBS_COUNT_TYPE_VENUE = (
    "Mantém a tabela longa. Assume first_preference, two_candidate_preferred e "
    "two_party_preferred. A preferência entre dois partidos falta em 26 dos 47 "
    "distritos de 2026, onde a fonte grava zero em todos os locais; em 2022 está "
    "presente nos 47."
)
OBS_LC_UNGROUPED = (
    "No Conselho Legislativo os votos são publicados por grupo da cédula, e os "
    "grupos não esgotam o total válido: em 2022 a soma dos 20 grupos fica 63 votos "
    "abaixo do total publicado, diferença que corresponde a candidaturas sem grupo. "
    "Em 2026 a soma fecha exatamente."
)
OBS_TWO_PREFERRED = (
    "As duas apurações preferidas não são redundantes: divergem em 117 das 1.273 "
    "linhas de 2022 e em 137 das 536 de 2026, porque a preferência entre dois "
    "partidos força a comparação entre os dois maiores partidos e a preferência "
    "entre dois candidatos usa os dois mais votados."
)
OBS_VENUE_NAME = (
    "Nome do local de votação. Nos blocos de voto por declaração e por ausência de "
    "2026 o campo traz o rótulo do tipo de declaração, como Postal - Declaration 1, "
    "e não um local físico."
)
OBS_VENUE_TYPE = (
    "Código do tipo de local de votação. O vocabulário muda entre os pleitos: "
    "apenas Polling Booth é comum a 2022 e 2026, e a ECSA não publica "
    "correspondência entre os rótulos e os códigos curtos que usa em 2026."
)
OBS_VENUE_TOTAL = "Total do local de votação, repetido em cada linha de candidatura ou de grupo."
OBS_VENUE_DISTRICT = (
    "Distrito onde os votos foram apurados, e não a disputa. O Conselho "
    "Legislativo é uma disputa estadual única, mas as cédulas são apuradas "
    "distrito a distrito, portanto contest_id assume lc-state enquanto esta coluna "
    "nomeia o distrito do local."
)
OBS_ROUND_NUMBER = (
    "Número de ordem da rodada de distribuição; aritmética sobre ele não tem "
    "sentido. A rodada zero é a das primeiras preferências."
)
OBS_ROUND_TYPE = (
    "Assume FirstPreference e ExclusionRound1 a ExclusionRound10. Nulo em 2022, "
    "cujo bloco de distribuição traz apenas o total progressivo por rodada, sem "
    "identificar a candidatura excluída."
)
OBS_DOP_COVERAGE = (
    "A distribuição completa de preferências existe apenas no pleito geral de "
    "2026. Em 2022 a fonte publica somente o total progressivo por rodada, e as "
    "três eleições suplementares não trazem distribuição alguma."
)
OBS_YES_NO = "Assume os valores yes e no."
OBS_ELECTED_SOURCE = (
    "Em 2026 vem do campo isElected da distribuição de preferências. Em 2022 e nas "
    "eleições suplementares é derivado de quem lidera a apuração final, porque a "
    "fonte não publica indicador de eleição nesses pleitos."
)
OBS_ENROLMENT = (
    "Pessoas inscritas no distrito. No Conselho Legislativo é a soma dos 47 "
    "distritos, já que a circunscrição é o estado inteiro."
)
OBS_POLLING_PLACES = (
    "Locais de votação apurados e locais de votação totais, publicados pela ECSA "
    "apenas para o Conselho Legislativo. Nulos na Assembleia."
)
OBS_ELECTION_TYPE = (
    "Assume os valores state_general_election e state_by_election."
)
OBS_LC_SEATS = (
    "Cadeiras do Conselho Legislativo em disputa no evento: 11 nos pleitos gerais "
    "e nulo nas eleições suplementares, que são apenas de distrito."
)
OBS_HA_DISTRICTS = (
    "Distritos da Assembleia em disputa no evento: 47 nos pleitos gerais e 1 nas "
    "eleições suplementares."
)
OBS_DATA_VERSION = (
    "Versão do conjunto de resultados publicada pela ECSA, usada pela própria "
    "aplicação da comissão para buscar apenas o que mudou. Número de ordem, sem "
    "sentido aritmético."
)
OBS_DISCLOSURE_PORTAL = (
    "Assume os valores funding2024, o portal em vigor a partir de 2023, e "
    "fdarchive, o arquivo dos períodos de 2018 a 2023. Os dois publicam colunas "
    "diferentes: só o portal em vigor traz a entidade beneficiária."
)
OBS_DISCLOSURE_GIFTS = (
    "Esta tabela cobre a declaração, não as doações individuais. O detalhe por "
    "doação existe apenas dentro dos PDF anexados a cada declaração e não é "
    "extraído aqui."
)
OBS_DISCLOSURE_BAN = (
    "A Austrália Meridional proibiu doações políticas a partir de 1º de julho de "
    "2025, o que muda quais declarações passam a existir a partir dessa data."
)
OBS_DISCLOSURE_VALUE = (
    "Valor agregado da declaração tal como publicado no índice. Não é a soma "
    "verificada das doações, que só constam do PDF anexo."
)
OBS_DISCLOSURE_YEAR = (
    "Coluna de particionamento, derivada da data de início do período de "
    "referência da declaração."
)
OBS_CANDIDATE_HA_ONLY = (
    "Cobre apenas a Assembleia. A API publica o Conselho Legislativo em grão de "
    "grupo da cédula, sem lista de candidaturas, portanto nenhuma linha do "
    "Conselho é publicada aqui."
)
OBS_SOURCE_URL = (
    "Endereço da rota da API da ECSA que serve os resultados do evento."
)
OBS_LAST_UPDATED = (
    "Momento da última atualização dos resultados pela ECSA, publicado no próprio "
    "arquivo de resultados."
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
    observations=OBS_PARTITION,
)
ELECTION_ID = C(
    "election_id",
    "STRING",
    "Identificador do evento eleitoral",
    "Identifier of the electoral event",
    "Identificador del evento electoral",
    observations=OBS_ELECTION_FK,
)
CONTEST_ID = C(
    "contest_id",
    "STRING",
    "Identificador da disputa dentro do evento eleitoral",
    "Identifier of the contest within the electoral event",
    "Identificador de la disputa dentro del evento electoral",
    observations=OBS_CONTEST_ID,
)
SED_ID = C(
    "state_electoral_division_id",
    "STRING",
    "Código do distrito eleitoral estadual no padrão ASGS 2021 do ABS",
    "Australian Statistical Geography Standard 2021 code of the state electoral division",
    "Código del distrito electoral estatal en el estándar ASGS 2021 del ABS",
    directory_column=DIR_SED,
    observations=OBS_SED,
)
CHAMBER = C(
    "chamber",
    "STRING",
    "Câmara do Parlamento da Austrália Meridional em disputa",
    "Chamber of the South Australian Parliament being contested",
    "Cámara del Parlamento de Australia Meridional en disputa",
    covered_by_dictionary="yes",
    observations=OBS_CHAMBER,
)
GOVERNMENT_LEVEL = C(
    "government_level",
    "STRING",
    "Esfera de governo da disputa",
    "Level of government of the contest",
    "Nivel de gobierno de la disputa",
    covered_by_dictionary="yes",
    observations=OBS_GOV_LEVEL,
)
CONTEST_TYPE = C(
    "contest_type",
    "STRING",
    "Tipo de cadeira disputada",
    "Type of seat contested",
    "Tipo de escaño disputado",
    covered_by_dictionary="yes",
    observations=OBS_CONTEST_TYPE,
)
VOTING_SYSTEM = C(
    "voting_system",
    "STRING",
    "Sistema de votação aplicado à disputa",
    "Voting system applied to the contest",
    "Sistema de votación aplicado a la disputa",
    covered_by_dictionary="yes",
    observations=OBS_VOTING_SYSTEM,
)
DISTRICT_NAME = C(
    "district_name",
    "STRING",
    "Nome da disputa: o distrito eleitoral estadual, ou State para o Conselho Legislativo",
    "Name of the contest: the state electoral district, or State for the Legislative Council",
    "Nombre de la disputa: el distrito electoral estatal, o State para el Consejo Legislativo",
    observations=OBS_DISTRICT_NAME,
)

CONTEST_BLOCK = [
    YEAR,
    ELECTION_ID,
    CONTEST_ID,
    SED_ID,
    CHAMBER,
    GOVERNMENT_LEVEL,
    CONTEST_TYPE,
    VOTING_SYSTEM,
    DISTRICT_NAME,
]

BALLOT_ORDER_NUMBER = C(
    "ballot_order_number",
    "STRING",
    "Posição da pessoa candidata na cédula",
    "Position of the candidate on the ballot paper",
    "Posición de la persona candidata en la boleta",
    original_name="candidateId",
    observations=OBS_BALLOT_ORDER,
)
BALLOT_NAME = C(
    "ballot_name",
    "STRING",
    "Nome da pessoa candidata tal como impresso na cédula",
    "Name of the candidate as printed on the ballot paper",
    "Nombre de la persona candidata tal como aparece impreso en la boleta",
    original_name="candidateName",
    observations=OBS_BALLOT_NAME,
)
PARTY_CODE = C(
    "party_code",
    "STRING",
    "Sigla do partido da pessoa candidata ou do grupo",
    "Abbreviation of the candidate's or group's party",
    "Sigla del partido de la persona candidata o del grupo",
    original_name="partyId",
    observations=OBS_PARTY_CODE,
)
PARTY_NAME = C(
    "party_name",
    "STRING",
    "Nome registrado do partido da pessoa candidata ou do grupo",
    "Registered name of the candidate's or group's party",
    "Nombre registrado del partido de la persona candidata o del grupo",
    original_name="partyName",
)
GROUP_CODE = C(
    "group_code",
    "STRING",
    "Letra do grupo na cédula do Conselho Legislativo",
    "Letter of the group on the Legislative Council ballot paper",
    "Letra del grupo en la boleta del Consejo Legislativo",
    original_name="groupId",
    observations=OBS_GROUP_CODE,
)
GROUP_NAME = C(
    "group_name",
    "STRING",
    "Nome do grupo impresso na cédula do Conselho Legislativo",
    "Name of the group printed on the Legislative Council ballot paper",
    "Nombre del grupo impreso en la boleta del Consejo Legislativo",
    original_name="groupName",
    observations=OBS_LC_UNGROUPED,
)
VOTING_CENTRE_NAME = C(
    "voting_centre_name",
    "STRING",
    "Nome do local de votação ou do tipo de voto por declaração",
    "Name of the voting centre, or of the declaration vote type",
    "Nombre del local de votación o del tipo de voto por declaración",
    original_name="pollingPlaceName",
    observations=OBS_VENUE_NAME,
)
VOTING_CENTRE_TYPE = C(
    "voting_centre_type",
    "STRING",
    "Tipo do local de votação tal como publicado pela ECSA",
    "Type of the voting centre as published by the ECSA",
    "Tipo del local de votación tal como lo publica la ECSA",
    original_name="pollingPlaceType",
    observations=OBS_VENUE_TYPE,
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
    ),
    C(
        "election_name",
        "STRING",
        "Nome do evento eleitoral publicado pela ECSA",
        "Name of the electoral event published by the ECSA",
        "Nombre del evento electoral publicado por la ECSA",
        original_name="electionEvent",
    ),
    C(
        "election_type",
        "STRING",
        "Tipo do evento eleitoral",
        "Type of the electoral event",
        "Tipo del evento electoral",
        covered_by_dictionary="yes",
        original_name="electionType",
        observations=OBS_ELECTION_TYPE,
    ),
    C(
        "government_level",
        "STRING",
        "Esfera de governo do evento eleitoral",
        "Level of government of the electoral event",
        "Nivel de gobierno del evento electoral",
        covered_by_dictionary="yes",
        observations=OBS_GOV_LEVEL,
    ),
    C(
        "election_date",
        "DATE",
        "Data da votação",
        "Date of the poll",
        "Fecha de la votación",
        original_name="electionDate",
    ),
    C(
        "assembly_districts_contested",
        "INT64",
        "Número de distritos da Assembleia em disputa no evento",
        "Number of Assembly districts contested at the event",
        "Número de distritos de la Asamblea en disputa en el evento",
        measurement_unit="district",
        observations=OBS_HA_DISTRICTS,
    ),
    C(
        "council_seats_contested",
        "INT64",
        "Número de cadeiras do Conselho Legislativo em disputa no evento",
        "Number of Legislative Council seats contested at the event",
        "Número de escaños del Consejo Legislativo en disputa en el evento",
        measurement_unit="seat",
        original_name="lcSeatsContested",
        observations=OBS_LC_SEATS,
    ),
    C(
        "results_last_updated",
        "DATETIME",
        "Momento da última atualização dos resultados pela ECSA",
        "Time the results were last updated by the ECSA",
        "Momento de la última actualización de los resultados por la ECSA",
        original_name="lastUpdated",
        observations=OBS_LAST_UPDATED,
    ),
    C(
        "results_data_version",
        "STRING",
        "Versão do conjunto de resultados publicada pela ECSA",
        "Version of the results dataset published by the ECSA",
        "Versión del conjunto de resultados publicada por la ECSA",
        original_name="dataVersion",
        observations=OBS_DATA_VERSION,
    ),
    C(
        "results_source_url",
        "STRING",
        "Endereço da API que serve os resultados do evento",
        "URL of the API serving the results of the event",
        "Dirección de la API que sirve los resultados del evento",
        observations=OBS_SOURCE_URL,
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
        observations=OBS_CANDIDATE_HA_ONLY,
    ),
    C(
        "candidate_given_names",
        "STRING",
        "Prenomes da pessoa candidata",
        "Given names of the candidate",
        "Nombres de pila de la persona candidata",
    ),
    PARTY_CODE,
    PARTY_NAME,
    C(
        "is_declared_elected",
        "STRING",
        "Indica se a pessoa candidata foi eleita na disputa",
        "Whether the candidate was elected in the contest",
        "Indica si la persona candidata fue electa en la disputa",
        covered_by_dictionary="yes",
        observations=OBS_ELECTED_SOURCE,
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
        observations=OBS_COUNT_TYPE_DISTRICT,
    ),
    BALLOT_ORDER_NUMBER,
    BALLOT_NAME,
    PARTY_CODE,
    PARTY_NAME,
    GROUP_CODE,
    GROUP_NAME,
    votes(
        "votes",
        "Votos atribuídos à candidatura ou ao grupo na apuração",
        "Votes credited to the candidate or group in the count",
        "Votos atribuidos a la candidatura o al grupo en el escrutinio",
        obs=OBS_TWO_PREFERRED,
    ),
    pct(
        "percentage",
        "Votos como percentual dos votos válidos da disputa na mesma apuração",
        "Votes as a share of the formal votes of the contest in the same count",
        "Votos como porcentaje de los votos válidos de la disputa en el mismo escrutinio",
    ),
]

TABLES["result_voting_centre"] = [
    *CONTEST_BLOCK,
    C(
        "voting_centre_district_name",
        "STRING",
        "Nome do distrito onde o local de votação foi apurado",
        "Name of the district the voting centre was counted in",
        "Nombre del distrito donde se escrutó el local de votación",
        observations=OBS_VENUE_DISTRICT,
    ),
    VOTING_CENTRE_NAME,
    VOTING_CENTRE_TYPE,
    C(
        "count_type",
        "STRING",
        "Tipo de apuração ao qual os votos se referem",
        "Type of count the votes refer to",
        "Tipo de escrutinio al que se refieren los votos",
        covered_by_dictionary="yes",
        observations=OBS_COUNT_TYPE_VENUE,
    ),
    BALLOT_ORDER_NUMBER,
    BALLOT_NAME,
    PARTY_CODE,
    PARTY_NAME,
    GROUP_CODE,
    GROUP_NAME,
    votes(
        "votes",
        "Votos atribuídos à candidatura ou ao grupo no local de votação",
        "Votes credited to the candidate or group at the voting centre",
        "Votos atribuidos a la candidatura o al grupo en el local de votación",
    ),
    votes(
        "votes_formal",
        "Total de votos válidos no local de votação",
        "Total formal votes at the voting centre",
        "Total de votos válidos en el local de votación",
        obs=OBS_VENUE_TOTAL,
    ),
    votes(
        "votes_informal",
        "Total de votos inválidos no local de votação",
        "Total informal votes at the voting centre",
        "Total de votos inválidos en el local de votación",
        original="informalVotes",
        obs=OBS_VENUE_TOTAL,
    ),
]

TABLES["distribution_of_preferences"] = [
    *CONTEST_BLOCK,
    C(
        "round_number",
        "STRING",
        "Número da rodada de distribuição de preferências",
        "Number of the preference distribution round",
        "Número de la ronda de distribución de preferencias",
        original_name="roundNumber",
        observations=OBS_ROUND_NUMBER,
    ),
    C(
        "round_type",
        "STRING",
        "Tipo da rodada de distribuição",
        "Type of the distribution round",
        "Tipo de la ronda de distribución",
        covered_by_dictionary="yes",
        original_name="roundType",
        observations=OBS_ROUND_TYPE,
    ),
    C(
        "excluded_ballot_name",
        "STRING",
        "Nome na cédula da pessoa candidata excluída na rodada",
        "Ballot name of the candidate excluded at this round",
        "Nombre en la boleta de la persona candidata excluida en la ronda",
        original_name="excludedCandidateName",
        observations=OBS_DOP_COVERAGE,
    ),
    votes(
        "votes_excluded",
        "Votos da pessoa candidata excluída, redistribuídos na rodada",
        "Votes of the excluded candidate, redistributed at this round",
        "Votos de la persona candidata excluida, redistribuidos en la ronda",
        original="excludedCandidateVotes",
    ),
    BALLOT_ORDER_NUMBER,
    BALLOT_NAME,
    PARTY_CODE,
    PARTY_NAME,
    votes(
        "votes_transferred",
        "Votos transferidos para a candidatura receptora na rodada",
        "Votes transferred to the receiving candidate at this round",
        "Votos transferidos a la candidatura receptora en la ronda",
        original="voteChange",
    ),
    votes(
        "votes_progressive_total",
        "Total acumulado da candidatura ao fim da rodada",
        "Progressive total of the candidate at the end of the round",
        "Total acumulado de la candidatura al final de la ronda",
        original="progressiveTotal",
    ),
    C(
        "is_excluded",
        "STRING",
        "Indica se a candidatura foi excluída nesta rodada",
        "Whether the candidate was excluded at this round",
        "Indica si la candidatura fue excluida en esta ronda",
        covered_by_dictionary="yes",
        original_name="isExcluded",
        observations=OBS_YES_NO,
    ),
    C(
        "is_elected",
        "STRING",
        "Indica se a candidatura foi eleita nesta rodada",
        "Whether the candidate was elected at this round",
        "Indica si la candidatura fue electa en esta ronda",
        covered_by_dictionary="yes",
        original_name="isElected",
        observations=OBS_YES_NO,
    ),
]

TABLES["voting_centre"] = [
    YEAR,
    ELECTION_ID,
    SED_ID,
    C(
        "district_name",
        "STRING",
        "Nome do distrito atendido pelo local de votação",
        "Name of the district served by the voting centre",
        "Nombre del distrito atendido por el local de votación",
    ),
    VOTING_CENTRE_NAME,
    VOTING_CENTRE_TYPE,
]

TABLES["enrolment_turnout"] = [
    *CONTEST_BLOCK,
    C(
        "enrolment",
        "INT64",
        "Número de pessoas inscritas na disputa",
        "Number of electors enrolled in the contest",
        "Número de personas inscritas en la disputa",
        measurement_unit="person",
        original_name="districtEnrolled",
        observations=OBS_ENROLMENT,
    ),
    C(
        "candidates_count",
        "INT64",
        "Número de candidaturas ou de grupos na disputa",
        "Number of candidates or groups in the contest",
        "Número de candidaturas o de grupos en la disputa",
        measurement_unit="candidate",
    ),
    votes(
        "votes_formal",
        "Total de votos válidos apurados na disputa",
        "Total formal votes counted in the contest",
        "Total de votos válidos escrutados en la disputa",
    ),
    votes(
        "votes_informal",
        "Total de votos inválidos apurados na disputa",
        "Total informal votes counted in the contest",
        "Total de votos inválidos escrutados en la disputa",
    ),
    votes(
        "votes_total",
        "Total de votos apurados na disputa",
        "Total votes counted in the contest",
        "Total de votos escrutados en la disputa",
    ),
    pct(
        "percentage_informal",
        "Votos inválidos como percentual do total apurado",
        "Informal votes as a share of the total count",
        "Votos inválidos como porcentaje del total escrutado",
    ),
    pct(
        "percentage_roll_counted",
        "Votos apurados como percentual das pessoas inscritas",
        "Votes counted as a share of enrolled electors",
        "Votos escrutados como porcentaje de las personas inscritas",
    ),
    C(
        "polling_places_counted",
        "INT64",
        "Locais de votação apurados na disputa",
        "Voting centres counted in the contest",
        "Locales de votación escrutados en la disputa",
        measurement_unit="voting centre",
        original_name="pollingPlacesCounted",
        observations=OBS_POLLING_PLACES,
    ),
    C(
        "polling_places_total",
        "INT64",
        "Locais de votação da disputa",
        "Voting centres in the contest",
        "Locales de votación de la disputa",
        measurement_unit="voting centre",
        original_name="totalPollingPlaces",
    ),
]

TABLES["disclosure_return"] = [
    C(
        "year",
        "INT64",
        "Ano de início do período de referência da declaração",
        "Year the reporting period of the return starts",
        "Año de inicio del período de referencia de la declaración",
        measurement_unit="year",
        directory_column=DIR_YEAR,
        observations=OBS_DISCLOSURE_YEAR,
    ),
    C(
        "return_id",
        "STRING",
        "Identificador da declaração no portal que a publica",
        "Identifier of the return in the portal that publishes it",
        "Identificador de la declaración en el portal que la publica",
        original_name="ID",
        observations=OBS_DISCLOSURE_GIFTS,
    ),
    C(
        "portal",
        "STRING",
        "Portal da ECSA que publica a declaração",
        "ECSA portal that publishes the return",
        "Portal de la ECSA que publica la declaración",
        covered_by_dictionary="yes",
        observations=OBS_DISCLOSURE_PORTAL,
    ),
    C(
        "return_type",
        "STRING",
        "Tipo da declaração apresentada",
        "Type of the return lodged",
        "Tipo de la declaración presentada",
        original_name="RETURN TYPE",
        observations=OBS_DISCLOSURE_BAN,
    ),
    C(
        "date_lodged",
        "DATE",
        "Data em que a declaração foi apresentada",
        "Date the return was lodged",
        "Fecha en que se presentó la declaración",
        original_name="DATE LODGED",
    ),
    C(
        "submitter_name",
        "STRING",
        "Nome de quem apresentou a declaração",
        "Name of the party that lodged the return",
        "Nombre de quien presentó la declaración",
        original_name="SUBMITTER",
    ),
    C(
        "return_for_name",
        "STRING",
        "Nome da entidade à qual a declaração se refere",
        "Name of the entity the return is lodged for",
        "Nombre de la entidad a la que se refiere la declaración",
        original_name="FOR",
    ),
    C(
        "recipient_name",
        "STRING",
        "Nome da entidade beneficiária declarada",
        "Name of the declared recipient entity",
        "Nombre de la entidad beneficiaria declarada",
        original_name="RECIPIENT",
    ),
    C(
        "period_start_date",
        "DATE",
        "Data inicial do período de referência da declaração",
        "Start date of the reporting period of the return",
        "Fecha inicial del período de referencia de la declaración",
        original_name="FROM",
    ),
    C(
        "period_end_date",
        "DATE",
        "Data final do período de referência da declaração",
        "End date of the reporting period of the return",
        "Fecha final del período de referencia de la declaración",
        original_name="TO",
    ),
    aud(
        "declared_value",
        "Valor agregado declarado, em dólares australianos",
        "Aggregate declared value, in Australian dollars",
        "Valor agregado declarado, en dólares australianos",
        original="VALUE",
        obs=OBS_DISCLOSURE_VALUE,
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

PARTITION_COLUMNS: dict[str, list[str]] = {
    name: ([] if name == "dicionario" else ["year"]) for name in TABLES
}

# Starts at 2015, not at the 2022 start of the results coverage: the disclosure
# archive carries reporting periods opening in 2015. Outside the range those rows
# land in BigQuery's __UNPARTITIONED__ bucket, which is not a load error and is
# therefore easy to miss.
PARTITION_RANGE = {"start": 2015, "end": 2035, "interval": 1}


def column_names(table: str) -> list[str]:
    return [c.name for c in TABLES[table]]


def column_types(table: str) -> dict[str, str]:
    return {c.name: c.bigquery_type for c in TABLES[table]}


# --------------------------------------------------------------------------------------
# Observation translations
# --------------------------------------------------------------------------------------

# Column ``observations`` are authored in Portuguese; the backend stores one field per
# language and a bare ``observations`` key is written to Portuguese only, which is how
# 3,022 production columns ended up PT-only. Every note is translated here and
# ``validate()`` fails on any note that is not.
OBSERVATION_TRANSLATIONS: dict[str, tuple[str, str]] = {
    OBS_PARTITION: (
        "Partition column. Each state electoral event falls in a single year.",
        "Columna de particionamiento. Cada evento electoral estatal ocupa un solo año.",
    ),
    OBS_ELECTION_FK: (
        "Foreign key to the election table, present in every results table.",
        "Clave foránea hacia la tabla election, presente en todas las tablas de resultados.",
    ),
    OBS_CONTEST_ID: (
        "Built from the chamber prefix and the district slug, for example ha-adelaide "
        "for the Assembly and lc-state for the Legislative Council, which is a single "
        "statewide contest. Unique within each electoral event.",
        "Formado por el prefijo de la cámara y el slug del distrito, por ejemplo "
        "ha-adelaide para la Asamblea y lc-state para el Consejo Legislativo, que es una "
        "única disputa estatal. Es único dentro de cada evento electoral.",
    ),
    OBS_SED: (
        "ABS ASGS 2021 code, assigned by a case-insensitive name match. It covers 47 of "
        "the 47 districts of 2022 and 46 of the 47 of 2026: Ngadjuri, created in the "
        "redistribution that replaced Frome, postdates the 2021 vintage and is left "
        "null. Null in the Legislative Council, which is a single statewide division.",
        "Código ASGS 2021 del ABS, asignado por cruce de nombres sin distinción de "
        "mayúsculas. Cubre 47 de los 47 distritos de 2022 y 46 de los 47 de 2026: "
        "Ngadjuri, creado en la redistribución que sustituyó a Frome, es posterior a la "
        "vintage 2021 y queda nulo. Nulo en el Consejo Legislativo, que es una única "
        "circunscripción estatal.",
    ),
    OBS_CHAMBER: (
        "Takes the values house_of_assembly and legislative_council. South Australia is "
        "bicameral, so the chamber is a column of the contest and never a constant of "
        "the dataset.",
        "Toma los valores house_of_assembly y legislative_council. Australia Meridional "
        "es bicameral, por lo que la cámara es una columna de la disputa y nunca una "
        "constante del conjunto.",
    ),
    OBS_GOV_LEVEL: (
        "Takes the value state on every row. Local government elections are served by "
        "the same API and are deliberately outside this dataset.",
        "Toma el valor state en todas las filas. Las elecciones de gobierno local son "
        "servidas por la misma API y quedan deliberadamente fuera de este conjunto.",
    ),
    OBS_CONTEST_TYPE: (
        "Takes the values state_district, one seat per district in the Assembly, and "
        "state_at_large, the single statewide 11 seat election of the Legislative "
        "Council.",
        "Toma los valores state_district, un escaño por distrito en la Asamblea, y "
        "state_at_large, la única elección estatal de 11 escaños del Consejo "
        "Legislativo.",
    ),
    OBS_VOTING_SYSTEM: (
        "Takes the values compulsory_preferential, used in the Assembly, and "
        "single_transferable_vote, used in the Legislative Council with above and below "
        "the line voting.",
        "Toma los valores compulsory_preferential, usado en la Asamblea, y "
        "single_transferable_vote, usado en el Consejo Legislativo con voto sobre y bajo "
        "la línea.",
    ),
    OBS_DISTRICT_NAME: (
        "Name of the district in the Assembly; takes the value State in the Legislative "
        "Council. Frome was replaced by Ngadjuri between 2022 and 2026, the only name "
        "change between the two general elections.",
        "Nombre del distrito en la Asamblea; toma el valor State en el Consejo "
        "Legislativo. Frome fue sustituido por Ngadjuri entre 2022 y 2026, y es el único "
        "cambio de nombre entre los dos comicios generales.",
    ),
    OBS_BALLOT_ORDER: (
        "Position of the candidate on the district ballot paper, published by the ECSA "
        "in the candidateId field. Arithmetic on it is meaningless, so it is published "
        "as text. Null on Legislative Council rows, whose grain is the ballot group.",
        "Posición de la persona candidata en la boleta del distrito, publicada por la "
        "ECSA en el campo candidateId. La aritmética sobre ella no tiene sentido, por eso "
        "se publica como texto. Nula en las filas del Consejo Legislativo, cuyo grano es "
        "el grupo de la boleta.",
    ),
    OBS_BALLOT_NAME: (
        'Format "SURNAME, Given names", as published in the candidates file. The ECSA '
        'publishes the same name as "Given names SURNAME" in the distribution and '
        "declaration blocks; both are normalised to this format.",
        'Formato "APELLIDO, Nombres", como se publica en el archivo de candidaturas. La '
        'propia ECSA publica el mismo nombre como "Nombres APELLIDO" en los bloques de '
        "distribución y de declaración; ambos se normalizan a este formato.",
    ),
    OBS_PARTY_CODE: (
        "Abbreviation of the party. In part of the 2026 declaration vote blocks the "
        "source writes the abbreviation into the party name field; the match is made on "
        "ballot position, not on the text.",
        "Sigla del partido. En parte de los bloques de voto por declaración de 2026 la "
        "fuente graba la sigla en el campo de nombre del partido; el cruce se hace por la "
        "posición en la boleta, no por el texto.",
    ),
    OBS_GROUP_CODE: (
        "Letter of the group column on the Legislative Council ballot paper. Null in the "
        "Assembly. Published from 2026 onwards only: in 2022 the Council API carries "
        "party totals alone, with no group letter.",
        "Letra de la columna del grupo en la boleta del Consejo Legislativo. Nula en la "
        "Asamblea. Publicada solo a partir de 2026: en 2022 la API del Consejo trae "
        "únicamente totales por partido, sin letra de grupo.",
    ),
    OBS_GROUP_NAME: (
        "Name of the group or party printed on the Legislative Council ballot paper. "
        "Null in the Assembly.",
        "Nombre del grupo o partido impreso en la boleta del Consejo Legislativo. Nulo "
        "en la Asamblea.",
    ),
    OBS_COUNT_TYPE_DISTRICT: (
        "Keeps the table long. Takes first_preference, the total of first preferences; "
        "first_preference_ordinary and first_preference_declaration, its two components; "
        "two_candidate_preferred and two_party_preferred; and the declaration components "
        "of those two. In 2026 the declaration component excludes absent votes, which "
        "the source publishes in a separate block, so the components do not sum to the "
        "total at that election.",
        "Mantiene la tabla larga. Toma first_preference, el total de primeras "
        "preferencias; first_preference_ordinary y first_preference_declaration, sus dos "
        "componentes; two_candidate_preferred y two_party_preferred; y los componentes "
        "por declaración de esos dos. En 2026 el componente de declaración excluye los "
        "votos por ausencia, que la fuente publica en un bloque aparte, por lo que los "
        "componentes no suman el total en ese comicio.",
    ),
    OBS_COUNT_TYPE_VENUE: (
        "Keeps the table long. Takes first_preference, two_candidate_preferred and "
        "two_party_preferred. The two party preferred count is missing in 26 of the 47 "
        "districts of 2026, where the source records zero at every centre; in 2022 it is "
        "present in all 47.",
        "Mantiene la tabla larga. Toma first_preference, two_candidate_preferred y "
        "two_party_preferred. La preferencia entre dos partidos falta en 26 de los 47 "
        "distritos de 2026, donde la fuente graba cero en todos los locales; en 2022 está "
        "presente en los 47.",
    ),
    OBS_LC_UNGROUPED: (
        "In the Legislative Council the votes are published by ballot group, and the "
        "groups do not exhaust the formal total: in 2022 the sum of the 20 groups "
        "falls 63 votes short of the published total, a difference that corresponds "
        "to ungrouped candidates. In 2026 the sum reconciles exactly.",
        "En el Consejo Legislativo los votos se publican por grupo de la boleta, y los "
        "grupos no agotan el total válido: en 2022 la suma de los 20 grupos queda 63 "
        "votos por debajo del total publicado, diferencia que corresponde a "
        "candidaturas sin grupo. En 2026 la suma cierra exactamente.",
    ),
    OBS_TWO_PREFERRED: (
        "The two preferred counts are not redundant: they differ in 117 of the 1,273 "
        "rows of 2022 and in 137 of the 536 of 2026, because the two party preferred "
        "count forces the comparison between the two largest parties while the two "
        "candidate preferred count uses the two best polling candidates.",
        "Los dos escrutinios preferidos no son redundantes: difieren en 117 de las 1.273 "
        "filas de 2022 y en 137 de las 536 de 2026, porque la preferencia entre dos "
        "partidos fuerza la comparación entre los dos mayores partidos y la preferencia "
        "entre dos candidatos usa los dos más votados.",
    ),
    OBS_VENUE_NAME: (
        "Name of the voting centre. In the 2026 declaration and absent vote blocks the "
        "field carries the declaration type label, such as Postal - Declaration 1, and "
        "not a physical place.",
        "Nombre del local de votación. En los bloques de voto por declaración y por "
        "ausencia de 2026 el campo trae la etiqueta del tipo de declaración, como Postal "
        "- Declaration 1, y no un local físico.",
    ),
    OBS_VENUE_TYPE: (
        "Code for the type of voting centre. The vocabulary changes between elections: "
        "only Polling Booth is common to 2022 and 2026, and the ECSA publishes no "
        "mapping between the labels and the short codes it uses in 2026.",
        "Código del tipo de local de votación. El vocabulario cambia entre comicios: solo "
        "Polling Booth es común a 2022 y 2026, y la ECSA no publica correspondencia entre "
        "las etiquetas y los códigos cortos que usa en 2026.",
    ),
    OBS_VENUE_TOTAL: (
        "Voting centre total, repeated on every candidate or group row.",
        "Total del local de votación, repetido en cada fila de candidatura o de grupo.",
    ),
    OBS_VENUE_DISTRICT: (
        "The district the votes were counted in, not the contest. The Legislative "
        "Council is a single statewide contest, but ballots are counted district by "
        "district, so contest_id takes lc-state while this column names the district of "
        "the centre.",
        "Distrito donde se escrutaron los votos, y no la disputa. El Consejo Legislativo "
        "es una única disputa estatal, pero las boletas se escrutan distrito a distrito, "
        "por lo que contest_id toma lc-state mientras esta columna nombra el distrito del "
        "local.",
    ),
    OBS_ROUND_NUMBER: (
        "Sequence number of the distribution round; arithmetic on it is meaningless. "
        "Round zero is the first preference round.",
        "Número de orden de la ronda de distribución; la aritmética sobre él no tiene "
        "sentido. La ronda cero es la de las primeras preferencias.",
    ),
    OBS_ROUND_TYPE: (
        "Takes FirstPreference and ExclusionRound1 to ExclusionRound10. Null in 2022, "
        "whose distribution block carries only the progressive total per round, without "
        "identifying the excluded candidate.",
        "Toma FirstPreference y ExclusionRound1 a ExclusionRound10. Nulo en 2022, cuyo "
        "bloque de distribución trae solo el total progresivo por ronda, sin identificar "
        "la candidatura excluida.",
    ),
    OBS_DOP_COVERAGE: (
        "The full distribution of preferences exists only at the 2026 general election. "
        "In 2022 the source publishes only the progressive total per round, and the three "
        "by-elections carry no distribution at all.",
        "La distribución completa de preferencias existe solo en el comicio general de "
        "2026. En 2022 la fuente publica solamente el total progresivo por ronda, y las "
        "tres elecciones parciales no traen distribución alguna.",
    ),
    OBS_YES_NO: (
        "Takes the values yes and no.",
        "Toma los valores yes y no.",
    ),
    OBS_ELECTED_SOURCE: (
        "In 2026 it comes from the isElected field of the distribution of preferences. "
        "In 2022 and at the by-elections it is derived from who leads the final count, "
        "because the source publishes no elected flag at those events.",
        "En 2026 proviene del campo isElected de la distribución de preferencias. En 2022 "
        "y en las elecciones parciales se deriva de quien lidera el escrutinio final, "
        "porque la fuente no publica indicador de elección en esos comicios.",
    ),
    OBS_ENROLMENT: (
        "Electors enrolled in the district. In the Legislative Council it is the sum of "
        "the 47 districts, since the division is the whole state.",
        "Personas inscritas en el distrito. En el Consejo Legislativo es la suma de los "
        "47 distritos, ya que la circunscripción es todo el estado.",
    ),
    OBS_POLLING_PLACES: (
        "Voting centres counted and voting centres in total, published by the ECSA for "
        "the Legislative Council only. Null in the Assembly.",
        "Locales de votación escrutados y locales de votación totales, publicados por la "
        "ECSA solo para el Consejo Legislativo. Nulos en la Asamblea.",
    ),
    OBS_ELECTION_TYPE: (
        "Takes the values state_general_election and state_by_election.",
        "Toma los valores state_general_election y state_by_election.",
    ),
    OBS_LC_SEATS: (
        "Legislative Council seats contested at the event: 11 at the general elections "
        "and null at the by-elections, which are district contests only.",
        "Escaños del Consejo Legislativo en disputa en el evento: 11 en los comicios "
        "generales y nulo en las elecciones parciales, que son solo de distrito.",
    ),
    OBS_HA_DISTRICTS: (
        "Assembly districts contested at the event: 47 at the general elections and 1 at "
        "the by-elections.",
        "Distritos de la Asamblea en disputa en el evento: 47 en los comicios generales y "
        "1 en las elecciones parciales.",
    ),
    OBS_DATA_VERSION: (
        "Version of the results dataset published by the ECSA, used by the commission's "
        "own application to fetch only what changed. A sequence number, with no "
        "arithmetic meaning.",
        "Versión del conjunto de resultados publicada por la ECSA, usada por la propia "
        "aplicación de la comisión para buscar solo lo que cambió. Número de orden, sin "
        "sentido aritmético.",
    ),
    OBS_DISCLOSURE_PORTAL: (
        "Takes the values funding2024, the portal in force from 2023, and fdarchive, the "
        "archive of the 2018 to 2023 periods. The two publish different columns: only "
        "the current portal carries the recipient entity.",
        "Toma los valores funding2024, el portal vigente a partir de 2023, y fdarchive, "
        "el archivo de los períodos de 2018 a 2023. Los dos publican columnas distintas: "
        "solo el portal vigente trae la entidad beneficiaria.",
    ),
    OBS_DISCLOSURE_GIFTS: (
        "This table covers the return, not the individual gifts. Gift level detail exists "
        "only inside the PDF attached to each return and is not extracted here.",
        "Esta tabla cubre la declaración, no las donaciones individuales. El detalle por "
        "donación existe solo dentro del PDF adjunto a cada declaración y no se extrae "
        "aquí.",
    ),
    OBS_DISCLOSURE_BAN: (
        "South Australia banned political donations from 1 July 2025, which changes which "
        "returns exist from that date.",
        "Australia Meridional prohibió las donaciones políticas a partir del 1 de julio de "
        "2025, lo que cambia qué declaraciones existen desde esa fecha.",
    ),
    OBS_DISCLOSURE_VALUE: (
        "Aggregate value of the return as published in the index. It is not a verified "
        "sum of the gifts, which appear only in the attached PDF.",
        "Valor agregado de la declaración tal como se publica en el índice. No es la suma "
        "verificada de las donaciones, que solo constan en el PDF adjunto.",
    ),
    OBS_DISCLOSURE_YEAR: (
        "Partition column, derived from the start date of the reporting period of the "
        "return.",
        "Columna de particionamiento, derivada de la fecha de inicio del período de "
        "referencia de la declaración.",
    ),
    OBS_CANDIDATE_HA_ONLY: (
        "Covers the Assembly only. The API publishes the Legislative Council at ballot "
        "group grain, with no candidate list, so no Council row is published here.",
        "Cubre solo la Asamblea. La API publica el Consejo Legislativo en grano de grupo "
        "de la boleta, sin lista de candidaturas, por lo que no se publica ninguna fila "
        "del Consejo aquí.",
    ),
    OBS_SOURCE_URL: (
        "URL of the ECSA API route that serves the results of the event.",
        "Dirección de la ruta de la API de la ECSA que sirve los resultados del evento.",
    ),
    OBS_LAST_UPDATED: (
        "Time the results were last updated by the ECSA, published in the results file "
        "itself.",
        "Momento de la última actualización de los resultados por la ECSA, publicado en el "
        "propio archivo de resultados.",
    ),
}


def validate() -> None:
    """Fail loudly on a duplicated column, an untyped quantity, an untranslated
    note or a description that breaks the house style."""
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
    observation_levels: list[tuple[str, str | None]] = field(
        default_factory=list
    )


TABLE_META: dict[str, TableMeta] = {
    "election": TableMeta(
        "Eventos eleitorais",
        "Electoral events",
        "Eventos electorales",
        "Catálogo dos cinco eventos eleitorais estaduais da Austrália Meridional "
        "cobertos pelo conjunto: os pleitos gerais de 2022 e 2026 e as três eleições "
        "suplementares de distrito realizadas entre eles. Serve de chave para todas as "
        "demais tabelas por election_id.",
        "Catalogue of the five South Australian state electoral events covered by the "
        "dataset: the 2022 and 2026 general elections and the three district "
        "by-elections held between them. Acts as the key for every other table through "
        "election_id.",
        "Catálogo de los cinco eventos electorales estatales de Australia Meridional "
        "cubiertos por el conjunto: los comicios generales de 2022 y 2026 y las tres "
        "elecciones parciales de distrito celebradas entre ellos. Sirve de clave para las "
        "demás tablas mediante election_id.",
        unique_key=["election_id"],
        ignore_null_proportion=["council_seats_contested"],
        observation_levels=[("year", "year"), ("election", "election_id")],
    ),
    "candidate": TableMeta(
        "Candidaturas",
        "Candidates",
        "Candidaturas",
        "Pessoas candidatas em cada distrito da Assembleia, com posição na cédula, "
        "partido e indicação de eleição. Cobre apenas a Assembleia: a API publica o "
        "Conselho Legislativo em grão de grupo da cédula e não traz lista de "
        "candidaturas.",
        "Candidates in each Assembly district, with ballot position, party and elected "
        "status. Covers the Assembly only: the API publishes the Legislative Council at "
        "ballot group grain and carries no candidate list.",
        "Personas candidatas en cada distrito de la Asamblea, con posición en la boleta, "
        "partido e indicación de elección. Cubre solo la Asamblea: la API publica el "
        "Consejo Legislativo en grano de grupo de la boleta y no trae lista de "
        "candidaturas.",
        unique_key=[
            "year",
            "election_id",
            "contest_id",
            "ballot_order_number",
        ],
        observation_levels=[
            ("year", "year"),
            ("election", "election_id"),
            ("district", "contest_id"),
            ("person", "ballot_name"),
        ],
    ),
    "result_district": TableMeta(
        "Resultados por disputa",
        "Results by contest",
        "Resultados por disputa",
        "Votos de cada candidatura ou grupo no total da disputa, em formato longo por "
        "count_type: primeiras preferências e as suas parcelas ordinária e por "
        "declaração, preferência entre dois candidatos e preferência entre dois "
        "partidos. Filtre sempre por count_type antes de somar.",
        "Votes for each candidate or group across the whole contest, long on count_type: "
        "first preferences and their ordinary and declaration components, two candidate "
        "preferred and two party preferred. Always filter on count_type before summing.",
        "Votos de cada candidatura o grupo en el total de la disputa, en formato largo "
        "por count_type: primeras preferencias y sus componentes ordinario y por "
        "declaración, preferencia entre dos candidatos y preferencia entre dos partidos. "
        "Filtre siempre por count_type antes de sumar.",
        unique_key=[
            "year",
            "election_id",
            "contest_id",
            "count_type",
            "ballot_order_number",
            "group_code",
        ],
        nullable_key=["ballot_order_number", "group_code"],
        ignore_null_proportion=[
            "ballot_order_number",
            "ballot_name",
            "group_code",
            "group_name",
            "state_electoral_division_id",
            "percentage",
        ],
        observation_levels=[
            ("year", "year"),
            ("election", "election_id"),
            ("district", "contest_id"),
            ("person", "ballot_name"),
        ],
    ),
    "result_voting_centre": TableMeta(
        "Resultados por local de votação",
        "Results by voting centre",
        "Resultados por local de votación",
        "Votos de cada candidatura ou grupo em cada local de votação, em formato longo "
        "por count_type, com os totais válidos e inválidos do local repetidos em cada "
        "linha. Os blocos de voto por declaração e por ausência de 2026 aparecem como "
        "locais cujo nome é o rótulo do tipo de declaração.",
        "Votes for each candidate or group at each voting centre, long on count_type, "
        "with the centre's formal and informal totals repeated on every row. The 2026 "
        "declaration and absent vote blocks appear as centres whose name is the "
        "declaration type label.",
        "Votos de cada candidatura o grupo en cada local de votación, en formato largo "
        "por count_type, con los totales válidos e inválidos del local repetidos en cada "
        "fila. Los bloques de voto por declaración y por ausencia de 2026 aparecen como "
        "locales cuyo nombre es la etiqueta del tipo de declaración.",
        unique_key=[
            "year",
            "election_id",
            "contest_id",
            "voting_centre_district_name",
            "voting_centre_name",
            "count_type",
            "ballot_order_number",
            "group_code",
        ],
        nullable_key=["ballot_order_number", "group_code"],
        ignore_null_proportion=[
            "ballot_order_number",
            "ballot_name",
            "group_code",
            "group_name",
            "state_electoral_division_id",
            "party_code",
            "party_name",
            "voting_centre_type",
            "votes_informal",
        ],
        observation_levels=[
            ("year", "year"),
            ("election", "election_id"),
            ("district", "voting_centre_district_name"),
            ("electoral_booth", "voting_centre_name"),
            ("person", "ballot_name"),
        ],
    ),
    "distribution_of_preferences": TableMeta(
        "Distribuição de preferências",
        "Distribution of preferences",
        "Distribución de preferencias",
        "Transferências de voto na distribuição de preferências da Assembleia, uma linha "
        "por rodada e candidatura receptora. A distribuição completa, com candidatura "
        "excluída e votos transferidos, existe apenas no pleito de 2026; 2022 traz "
        "somente o total progressivo por rodada e as eleições suplementares não trazem "
        "distribuição.",
        "Vote transfers in the Assembly distribution of preferences, one row per round "
        "and receiving candidate. The full distribution, with the excluded candidate and "
        "the votes transferred, exists only at the 2026 election; 2022 carries only the "
        "progressive total per round and the by-elections carry no distribution.",
        "Transferencias de voto en la distribución de preferencias de la Asamblea, una "
        "fila por ronda y candidatura receptora. La distribución completa, con "
        "candidatura excluida y votos transferidos, existe solo en el comicio de 2026; "
        "2022 trae solamente el total progresivo por ronda y las elecciones parciales no "
        "traen distribución.",
        unique_key=[
            "year",
            "election_id",
            "contest_id",
            "round_number",
            "ballot_order_number",
        ],
        ignore_null_proportion=[
            "round_type",
            "excluded_ballot_name",
            "votes_excluded",
            "votes_transferred",
            "is_excluded",
            "is_elected",
            "party_code",
            "party_name",
            "state_electoral_division_id",
        ],
        observation_levels=[
            ("year", "year"),
            ("election", "election_id"),
            ("district", "contest_id"),
            ("person", "ballot_name"),
        ],
    ),
    "voting_centre": TableMeta(
        "Locais de votação",
        "Voting centres",
        "Locales de votación",
        "Locais de votação de cada evento eleitoral, com o distrito atendido e o tipo do "
        "local. A ECSA publica os campos de endereço, coordenadas e acessibilidade "
        "vazios em todas as linhas, portanto eles não constam do conjunto.",
        "Voting centres for each electoral event, with the district served and the type "
        "of the centre. The ECSA publishes the address, coordinate and accessibility "
        "fields empty on every row, so they are not carried in the dataset.",
        "Locales de votación de cada evento electoral, con el distrito atendido y el tipo "
        "del local. La ECSA publica los campos de dirección, coordenadas y accesibilidad "
        "vacíos en todas las filas, por lo que no constan en el conjunto.",
        unique_key=[
            "year",
            "election_id",
            "district_name",
            "voting_centre_name",
        ],
        ignore_null_proportion=["state_electoral_division_id"],
        observation_levels=[
            ("year", "year"),
            ("election", "election_id"),
            ("district", "district_name"),
            ("electoral_booth", "voting_centre_name"),
        ],
    ),
    "enrolment_turnout": TableMeta(
        "Inscrições e comparecimento",
        "Enrolment and turnout",
        "Inscripciones y participación",
        "Uma linha por disputa, com pessoas inscritas, votos válidos, inválidos e totais "
        "e os percentuais correspondentes. Os contadores de locais de votação apurados "
        "são publicados pela ECSA apenas para o Conselho Legislativo.",
        "One row per contest, with enrolled electors, formal, informal and total votes "
        "and the corresponding shares. The counted voting centre tallies are published by "
        "the ECSA for the Legislative Council only.",
        "Una fila por disputa, con personas inscritas, votos válidos, inválidos y totales "
        "y los porcentajes correspondientes. Los contadores de locales de votación "
        "escrutados son publicados por la ECSA solo para el Consejo Legislativo.",
        unique_key=["year", "election_id", "contest_id"],
        ignore_null_proportion=[
            "polling_places_counted",
            "polling_places_total",
            "state_electoral_division_id",
        ],
        observation_levels=[
            ("year", "year"),
            ("election", "election_id"),
            ("district", "contest_id"),
        ],
    ),
    "disclosure_return": TableMeta(
        "Declarações de financiamento",
        "Funding disclosure returns",
        "Declaraciones de financiamiento",
        "Declarações de financiamento de campanha apresentadas à ECSA, reunindo o portal "
        "em vigor e o arquivo anterior. Cobre a declaração e o seu valor agregado, não "
        "as doações individuais, que existem apenas dentro dos PDF anexados. A Austrália "
        "Meridional proibiu doações políticas a partir de 1º de julho de 2025.",
        "Campaign funding disclosure returns lodged with the ECSA, pooling the current "
        "portal and the earlier archive. It covers the return and its aggregate value, "
        "not the individual gifts, which exist only inside the attached PDFs. South "
        "Australia banned political donations from 1 July 2025.",
        "Declaraciones de financiamiento de campaña presentadas a la ECSA, reuniendo el "
        "portal vigente y el archivo anterior. Cubre la declaración y su valor agregado, "
        "no las donaciones individuales, que existen solo dentro de los PDF adjuntos. "
        "Australia Meridional prohibió las donaciones políticas a partir del 1 de julio "
        "de 2025.",
        unique_key=["portal", "return_id"],
        ignore_null_proportion=["recipient_name", "declared_value"],
        observation_levels=[("year", "year"), ("other", "return_id")],
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
        # Only the voting centre type vocabulary is election-specific, so most
        # entries carry no temporal coverage.
        ignore_null_proportion=["cobertura_temporal"],
        observation_levels=[],
    ),
}

assert set(TABLE_META) == set(TABLES), (
    f"TABLE_META and TABLES disagree: {set(TABLE_META) ^ set(TABLES)}"
)
