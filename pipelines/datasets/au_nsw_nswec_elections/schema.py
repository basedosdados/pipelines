"""Column specification for every au_nsw_nswec_elections table.

Single source of truth: the architecture CSVs, the cleaning transform and the dbt
models are all generated from (or validated against) the ``TABLES`` mapping below.

Typing follows the house rule — INT64/FLOAT64 only where arithmetic is meaningful and
a measurement unit exists; every code, flag, ballot position, preference number and
identifier is STRING.

The table skeleton and the column vocabulary are shared with ``au_qld_ecq_elections``
(Queensland) so that a later cross-jurisdiction union is a dbt model rather than five
re-onboardings. Three concepts are new here and are absent from Queensland, which has a
unicameral parliament and no published ballot-level data:

``chamber``
    New South Wales is bicameral. The chamber is carried as a column on every contest
    rather than splitting the tables, because the two chambers share the same grain.
``group_code`` / ``group_name``
    Legislative Council ballot groups (A to U, plus ungrouped). No Queensland analogue.
``ballot_preference``
    One row per ballot paper per preference. Queensland publishes no equivalent.
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


# --------------------------------------------------------------------------------------
# Shared columns
# --------------------------------------------------------------------------------------

OBS_PARTITION = "Coluna de particionamento. Cada eleição geral estadual ocupa um único ano."
OBS_ELECTION_FK = (
    "Chave estrangeira para a tabela election, presente em todas as tabelas de "
    "resultados."
)
OBS_CONTEST_ID = (
    "Formado pelo prefixo da câmara e pelo slug do distrito, por exemplo la-albury "
    "para a Assembleia Legislativa e lc-state para o Conselho Legislativo, que é uma "
    "única disputa estadual. É único dentro de cada evento eleitoral."
)
OBS_SED = (
    "Código ASGS 2021 do ABS, atribuído por cruzamento de nomes apenas onde o nome do "
    "distrito da NSWEC corresponde exatamente à vintage 2021. As eleições de 2011, "
    "2015 e 2019 usaram limites anteriores, portanto a coluna fica nula nos distritos "
    "que não sobreviveram inalterados às redistribuições. Nula no Conselho "
    "Legislativo, que é uma circunscrição estadual única."
)
OBS_CHAMBER = (
    "Assume os valores legislative_assembly e legislative_council. Nova Gales do Sul "
    "é bicameral, portanto a câmara é uma coluna da disputa e nunca uma constante do "
    "conjunto."
)
OBS_GOV_LEVEL = "Assume o valor state em todas as linhas deste conjunto."
OBS_CONTEST_TYPE = (
    "Assume os valores state_district, uma cadeira por distrito na Assembleia "
    "Legislativa, e state_at_large, a eleição estadual única de 21 cadeiras do "
    "Conselho Legislativo."
)
OBS_VOTING_SYSTEM = (
    "Assume os valores optional_preferential, usado na Assembleia Legislativa, e "
    "single_transferable_vote, usado no Conselho Legislativo com voto acima e abaixo "
    "da linha. É definido por disputa: os dois coexistem em cada evento eleitoral."
)
OBS_RVC_DISTRICT = (
    "É o distrito onde os votos foram apurados, e não a disputa. No Conselho "
    "Legislativo, que é uma disputa estadual única, as cédulas ainda são apuradas "
    "distrito a distrito, portanto contest_id assume o valor lc-state enquanto esta "
    "coluna nomeia o distrito do local de votação."
)
OBS_BALLOT_ORDER = (
    "Número de ordem na cédula; aritmética sobre ele não tem sentido, por isso é "
    "publicado como texto."
)
OBS_BALLOT_NAME = 'Formato "SOBRENOME Prenomes", como impresso na cédula.'
OBS_GROUP_CODE = (
    "Letra do grupo na cédula do Conselho Legislativo, de A a U conforme o evento. "
    "Assume o valor UG nas candidaturas sem grupo. Nula na Assembleia Legislativa."
)
OBS_GROUP_NAME = (
    "Nome registrado do grupo, quando o grupo optou por imprimi-lo na cédula. Grupos "
    "sem nome registrado aparecem apenas pela letra. Nulo na Assembleia Legislativa."
)
OBS_YES_NO = "Assume os valores yes e no."
OBS_VOTE_TYPE = (
    "Assume os valores PP, local de votação no dia; PR, local de votação antecipada; "
    "DI, instalação declarada; e DV, voto por declaração. Os rótulos estão na tabela "
    "dicionario."
)
OBS_VOTE_SUB_TYPE = (
    "Detalha vote_type_code. Assume os valores Voting Centre, Early Voting Centre, "
    "Declared Facility, Absent, Enrolment / Provisional e Postal. Em 2015 a NSWEC "
    "usava Polling Place e Declared Institution para os dois primeiros; os rótulos "
    "históricos são preservados como publicados."
)
OBS_VENUE_TOTAL = (
    "Total do local de votação, repetido em cada linha de candidatura."
)
OBS_COUNT_STATUS = (
    "Assume o valor final de 2015 em diante. Em 2011 assume election_night, "
    "check_count e check_count_and_declaration, os três estágios que a NSWEC publicou "
    "separadamente, e post_election_night na tabela de preferência entre dois "
    "candidatos daquele evento. Filtre sempre por count_status ao comparar eventos."
)

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
    "Identificador do evento eleitoral atribuído pela NSWEC",
    "NSWEC identifier of the electoral event",
    "Identificador del evento electoral asignado por la NSWEC",
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
    "Câmara do Parlamento de Nova Gales do Sul em disputa",
    "Chamber of the New South Wales Parliament being contested",
    "Cámara del Parlamento de Nueva Gales del Sur en disputa",
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

COUNT_STATUS = C(
    "count_status",
    "STRING",
    "Estágio da apuração ao qual a linha se refere",
    "Stage of the count the row refers to",
    "Etapa del escrutinio a la que se refiere la fila",
    covered_by_dictionary="yes",
    observations=OBS_COUNT_STATUS,
)
BALLOT_ORDER_NUMBER = C(
    "ballot_order_number",
    "STRING",
    "Posição da pessoa candidata na cédula",
    "Position of the candidate on the ballot paper",
    "Posición de la persona candidata en la boleta",
    observations=OBS_BALLOT_ORDER,
)
BALLOT_NAME = C(
    "ballot_name",
    "STRING",
    "Nome da pessoa candidata tal como impresso na cédula",
    "Name of the candidate as printed on the ballot paper",
    "Nombre de la persona candidata tal como aparece impreso en la boleta",
    original_name="Candidate Ballot Name",
    observations=OBS_BALLOT_NAME,
)
PARTY_CODE = C(
    "party_code",
    "STRING",
    "Sigla do partido da pessoa candidata",
    "Abbreviation of the candidate's party",
    "Sigla del partido de la persona candidata",
    original_name="Party Acronym",
)
PARTY_NAME = C(
    "party_name",
    "STRING",
    "Nome registrado do partido da pessoa candidata",
    "Registered name of the candidate's party",
    "Nombre registrado del partido de la persona candidata",
    original_name="Party Name",
)
GROUP_CODE = C(
    "group_code",
    "STRING",
    "Letra do grupo na cédula do Conselho Legislativo",
    "Letter of the group on the Legislative Council ballot paper",
    "Letra del grupo en la boleta del Consejo Legislativo",
    covered_by_dictionary="yes",
    observations=OBS_GROUP_CODE,
)
GROUP_NAME = C(
    "group_name",
    "STRING",
    "Nome do grupo impresso na cédula do Conselho Legislativo",
    "Name of the group printed on the Legislative Council ballot paper",
    "Nombre del grupo impreso en la boleta del Consejo Legislativo",
    observations=OBS_GROUP_NAME,
)
VOTING_CENTRE_NAME = C(
    "voting_centre_name",
    "STRING",
    "Nome do local de votação ou do tipo de voto por declaração",
    "Name of the voting centre, or of the declaration vote type",
    "Nombre del local de votación o del tipo de voto por declaración",
    original_name="Venue/Declaration Name",
    observations=(
        "A NSWEC não publica identificador estável de local de votação nos "
        "resultados, portanto o nome é a única chave disponível. Nos votos por "
        "declaração o campo traz o tipo do voto, como Postal ou Absent, e não um "
        "local físico."
    ),
)
VOTE_TYPE_CODE = C(
    "vote_type_code",
    "STRING",
    "Código do tipo de voto",
    "Code for the type of vote",
    "Código del tipo de voto",
    covered_by_dictionary="yes",
    original_name="Vote Type",
    observations=OBS_VOTE_TYPE,
)
VOTE_SUB_TYPE = C(
    "vote_sub_type",
    "STRING",
    "Subtipo do voto tal como publicado pela NSWEC",
    "Sub type of the vote as published by the NSWEC",
    "Subtipo del voto tal como lo publica la NSWEC",
    covered_by_dictionary="yes",
    original_name="Vote Sub Type",
    observations=OBS_VOTE_SUB_TYPE,
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
        "Identificador do evento eleitoral atribuído pela NSWEC",
        "NSWEC identifier of the electoral event",
        "Identificador del evento electoral asignado por la NSWEC",
        observations=(
            "Chave da tabela. Corresponde à raiz do site de resultados do evento, por "
            "exemplo SG2301 para 2023 e SGE2015 para 2015: a NSWEC mudou o padrão de "
            "nomes entre 2015 e 2019."
        ),
    ),
    C(
        "election_name",
        "STRING",
        "Nome do evento eleitoral",
        "Name of the electoral event",
        "Nombre del evento electoral",
    ),
    C(
        "election_type",
        "STRING",
        "Tipo do evento eleitoral",
        "Type of the electoral event",
        "Tipo del evento electoral",
        covered_by_dictionary="yes",
        observations=(
            "Assume o valor State General em todas as linhas. As eleições "
            "suplementares estaduais, publicadas sob os códigos SB, não estão neste "
            "conjunto."
        ),
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
    ),
    C(
        "results_archive_url",
        "STRING",
        "Endereço do site de resultados publicado pela NSWEC para o evento",
        "URL of the results site published by the NSWEC for the event",
        "Dirección del sitio de resultados publicado por la NSWEC para el evento",
    ),
    C(
        "assembly_districts",
        "INT64",
        "Número de distritos disputados na Assembleia Legislativa",
        "Number of districts contested in the Legislative Assembly",
        "Número de distritos disputados en la Asamblea Legislativa",
        measurement_unit="seat",
    ),
    C(
        "council_seats_contested",
        "INT64",
        "Número de cadeiras disputadas no Conselho Legislativo",
        "Number of seats contested in the Legislative Council",
        "Número de escaños disputados en el Consejo Legislativo",
        measurement_unit="seat",
        observations=(
            "Metade das 42 cadeiras do Conselho Legislativo é renovada a cada eleição "
            "geral, para mandatos de oito anos."
        ),
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
        observations=(
            "Derivado de ballot_name: a NSWEC imprime o sobrenome inteiramente em "
            "maiúsculas, o que separa as duas partes sem ambiguidade."
        ),
    ),
    C(
        "candidate_given_names",
        "STRING",
        "Prenomes da pessoa candidata",
        "Given names of the candidate",
        "Nombres de pila de la persona candidata",
        observations=(
            "Derivado de ballot_name: a NSWEC imprime o sobrenome inteiramente em "
            "maiúsculas, o que separa as duas partes sem ambiguidade."
        ),
    ),
    PARTY_CODE,
    PARTY_NAME,
    GROUP_CODE,
    GROUP_NAME,
    C(
        "is_declared_elected",
        "STRING",
        "Indica se a pessoa candidata foi declarada eleita na disputa",
        "Whether the candidate was declared elected in the contest",
        "Indica si la persona candidata fue declarada electa en la disputa",
        covered_by_dictionary="yes",
        observations=OBS_YES_NO,
    ),
    C(
        "elected_at_count",
        "STRING",
        "Número da contagem em que a pessoa candidata foi eleita",
        "Number of the count at which the candidate was elected",
        "Número del escrutinio en el que la persona candidata fue electa",
        observations=(
            "Número de ordem; aritmética sobre ele não tem sentido. Publicado apenas "
            "para o Conselho Legislativo, cuja apuração por voto único transferível "
            "elege ao longo de centenas de contagens."
        ),
    ),
]

TABLES["enrolment_turnout"] = [
    *CONTEST_BLOCK,
    COUNT_STATUS,
    C(
        "number_to_elect",
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
    C(
        "enrolment",
        "INT64",
        "Número de pessoas inscritas na disputa",
        "Number of electors enrolled in the contest",
        "Número de personas inscritas en la disputa",
        measurement_unit="person",
        observations=(
            "Inscrições apuradas na data de fechamento do caderno eleitoral do "
            "evento, informada pela NSWEC na própria página de comparecimento."
        ),
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
        "Total de cédulas inválidas",
        "Total informal ballot papers",
        "Total de boletas inválidas",
    ),
    pct(
        "percentage_informal",
        "Cédulas inválidas como percentual do total apurado",
        "Informal ballot papers as a share of the total count",
        "Boletas inválidas como porcentaje del total escrutado",
    ),
    pct(
        "percentage_roll_counted",
        "Cédulas apuradas como percentual das pessoas inscritas",
        "Ballot papers counted as a share of enrolled electors",
        "Boletas escrutadas como porcentaje de las personas inscritas",
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
            "Assume os valores first_preference, above_the_line e "
            "two_candidate_preferred. A NSWEC não publica apuração de preferência "
            "entre dois partidos. O valor two_candidate_preferred é derivado da "
            "contagem final da distribuição de preferências, que por definição opõe "
            "as duas últimas candidaturas; above_the_line traz os votos de grupo "
            "acima da linha no Conselho Legislativo, cujas linhas não têm "
            "candidatura associada."
        ),
    ),
    BALLOT_ORDER_NUMBER,
    BALLOT_NAME,
    PARTY_CODE,
    PARTY_NAME,
    GROUP_CODE,
    GROUP_NAME,
    votes(
        "votes",
        "Votos apurados na linha",
        "Votes counted on the row",
        "Votos escrutados en la fila",
    ),
    pct(
        "percentage",
        "Votos da linha como percentual dos votos válidos da disputa",
        "Row votes as a share of the formal votes in the contest",
        "Votos de la fila como porcentaje de los votos válidos de la disputa",
    ),
    C(
        "quota_count",
        "FLOAT64",
        "Votos da linha expressos em quotas do Conselho Legislativo",
        "Row votes expressed in Legislative Council quotas",
        "Votos de la fila expresados en cuotas del Consejo Legislativo",
        measurement_unit="quota",
        observations=(
            "Publicado apenas para o Conselho Legislativo. A quota é o total de votos "
            "válidos dividido por 22, uma cadeira a mais do que as 21 em disputa."
        ),
    ),
]

RVC_DISTRICT_NAME = C(
    "voting_centre_district_name",
    "STRING",
    "Nome do distrito eleitoral estadual atendido pelo local de votação",
    "Name of the state electoral district served by the voting centre",
    "Nombre del distrito electoral estatal atendido por el local de votación",
    observations=OBS_RVC_DISTRICT,
)

TABLES["result_voting_centre"] = [
    *CONTEST_BLOCK,
    RVC_DISTRICT_NAME,
    VOTING_CENTRE_NAME,
    VOTE_TYPE_CODE,
    VOTE_SUB_TYPE,
    C(
        "count_type",
        "STRING",
        "Tipo de apuração ao qual os votos se referem",
        "Type of count the votes refer to",
        "Tipo de escrutinio al que se refieren los votos",
        covered_by_dictionary="yes",
        observations=(
            "Assume os valores first_preference, votos de primeira preferência por "
            "candidatura na Assembleia Legislativa, e above_the_line, votos de grupo "
            "acima da linha no Conselho Legislativo. A NSWEC não publica apuração "
            "abaixo da linha por candidatura no nível do local de votação."
        ),
    ),
    BALLOT_ORDER_NUMBER,
    BALLOT_NAME,
    PARTY_CODE,
    PARTY_NAME,
    GROUP_CODE,
    GROUP_NAME,
    votes(
        "votes",
        "Votos apurados na linha no local de votação",
        "Votes counted on the row at the voting centre",
        "Votos escrutados en la fila en el local de votación",
        original="Final FP Votes",
    ),
    votes(
        "votes_above_the_line",
        "Total de votos acima da linha no local de votação",
        "Total above the line votes at the voting centre",
        "Total de votos por encima de la línea en el local de votación",
        original="ATL",
        obs=(
            "Total do local de votação, repetido em cada linha. Publicado apenas para "
            "o Conselho Legislativo."
        ),
    ),
    votes(
        "votes_below_the_line",
        "Total de votos abaixo da linha no local de votação",
        "Total below the line votes at the voting centre",
        "Total de votos por debajo de la línea en el local de votación",
        original="BTL",
        obs=(
            "Total do local de votação, repetido em cada linha. Publicado apenas para "
            "o Conselho Legislativo."
        ),
    ),
    votes(
        "votes_formal",
        "Total de votos válidos no local de votação",
        "Total formal votes at the voting centre",
        "Total de votos válidos en el local de votación",
        original="Formal",
        obs=OBS_VENUE_TOTAL,
    ),
    votes(
        "votes_informal",
        "Total de cédulas inválidas no local de votação",
        "Total informal ballot papers at the voting centre",
        "Total de boletas inválidas en el local de votación",
        original="Informal",
        obs=OBS_VENUE_TOTAL,
    ),
    votes(
        "votes_total",
        "Total de cédulas apuradas no local de votação",
        "Total ballot papers counted at the voting centre",
        "Total de boletas escrutadas en el local de votación",
        original="Total",
        obs=OBS_VENUE_TOTAL,
    ),
]

TABLES["distribution_of_preferences"] = [
    *CONTEST_BLOCK,
    C(
        "distribution_number",
        "STRING",
        "Número da contagem na distribuição de preferências",
        "Number of the count in the distribution of preferences",
        "Número del escrutinio en la distribución de preferencias",
        observations=(
            "Número de ordem; aritmética sobre ele não tem sentido. A contagem 1 é a "
            "apuração de primeira preferência, sem candidatura excluída."
        ),
    ),
    C(
        "excluded_ballot_name",
        "STRING",
        "Nome na cédula da pessoa candidata excluída na contagem",
        "Ballot name of the candidate excluded at this count",
        "Nombre en la boleta de la persona candidata excluida en el escrutinio",
        observations=(
            "Nulo na contagem 1, que é a apuração de primeira preferência e não exclui "
            "ninguém."
        ),
    ),
    C(
        "excluded_party_code",
        "STRING",
        "Sigla do partido da pessoa candidata excluída na contagem",
        "Party abbreviation of the candidate excluded at this count",
        "Sigla del partido de la persona candidata excluida en el escrutinio",
        observations=(
            "Nulo na contagem 1, que é a apuração de primeira preferência e não exclui "
            "ninguém."
        ),
    ),
    BALLOT_NAME,
    PARTY_CODE,
    votes(
        "votes_transferred",
        "Votos transferidos para a pessoa candidata na contagem",
        "Votes transferred to the candidate at this count",
        "Votos transferidos a la persona candidata en el escrutinio",
        original="VotesDistributed",
        obs=(
            "Na contagem 1 traz os votos de primeira preferência, que não são uma "
            "transferência."
        ),
    ),
    votes(
        "votes_progressive_total",
        "Total acumulado da pessoa candidata após a contagem",
        "Running total for the candidate after this count",
        "Total acumulado de la persona candidata tras el escrutinio",
        original="ProgressiveTotals",
    ),
    C(
        "is_excluded",
        "STRING",
        "Indica se a pessoa candidata foi excluída nesta contagem",
        "Whether the candidate was excluded at this count",
        "Indica si la persona candidata fue excluida en este escrutinio",
        covered_by_dictionary="yes",
        observations=OBS_YES_NO,
    ),
    C(
        "is_elected",
        "STRING",
        "Indica se a pessoa candidata estava em posição de eleita após a contagem",
        "Whether the candidate stood elected after this count",
        "Indica si la persona candidata estaba en posición de electa tras el escrutinio",
        covered_by_dictionary="yes",
        observations=OBS_YES_NO,
    ),
    votes(
        "votes_in_count",
        "Total de votos que permanecem na apuração após a contagem",
        "Total votes remaining in the count after this count",
        "Total de votos que permanecen en el escrutinio tras este escrutinio",
        original="Total Votes in Count",
        obs="Total da contagem, repetido em cada linha de candidatura.",
    ),
    votes(
        "votes_exhausted",
        "Votos esgotados nesta contagem, sem preferência seguinte válida",
        "Votes exhausted at this count, with no valid next preference",
        "Votos agotados en este escrutinio, sin preferencia siguiente válida",
        original="Exhausted Votes",
        obs="Total da contagem, repetido em cada linha de candidatura.",
    ),
    votes(
        "votes_exhausted_total",
        "Total acumulado de votos esgotados após a contagem",
        "Running total of exhausted votes after this count",
        "Total acumulado de votos agotados tras el escrutinio",
        obs="Total da contagem, repetido em cada linha de candidatura.",
    ),
    votes(
        "absolute_majority",
        "Maioria absoluta exigida após a contagem",
        "Absolute majority required after this count",
        "Mayoría absoluta exigida tras el escrutinio",
        original="Absolute Majority",
        obs="Total da contagem, repetido em cada linha de candidatura.",
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
        observations=(
            "O grão da tabela é uma linha por evento eleitoral, distrito atendido e "
            "local de votação: um local compartilhado entre distritos aparece uma vez "
            "por distrito."
        ),
    ),
    VOTING_CENTRE_NAME,
    VOTE_TYPE_CODE,
    VOTE_SUB_TYPE,
    C(
        "building_name",
        "STRING",
        "Nome do estabelecimento que sedia o local de votação",
        "Name of the premises hosting the voting centre",
        "Nombre del establecimiento que alberga el local de votación",
        observations=(
            "Publicado apenas no registro de locais de 2023. Nulo nos eventos "
            "anteriores, para os quais a NSWEC não publica registro de locais."
        ),
    ),
    C(
        "address",
        "STRING",
        "Endereço do local de votação",
        "Street address of the voting centre",
        "Dirección del local de votación",
        observations=(
            "Publicado apenas no registro de locais de 2023. Nulo nos eventos "
            "anteriores, para os quais a NSWEC não publica registro de locais."
        ),
    ),
    C(
        "locality",
        "STRING",
        "Localidade ou subúrbio do local de votação",
        "Locality or suburb of the voting centre",
        "Localidad o barrio del local de votación",
        observations=(
            "Publicado apenas no registro de locais de 2023. Nulo nos eventos "
            "anteriores, para os quais a NSWEC não publica registro de locais."
        ),
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
        "latitude",
        "FLOAT64",
        "Latitude do local de votação",
        "Latitude of the voting centre",
        "Latitud del local de votación",
        measurement_unit="degree",
        observations=(
            "Publicado apenas no registro de locais de 2023. Nulo nos eventos "
            "anteriores, para os quais a NSWEC não publica registro de locais."
        ),
    ),
    C(
        "longitude",
        "FLOAT64",
        "Longitude do local de votação",
        "Longitude of the voting centre",
        "Longitud del local de votación",
        measurement_unit="degree",
        observations=(
            "Publicado apenas no registro de locais de 2023. Nulo nos eventos "
            "anteriores, para os quais a NSWEC não publica registro de locais."
        ),
    ),
    C(
        "is_in_results",
        "STRING",
        "Indica se o local de votação aparece nos resultados apurados do evento",
        "Whether the voting centre appears in the counted results of the event",
        "Indica si el local de votación aparece en los resultados escrutados del evento",
        covered_by_dictionary="yes",
        observations=OBS_YES_NO,
    ),
]

TABLES["ballot_preference"] = [
    YEAR,
    ELECTION_ID,
    CONTEST_ID,
    SED_ID,
    CHAMBER,
    DISTRICT_NAME,
    VOTING_CENTRE_NAME,
    C(
        "ballot_paper_id",
        "STRING",
        "Identificador da cédula atribuído pela NSWEC na digitalização",
        "Identifier of the ballot paper assigned by the NSWEC at data capture",
        "Identificador de la boleta asignado por la NSWEC en la digitalización",
        original_name="BPNumber",
        observations=(
            "Único apenas dentro da combinação de distrito e local de votação, nunca "
            "isoladamente. Cédulas inválidas em branco recebem identificadores da "
            "forma BLANK_<lote>_<sequência>."
        ),
    ),
    C(
        "formality",
        "STRING",
        "Formalidade da cédula",
        "Formality of the ballot paper",
        "Formalidad de la boleta",
        covered_by_dictionary="yes",
        original_name="Formality",
        observations=(
            "Assume os valores Formal e Informal. Cada cédula inválida ocupa uma única "
            "linha, com candidatura e preferências nulas."
        ),
    ),
    BALLOT_NAME,
    PARTY_CODE,
    C(
        "preference_number",
        "STRING",
        "Preferência tal como marcada pela pessoa eleitora na cédula",
        "Preference as marked by the elector on the ballot paper",
        "Preferencia tal como la marcó la persona electora en la boleta",
        original_name="PrefMarking",
        observations=(
            "Número de ordem, publicado como texto porque a coluna não é numérica: as "
            "cláusulas de salvaguarda de Nova Gales do Sul aceitam um tique ou uma "
            "cruz como primeira preferência, de modo que a coluna também assume os "
            "valores /, x e X."
        ),
    ),
    C(
        "preference_counted_number",
        "STRING",
        "Preferência efetivamente considerada na apuração",
        "Preference actually used in the count",
        "Preferencia efectivamente considerada en el escrutinio",
        original_name="PrefCounted",
        observations=(
            "Difere de preference_number quando a sequência marcada tem falhas: as "
            "preferências posteriores à falha ficam nulas porque não entram na "
            "apuração."
        ),
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

# 2011 is the earliest event on the NSWEC past virtual tally room; the upper bound
# leaves room for the 2027 and 2031 general elections without a schema change.
PARTITION_RANGE = {"start": 2011, "end": 2035, "interval": 1}


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
    "Coluna de particionamento. Cada eleição geral estadual ocupa um único ano.": (
        "Partition column. Each state general election occupies a single year.",
        "Columna de particionamiento. Cada elección general estatal ocupa un único año.",
    ),
    "Chave da tabela. Corresponde à raiz do site de resultados do evento, por exemplo SG2301 para 2023 e SGE2015 para 2015: a NSWEC mudou o padrão de nomes entre 2015 e 2019.": (
        "Key of the table. It matches the root of the event's results site, for example SG2301 for 2023 and SGE2015 for 2015: the NSWEC changed its naming pattern between 2015 and 2019.",
        "Clave de la tabla. Corresponde a la raíz del sitio de resultados del evento, por ejemplo SG2301 para 2023 y SGE2015 para 2015: la NSWEC cambió el patrón de nombres entre 2015 y 2019.",
    ),
    "Assume o valor State General em todas as linhas. As eleições suplementares estaduais, publicadas sob os códigos SB, não estão neste conjunto.": (
        "Takes the value State General on every row. State by-elections, published under SB codes, are not in this dataset.",
        "Toma el valor State General en todas las filas. Las elecciones parciales estatales, publicadas bajo los códigos SB, no están en este conjunto.",
    ),
    "Assume o valor state em todas as linhas deste conjunto.": (
        "Takes the value state on every row of this dataset.",
        "Toma el valor state en todas las filas de este conjunto.",
    ),
    "Metade das 42 cadeiras do Conselho Legislativo é renovada a cada eleição geral, para mandatos de oito anos.": (
        "Half of the 42 Legislative Council seats is renewed at each general election, for eight year terms.",
        "La mitad de los 42 escaños del Consejo Legislativo se renueva en cada elección general, para mandatos de ocho años.",
    ),
    "Chave estrangeira para a tabela election, presente em todas as tabelas de resultados.": (
        "Foreign key to the election table, present in every results table.",
        "Clave foránea hacia la tabla election, presente en todas las tablas de resultados.",
    ),
    "Formado pelo prefixo da câmara e pelo slug do distrito, por exemplo la-albury para a Assembleia Legislativa e lc-state para o Conselho Legislativo, que é uma única disputa estadual. É único dentro de cada evento eleitoral.": (
        "Built from the chamber prefix and the district slug, for example la-albury for the Legislative Assembly and lc-state for the Legislative Council, which is a single statewide contest. Unique within each electoral event.",
        "Formado por el prefijo de la cámara y el slug del distrito, por ejemplo la-albury para la Asamblea Legislativa y lc-state para el Consejo Legislativo, que es una única disputa estatal. Es único dentro de cada evento electoral.",
    ),
    "Código ASGS 2021 do ABS, atribuído por cruzamento de nomes apenas onde o nome do distrito da NSWEC corresponde exatamente à vintage 2021. As eleições de 2011, 2015 e 2019 usaram limites anteriores, portanto a coluna fica nula nos distritos que não sobreviveram inalterados às redistribuições. Nula no Conselho Legislativo, que é uma circunscrição estadual única.": (
        "ABS ASGS 2021 code, assigned by name crosswalk only where the NSWEC district name matches the 2021 vintage exactly. The 2011, 2015 and 2019 elections used earlier boundaries, so the column is null for districts that did not survive the redistributions unchanged. Null for the Legislative Council, which is a single statewide electorate.",
        "Código ASGS 2021 del ABS, asignado por cruce de nombres solo donde el nombre del distrito de la NSWEC corresponde exactamente a la vintage 2021. Las elecciones de 2011, 2015 y 2019 usaron límites anteriores, por lo que la columna queda nula en los distritos que no sobrevivieron sin cambios a las redistribuciones. Nula en el Consejo Legislativo, que es una circunscripción estatal única.",
    ),
    "Assume os valores legislative_assembly e legislative_council. Nova Gales do Sul é bicameral, portanto a câmara é uma coluna da disputa e nunca uma constante do conjunto.": (
        "Takes the values legislative_assembly and legislative_council. New South Wales is bicameral, so the chamber is a column of the contest and never a constant of the dataset.",
        "Toma los valores legislative_assembly y legislative_council. Nueva Gales del Sur es bicameral, por lo que la cámara es una columna de la disputa y nunca una constante del conjunto.",
    ),
    "Assume os valores state_district, uma cadeira por distrito na Assembleia Legislativa, e state_at_large, a eleição estadual única de 21 cadeiras do Conselho Legislativo.": (
        "Takes the values state_district, one seat per district in the Legislative Assembly, and state_at_large, the single statewide 21 seat election of the Legislative Council.",
        "Toma los valores state_district, un escaño por distrito en la Asamblea Legislativa, y state_at_large, la elección estatal única de 21 escaños del Consejo Legislativo.",
    ),
    "Assume os valores optional_preferential, usado na Assembleia Legislativa, e single_transferable_vote, usado no Conselho Legislativo com voto acima e abaixo da linha. É definido por disputa: os dois coexistem em cada evento eleitoral.": (
        "Takes the values optional_preferential, used in the Legislative Assembly, and single_transferable_vote, used in the Legislative Council with above the line and below the line voting. Set per contest: the two coexist within each electoral event.",
        "Toma los valores optional_preferential, usado en la Asamblea Legislativa, y single_transferable_vote, usado en el Consejo Legislativo con voto por encima y por debajo de la línea. Se define por disputa: los dos coexisten en cada evento electoral.",
    ),
    "É o distrito onde os votos foram apurados, e não a disputa. No Conselho Legislativo, que é uma disputa estadual única, as cédulas ainda são apuradas distrito a distrito, portanto contest_id assume o valor lc-state enquanto esta coluna nomeia o distrito do local de votação.": (
        "The district where the votes were counted, not the contest. In the Legislative Council, which is a single statewide contest, ballot papers are still counted district by district, so contest_id takes the value lc-state while this column names the district of the voting centre.",
        "Es el distrito donde se escrutaron los votos, y no la disputa. En el Consejo Legislativo, que es una disputa estatal única, las boletas se escrutan igualmente distrito a distrito, por lo que contest_id toma el valor lc-state mientras esta columna nombra el distrito del local de votación.",
    ),
    "Número de ordem na cédula; aritmética sobre ele não tem sentido, por isso é publicado como texto.": (
        "Ballot order number; arithmetic on it is meaningless, so it is published as text.",
        "Número de orden en la boleta; la aritmética sobre él no tiene sentido, por eso se publica como texto.",
    ),
    'Formato "SOBRENOME Prenomes", como impresso na cédula.': (
        'Format "SURNAME Given names", as printed on the ballot paper.',
        'Formato "APELLIDO Nombres", tal como aparece impreso en la boleta.',
    ),
    "Derivado de ballot_name: a NSWEC imprime o sobrenome inteiramente em maiúsculas, o que separa as duas partes sem ambiguidade.": (
        "Derived from ballot_name: the NSWEC prints the surname entirely in upper case, which separates the two parts unambiguously.",
        "Derivado de ballot_name: la NSWEC imprime el apellido enteramente en mayúsculas, lo que separa las dos partes sin ambigüedad.",
    ),
    "Letra do grupo na cédula do Conselho Legislativo, de A a U conforme o evento. Assume o valor UG nas candidaturas sem grupo. Nula na Assembleia Legislativa.": (
        "Letter of the group on the Legislative Council ballot paper, from A to U depending on the event. Takes the value UG for ungrouped candidates. Null in the Legislative Assembly.",
        "Letra del grupo en la boleta del Consejo Legislativo, de A a U según el evento. Toma el valor UG en las candidaturas sin grupo. Nula en la Asamblea Legislativa.",
    ),
    "Nome registrado do grupo, quando o grupo optou por imprimi-lo na cédula. Grupos sem nome registrado aparecem apenas pela letra. Nulo na Assembleia Legislativa.": (
        "Registered name of the group, where the group chose to print it on the ballot paper. Groups with no registered name appear by letter only. Null in the Legislative Assembly.",
        "Nombre registrado del grupo, cuando el grupo optó por imprimirlo en la boleta. Los grupos sin nombre registrado aparecen solo por la letra. Nulo en la Asamblea Legislativa.",
    ),
    "Assume os valores yes e no.": (
        "Takes the values yes and no.",
        "Toma los valores yes y no.",
    ),
    "Número de ordem; aritmética sobre ele não tem sentido. Publicado apenas para o Conselho Legislativo, cuja apuração por voto único transferível elege ao longo de centenas de contagens.": (
        "Sequence number; arithmetic on it is meaningless. Published only for the Legislative Council, whose single transferable vote count elects across hundreds of counts.",
        "Número de orden; la aritmética sobre él no tiene sentido. Publicado solo para el Consejo Legislativo, cuyo escrutinio por voto único transferible elige a lo largo de cientos de escrutinios.",
    ),
    "Assume o valor final de 2015 em diante. Em 2011 assume election_night, check_count e check_count_and_declaration, os três estágios que a NSWEC publicou separadamente, e post_election_night na tabela de preferência entre dois candidatos daquele evento. Filtre sempre por count_status ao comparar eventos.": (
        "Takes the value final from 2015 onwards. For 2011 it takes election_night, check_count and check_count_and_declaration, the three stages the NSWEC published separately, and post_election_night on that event's two candidate preferred table. Always filter on count_status when comparing events.",
        "Toma el valor final de 2015 en adelante. En 2011 toma election_night, check_count y check_count_and_declaration, las tres etapas que la NSWEC publicó por separado, y post_election_night en la tabla de preferencia entre dos candidatos de ese evento. Filtre siempre por count_status al comparar eventos.",
    ),
    "Inscrições apuradas na data de fechamento do caderno eleitoral do evento, informada pela NSWEC na própria página de comparecimento.": (
        "Enrolment as at the close of the roll for the event, stated by the NSWEC on the turnout page itself.",
        "Inscripciones contabilizadas en la fecha de cierre del padrón electoral del evento, informada por la NSWEC en la propia página de participación.",
    ),
    "Assume os valores first_preference, above_the_line e two_candidate_preferred. A NSWEC não publica apuração de preferência entre dois partidos. O valor two_candidate_preferred é derivado da contagem final da distribuição de preferências, que por definição opõe as duas últimas candidaturas; above_the_line traz os votos de grupo acima da linha no Conselho Legislativo, cujas linhas não têm candidatura associada.": (
        "Takes the values first_preference, above_the_line and two_candidate_preferred. The NSWEC publishes no two party preferred count. The value two_candidate_preferred is derived from the final count of the distribution of preferences, which by definition pits the last two candidates against each other; above_the_line carries the group ticket votes of the Legislative Council, whose rows have no associated candidate.",
        "Toma los valores first_preference, above_the_line y two_candidate_preferred. La NSWEC no publica escrutinio de preferencia entre dos partidos. El valor two_candidate_preferred se deriva del escrutinio final de la distribución de preferencias, que por definición enfrenta a las dos últimas candidaturas; above_the_line trae los votos de grupo por encima de la línea en el Consejo Legislativo, cuyas filas no tienen candidatura asociada.",
    ),
    "Publicado apenas para o Conselho Legislativo. A quota é o total de votos válidos dividido por 22, uma cadeira a mais do que as 21 em disputa.": (
        "Published only for the Legislative Council. The quota is the total formal vote divided by 22, one more than the 21 seats contested.",
        "Publicado solo para el Consejo Legislativo. La cuota es el total de votos válidos dividido por 22, un escaño más que los 21 en disputa.",
    ),
    "A NSWEC não publica identificador estável de local de votação nos resultados, portanto o nome é a única chave disponível. Nos votos por declaração o campo traz o tipo do voto, como Postal ou Absent, e não um local físico.": (
        "The NSWEC publishes no stable voting centre identifier in the results, so the name is the only key available. On declaration votes the field carries the vote type, such as Postal or Absent, rather than a physical venue.",
        "La NSWEC no publica identificador estable de local de votación en los resultados, por lo que el nombre es la única clave disponible. En los votos por declaración el campo trae el tipo de voto, como Postal o Absent, y no un local físico.",
    ),
    "Assume os valores PP, local de votação no dia; PR, local de votação antecipada; DI, instalação declarada; e DV, voto por declaração. Os rótulos estão na tabela dicionario.": (
        "Takes the values PP, election day voting centre; PR, early voting centre; DI, declared facility; and DV, declaration vote. The labels live in the dicionario table.",
        "Toma los valores PP, local de votación del día; PR, local de votación anticipada; DI, instalación declarada; y DV, voto por declaración. Las etiquetas están en la tabla dicionario.",
    ),
    "Detalha vote_type_code. Assume os valores Voting Centre, Early Voting Centre, Declared Facility, Absent, Enrolment / Provisional e Postal. Em 2015 a NSWEC usava Polling Place e Declared Institution para os dois primeiros; os rótulos históricos são preservados como publicados.": (
        "Refines vote_type_code. Takes the values Voting Centre, Early Voting Centre, Declared Facility, Absent, Enrolment / Provisional and Postal. In 2015 the NSWEC used Polling Place and Declared Institution for the first two; the historical labels are preserved as published.",
        "Detalla vote_type_code. Toma los valores Voting Centre, Early Voting Centre, Declared Facility, Absent, Enrolment / Provisional y Postal. En 2015 la NSWEC usaba Polling Place y Declared Institution para los dos primeros; las etiquetas históricas se conservan tal como se publicaron.",
    ),
    "Assume os valores first_preference, votos de primeira preferência por candidatura na Assembleia Legislativa, e above_the_line, votos de grupo acima da linha no Conselho Legislativo. A NSWEC não publica apuração abaixo da linha por candidatura no nível do local de votação.": (
        "Takes the values first_preference, first preference votes per candidate in the Legislative Assembly, and above_the_line, group ticket votes in the Legislative Council. The NSWEC publishes no below the line count per candidate at voting centre level.",
        "Toma los valores first_preference, votos de primera preferencia por candidatura en la Asamblea Legislativa, y above_the_line, votos de grupo por encima de la línea en el Consejo Legislativo. La NSWEC no publica escrutinio por debajo de la línea por candidatura a nivel de local de votación.",
    ),
    "Total do local de votação, repetido em cada linha. Publicado apenas para o Conselho Legislativo.": (
        "Voting centre total, repeated on every row. Published only for the Legislative Council.",
        "Total del local de votación, repetido en cada fila. Publicado solo para el Consejo Legislativo.",
    ),
    "Total do local de votação, repetido em cada linha de candidatura.": (
        "Voting centre total, repeated on every candidate row.",
        "Total del local de votación, repetido en cada fila de candidatura.",
    ),
    "Número de ordem; aritmética sobre ele não tem sentido. A contagem 1 é a apuração de primeira preferência, sem candidatura excluída.": (
        "Sequence number; arithmetic on it is meaningless. Count 1 is the first preference count, with no excluded candidate.",
        "Número de orden; la aritmética sobre él no tiene sentido. El escrutinio 1 es el de primera preferencia, sin candidatura excluida.",
    ),
    "Nulo na contagem 1, que é a apuração de primeira preferência e não exclui ninguém.": (
        "Null at count 1, which is the first preference count and excludes nobody.",
        "Nulo en el escrutinio 1, que es el de primera preferencia y no excluye a nadie.",
    ),
    "Na contagem 1 traz os votos de primeira preferência, que não são uma transferência.": (
        "At count 1 it carries the first preference votes, which are not a transfer.",
        "En el escrutinio 1 trae los votos de primera preferencia, que no son una transferencia.",
    ),
    "Total da contagem, repetido em cada linha de candidatura.": (
        "Count total, repeated on every candidate row.",
        "Total del escrutinio, repetido en cada fila de candidatura.",
    ),
    "O grão da tabela é uma linha por evento eleitoral, distrito atendido e local de votação: um local compartilhado entre distritos aparece uma vez por distrito.": (
        "The grain of the table is one row per electoral event, district served and voting centre: a centre shared between districts appears once per district.",
        "El grano de la tabla es una fila por evento electoral, distrito atendido y local de votación: un local compartido entre distritos aparece una vez por distrito.",
    ),
    "Publicado apenas no registro de locais de 2023. Nulo nos eventos anteriores, para os quais a NSWEC não publica registro de locais.": (
        "Published only in the 2023 voting centre registry. Null for earlier events, for which the NSWEC publishes no venue registry.",
        "Publicado solo en el registro de locales de 2023. Nulo en los eventos anteriores, para los cuales la NSWEC no publica registro de locales.",
    ),
    "Código postal, não uma quantidade; publicado como texto.": (
        "A postcode, not a quantity; published as text.",
        "Código postal, no una cantidad; publicado como texto.",
    ),
    "Único apenas dentro da combinação de distrito e local de votação, nunca isoladamente. Cédulas inválidas em branco recebem identificadores da forma BLANK_<lote>_<sequência>.": (
        "Unique only within the combination of district and voting centre, never on its own. Blank informal ballot papers receive identifiers of the form BLANK_<batch>_<sequence>.",
        "Único solo dentro de la combinación de distrito y local de votación, nunca por sí solo. Las boletas inválidas en blanco reciben identificadores de la forma BLANK_<lote>_<secuencia>.",
    ),
    "Assume os valores Formal e Informal. Cada cédula inválida ocupa uma única linha, com candidatura e preferências nulas.": (
        "Takes the values Formal and Informal. Each informal ballot paper occupies a single row, with null candidate and preferences.",
        "Toma los valores Formal e Informal. Cada boleta inválida ocupa una única fila, con candidatura y preferencias nulas.",
    ),
    "Número de ordem, publicado como texto porque a coluna não é numérica: as cláusulas de salvaguarda de Nova Gales do Sul aceitam um tique ou uma cruz como primeira preferência, de modo que a coluna também assume os valores /, x e X.": (
        "Sequence number, published as text because the column is not numeric: the New South Wales savings provisions accept a tick or a cross as a first preference, so the column also takes the values /, x and X.",
        "Número de orden, publicado como texto porque la columna no es numérica: las cláusulas de salvaguarda de Nueva Gales del Sur aceptan una marca o una cruz como primera preferencia, de modo que la columna también toma los valores /, x y X.",
    ),
    "Difere de preference_number quando a sequência marcada tem falhas: as preferências posteriores à falha ficam nulas porque não entram na apuração.": (
        "Differs from preference_number when the marked sequence has gaps: preferences after the gap are null because they do not enter the count.",
        "Difiere de preference_number cuando la secuencia marcada tiene fallas: las preferencias posteriores a la falla quedan nulas porque no entran en el escrutinio.",
    ),
}


def validate() -> None:
    """Fail loudly on a duplicated column, a typed quantity with no measurement unit,
    a malformed description, or an observation note with no EN/ES translation."""
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
            if c.observations:
                if c.observations not in OBSERVATION_TRANSLATIONS:
                    raise ValueError(
                        f"{table}.{c.name}: observation note has no EN/ES translation"
                    )
                en, es = OBSERVATION_TRANSLATIONS[c.observations]
                # A translation identical to the source is a copy-paste, not a
                # translation. Cheap gate; catches the failure mode that matters.
                if en == c.observations or es == c.observations or en == es:
                    raise ValueError(
                        f"{table}.{c.name}: observation translation is not distinct"
                    )
    for table in TABLES:
        if table not in TABLE_META:
            raise ValueError(f"{table}: no TableMeta")
        meta = TABLE_META[table]
        known = set(column_names(table))
        for key in (
            *meta.unique_key,
            *meta.ignore_null_proportion,
            *meta.nullable_key,
        ):
            if key not in known:
                raise ValueError(
                    f"{table}: TableMeta references unknown column {key}"
                )


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
    # Observation levels: entity slug -> the columns that identify that level.
    observation_levels: dict[str, list[str]] = field(default_factory=dict)


TABLE_META: dict[str, TableMeta] = {
    "election": TableMeta(
        "Eventos eleitorais",
        "Electoral events",
        "Eventos electorales",
        "Catálogo das eleições gerais estaduais de Nova Gales do Sul cobertas pelo "
        "conjunto, com data da votação, número de distritos da Assembleia Legislativa "
        "e número de cadeiras do Conselho Legislativo em disputa. Serve de chave para "
        "todas as demais tabelas por election_id.",
        "Catalogue of the New South Wales state general elections covered by the "
        "dataset, with the date of the poll, the number of Legislative Assembly "
        "districts and the number of Legislative Council seats contested. Acts as the "
        "key for every other table through election_id.",
        "Catálogo de las elecciones generales estatales de Nueva Gales del Sur "
        "cubiertas por el conjunto, con la fecha de la votación, el número de "
        "distritos de la Asamblea Legislativa y el número de escaños del Consejo "
        "Legislativo en disputa. Sirve de clave para las demás tablas mediante "
        "election_id.",
        unique_key=["election_id"],
        observation_levels={"year": ["year"]},
    ),
    "candidate": TableMeta(
        "Candidaturas",
        "Candidates",
        "Candidaturas",
        "Pessoas candidatas em cada disputa, com posição na cédula, partido, grupo do "
        "Conselho Legislativo e indicação de eleição. Reúne as duas câmaras, "
        "distinguidas por chamber.",
        "Candidates in each contest, with ballot position, party, Legislative Council "
        "group and declared elected status. Pools both chambers, told apart by "
        "chamber.",
        "Personas candidatas en cada disputa, con posición en la boleta, partido, "
        "grupo del Consejo Legislativo e indicación de elección. Reúne las dos "
        "cámaras, distinguidas por chamber.",
        unique_key=[
            "year",
            "election_id",
            "contest_id",
            "ballot_name",
            "party_code",
        ],
        ignore_null_proportion=[
            "state_electoral_division_id",
            "group_code",
            "group_name",
            "elected_at_count",
            "ballot_order_number",
            "party_code",
            "party_name",
            "candidate_given_names",
        ],
        nullable_key=["party_code"],
        observation_levels={
            "year": ["year"],
            "district": ["contest_id"],
            "candidate": ["ballot_name"],
        },
    ),
    "enrolment_turnout": TableMeta(
        "Inscrições e comparecimento",
        "Enrolment and turnout",
        "Inscripciones y participación",
        "Uma linha por disputa, com pessoas inscritas, cédulas apuradas, votos válidos "
        "e inválidos e os percentuais correspondentes. O comparecimento é publicado "
        "por distrito na Assembleia Legislativa e uma única vez, no estado, para o "
        "Conselho Legislativo.",
        "One row per contest, with enrolled electors, ballot papers counted, formal "
        "and informal votes and the corresponding shares. Turnout is published per "
        "district in the Legislative Assembly and once, statewide, for the "
        "Legislative Council.",
        "Una fila por disputa, con personas inscritas, boletas escrutadas, votos "
        "válidos e inválidos y los porcentajes correspondientes. La participación se "
        "publica por distrito en la Asamblea Legislativa y una única vez, a nivel "
        "estatal, para el Consejo Legislativo.",
        unique_key=["year", "election_id", "contest_id", "count_status"],
        ignore_null_proportion=["state_electoral_division_id"],
        observation_levels={"year": ["year"], "district": ["contest_id"]},
    ),
    "result_district": TableMeta(
        "Resultados por disputa",
        "Results by contest",
        "Resultados por disputa",
        "Votos apurados no total da disputa, em formato longo por count_type: primeira "
        "preferência, voto de grupo acima da linha e preferência entre dois "
        "candidatos. A NSWEC não publica apuração de preferência entre dois partidos.",
        "Votes counted across the whole contest, in long form by count_type: first "
        "preference, above the line group vote and two candidate preferred. The NSWEC "
        "publishes no two party preferred count.",
        "Votos escrutados en el total de la disputa, en formato largo por count_type: "
        "primera preferencia, voto de grupo por encima de la línea y preferencia entre "
        "dos candidatos. La NSWEC no publica escrutinio de preferencia entre dos "
        "partidos.",
        unique_key=[
            "year",
            "election_id",
            "contest_id",
            "count_status",
            "count_type",
            "ballot_name",
            "party_code",
            "group_code",
        ],
        ignore_null_proportion=[
            "state_electoral_division_id",
            "group_code",
            "group_name",
            "quota_count",
            "ballot_order_number",
            "ballot_name",
            "party_code",
            "party_name",
            "percentage",
        ],
        nullable_key=["ballot_name", "group_code", "party_code"],
        observation_levels={
            "year": ["year"],
            "district": ["contest_id"],
            "candidate": ["ballot_name"],
        },
    ),
    "result_voting_centre": TableMeta(
        "Resultados por local de votação",
        "Results by voting centre",
        "Resultados por local de votación",
        "Votos apurados em cada local de votação e em cada tipo de voto por "
        "declaração, com os totais do local repetidos em cada linha. Cobre a primeira "
        "preferência por candidatura na Assembleia Legislativa e o voto de grupo acima "
        "da linha no Conselho Legislativo.",
        "Votes counted at each voting centre and for each declaration vote type, with "
        "the voting centre totals repeated on every row. Covers first preference per "
        "candidate in the Legislative Assembly and the above the line group vote in "
        "the Legislative Council.",
        "Votos escrutados en cada local de votación y en cada tipo de voto por "
        "declaración, con los totales del local repetidos en cada fila. Cubre la "
        "primera preferencia por candidatura en la Asamblea Legislativa y el voto de "
        "grupo por encima de la línea en el Consejo Legislativo.",
        unique_key=[
            "year",
            "election_id",
            "contest_id",
            "count_type",
            "vote_sub_type",
            "voting_centre_district_name",
            "voting_centre_name",
            "ballot_name",
            "group_code",
        ],
        ignore_null_proportion=[
            "state_electoral_division_id",
            "group_code",
            "group_name",
            "ballot_order_number",
            "ballot_name",
            "party_code",
            "party_name",
            "votes_above_the_line",
            "votes_below_the_line",
        ],
        nullable_key=["ballot_name", "group_code"],
        observation_levels={
            "year": ["year"],
            "district": ["contest_id"],
            "voting_centre": ["voting_centre_name"],
        },
    ),
    "distribution_of_preferences": TableMeta(
        "Distribuição de preferências",
        "Distribution of preferences",
        "Distribución de preferencias",
        "Transferências de voto na distribuição de preferências da Assembleia "
        "Legislativa: uma linha por contagem e por candidatura, com os votos "
        "transferidos, o total acumulado e os totais da contagem. A contagem final "
        "define o resultado de preferência entre dois candidatos.",
        "Vote transfers in the Legislative Assembly distribution of preferences: one "
        "row per count and candidate, with the votes transferred, the running total "
        "and the count totals. The final count defines the two candidate preferred "
        "result.",
        "Transferencias de voto en la distribución de preferencias de la Asamblea "
        "Legislativa: una fila por escrutinio y por candidatura, con los votos "
        "transferidos, el total acumulado y los totales del escrutinio. El escrutinio "
        "final define el resultado de preferencia entre dos candidatos.",
        unique_key=[
            "year",
            "election_id",
            "contest_id",
            "distribution_number",
            "ballot_name",
        ],
        ignore_null_proportion=[
            "state_electoral_division_id",
            "excluded_ballot_name",
            "excluded_party_code",
            "votes_progressive_total",
            "votes_transferred",
        ],
        observation_levels={
            "year": ["year"],
            "district": ["contest_id"],
            "candidate": ["ballot_name"],
        },
    ),
    "voting_centre": TableMeta(
        "Locais de votação",
        "Voting centres",
        "Locales de votación",
        "Locais de votação e tipos de voto por declaração de cada evento eleitoral, "
        "por distrito atendido. O endereço, a acessibilidade e as coordenadas vêm do "
        "registro de locais, que a NSWEC publica apenas para 2023.",
        "Voting centres and declaration vote types for each electoral event, by "
        "district served. The address, accessibility rating and coordinates come from "
        "the venue registry, which the NSWEC publishes for 2023 only.",
        "Locales de votación y tipos de voto por declaración de cada evento electoral, "
        "por distrito atendido. La dirección, la accesibilidad y las coordenadas "
        "provienen del registro de locales, que la NSWEC publica solo para 2023.",
        unique_key=[
            "year",
            "election_id",
            "district_name",
            "voting_centre_name",
            "vote_sub_type",
        ],
        ignore_null_proportion=[
            "state_electoral_division_id",
            "building_name",
            "address",
            "locality",
            "postcode",
            "latitude",
            "longitude",
        ],
        observation_levels={
            "year": ["year"],
            "district": ["district_name"],
            "voting_centre": ["voting_centre_name"],
        },
    ),
    "ballot_preference": TableMeta(
        "Preferências por cédula",
        "Ballot paper preferences",
        "Preferencias por boleta",
        "Uma linha por cédula da Assembleia Legislativa e por preferência marcada, com "
        "a candidatura indicada, a preferência marcada e a preferência efetivamente "
        "considerada na apuração. Cada cédula inválida ocupa uma única linha. É o grão "
        "mais fino publicado pela NSWEC e permite recalcular qualquer par de "
        "preferência entre dois candidatos.",
        "One row per Legislative Assembly ballot paper and marked preference, with the "
        "candidate indicated, the preference as marked and the preference actually "
        "used in the count. Each informal ballot paper occupies a single row. This is "
        "the finest grain the NSWEC publishes and it allows any two candidate "
        "preferred pair to be recomputed.",
        "Una fila por boleta de la Asamblea Legislativa y por preferencia marcada, con "
        "la candidatura indicada, la preferencia marcada y la preferencia "
        "efectivamente considerada en el escrutinio. Cada boleta inválida ocupa una "
        "única fila. Es el grano más fino publicado por la NSWEC y permite recalcular "
        "cualquier par de preferencia entre dos candidatos.",
        unique_key=[
            "year",
            "election_id",
            "contest_id",
            "voting_centre_name",
            "ballot_paper_id",
            "ballot_name",
        ],
        ignore_null_proportion=[
            "state_electoral_division_id",
            "ballot_name",
            "party_code",
            "preference_number",
            "preference_counted_number",
        ],
        nullable_key=["ballot_name"],
        observation_levels={
            "year": ["year"],
            "district": ["contest_id"],
            "voting_centre": ["voting_centre_name"],
        },
    ),
    "dicionario": TableMeta(
        "Dicionário",
        "Dictionary",
        "Diccionario",
        "Traduções das colunas codificadas do conjunto: para cada tabela e coluna, o "
        "significado de cada chave armazenada.",
        "Translations of the coded columns of the dataset: for each table and column, "
        "the meaning of each stored key.",
        "Traducciones de las columnas codificadas del conjunto: para cada tabla y "
        "columna, el significado de cada clave almacenada.",
        unique_key=["id_tabela", "nome_coluna", "chave"],
    ),
}


validate()
