"""Column specification for every au_aec_elections table.

Single source of truth: the architecture CSVs, the cleaning transform and the dbt
models are all generated from (or validated against) the ``TABLES`` mapping below.

Typing follows the house rule — INT64/FLOAT64 only where arithmetic is meaningful and
a measurement unit exists; every code, flag, sequence number and identifier is STRING.
"""

from __future__ import annotations

from dataclasses import dataclass, field

DIR_YEAR = "br_bd_diretorios_data_tempo.ano:ano"
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
    observations="Coluna de particionamento. Vários eventos podem compartilhar o mesmo ano",
)
ELECTION_ID = C(
    "election_id",
    "STRING",
    "Identificador do evento eleitoral atribuído pela AEC",
    "AEC identifier of the electoral event",
    "Identificador del evento electoral asignado por la AEC",
    observations="Corresponde ao Event ID usado nas URLs de results.aec.gov.au",
)
STATE = C(
    "state_abbreviation",
    "STRING",
    "Sigla do estado ou território",
    "State or territory abbreviation",
    "Sigla del estado o territorio",
    directory_column=DIR_STATE,
    original_name="StateAb",
)
DIVISION_ID = C(
    "division_id",
    "STRING",
    "Identificador da divisão eleitoral federal atribuído pela AEC",
    "AEC identifier of the Commonwealth electoral division",
    "Identificador de la división electoral federal asignado por la AEC",
    original_name="DivisionID",
    observations="Identificador próprio da AEC; não corresponde ao código CED do ABS",
)
DIVISION_NAME = C(
    "division_name",
    "STRING",
    "Nome da divisão eleitoral federal",
    "Name of the Commonwealth electoral division",
    "Nombre de la división electoral federal",
    original_name="DivisionNm",
)
PP_ID = C(
    "polling_place_id",
    "STRING",
    "Identificador do local de votação atribuído pela AEC",
    "AEC identifier of the polling place",
    "Identificador del local de votación asignado por la AEC",
    original_name="PollingPlaceID",
)
PP_NAME = C(
    "polling_place_name",
    "STRING",
    "Nome do local de votação",
    "Name of the polling place",
    "Nombre del local de votación",
    original_name="PollingPlace",
)
CANDIDATE_ID = C(
    "candidate_id",
    "STRING",
    "Identificador da pessoa candidata atribuído pela AEC",
    "AEC identifier of the candidate",
    "Identificador de la persona candidata asignado por la AEC",
    original_name="CandidateID",
)
SURNAME = C(
    "surname",
    "STRING",
    "Sobrenome da pessoa candidata",
    "Surname of the candidate",
    "Apellido de la persona candidata",
    original_name="Surname",
)
GIVEN_NAME = C(
    "given_name",
    "STRING",
    "Prenome da pessoa candidata",
    "Given name of the candidate",
    "Nombre de pila de la persona candidata",
    original_name="GivenNm",
)
PARTY_AB = C(
    "party_abbreviation",
    "STRING",
    "Sigla do partido da pessoa candidata",
    "Abbreviation of the candidate's party",
    "Sigla del partido de la persona candidata",
    original_name="PartyAb",
)
PARTY_NAME = C(
    "party_name",
    "STRING",
    "Nome do partido da pessoa candidata",
    "Name of the candidate's party",
    "Nombre del partido de la persona candidata",
    original_name="PartyNm",
)
BALLOT_POSITION = C(
    "ballot_position",
    "STRING",
    "Posição da pessoa candidata na cédula",
    "Position of the candidate on the ballot paper",
    "Posición de la persona candidata en la boleta",
    original_name="BallotPosition",
    observations="Número de ordem; aritmética sobre ele não tem sentido",
)
ELECTED = C(
    "elected",
    "STRING",
    "Indica se a pessoa candidata foi eleita neste evento",
    "Whether the candidate was elected at this event",
    "Indica si la persona candidata fue elegida en este evento",
    covered_by_dictionary="yes",
    original_name="Elected",
)
HISTORIC_ELECTED = C(
    "historic_elected",
    "STRING",
    "Indica se a pessoa candidata ocupava a cadeira antes deste evento",
    "Whether the candidate held the seat before this event",
    "Indica si la persona candidata ocupaba el escaño antes de este evento",
    covered_by_dictionary="yes",
    original_name="HistoricElected",
)
SITTING_MEMBER = C(
    "sitting_member",
    "STRING",
    "Indica se a pessoa candidata era parlamentar em exercício",
    "Whether the candidate was the sitting member",
    "Indica si la persona candidata era parlamentaria en ejercicio",
    covered_by_dictionary="yes",
    original_name="SittingMemberFl",
    observations="Publicado apenas na eleição de 2004, que não traz elected nem historic_elected",
)


def votes(name: str, pt: str, en: str, es: str, original: str = "") -> Column:
    return C(
        name,
        "INT64",
        pt,
        en,
        es,
        measurement_unit="vote",
        original_name=original,
    )


def pct(name: str, pt: str, en: str, es: str, original: str = "") -> Column:
    return C(
        name,
        "FLOAT64",
        pt,
        en,
        es,
        measurement_unit="percent",
        original_name=original,
    )


def aud(name: str, pt: str, en: str, es: str, original: str = "") -> Column:
    return C(
        name,
        "FLOAT64",
        pt,
        en,
        es,
        measurement_unit="AUD",
        original_name=original,
    )


ORDINARY = votes(
    "ordinary_votes",
    "Votos ordinários",
    "Ordinary votes",
    "Votos ordinarios",
    "OrdinaryVotes",
)
ABSENT = votes(
    "absent_votes",
    "Votos de ausentes",
    "Absent votes",
    "Votos de ausentes",
    "AbsentVotes",
)
PROVISIONAL = votes(
    "provisional_votes",
    "Votos provisórios",
    "Provisional votes",
    "Votos provisionales",
    "ProvisionalVotes",
)
PRE_POLL = votes(
    "pre_poll_votes",
    "Votos antecipados",
    "Pre-poll votes",
    "Votos anticipados",
    "PrePollVotes",
)
POSTAL = votes(
    "postal_votes",
    "Votos por correio",
    "Postal votes",
    "Votos por correo",
    "PostalVotes",
)
TOTAL_VOTES = votes(
    "total_votes",
    "Total de votos",
    "Total votes",
    "Total de votos",
    "TotalVotes",
)
FORMAL_VOTES = votes(
    "formal_votes",
    "Votos válidos",
    "Formal votes",
    "Votos válidos",
    "FormalVotes",
)
INFORMAL_VOTES = votes(
    "informal_votes",
    "Votos inválidos",
    "Informal votes",
    "Votos inválidos",
    "InformalVotes",
)

SWING = C(
    "swing",
    "FLOAT64",
    "Variação do percentual de votos em relação ao evento anterior",
    "Change in vote share relative to the previous event",
    "Variación del porcentaje de votos respecto al evento anterior",
    measurement_unit="percent",
    original_name="Swing",
    observations="Expresso em pontos percentuais",
)

TPP_BLOCK = [
    votes(
        "labor_votes",
        "Votos do Australian Labor Party na apuração de dois partidos",
        "Australian Labor Party votes in the two party preferred count",
        "Votos del Australian Labor Party en el conteo de dos partidos",
        "Australian Labor Party Votes",
    ),
    pct(
        "labor_percentage",
        "Percentual do Australian Labor Party na apuração de dois partidos",
        "Australian Labor Party share in the two party preferred count",
        "Porcentaje del Australian Labor Party en el conteo de dos partidos",
        "Australian Labor Party Percentage",
    ),
    votes(
        "coalition_votes",
        "Votos da coligação Liberal/National na apuração de dois partidos",
        "Liberal/National Coalition votes in the two party preferred count",
        "Votos de la coalición Liberal/National en el conteo de dos partidos",
        "Liberal/National Coalition Votes",
    ),
    pct(
        "coalition_percentage",
        "Percentual da coligação Liberal/National na apuração de dois partidos",
        "Liberal/National Coalition share in the two party preferred count",
        "Porcentaje de la coalición Liberal/National en el conteo de dos partidos",
        "Liberal/National Coalition Percentage",
    ),
    TOTAL_VOTES,
    SWING,
]

FINANCIAL_YEAR = C(
    "financial_year",
    "STRING",
    "Exercício financeiro australiano da declaração, no formato original da AEC",
    "Australian financial year of the return, in the AEC's original format",
    "Ejercicio financiero australiano de la declaración, en el formato original de la AEC",
    original_name="Financial Year",
    observations="A AEC usa dois formatos ao longo do tempo: 1998-1999 até 2010-2011 e 2011-12 em diante",
)
ELECTION_NAME_COL = C(
    "election_name",
    "STRING",
    "Nome do evento eleitoral ao qual a declaração se refere",
    "Name of the electoral event the return relates to",
    "Nombre del evento electoral al que se refiere la declaración",
    original_name="Event",
)
DISCLOSURE_TYPE = C(
    "disclosure_type",
    "STRING",
    "Tipo de declaração: anual, de eleição ou de referendo",
    "Type of disclosure: annual, election or referendum",
    "Tipo de declaración: anual, de elección o de referendo",
    covered_by_dictionary="yes",
)
RETURN_TYPE = C(
    "return_type",
    "STRING",
    "Categoria da declaração no Transparency Register",
    "Category of the return in the Transparency Register",
    "Categoría de la declaración en el Transparency Register",
    covered_by_dictionary="yes",
    original_name="Return Type",
)


# --------------------------------------------------------------------------------------
# Tables
# --------------------------------------------------------------------------------------

TABLES: dict[str, list[Column]] = {}

TABLES["election"] = [
    YEAR,
    ELECTION_ID,
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
        "Tipo do evento: eleição geral, eleição suplementar, eleição de Senado ou referendo",
        "Type of event: general election, by-election, Senate election or referendum",
        "Tipo de evento: elección general, elección parcial, elección de Senado o referendo",
        covered_by_dictionary="yes",
    ),
    C(
        "division_name",
        "STRING",
        "Divisão eleitoral disputada, quando o evento se restringe a uma divisão",
        "Electoral division contested, when the event covers a single division",
        "División electoral disputada, cuando el evento abarca una sola división",
        observations="Preenchido apenas para eleições suplementares",
    ),
    C(
        "state_abbreviation",
        "STRING",
        "Estado ou território ao qual o evento se restringe, quando aplicável",
        "State or territory the event is confined to, where applicable",
        "Estado o territorio al que se limita el evento, cuando corresponde",
        directory_column=DIR_STATE,
        observations="Vazio para eleições gerais e para o referendo, que são nacionais",
    ),
]

TABLES["polling_place"] = [
    YEAR,
    ELECTION_ID,
    STATE,
    DIVISION_ID,
    DIVISION_NAME,
    PP_ID,
    C(
        "polling_place_type_id",
        "STRING",
        "Código do tipo de local de votação",
        "Code for the type of polling place",
        "Código del tipo de local de votación",
        covered_by_dictionary="yes",
        original_name="PollingPlaceTypeID",
    ),
    C(
        "polling_place_name",
        "STRING",
        "Nome do local de votação",
        "Name of the polling place",
        "Nombre del local de votación",
        original_name="PollingPlaceNm",
    ),
    C(
        "premises_name",
        "STRING",
        "Nome do estabelecimento que sedia o local de votação",
        "Name of the premises hosting the polling place",
        "Nombre del establecimiento que alberga el local de votación",
        original_name="PremisesNm",
    ),
    C(
        "premises_address_1",
        "STRING",
        "Primeira linha do endereço do estabelecimento",
        "First line of the premises address",
        "Primera línea de la dirección del establecimiento",
        original_name="PremisesAddress1",
    ),
    C(
        "premises_address_2",
        "STRING",
        "Segunda linha do endereço do estabelecimento",
        "Second line of the premises address",
        "Segunda línea de la dirección del establecimiento",
        original_name="PremisesAddress2",
    ),
    C(
        "premises_address_3",
        "STRING",
        "Terceira linha do endereço do estabelecimento",
        "Third line of the premises address",
        "Tercera línea de la dirección del establecimiento",
        original_name="PremisesAddress3",
    ),
    C(
        "premises_suburb",
        "STRING",
        "Bairro ou localidade do estabelecimento",
        "Suburb or locality of the premises",
        "Barrio o localidad del establecimiento",
        original_name="PremisesSuburb",
    ),
    C(
        "premises_state_abbreviation",
        "STRING",
        "Sigla do estado do estabelecimento",
        "State abbreviation of the premises",
        "Sigla del estado del establecimiento",
        directory_column=DIR_STATE,
        original_name="PremisesStateAb",
    ),
    C(
        "premises_postcode",
        "STRING",
        "Código postal do estabelecimento",
        "Postcode of the premises",
        "Código postal del establecimiento",
        original_name="PremisesPostCode",
    ),
    C(
        "latitude",
        "FLOAT64",
        "Latitude do local de votação",
        "Latitude of the polling place",
        "Latitud del local de votación",
        measurement_unit="degree",
        original_name="Latitude",
    ),
    C(
        "longitude",
        "FLOAT64",
        "Longitude do local de votação",
        "Longitude of the polling place",
        "Longitud del local de votación",
        measurement_unit="degree",
        original_name="Longitude",
    ),
]

PARTY_STATE = C(
    "state_abbreviation",
    "STRING",
    "Estado ou território de registro do partido, ou NAT para registro nacional",
    "State or territory the party is registered in, or NAT for national registration",
    "Estado o territorio de registro del partido, o NAT para registro nacional",
    covered_by_dictionary="yes",
    original_name="StateAb",
    observations=(
        "Não referencia o diretório de estados: a AEC usa o valor NAT, que não é "
        "um estado, para partidos de registro nacional"
    ),
)

TABLES["party"] = [
    YEAR,
    ELECTION_ID,
    PARTY_STATE,
    PARTY_AB,
    C(
        "registered_party_abbreviation",
        "STRING",
        "Sigla do partido tal como registrada na AEC",
        "Party abbreviation as registered with the AEC",
        "Sigla del partido tal como está registrada en la AEC",
        original_name="RegisteredPartyAb",
    ),
    C(
        "party_name",
        "STRING",
        "Nome do partido",
        "Name of the party",
        "Nombre del partido",
        original_name="PartyNm",
    ),
]

TABLES["house_candidate"] = [
    YEAR,
    ELECTION_ID,
    STATE,
    DIVISION_ID,
    DIVISION_NAME,
    CANDIDATE_ID,
    SURNAME,
    GIVEN_NAME,
    PARTY_AB,
    PARTY_NAME,
    ELECTED,
    HISTORIC_ELECTED,
    SITTING_MEMBER,
]

TABLES["house_first_preference_division"] = [
    YEAR,
    ELECTION_ID,
    STATE,
    DIVISION_ID,
    DIVISION_NAME,
    CANDIDATE_ID,
    SURNAME,
    GIVEN_NAME,
    BALLOT_POSITION,
    PARTY_AB,
    PARTY_NAME,
    ELECTED,
    HISTORIC_ELECTED,
    SITTING_MEMBER,
    ORDINARY,
    ABSENT,
    PROVISIONAL,
    PRE_POLL,
    POSTAL,
    TOTAL_VOTES,
    SWING,
]

_PP_CANDIDATE_TAIL = [
    SURNAME,
    GIVEN_NAME,
    BALLOT_POSITION,
    PARTY_AB,
    PARTY_NAME,
    ELECTED,
    HISTORIC_ELECTED,
    SITTING_MEMBER,
    ORDINARY,
    SWING,
]

TABLES["house_first_preference_polling_place"] = [
    YEAR,
    ELECTION_ID,
    STATE,
    DIVISION_ID,
    DIVISION_NAME,
    PP_ID,
    PP_NAME,
    CANDIDATE_ID,
    *_PP_CANDIDATE_TAIL,
]

TABLES["house_two_candidate_preferred_polling_place"] = [
    YEAR,
    ELECTION_ID,
    STATE,
    DIVISION_ID,
    DIVISION_NAME,
    PP_ID,
    PP_NAME,
    CANDIDATE_ID,
    *_PP_CANDIDATE_TAIL,
]

TABLES["house_two_party_preferred_division"] = [
    YEAR,
    ELECTION_ID,
    STATE,
    DIVISION_ID,
    DIVISION_NAME,
    C(
        "party_abbreviation",
        "STRING",
        "Sigla do partido que venceu a divisão na apuração de dois partidos",
        "Abbreviation of the party that won the division on the two party preferred count",
        "Sigla del partido que ganó la división en el conteo de dos partidos",
        original_name="PartyAb",
    ),
    *TPP_BLOCK,
]

TABLES["house_two_party_preferred_polling_place"] = [
    YEAR,
    ELECTION_ID,
    STATE,
    DIVISION_ID,
    DIVISION_NAME,
    PP_ID,
    PP_NAME,
    *TPP_BLOCK,
]

TABLES["senate_candidate"] = [
    YEAR,
    ELECTION_ID,
    STATE,
    CANDIDATE_ID,
    SURNAME,
    GIVEN_NAME,
    PARTY_AB,
    PARTY_NAME,
    ELECTED,
    HISTORIC_ELECTED,
    SITTING_MEMBER,
    C(
        "elected_order",
        "STRING",
        "Ordem em que a pessoa foi eleita para o Senado no estado",
        "Order in which the senator was elected within the state",
        "Orden en que la persona fue elegida al Senado en el estado",
        original_name="ElectedOrder",
        observations="Preenchido apenas para quem foi eleito",
    ),
]

TABLES["senate_first_preference_division"] = [
    YEAR,
    ELECTION_ID,
    STATE,
    DIVISION_ID,
    DIVISION_NAME,
    C(
        "group_abbreviation",
        "STRING",
        "Letra do grupo na cédula do Senado",
        "Ballot group letter on the Senate ballot paper",
        "Letra del grupo en la boleta del Senado",
        original_name="Group",
        observations="Publicado como Ticket até 2019 e como Group a partir de 2022",
    ),
    BALLOT_POSITION,
    CANDIDATE_ID,
    C(
        "candidate_details",
        "STRING",
        "Descrição da pessoa candidata ou da linha acima da cédula",
        "Description of the candidate or of the above-the-line ballot row",
        "Descripción de la persona candidata o de la línea superior de la boleta",
        original_name="CandidateDetails",
    ),
    PARTY_AB,
    C(
        "party_name",
        "STRING",
        "Nome do partido da pessoa candidata",
        "Name of the candidate's party",
        "Nombre del partido de la persona candidata",
        original_name="PartyName",
    ),
    ELECTED,
    HISTORIC_ELECTED,
    ORDINARY,
    ABSENT,
    PROVISIONAL,
    PRE_POLL,
    POSTAL,
    TOTAL_VOTES,
]

TABLES["division_summary"] = [
    YEAR,
    ELECTION_ID,
    C(
        "chamber",
        "STRING",
        "Apuração à qual os totais se referem: Câmara, Senado ou referendo",
        "Count the totals refer to: House, Senate or referendum",
        "Conteo al que se refieren los totales: Cámara, Senado o referendo",
        covered_by_dictionary="yes",
    ),
    STATE,
    DIVISION_ID,
    DIVISION_NAME,
    C(
        "enrolment",
        "INT64",
        "Número de pessoas inscritas na divisão",
        "Number of electors enrolled in the division",
        "Número de personas inscritas en la división",
        measurement_unit="person",
        original_name="Enrolment",
    ),
    C(
        "turnout",
        "INT64",
        "Número de pessoas que compareceram",
        "Number of electors who turned out",
        "Número de personas que acudieron a votar",
        measurement_unit="person",
        original_name="Turnout",
    ),
    pct(
        "turnout_percentage",
        "Comparecimento como percentual das pessoas inscritas",
        "Turnout as a share of enrolled electors",
        "Participación como porcentaje de las personas inscritas",
        "TurnoutPercentage",
    ),
    C(
        "turnout_swing",
        "FLOAT64",
        "Variação do comparecimento em relação ao evento anterior",
        "Change in turnout relative to the previous event",
        "Variación de la participación respecto al evento anterior",
        measurement_unit="percent",
        original_name="TurnoutSwing",
        observations="Expresso em pontos percentuais",
    ),
    ORDINARY,
    ABSENT,
    PROVISIONAL,
    PRE_POLL,
    POSTAL,
    FORMAL_VOTES,
    INFORMAL_VOTES,
    pct(
        "informal_percentage",
        "Votos inválidos como percentual do total apurado",
        "Informal votes as a share of the total count",
        "Votos inválidos como porcentaje del total escrutado",
        "InformalPercent",
    ),
    C(
        "informal_swing",
        "FLOAT64",
        "Variação do percentual de votos inválidos em relação ao evento anterior",
        "Change in the informal vote share relative to the previous event",
        "Variación del porcentaje de votos inválidos respecto al evento anterior",
        measurement_unit="percent",
        original_name="InformalSwing",
        observations="Expresso em pontos percentuais",
    ),
    TOTAL_VOTES,
]

TABLES["referendum_polling_place"] = [
    YEAR,
    ELECTION_ID,
    C(
        "question_number",
        "STRING",
        "Número da pergunta submetida a referendo",
        "Number of the question put to referendum",
        "Número de la pregunta sometida a referendo",
        original_name="QuestionNo",
    ),
    STATE,
    DIVISION_ID,
    C(
        "division_name",
        "STRING",
        "Nome da divisão eleitoral federal",
        "Name of the Commonwealth electoral division",
        "Nombre de la división electoral federal",
        original_name="DivisionName",
    ),
    C(
        "polling_place_id",
        "STRING",
        "Identificador do local de votação atribuído pela AEC",
        "AEC identifier of the polling place",
        "Identificador del local de votación asignado por la AEC",
        original_name="PollingPlaceId",
    ),
    C(
        "polling_place_name",
        "STRING",
        "Nome do local de votação",
        "Name of the polling place",
        "Nombre del local de votación",
        original_name="PollingPlaceNm",
    ),
    votes(
        "yes_votes",
        "Votos pelo Sim",
        "Yes votes",
        "Votos por el Sí",
        "YesVotes",
    ),
    pct(
        "yes_percentage",
        "Percentual de votos pelo Sim sobre os votos válidos",
        "Yes votes as a share of formal votes",
        "Porcentaje de votos por el Sí sobre los votos válidos",
        "YesPercentage",
    ),
    votes(
        "no_votes", "Votos pelo Não", "No votes", "Votos por el No", "NoVotes"
    ),
    pct(
        "no_percentage",
        "Percentual de votos pelo Não sobre os votos válidos",
        "No votes as a share of formal votes",
        "Porcentaje de votos por el No sobre los votos válidos",
        "NoPercentage",
    ),
    FORMAL_VOTES,
    pct(
        "formal_percentage",
        "Votos válidos como percentual do total apurado",
        "Formal votes as a share of the total count",
        "Votos válidos como porcentaje del total escrutado",
        "FormalPercentage",
    ),
    INFORMAL_VOTES,
    pct(
        "informal_percentage",
        "Votos inválidos como percentual do total apurado",
        "Informal votes as a share of the total count",
        "Votos inválidos como porcentaje del total escrutado",
        "InformalPercentage",
    ),
    TOTAL_VOTES,
]

TABLES["disclosure_donation"] = [
    YEAR,
    DISCLOSURE_TYPE,
    FINANCIAL_YEAR,
    ELECTION_NAME_COL,
    RETURN_TYPE,
    C(
        "direction",
        "STRING",
        "Lado da declaração de onde a doação foi extraída: declarada pelo doador ou pelo recebedor",
        "Side of the return the donation was taken from: reported by the donor or by the recipient",
        "Lado de la declaración del que se extrajo la donación: declarada por el donante o por el receptor",
        covered_by_dictionary="yes",
        observations="A mesma doação pode aparecer nos dois lados; filtre por direction para evitar dupla contagem",
    ),
    C(
        "donor_name",
        "STRING",
        "Nome de quem fez a doação",
        "Name of the donor",
        "Nombre de quien hizo la donación",
    ),
    C(
        "recipient_name",
        "STRING",
        "Nome de quem recebeu a doação",
        "Name of the recipient",
        "Nombre de quien recibió la donación",
    ),
    C(
        "donation_date",
        "DATE",
        "Data da doação",
        "Date of the donation",
        "Fecha de la donación",
        observations="Ausente em parte das declarações antigas",
    ),
    aud(
        "value",
        "Valor da doação em dólares australianos",
        "Value of the donation in Australian dollars",
        "Valor de la donación en dólares australianos",
    ),
]

TABLES["disclosure_receipt"] = [
    YEAR,
    FINANCIAL_YEAR,
    RETURN_TYPE,
    C(
        "recipient_name",
        "STRING",
        "Nome da entidade que declarou o recebimento",
        "Name of the entity that reported the receipt",
        "Nombre de la entidad que declaró el ingreso",
        original_name="Recipient Name",
    ),
    C(
        "received_from",
        "STRING",
        "Nome de quem originou o recebimento",
        "Name of the party the receipt came from",
        "Nombre de quien originó el ingreso",
        original_name="Received From",
    ),
    C(
        "receipt_type",
        "STRING",
        "Natureza do recebimento declarado",
        "Nature of the reported receipt",
        "Naturaleza del ingreso declarado",
        covered_by_dictionary="yes",
        original_name="Receipt Type",
    ),
    aud(
        "value",
        "Valor do recebimento em dólares australianos",
        "Value of the receipt in Australian dollars",
        "Valor del ingreso en dólares australianos",
        "Value",
    ),
]

TABLES["disclosure_return_annual"] = [
    YEAR,
    FINANCIAL_YEAR,
    RETURN_TYPE,
    C(
        "name",
        "STRING",
        "Nome da entidade declarante",
        "Name of the reporting entity",
        "Nombre de la entidad declarante",
        original_name="Name",
    ),
    C(
        "lodged_on_behalf_of",
        "STRING",
        "Entidade em nome da qual a declaração foi apresentada",
        "Entity the return was lodged on behalf of",
        "Entidad en cuyo nombre se presentó la declaración",
        original_name="Lodged on behalf of",
    ),
    C(
        "party_group",
        "STRING",
        "Agrupamento partidário ao qual a entidade pertence",
        "Party grouping the entity belongs to",
        "Agrupación partidaria a la que pertenece la entidad",
        original_name="Party Group",
    ),
    C(
        "associated_parties",
        "STRING",
        "Partidos aos quais a entidade associada está vinculada",
        "Parties the associated entity is linked to",
        "Partidos a los que está vinculada la entidad asociada",
        original_name="AssociatedParties",
    ),
    C(
        "client_type",
        "STRING",
        "Categoria da entidade no cadastro da AEC",
        "Category of the entity in the AEC register",
        "Categoría de la entidad en el registro de la AEC",
        covered_by_dictionary="yes",
        original_name="ClientType",
    ),
    C(
        "client_file_id",
        "STRING",
        "Identificador da entidade no cadastro da AEC",
        "Identifier of the entity in the AEC register",
        "Identificador de la entidad en el registro de la AEC",
        original_name="ClientFileId",
    ),
    C(
        "abn",
        "STRING",
        "Australian Business Number da entidade",
        "Australian Business Number of the entity",
        "Australian Business Number de la entidad",
        original_name="ABN",
    ),
    C(
        "acn",
        "STRING",
        "Australian Company Number da entidade",
        "Australian Company Number of the entity",
        "Australian Company Number de la entidad",
        original_name="ACN",
    ),
    aud(
        "total_receipts",
        "Total de recebimentos declarados",
        "Total receipts reported",
        "Total de ingresos declarados",
        "Total Receipts",
    ),
    aud(
        "total_payments",
        "Total de pagamentos declarados",
        "Total payments reported",
        "Total de pagos declarados",
        "Total Payments",
    ),
    aud(
        "total_debts",
        "Total de dívidas declaradas",
        "Total debts reported",
        "Total de deudas declaradas",
        "Total Debts",
    ),
    aud(
        "total_discretionary_benefits",
        "Total de benefícios discricionários declarados",
        "Total discretionary benefits reported",
        "Total de beneficios discrecionales declarados",
        "Total Discretionary Benefits",
    ),
    aud(
        "capital_contributions",
        "Total de aportes de capital declarados",
        "Total capital contributions reported",
        "Total de aportes de capital declarados",
        "Capital Contributions",
    ),
    aud(
        "total_donations_made",
        "Total de doações feitas",
        "Total donations made",
        "Total de donaciones realizadas",
        "Total Donations Made",
    ),
    aud(
        "total_donations_received",
        "Total de doações recebidas",
        "Total donations received",
        "Total de donaciones recibidas",
        "Total Donations Received",
    ),
    aud(
        "total_expenditure",
        "Total de despesas declaradas",
        "Total expenditure reported",
        "Total de gastos declarados",
        "Total Expenditure",
    ),
    aud(
        "electoral_expenditure",
        "Total de despesas eleitorais declaradas",
        "Total electoral expenditure reported",
        "Total de gastos electorales declarados",
        "Electoral Expenditure",
    ),
    C(
        "number_of_donors",
        "INT64",
        "Número de pessoas ou entidades doadoras declaradas",
        "Number of donors reported",
        "Número de personas o entidades donantes declaradas",
        measurement_unit="donor",
        original_name="Number of Donors",
    ),
]

TABLES["disclosure_election_return"] = [
    YEAR,
    ELECTION_NAME_COL,
    C(
        "return_type",
        "STRING",
        "Se a declaração é de pessoa candidata ou de grupo do Senado",
        "Whether the return is for a candidate or a Senate group",
        "Si la declaración es de persona candidata o de grupo del Senado",
        covered_by_dictionary="yes",
        original_name="Return Type (Candidate/Senate Group)",
    ),
    C(
        "name",
        "STRING",
        "Nome da pessoa candidata ou do grupo do Senado",
        "Name of the candidate or Senate group",
        "Nombre de la persona candidata o del grupo del Senado",
        original_name="Name",
    ),
    C(
        "party_id",
        "STRING",
        "Identificador do partido atribuído pela AEC",
        "AEC identifier of the party",
        "Identificador del partido asignado por la AEC",
        original_name="Party ID",
    ),
    C(
        "party_name",
        "STRING",
        "Nome do partido",
        "Name of the party",
        "Nombre del partido",
        original_name="Party Name",
    ),
    C(
        "electorate_name",
        "STRING",
        "Divisão eleitoral disputada",
        "Electoral division contested",
        "División electoral disputada",
        original_name="Electorate Name",
    ),
    C(
        "electorate_state",
        "STRING",
        "Estado ou território da divisão disputada",
        "State or territory of the division contested",
        "Estado o territorio de la división disputada",
        original_name="Electorate State",
    ),
    C(
        "nil_return",
        "STRING",
        "Indica se a declaração foi apresentada sem movimentação",
        "Whether the return was lodged as a nil return",
        "Indica si la declaración se presentó sin movimientos",
        covered_by_dictionary="yes",
        original_name="Nil Return",
    ),
    C(
        "amendment_number",
        "STRING",
        "Número da retificação da declaração",
        "Amendment number of the return",
        "Número de la rectificación de la declaración",
        original_name="Amendment No",
    ),
    aud(
        "total_gift_value",
        "Valor total das doações recebidas",
        "Total value of gifts received",
        "Valor total de las donaciones recibidas",
        "Total Gift Value",
    ),
    C(
        "number_of_donors",
        "INT64",
        "Número de pessoas ou entidades doadoras declaradas",
        "Number of donors reported",
        "Número de personas o entidades donantes declaradas",
        measurement_unit="donor",
        original_name="Number Of Donors",
    ),
    aud(
        "total_electoral_expenditure",
        "Total de despesas eleitorais declaradas",
        "Total electoral expenditure reported",
        "Total de gastos electorales declarados",
        "Total Electoral Expenditure",
    ),
    aud(
        "discretionary_benefits_received",
        "Total de benefícios discricionários recebidos",
        "Total discretionary benefits received",
        "Total de beneficios discrecionales recibidos",
        "Discretionary Benefits Received",
    ),
    aud(
        "broadcasting_cost",
        "Despesa com veiculação em rádio e televisão",
        "Expenditure on broadcasting",
        "Gasto en radio y televisión",
        "Broadcasting Cost",
    ),
    aud(
        "publishing_cost",
        "Despesa com veiculação em mídia impressa",
        "Expenditure on publishing",
        "Gasto en medios impresos",
        "Publishing Cost",
    ),
    aud(
        "display_ad_cost",
        "Despesa com publicidade externa",
        "Expenditure on display advertising",
        "Gasto en publicidad exterior",
        "Display Ad Cost",
    ),
    aud(
        "direct_mailing_cost",
        "Despesa com mala direta",
        "Expenditure on direct mailing",
        "Gasto en correo directo",
        "Direct Mailing",
    ),
    aud(
        "campaign_material_cost",
        "Despesa com material de campanha",
        "Expenditure on campaign material",
        "Gasto en material de campaña",
        "Campaign Material Costs",
    ),
    aud(
        "opinion_poll_cost",
        "Despesa com pesquisas de opinião",
        "Expenditure on opinion polls",
        "Gasto en encuestas de opinión",
        "Opinion Polls",
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

PARTITION_RANGE = {"start": 1998, "end": 2035, "interval": 1}


def column_names(table: str) -> list[str]:
    return [c.name for c in TABLES[table]]


def column_types(table: str) -> dict[str, str]:
    return {c.name: c.bigquery_type for c in TABLES[table]}


def validate() -> None:
    """Fail loudly on a duplicated column name within a table."""
    for table, cols in TABLES.items():
        names = [c.name for c in cols]
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise ValueError(f"{table}: duplicated columns {sorted(dupes)}")


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
        "Catálogo dos eventos eleitorais federais cobertos pelo conjunto: eleições gerais, "
        "eleições suplementares, a eleição de Senado da Austrália Ocidental de 2014 e o "
        "referendo de 2023. Serve de chave para todas as demais tabelas por election_id.",
        "Catalogue of the federal electoral events covered by the dataset: general elections, "
        "by-elections, the 2014 Western Australia Senate election and the 2023 referendum. "
        "Acts as the key for every other table through election_id.",
        "Catálogo de los eventos electorales federales cubiertos por el conjunto: elecciones "
        "generales, elecciones parciales, la elección de Senado de Australia Occidental de 2014 "
        "y el referendo de 2023. Sirve de clave para las demás tablas mediante election_id.",
        unique_key=["election_id"],
    ),
    "polling_place": TableMeta(
        "Locais de votação",
        "Polling places",
        "Locales de votación",
        "Locais de votação de cada evento eleitoral, com endereço e coordenadas. A eleição de "
        "2004 não publica latitude e longitude.",
        "Polling places for each electoral event, with address and coordinates. The 2004 "
        "election publishes no latitude or longitude.",
        "Locales de votación de cada evento electoral, con dirección y coordenadas. La elección "
        "de 2004 no publica latitud ni longitud.",
        unique_key=["year", "election_id", "polling_place_id"],
        ignore_null_proportion=["premises_address_2", "premises_address_3"],
    ),
    "party": TableMeta(
        "Partidos",
        "Parties",
        "Partidos",
        "Partidos registrados em cada evento eleitoral, por estado. A AEC registra a mesma sigla "
        "sob mais de um nome em alguns estados, de modo que a sigla sozinha não identifica a linha.",
        "Parties registered at each electoral event, by state. The AEC registers the same "
        "abbreviation under more than one name in some states, so the abbreviation alone does not "
        "identify a row.",
        "Partidos registrados en cada evento electoral, por estado. La AEC registra la misma sigla "
        "bajo más de un nombre en algunos estados, por lo que la sigla sola no identifica la fila.",
        unique_key=[
            "year",
            "election_id",
            "state_abbreviation",
            "party_abbreviation",
            "registered_party_abbreviation",
            "party_name",
        ],
        nullable_key=["registered_party_abbreviation"],
    ),
    "house_candidate": TableMeta(
        "Candidaturas à Câmara",
        "House candidates",
        "Candidaturas a la Cámara",
        "Pessoas candidatas à Câmara dos Representantes em cada evento eleitoral, com partido, "
        "divisão e indicação de eleição.",
        "Candidates for the House of Representatives at each electoral event, with party, "
        "division and elected status.",
        "Personas candidatas a la Cámara de Representantes en cada evento electoral, con partido, "
        "división e indicación de elección.",
        unique_key=["year", "election_id", "candidate_id"],
        ignore_null_proportion=["sitting_member"],
    ),
    "house_first_preference_division": TableMeta(
        "Primeiras preferências da Câmara por divisão",
        "House first preferences by division",
        "Primeras preferencias de la Cámara por división",
        "Votos de primeira preferência para a Câmara dos Representantes, por divisão, pessoa "
        "candidata e tipo de voto. Cobre apenas as eleições gerais: eleições suplementares não "
        "publicam apuração por divisão.",
        "First preference votes for the House of Representatives, by division, candidate and vote "
        "type. Covers general elections only: by-elections publish no division-level count.",
        "Votos de primera preferencia para la Cámara de Representantes, por división, persona "
        "candidata y tipo de voto. Cubre solo las elecciones generales: las elecciones parciales "
        "no publican escrutinio por división.",
        unique_key=["year", "election_id", "division_id", "candidate_id"],
        ignore_null_proportion=["sitting_member"],
    ),
    "house_first_preference_polling_place": TableMeta(
        "Primeiras preferências da Câmara por local de votação",
        "House first preferences by polling place",
        "Primeras preferencias de la Cámara por local de votación",
        "Votos ordinários de primeira preferência para a Câmara dos Representantes, por local de "
        "votação e pessoa candidata. Inclui eleições gerais e suplementares.",
        "Ordinary first preference votes for the House of Representatives, by polling place and "
        "candidate. Covers both general elections and by-elections.",
        "Votos ordinarios de primera preferencia para la Cámara de Representantes, por local de "
        "votación y persona candidata. Incluye elecciones generales y parciales.",
        unique_key=[
            "year",
            "election_id",
            "division_id",
            "polling_place_id",
            "candidate_id",
        ],
        ignore_null_proportion=["sitting_member"],
    ),
    "house_two_candidate_preferred_polling_place": TableMeta(
        "Preferência entre dois candidatos da Câmara por local de votação",
        "House two candidate preferred by polling place",
        "Preferencia entre dos candidatos de la Cámara por local de votación",
        "Apuração de preferência entre as duas pessoas candidatas mais votadas de cada divisão, "
        "por local de votação. Inclui eleições gerais e suplementares.",
        "Two candidate preferred count between the two leading candidates in each division, by "
        "polling place. Covers both general elections and by-elections.",
        "Escrutinio de preferencia entre las dos personas candidatas más votadas de cada división, "
        "por local de votación. Incluye elecciones generales y parciales.",
        unique_key=[
            "year",
            "election_id",
            "division_id",
            "polling_place_id",
            "candidate_id",
        ],
        ignore_null_proportion=["sitting_member"],
    ),
    "house_two_party_preferred_division": TableMeta(
        "Preferência entre dois partidos da Câmara por divisão",
        "House two party preferred by division",
        "Preferencia entre dos partidos de la Cámara por división",
        "Apuração de preferência entre o Australian Labor Party e a coligação Liberal/National, "
        "por divisão. Cobre apenas as eleições gerais.",
        "Two party preferred count between the Australian Labor Party and the Liberal/National "
        "Coalition, by division. Covers general elections only.",
        "Escrutinio de preferencia entre el Australian Labor Party y la coalición Liberal/National, "
        "por división. Cubre solo las elecciones generales.",
        unique_key=["year", "election_id", "division_id"],
    ),
    "house_two_party_preferred_polling_place": TableMeta(
        "Preferência entre dois partidos da Câmara por local de votação",
        "House two party preferred by polling place",
        "Preferencia entre dos partidos de la Cámara por local de votación",
        "Apuração de preferência entre o Australian Labor Party e a coligação Liberal/National, "
        "por local de votação. Inclui eleições gerais e suplementares.",
        "Two party preferred count between the Australian Labor Party and the Liberal/National "
        "Coalition, by polling place. Covers both general elections and by-elections.",
        "Escrutinio de preferencia entre el Australian Labor Party y la coalición Liberal/National, "
        "por local de votación. Incluye elecciones generales y parciales.",
        unique_key=["year", "election_id", "division_id", "polling_place_id"],
    ),
    "senate_candidate": TableMeta(
        "Candidaturas ao Senado",
        "Senate candidates",
        "Candidaturas al Senado",
        "Pessoas candidatas ao Senado em cada evento eleitoral, com partido, estado e ordem de "
        "eleição para quem foi eleito.",
        "Candidates for the Senate at each electoral event, with party, state and the order of "
        "election for those elected.",
        "Personas candidatas al Senado en cada evento electoral, con partido, estado y orden de "
        "elección para quienes fueron elegidos.",
        unique_key=["year", "election_id", "candidate_id"],
        ignore_null_proportion=["sitting_member", "elected_order"],
    ),
    "senate_first_preference_division": TableMeta(
        "Primeiras preferências do Senado por divisão",
        "Senate first preferences by division",
        "Primeras preferencias del Senado por división",
        "Votos de primeira preferência para o Senado, por divisão, grupo da cédula, pessoa "
        "candidata e tipo de voto. Inclui as linhas acima da cédula.",
        "First preference votes for the Senate, by division, ballot group, candidate and vote "
        "type. Includes the above-the-line ballot rows.",
        "Votos de primera preferencia para el Senado, por división, grupo de la boleta, persona "
        "candidata y tipo de voto. Incluye las líneas superiores de la boleta.",
        unique_key=["year", "election_id", "division_id", "candidate_id"],
    ),
    "division_summary": TableMeta(
        "Resumo por divisão",
        "Division summary",
        "Resumen por división",
        "Inscrições, comparecimento, votos válidos e inválidos e composição por tipo de voto, por "
        "divisão e apuração. A coluna chamber separa Câmara, Senado e referendo.",
        "Enrolment, turnout, formal and informal votes and the composition by vote type, per "
        "division and count. The chamber column separates House, Senate and referendum.",
        "Inscripciones, participación, votos válidos e inválidos y composición por tipo de voto, "
        "por división y escrutinio. La columna chamber separa Cámara, Senado y referendo.",
        unique_key=["year", "election_id", "chamber", "division_id"],
    ),
    "referendum_polling_place": TableMeta(
        "Resultados do referendo por local de votação",
        "Referendum results by polling place",
        "Resultados del referendo por local de votación",
        "Votos pelo Sim e pelo Não no referendo de 2023 sobre a Voz Indígena ao Parlamento, por "
        "local de votação.",
        "Yes and No votes at the 2023 referendum on an Aboriginal and Torres Strait Islander Voice "
        "to Parliament, by polling place.",
        "Votos por el Sí y por el No en el referendo de 2023 sobre la Voz Indígena al Parlamento, "
        "por local de votación.",
        unique_key=[
            "year",
            "election_id",
            "question_number",
            "division_id",
            "polling_place_id",
        ],
    ),
    "disclosure_donation": TableMeta(
        "Doações declaradas",
        "Disclosed donations",
        "Donaciones declaradas",
        "Doações individuais declaradas ao Transparency Register da AEC, reunindo as declarações "
        "anuais, de eleição e de referendo. Cada linha é uma aresta doador-recebedor; a coluna "
        "direction indica se a doação foi declarada por quem doou ou por quem recebeu, e a mesma "
        "doação pode constar dos dois lados.",
        "Individual donations disclosed to the AEC Transparency Register, pooling the annual, "
        "election and referendum returns. Each row is a donor-recipient edge; the direction column "
        "records whether the donation was reported by the donor or by the recipient, and the same "
        "donation may appear on both sides.",
        "Donaciones individuales declaradas al Transparency Register de la AEC, reuniendo las "
        "declaraciones anuales, de elección y de referendo. Cada fila es una arista donante-receptor; "
        "la columna direction indica si la donación fue declarada por quien donó o por quien recibió, "
        "y la misma donación puede constar en ambos lados.",
        ignore_null_proportion=["election_name"],
    ),
    "disclosure_receipt": TableMeta(
        "Recebimentos declarados",
        "Disclosed receipts",
        "Ingresos declarados",
        "Recebimentos individuais declarados nas declarações anuais ao Transparency Register da "
        "AEC, incluindo doações, contribuições de filiados e outros recebimentos.",
        "Individual receipts disclosed in annual returns to the AEC Transparency Register, "
        "covering donations, subscriptions and other receipts.",
        "Ingresos individuales declarados en las declaraciones anuales al Transparency Register de "
        "la AEC, incluyendo donaciones, cuotas de afiliados y otros ingresos.",
    ),
    "disclosure_return_annual": TableMeta(
        "Declarações anuais",
        "Annual returns",
        "Declaraciones anuales",
        "Totais das declarações anuais apresentadas ao Transparency Register da AEC por partidos, "
        "entidades associadas, terceiros, terceiros significativos, doadores e parlamentares. As "
        "colunas variam conforme o formulário, de modo que cada linha preenche apenas as que se "
        "aplicam ao seu return_type. As cinco categorias de despesa eleitoral das declarações de "
        "terceiros não são carregadas.",
        "Totals from the annual returns lodged with the AEC Transparency Register by parties, "
        "associated entities, third parties, significant third parties, donors and members of "
        "parliament. Columns vary by form, so each row populates only those that apply to its "
        "return_type. The five electoral expenditure categories of third party returns are not "
        "carried.",
        "Totales de las declaraciones anuales presentadas al Transparency Register de la AEC por "
        "partidos, entidades asociadas, terceros, terceros significativos, donantes y "
        "parlamentarios. Las columnas varían según el formulario, por lo que cada fila completa "
        "solo las que corresponden a su return_type. Las cinco categorías de gasto electoral de las "
        "declaraciones de terceros no se cargan.",
        ignore_null_proportion=[
            "lodged_on_behalf_of",
            "party_group",
            "associated_parties",
            "client_type",
            "client_file_id",
            "abn",
            "acn",
            "total_expenditure",
            "electoral_expenditure",
            "number_of_donors",
        ],
    ),
    "disclosure_election_return": TableMeta(
        "Declarações de eleição de candidaturas e grupos do Senado",
        "Candidate and Senate group election returns",
        "Declaraciones de elección de candidaturas y grupos del Senado",
        "Totais das declarações de eleição apresentadas por pessoas candidatas e grupos do Senado, "
        "com doações recebidas e despesa eleitoral aberta por categoria.",
        "Totals from the election returns lodged by candidates and Senate groups, with donations "
        "received and electoral expenditure broken down by category.",
        "Totales de las declaraciones de elección presentadas por personas candidatas y grupos del "
        "Senado, con donaciones recibidas y gasto electoral desglosado por categoría.",
    ),
    "dicionario": TableMeta(
        "Dicionário",
        "Dictionary",
        "Diccionario",
        "Correspondência entre as chaves codificadas das colunas do conjunto e o seu significado.",
        "Mapping between the coded keys used in the dataset's columns and their meaning.",
        "Correspondencia entre las claves codificadas de las columnas del conjunto y su significado.",
    ),
}

assert set(TABLE_META) == set(TABLES), (
    f"TABLE_META and TABLES disagree: {set(TABLE_META) ^ set(TABLES)}"
)
