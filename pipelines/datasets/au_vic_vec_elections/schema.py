"""Column specification for every au_vic_vec_elections table.

Single source of truth: the architecture CSVs, the cleaning transform and the dbt
models are all generated from (or validated against) the ``TABLES`` mapping below.

Typing follows the house rule — INT64/FLOAT64 only where arithmetic is meaningful and
a measurement unit exists; every code, flag, ballot position, sequence number and
identifier is STRING.
"""

from __future__ import annotations

from dataclasses import dataclass

# Directory foreign keys use the BACKEND slug (diretorios_au,
# diretorios_data_tempo), not the BigQuery dataset id (br_bd_diretorios_*).
# An unresolved directory_column makes bulk_upsert_columns drop the whole column.
DIR_YEAR = "diretorios_data_tempo.ano:ano"
DIR_SED_2011 = (
    "diretorios_au.state_electoral_division_2011:id_state_electoral_division"
)
DIR_SED_2016 = (
    "diretorios_au.state_electoral_division_2016:id_state_electoral_division"
)
DIR_SED_2021 = (
    "diretorios_au.state_electoral_division_2021:id_state_electoral_division"
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
    observations="Coluna de particionamento. Vários eventos eleitorais podem compartilhar o mesmo ano.",
)
ELECTION_ID = C(
    "election_id",
    "STRING",
    "Identificador do evento eleitoral",
    "Identifier of the electoral event",
    "Identificador del evento electoral",
    observations="Chave estrangeira para a tabela election, presente em todas as tabelas de resultados. Assume valores como state2002, state2022, prahran_by2025 e nepean_by2026.",
)
CONTEST_ID = C(
    "contest_id",
    "STRING",
    "Identificador da disputa por uma cadeira dentro do evento eleitoral",
    "Identifier of the contest for a single seat within the electoral event",
    "Identificador de la disputa por un escaño dentro del evento electoral",
    observations="Identifica uma única disputa por cadeira dentro do evento eleitoral. É o slug do distrito, da região ou da província, como em state2022-albert_park.",
)
CHAMBER = C(
    "chamber",
    "STRING",
    "Câmara do parlamento estadual à qual a disputa se refere",
    "Chamber of the state parliament the contest refers to",
    "Cámara del parlamento estatal a la que se refiere la disputa",
    covered_by_dictionary="yes",
    observations="Assume os valores legislative_assembly e legislative_council. A Assembleia Legislativa é disputada por distrito de cadeira única; o Conselho Legislativo foi disputado por província de cadeira única até 2002 e por região de múltiplas cadeiras a partir de 2006.",
)
GOVERNMENT_LEVEL = C(
    "government_level",
    "STRING",
    "Esfera de governo da disputa",
    "Level of government of the contest",
    "Nivel de gobierno de la disputa",
    covered_by_dictionary="yes",
    observations="Assume o valor state. A coluna é definida por disputa, e não como constante do conjunto, para que as disputas de governo local, também administradas pela VEC e ainda não incluídas, possam ser acrescentadas a estas mesmas tabelas sem reestruturá-las.",
)
CONTEST_TYPE = C(
    "contest_type",
    "STRING",
    "Tipo de cadeira disputada",
    "Type of seat contested",
    "Tipo de escaño disputado",
    covered_by_dictionary="yes",
    observations="Assume os valores state_district, state_region e state_province.",
)
VOTING_SYSTEM = C(
    "voting_system",
    "STRING",
    "Sistema de votação aplicado à disputa",
    "Voting system applied to the contest",
    "Sistema de votación aplicado a la disputa",
    covered_by_dictionary="yes",
    observations="Assume os valores compulsory_preferential e single_transferable_vote. É definido por disputa: a Assembleia Legislativa e as províncias do Conselho Legislativo anteriores a 2006 usam voto preferencial obrigatório para uma única cadeira, e as regiões do Conselho Legislativo a partir de 2006 usam o voto único transferível.",
)
DISTRICT_NAME = C(
    "district_name",
    "STRING",
    "Nome da disputa: distrito da Assembleia Legislativa, região ou província do Conselho Legislativo",
    "Name of the contest: Legislative Assembly district, Legislative Council region or Legislative Council province",
    "Nombre de la disputa: distrito de la Asamblea Legislativa, región o provincia del Consejo Legislativo",
)
SED_ID = C(
    "state_electoral_division_id",
    "STRING",
    "Código do distrito eleitoral estadual no padrão ASGS 2021 do ABS",
    "Australian Statistical Geography Standard 2021 code of the state electoral division",
    "Código del distrito electoral estatal en el estándar ASGS 2021 del ABS",
    directory_column=DIR_SED_2021,
    observations="Código ASGS 2021 do ABS para o distrito eleitoral estadual, cruzado por nome. Apenas os distritos da Assembleia Legislativa são cruzados, e somente onde a vintage 2021 ainda traz o nome do distrito; as fronteiras distritais de Victoria foram redivididas em 2001, 2013 e 2021, de modo que distritos de eleições anteriores frequentemente não têm correspondente na vintage 2021 e ficam nulos. Nulo em toda disputa do Conselho Legislativo.",
)

# Spliced verbatim into the six contest-level fact tables.
CONTEST_BLOCK = [
    YEAR,
    ELECTION_ID,
    CONTEST_ID,
    CHAMBER,
    GOVERNMENT_LEVEL,
    CONTEST_TYPE,
    VOTING_SYSTEM,
    DISTRICT_NAME,
    SED_ID,
]

BALLOT_POSITION = C(
    "ballot_position",
    "STRING",
    "Posição da pessoa candidata na cédula",
    "Position of the candidate on the ballot paper",
    "Posición de la persona candidata en la boleta",
    observations="Número de ordem na cédula; aritmética sobre ele não tem sentido, por isso é publicado como texto.",
)
BALLOT_NAME = C(
    "ballot_name",
    "STRING",
    "Nome da pessoa candidata tal como impresso na cédula",
    "Name of the candidate as printed on the ballot paper",
    "Nombre de la persona candidata tal como aparece impreso en la boleta",
    observations='Formato "SOBRENOME, Prenomes".',
)
PARTY_NAME = C(
    "party_name",
    "STRING",
    "Nome do partido da pessoa candidata",
    "Name of the candidate's party",
    "Nombre del partido de la persona candidata",
)
VOTES = votes(
    "votes",
    "Votos apurados para a pessoa candidata",
    "Votes counted for the candidate",
    "Votos escrutados para la persona candidata",
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
        observations="Chave da tabela. Assume valores como state2002, state2006, state2010, state2014, state2018, state2022, prahran_by2025 e nepean_by2026.",
    ),
    C(
        "election_name",
        "STRING",
        "Nome do evento eleitoral",
        "Name of the electoral event",
        "Nombre del evento electoral",
        observations='Assume valores como "State Election 2022".',
    ),
    C(
        "election_type",
        "STRING",
        "Tipo do evento eleitoral",
        "Type of the electoral event",
        "Tipo del evento electoral",
        covered_by_dictionary="yes",
        observations="Assume os valores state_general e state_by_election.",
    ),
    C(
        "government_level",
        "STRING",
        "Esfera de governo do evento eleitoral",
        "Level of government of the electoral event",
        "Nivel de gobierno del evento electoral",
        covered_by_dictionary="yes",
        observations="Assume o valor state.",
    ),
    C(
        "election_date",
        "DATE",
        "Data da votação",
        "Date of the poll",
        "Fecha de la votación",
    ),
    C(
        "source_url",
        "STRING",
        "Endereço da página de resultados publicada pela VEC para o evento",
        "URL of the results page published by the VEC for the event",
        "Dirección de la página de resultados publicada por la VEC para el evento",
    ),
]

TABLES["candidate"] = [
    *CONTEST_BLOCK,
    BALLOT_POSITION,
    BALLOT_NAME,
    C(
        "candidate_surname",
        "STRING",
        "Sobrenome da pessoa candidata",
        "Surname of the candidate",
        "Apellido de la persona candidata",
    ),
    C(
        "candidate_given_names",
        "STRING",
        "Prenomes da pessoa candidata",
        "Given names of the candidate",
        "Nombres de pila de la persona candidata",
    ),
    PARTY_NAME,
    C(
        "group_letter",
        "STRING",
        "Letra do grupo da pessoa candidata na cédula do Conselho Legislativo",
        "Group letter of the candidate on the Legislative Council ballot paper",
        "Letra del grupo de la persona candidata en la boleta del Consejo Legislativo",
        observations="Letra do grupo na cédula de votação por grupo do Conselho Legislativo. Nula nas disputas da Assembleia Legislativa.",
    ),
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
        "elected_order",
        "STRING",
        "Ordem em que a pessoa candidata foi eleita na disputa de múltiplas cadeiras do Conselho Legislativo",
        "Order in which the candidate was elected in the multi-member Legislative Council contest",
        "Orden en que la persona candidata fue electa en la disputa de múltiples escaños del Consejo Legislativo",
        observations="Número de ordem; aritmética sobre ele não tem sentido. Nulo fora das disputas de múltiplas cadeiras do Conselho Legislativo.",
    ),
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
    votes(
        "votes_total",
        "Total de votos apurados",
        "Total votes counted",
        "Total de votos escrutados",
    ),
    pct(
        "percentage_informal",
        "Votos inválidos como percentual do total apurado",
        "Informal votes as a share of the total count",
        "Votos inválidos como porcentaje del total escrutado",
    ),
    pct(
        "percentage_turnout",
        "Votos apurados como percentual das pessoas inscritas",
        "Votes counted as a share of enrolled electors",
        "Votos escrutados como porcentaje de las personas inscritas",
    ),
    votes(
        "quota",
        "Quota de votos necessária para eleger uma pessoa candidata",
        "Vote quota required to elect a candidate",
        "Cuota de votos necesaria para elegir a una persona candidata",
        obs="Quota do voto único transferível. Nula nas disputas da Assembleia Legislativa.",
    ),
    C(
        "seats_to_elect",
        "INT64",
        "Número de cadeiras a preencher na disputa",
        "Number of seats to be filled in the contest",
        "Número de escaños a ocupar en la disputa",
        measurement_unit="seat",
    ),
]

TABLES["result_district"] = [
    *CONTEST_BLOCK,
    C(
        "count_type",
        "STRING",
        "Tipo de apuração ao qual a linha se refere",
        "Type of count the row refers to",
        "Tipo de escrutinio al que se refiere la fila",
        covered_by_dictionary="yes",
        observations="Assume os valores first_preference, two_candidate_preferred e two_party_preferred. Filtre sempre por esta coluna: as três são apurações distintas da mesma disputa, e não recortes redundantes.",
    ),
    BALLOT_POSITION,
    BALLOT_NAME,
    PARTY_NAME,
    VOTES,
    pct(
        "percentage",
        "Votos da pessoa candidata como percentual do total apurado na disputa",
        "Votes for the candidate as a share of the total count in the contest",
        "Votos de la persona candidata como porcentaje del total escrutado en la disputa",
    ),
]

TABLES["result_voting_centre"] = [
    *CONTEST_BLOCK,
    C(
        "voting_centre_name",
        "STRING",
        "Nome do local de votação",
        "Name of the voting centre",
        "Nombre del local de votación",
    ),
    C(
        "vote_type",
        "STRING",
        "Tipo de voto ao qual a linha se refere",
        "Type of vote the row refers to",
        "Tipo de voto al que se refiere la fila",
        covered_by_dictionary="yes",
        observations="Assume os valores ordinary, absent, early, postal, provisional e marked_as_voted. Os votos ordinários são reportados por local de votação; os demais tipos de voto declarado são reportados uma única vez para toda a disputa e trazem o tipo de voto como nome do local de votação.",
    ),
    C(
        "count_type",
        "STRING",
        "Tipo de apuração ao qual a linha se refere",
        "Type of count the row refers to",
        "Tipo de escrutinio al que se refiere la fila",
        covered_by_dictionary="yes",
        observations="Assume os valores first_preference e two_candidate_preferred. Filtre sempre por esta coluna: as duas são apurações distintas da mesma disputa, e não recortes redundantes.",
    ),
    BALLOT_POSITION,
    BALLOT_NAME,
    PARTY_NAME,
    VOTES,
]

TABLES["distribution_of_preferences"] = [
    *CONTEST_BLOCK,
    C(
        "count_number",
        "STRING",
        "Número da contagem dentro da distribuição de preferências",
        "Number of the count within the distribution of preferences",
        "Número del conteo dentro de la distribución de preferencias",
        observations="Número de ordem; aritmética sobre ele não tem sentido.",
    ),
    C(
        "count_description",
        "STRING",
        "Transferência ou exclusão realizada pela contagem",
        "Transfer or exclusion the count performs",
        "Transferencia o exclusión que realiza el conteo",
    ),
    C(
        "transfer_value",
        "FLOAT64",
        "Valor pelo qual cada cédula é transferida na contagem",
        "Value at which each ballot paper is transferred in the count",
        "Valor al que se transfiere cada boleta en el conteo",
        measurement_unit="ratio",
    ),
    BALLOT_NAME,
    PARTY_NAME,
    C(
        "ballot_papers_transferred",
        "INT64",
        "Número de cédulas transferidas para a pessoa candidata na contagem",
        "Number of ballot papers transferred to the candidate in the count",
        "Número de boletas transferidas a la persona candidata en el conteo",
        measurement_unit="ballot_paper",
        observations="Nas disputas da Assembleia Legislativa toda cédula é transferida ao valor um, de modo que cédulas e votos transferidos são iguais; eles divergem apenas nas apurações por voto único transferível do Conselho Legislativo, em que um excedente é transferido a valor fracionário.",
    ),
    votes(
        "votes_transferred",
        "Votos transferidos para a pessoa candidata na contagem",
        "Votes transferred to the candidate in the count",
        "Votos transferidos a la persona candidata en el conteo",
    ),
    votes(
        "votes_progressive_total",
        "Total acumulado de votos da pessoa candidata ao fim da contagem",
        "Progressive total of votes for the candidate at the end of the count",
        "Total acumulado de votos de la persona candidata al final del conteo",
    ),
]

TABLES["disclosure_gift"] = [
    C(
        "year",
        "INT64",
        "Ano da doação",
        "Year of the donation",
        "Año de la donación",
        measurement_unit="year",
        directory_column=DIR_YEAR,
        observations="Coluna de particionamento, derivada de date_received quando presente e, caso contrário, de date_made; as duas datas nunca estão ausentes ao mesmo tempo.",
    ),
    C(
        "donation_id",
        "STRING",
        "Identificador da doação atribuído pelo registro de doações da VEC",
        "Identifier of the donation assigned by the VEC donation register",
        "Identificador de la donación asignado por el registro de donaciones de la VEC",
        observations="Chave da tabela. É o identificador que o registro atribui ao lançamento, e não colide entre os dois portais em que o registro está dividido.",
    ),
    C(
        "date_made",
        "DATE",
        "Data em que a doação foi feita",
        "Date the donation was made",
        "Fecha en que se realizó la donación",
        observations="Ausente nos 269 lançamentos cujo lado do doador não está reconciliado; o registro registra uma doação tanto pelo doador quanto pelo beneficiário, e um lado não reconciliado não fornece data.",
    ),
    C(
        "date_received",
        "DATE",
        "Data em que a doação foi recebida",
        "Date the donation was received",
        "Fecha en que se recibió la donación",
        observations="Ausente nos 70 lançamentos cujo lado do beneficiário não está reconciliado. Os lançamentos declarados antes de 1º de julho de 2020 trazem uma única data de doação, publicada aqui como date_received.",
    ),
    C(
        "donor_id",
        "STRING",
        "Identificador do doador atribuído pelo registro de doações da VEC",
        "Identifier of the donor assigned by the VEC donation register",
        "Identificador del donante asignado por el registro de donaciones de la VEC",
        observations="O registro atribui a cada doador e a cada beneficiário um identificador estável, de modo que as doações podem ser vinculadas entre anos sem cruzamento de texto livre; cerca de 2.813 doadores distintos aparecem, e o identificador é estável entre os dois portais em que o registro está dividido.",
    ),
    C(
        "donor_name",
        "STRING",
        "Nome do doador",
        "Name of the donor",
        "Nombre del donante",
    ),
    C(
        "donor_suburb",
        "STRING",
        "Subúrbio do doador",
        "Suburb of the donor",
        "Suburbio del donante",
    ),
    C(
        "donor_state",
        "STRING",
        "Estado ou território do doador",
        "State or territory of the donor",
        "Estado o territorio del donante",
    ),
    C(
        "recipient_id",
        "STRING",
        "Identificador do beneficiário atribuído pelo registro de doações da VEC",
        "Identifier of the recipient assigned by the VEC donation register",
        "Identificador del beneficiario asignado por el registro de donaciones de la VEC",
        observations="Cerca de 164 beneficiários distintos, compostos por pessoas candidatas, partidos políticos registrados e entidades nomeadas.",
    ),
    C(
        "recipient_name",
        "STRING",
        "Nome do beneficiário da doação",
        "Name of the recipient of the donation",
        "Nombre del beneficiario de la donación",
    ),
    C(
        "recipient_party_id",
        "STRING",
        "Identificador do partido político registrado ao qual a doação é atribuída",
        "Identifier of the registered political party the donation is attributed to",
        "Identificador del partido político registrado al que se atribuye la donación",
        observations="Cerca de 22 partidos registrados distintos; igual a recipient_id quando o próprio beneficiário é o partido registrado.",
    ),
    C(
        "recipient_party_name",
        "STRING",
        "Nome do partido político registrado ao qual a doação é atribuída",
        "Name of the registered political party the donation is attributed to",
        "Nombre del partido político registrado al que se atribuye la donación",
    ),
    aud(
        "gift_value",
        "Valor da doação em dólares australianos",
        "Value of the donation in Australian dollars",
        "Valor de la donación en dólares australianos",
    ),
    C(
        "donation_type",
        "STRING",
        "Tipo da doação",
        "Type of the donation",
        "Tipo de la donación",
        covered_by_dictionary="yes",
        observations="Assume os valores money, service, property e loan.",
    ),
    C(
        "disclosure_status",
        "STRING",
        "Situação de reconciliação da declaração da doação",
        "Reconciliation status of the donation disclosure",
        "Situación de reconciliación de la declaración de la donación",
        covered_by_dictionary="yes",
        observations="Assume os valores reconciled, donor_unreconciled e recipient_unreconciled. Uma doação é declarada tanto pelo doador quanto pelo beneficiário; um status não reconciliado significa que apenas um dos lados a declarou, e a data do lado ausente é nula.",
    ),
    C(
        "electorate_name",
        "STRING",
        "Nome do distrito eleitoral ao qual a doação é atribuída",
        "Name of the electorate the donation is attributed to",
        "Nombre del distrito electoral al que se atribuye la donación",
        observations="Preenchida em uma minoria dos lançamentos, 703 de 4.374.",
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

# Starts at 2002, the first state election covered by the VEC results archive, and
# runs well past the current coverage so a future refresh cannot drop rows into
# BigQuery's __UNPARTITIONED__ bucket, which is not a load error and is easy to miss.
PARTITION_RANGE = {"start": 2002, "end": 2035, "interval": 1}


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
    "Chave estrangeira para a tabela election, presente em todas as tabelas de resultados. Assume valores como state2002, state2022, prahran_by2025 e nepean_by2026.": (
        "Foreign key to the election table, present in every fact table. Takes values such as state2002, state2022, prahran_by2025 and nepean_by2026.",
        "Clave foránea hacia la tabla election, presente en todas las tablas de hechos. Toma valores como state2002, state2022, prahran_by2025 y nepean_by2026.",
    ),
    "Identifica uma única disputa por cadeira dentro do evento eleitoral. É o slug do distrito, da região ou da província, como em state2022-albert_park.": (
        "Identifies a single seat race within the electoral event. It is the slug of the district, region or province, as in state2022-albert_park.",
        "Identifica una única disputa por escaño dentro del evento electoral. Es el slug del distrito, de la región o de la provincia, como en state2022-albert_park.",
    ),
    "Assume os valores legislative_assembly e legislative_council. A Assembleia Legislativa é disputada por distrito de cadeira única; o Conselho Legislativo foi disputado por província de cadeira única até 2002 e por região de múltiplas cadeiras a partir de 2006.": (
        "Takes the values legislative_assembly and legislative_council. The Legislative Assembly is contested by single-member district; the Legislative Council was contested by single-member province until 2002 and by multi-member region from 2006.",
        "Toma los valores legislative_assembly y legislative_council. La Asamblea Legislativa se disputa por distrito de escaño único; el Consejo Legislativo se disputó por provincia de escaño único hasta 2002 y por región de múltiples escaños a partir de 2006.",
    ),
    "Assume o valor state. A coluna é definida por disputa, e não como constante do conjunto, para que as disputas de governo local, também administradas pela VEC e ainda não incluídas, possam ser acrescentadas a estas mesmas tabelas sem reestruturá-las.": (
        "Takes the value state. The column is defined per contest rather than as a dataset constant so that local government contests, which the VEC also administers and which are not yet included, can be added to these same tables without restructuring them.",
        "Toma el valor state. La columna se define por disputa, y no como constante del conjunto, para que las disputas de gobierno local, que la VEC también administra y que aún no están incluidas, puedan agregarse a estas mismas tablas sin reestructurarlas.",
    ),
    "Assume os valores state_district, state_region e state_province.": (
        "Takes the values state_district, state_region and state_province.",
        "Toma los valores state_district, state_region y state_province.",
    ),
    "Assume os valores compulsory_preferential e single_transferable_vote. É definido por disputa: a Assembleia Legislativa e as províncias do Conselho Legislativo anteriores a 2006 usam voto preferencial obrigatório para uma única cadeira, e as regiões do Conselho Legislativo a partir de 2006 usam o voto único transferível.": (
        "Takes the values compulsory_preferential and single_transferable_vote. It is defined per contest: the Legislative Assembly and the pre-2006 Legislative Council provinces use compulsory preferential voting for a single seat, and the Legislative Council regions from 2006 use the single transferable vote.",
        "Toma los valores compulsory_preferential y single_transferable_vote. Se define por disputa: la Asamblea Legislativa y las provincias del Consejo Legislativo anteriores a 2006 usan voto preferencial obligatorio para un único escaño, y las regiones del Consejo Legislativo a partir de 2006 usan el voto único transferible.",
    ),
    "Código ASGS 2021 do ABS para o distrito eleitoral estadual, cruzado por nome. Apenas os distritos da Assembleia Legislativa são cruzados, e somente onde a vintage 2021 ainda traz o nome do distrito; as fronteiras distritais de Victoria foram redivididas em 2001, 2013 e 2021, de modo que distritos de eleições anteriores frequentemente não têm correspondente na vintage 2021 e ficam nulos. Nulo em toda disputa do Conselho Legislativo.": (
        "ABS ASGS 2021 code of the state electoral division, matched by name. Only Legislative Assembly districts are matched, and only where the 2021 vintage still carries the district's name; Victorian district boundaries were redivided in 2001, 2013 and 2021, so districts from earlier elections frequently have no counterpart in the 2021 vintage and are left null. Null for every Legislative Council contest.",
        "Código ASGS 2021 del ABS del distrito electoral estatal, cruzado por nombre. Solo se cruzan los distritos de la Asamblea Legislativa, y únicamente donde la vintage 2021 aún trae el nombre del distrito; las fronteras distritales de Victoria fueron redivididas en 2001, 2013 y 2021, de modo que los distritos de elecciones anteriores frecuentemente no tienen contraparte en la vintage 2021 y quedan nulos. Nulo en toda disputa del Consejo Legislativo.",
    ),
    "Chave da tabela. Assume valores como state2002, state2006, state2010, state2014, state2018, state2022, prahran_by2025 e nepean_by2026.": (
        "Key of the table. Takes values such as state2002, state2006, state2010, state2014, state2018, state2022, prahran_by2025 and nepean_by2026.",
        "Clave de la tabla. Toma valores como state2002, state2006, state2010, state2014, state2018, state2022, prahran_by2025 y nepean_by2026.",
    ),
    'Assume valores como "State Election 2022".': (
        'Takes values such as "State Election 2022".',
        'Toma valores como "State Election 2022".',
    ),
    "Assume os valores state_general e state_by_election.": (
        "Takes the values state_general and state_by_election.",
        "Toma los valores state_general y state_by_election.",
    ),
    "Assume o valor state.": (
        "Takes the value state.",
        "Toma el valor state.",
    ),
    "Número de ordem na cédula; aritmética sobre ele não tem sentido, por isso é publicado como texto.": (
        "Ballot sequence number; arithmetic on it is meaningless, which is why it is published as text.",
        "Número de orden en la boleta; la aritmética sobre él no tiene sentido, por eso se publica como texto.",
    ),
    'Formato "SOBRENOME, Prenomes".': (
        'Format "SURNAME, Given names".',
        'Formato "APELLIDO, Nombres de pila".',
    ),
    "Letra do grupo na cédula de votação por grupo do Conselho Legislativo. Nula nas disputas da Assembleia Legislativa.": (
        "Group letter on the Legislative Council group voting ticket. Null in Legislative Assembly contests.",
        "Letra del grupo en la boleta de votación por grupo del Consejo Legislativo. Nula en las disputas de la Asamblea Legislativa.",
    ),
    "Assume os valores yes e no.": (
        "Takes the values yes and no.",
        "Toma los valores yes y no.",
    ),
    "Número de ordem; aritmética sobre ele não tem sentido. Nulo fora das disputas de múltiplas cadeiras do Conselho Legislativo.": (
        "Sequence number; arithmetic on it is meaningless. Null outside the multi-member Legislative Council contests.",
        "Número de orden; la aritmética sobre él no tiene sentido. Nulo fuera de las disputas de múltiples escaños del Consejo Legislativo.",
    ),
    "Quota do voto único transferível. Nula nas disputas da Assembleia Legislativa.": (
        "Single transferable vote quota. Null in Legislative Assembly contests.",
        "Cuota del voto único transferible. Nula en las disputas de la Asamblea Legislativa.",
    ),
    "Assume os valores first_preference, two_candidate_preferred e two_party_preferred. Filtre sempre por esta coluna: as três são apurações distintas da mesma disputa, e não recortes redundantes.": (
        "Takes the values first_preference, two_candidate_preferred and two_party_preferred. Always filter on it: the three are different counts of the same contest, not redundant snapshots.",
        "Toma los valores first_preference, two_candidate_preferred y two_party_preferred. Filtre siempre por esta columna: los tres son escrutinios distintos de la misma disputa, y no recortes redundantes.",
    ),
    "Assume os valores ordinary, absent, early, postal, provisional e marked_as_voted. Os votos ordinários são reportados por local de votação; os demais tipos de voto declarado são reportados uma única vez para toda a disputa e trazem o tipo de voto como nome do local de votação.": (
        "Takes the values ordinary, absent, early, postal, provisional and marked_as_voted. Ordinary votes are reported per voting centre; the remaining declaration vote types are reported once for the whole contest and carry the vote type as the voting centre name.",
        "Toma los valores ordinary, absent, early, postal, provisional y marked_as_voted. Los votos ordinarios se reportan por local de votación; los demás tipos de voto declarado se reportan una única vez para toda la disputa y traen el tipo de voto como nombre del local de votación.",
    ),
    "Assume os valores first_preference e two_candidate_preferred. Filtre sempre por esta coluna: as duas são apurações distintas da mesma disputa, e não recortes redundantes.": (
        "Takes the values first_preference and two_candidate_preferred. Always filter on it: the two are different counts of the same contest, not redundant snapshots.",
        "Toma los valores first_preference y two_candidate_preferred. Filtre siempre por esta columna: los dos son escrutinios distintos de la misma disputa, y no recortes redundantes.",
    ),
    "Número de ordem; aritmética sobre ele não tem sentido.": (
        "Sequence number; arithmetic on it is meaningless.",
        "Número de orden; la aritmética sobre él no tiene sentido.",
    ),
    "Nas disputas da Assembleia Legislativa toda cédula é transferida ao valor um, de modo que cédulas e votos transferidos são iguais; eles divergem apenas nas apurações por voto único transferível do Conselho Legislativo, em que um excedente é transferido a valor fracionário.": (
        "In Legislative Assembly contests every ballot paper transfers at value one, so ballot papers and votes transferred are equal; they diverge only in Legislative Council single transferable vote counts, where a surplus transfers at a fractional value.",
        "En las disputas de la Asamblea Legislativa toda boleta se transfiere al valor uno, de modo que boletas y votos transferidos son iguales; divergen solo en los escrutinios por voto único transferible del Consejo Legislativo, en los que un excedente se transfiere a valor fraccionario.",
    ),
    "Coluna de particionamento, derivada de date_received quando presente e, caso contrário, de date_made; as duas datas nunca estão ausentes ao mesmo tempo.": (
        "Partition column, derived from date_received where present and otherwise from date_made; the two dates are never both absent.",
        "Columna de particionamiento, derivada de date_received cuando está presente y, en caso contrario, de date_made; las dos fechas nunca están ausentes al mismo tiempo.",
    ),
    "Chave da tabela. É o identificador que o registro atribui ao lançamento, e não colide entre os dois portais em que o registro está dividido.": (
        "Key of the table. It is the identifier the register assigns the record, and it does not collide between the two portals the register is split across.",
        "Clave de la tabla. Es el identificador que el registro asigna al asiento, y no colisiona entre los dos portales en los que el registro está dividido.",
    ),
    "Ausente nos 269 lançamentos cujo lado do doador não está reconciliado; o registro registra uma doação tanto pelo doador quanto pelo beneficiário, e um lado não reconciliado não fornece data.": (
        "Absent on the 269 records whose donor side is unreconciled; the register records a donation from both the donor and the recipient, and an unreconciled side supplies no date.",
        "Ausente en los 269 asientos cuyo lado del donante no está reconciliado; el registro registra una donación tanto por el donante como por el beneficiario, y un lado no reconciliado no aporta fecha.",
    ),
    "Ausente nos 70 lançamentos cujo lado do beneficiário não está reconciliado. Os lançamentos declarados antes de 1º de julho de 2020 trazem uma única data de doação, publicada aqui como date_received.": (
        "Absent on the 70 records whose recipient side is unreconciled. Records disclosed before 1 July 2020 carry a single donation date, published here as date_received.",
        "Ausente en los 70 asientos cuyo lado del beneficiario no está reconciliado. Los asientos declarados antes del 1 de julio de 2020 traen una única fecha de donación, publicada aquí como date_received.",
    ),
    "O registro atribui a cada doador e a cada beneficiário um identificador estável, de modo que as doações podem ser vinculadas entre anos sem cruzamento de texto livre; cerca de 2.813 doadores distintos aparecem, e o identificador é estável entre os dois portais em que o registro está dividido.": (
        "The register assigns every donor and recipient a stable identifier, so donations can be linked across years without matching free text; about 2,813 distinct donors appear, and the identifier is stable across the two portals the register is split across.",
        "El registro asigna a cada donante y a cada beneficiario un identificador estable, de modo que las donaciones pueden vincularse entre años sin cruzar texto libre; aparecen cerca de 2.813 donantes distintos, y el identificador es estable entre los dos portales en los que el registro está dividido.",
    ),
    "Cerca de 164 beneficiários distintos, compostos por pessoas candidatas, partidos políticos registrados e entidades nomeadas.": (
        "About 164 distinct recipients, comprising candidates, registered political parties and nominated entities.",
        "Cerca de 164 beneficiarios distintos, compuestos por personas candidatas, partidos políticos registrados y entidades nominadas.",
    ),
    "Cerca de 22 partidos registrados distintos; igual a recipient_id quando o próprio beneficiário é o partido registrado.": (
        "About 22 distinct registered parties; equal to recipient_id when the recipient is itself the registered party.",
        "Cerca de 22 partidos registrados distintos; igual a recipient_id cuando el propio beneficiario es el partido registrado.",
    ),
    "Assume os valores money, service, property e loan.": (
        "Takes the values money, service, property and loan.",
        "Toma los valores money, service, property y loan.",
    ),
    "Assume os valores reconciled, donor_unreconciled e recipient_unreconciled. Uma doação é declarada tanto pelo doador quanto pelo beneficiário; um status não reconciliado significa que apenas um dos lados a declarou, e a data do lado ausente é nula.": (
        "Takes the values reconciled, donor_unreconciled and recipient_unreconciled. A donation is disclosed by both the donor and the recipient; an unreconciled status means only one side has disclosed it, and the date for the missing side is null.",
        "Toma los valores reconciled, donor_unreconciled y recipient_unreconciled. Una donación es declarada tanto por el donante como por el beneficiario; un estado no reconciliado significa que solo uno de los lados la declaró, y la fecha del lado ausente es nula.",
    ),
    "Preenchida em uma minoria dos lançamentos, 703 de 4.374.": (
        "Populated on a minority of records, 703 of 4,374.",
        "Poblada en una minoría de los asientos, 703 de 4.374.",
    ),
}


def validate() -> None:
    """Fail loudly on a duplicated column name within a table, a typed quantity
    published without a measurement unit, or an observation note with no EN/ES
    translation."""
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
