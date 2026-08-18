"""Generate the architecture CSVs for us_fec_campaign_finance.

    uv run python build_architecture.py

Writes one CSV per table into this directory. The CSVs are the source of truth for
column names, order, and BigQuery types: clean.py, gen_dbt.py and the metadata step
all read them.

Design notes
------------
* Column names are English, because the data and its documentation are English
  (.claude/rules/data-basis-style.md).
* The partition column is ``cycle`` (INT64): the FEC publishes bulk files per
  two-year election cycle, labelled by the even year in which the cycle ends.
  It is deliberately *not* linked to br_bd_diretorios_data_tempo.ano, because a
  cycle is a two-year period, not a year.
* Types follow arithmetic meaning: only ``transaction_amount`` (USD) and the year
  columns are numeric. Every FEC code, identifier and flag is STRING, and the
  coded ones carry covered_by_dictionary=yes.
* ``sub_id``, ``file_number`` and ``line_num`` look numeric in the source but are
  record identifiers / form line labels — summing them is meaningless, so STRING.
* directory_column is set only on FEC-controlled state fields. Filer-entered
  address states (contributor/payee/mailing) include foreign and military codes
  and free-text errors, so they are left unlinked and documented instead.
"""

import csv
from pathlib import Path

HERE = Path(__file__).resolve().parent

HEADER = [
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
    "description_en",
    "description_es",
]

STATE_DIR = "br_bd_diretorios_us.state:abbreviation"

DIRTY_STATE_NOTE = (
    "Preenchido pelo declarante; inclui códigos estrangeiros, militares (AA, AE, AP) "
    "e erros de digitação, por isso não é ligado ao diretório de estados."
)


def col(
    name,
    bq_type,
    desc_pt,
    desc_en,
    desc_es,
    *,
    dictionary="no",
    directory="",
    unit="",
    sensitive="no",
    obs="",
    original="",
    coverage="",
):
    return {
        "name": name,
        "bigquery_type": bq_type,
        "description": desc_pt,
        "temporal_coverage": coverage,
        "covered_by_dictionary": dictionary,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": sensitive,
        "observations": obs,
        "original_name": original,
        "description_en": desc_en,
        "description_es": desc_es,
    }


# --------------------------------------------------------------------------- #
# Shared column builders — the transaction files share most of their layout.
# --------------------------------------------------------------------------- #

CYCLE = col(
    "cycle",
    "INT64",
    "Ciclo eleitoral de dois anos, identificado pelo ano par em que termina",
    "Two-year election cycle, identified by the even year in which it ends",
    "Ciclo electoral de dos años, identificado por el año par en que termina",
    unit="year",
    obs="Coluna de partição. Corresponde ao arquivo em massa da FEC publicado "
    "para aquele ciclo; o ciclo é rotulado pelo ano par em que termina, de modo "
    "que cycle=2026 cobre as transações de 2025 e 2026.",
)


def cycle_for(first_year):
    c = dict(CYCLE)
    c["temporal_coverage"] = f"{first_year}(2)2026"
    return c


def txn_common(
    counterparty_prefix,
    counterparty_label_pt,
    counterparty_label_en,
    counterparty_label_es,
    sensitive="no",
):
    """The contributor/counterparty block shared by indiv, pas2 and oth."""
    p = counterparty_prefix
    return [
        col(
            f"{p}_name",
            "STRING",
            f"Nome do {counterparty_label_pt}",
            f"Name of the {counterparty_label_en}",
            f"Nombre del {counterparty_label_es}",
            sensitive=sensitive,
            original="NAME",
        ),
        col(
            f"{p}_city",
            "STRING",
            f"Cidade do {counterparty_label_pt}",
            f"City of the {counterparty_label_en}",
            f"Ciudad del {counterparty_label_es}",
            sensitive=sensitive,
            original="CITY",
        ),
        col(
            f"{p}_state",
            "STRING",
            f"Sigla de duas letras do estado do {counterparty_label_pt}",
            f"Two-letter state code of the {counterparty_label_en}",
            f"Código de dos letras del estado del {counterparty_label_es}",
            sensitive=sensitive,
            obs=DIRTY_STATE_NOTE,
            original="STATE",
        ),
        col(
            f"{p}_zip_code",
            "STRING",
            f"Código postal (ZIP) do {counterparty_label_pt}",
            f"ZIP code of the {counterparty_label_en}",
            f"Código postal (ZIP) del {counterparty_label_es}",
            sensitive=sensitive,
            original="ZIP_CODE",
        ),
        col(
            f"{p}_employer",
            "STRING",
            f"Empregador do {counterparty_label_pt}",
            f"Employer of the {counterparty_label_en}",
            f"Empleador del {counterparty_label_es}",
            sensitive=sensitive,
            original="EMPLOYER",
        ),
        col(
            f"{p}_occupation",
            "STRING",
            f"Ocupação do {counterparty_label_pt}",
            f"Occupation of the {counterparty_label_en}",
            f"Ocupación del {counterparty_label_es}",
            sensitive=sensitive,
            original="OCCUPATION",
        ),
    ]


def txn_codes():
    return [
        col(
            "amendment_indicator",
            "STRING",
            "Indica se o relatório é novo, uma emenda ou uma rescisão",
            "Indicates whether the report is new, an amendment or a termination",
            "Indica si el informe es nuevo, una enmienda o una rescisión",
            dictionary="yes",
            original="AMNDT_IND",
        ),
        col(
            "report_type",
            "STRING",
            "Tipo de relatório apresentado à FEC",
            "Type of report filed with the FEC",
            "Tipo de informe presentado ante la FEC",
            dictionary="yes",
            original="RPT_TP",
        ),
        col(
            "election_type_year",
            "STRING",
            "Tipo de eleição e ano ao qual a transação se refere",
            "Election type and year the transaction refers to",
            "Tipo de elección y año al que se refiere la transacción",
            obs="Código composto: uma letra de tipo de eleição (P primária, G geral, "
            "O outra, C convenção, R segundo turno, S especial, E recontagem) seguida "
            "do ano com quatro dígitos, por exemplo P2026. Valores antigos podem estar "
            "irregulares.",
            original="TRANSACTION_PGI",
        ),
        col(
            "transaction_type",
            "STRING",
            "Código do tipo de transação",
            "Transaction type code",
            "Código del tipo de transacción",
            dictionary="yes",
            original="TRANSACTION_TP",
        ),
        col(
            "entity_type",
            "STRING",
            "Tipo de entidade da contraparte (candidato, comitê, indivíduo, organização)",
            "Entity type of the counterparty (candidate, committee, individual, organization)",
            "Tipo de entidad de la contraparte (candidato, comité, individuo, organización)",
            dictionary="yes",
            original="ENTITY_TP",
        ),
    ]


def txn_tail():
    return [
        col(
            "transaction_date",
            "DATE",
            "Data da transação",
            "Date of the transaction",
            "Fecha de la transacción",
            obs="A FEC publica a data como declarada, e há erros de digitação: os dados brutos trazem datas de 1899 a 2202. Datas fora da janela plausível (anteriores à criação da FEC em 1975 ou mais de um ano após o fim do próprio ciclo) são gravadas como nulas; apenas a data é descartada, a linha é preservada. São cerca de 128 linhas em 79 milhões.",
            original="TRANSACTION_DT",
        ),
        col(
            "transaction_amount",
            "FLOAT64",
            "Valor da transação em dólares",
            "Amount of the transaction in dollars",
            "Monto de la transacción en dólares",
            unit="USD",
            original="TRANSACTION_AMT",
        ),
        col(
            "memo_code",
            "STRING",
            "Indicador de memorando; X sinaliza que o valor não entra no total do relatório",
            "Memo indicator; X flags that the amount is not included in the report total",
            "Indicador de memorando; X señala que el monto no entra en el total del informe",
            dictionary="yes",
            original="MEMO_CD",
        ),
        col(
            "memo_text",
            "STRING",
            "Descrição textual da atividade informada no memorando",
            "Free-text description of the activity reported in the memo",
            "Descripción textual de la actividad informada en el memorando",
            original="MEMO_TEXT",
        ),
    ]


def txn_ids(extra=()):
    base = [
        col(
            "committee_id",
            "STRING",
            "Identificador de nove caracteres do comitê declarante atribuído pela FEC",
            "Nine-character identifier of the filing committee assigned by the FEC",
            "Identificador de nueve caracteres del comité declarante asignado por la FEC",
            original="CMTE_ID",
        ),
    ]
    base.extend(extra)
    base.extend(
        [
            col(
                "transaction_id",
                "STRING",
                "Identificador da transação, único por comitê e por relatório",
                "Transaction identifier, unique per committee and per report",
                "Identificador de la transacción, único por comité y por informe",
                original="TRAN_ID",
            ),
            col(
                "sub_id",
                "STRING",
                "Número do registro na FEC, único em toda a base",
                "FEC record number, unique across the entire database",
                "Número de registro en la FEC, único en toda la base",
                obs="Identificador; aritmética sobre ele não tem significado, por isso STRING.",
                original="SUB_ID",
            ),
            col(
                "file_number",
                "STRING",
                "Identificador do relatório em que a transação foi informada",
                "Identifier of the report in which the transaction was filed",
                "Identificador del informe en el que se declaró la transacción",
                original="FILE_NUM",
            ),
            col(
                "image_number",
                "STRING",
                "Número da imagem digitalizada do documento na FEC",
                "Number of the scanned document image at the FEC",
                "Número de la imagen escaneada del documento en la FEC",
                original="IMAGE_NUM",
            ),
        ]
    )
    return base


OTHER_ID = col(
    "other_id",
    "STRING",
    "Identificador FEC da contraparte quando ela é um comitê ou candidato registrado",
    "FEC identifier of the counterparty when it is a registered committee or candidate",
    "Identificador FEC de la contraparte cuando es un comité o candidato registrado",
    original="OTHER_ID",
)


# --------------------------------------------------------------------------- #
# Tables
# --------------------------------------------------------------------------- #

TABLES = {}

TABLES["candidate"] = [
    cycle_for(1980),
    col(
        "candidate_id",
        "STRING",
        "Identificador de nove caracteres do candidato atribuído pela FEC",
        "Nine-character candidate identifier assigned by the FEC",
        "Identificador de nueve caracteres del candidato asignado por la FEC",
        original="CAND_ID",
    ),
    col(
        "principal_committee_id",
        "STRING",
        "Identificador do comitê de campanha principal do candidato no ciclo",
        "Identifier of the candidate's principal campaign committee in the cycle",
        "Identificador del comité de campaña principal del candidato en el ciclo",
        original="CAND_PCC",
    ),
    col(
        "candidate_name",
        "STRING",
        "Nome do candidato conforme registrado na FEC",
        "Name of the candidate as registered with the FEC",
        "Nombre del candidato según registrado en la FEC",
        original="CAND_NAME",
    ),
    col(
        "party",
        "STRING",
        "Partido declarado pelo candidato",
        "Party reported by the candidate",
        "Partido declarado por el candidato",
        dictionary="yes",
        obs="Campo preenchido pelo declarante: cerca de 99% dos valores seguem a lista de códigos de partido da FEC, registrada no dicionário, mas o restante traz variantes não canônicas (por exemplo GOP, Rep, siglas de estado).",
        original="CAND_PTY_AFFILIATION",
    ),
    col(
        "election_year",
        "INT64",
        "Ano da eleição informado na declaração de candidatura",
        "Election year reported on the statement of candidacy",
        "Año de la elección informado en la declaración de candidatura",
        unit="year",
        original="CAND_ELECTION_YR",
    ),
    col(
        "office",
        "STRING",
        "Cargo disputado: H (Câmara), S (Senado) ou P (Presidência)",
        "Office sought: H (House), S (Senate) or P (President)",
        "Cargo disputado: H (Cámara), S (Senado) o P (Presidencia)",
        dictionary="yes",
        original="CAND_OFFICE",
    ),
    col(
        "office_state",
        "STRING",
        "Estado do cargo disputado",
        "State of the office sought",
        "Estado del cargo disputado",
        directory=STATE_DIR,
        obs="Assume o valor US nas candidaturas presidenciais.",
        original="CAND_OFFICE_ST",
    ),
    col(
        "office_district",
        "STRING",
        "Distrito eleitoral do cargo disputado; 00 para Senado e Presidência",
        "Electoral district of the office sought; 00 for Senate and President",
        "Distrito electoral del cargo disputado; 00 para Senado y Presidencia",
        obs="Rótulo de dois dígitos com zero à esquerda, não uma quantidade.",
        original="CAND_OFFICE_DISTRICT",
    ),
    col(
        "incumbent_challenger_status",
        "STRING",
        "Situação do candidato: I (incumbente), C (desafiante) ou O (cadeira aberta)",
        "Candidate status: I (incumbent), C (challenger) or O (open seat)",
        "Situación del candidato: I (titular), C (retador) u O (escaño abierto)",
        dictionary="yes",
        original="CAND_ICI",
    ),
    col(
        "candidate_status",
        "STRING",
        "Situação do registro do candidato no ciclo",
        "Status of the candidate's registration in the cycle",
        "Situación del registro del candidato en el ciclo",
        dictionary="yes",
        original="CAND_STATUS",
    ),
    col(
        "address_1",
        "STRING",
        "Primeira linha do endereço postal do candidato",
        "First line of the candidate's mailing address",
        "Primera línea de la dirección postal del candidato",
        original="CAND_ST1",
    ),
    col(
        "address_2",
        "STRING",
        "Segunda linha do endereço postal do candidato",
        "Second line of the candidate's mailing address",
        "Segunda línea de la dirección postal del candidato",
        original="CAND_ST2",
    ),
    col(
        "city",
        "STRING",
        "Cidade do endereço postal do candidato",
        "City of the candidate's mailing address",
        "Ciudad de la dirección postal del candidato",
        original="CAND_CITY",
    ),
    col(
        "state",
        "STRING",
        "Sigla do estado do endereço postal do candidato",
        "State code of the candidate's mailing address",
        "Código del estado de la dirección postal del candidato",
        obs=DIRTY_STATE_NOTE,
        original="CAND_ST",
    ),
    col(
        "zip_code",
        "STRING",
        "Código postal (ZIP) do endereço do candidato",
        "ZIP code of the candidate's address",
        "Código postal (ZIP) de la dirección del candidato",
        original="CAND_ZIP",
    ),
]

TABLES["committee"] = [
    cycle_for(1980),
    col(
        "committee_id",
        "STRING",
        "Identificador de nove caracteres do comitê atribuído pela FEC",
        "Nine-character committee identifier assigned by the FEC",
        "Identificador de nueve caracteres del comité asignado por la FEC",
        original="CMTE_ID",
    ),
    col(
        "candidate_id",
        "STRING",
        "Identificador do candidato associado, quando o comitê é de campanha",
        "Identifier of the associated candidate, when the committee is a campaign committee",
        "Identificador del candidato asociado, cuando el comité es de campaña",
        original="CAND_ID",
    ),
    col(
        "committee_name",
        "STRING",
        "Nome do comitê conforme registrado na FEC",
        "Name of the committee as registered with the FEC",
        "Nombre del comité según registrado en la FEC",
        original="CMTE_NM",
    ),
    col(
        "treasurer_name",
        "STRING",
        "Nome do tesoureiro registrado do comitê",
        "Name of the committee's registered treasurer",
        "Nombre del tesorero registrado del comité",
        original="TRES_NM",
    ),
    col(
        "committee_designation",
        "STRING",
        "Designação do comitê (principal, autorizado, PAC de liderança, entre outras)",
        "Committee designation (principal, authorized, leadership PAC, among others)",
        "Designación del comité (principal, autorizado, PAC de liderazgo, entre otras)",
        dictionary="yes",
        original="CMTE_DSGN",
    ),
    col(
        "committee_type",
        "STRING",
        "Tipo de comitê (campanha da Câmara, Senado, presidencial, PAC, partido, Super PAC)",
        "Committee type (House, Senate or presidential campaign, PAC, party, Super PAC)",
        "Tipo de comité (campaña de la Cámara, Senado, presidencial, PAC, partido, Super PAC)",
        dictionary="yes",
        original="CMTE_TP",
    ),
    col(
        "party",
        "STRING",
        "Partido ao qual o comitê é filiado",
        "Party the committee is affiliated with",
        "Partido al que el comité está afiliado",
        dictionary="yes",
        obs="Campo preenchido pelo declarante: cerca de 99% dos valores seguem a lista de códigos de partido da FEC, registrada no dicionário, mas o restante traz variantes não canônicas (por exemplo GOP, Rep, siglas de estado).",
        original="CMTE_PTY_AFFILIATION",
    ),
    col(
        "filing_frequency",
        "STRING",
        "Frequência de apresentação de relatórios do comitê",
        "Frequency with which the committee files reports",
        "Frecuencia con que el comité presenta informes",
        dictionary="yes",
        original="CMTE_FILING_FREQ",
    ),
    col(
        "organization_type",
        "STRING",
        "Categoria do grupo de interesse por trás do comitê",
        "Interest group category behind the committee",
        "Categoría del grupo de interés detrás del comité",
        dictionary="yes",
        original="ORG_TP",
    ),
    col(
        "connected_organization_name",
        "STRING",
        "Nome da organização conectada ou patrocinadora do comitê",
        "Name of the organization connected to or sponsoring the committee",
        "Nombre de la organización conectada o patrocinadora del comité",
        original="CONNECTED_ORG_NM",
    ),
    col(
        "address_1",
        "STRING",
        "Primeira linha do endereço postal do comitê",
        "First line of the committee's mailing address",
        "Primera línea de la dirección postal del comité",
        original="CMTE_ST1",
    ),
    col(
        "address_2",
        "STRING",
        "Segunda linha do endereço postal do comitê",
        "Second line of the committee's mailing address",
        "Segunda línea de la dirección postal del comité",
        original="CMTE_ST2",
    ),
    col(
        "city",
        "STRING",
        "Cidade do endereço postal do comitê",
        "City of the committee's mailing address",
        "Ciudad de la dirección postal del comité",
        original="CMTE_CITY",
    ),
    col(
        "state",
        "STRING",
        "Sigla do estado do endereço postal do comitê",
        "State code of the committee's mailing address",
        "Código del estado de la dirección postal del comité",
        obs=DIRTY_STATE_NOTE,
        original="CMTE_ST",
    ),
    col(
        "zip_code",
        "STRING",
        "Código postal (ZIP) do endereço do comitê",
        "ZIP code of the committee's address",
        "Código postal (ZIP) de la dirección del comité",
        original="CMTE_ZIP",
    ),
]

TABLES["candidate_committee_link"] = [
    cycle_for(2010),
    col(
        "candidate_id",
        "STRING",
        "Identificador do candidato",
        "Candidate identifier",
        "Identificador del candidato",
        original="CAND_ID",
    ),
    col(
        "committee_id",
        "STRING",
        "Identificador do comitê ligado ao candidato",
        "Identifier of the committee linked to the candidate",
        "Identificador del comité vinculado al candidato",
        original="CMTE_ID",
    ),
    col(
        "linkage_id",
        "STRING",
        "Identificador da ligação entre candidato e comitê",
        "Identifier of the candidate-committee linkage",
        "Identificador del vínculo entre candidato y comité",
        original="LINKAGE_ID",
    ),
    col(
        "candidate_election_year",
        "INT64",
        "Ano da eleição do candidato",
        "Election year of the candidate",
        "Año de la elección del candidato",
        unit="year",
        original="CAND_ELECTION_YR",
    ),
    col(
        "fec_election_year",
        "INT64",
        "Ano do ciclo eleitoral de dois anos ao qual a ligação se refere",
        "Year of the two-year election cycle the linkage refers to",
        "Año del ciclo electoral de dos años al que se refiere el vínculo",
        unit="year",
        original="FEC_ELECTION_YR",
    ),
    col(
        "committee_type",
        "STRING",
        "Tipo do comitê ligado ao candidato",
        "Type of the committee linked to the candidate",
        "Tipo del comité vinculado al candidato",
        dictionary="yes",
        original="CMTE_TP",
    ),
    col(
        "committee_designation",
        "STRING",
        "Designação do comitê ligado ao candidato",
        "Designation of the committee linked to the candidate",
        "Designación del comité vinculado al candidato",
        dictionary="yes",
        original="CMTE_DSGN",
    ),
]

TABLES["contribution_individual"] = [
    cycle_for(1980),
    *txn_ids(extra=[OTHER_ID]),
    *txn_codes(),
    *txn_common(
        "contributor",
        "contribuinte",
        "contributor",
        "contribuyente",
        sensitive="yes",
    ),
    *txn_tail(),
]

TABLES["contribution_committee"] = [
    cycle_for(1980),
    *txn_ids(
        extra=[
            OTHER_ID,
            col(
                "candidate_id",
                "STRING",
                "Identificador do candidato beneficiário da contribuição ou da despesa",
                "Identifier of the candidate benefiting from the contribution or expenditure",
                "Identificador del candidato beneficiario de la contribución o del gasto",
                original="CAND_ID",
            ),
        ]
    ),
    *txn_codes(),
    *txn_common(
        "contributor",
        "comitê contribuinte",
        "contributing committee",
        "comité contribuyente",
    ),
    *txn_tail(),
]

TABLES["committee_transaction"] = [
    cycle_for(1980),
    *txn_ids(extra=[OTHER_ID]),
    *txn_codes(),
    *txn_common(
        "counterparty",
        "comitê ou entidade contraparte",
        "counterparty committee or entity",
        "comité o entidad contraparte",
    ),
    *txn_tail(),
]

TABLES["disbursement"] = [
    cycle_for(2004),
    *txn_ids(
        extra=[
            col(
                "back_reference_transaction_id",
                "STRING",
                "Identificador da transação relacionada dentro do mesmo relatório",
                "Identifier of the related transaction within the same report",
                "Identificador de la transacción relacionada dentro del mismo informe",
                original="BACK_REF_TRAN_ID",
            )
        ]
    ),
    col(
        "amendment_indicator",
        "STRING",
        "Indica se o relatório é novo, uma emenda ou uma rescisão",
        "Indicates whether the report is new, an amendment or a termination",
        "Indica si el informe es nuevo, una enmienda o una rescisión",
        dictionary="yes",
        original="AMNDT_IND",
    ),
    col(
        "report_year",
        "INT64",
        "Ano do relatório em que a despesa foi informada",
        "Year of the report in which the disbursement was filed",
        "Año del informe en que se declaró el gasto",
        unit="year",
        original="RPT_YR",
    ),
    col(
        "report_type",
        "STRING",
        "Tipo de relatório apresentado à FEC",
        "Type of report filed with the FEC",
        "Tipo de informe presentado ante la FEC",
        dictionary="yes",
        original="RPT_TP",
    ),
    col(
        "line_number",
        "STRING",
        "Número da linha do formulário da FEC em que a despesa foi informada",
        "Line number of the FEC form on which the disbursement was reported",
        "Número de línea del formulario de la FEC en que se declaró el gasto",
        obs="Rótulo de linha do formulário, não uma quantidade.",
        original="LINE_NUM",
    ),
    col(
        "form_type",
        "STRING",
        "Código do formulário da FEC utilizado",
        "Code of the FEC form used",
        "Código del formulario de la FEC utilizado",
        original="FORM_TP_CD",
    ),
    col(
        "schedule_type",
        "STRING",
        "Código do anexo do formulário; SB corresponde ao Schedule B de despesas",
        "Code of the form schedule; SB is Schedule B, operating expenditures",
        "Código del anexo del formulario; SB corresponde al Schedule B de gastos",
        original="SCHED_TP_CD",
    ),
    col(
        "election_type_year",
        "STRING",
        "Tipo de eleição e ano ao qual a despesa se refere",
        "Election type and year the disbursement refers to",
        "Tipo de elección y año al que se refiere el gasto",
        obs="Código composto: uma letra de tipo de eleição (P primária, G geral, "
        "O outra, C convenção, R segundo turno, S especial, E recontagem) seguida "
        "do ano com quatro dígitos, por exemplo P2026.",
        original="TRANSACTION_PGI",
    ),
    col(
        "entity_type",
        "STRING",
        "Tipo de entidade do beneficiário do pagamento",
        "Entity type of the payee",
        "Tipo de entidad del beneficiario del pago",
        dictionary="yes",
        original="ENTITY_TP",
    ),
    *txn_common(
        "payee", "beneficiário do pagamento", "payee", "beneficiario del pago"
    )[:4],
    col(
        "transaction_date",
        "DATE",
        "Data do desembolso",
        "Date of the disbursement",
        "Fecha del desembolso",
        obs="A FEC publica a data como declarada, e há erros de digitação: os dados brutos trazem datas de 1899 a 2202. Datas fora da janela plausível (anteriores à criação da FEC em 1975 ou mais de um ano após o fim do próprio ciclo) são gravadas como nulas; apenas a data é descartada, a linha é preservada. São cerca de 128 linhas em 79 milhões.",
        original="TRANSACTION_DT",
    ),
    col(
        "transaction_amount",
        "FLOAT64",
        "Valor do desembolso em dólares",
        "Amount of the disbursement in dollars",
        "Monto del desembolso en dólares",
        unit="USD",
        original="TRANSACTION_AMT",
    ),
    col(
        "purpose",
        "STRING",
        "Descrição da finalidade do desembolso informada pelo comitê",
        "Description of the purpose of the disbursement reported by the committee",
        "Descripción de la finalidad del desembolso informada por el comité",
        original="PURPOSE",
    ),
    col(
        "category",
        "STRING",
        "Código da categoria do desembolso",
        "Disbursement category code",
        "Código de la categoría del desembolso",
        dictionary="yes",
        obs="Campo preenchido pelo declarante: cerca de 99% dos valores seguem a lista de categorias de desembolso da FEC, registrada no dicionário, mas o restante traz valores não canônicos (por exemplo siglas de estado).",
        original="CATEGORY",
    ),
    col(
        "category_description",
        "STRING",
        "Descrição da categoria do desembolso conforme informada no arquivo",
        "Description of the disbursement category as reported in the file",
        "Descripción de la categoría del desembolso según el archivo",
        original="CATEGORY_DESC",
    ),
    col(
        "memo_code",
        "STRING",
        "Indicador de memorando; X sinaliza que o valor não entra no total do relatório",
        "Memo indicator; X flags that the amount is not included in the report total",
        "Indicador de memorando; X señala que el monto no entra en el total del informe",
        dictionary="yes",
        original="MEMO_CD",
    ),
    col(
        "memo_text",
        "STRING",
        "Descrição textual da atividade informada no memorando",
        "Free-text description of the activity reported in the memo",
        "Descripción textual de la actividad informada en el memorando",
        original="MEMO_TEXT",
    ),
]

TABLES["dicionario"] = [
    col(
        "id_tabela",
        "STRING",
        "Nome da tabela à qual a coluna pertence",
        "Name of the table the column belongs to",
        "Nombre de la tabla a la que pertenece la columna",
    ),
    col(
        "nome_coluna",
        "STRING",
        "Nome da coluna codificada",
        "Name of the coded column",
        "Nombre de la columna codificada",
    ),
    col(
        "chave",
        "STRING",
        "Valor do código armazenado na coluna",
        "Value of the code stored in the column",
        "Valor del código almacenado en la columna",
    ),
    col(
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal em que a chave é válida",
        "Temporal coverage over which the key is valid",
        "Cobertura temporal en la que la clave es válida",
    ),
    col(
        "valor",
        "STRING",
        "Descrição correspondente ao código",
        "Description corresponding to the code",
        "Descripción correspondiente al código",
    ),
]


def main():
    for table, columns in TABLES.items():
        path = HERE / f"{table}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=HEADER)
            writer.writeheader()
            writer.writerows(columns)
        print(f"{table:28s} {len(columns):3d} columns -> {path.name}")


if __name__ == "__main__":
    main()
