"""English and Spanish for every column description in br_bd_execucao_estadual.

The dataset is Brazilian, so `schema.yml` carries the authoritative Portuguese and this
module carries the other two languages the backend requires. Keying by the Portuguese
string rather than by (table, column) means a description shared across tables --
`sigla_uf`, `valor_empenhado`, `ano` -- is translated once and cannot drift between
tables.

Terminology is fixed across the dataset, because these words are the ones a reader
searches on and Brazilian budget vocabulary has no clean English equivalent:

    empenho          -> commitment          (the reservation of budget, phase 1)
    liquidação       -> verification        (goods received and checked, phase 2)
    pagamento        -> payment             (phase 3)
    dotação          -> appropriation
    licitação        -> tender
    homologado       -> awarded
    unidade gestora  -> managing unit
    órgão            -> agency
    credor           -> creditor            (payee of an empenho)
    fornecedor       -> supplier            (bidder in a tender)
"""

from __future__ import annotations

# Column notes: source anomalies and caveats, keyed by (table slug, column).
#
# These live in the backend's `observations` field, NOT in `description`. A description
# says what the column holds; a caveat about a bad exercise or a typo in the source is a
# note about the data, and burying it in the description makes the description unreadable
# and the caveat easy to miss. Both are per-language.
OBSERVATIONS: dict[tuple[str, str], tuple[str, str, str]] = {
    ("licitacao", "valor_referencia"): (
        "Contém erros de digitação da fonte. Em Minas Gerais, o processo "
        "1561122 000030/2011 registra R$81,76 trilhões contra um valor homologado de "
        "R$4,58 milhões, e sozinho responde por 99,8% do total do estado. Filtre "
        "valores extremos antes de agregar.",
        "Contains typing errors from the source. In Minas Gerais, process "
        "1561122 000030/2011 records R$81.76 trillion against an awarded amount of "
        "R$4.58 million, and on its own accounts for 99.8% of the state total. Filter "
        "extreme values before aggregating.",
        "Contiene errores de digitación de la fuente. En Minas Gerais, el proceso "
        "1561122 000030/2011 registra R$81,76 billones frente a un valor adjudicado de "
        "R$4,58 millones, y por sí solo representa el 99,8% del total del estado. "
        "Filtre valores extremos antes de agregar.",
    ),
    ("despesa", "valor_empenhado"): (
        "Contém anomalias da fonte. Em Pernambuco, o exercício de 2009 não reconcilia "
        "em escala com os anos vizinhos e não deve ser usado para comparações de "
        "nível, e 36 linhas do período 2008-2010 trazem valores-sentinela de quinze "
        "dígitos. Filtre valores extremos antes de agregar.",
        "Contains source anomalies. In Pernambuco, the 2009 budget year does not "
        "reconcile in scale with neighbouring years and must not be used for level "
        "comparisons, and 36 rows in 2008-2010 carry fifteen-digit sentinel values. "
        "Filter extreme values before aggregating.",
        "Contiene anomalías de la fuente. En Pernambuco, el ejercicio de 2009 no "
        "reconcilia en escala con los años vecinos y no debe usarse para comparaciones "
        "de nivel, y 36 filas del período 2008-2010 traen valores centinela de quince "
        "dígitos. Filtre valores extremos antes de agregar.",
    ),
    ("despesa", "valor_liquidado"): (
        "Contém anomalias da fonte. Em Pernambuco, o exercício de 2009 não reconcilia "
        "em escala com os anos vizinhos e não deve ser usado para comparações de "
        "nível, e 36 linhas do período 2008-2010 trazem valores-sentinela de quinze "
        "dígitos. Filtre valores extremos antes de agregar.",
        "Contains source anomalies. In Pernambuco, the 2009 budget year does not "
        "reconcile in scale with neighbouring years and must not be used for level "
        "comparisons, and 36 rows in 2008-2010 carry fifteen-digit sentinel values. "
        "Filter extreme values before aggregating.",
        "Contiene anomalías de la fuente. En Pernambuco, el ejercicio de 2009 no "
        "reconcilia en escala con los años vecinos y no debe usarse para comparaciones "
        "de nivel, y 36 filas del período 2008-2010 traen valores centinela de quince "
        "dígitos. Filtre valores extremos antes de agregar.",
    ),
}

# `quantidade` has no fixed unit, so it carries none; the note says why in place of one.
_QUANTIDADE = (
    "Sem unidade de medida fixa: a unidade varia por item e é publicada na coluna "
    "unidade_medida. Não some quantidades de itens diferentes.",
    "No fixed unit of measurement: the unit varies per item and is published in the "
    "unidade_medida column. Do not sum quantities across different items.",
    "Sin unidad de medida fija: la unidad varía por ítem y se publica en la columna "
    "unidade_medida. No sume cantidades de ítems diferentes.",
)
OBSERVATIONS[("despesa", "mes")] = (
    "Preenchido apenas por Minas Gerais em todo o período e por Pernambuco somente em "
    "2008-2010; de 2011 em diante Pernambuco publica apenas o exercício. Nas linhas "
    "pernambucanas o campo traz períodos contábeis, não só meses do calendário: 28.517 "
    "linhas usam 13 (encerramento do exercício) e há também o período 0 (abertura). "
    "Filtre para 1 a 12 antes de tratar a coluna como mês.",
    "Filled by Minas Gerais throughout and by Pernambuco only for 2008-2010; from 2011 "
    "on Pernambuco publishes the exercise alone. On Pernambuco's rows the field carries "
    "accounting periods, not just calendar months: 28,517 rows use 13 (year-end close) "
    "and period 0 (opening) also occurs. Filter to 1-12 before treating it as a month.",
    "Completado por Minas Gerais en todo el período y por Pernambuco solo en 2008-2010; "
    "desde 2011 Pernambuco publica únicamente el ejercicio. En las filas pernambucanas "
    "el campo trae períodos contables, no solo meses del calendario: 28.517 filas usan "
    "13 (cierre del ejercicio) y también aparece el período 0 (apertura). Filtre de 1 a "
    "12 antes de tratar la columna como mes.",
)

OBSERVATIONS[("licitacao_item", "quantidade")] = _QUANTIDADE
OBSERVATIONS[("licitacao_participante", "quantidade")] = _QUANTIDADE


# Portuguese -> (English, Spanish)
DESCRIPTIONS: dict[str, tuple[str, str]] = {
    # --- annual execution (despesa_anual, SP only) ------------------------------
    "Nome da fonte de recursos": (
        "Name of the funding source",
        "Nombre de la fuente de recursos",
    ),
    (
        "Código da natureza da despesa, com oito dígitos reunindo categoria econômica, "
        "grupo, modalidade de aplicação, elemento e subelemento. Mais detalhado que a "
        "coluna elemento_despesa das demais tabelas"
    ): (
        "Expenditure nature code, eight digits combining economic category, group, "
        "application modality, element and subelement. More detailed than the "
        "elemento_despesa column in the other tables",
        "Código de la naturaleza del gasto, con ocho dígitos que reúnen categoría "
        "económica, grupo, modalidad de aplicación, elemento y subelemento. Más "
        "detallado que la columna elemento_despesa de las demás tablas",
    ),
    "Nome da natureza da despesa": (
        "Name of the expenditure nature",
        "Nombre de la naturaleza del gasto",
    ),
    (
        "CPF ou CNPJ do credor. São Paulo publica os CPF sem máscara, diferentemente de "
        "Minas Gerais, que os oculta, e de Pernambuco, que os mascara"
    ): (
        "Creditor's CPF or CNPJ. São Paulo publishes CPFs unmasked, unlike Minas Gerais, "
        "which redacts them, and Pernambuco, which masks them",
        "CPF o CNPJ del acreedor. São Paulo publica los CPF sin máscara, a diferencia de "
        "Minas Gerais, que los oculta, y de Pernambuco, que los enmascara",
    ),
    (
        "Tipo do identificador do credor, derivado do seu formato: CNPJ, CPF, "
        "UNIDADE_GESTORA ou OUTRO"
    ): (
        "Type of the creditor's identifier, derived from its format: CNPJ, CPF, "
        "UNIDADE_GESTORA or OUTRO",
        "Tipo del identificador del acreedor, derivado de su formato: CNPJ, CPF, "
        "UNIDADE_GESTORA u OUTRO",
    ),
    (
        "Valor pago no exercício referente a restos a pagar de exercícios anteriores, "
        "mantido separado de valor_pago para não duplicar a contagem no ano do empenho"
    ): (
        "Amount paid in the year against commitments carried over from earlier years, "
        "kept separate from valor_pago so it is not double-counted in the year the "
        "commitment belongs to",
        "Monto pagado en el ejercicio correspondiente a restos por pagar de ejercicios "
        "anteriores, mantenido separado de valor_pago para no duplicar el conteo en el "
        "año del compromiso",
    ),
    # --- payment (pagamento, PE only) -------------------------------------------
    "Ano do exercício em que o pagamento foi lançado": (
        "Budget year in which the payment was recorded",
        "Año del ejercicio en que se registró el pago",
    ),
    "Mês do lançamento do pagamento": (
        "Month the payment was recorded",
        "Mes del registro del pago",
    ),
    "Data do lançamento do pagamento": (
        "Date the payment was recorded",
        "Fecha del registro del pago",
    ),
    (
        "Identificador único do pagamento construído pela Data Basis, no formato "
        "<sigla_uf>-<ordem bancária>-<sequência dentro da ordem>, porque uma ordem "
        "bancária paga vários empenhos e não identifica a linha sozinha. Vinte linhas "
        "de 2025 não têm ordem bancária, apenas empenho e valor, e recebem o marcador "
        "SEMOB no lugar do número"
    ): (
        "Unique payment identifier built by Data Basis, in the format "
        "<sigla_uf>-<bank order>-<sequence within the order>, because one bank order "
        "pays several commitments and does not identify the line on its own. Twenty "
        "rows in 2025 have no bank order, only a commitment and a value, and carry the "
        "marker SEMOB in place of the number",
        "Identificador único del pago construido por Data Basis, en el formato "
        "<sigla_uf>-<orden bancaria>-<secuencia dentro de la orden>, porque una orden "
        "bancaria paga varios compromisos y no identifica la línea por sí sola. Veinte "
        "filas de 2025 no tienen orden bancaria, solo compromiso y valor, y llevan el "
        "marcador SEMOB en lugar del número",
    ),
    "Número da ordem bancária que efetuou o pagamento": (
        "Number of the bank order that made the payment",
        "Número de la orden bancaria que efectuó el pago",
    ),
    (
        "Identificador do empenho pago, no mesmo formato de despesa.id_empenho_bd, "
        "para ligar o pagamento à execução orçamentária"
    ): (
        "Identifier of the commitment paid, in the same format as "
        "despesa.id_empenho_bd, to link the payment to budget execution",
        "Identificador del compromiso pagado, en el mismo formato de "
        "despesa.id_empenho_bd, para vincular el pago a la ejecución presupuestaria",
    ),
    "Número da nota de empenho paga": (
        "Number of the commitment note paid",
        "Número de la nota de compromiso pagada",
    ),
    (
        "Situação da ordem bancária. Nem toda linha é dinheiro que saiu do tesouro: "
        "além de PAGA, ocorrem DEVOLVIDA, CANCELADA, DEVOLVIDA APOS PAGTO, AJUSTADA, "
        "ENVIADA e GERADA"
    ): (
        "Status of the bank order. Not every row is money that left the treasury: "
        "besides PAGA (paid), the values DEVOLVIDA (returned), CANCELADA (cancelled), "
        "DEVOLVIDA APOS PAGTO (returned after payment), AJUSTADA (adjusted), ENVIADA "
        "(sent) and GERADA (generated) also occur",
        "Situación de la orden bancaria. No toda fila es dinero que salió del tesoro: "
        "además de PAGA, ocurren DEVOLVIDA, CANCELADA, DEVOLVIDA APOS PAGTO, AJUSTADA, "
        "ENVIADA y GERADA",
    ),
    "Nome da unidade gestora que emitiu a ordem bancária": (
        "Name of the managing unit that issued the bank order",
        "Nombre de la unidad gestora que emitió la orden bancaria",
    ),
    (
        "CPF ou CNPJ de quem recebeu a ordem bancária, apenas a parte anterior ao "
        "nome. Nem sempre é um documento: o estado emite pseudocódigos para "
        "beneficiários sem CNPJ"
    ): (
        "CPF or CNPJ of whoever received the bank order, taken as the part before the "
        "name. It is not always a document: the state issues pseudo-codes for payees "
        "without a CNPJ",
        "CPF o CNPJ de quien recibió la orden bancaria, solo la parte anterior al "
        "nombre. No siempre es un documento: el estado emite seudocódigos para "
        "beneficiarios sin CNPJ",
    ),
    "Nome ou razão social de quem recebeu a ordem bancária": (
        "Name or legal name of whoever received the bank order",
        "Nombre o razón social de quien recibió la orden bancaria",
    ),
    (
        "CPF ou CNPJ do credor nomeado no empenho, que difere do credor da ordem "
        "bancária em cerca de metade das linhas"
    ): (
        "CPF or CNPJ of the creditor named on the commitment, which differs from the "
        "bank order's payee in about half of the rows",
        "CPF o CNPJ del acreedor nombrado en el compromiso, que difiere del acreedor de "
        "la orden bancaria en cerca de la mitad de las filas",
    ),
    "Nome ou razão social do credor nomeado no empenho": (
        "Name or legal name of the creditor named on the commitment",
        "Nombre o razón social del acreedor nombrado en el compromiso",
    ),
    "Finalidade do pagamento informada na ordem bancária": (
        "Purpose of the payment as stated on the bank order",
        "Finalidad del pago informada en la orden bancaria",
    ),
    "Valor da ordem bancária em reais correntes": (
        "Amount of the bank order, in current reais",
        "Monto de la orden bancaria, en reales corrientes",
    ),
    # --- temporal ---------------------------------------------------------------
    "Ano de abertura do processo de contratação": (
        "Year the procurement process was opened",
        "Año de apertura del proceso de contratación",
    ),
    "Ano de referência do item licitado": (
        "Reference year of the tendered item",
        "Año de referencia del ítem licitado",
    ),
    "Ano do exercício orçamentário": (
        "Budget year",
        "Año del ejercicio presupuestario",
    ),
    "Ano do exercício orçamentário ao qual a despesa pertence": (
        "Budget year the expenditure belongs to",
        "Año del ejercicio presupuestario al que pertenece el gasto",
    ),
    "Ano do processo de aquisição": (
        "Year of the procurement process",
        "Año del proceso de adquisición",
    ),
    "Mês de abertura do processo de contratação": (
        "Month the procurement process was opened",
        "Mes de apertura del proceso de contratación",
    ),
    "Mês de referência do movimento": (
        "Reference month of the transaction",
        "Mes de referencia del movimiento",
    ),
    "Mês do exercício orçamentário": (
        "Month of the budget year",
        "Mes del ejercicio presupuestario",
    ),
    "Mês do pedido de empenho": (
        "Month the commitment was requested",
        "Mes de la solicitud de compromiso",
    ),
    "Data de cadastro ou abertura do processo": (
        "Date the process was registered or opened",
        "Fecha de registro o apertura del proceso",
    ),
    "Data de emissão do empenho": (
        "Date the commitment was issued",
        "Fecha de emisión del compromiso",
    ),
    "Data de homologação do item": (
        "Date the item was awarded",
        "Fecha de adjudicación del ítem",
    ),
    "Data de homologação do resultado do processo": (
        "Date the outcome of the process was awarded",
        "Fecha de adjudicación del resultado del proceso",
    ),
    "Data de publicação do edital no diário oficial": (
        "Date the tender notice was published in the official gazette",
        "Fecha de publicación del pliego en el diario oficial",
    ),
    "Data do pedido de empenho": (
        "Date the commitment was requested",
        "Fecha de la solicitud de compromiso",
    ),
    "Data e hora da autorização do empenho": (
        "Date and time the commitment was authorised",
        "Fecha y hora de la autorización del compromiso",
    ),
    # --- geography --------------------------------------------------------------
    "Sigla da unidade da federação a que o código se refere": (
        "Abbreviation of the federative unit the code refers to",
        "Sigla de la unidad federativa a la que se refiere el código",
    ),
    "Sigla da unidade da federação do governo estadual": (
        "Abbreviation of the federative unit of the state government",
        "Sigla de la unidad federativa del gobierno estatal",
    ),
    # --- identifiers ------------------------------------------------------------
    "Identificador do empenho no sistema de origem": (
        "Commitment identifier in the source system",
        "Identificador del compromiso en el sistema de origen",
    ),
    "Identificador do processo licitatório no sistema de origem": (
        "Tender process identifier in the source system",
        "Identificador del proceso licitatorio en el sistema de origen",
    ),
    (
        "Identificador interno do empenho no sistema de origem. Preenchido por Minas "
        "Gerais, que publica o vínculo diretamente"
    ): (
        "Internal commitment identifier in the source system. Filled by Minas Gerais, "
        "which publishes the link directly",
        "Identificador interno del compromiso en el sistema de origen. Completado por "
        "Minas Gerais, que publica el vínculo directamente",
    ),
    (
        "Identificador único do empenho construído pela Data Basis, no formato "
        "<sigla_uf><ano>-<identificador de origem>"
    ): (
        "Unique commitment identifier built by Data Basis, in the format "
        "<sigla_uf><ano>-<source identifier>",
        "Identificador único del compromiso construido por Data Basis, en el formato "
        "<sigla_uf><ano>-<identificador de origen>",
    ),
    "Identificador único do item construído pela Data Basis": (
        "Unique item identifier built by Data Basis",
        "Identificador único del ítem construido por Data Basis",
    ),
    (
        "Identificador único do item construído pela Data Basis. Na Bahia é "
        "<sigla_uf>-<processo>-<sequência do item>; em Minas Gerais é "
        "<sigla_uf>-<processo>-<produto catalogado>-<ocorrência>, porque a fonte não "
        "numera os itens e o mesmo produto pode ser comprado mais de uma vez no mesmo "
        "processo"
    ): (
        "Unique item identifier built by Data Basis. In Bahia it is "
        "<sigla_uf>-<process>-<item sequence>; in Minas Gerais it is "
        "<sigla_uf>-<process>-<catalogued product>-<occurrence>, because the source "
        "does not number items and the same product can be purchased more than once in "
        "the same process",
        "Identificador único del ítem construido por Data Basis. En Bahía es "
        "<sigla_uf>-<proceso>-<secuencia del ítem>; en Minas Gerais es "
        "<sigla_uf>-<proceso>-<producto catalogado>-<ocurrencia>, porque la fuente no "
        "numera los ítems y el mismo producto puede comprarse más de una vez en el "
        "mismo proceso",
    ),
    (
        "Identificador único do processo licitatório associado ao empenho, construído "
        "pela Data Basis. Preenchido apenas quando a fonte publica o vínculo entre "
        "licitação e empenho"
    ): (
        "Unique identifier of the tender process linked to the commitment, built by "
        "Data Basis. Filled only where the source publishes the link between tender "
        "and commitment",
        "Identificador único del proceso licitatorio asociado al compromiso, construido "
        "por Data Basis. Completado solo cuando la fuente publica el vínculo entre "
        "licitación y compromiso",
    ),
    "Identificador único do processo licitatório construído pela Data Basis": (
        "Unique tender process identifier built by Data Basis",
        "Identificador único del proceso licitatorio construido por Data Basis",
    ),
    (
        "Identificador único do processo licitatório construído pela Data Basis, no "
        "formato <sigla_uf>-<identificador de origem>"
    ): (
        "Unique tender process identifier built by Data Basis, in the format "
        "<sigla_uf>-<source identifier>",
        "Identificador único del proceso licitatorio construido por Data Basis, en el "
        "formato <sigla_uf>-<identificador de origen>",
    ),
    "Número da nota de empenho atribuído pela unidade executora": (
        "Commitment note number assigned by the executing unit",
        "Número de la nota de compromiso asignado por la unidad ejecutora",
    ),
    "Número do empenho no sistema orçamentário do estado": (
        "Commitment number in the state budget system",
        "Número del compromiso en el sistema presupuestario del estado",
    ),
    (
        "Número do empenho no sistema orçamentário do estado. Preenchido pela Bahia, "
        "que não possui identificador interno e liga o processo ao empenho pelo "
        "instrumento orçamentário"
    ): (
        "Commitment number in the state budget system. Filled by Bahia, which has no "
        "internal identifier and links the process to the commitment through the "
        "budget instrument",
        "Número del compromiso en el sistema presupuestario del estado. Completado por "
        "Bahía, que no tiene identificador interno y vincula el proceso al compromiso "
        "mediante el instrumento presupuestario",
    ),
    "Número do instrumento orçamentário que liga o item da licitação ao empenho, na Bahia": (
        "Number of the budget instrument linking the tender item to the commitment, in "
        "Bahia",
        "Número del instrumento presupuestario que vincula el ítem de la licitación al "
        "compromiso, en Bahía",
    ),
    "Número do instrumento orçamentário que originou o empenho": (
        "Number of the budget instrument the commitment originated from",
        "Número del instrumento presupuestario que originó el compromiso",
    ),
    "Número do item dentro do processo": (
        "Item number within the process",
        "Número del ítem dentro del proceso",
    ),
    (
        "Número do item dentro do processo, conforme a fonte. Nulo em Minas Gerais, que "
        "não publica numeração de item"
    ): (
        "Item number within the process, as published by the source. Null in Minas "
        "Gerais, which does not publish item numbering",
        "Número del ítem dentro del proceso, según la fuente. Nulo en Minas Gerais, que "
        "no publica numeración de ítems",
    ),
    "Número do processo licitatório no sistema de origem": (
        "Tender process number in the source system",
        "Número del proceso licitatorio en el sistema de origen",
    ),
    "Número do processo no sistema eletrônico de informações": (
        "Process number in the electronic information system",
        "Número del proceso en el sistema electrónico de informaciones",
    ),
    "Número do processo no sistema eletrônico de informações do estado": (
        "Process number in the state's electronic information system",
        "Número del proceso en el sistema electrónico de informaciones del estado",
    ),
    (
        "Número da licitação atribuído pelo estado, distinto do número do processo de "
        "aquisição. Publicado apenas pela Bahia"
    ): (
        "Tender number assigned by the state, distinct from the procurement process "
        "number. Published only by Bahia",
        "Número de la licitación asignado por el estado, distinto del número del "
        "proceso de adquisición. Publicado solo por Bahía",
    ),
    "Número da conta de dotação orçamentária anual": (
        "Annual budget appropriation account number",
        "Número de la cuenta de asignación presupuestaria anual",
    ),
    "Código do credor no sistema de origem": (
        "Creditor code in the source system",
        "Código del acreedor en el sistema de origen",
    ),
    # --- budget classification --------------------------------------------------
    "Código da ação de governo": (
        "Government action code",
        "Código de la acción de gobierno",
    ),
    "Código da ação orçamentária": (
        "Budget action code",
        "Código de la acción presupuestaria",
    ),
    "Código da categoria econômica da despesa": (
        "Economic category code of the expenditure",
        "Código de la categoría económica del gasto",
    ),
    "Código da destinação de recursos": (
        "Resource earmarking code",
        "Código de la destinación de recursos",
    ),
    "Código da fonte de recursos": (
        "Funding source code",
        "Código de la fuente de recursos",
    ),
    "Código da fonte de recursos que financia a despesa": (
        "Code of the funding source financing the expenditure",
        "Código de la fuente de recursos que financia el gasto",
    ),
    "Código da função de governo": (
        "Government function code",
        "Código de la función de gobierno",
    ),
    "Código da função de governo, conforme a classificação funcional": (
        "Government function code, per the functional classification",
        "Código de la función de gobierno, según la clasificación funcional",
    ),
    "Código da modalidade de aplicação dos recursos": (
        "Application modality code of the resources",
        "Código de la modalidad de aplicación de los recursos",
    ),
    "Código da região orçamentária de aplicação": (
        "Budget region of application code",
        "Código de la región presupuestaria de aplicación",
    ),
    "Código da subfunção de governo": (
        "Government subfunction code",
        "Código de la subfunción de gobierno",
    ),
    "Código da subfunção de governo, conforme a classificação funcional": (
        "Government subfunction code, per the functional classification",
        "Código de la subfunción de gobierno, según la clasificación funcional",
    ),
    "Código da unidade gestora executora da despesa": (
        "Code of the managing unit executing the expenditure",
        "Código de la unidad gestora ejecutora del gasto",
    ),
    "Código da unidade orçamentária": (
        "Budget unit code",
        "Código de la unidad presupuestaria",
    ),
    "Código do elemento de despesa": (
        "Expenditure element code",
        "Código del elemento de gasto",
    ),
    "Código do grupo de natureza da despesa": (
        "Expenditure nature group code",
        "Código del grupo de naturaleza del gasto",
    ),
    (
        "Código do item de despesa a que o item licitado pertence, no mesmo espaço de "
        "códigos de despesa.item_despesa. Preenchido apenas em Minas Gerais"
    ): (
        "Code of the expenditure item the tendered item belongs to, in the same code "
        "space as despesa.item_despesa. Filled only in Minas Gerais",
        "Código del ítem de gasto al que pertenece el ítem licitado, en el mismo "
        "espacio de códigos de despesa.item_despesa. Completado solo en Minas Gerais",
    ),
    "Código do item de despesa, detalhamento do elemento": (
        "Expenditure item code, a breakdown of the element",
        "Código del ítem de gasto, desglose del elemento",
    ),
    "Código do programa de governo": (
        "Government programme code",
        "Código del programa de gobierno",
    ),
    "Código do programa orçamentário": (
        "Budget programme code",
        "Código del programa presupuestario",
    ),
    "Código do subelemento de despesa": (
        "Expenditure subelement code",
        "Código del subelemento de gasto",
    ),
    "Código do tipo de despesa": (
        "Expenditure type code",
        "Código del tipo de gasto",
    ),
    "Código do tipo de empenho": (
        "Commitment type code",
        "Código del tipo de compromiso",
    ),
    "Código do órgão orçamentário": (
        "Budget agency code",
        "Código del órgano presupuestario",
    ),
    "Código do órgão ou entidade responsável pela contratação": (
        "Code of the agency or entity responsible for the procurement",
        "Código del órgano o entidad responsable de la contratación",
    ),
    "Código do órgão ou unidade orçamentária responsável pela despesa": (
        "Code of the agency or budget unit responsible for the expenditure",
        "Código del órgano o unidad presupuestaria responsable del gasto",
    ),
    "Nome da unidade gestora executora da despesa": (
        "Name of the managing unit executing the expenditure",
        "Nombre de la unidad gestora ejecutora del gasto",
    ),
    "Nome da unidade orçamentária": (
        "Name of the budget unit",
        "Nombre de la unidad presupuestaria",
    ),
    "Nome do órgão orçamentário": (
        "Name of the budget agency",
        "Nombre del órgano presupuestario",
    ),
    "Nome do órgão ou entidade responsável pela contratação": (
        "Name of the agency or entity responsible for the procurement",
        "Nombre del órgano o entidad responsable de la contratación",
    ),
    "Nome do órgão ou unidade orçamentária": (
        "Name of the agency or budget unit",
        "Nombre del órgano o unidad presupuestaria",
    ),
    "Poder ao qual o órgão pertence": (
        "Branch of government the agency belongs to",
        "Poder al que pertenece el órgano",
    ),
    "Poder ao qual pertence o órgão solicitante": (
        "Branch of government the requesting agency belongs to",
        "Poder al que pertenece el órgano solicitante",
    ),
    # --- creditors and suppliers ------------------------------------------------
    "CPF ou CNPJ do credor apenas com dígitos": (
        "Creditor's CPF or CNPJ, digits only",
        "CPF o CNPJ del acreedor, solo dígitos",
    ),
    "CPF ou CNPJ do credor com máscara, como publicado pela fonte": (
        "Creditor's CPF or CNPJ with punctuation, as published by the source",
        "CPF o CNPJ del acreedor con máscara, tal como lo publica la fuente",
    ),
    (
        "CPF ou CNPJ do credor. CPFs são divulgados de forma parcialmente mascarada "
        "pela fonte"
    ): (
        "Creditor's CPF or CNPJ. CPFs are partially masked by the source",
        "CPF o CNPJ del acreedor. Los CPF son divulgados parcialmente enmascarados por "
        "la fuente",
    ),
    "CPF ou CNPJ do fornecedor apenas com dígitos": (
        "Supplier's CPF or CNPJ, digits only",
        "CPF o CNPJ del proveedor, solo dígitos",
    ),
    "CPF ou CNPJ do fornecedor com máscara, como publicado pela fonte": (
        "Supplier's CPF or CNPJ with punctuation, as published by the source",
        "CPF o CNPJ del proveedor con máscara, tal como lo publica la fuente",
    ),
    "CPF ou CNPJ do fornecedor vencedor": (
        "Winning supplier's CPF or CNPJ",
        "CPF o CNPJ del proveedor ganador",
    ),
    "Nome ou razão social do credor": (
        "Creditor's name or legal name",
        "Nombre o razón social del acreedor",
    ),
    "Nome ou razão social do fornecedor participante": (
        "Participating supplier's name or legal name",
        "Nombre o razón social del proveedor participante",
    ),
    "Nome ou razão social do fornecedor vencedor": (
        "Winning supplier's name or legal name",
        "Nombre o razón social del proveedor ganador",
    ),
    "Tipo do documento de identificação do credor": (
        "Type of the creditor's identification document",
        "Tipo del documento de identificación del acreedor",
    ),
    "Tipo do documento de identificação do fornecedor vencedor": (
        "Type of the winning supplier's identification document",
        "Tipo del documento de identificación del proveedor ganador",
    ),
    # --- tender attributes ------------------------------------------------------
    "Categoria do processo de aquisição": (
        "Category of the procurement process",
        "Categoría del proceso de adquisición",
    ),
    "Categoria do processo de aquisição atribuída pelo estado": (
        "Category of the procurement process assigned by the state",
        "Categoría del proceso de adquisición asignada por el estado",
    ),
    "Classe do material ou serviço no catálogo do estado": (
        "Class of the good or service in the state catalogue",
        "Clase del material o servicio en el catálogo del estado",
    ),
    "Critério de julgamento das propostas, por item ou por lote": (
        "Award criterion for bids, by item or by lot",
        "Criterio de evaluación de las propuestas, por ítem o por lote",
    ),
    "Descrição do material ou serviço licitado": (
        "Description of the tendered good or service",
        "Descripción del material o servicio licitado",
    ),
    "Descrição do objeto da contratação": (
        "Description of the subject of the procurement",
        "Descripción del objeto de la contratación",
    ),
    "Endereço eletrônico do edital do processo": (
        "URL of the tender notice for the process",
        "Dirección electrónica del pliego del proceso",
    ),
    "Forma de contratação adotada, como licitação ou contratação direta": (
        "Contracting form adopted, such as tender or direct contracting",
        "Forma de contratación adoptada, como licitación o contratación directa",
    ),
    "Grupo de materiais ou serviços objeto do processo": (
        "Group of goods or services covered by the process",
        "Grupo de materiales o servicios objeto del proceso",
    ),
    "Grupo do material ou serviço no catálogo do estado": (
        "Group of the good or service in the state catalogue",
        "Grupo del material o servicio en el catálogo del estado",
    ),
    "Código do item no catálogo de materiais e serviços do estado": (
        "Item code in the state catalogue of goods and services",
        "Código del ítem en el catálogo de materiales y servicios del estado",
    ),
    "Modalidade do processo de contratação que originou a despesa": (
        "Modality of the procurement process the expenditure originated from",
        "Modalidad del proceso de contratación que originó el gasto",
    ),
    (
        "Procedimento de contratação, como pregão, concorrência, dispensa ou "
        "inexigibilidade, no vocabulário da fonte"
    ): (
        "Procurement procedure, such as reverse auction, open tender, waiver or "
        "non-enforceability, in the source's own vocabulary",
        "Procedimiento de contratación, como subasta inversa, licitación pública, "
        "dispensa o inexigibilidad, en el vocabulario de la fuente",
    ),
    (
        "Indica se o processo é um registro de preços, em que uma licitação autoriza "
        "compras parceladas ao longo de um período. Publicado apenas pela Bahia"
    ): (
        "Whether the process is a framework agreement, in which one tender authorises "
        "instalment purchases over a period. Published only by Bahia",
        "Indica si el proceso es un acuerdo marco, en el que una licitación autoriza "
        "compras fraccionadas a lo largo de un período. Publicado solo por Bahía",
    ),
    "Situação do processo de contratação": (
        "Status of the procurement process",
        "Situación del proceso de contratación",
    ),
    "Situação do fornecedor naquele item, como classificado, desclassificado ou habilitado": (
        "Status of the supplier for that item, such as qualified, disqualified or "
        "eligible",
        "Situación del proveedor en ese ítem, como clasificado, descalificado o "
        "habilitado",
    ),
    (
        "Indica se o fornecedor venceu o item, conforme a situação publicada pela fonte. "
        "Não é inferido a partir do valor homologado: 84% das linhas marcadas como "
        "Perdedor também trazem valor homologado positivo, porque a coluna registra o "
        "valor do item e não o que aquele fornecedor recebeu"
    ): (
        "Whether the supplier won the item, per the status published by the source. It "
        "is not inferred from the awarded amount: 84% of rows labelled Loser also carry "
        "a positive awarded amount, because that column records the item's value rather "
        "than what the particular supplier received",
        "Indica si el proveedor ganó el ítem, según la situación publicada por la "
        "fuente. No se infiere del valor adjudicado: 84% de las filas marcadas como "
        "Perdedor también traen valor adjudicado positivo, porque la columna registra "
        "el valor del ítem y no lo que ese proveedor recibió",
    ),
    "Tipo da licitação, como menor preço ou melhor técnica": (
        "Tender type, such as lowest price or best technical proposal",
        "Tipo de licitación, como menor precio o mejor técnica",
    ),
    "Unidade de medida em que o item foi cotado": (
        "Unit of measurement in which the item was quoted",
        "Unidad de medida en que se cotizó el ítem",
    ),
    "Quantidade pedida do item": (
        "Quantity of the item requested",
        "Cantidad solicitada del ítem",
    ),
    "Quantidade solicitada do item": (
        "Quantity of the item requested",
        "Cantidad solicitada del ítem",
    ),
    # --- expenditure attributes -------------------------------------------------
    "Descrição da finalidade do gasto informada na origem": (
        "Description of the purpose of the spending as reported at source",
        "Descripción de la finalidad del gasto informada en el origen",
    ),
    "Tipo do documento contábil que registrou o movimento": (
        "Type of the accounting document that recorded the transaction",
        "Tipo del documento contable que registró el movimiento",
    ),
    "Tipo do empenho, como ordinário, estimado ou global": (
        "Commitment type, such as ordinary, estimated or global",
        "Tipo del compromiso, como ordinario, estimado o global",
    ),
    (
        "Chave de dotação orçamentária do empenho. Não permite atribuir valores por "
        "credor: há cerca de seis empenhos por dotação"
    ): (
        "Budget appropriation key of the commitment. It does not allow amounts to be "
        "attributed per creditor: there are about six commitments per appropriation",
        "Clave de asignación presupuestaria del compromiso. No permite atribuir valores "
        "por acreedor: hay cerca de seis compromisos por asignación",
    ),
    "Dotação orçamentária que vincula o processo ao empenho": (
        "Budget appropriation linking the process to the commitment",
        "Asignación presupuestaria que vincula el proceso al compromiso",
    ),
    # --- dictionary -------------------------------------------------------------
    "Nome da coluna codificada": (
        "Name of the coded column",
        "Nombre de la columna codificada",
    ),
    "Nome da tabela à qual a coluna codificada pertence": (
        "Name of the table the coded column belongs to",
        "Nombre de la tabla a la que pertenece la columna codificada",
    ),
    "Descrição do valor da chave codificada": (
        "Description of the value of the coded key",
        "Descripción del valor de la clave codificada",
    ),
    "Valor da chave codificada presente na coluna": (
        "Value of the coded key present in the column",
        "Valor de la clave codificada presente en la columna",
    ),
    "Intervalo de exercícios em que aquele rótulo esteve vigente, na notação inicio(intervalo)fim": (
        "Range of budget years over which that label was in force, in the notation "
        "start(interval)end",
        "Intervalo de ejercicios en que esa etiqueta estuvo vigente, en la notación "
        "inicio(intervalo)fin",
    ),
    # --- money ------------------------------------------------------------------
    "Preço unitário de referência estimado pelo órgão em reais correntes": (
        "Reference unit price estimated by the agency, in current reais",
        "Precio unitario de referencia estimado por el órgano, en reales corrientes",
    ),
    "Preço unitário homologado em reais correntes": (
        "Awarded unit price, in current reais",
        "Precio unitario adjudicado, en reales corrientes",
    ),
    "Valor concedido por descentralização em reais correntes": (
        "Amount granted through decentralisation, in current reais",
        "Monto concedido por descentralización, en reales corrientes",
    ),
    "Valor da dotação atualizada em reais correntes": (
        "Updated appropriation amount, in current reais",
        "Monto de la asignación actualizada, en reales corrientes",
    ),
    "Valor da dotação inicial em reais correntes": (
        "Initial appropriation amount, in current reais",
        "Monto de la asignación inicial, en reales corrientes",
    ),
    "Valor de referência estimado pelo órgão em reais correntes": (
        "Reference amount estimated by the agency, in current reais",
        "Monto de referencia estimado por el órgano, en reales corrientes",
    ),
    "Valor empenhado em reais correntes": (
        "Committed amount, in current reais",
        "Monto comprometido, en reales corrientes",
    ),
    "Valor liquidado em reais correntes": (
        "Verified amount, in current reais",
        "Monto liquidado, en reales corrientes",
    ),
    "Valor empenhado no mês em reais correntes": (
        "Amount committed in the month, in current reais",
        "Monto comprometido en el mes, en reales corrientes",
    ),
    "Valor homologado ao final do processo em reais correntes": (
        "Amount awarded at the end of the process, in current reais",
        "Monto adjudicado al final del proceso, en reales corrientes",
    ),
    "Valor liquidado no mês em reais correntes": (
        "Amount verified in the month, in current reais",
        "Monto liquidado en el mes, en reales corrientes",
    ),
    "Valor pago em reais correntes": (
        "Amount paid, in current reais",
        "Monto pagado, en reales corrientes",
    ),
    "Valor pago no mês em reais correntes": (
        "Amount paid in the month, in current reais",
        "Monto pagado en el mes, en reales corrientes",
    ),
    "Valor recebido por descentralização em reais correntes": (
        "Amount received through decentralisation, in current reais",
        "Monto recibido por descentralización, en reales corrientes",
    ),
    "Valor total cotado pelo fornecedor em reais correntes": (
        "Total amount quoted by the supplier, in current reais",
        "Monto total cotizado por el proveedor, en reales corrientes",
    ),
    "Valor total de referência do item em reais correntes": (
        "Total reference amount of the item, in current reais",
        "Monto total de referencia del ítem, en reales corrientes",
    ),
    "Valor total do item atualizado em reais correntes": (
        "Updated total amount of the item, in current reais",
        "Monto total del ítem actualizado, en reales corrientes",
    ),
    "Valor total homologado ao fornecedor em reais correntes": (
        "Total amount awarded to the supplier, in current reais",
        "Monto total adjudicado al proveedor, en reales corrientes",
    ),
    "Valor total homologado do item em reais correntes": (
        "Total awarded amount of the item, in current reais",
        "Monto total adjudicado del ítem, en reales corrientes",
    ),
    "Valor unitário cotado pelo fornecedor em reais correntes": (
        "Unit amount quoted by the supplier, in current reais",
        "Monto unitario cotizado por el proveedor, en reales corrientes",
    ),
    "Valor unitário homologado ao fornecedor em reais correntes": (
        "Unit amount awarded to the supplier, in current reais",
        "Monto unitario adjudicado al proveedor, en reales corrientes",
    ),
}
