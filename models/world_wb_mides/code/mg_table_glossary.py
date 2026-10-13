"""Trilingual names and descriptions for every table in the `mides` dataset.

`TABLES` holds all 52: the 43 MG-only tables this directory's scripts build,
and the 9 older multi-state ones listed in `ORIGINAL_TABLES`. `MG_TABLES` is
the 43, and it -- not `TABLES` -- is what those scripts iterate.

Shared by `gen_mg_schema.py` (which emits Portuguese into `schema.yml`, per the
house dbt convention) and by metadata registration (which needs all three
languages). Portuguese is the source of truth; English and Spanish translate it.

The SUFFIX is appended to every description because these tables exist for one
state only, and a reader landing on `contrato` from the dataset page has no other
signal that its Coverage is MG-only.
"""

from __future__ import annotations

LANGS = ("pt", "en", "es")

SUFFIX: tuple[str, str, str] = (
    "Publicado apenas para Minas Gerais, a partir dos arquivos do SICOM/TCE-MG; "
    "a Coverage da tabela registra sigla_uf = MG.",
    "Published for Minas Gerais only, from the SICOM/TCE-MG files; the table's "
    "Coverage records sigla_uf = MG.",
    "Publicado solo para Minas Gerais, a partir de los archivos del SICOM/TCE-MG; "
    "la Coverage de la tabla registra sigla_uf = MG.",
)

# Tables whose own `_bd` key is NULL for some rows, because the TCE-MG source
# publishes child records citing a parent record it never publishes. Measured
# 2026-09-28 against the dev build: for every table checked, the orphan key
# appears in the parent under no municipality and no exercise, so this is a
# referential gap in the source, not a join defect. Rates run from 0.00%
# (restos_pagar_movimentacao, 73 of 2,624,515) to 4.06% (contrato_rescisao, 748
# of 18,430); contrato_apostilamento 3.27%, licitacao_parecer 3.25%.
#
# Accepted deliberately on 2026-09-28: the rows are kept and published rather
# than dropped, so these tables carry a primary key that is NULL for 3-4% of
# rows. A user joining on that key silently loses those rows, which is why the
# caveat below is part of every affected table's published description.
ORPHAN_PARENT_KEY: frozenset[str] = frozenset(
    {
        "contrato_apostilamento",
        "contrato_credito",
        "contrato_item",
        "contrato_rescisao",
        "contrato_termo_aditivo",
        "contrato_termo_aditivo_item",
        "dispensa_cotacao",
        "dispensa_credenciado",
        "empenho_fonte",
        "licitacao_comissao",
        "licitacao_cotacao",
        "licitacao_dotacao",
        "licitacao_homologacao",
        "licitacao_julgamento",
        "licitacao_parecer",
        "licitacao_quadro_societario",
        "licitacao_responsavel",
        "pagamento_movimento",
        "registro_preco_adesao_cotacao",
        "registro_preco_adesao_item",
        "registro_preco_adesao_vencedor",
        "restos_pagar_movimentacao",
        "restos_pagar_movimentacao_credor",
        "restos_pagar_movimentacao_fonte",
    }
)

ORPHAN_KEY_SUFFIX: tuple[str, str, str] = (
    "Em até 4% das linhas a fonte cita um registro-pai que não publica; "
    "nessas linhas a chave estrangeira e a chave primária construída por "
    "Data Basis são nulas.",
    "In up to 4% of rows the source cites a parent record it does not publish; "
    "in those rows the foreign key and the Data Basis primary key are null.",
    "En hasta 4% de las filas la fuente cita un registro padre que no publica; "
    "en esas filas la clave foránea y la clave primaria construida por Data "
    "Basis son nulas.",
)

# `liquidacao_nota_fiscal` publishes no link to its liquidacao: the join it used
# was measured wrong for 99.7% of the rows it resolved (see the model's header),
# so `id_liquidacao_bd` is NULL for every row until TCE-MG clarifies the
# identifier semantics. Its own key is self-sufficient and never NULL, which is
# why the table is NOT in ORPHAN_PARENT_KEY.
UNRESOLVED_PARENT: frozenset[str] = frozenset({"liquidacao_nota_fiscal"})

UNRESOLVED_PARENT_SUFFIX: tuple[str, str, str] = (
    "A coluna id_liquidacao_bd está nula em todas as linhas: a correspondência "
    "entre a nota fiscal e a liquidação não pôde ser estabelecida na fonte. A "
    "coluna id_liquidacao preserva o identificador original.",
    "The id_liquidacao_bd column is null on every row: the link between the "
    "invoice and the liquidation could not be established in the source. The "
    "id_liquidacao column preserves the original identifier.",
    "La columna id_liquidacao_bd está nula en todas las filas: la "
    "correspondencia entre la factura y la liquidación no pudo establecerse en "
    "la fuente. La columna id_liquidacao preserva el identificador original.",
)

# Rows the current vintage OMITS because the source record carries an unescaped
# `;` inside a free-text field, so its delimiter count does not match the header.
# `clean_mg.py` drops such rows and counts them; `repair_mg.py` now reconstructs
# those whose reading is uniquely determined, so these counts fall at the next
# refresh. Measured 2026-09-28 against the archives on disk; the figure is the
# omission in the data as published today, which is what a user needs to know.
RAGGED_OMITTED: dict[str, int] = {
    "empenho_fonte": 122952,
    "liquidacao_fonte": 109851,
    "despesa_dotacao": 58253,
    "licitacao_responsavel": 24991,
    "restos_pagar_movimentacao_fonte": 3696,
    "dispensa_dotacao": 608,
}

RAGGED_SUFFIX: tuple[str, str, str] = (
    "Esta tabela omite {n} linhas cujo registro na fonte contém um ponto e vírgula "
    "não escapado em campo de texto livre, o que impede a leitura da linha; a "
    "contagem cai a cada nova extração.",
    "This table omits {n} rows whose source record contains an unescaped semicolon "
    "in a free-text field, which prevents the row from being parsed; the count "
    "falls with each new extraction.",
    "Esta tabla omite {n} filas cuyo registro en la fuente contiene un punto y "
    "coma no escapado en un campo de texto libre, lo que impide leer la fila; el "
    "recuento disminuye con cada nueva extracción.",
)

#: The 9 tables that predate the MG harvest: multi-state, built from several
#: Tribunais de Contas, and registered before the scripts in this directory
#: existed. Their names and descriptions are recorded in TABLES below so the
#: dataset has one glossary, but they are NOT in MG_TABLES and nothing here
#: writes them -- their area, coverage and observation levels are not MG's, and
#: `register_mg_metadata` would stamp them with br_mg and 2014-2026.
ORIGINAL_TABLES: frozenset[str] = frozenset(
    {
        "empenho",
        "liquidacao",
        "pagamento",
        "licitacao",
        "licitacao_item",
        "licitacao_participante",
        "orgao_unidade_gestora",
        "relacionamentos",
        "dicionario",
    }
)

# table slug -> (name_pt, name_en, name_es, description_pt, description_en, description_es)
TABLES: dict[str, tuple[str, str, str, str, str, str]] = {
    "alteracao_orcamentaria": (
        "Alteração orçamentária",
        "Budget amendment",
        "Modificación presupuestaria",
        "Alterações orçamentárias (créditos adicionais, suplementações, anulações e transposições) autorizadas por decreto municipal.",
        "Budget amendments (additional credits, supplementations, cancellations and transfers) authorised by municipal decree.",
        "Modificaciones presupuestarias (créditos adicionales, suplementaciones, anulaciones y transposiciones) autorizadas por decreto municipal.",
    ),
    "contrato": (
        "Contrato",
        "Contract",
        "Contrato",
        "Contratos administrativos firmados pelos municípios, com objeto, vigência, signatário e valores empenhados, liquidados e pagos.",
        "Administrative contracts signed by municipalities, with object, validity, signatory and committed, verified and paid amounts.",
        "Contratos administrativos firmados por los municipios, con objeto, vigencia, firmante y montos comprometidos, liquidados y pagados.",
    ),
    "contrato_apostilamento": (
        "Contrato - Apostilamento",
        "Contract - Endorsement",
        "Contrato - Apostillado",
        "Apostilamentos registrados sobre contratos administrativos, com o respectivo valor e data.",
        "Endorsements recorded against administrative contracts, with their amount and date.",
        "Apostillados registrados sobre contratos administrativos, con su monto y fecha.",
    ),
    "contrato_contabilizacao": (
        "Contrato - Contabilização",
        "Contract - Accounting entry",
        "Contrato - Contabilización",
        "Vínculo entre cada contrato e os empenhos que o executam orçamentariamente.",
        "Link between each contract and the budget commitments that execute it.",
        "Vínculo entre cada contrato y los compromisos que lo ejecutan presupuestariamente.",
    ),
    "contrato_credito": (
        "Contrato - Crédito orçamentário",
        "Contract - Budget credit",
        "Contrato - Crédito presupuestario",
        "Créditos orçamentários (dotações) vinculados a cada contrato administrativo.",
        "Budget credits (appropriations) linked to each administrative contract.",
        "Créditos presupuestarios (partidas) vinculados a cada contrato administrativo.",
    ),
    "contrato_item": (
        "Contrato - Item",
        "Contract - Item",
        "Contrato - Ítem",
        "Itens contratados em cada contrato administrativo, com quantidade, unidade de medida e preço unitário.",
        "Items contracted under each administrative contract, with quantity, unit of measure and unit price.",
        "Ítems contratados en cada contrato administrativo, con cantidad, unidad de medida y precio unitario.",
    ),
    "contrato_rescisao": (
        "Contrato - Rescisão",
        "Contract - Termination",
        "Contrato - Rescisión",
        "Rescisões de contratos administrativos, com motivo, data e valor rescindido.",
        "Terminations of administrative contracts, with reason, date and terminated amount.",
        "Rescisiones de contratos administrativos, con motivo, fecha y monto rescindido.",
    ),
    "contrato_termo_aditivo": (
        "Contrato - Termo aditivo",
        "Contract - Amendment",
        "Contrato - Anexo",
        "Termos aditivos de contratos administrativos, com tipo, vigência e valor acrescido ou reduzido.",
        "Amendments to administrative contracts, with type, validity and amount added or reduced.",
        "Anexos de contratos administrativos, con tipo, vigencia y monto incrementado o reducido.",
    ),
    "contrato_termo_aditivo_item": (
        "Contrato - Termo aditivo - Item",
        "Contract - Amendment - Item",
        "Contrato - Anexo - Ítem",
        "Itens alterados por termo aditivo, com quantidade e valor acrescidos ou reduzidos.",
        "Items changed by a contract amendment, with quantity and amount added or reduced.",
        "Ítems modificados por anexo, con cantidad y monto incrementados o reducidos.",
    ),
    "decreto": (
        "Decreto",
        "Decree",
        "Decreto",
        "Decretos municipais que autorizam alterações orçamentárias.",
        "Municipal decrees authorising budget amendments.",
        "Decretos municipales que autorizan modificaciones presupuestarias.",
    ),
    "despesa_dotacao": (
        "Dotação de despesa",
        "Expenditure appropriation",
        "Partida de gasto",
        "Dotações orçamentárias da despesa, com classificação funcional-programática e fonte de recursos.",
        "Budget appropriations for expenditure, with functional-programmatic classification and funding source.",
        "Partidas presupuestarias del gasto, con clasificación funcional-programática y fuente de recursos.",
    ),
    "dicionario": (
        "Dicionário",
        "Dictionary",
        "Diccionario",
        "Dicionário para tradução dos códigos das tabelas do do conjunto Microdados de Despesas de Entes Subnacionais (MiDES). Para códigos definidos por outras instituições, como id_municipio ou cnaes, buscar por diretórios",
        "Dictionary for translating the codes of the tables in the Subnational Entities Expenditure Microdata Set (MiDES). For codes defined by other institutions, such as id_municipio or cnaes, search for directories",
        "Diccionario para la traducción de los códigos de las tablas del conjunto de Microdatos de Gastos de Entidades Subnacionales (MiDES). Para códigos definidos por otras instituciones, como id_municipio o cnaes, buscar en directorios",
    ),
    "dispensa": (
        "Dispensa e inexigibilidade",
        "Procurement waiver",
        "Dispensa e inexigibilidad",
        "Processos de dispensa e inexigibilidade de licitação, com objeto, natureza e fundamentação.",
        "Procurement waiver and non-enforceability processes, with object, nature and legal basis.",
        "Procesos de dispensa e inexigibilidad de licitación, con objeto, naturaleza y fundamentación.",
    ),
    "dispensa_cotacao": (
        "Dispensa e inexigibilidade - Cotação",
        "Procurement waiver - Quotation",
        "Dispensa e inexigibilidad - Cotización",
        "Cotações de preço coletadas em cada processo de dispensa de licitação.",
        "Price quotations collected in each procurement waiver process.",
        "Cotizaciones de precio recogidas en cada proceso de dispensa de licitación.",
    ),
    "dispensa_credenciado": (
        "Dispensa e inexigibilidade - Credenciado",
        "Procurement waiver - Accredited supplier",
        "Dispensa e inexigibilidad - Acreditado",
        "Fornecedores credenciados em processos de credenciamento.",
        "Suppliers accredited through accreditation processes.",
        "Proveedores acreditados en procesos de acreditación.",
    ),
    "dispensa_dotacao": (
        "Dispensa e inexigibilidade - Dotação orçamentária",
        "Procurement waiver - Budget appropriation",
        "Dispensa e inexigibilidad - Partida presupuestaria",
        "Dotações orçamentárias vinculadas a cada processo de dispensa de licitação.",
        "Budget appropriations linked to each procurement waiver process.",
        "Partidas presupuestarias vinculadas a cada proceso de dispensa de licitación.",
    ),
    "dispensa_fornecedor": (
        "Dispensa e inexigibilidade - Fornecedor",
        "Procurement waiver - Supplier",
        "Dispensa e inexigibilidad - Proveedor",
        "Fornecedores contratados em cada processo de dispensa de licitação.",
        "Suppliers contracted in each procurement waiver process.",
        "Proveedores contratados en cada proceso de dispensa de licitación.",
    ),
    "dispensa_item": (
        "Dispensa e inexigibilidade - Item",
        "Procurement waiver - Item",
        "Dispensa e inexigibilidad - Ítem",
        "Itens objeto de cada processo de dispensa de licitação.",
        "Items that are the object of each procurement waiver process.",
        "Ítems objeto de cada proceso de dispensa de licitación.",
    ),
    "dispensa_responsavel": (
        "Dispensa e inexigibilidade - Responsável",
        "Procurement waiver - Responsible officer",
        "Dispensa e inexigibilidad - Responsable",
        "Responsáveis designados para cada processo de dispensa de licitação.",
        "Officers designated as responsible for each procurement waiver process.",
        "Responsables designados para cada proceso de dispensa de licitación.",
    ),
    "empenho": (
        "Empenho",
        "Commitment",
        "Compromiso",
        "Dados a nível de empenho.",
        "Data at the commitment level.",
        "Datos a nivel de compromiso.",
    ),
    "empenho_credor": (
        "Empenho - Credor",
        "Commitment - Creditor",
        "Compromiso - Acreedor",
        "Credores vinculados a cada empenho, identificados por CPF ou CNPJ.",
        "Creditors linked to each budget commitment, identified by CPF or CNPJ.",
        "Acreedores vinculados a cada compromiso, identificados por CPF o CNPJ.",
    ),
    "empenho_fonte": (
        "Empenho - Fonte de recurso",
        "Commitment - Funding source",
        "Compromiso - Fuente de recurso",
        "Decomposição de cada empenho por fonte de recursos e dotação orçamentária.",
        "Breakdown of each budget commitment by funding source and appropriation.",
        "Desglose de cada compromiso por fuente de recursos y partida presupuestaria.",
    ),
    "lei_decreto": (
        "Decreto - Lei autorizadora",
        "Decree - Authorising law",
        "Decreto - Ley autorizadora",
        "Leis municipais que autorizam os decretos de alteração orçamentária.",
        "Municipal laws authorising the budget amendment decrees.",
        "Leyes municipales que autorizan los decretos de modificación presupuestaria.",
    ),
    "licitacao": (
        "Licitação",
        "Tender",
        "Licitación",
        "Dados a nível de licitação.",
        "Data at the tender level.",
        "Datos a nivel de licitación.",
    ),
    "licitacao_comissao": (
        "Licitação - Comissão",
        "Tender - Committee",
        "Licitación - Comisión",
        "Membros da comissão de licitação designados em cada processo licitatório.",
        "Members of the procurement committee designated in each procurement process.",
        "Miembros de la comisión de licitación designados en cada proceso licitatorio.",
    ),
    "licitacao_cotacao": (
        "Licitação - Cotação",
        "Tender - Quotation",
        "Licitación - Cotización",
        "Cotações de preço apresentadas para cada item licitado.",
        "Price quotations submitted for each tendered item.",
        "Cotizaciones de precio presentadas para cada ítem licitado.",
    ),
    "licitacao_dotacao": (
        "Licitação - Dotação orçamentária",
        "Tender - Budget appropriation",
        "Licitación - Partida presupuestaria",
        "Dotações orçamentárias vinculadas a cada processo licitatório.",
        "Budget appropriations linked to each procurement process.",
        "Partidas presupuestarias vinculadas a cada proceso licitatorio.",
    ),
    "licitacao_homologacao": (
        "Licitação - Homologação",
        "Tender - Award",
        "Licitación - Homologación",
        "Homologação e adjudicação de cada item licitado, com o vencedor e o valor homologado.",
        "Award and adjudication of each tendered item, with the winner and the awarded amount.",
        "Homologación y adjudicación de cada ítem licitado, con el ganador y el monto homologado.",
    ),
    "licitacao_item": (
        "Licitação - Item",
        "Tender - Item",
        "Licitación - Ítem",
        "Dados a nível de licitação-item.",
        "Data at the bid-item level.",
        "Datos a nivel de licitación-item.",
    ),
    "licitacao_julgamento": (
        "Licitação - Julgamento",
        "Tender - Adjudication",
        "Licitación - Juzgamiento",
        "Propostas julgadas para cada item licitado, com licitante, valor e classificação.",
        "Bids adjudicated for each tendered item, with bidder, amount and ranking.",
        "Propuestas juzgadas para cada ítem licitado, con licitante, monto y clasificación.",
    ),
    "licitacao_parecer": (
        "Licitação - Parecer",
        "Tender - Opinion",
        "Licitación - Dictamen",
        "Pareceres técnicos e jurídicos emitidos em cada processo licitatório.",
        "Technical and legal opinions issued in each procurement process.",
        "Dictámenes técnicos y jurídicos emitidos en cada proceso licitatorio.",
    ),
    "licitacao_participante": (
        "Licitação - Participante",
        "Tender - Participant",
        "Licitación - Participante",
        "Dados a nível de licitação-participante.",
        "Data at the tender-participant level.",
        "Datos a nivel de licitación-participante.",
    ),
    "licitacao_quadro_societario": (
        "Licitação - Participante - Quadro societário",
        "Tender - Participant - Shareholding",
        "Licitación - Participante - Cuadro societario",
        "Quadro societário dos participantes de processos licitatórios.",
        "Shareholding structure of participants in procurement processes.",
        "Cuadro societario de los participantes en procesos licitatorios.",
    ),
    "licitacao_responsavel": (
        "Licitação - Responsável",
        "Tender - Responsible officer",
        "Licitación - Responsable",
        "Responsáveis designados para cada processo licitatório.",
        "Officers designated as responsible for each procurement process.",
        "Responsables designados para cada proceso licitatorio.",
    ),
    "liquidacao": (
        "Liquidação",
        "Settlement",
        "Liquidación",
        "Dados a nível de liquidação.",
        "Data at settlement level.",
        "Datos a nivel de liquidación.",
    ),
    "liquidacao_fonte": (
        "Liquidação - Fonte de recurso",
        "Settlement - Funding source",
        "Liquidación - Fuente de recurso",
        "Decomposição de cada liquidação por fonte de recursos.",
        "Breakdown of each expenditure verification by funding source.",
        "Desglose de cada liquidación por fuente de recursos.",
    ),
    "liquidacao_nota_fiscal": (
        "Liquidação - Nota fiscal",
        "Settlement - Invoice",
        "Liquidación - Factura",
        "Notas fiscais vinculadas a cada liquidação de despesa.",
        "Invoices linked to each expenditure verification.",
        "Facturas vinculadas a cada liquidación de gasto.",
    ),
    "nota_fiscal": (
        "Nota fiscal",
        "Invoice",
        "Factura",
        "Notas fiscais recebidas pelos municípios, com emitente, série, chave e valores.",
        "Invoices received by municipalities, with issuer, series, key and amounts.",
        "Facturas recibidas por los municipios, con emisor, serie, clave y montos.",
    ),
    "nota_fiscal_item": (
        "Nota fiscal - Item",
        "Invoice - Item",
        "Factura - Ítem",
        "Itens discriminados em cada nota fiscal, com quantidade e valor unitário.",
        "Items itemised on each invoice, with quantity and unit value.",
        "Ítems discriminados en cada factura, con cantidad y valor unitario.",
    ),
    "orgao_unidade_gestora": (
        "Órgão e unidade gestora",
        "Government body and managing unit",
        "Órgano y unidad gestora",
        "Dados auxiliares a nível de órgão e unidade gestora.",
        "Auxiliary data at the level of organ and managing unit.",
        "Datos auxiliares a nivel de órgano y unidad gestora.",
    ),
    "pagamento": (
        "Pagamento",
        "Payment",
        "Pago",
        "Dados a nível de pagamento.",
        "Data at the payment level.",
        "Datos a nivel de pago.",
    ),
    "pagamento_movimento": (
        "Pagamento - Movimentação",
        "Payment - Movement",
        "Pago - Movimiento",
        "Movimentações bancárias associadas a cada pagamento, com instituição financeira, agência e conta.",
        "Banking movements associated with each payment, with financial institution, branch and account.",
        "Movimientos bancarios asociados a cada pago, con institución financiera, sucursal y cuenta.",
    ),
    "registro_preco_adesao": (
        "Adesão a registro de preços",
        "Price registry adhesion",
        "Adhesión a registro de precios",
        "Adesões a atas de registro de preços gerenciadas por outro órgão.",
        "Adhesions to price registries managed by another public body.",
        "Adhesiones a actas de registro de precios gestionadas por otro órgano.",
    ),
    "registro_preco_adesao_cotacao": (
        "Adesão a registro de preços - Cotação",
        "Price registry adhesion - Quotation",
        "Adhesión a registro de precios - Cotización",
        "Cotações de preço coletadas para cada adesão a ata de registro de preços.",
        "Price quotations collected for each price-registry adhesion.",
        "Cotizaciones de precio recogidas para cada adhesión al acta de registro de precios.",
    ),
    "registro_preco_adesao_item": (
        "Adesão a registro de preços - Item",
        "Price registry adhesion - Item",
        "Adhesión a registro de precios - Ítem",
        "Itens objeto de cada adesão a ata de registro de preços.",
        "Items that are the object of each price-registry adhesion.",
        "Ítems objeto de cada adhesión al acta de registro de precios.",
    ),
    "registro_preco_adesao_vencedor": (
        "Adesão a registro de preços - Vencedor",
        "Price registry adhesion - Winner",
        "Adhesión a registro de precios - Ganador",
        "Fornecedores vencedores em cada adesão a ata de registro de preços.",
        "Winning suppliers in each price-registry adhesion.",
        "Proveedores ganadores en cada adhesión al acta de registro de precios.",
    ),
    "relacionamentos": (
        "Relacionamentos",
        "Relationships",
        "Relaciones",
        "Dados a nível de relacionamento.",
        "Relationship-level data.",
        "Datos a nivel de relación.",
    ),
    "restos_pagar": (
        "Restos a pagar",
        "Carried-over commitment",
        "Residuos pasivos",
        "Saldos de restos a pagar processados e não processados inscritos por exercício de origem.",
        "Balances of processed and unprocessed carried-over commitments, recorded by originating financial year.",
        "Saldos de residuos pasivos procesados y no procesados inscritos por ejercicio de origen.",
    ),
    "restos_pagar_credor": (
        "Restos a pagar - Credor",
        "Carried-over commitment - Creditor",
        "Residuos pasivos - Acreedor",
        "Credores vinculados a cada inscrição de restos a pagar.",
        "Creditors linked to each carried-over commitment record.",
        "Acreedores vinculados a cada inscripción de residuos pasivos.",
    ),
    "restos_pagar_movimentacao": (
        "Restos a pagar - Movimentação",
        "Carried-over commitment - Movement",
        "Residuos pasivos - Movimiento",
        "Movimentações de restos a pagar: pagamentos, anulações, cancelamentos e outras baixas.",
        "Movements of carried-over commitments: payments, cancellations, write-offs and other reductions.",
        "Movimientos de residuos pasivos: pagos, anulaciones, cancelaciones y otras bajas.",
    ),
    "restos_pagar_movimentacao_credor": (
        "Restos a pagar - Movimentação - Credor",
        "Carried-over commitment - Movement - Creditor",
        "Residuos pasivos - Movimiento - Acreedor",
        "Credores vinculados a cada movimentação de restos a pagar.",
        "Creditors linked to each carried-over commitment movement.",
        "Acreedores vinculados a cada movimiento de residuos pasivos.",
    ),
    "restos_pagar_movimentacao_fonte": (
        "Restos a pagar - Movimentação - Fonte de recurso",
        "Carried-over commitment - Movement - Funding source",
        "Residuos pasivos - Movimiento - Fuente de recurso",
        "Decomposição de cada movimentação de restos a pagar por fonte de recursos.",
        "Breakdown of each carried-over commitment movement by funding source.",
        "Desglose de cada movimiento de residuos pasivos por fuente de recursos.",
    ),
}


def name(table: str, lang: str = "pt") -> str:
    return TABLES[table][LANGS.index(lang)]


def description(table: str, lang: str = "pt", with_suffix: bool = True) -> str:
    idx = LANGS.index(lang)
    text = TABLES[table][3 + idx]
    if not with_suffix:
        return text
    out = f"{text} {SUFFIX[idx]}"
    if table in ORPHAN_PARENT_KEY:
        out = f"{out} {ORPHAN_KEY_SUFFIX[idx]}"
    if table in UNRESOLVED_PARENT:
        out = f"{out} {UNRESOLVED_PARENT_SUFFIX[idx]}"
    if table in RAGGED_OMITTED:
        # pt and es group thousands with '.', en with ','.
        grouped = f"{RAGGED_OMITTED[table]:,}"
        if lang != "en":
            grouped = grouped.replace(",", ".")
        out = f"{out} {RAGGED_SUFFIX[idx].format(n=grouped)}"
    return out


#: Display order of the dataset's tables on the site, depth-first: each child
#: sits directly under its parent, matching the hierarchical names above
#: ("Contrato", then "Contrato - Item", then "Contrato - Termo aditivo - Item").
#: Covers all 52 tables, not only the 43 in `MG_TABLES`, because
#: `reorder_tables` restates the whole dataset -- the 9 original multi-state
#: tables lead, since the expenditure chain is what the dataset is about.

#: The 43 MG-only tables: everything in TABLES that is not one of the 9 above.
#: This -- not TABLES -- is the scope of `register_mg_metadata`,
#: `gen_mg_schema`, `verify_mg_metadata`, `verify_mg_bigquery` and
#: `build_mg_auxiliary_files`.
MG_TABLES: frozenset[str] = frozenset(TABLES) - ORIGINAL_TABLES

TABLE_ORDER = [
    "empenho",
    "empenho_credor",
    "empenho_fonte",
    "liquidacao",
    "liquidacao_fonte",
    "liquidacao_nota_fiscal",
    "pagamento",
    "pagamento_movimento",
    "restos_pagar",
    "restos_pagar_credor",
    "restos_pagar_movimentacao",
    "restos_pagar_movimentacao_credor",
    "restos_pagar_movimentacao_fonte",
    "licitacao",
    "licitacao_item",
    "licitacao_participante",
    "licitacao_quadro_societario",
    "licitacao_cotacao",
    "licitacao_comissao",
    "licitacao_responsavel",
    "licitacao_julgamento",
    "licitacao_homologacao",
    "licitacao_parecer",
    "licitacao_dotacao",
    "dispensa",
    "dispensa_item",
    "dispensa_fornecedor",
    "dispensa_cotacao",
    "dispensa_credenciado",
    "dispensa_responsavel",
    "dispensa_dotacao",
    "registro_preco_adesao",
    "registro_preco_adesao_item",
    "registro_preco_adesao_cotacao",
    "registro_preco_adesao_vencedor",
    "contrato",
    "contrato_item",
    "contrato_termo_aditivo",
    "contrato_termo_aditivo_item",
    "contrato_apostilamento",
    "contrato_rescisao",
    "contrato_credito",
    "contrato_contabilizacao",
    "nota_fiscal",
    "nota_fiscal_item",
    "despesa_dotacao",
    "alteracao_orcamentaria",
    "decreto",
    "lei_decreto",
    "orgao_unidade_gestora",
    "relacionamentos",
    "dicionario",
]
