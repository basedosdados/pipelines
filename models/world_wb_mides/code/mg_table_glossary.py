"""Trilingual names and descriptions for the 43 MG-only tables.

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

# table slug -> (name_pt, name_en, name_es, description_pt, description_en, description_es)
TABLES: dict[str, tuple[str, str, str, str, str, str]] = {
    "alteracao_orcamentaria": (
        "Alterações Orçamentárias",
        "Budget Amendments",
        "Modificaciones Presupuestarias",
        "Alterações orçamentárias (créditos adicionais, suplementações, anulações e transposições) autorizadas por decreto municipal.",
        "Budget amendments (additional credits, supplementations, cancellations and transfers) authorised by municipal decree.",
        "Modificaciones presupuestarias (créditos adicionales, suplementaciones, anulaciones y transposiciones) autorizadas por decreto municipal.",
    ),
    "contrato": (
        "Contratos",
        "Contracts",
        "Contratos",
        "Contratos administrativos firmados pelos municípios, com objeto, vigência, signatário e valores empenhados, liquidados e pagos.",
        "Administrative contracts signed by municipalities, with object, validity, signatory and committed, verified and paid amounts.",
        "Contratos administrativos firmados por los municipios, con objeto, vigencia, firmante y montos comprometidos, liquidados y pagados.",
    ),
    "contrato_apostilamento": (
        "Apostilamentos de Contrato",
        "Contract Endorsements",
        "Apostillados de Contrato",
        "Apostilamentos registrados sobre contratos administrativos, com o respectivo valor e data.",
        "Endorsements recorded against administrative contracts, with their amount and date.",
        "Apostillados registrados sobre contratos administrativos, con su monto y fecha.",
    ),
    "contrato_contabilizacao": (
        "Contabilização de Contratos",
        "Contract Accounting Entries",
        "Contabilización de Contratos",
        "Vínculo entre cada contrato e os empenhos que o executam orçamentariamente.",
        "Link between each contract and the budget commitments that execute it.",
        "Vínculo entre cada contrato y los compromisos que lo ejecutan presupuestariamente.",
    ),
    "contrato_credito": (
        "Créditos Orçamentários de Contratos",
        "Contract Budget Credits",
        "Créditos Presupuestarios de Contratos",
        "Créditos orçamentários (dotações) vinculados a cada contrato administrativo.",
        "Budget credits (appropriations) linked to each administrative contract.",
        "Créditos presupuestarios (partidas) vinculados a cada contrato administrativo.",
    ),
    "contrato_item": (
        "Itens de Contrato",
        "Contract Items",
        "Ítems de Contrato",
        "Itens contratados em cada contrato administrativo, com quantidade, unidade de medida e preço unitário.",
        "Items contracted under each administrative contract, with quantity, unit of measure and unit price.",
        "Ítems contratados en cada contrato administrativo, con cantidad, unidad de medida y precio unitario.",
    ),
    "contrato_rescisao": (
        "Rescisões de Contrato",
        "Contract Terminations",
        "Rescisiones de Contrato",
        "Rescisões de contratos administrativos, com motivo, data e valor rescindido.",
        "Terminations of administrative contracts, with reason, date and terminated amount.",
        "Rescisiones de contratos administrativos, con motivo, fecha y monto rescindido.",
    ),
    "contrato_termo_aditivo": (
        "Termos Aditivos de Contrato",
        "Contract Amendments",
        "Anexos de Contrato",
        "Termos aditivos de contratos administrativos, com tipo, vigência e valor acrescido ou reduzido.",
        "Amendments to administrative contracts, with type, validity and amount added or reduced.",
        "Anexos de contratos administrativos, con tipo, vigencia y monto incrementado o reducido.",
    ),
    "contrato_termo_aditivo_item": (
        "Itens de Termo Aditivo",
        "Contract Amendment Items",
        "Ítems de Anexo de Contrato",
        "Itens alterados por termo aditivo, com quantidade e valor acrescidos ou reduzidos.",
        "Items changed by a contract amendment, with quantity and amount added or reduced.",
        "Ítems modificados por anexo, con cantidad y monto incrementados o reducidos.",
    ),
    "decreto": (
        "Decretos",
        "Decrees",
        "Decretos",
        "Decretos municipais que autorizam alterações orçamentárias.",
        "Municipal decrees authorising budget amendments.",
        "Decretos municipales que autorizan modificaciones presupuestarias.",
    ),
    "despesa_dotacao": (
        "Dotações de Despesa",
        "Expenditure Appropriations",
        "Partidas de Gasto",
        "Dotações orçamentárias da despesa, com classificação funcional-programática e fonte de recursos.",
        "Budget appropriations for expenditure, with functional-programmatic classification and funding source.",
        "Partidas presupuestarias del gasto, con clasificación funcional-programática y fuente de recursos.",
    ),
    "dispensa": (
        "Dispensas e Inexigibilidades",
        "Procurement Waivers",
        "Dispensas e Inexigibilidades",
        "Processos de dispensa e inexigibilidade de licitação, com objeto, natureza e fundamentação.",
        "Procurement waiver and non-enforceability processes, with object, nature and legal basis.",
        "Procesos de dispensa e inexigibilidad de licitación, con objeto, naturaleza y fundamentación.",
    ),
    "dispensa_cotacao": (
        "Cotações de Dispensa",
        "Waiver Quotations",
        "Cotizaciones de Dispensa",
        "Cotações de preço coletadas em cada processo de dispensa de licitação.",
        "Price quotations collected in each procurement waiver process.",
        "Cotizaciones de precio recogidas en cada proceso de dispensa de licitación.",
    ),
    "dispensa_credenciado": (
        "Credenciados em Dispensa",
        "Accredited Suppliers",
        "Acreditados en Dispensa",
        "Fornecedores credenciados em processos de credenciamento.",
        "Suppliers accredited through accreditation processes.",
        "Proveedores acreditados en procesos de acreditación.",
    ),
    "dispensa_dotacao": (
        "Dotações de Dispensa",
        "Waiver Budget Appropriations",
        "Partidas de Dispensa",
        "Dotações orçamentárias vinculadas a cada processo de dispensa de licitação.",
        "Budget appropriations linked to each procurement waiver process.",
        "Partidas presupuestarias vinculadas a cada proceso de dispensa de licitación.",
    ),
    "dispensa_fornecedor": (
        "Fornecedores de Dispensa",
        "Waiver Suppliers",
        "Proveedores de Dispensa",
        "Fornecedores contratados em cada processo de dispensa de licitação.",
        "Suppliers contracted in each procurement waiver process.",
        "Proveedores contratados en cada proceso de dispensa de licitación.",
    ),
    "dispensa_item": (
        "Itens de Dispensa",
        "Waiver Items",
        "Ítems de Dispensa",
        "Itens objeto de cada processo de dispensa de licitação.",
        "Items that are the object of each procurement waiver process.",
        "Ítems objeto de cada proceso de dispensa de licitación.",
    ),
    "dispensa_responsavel": (
        "Responsáveis por Dispensa",
        "Waiver Responsible Officers",
        "Responsables de Dispensa",
        "Responsáveis designados para cada processo de dispensa de licitação.",
        "Officers designated as responsible for each procurement waiver process.",
        "Responsables designados para cada proceso de dispensa de licitación.",
    ),
    "empenho_credor": (
        "Credores de Empenho",
        "Commitment Creditors",
        "Acreedores de Compromiso",
        "Credores vinculados a cada empenho, identificados por CPF ou CNPJ.",
        "Creditors linked to each budget commitment, identified by CPF or CNPJ.",
        "Acreedores vinculados a cada compromiso, identificados por CPF o CNPJ.",
    ),
    "empenho_fonte": (
        "Fontes de Recurso do Empenho",
        "Commitment Funding Sources",
        "Fuentes de Recurso del Compromiso",
        "Decomposição de cada empenho por fonte de recursos e dotação orçamentária.",
        "Breakdown of each budget commitment by funding source and appropriation.",
        "Desglose de cada compromiso por fuente de recursos y partida presupuestaria.",
    ),
    "lei_decreto": (
        "Leis Autorizadoras de Decreto",
        "Laws Authorising Decrees",
        "Leyes Autorizadoras de Decreto",
        "Leis municipais que autorizam os decretos de alteração orçamentária.",
        "Municipal laws authorising the budget amendment decrees.",
        "Leyes municipales que autorizan los decretos de modificación presupuestaria.",
    ),
    "licitacao_comissao": (
        "Comissões de Licitação",
        "Procurement Committees",
        "Comisiones de Licitación",
        "Membros da comissão de licitação designados em cada processo licitatório.",
        "Members of the procurement committee designated in each procurement process.",
        "Miembros de la comisión de licitación designados en cada proceso licitatorio.",
    ),
    "licitacao_cotacao": (
        "Cotações de Licitação",
        "Procurement Quotations",
        "Cotizaciones de Licitación",
        "Cotações de preço apresentadas para cada item licitado.",
        "Price quotations submitted for each tendered item.",
        "Cotizaciones de precio presentadas para cada ítem licitado.",
    ),
    "licitacao_dotacao": (
        "Dotações de Licitação",
        "Procurement Budget Appropriations",
        "Partidas de Licitación",
        "Dotações orçamentárias vinculadas a cada processo licitatório.",
        "Budget appropriations linked to each procurement process.",
        "Partidas presupuestarias vinculadas a cada proceso licitatorio.",
    ),
    "licitacao_homologacao": (
        "Homologações de Licitação",
        "Procurement Awards",
        "Homologaciones de Licitación",
        "Homologação e adjudicação de cada item licitado, com o vencedor e o valor homologado.",
        "Award and adjudication of each tendered item, with the winner and the awarded amount.",
        "Homologación y adjudicación de cada ítem licitado, con el ganador y el monto homologado.",
    ),
    "licitacao_julgamento": (
        "Julgamentos de Licitação",
        "Procurement Adjudications",
        "Juzgamientos de Licitación",
        "Propostas julgadas para cada item licitado, com licitante, valor e classificação.",
        "Bids adjudicated for each tendered item, with bidder, amount and ranking.",
        "Propuestas juzgadas para cada ítem licitado, con licitante, monto y clasificación.",
    ),
    "licitacao_parecer": (
        "Pareceres de Licitação",
        "Procurement Opinions",
        "Dictámenes de Licitación",
        "Pareceres técnicos e jurídicos emitidos em cada processo licitatório.",
        "Technical and legal opinions issued in each procurement process.",
        "Dictámenes técnicos y jurídicos emitidos en cada proceso licitatorio.",
    ),
    "licitacao_quadro_societario": (
        "Quadro Societário de Participantes",
        "Bidder Shareholding",
        "Cuadro Societario de Participantes",
        "Quadro societário dos participantes de processos licitatórios.",
        "Shareholding structure of participants in procurement processes.",
        "Cuadro societario de los participantes en procesos licitatorios.",
    ),
    "licitacao_responsavel": (
        "Responsáveis por Licitação",
        "Procurement Responsible Officers",
        "Responsables de Licitación",
        "Responsáveis designados para cada processo licitatório.",
        "Officers designated as responsible for each procurement process.",
        "Responsables designados para cada proceso licitatorio.",
    ),
    "liquidacao_fonte": (
        "Fontes de Recurso da Liquidação",
        "Verification Funding Sources",
        "Fuentes de Recurso de la Liquidación",
        "Decomposição de cada liquidação por fonte de recursos.",
        "Breakdown of each expenditure verification by funding source.",
        "Desglose de cada liquidación por fuente de recursos.",
    ),
    "liquidacao_nota_fiscal": (
        "Notas Fiscais da Liquidação",
        "Verification Invoices",
        "Facturas de la Liquidación",
        "Notas fiscais vinculadas a cada liquidação de despesa.",
        "Invoices linked to each expenditure verification.",
        "Facturas vinculadas a cada liquidación de gasto.",
    ),
    "nota_fiscal": (
        "Notas Fiscais",
        "Invoices",
        "Facturas",
        "Notas fiscais recebidas pelos municípios, com emitente, série, chave e valores.",
        "Invoices received by municipalities, with issuer, series, key and amounts.",
        "Facturas recibidas por los municipios, con emisor, serie, clave y montos.",
    ),
    "nota_fiscal_item": (
        "Itens de Nota Fiscal",
        "Invoice Items",
        "Ítems de Factura",
        "Itens discriminados em cada nota fiscal, com quantidade e valor unitário.",
        "Items itemised on each invoice, with quantity and unit value.",
        "Ítems discriminados en cada factura, con cantidad y valor unitario.",
    ),
    "pagamento_movimento": (
        "Movimentações de Pagamento",
        "Payment Movements",
        "Movimientos de Pago",
        "Movimentações bancárias associadas a cada pagamento, com instituição financeira, agência e conta.",
        "Banking movements associated with each payment, with financial institution, branch and account.",
        "Movimientos bancarios asociados a cada pago, con institución financiera, sucursal y cuenta.",
    ),
    "registro_preco_adesao": (
        "Adesões a Registro de Preços",
        "Price Registry Adhesions",
        "Adhesiones a Registro de Precios",
        "Adesões a atas de registro de preços gerenciadas por outro órgão.",
        "Adhesions to price registries managed by another public body.",
        "Adhesiones a actas de registro de precios gestionadas por otro órgano.",
    ),
    "registro_preco_adesao_cotacao": (
        "Cotações de Adesão",
        "Adhesion Quotations",
        "Cotizaciones de Adhesión",
        "Cotações de preço coletadas para cada adesão a ata de registro de preços.",
        "Price quotations collected for each price-registry adhesion.",
        "Cotizaciones de precio recogidas para cada adhesión al acta de registro de precios.",
    ),
    "registro_preco_adesao_item": (
        "Itens de Adesão",
        "Adhesion Items",
        "Ítems de Adhesión",
        "Itens objeto de cada adesão a ata de registro de preços.",
        "Items that are the object of each price-registry adhesion.",
        "Ítems objeto de cada adhesión al acta de registro de precios.",
    ),
    "registro_preco_adesao_vencedor": (
        "Vencedores de Adesão",
        "Adhesion Winners",
        "Ganadores de Adhesión",
        "Fornecedores vencedores em cada adesão a ata de registro de preços.",
        "Winning suppliers in each price-registry adhesion.",
        "Proveedores ganadores en cada adhesión al acta de registro de precios.",
    ),
    "restos_pagar": (
        "Restos a Pagar",
        "Carried-Over Commitments",
        "Residuos Pasivos",
        "Saldos de restos a pagar processados e não processados inscritos por exercício de origem.",
        "Balances of processed and unprocessed carried-over commitments, recorded by originating financial year.",
        "Saldos de residuos pasivos procesados y no procesados inscritos por ejercicio de origen.",
    ),
    "restos_pagar_credor": (
        "Credores de Restos a Pagar",
        "Carried-Over Commitment Creditors",
        "Acreedores de Residuos Pasivos",
        "Credores vinculados a cada inscrição de restos a pagar.",
        "Creditors linked to each carried-over commitment record.",
        "Acreedores vinculados a cada inscripción de residuos pasivos.",
    ),
    "restos_pagar_movimentacao": (
        "Movimentações de Restos a Pagar",
        "Carried-Over Commitment Movements",
        "Movimientos de Residuos Pasivos",
        "Movimentações de restos a pagar: pagamentos, anulações, cancelamentos e outras baixas.",
        "Movements of carried-over commitments: payments, cancellations, write-offs and other reductions.",
        "Movimientos de residuos pasivos: pagos, anulaciones, cancelaciones y otras bajas.",
    ),
    "restos_pagar_movimentacao_credor": (
        "Credores da Movimentação de Restos a Pagar",
        "Movement Creditors",
        "Acreedores del Movimiento de Residuos Pasivos",
        "Credores vinculados a cada movimentação de restos a pagar.",
        "Creditors linked to each carried-over commitment movement.",
        "Acreedores vinculados a cada movimiento de residuos pasivos.",
    ),
    "restos_pagar_movimentacao_fonte": (
        "Fontes da Movimentação de Restos a Pagar",
        "Movement Funding Sources",
        "Fuentes del Movimiento de Residuos Pasivos",
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
    return f"{text} {SUFFIX[idx]}" if with_suffix else text
