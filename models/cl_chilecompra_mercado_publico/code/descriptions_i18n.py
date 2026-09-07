"""Trilingual column descriptions for cl_chilecompra_mercado_publico.

The architecture CSVs carry Portuguese only, but every column must be registered in
Portuguese, English and Spanish. This maps each distinct Portuguese description to its
English and Spanish forms, so the three stay in one place and a description edited in
the architecture fails loudly here rather than silently registering PT-only.

Terminology held constant throughout:

| Portuguese | English | Spanish |
|---|---|---|
| ordem de compra | purchase order | orden de compra |
| licitacao | tender | licitación |
| fornecedor | supplier | proveedor |
| fornecedor proponente | bidding supplier | proveedor oferente |
| unidade compradora | purchasing unit | unidad compradora |
| proposta | bid | oferta |
| linha de item | item line | línea de ítem |
| comuna / regiao | comuna / region | comuna / región |

Proper nouns are left untranslated in all three: Mercado Público, Compra Ágil,
Contraloría, convenio marco, trato directo, bases tipo, UNSPSC, RUT, CUT, IVA.
"""

from __future__ import annotations

# Portuguese -> (English, Spanish)
DESCRIPTIONS: dict[str, tuple[str, str]] = {
    "Aliquota de IVA aplicada a ordem de compra": (
        "VAT rate applied to the purchase order",
        "Tasa de IVA aplicada a la orden de compra",
    ),
    "Ano de envio da ordem de compra": (
        "Year the purchase order was sent",
        "Año de envío de la orden de compra",
    ),
    "Ano de publicacao da licitacao": (
        "Year the tender was published",
        "Año de publicación de la licitación",
    ),
    "Atividade economica declarada pelo fornecedor": (
        "Economic activity declared by the supplier",
        "Actividad económica declarada por el proveedor",
    ),
    "Atividades economicas declaradas pela unidade compradora": (
        "Economic activities declared by the purchasing unit",
        "Actividades económicas declaradas por la unidad compradora",
    ),
    "Categoria do item, com a hierarquia completa de rubros": (
        "Item category, with the full rubro hierarchy",
        "Categoría del ítem, con la jerarquía completa de rubros",
    ),
    "Cidade da unidade compradora": (
        "City of the purchasing unit",
        "Ciudad de la unidad compradora",
    ),
    "Codigo UNSPSC do produto ou servico": (
        "UNSPSC code of the product or service",
        "Código UNSPSC del producto o servicio",
    ),
    "Codigo da categoria do item": (
        "Code of the item category",
        "Código de la categoría del ítem",
    ),
    "Codigo da filial do fornecedor": (
        "Code of the supplier's branch",
        "Código de la sucursal del proveedor",
    ),
    "Codigo da filial do fornecedor proponente": (
        "Code of the bidding supplier's branch",
        "Código de la sucursal del proveedor oferente",
    ),
    "Codigo da forma de pagamento": (
        "Code of the payment method",
        "Código de la forma de pago",
    ),
    "Codigo da licitacao que originou a ordem de compra": (
        "Code of the tender the purchase order came from",
        "Código de la licitación que originó la orden de compra",
    ),
    "Codigo da linha de item da licitacao": (
        "Code of the tender's item line",
        "Código de la línea de ítem de la licitación",
    ),
    "Codigo da moeda da licitacao": (
        "Code of the tender's currency",
        "Código de la moneda de la licitación",
    ),
    "Codigo da unidade compradora": (
        "Code of the purchasing unit",
        "Código de la unidad compradora",
    ),
    "Codigo detalhado do estado da licitacao": (
        "Detailed code of the tender status",
        "Código detallado del estado de la licitación",
    ),
    "Codigo do convenio marco que originou a ordem de compra": (
        "Code of the convenio marco the purchase order came from",
        "Código del convenio marco que originó la orden de compra",
    ),
    "Codigo do estado da licitacao": (
        "Code of the tender status",
        "Código del estado de la licitación",
    ),
    "Codigo do estado da ordem de compra": (
        "Code of the purchase order status",
        "Código del estado de la orden de compra",
    ),
    "Codigo do estado da ordem segundo o fornecedor": (
        "Code of the order status as reported by the supplier",
        "Código del estado de la orden según el proveedor",
    ),
    "Codigo do fornecedor": ("Code of the supplier", "Código del proveedor"),
    "Codigo do fornecedor proponente": (
        "Code of the bidding supplier",
        "Código del proveedor oferente",
    ),
    "Codigo do organismo publico comprador": (
        "Code of the public purchasing agency",
        "Código del organismo público comprador",
    ),
    "Codigo do organismo publico licitante": (
        "Code of the public tendering agency",
        "Código del organismo público licitante",
    ),
    "Codigo do tipo de contrato": (
        "Code of the contract type",
        "Código del tipo de contrato",
    ),
    "Codigo do tipo de despacho": (
        "Code of the dispatch type",
        "Código del tipo de despacho",
    ),
    "Codigo do tipo de licitacao": (
        "Code of the tender type",
        "Código del tipo de licitación",
    ),
    "Codigo do tipo de ordem de compra": (
        "Code of the purchase order type",
        "Código del tipo de orden de compra",
    ),
    "Codigo publico da licitacao, no formato <unidade>-<correlativo>-<tipo><ano>": (
        "Public code of the tender, formatted <unit>-<sequence>-<type><year>",
        "Código público de la licitación, en formato <unidad>-<correlativo>-<tipo><año>",
    ),
    "Codigo publico da ordem de compra, no formato <unidade>-<correlativo>-<tipo><ano>": (
        "Public code of the purchase order, formatted <unit>-<sequence>-<type><year>",
        "Código público de la orden de compra, en formato <unidad>-<correlativo>-<tipo><año>",
    ),
    "Codigo unico territorial (CUT) da comuna da unidade compradora": (
        "Unique territorial code (CUT) of the purchasing unit's comuna",
        "Código único territorial (CUT) de la comuna de la unidad compradora",
    ),
    "Codigo unico territorial (CUT) da comuna do fornecedor": (
        "Unique territorial code (CUT) of the supplier's comuna",
        "Código único territorial (CUT) de la comuna del proveedor",
    ),
    "Codigo unico territorial (CUT) da regiao da unidade compradora": (
        "Unique territorial code (CUT) of the purchasing unit's region",
        "Código único territorial (CUT) de la región de la unidad compradora",
    ),
    "Codigo unico territorial (CUT) da regiao do fornecedor": (
        "Unique territorial code (CUT) of the supplier's region",
        "Código único territorial (CUT) de la región del proveedor",
    ),
    "Comuna da unidade compradora": (
        "Comuna of the purchasing unit",
        "Comuna de la unidad compradora",
    ),
    "Comuna do fornecedor": ("Comuna of the supplier", "Comuna del proveedor"),
    "Criterios de avaliacao das propostas, separados por ponto e virgula": (
        "Bid evaluation criteria, separated by semicolons",
        "Criterios de evaluación de las ofertas, separados por punto y coma",
    ),
    "Data da solicitacao de cancelamento": (
        "Date the cancellation was requested",
        "Fecha de la solicitud de cancelación",
    ),
    "Data da ultima modificacao da ordem de compra": (
        "Date the purchase order was last modified",
        "Fecha de la última modificación de la orden de compra",
    ),
    "Data da visita a campo": (
        "Date of the site visit",
        "Fecha de la visita a terreno",
    ),
    "Data de aceitacao da ordem pelo fornecedor": (
        "Date the supplier accepted the order",
        "Fecha de aceptación de la orden por el proveedor",
    ),
    "Data de adjudicacao da licitacao": (
        "Date the tender was awarded",
        "Fecha de adjudicación de la licitación",
    ),
    "Data de cancelamento da ordem de compra": (
        "Date the purchase order was cancelled",
        "Fecha de cancelación de la orden de compra",
    ),
    "Data de criacao da licitacao": (
        "Date the tender was created",
        "Fecha de creación de la licitación",
    ),
    "Data de criacao da ordem de compra": (
        "Date the purchase order was created",
        "Fecha de creación de la orden de compra",
    ),
    "Data de encerramento do recebimento de propostas": (
        "Date bid submission closed",
        "Fecha de cierre de la recepción de ofertas",
    ),
    "Data de entrega de antecedentes": (
        "Date supporting documents were due",
        "Fecha de entrega de antecedentes",
    ),
    "Data de entrega de antecedentes em suporte fisico": (
        "Date supporting documents were due in physical form",
        "Fecha de entrega de antecedentes en soporte físico",
    ),
    "Data de envio da ordem de compra ao fornecedor": (
        "Date the purchase order was sent to the supplier",
        "Fecha de envío de la orden de compra al proveedor",
    ),
    "Data de envio da proposta": (
        "Date the bid was submitted",
        "Fecha de envío de la oferta",
    ),
    "Data de inicio da licitacao": (
        "Date the tender started",
        "Fecha de inicio de la licitación",
    ),
    "Data de publicacao da licitacao": (
        "Date the tender was published",
        "Fecha de publicación de la licitación",
    ),
    "Data de publicacao das respostas as consultas": (
        "Date answers to questions were published",
        "Fecha de publicación de las respuestas a las consultas",
    ),
    "Data do ato administrativo de aprovacao": (
        "Date of the approving administrative act",
        "Fecha del acto administrativo de aprobación",
    ),
    "Data do ato de abertura economica": (
        "Date of the financial opening",
        "Fecha del acto de apertura económica",
    ),
    "Data do ato de abertura tecnica": (
        "Date of the technical opening",
        "Fecha del acto de apertura técnica",
    ),
    "Data estimada de adjudicacao": (
        "Estimated award date",
        "Fecha estimada de adjudicación",
    ),
    "Data estimada de assinatura do contrato": (
        "Estimated contract signing date",
        "Fecha estimada de firma del contrato",
    ),
    "Data final da licitacao": (
        "End date of the tender",
        "Fecha final de la licitación",
    ),
    "Data limite do periodo de avaliacao": (
        "Deadline of the evaluation period",
        "Fecha límite del período de evaluación",
    ),
    "Datas adicionais informadas pelo usuario": (
        "Additional dates entered by the user",
        "Fechas adicionales informadas por el usuario",
    ),
    "Declaracao de proibicao de contratacao": (
        "Declaration of contracting prohibition",
        "Declaración de prohibición de contratación",
    ),
    "Descricao da forma de pagamento": (
        "Description of the payment method",
        "Descripción de la forma de pago",
    ),
    "Descricao da licitacao": (
        "Description of the tender",
        "Descripción de la licitación",
    ),
    "Descricao da linha de aquisicao": (
        "Description of the acquisition line",
        "Descripción de la línea de adquisición",
    ),
    "Descricao da proposta informada pelo fornecedor": (
        "Description of the bid entered by the supplier",
        "Descripción de la oferta informada por el proveedor",
    ),
    "Descricao do tipo de ordem de compra": (
        "Description of the purchase order type",
        "Descripción del tipo de orden de compra",
    ),
    "Descricao dos criterios ou requisitos ambientais": (
        "Description of the environmental criteria or requirements",
        "Descripción de los criterios o requisitos ambientales",
    ),
    "Descricao dos criterios ou requisitos sociais": (
        "Description of the social criteria or requirements",
        "Descripción de los criterios o requisitos sociales",
    ),
    "Descricao e observacoes da ordem de compra": (
        "Description and notes on the purchase order",
        "Descripción y observaciones de la orden de compra",
    ),
    "Duracao prevista do contrato": (
        "Expected contract duration",
        "Duración prevista del contrato",
    ),
    "Endereco da ficha da licitacao no Mercado Publico": (
        "Address of the tender's record page on Mercado Público",
        "Dirección de la ficha de la licitación en Mercado Público",
    ),
    "Endereco da ficha da ordem de compra no Mercado Publico": (
        "Address of the purchase order's record page on Mercado Público",
        "Dirección de la ficha de la orden de compra en Mercado Público",
    ),
    "Endereco da unidade compradora": (
        "Address of the purchasing unit",
        "Dirección de la unidad compradora",
    ),
    "Endereco da visita a campo": (
        "Address of the site visit",
        "Dirección de la visita a terreno",
    ),
    "Endereco de entrega de antecedentes": (
        "Address for delivering supporting documents",
        "Dirección de entrega de antecedentes",
    ),
    "Especificacao do item informada pelo comprador": (
        "Item specification entered by the buyer",
        "Especificación del ítem informada por el comprador",
    ),
    "Especificacao do item informada pelo fornecedor": (
        "Item specification entered by the supplier",
        "Especificación del ítem informada por el proveedor",
    ),
    "Estado da licitacao": ("Tender status", "Estado de la licitación"),
    "Estado da ordem de compra": (
        "Purchase order status",
        "Estado de la orden de compra",
    ),
    "Estado da ordem segundo o fornecedor": (
        "Order status as reported by the supplier",
        "Estado de la orden según el proveedor",
    ),
    "Estado da proposta": ("Bid status", "Estado de la oferta"),
    "Estado das etapas da licitacao": (
        "Status of the tender's stages",
        "Estado de las etapas de la licitación",
    ),
    "Estado de publicidade das propostas": (
        "Disclosure status of the bids",
        "Estado de publicidad de las ofertas",
    ),
    "Estado do convenio de suministro associado": (
        "Status of the associated supply agreement",
        "Estado del convenio de suministro asociado",
    ),
    "Estado final da proposta": (
        "Final bid status",
        "Estado final de la oferta",
    ),
    "Fonte de financiamento da compra": (
        "Funding source of the purchase",
        "Fuente de financiamiento de la compra",
    ),
    "Fonte de financiamento da licitacao": (
        "Funding source of the tender",
        "Fuente de financiamiento de la licitación",
    ),
    "Identificador da linha de item dentro da ordem de compra": (
        "Identifier of the item line within the purchase order",
        "Identificador de la línea de ítem dentro de la orden de compra",
    ),
    "Identificador do plano de compras associado": (
        "Identifier of the associated procurement plan",
        "Identificador del plan de compras asociado",
    ),
    "Identificador interno da licitacao no Mercado Publico": (
        "Internal identifier of the tender on Mercado Público",
        "Identificador interno de la licitación en Mercado Público",
    ),
    "Identificador interno da ordem de compra no Mercado Publico": (
        "Internal identifier of the purchase order on Mercado Público",
        "Identificador interno de la orden de compra en Mercado Público",
    ),
    "Indica se a licitacao aplica criterios ou requisitos ambientais": (
        "Indicates whether the tender applies environmental criteria or requirements",
        "Indica si la licitación aplica criterios o requisitos ambientales",
    ),
    "Indica se a licitacao aplica criterios ou requisitos sociais": (
        "Indicates whether the tender applies social criteria or requirements",
        "Indica si la licitación aplica criterios o requisitos sociales",
    ),
    "Indica se a licitacao e de obras": (
        "Indicates whether the tender is for public works",
        "Indica si la licitación es de obras",
    ),
    "Indica se a licitacao esta sujeita a toma de razao pela Contraloria": (
        "Indicates whether the tender is subject to toma de razón by the Contraloría",
        "Indica si la licitación está sujeta a toma de razón por la Contraloría",
    ),
    "Indica se a licitacao foi informada": (
        "Indicates whether the tender was reported",
        "Indica si la licitación fue informada",
    ),
    "Indica se a licitacao usa bases tipo": (
        "Indicates whether the tender uses bases tipo",
        "Indica si la licitación usa bases tipo",
    ),
    "Indica se a ordem de compra possui itens detalhados": (
        "Indicates whether the purchase order has itemised lines",
        "Indica si la orden de compra tiene ítems detallados",
    ),
    "Indica se a ordem foi emitida pela modalidade Compra Agil": (
        "Indicates whether the order was issued under the Compra Ágil modality",
        "Indica si la orden fue emitida bajo la modalidad Compra Ágil",
    ),
    "Indica se a ordem foi emitida por trato direto": (
        "Indicates whether the order was issued by trato directo",
        "Indica si la orden fue emitida por trato directo",
    ),
    "Indica se a proposta foi a selecionada para a linha de item": (
        "Indicates whether the bid was the one selected for the item line",
        "Indica si la oferta fue la seleccionada para la línea de ítem",
    ),
    "Indica se a subcontratacao e permitida": (
        "Indicates whether subcontracting is allowed",
        "Indica si la subcontratación está permitida",
    ),
    "Indica se ha prorrogacao de prazo": (
        "Indicates whether the deadline is extended",
        "Indica si hay prórroga de plazo",
    ),
    "Indica se o contrato e renovavel": (
        "Indicates whether the contract is renewable",
        "Indica si el contrato es renovable",
    ),
    "Indica se o valor estimado e publico": (
        "Indicates whether the estimated amount is public",
        "Indica si el monto estimado es público",
    ),
    "Justificativa do regime de publicidade das propostas": (
        "Justification for the bid disclosure regime",
        "Justificación del régimen de publicidad de las ofertas",
    ),
    "Justificativa do valor estimado": (
        "Justification for the estimated amount",
        "Justificación del monto estimado",
    ),
    "Mes de envio da ordem de compra": (
        "Month the purchase order was sent",
        "Mes de envío de la orden de compra",
    ),
    "Mes de publicacao da licitacao": (
        "Month the tender was published",
        "Mes de publicación de la licitación",
    ),
    "Modalidade da licitacao": (
        "Tender modality",
        "Modalidad de la licitación",
    ),
    "Moeda da licitacao": (
        "Currency of the tender",
        "Moneda de la licitación",
    ),
    "Moeda da linha de item": (
        "Currency of the item line",
        "Moneda de la línea de ítem",
    ),
    "Moeda em que a ordem de compra foi emitida": (
        "Currency the purchase order was issued in",
        "Moneda en que se emitió la orden de compra",
    ),
    "Moeda em que as propostas sao apresentadas": (
        "Currency the bids are submitted in",
        "Moneda en que se presentan las ofertas",
    ),
    "Nome da filial do fornecedor": (
        "Name of the supplier's branch",
        "Nombre de la sucursal del proveedor",
    ),
    "Nome da licitacao": ("Name of the tender", "Nombre de la licitación"),
    "Nome da linha de aquisicao": (
        "Name of the acquisition line",
        "Nombre de la línea de adquisición",
    ),
    "Nome da ordem de compra": (
        "Name of the purchase order",
        "Nombre de la orden de compra",
    ),
    "Nome da proposta apresentada": (
        "Name of the submitted bid",
        "Nombre de la oferta presentada",
    ),
    "Nome da unidade compradora": (
        "Name of the purchasing unit",
        "Nombre de la unidad compradora",
    ),
    "Nome de fantasia do fornecedor proponente": (
        "Trade name of the bidding supplier",
        "Nombre de fantasía del proveedor oferente",
    ),
    "Nome do fornecedor": ("Name of the supplier", "Nombre del proveedor"),
    "Nome do organismo publico comprador": (
        "Name of the public purchasing agency",
        "Nombre del organismo público comprador",
    ),
    "Nome do organismo publico licitante": (
        "Name of the public tendering agency",
        "Nombre del organismo público licitante",
    ),
    "Nome generico do produto ou servico": (
        "Generic name of the product or service",
        "Nombre genérico del producto o servicio",
    ),
    "Nota media atribuida ao fornecedor nesta ordem de compra": (
        "Average rating given to the supplier on this purchase order",
        "Calificación promedio otorgada al proveedor en esta orden de compra",
    ),
    "Numero correlativo da linha de item dentro da licitacao": (
        "Sequence number of the item line within the tender",
        "Número correlativo de la línea de ítem dentro de la licitación",
    ),
    "Numero do ato administrativo de aprovacao": (
        "Number of the approving administrative act",
        "Número del acto administrativo de aprobación",
    ),
    "Observacoes sobre o contrato": (
        "Notes on the contract",
        "Observaciones sobre el contrato",
    ),
    "Pais da unidade compradora": (
        "Country of the purchasing unit",
        "País de la unidad compradora",
    ),
    "Pais de origem do produto ou servico": (
        "Country of origin of the product or service",
        "País de origen del producto o servicio",
    ),
    "Pais do fornecedor": ("Country of the supplier", "País del proveedor"),
    "Prazo da licitacao": ("Term of the tender", "Plazo de la licitación"),
    "Preco unitario liquido do item": (
        "Net unit price of the item",
        "Precio unitario neto del ítem",
    ),
    "Procedimento de compra que originou a ordem": (
        "Procurement procedure the order came from",
        "Procedimiento de compra que originó la orden",
    ),
    "Quantidade adjudicada ao fornecedor na linha": (
        "Quantity awarded to the supplier on the line",
        "Cantidad adjudicada al proveedor en la línea",
    ),
    "Quantidade contratada do item": (
        "Quantity of the item contracted",
        "Cantidad contratada del ítem",
    ),
    "Quantidade de avaliacoes registradas para a ordem de compra": (
        "Number of ratings recorded for the purchase order",
        "Cantidad de evaluaciones registradas para la orden de compra",
    ),
    "Quantidade de etapas da licitacao": (
        "Number of stages in the tender",
        "Cantidad de etapas de la licitación",
    ),
    "Quantidade de proponentes na licitacao": (
        "Number of bidders on the tender",
        "Cantidad de oferentes en la licitación",
    ),
    "Quantidade de reclamacoes registradas na licitacao": (
        "Number of complaints recorded on the tender",
        "Cantidad de reclamos registrados en la licitación",
    ),
    "Quantidade ofertada na proposta": (
        "Quantity offered in the bid",
        "Cantidad ofertada en la oferta",
    ),
    "Quantidade solicitada do item": (
        "Quantity of the item requested",
        "Cantidad solicitada del ítem",
    ),
    "RUT da unidade compradora": (
        "RUT of the purchasing unit",
        "RUT de la unidad compradora",
    ),
    "RUT do fornecedor": ("RUT of the supplier", "RUT del proveedor"),
    "RUT do fornecedor proponente": (
        "RUT of the bidding supplier",
        "RUT del proveedor oferente",
    ),
    "Razao social do fornecedor proponente": (
        "Legal name of the bidding supplier",
        "Razón social del proveedor oferente",
    ),
    "Regiao da unidade compradora": (
        "Region of the purchasing unit",
        "Región de la unidad compradora",
    ),
    "Regiao do fornecedor": ("Region of the supplier", "Región del proveedor"),
    "Rubro UNSPSC de primeiro nivel": (
        "First-level UNSPSC rubro",
        "Rubro UNSPSC de primer nivel",
    ),
    "Rubro UNSPSC de segundo nivel": (
        "Second-level UNSPSC rubro",
        "Rubro UNSPSC de segundo nivel",
    ),
    "Rubro UNSPSC de terceiro nivel": (
        "Third-level UNSPSC rubro",
        "Rubro UNSPSC de tercer nivel",
    ),
    "Segunda descricao dos criterios ou requisitos sociais": (
        "Second description of the social criteria or requirements",
        "Segunda descripción de los criterios o requisitos sociales",
    ),
    "Setor do Estado a que pertence o organismo comprador": (
        "State sector the purchasing agency belongs to",
        "Sector del Estado al que pertenece el organismo comprador",
    ),
    "Setor do Estado a que pertence o organismo licitante": (
        "State sector the tendering agency belongs to",
        "Sector del Estado al que pertenece el organismo licitante",
    ),
    "Sigla abreviada do tipo de ordem de compra": (
        "Short abbreviation of the purchase order type",
        "Sigla abreviada del tipo de orden de compra",
    ),
    "Sigla do tipo de licitacao": (
        "Abbreviation of the tender type",
        "Sigla del tipo de licitación",
    ),
    "Sigla do tipo de ordem de compra": (
        "Abbreviation of the purchase order type",
        "Sigla del tipo de orden de compra",
    ),
    "Tipo de aquisicao da licitacao": (
        "Acquisition type of the tender",
        "Tipo de adquisición de la licitación",
    ),
    "Tipo de convocatoria da licitacao": (
        "Call type of the tender",
        "Tipo de convocatoria de la licitación",
    ),
    "Tipo de duracao do contrato": (
        "Type of contract duration",
        "Tipo de duración del contrato",
    ),
    "Tipo de estimativa do valor da licitacao": (
        "Type of estimate for the tender amount",
        "Tipo de estimación del monto de la licitación",
    ),
    "Tipo de imposto aplicado a ordem de compra": (
        "Type of tax applied to the purchase order",
        "Tipo de impuesto aplicado a la orden de compra",
    ),
    "Tipo de pagamento previsto": (
        "Expected payment type",
        "Tipo de pago previsto",
    ),
    "Tipo do ato administrativo de aprovacao": (
        "Type of the approving administrative act",
        "Tipo del acto administrativo de aprobación",
    ),
    "Unidade de medida da quantidade ofertada": (
        "Unit of measure of the quantity offered",
        "Unidad de medida de la cantidad ofertada",
    ),
    "Unidade de medida do item": (
        "Unit of measure of the item",
        "Unidad de medida del ítem",
    ),
    "Unidade de tempo da duracao do contrato": (
        "Time unit of the contract duration",
        "Unidad de tiempo de la duración del contrato",
    ),
    "Unidade de tempo do contrato da licitacao": (
        "Time unit of the tender's contract",
        "Unidad de tiempo del contrato de la licitación",
    ),
    "Unidade de tempo do periodo de avaliacao": (
        "Time unit of the evaluation period",
        "Unidad de tiempo del período de evaluación",
    ),
    "Unidade de tempo do prazo da licitacao": (
        "Time unit of the tender term",
        "Unidad de tiempo del plazo de la licitación",
    ),
    "Unidade do periodo de renovacao do contrato": (
        "Unit of the contract renewal period",
        "Unidad del período de renovación del contrato",
    ),
    "Valor adjudicado na linha de item": (
        "Amount awarded on the item line",
        "Monto adjudicado en la línea de ítem",
    ),
    "Valor do periodo de renovacao do contrato": (
        "Value of the contract renewal period",
        "Valor del período de renovación del contrato",
    ),
    "Valor dos descontos da linha de item": (
        "Amount of discounts on the item line",
        "Monto de los descuentos de la línea de ítem",
    ),
    "Valor dos descontos da ordem de compra": (
        "Amount of discounts on the purchase order",
        "Monto de los descuentos de la orden de compra",
    ),
    "Valor dos encargos da linha de item": (
        "Amount of charges on the item line",
        "Monto de los cargos de la línea de ítem",
    ),
    "Valor dos encargos da ordem de compra": (
        "Amount of charges on the purchase order",
        "Monto de los cargos de la orden de compra",
    ),
    "Valor dos impostos da linha de item": (
        "Amount of taxes on the item line",
        "Monto de los impuestos de la línea de ítem",
    ),
    "Valor dos impostos da ordem de compra": (
        "Amount of taxes on the purchase order",
        "Monto de los impuestos de la orden de compra",
    ),
    "Valor estimado adjudicado da licitacao": (
        "Estimated awarded amount of the tender",
        "Monto estimado adjudicado de la licitación",
    ),
    "Valor estimado da licitacao": (
        "Estimated amount of the tender",
        "Monto estimado de la licitación",
    ),
    "Valor total da ordem de compra convertido para pesos chilenos": (
        "Total amount of the purchase order converted to Chilean pesos",
        "Monto total de la orden de compra convertido a pesos chilenos",
    ),
    "Valor total da ordem de compra na moeda de emissao": (
        "Total amount of the purchase order in the currency it was issued in",
        "Monto total de la orden de compra en la moneda de emisión",
    ),
    "Valor total liquido da linha de item": (
        "Total net amount of the item line",
        "Monto total neto de la línea de ítem",
    ),
    "Valor total liquido da ordem de compra": (
        "Total net amount of the purchase order",
        "Monto total neto de la orden de compra",
    ),
    "Valor total ofertado na linha": (
        "Total amount offered on the line",
        "Monto total ofertado en la línea",
    ),
    "Valor unitario ofertado": (
        "Unit amount offered",
        "Monto unitario ofertado",
    ),
}


# Portuguese -> (English, Spanish), for the column `observations` field.
OBSERVATIONS: dict[str, tuple[str, str]] = {
    "A fonte deixa os codigos 48 e 49 sem rotulo em forma_pago, em 3.452.648 linhas, portanto o dicionario nao cobre a coluna inteira; as demais 44 chaves estao no dicionario": (
        "The source leaves codes 48 and 49 unlabelled in forma_pago, across 3,452,648 rows, so the dictionary does not cover the whole column; the other 44 keys are in the dictionary",
        "La fuente deja los códigos 48 y 49 sin rótulo en forma_pago, en 3.452.648 filas, por lo que el diccionario no cubre la columna entera; las otras 44 llaves están en el diccionario",
    ),
    "A fonte deixa os codigos 6 e 0 sem rotulo em estado_proveedor, em 14 linhas espalhadas por varios anos, portanto o dicionario nao cobre a coluna inteira; os 5 codigos restantes estao no dicionario": (
        "The source leaves codes 6 and 0 unlabelled in estado_proveedor, across 14 rows spread over several years, so the dictionary does not cover the whole column; the remaining 5 codes are in the dictionary",
        "La fuente deja los códigos 6 y 0 sin rótulo en estado_proveedor, en 14 filas repartidas en varios años, por lo que el diccionario no cubre la columna entera; los 5 códigos restantes están en el diccionario",
    ),
    "A fonte exporta duas colunas com o mesmo nome; a segunda chega como DescripcionCriteriosRequisitosSociales.1": (
        "The source exports two columns with the same name; the second arrives as DescripcionCriteriosRequisitosSociales.1",
        "La fuente exporta dos columnas con el mismo nombre; la segunda llega como DescripcionCriteriosRequisitosSociales.1",
    ),
    "A fonte lista os mesmos criterios em ordens diferentes; os itens sao ordenados alfabeticamente na limpeza": (
        "The source lists the same criteria in different orders; the items are sorted alphabetically during cleaning",
        "La fuente lista los mismos criterios en órdenes distintos; los ítems se ordenan alfabéticamente en la limpieza",
    ),
    "A fonte publica esta coluna apenas como codigo, sem divulgar a tabela de significados correspondente, portanto nao ha entrada no dicionario": (
        "The source publishes this column only as a code, without releasing the corresponding table of meanings, so there is no dictionary entry",
        "La fuente publica esta columna solo como código, sin divulgar la tabla de significados correspondiente, por lo que no hay entrada en el diccionario",
    ),
    "A unidade e dada pela coluna moneda": (
        "The unit is given by the moneda column",
        "La unidad está dada por la columna moneda",
    ),
    "A unidade e dada pela coluna moneda; constante por licitacao": (
        "The unit is given by the moneda column; constant per tender",
        "La unidad está dada por la columna moneda; constante por licitación",
    ),
    "A unidade e dada pela coluna moneda; use monto_total_clp para comparacoes": (
        "The unit is given by the moneda column; use monto_total_clp for comparisons",
        "La unidad está dada por la columna moneda; use monto_total_clp para comparaciones",
    ),
    "A unidade e dada pela coluna moneda_item": (
        "The unit is given by the moneda_item column",
        "La unidad está dada por la columna moneda_item",
    ),
    "A unidade e dada pela coluna moneda_oferta em licitacion_item": (
        "The unit is given by the moneda_oferta column in licitacion_item",
        "La unidad está dada por la columna moneda_oferta en licitacion_item",
    ),
    "A unidade e dada pela coluna unidad_medida": (
        "The unit is given by the unidad_medida column",
        "La unidad está dada por la columna unidad_medida",
    ),
    "A unidade e dada pela coluna unidad_medida em licitacion_item": (
        "The unit is given by the unidad_medida column in licitacion_item",
        "La unidad está dada por la columna unidad_medida en licitacion_item",
    ),
    "A unidade e dada pela coluna unidad_medida_oferta": (
        "The unit is given by the unidad_medida_oferta column",
        "La unidad está dada por la columna unidad_medida_oferta",
    ),
    "Ausente nos arquivos de 2007 a 2014": (
        "Absent from the 2007 to 2014 files",
        "Ausente en los archivos de 2007 a 2014",
    ),
    "Cabecalho grafado com acento ate 2014": (
        "The header is spelled with an accent up to 2014",
        "El encabezado se escribe con acento hasta 2014",
    ),
    "Cabecalho grafado com acento ate 2014 e sem acento a partir de 2015": (
        "The header is spelled with an accent up to 2014 and without one from 2015",
        "El encabezado se escribe con acento hasta 2014 y sin acento desde 2015",
    ),
    "Cabecalho perde o acento de generico a partir de 2015": (
        "The header loses the accent in generico from 2015",
        "El encabezado pierde el acento de genérico desde 2015",
    ),
    "Chave de ligacao com as tabelas licitacion_item e licitacion_oferta": (
        "Join key to the licitacion_item and licitacion_oferta tables",
        "Llave de enlace con las tablas licitacion_item y licitacion_oferta",
    ),
    "Chave logica; liga a orden_compra_item.codigo_licitacion": (
        "Logical key; joins to orden_compra_item.codigo_licitacion",
        "Llave lógica; enlaza con orden_compra_item.codigo_licitacion",
    ),
    "Classificacao ONU de produtos e servicos, nivel de produto": (
        "UN product and service classification, product level",
        "Clasificación ONU de productos y servicios, nivel de producto",
    ),
    "Coluna de particao, derivada de fecha_envio": (
        "Partition column, derived from fecha_envio",
        "Columna de partición, derivada de fecha_envio",
    ),
    "Coluna de particao, derivada de fecha_publicacion": (
        "Partition column, derived from fecha_publicacion",
        "Columna de partición, derivada de fecha_publicacion",
    ),
    "Coluna presente no cabecalho de 2007 a 2020, exceto 2017, e ausente a partir de 2021. So e efetivamente preenchida a partir de 2019: nula em 100% das linhas em 2007-01 e 2010-06, em 10 de 505.159 linhas em 2015-06, e preenchida em cerca de 41% das linhas em 2019-06 e 2020-06": (
        "Present in the header from 2007 to 2020 except 2017, and absent from 2021. It is only actually populated from 2019: null in 100% of rows in 2007-01 and 2010-06, in all but 10 of 505,159 rows in 2015-06, and populated in about 41% of rows in 2019-06 and 2020-06",
        "Presente en el encabezado de 2007 a 2020, excepto 2017, y ausente desde 2021. Solo se llena efectivamente desde 2019: nula en el 100% de las filas en 2007-01 y 2010-06, en todas salvo 10 de 505.159 filas en 2015-06, y poblada en cerca del 41% de las filas en 2019-06 y 2020-06",
    ),
    "Compoe a chave logica": (
        "Part of the logical key",
        "Compone la llave lógica",
    ),
    "Compoe a chave logica junto de codigo_licitacion": (
        "Part of the logical key together with codigo_licitacion",
        "Compone la llave lógica junto con codigo_licitacion",
    ),
    "Compoe a chave logica junto de codigo_orden_compra": (
        "Part of the logical key together with codigo_orden_compra",
        "Compone la llave lógica junto con codigo_orden_compra",
    ),
    "Compoe a chave logica junto de id_item": (
        "Part of the logical key together with id_item",
        "Compone la llave lógica junto con id_item",
    ),
    "Constante por licitacao, apesar do nome sugerir nivel de proposta": (
        "Constant per tender, despite the name suggesting bid level",
        "Constante por licitación, pese a que el nombre sugiere nivel de oferta",
    ),
    "Conversao feita pela propria fonte": (
        "Conversion performed by the source itself",
        "Conversión realizada por la propia fuente",
    ),
    "Define as particoes ano e mes": (
        "Defines the ano and mes partitions",
        "Define las particiones ano y mes",
    ),
    "Derivado do nome publicado pela fonte via a tabela de correspondencia code/geografia_crosswalk.csv. A geografia da unidade compradora vem controlada pelo sistema e resolve para um CUT em 100% das linhas nao nulas": (
        "Derived from the name the source publishes, via the crosswalk in code/geografia_crosswalk.csv. Purchasing-unit geography comes controlled by the system and resolves to a CUT for 100% of non-null rows",
        "Derivado del nombre que publica la fuente, mediante la tabla de correspondencia code/geografia_crosswalk.csv. La geografía de la unidad compradora viene controlada por el sistema y resuelve a un CUT en el 100% de las filas no nulas",
    ),
    "Derivado do nome publicado pela fonte via a tabela de correspondencia code/geografia_crosswalk.csv. A geografia do fornecedor e autodeclarada em texto livre, ao contrario da geografia da unidade compradora, que vem controlada pelo sistema. Cerca de 97% das linhas resolvem para um CUT; o restante traz variantes irredutiveis como 'CUARTA REGION', 'DECIMA LOS LAGOS', 'Santiago, Maipu' ou 'Extranjero'. Use a coluna de nome correspondente quando precisar do valor exatamente como publicado": (
        "Derived from the name the source publishes, via the crosswalk in code/geografia_crosswalk.csv. Supplier geography is self-reported free text, unlike purchasing-unit geography, which comes controlled by the system. About 97% of rows resolve to a CUT; the rest carry irreducible variants such as 'CUARTA REGION', 'DECIMA LOS LAGOS', 'Santiago, Maipu' or 'Extranjero'. Use the matching name column when you need the value exactly as published",
        "Derivado del nombre que publica la fuente, mediante la tabla de correspondencia code/geografia_crosswalk.csv. La geografía del proveedor es autodeclarada en texto libre, a diferencia de la de la unidad compradora, que viene controlada por el sistema. Cerca del 97% de las filas resuelve a un CUT; el resto trae variantes irreducibles como 'CUARTA REGION', 'DECIMA LOS LAGOS', 'Santiago, Maipu' o 'Extranjero'. Use la columna de nombre correspondiente cuando necesite el valor exactamente como fue publicado",
    ),
    "Espacos finais removidos na limpeza": (
        "Trailing spaces removed during cleaning",
        "Espacios finales eliminados en la limpieza",
    ),
    "Ex.: LR, LE, LQ, LP": ("E.g. LR, LE, LQ, LP", "Ej.: LR, LE, LQ, LP"),
    "Formato com pontos e digito verificador": (
        "Formatted with dots and a check digit",
        "Formato con puntos y dígito verificador",
    ),
    "Mesmo caso de moeda: valores invalidos impedem a cobertura completa pelo dicionario": (
        "Same case as moneda: invalid values prevent full coverage by the dictionary",
        "Mismo caso que moneda: valores inválidos impiden la cobertura completa por el diccionario",
    ),
    "Modalidade criada em 2020": (
        "Modality created in 2020",
        "Modalidad creada en 2020",
    ),
    "Multiplos valores separados por barra vertical": (
        "Multiple values separated by a vertical bar",
        "Múltiples valores separados por barra vertical",
    ),
    "Nome de origem contem erro de digitacao (Nombreroducto)": (
        "The source name contains a typo (Nombreroducto)",
        "El nombre de origen contiene un error de tipeo (Nombreroducto)",
    ),
    "Nome de origem contem erro de digitacao (Obervaciones)": (
        "The source name contains a typo (Obervaciones)",
        "El nombre de origen contiene un error de tipeo (Obervaciones)",
    ),
    "Predominantemente CLP; CLF e UTM sao unidades indexadas a inflacao. Alem dos codigos de moeda validos a coluna traz 331 linhas com os valores '1' e '-1', que nao correspondem a moeda alguma, portanto o dicionario nao cobre a coluna inteira": (
        "Predominantly CLP; CLF and UTM are inflation-indexed units. Besides the valid currency codes the column carries 331 rows holding '1' and '-1', which correspond to no currency, so the dictionary does not cover the whole column",
        "Predominantemente CLP; CLF y UTM son unidades indexadas a la inflación. Además de los códigos de moneda válidos la columna trae 331 filas con los valores '1' y '-1', que no corresponden a moneda alguna, por lo que el diccionario no cubre la columna entera",
    ),
    "Presente apenas nos arquivos a partir de 2020": (
        "Present only in the files from 2020 onward",
        "Presente solo en los archivos desde 2020 en adelante",
    ),
    "Presente apenas nos arquivos a partir de 2021": (
        "Present only in the files from 2021 onward",
        "Presente solo en los archivos desde 2021 en adelante",
    ),
    "Presente apenas nos arquivos de 2007 a 2014": (
        "Present only in the 2007 to 2014 files",
        "Presente solo en los archivos de 2007 a 2014",
    ),
    "Presente apenas nos arquivos de 2007 a 2014; chega como UnidadMedida.1": (
        "Present only in the 2007 to 2014 files; arrives as UnidadMedida.1",
        "Presente solo en los archivos de 2007 a 2014; llega como UnidadMedida.1",
    ),
    "Presente apenas nos arquivos de 2007 a 2019": (
        "Present only in the 2007 to 2019 files",
        "Presente solo en los archivos de 2007 a 2019",
    ),
    "Unidade dada pela coluna unidad_tiempo": (
        "Unit given by the unidad_tiempo column",
        "Unidad dada por la columna unidad_tiempo",
    ),
    "Unidade dada pela coluna unidad_tiempo_duracion_contrato": (
        "Unit given by the unidad_tiempo_duracion_contrato column",
        "Unidad dada por la columna unidad_tiempo_duracion_contrato",
    ),
    "Valor 1900-01-01 na origem indica ausencia e e convertido para nulo": (
        "The value 1900-01-01 at source means absent and is converted to null",
        "El valor 1900-01-01 en el origen indica ausencia y se convierte a nulo",
    ),
}
