"""Trilingual descriptions for the MG-only published columns.

WHY THIS IS A FILE AND NOT PROSE IN `schema.yml`
------------------------------------------------
The 43 MG-only tables publish 752 column slots over 334 distinct names, all of
them derived from SICOM's own `dsc_`/`vlr_`/`dat_`/`num_`/`cod_`/`seq_` prefixes.
TCE-MG publishes no column dictionary for these streams, so every description
here is written from the column name plus the stream it sits in -- it is NOT a
transcription of an official layout document.

Keeping the mapping in one auditable file, rather than scattering 752 generated
strings across `schema.yml` and the backend, makes that provenance checkable: a
reader can see the whole vocabulary at once and correct it in one place. It is
also why this is compositional -- roughly 60 whole-column terms and 200 noun
phrases generate all 752 descriptions, so a reviewer reads 260 strings, not 752.

Portuguese is the source of truth; English and Spanish are translations of it.
`dbt`'s `schema.yml` carries only Portuguese (house convention); the backend
needs all three, so `build_description` takes a language.
"""

from __future__ import annotations

PT, EN, ES = 0, 1, 2
LANGS = ("pt", "en", "es")

# Whole-column descriptions, keyed on the published column name, as (pt, en, es).
# Anything not here is assembled by the prefix rules below.
TERMS: dict[str, tuple[str, str, str]] = {
    # -- partition / geography ------------------------------------------------
    "ano": (
        "Número do ano de referência da informação",
        "Reference year of the record",
        "Número del año de referencia de la información",
    ),
    "mes": (
        "Número do mês de referência da informação",
        "Reference month of the record",
        "Número del mes de referencia de la información",
    ),
    "sigla_uf": (
        "Sigla da Unidade da Federação",
        "Abbreviation of the federative unit",
        "Sigla de la Unidad Federativa",
    ),
    "id_municipio": (
        "ID Município - IBGE 7 Dígitos",
        "Municipality ID - IBGE 7 digits",
        "ID Municipio - IBGE 7 dígitos",
    ),
    # -- administrative spine -------------------------------------------------
    "orgao": (
        "Código do órgão atribuído pelo município (cod_orgao na fonte)",
        "Body code assigned by the municipality (cod_orgao in the source)",
        "Código del órgano asignado por el municipio (cod_orgao en la fuente)",
    ),
    "id_unidade_gestora": (
        "Código da unidade gestora",
        "Managing unit code",
        "Código de la unidad gestora",
    ),
    "codigo_unidade": (
        "Código da unidade orçamentária",
        "Budget unit code",
        "Código de la unidad presupuestaria",
    ),
    "codigo_subunidade": (
        "Código da subunidade orçamentária",
        "Budget subunit code",
        "Código de la subunidad presupuestaria",
    ),
    "numero_versao_arq": (
        "Número da versão do arquivo remetido ao SICOM",
        "Version number of the file submitted to SICOM",
        "Número de versión del archivo remitido al SICOM",
    ),
    # -- orçamento ------------------------------------------------------------
    "acao": (
        "Descrição da ação orçamentária",
        "Description of the budget action",
        "Descripción de la acción presupuestaria",
    ),
    "subacao": (
        "Descrição da subação orçamentária",
        "Description of the budget sub-action",
        "Descripción de la subacción presupuestaria",
    ),
    "funcao": (
        "Descrição da função orçamentária",
        "Description of the budget function",
        "Descripción de la función presupuestaria",
    ),
    "subfuncao": (
        "Descrição da subfunção orçamentária",
        "Description of the budget subfunction",
        "Descripción de la subfunción presupuestaria",
    ),
    "programa": (
        "Descrição do programa orçamentário",
        "Description of the budget programme",
        "Descripción del programa presupuestario",
    ),
    "dotacao": (
        "Descrição da dotação orçamentária",
        "Description of the budget appropriation",
        "Descripción de la partida presupuestaria",
    ),
    "dotacao_ori": (
        "Descrição da dotação orçamentária de origem",
        "Description of the originating budget appropriation",
        "Descripción de la partida presupuestaria de origen",
    ),
    "natureza_despesa": (
        "Descrição da natureza da despesa",
        "Description of the expenditure category",
        "Descripción de la naturaleza del gasto",
    ),
    "cod_orcamentario": (
        "Código orçamentário da despesa",
        "Budget code of the expenditure",
        "Código presupuestario del gasto",
    ),
    "fonte_recurso": (
        "Descrição da fonte de recursos",
        "Description of the funding source",
        "Descripción de la fuente de recursos",
    ),
    # -- objeto / processo ----------------------------------------------------
    "objeto": (
        "Descrição do objeto",
        "Description of the object",
        "Descripción del objeto",
    ),
    "objetocontrato": (
        "Descrição do objeto do contrato",
        "Description of the contract object",
        "Descripción del objeto del contrato",
    ),
    "objeto_adesao": (
        "Descrição do objeto da adesão à ata de registro de preços",
        "Description of the object of the price-registry adhesion",
        "Descripción del objeto de la adhesión al acta de registro de precios",
    ),
    "natureza_objeto": (
        "Descrição da natureza do objeto",
        "Description of the nature of the object",
        "Descripción de la naturaleza del objeto",
    ),
    "nat_processo": (
        "Descrição da natureza do processo",
        "Description of the nature of the process",
        "Descripción de la naturaleza del proceso",
    ),
    "tipo_processo": (
        "Descrição do tipo de processo",
        "Description of the process type",
        "Descripción del tipo de proceso",
    ),
    "tipo_cadastro": (
        "Descrição do tipo de cadastro",
        "Description of the record type",
        "Descripción del tipo de registro",
    ),
    "modalidade": (
        "Descrição da modalidade da licitação",
        "Description of the procurement modality",
        "Descripción de la modalidad de la licitación",
    ),
    "justificativa": (
        "Justificativa da movimentação de restos a pagar",
        "Justification for the carried-over commitment movement",
        "Justificación del movimiento de residuos pasivos",
    ),
    "motivo": (
        "Descrição do motivo",
        "Description of the reason",
        "Descripción del motivo",
    ),
    "razao": (
        "Razão registrada pelo órgão",
        "Reason recorded by the body",
        "Razón registrada por el órgano",
    ),
    "finalidade_conta": (
        "Descrição da finalidade da conta bancária",
        "Description of the purpose of the bank account",
        "Descripción de la finalidad de la cuenta bancaria",
    ),
    "alteracao": (
        "Descrição da alteração",
        "Description of the amendment",
        "Descripción de la modificación",
    ),
    "documento": (
        "Descrição do documento",
        "Description of the document",
        "Descripción del documento",
    ),
    "tabela": (
        "Identificação da tabela de referência de preços",
        "Identifier of the reference price table",
        "Identificación de la tabla de referencia de precios",
    ),
    "item": (
        "Descrição do item",
        "Description of the item",
        "Descripción del ítem",
    ),
    "lote": (
        "Identificação do lote",
        "Lot identifier",
        "Identificación del lote",
    ),
    "unid_medida": (
        "Descrição da unidade de medida do item",
        "Description of the item's unit of measure",
        "Descripción de la unidad de medida del ítem",
    ),
    "unid_med_prazo": (
        "Descrição da unidade de medida do prazo",
        "Description of the unit of measure of the term",
        "Descripción de la unidad de medida del plazo",
    ),
    "prazoexecucao": (
        "Prazo de execução previsto",
        "Planned execution term",
        "Plazo de ejecución previsto",
    ),
    "fornecimento": (
        "Descrição da forma de fornecimento",
        "Description of the supply arrangement",
        "Descripción de la forma de suministro",
    ),
    "formapagamento": (
        "Descrição da forma de pagamento",
        "Description of the payment arrangement",
        "Descripción de la forma de pago",
    ),
    "garantia": (
        "Descrição da garantia contratual",
        "Description of the contractual guarantee",
        "Descripción de la garantía contractual",
    ),
    "multarecisoria": (
        "Descrição da multa rescisória",
        "Description of the termination penalty",
        "Descripción de la multa por rescisión",
    ),
    "multa_inadimplencia": (
        "Descrição da multa por inadimplência",
        "Description of the default penalty",
        "Descripción de la multa por incumplimiento",
    ),
    "veiculo_publicacao": (
        "Descrição do veículo de publicação",
        "Description of the publication outlet",
        "Descripción del medio de publicación",
    ),
    "tipo_instrumento": (
        "Descrição do tipo de instrumento contratual",
        "Description of the type of contractual instrument",
        "Descripción del tipo de instrumento contractual",
    ),
    "decorrentelicitacao": (
        "Indica se o contrato decorre de licitação",
        "Indicates whether the contract arises from a procurement process",
        "Indica si el contrato deriva de una licitación",
    ),
    "org_gerenciador": (
        "Órgão gerenciador da ata de registro de preços",
        "Body managing the price registry",
        "Órgano gestor del acta de registro de precios",
    ),
    "inst_financeira": (
        "Instituição financeira",
        "Financial institution",
        "Institución financiera",
    ),
    "agencia": (
        "Número da agência bancária",
        "Bank branch number",
        "Número de la sucursal bancaria",
    ),
    "conta_bancaria": (
        "Número da conta bancária",
        "Bank account number",
        "Número de la cuenta bancaria",
    ),
    "cargo": (
        "Descrição do cargo",
        "Description of the role",
        "Descripción del cargo",
    ),
    "ato_nomeacao": (
        "Descrição do ato de nomeação",
        "Description of the appointment act",
        "Descripción del acto de nombramiento",
    ),
    "mun_emitente": (
        "Município do emitente do documento fiscal",
        "Municipality of the tax document issuer",
        "Municipio del emisor del documento fiscal",
    ),
    "sigla_uf_credenciado": (
        "Sigla da Unidade da Federação de registro do credenciado",
        "Federative unit where the accredited supplier is registered",
        "Sigla de la Unidad Federativa de registro del acreditado",
    ),
    "sigla_uf_fornecedor": (
        "Sigla da Unidade da Federação de registro do fornecedor",
        "Federative unit where the supplier is registered",
        "Sigla de la Unidad Federativa de registro del proveedor",
    ),
    # -- indicadores ----------------------------------------------------------
    "ind_processo_lote": (
        "Indica se o processo é julgado por lote",
        "Indicates whether the process is judged by lot",
        "Indica si el proceso se juzga por lote",
    ),
    "ind_desonera_folha": (
        "Indica desoneração da folha de pagamento",
        "Indicates payroll tax relief",
        "Indica exoneración de la nómina",
    ),
    "ind_desc_tab_preco": (
        "Indica desconto sobre tabela de preços",
        "Indicates a discount on the price table",
        "Indica descuento sobre la tabla de precios",
    ),
}

# `<prefix>` -> one template per language, taking the expanded remainder.
PREFIXES: list[tuple[str, tuple[str, str, str]]] = [
    (
        "id_",
        (
            "Sequencial identificador {}",
            "Sequential identifier {}",
            "Secuencial identificador {}",
        ),
    ),
    ("codigo_", ("Código {}", "Code {}", "Código {}")),
    ("numero_", ("Número {}", "Number {}", "Número {}")),
    ("nome_", ("Nome {}", "Name {}", "Nombre {}")),
    ("nom_", ("Nome {}", "Name {}", "Nombre {}")),
    ("data_", ("Data {}", "Date {}", "Fecha {}")),
    ("valor_", ("Valor {}", "Value {}", "Valor {}")),
    (
        "tipo_",
        (
            "Descrição do tipo {}",
            "Description of the type {}",
            "Descripción del tipo {}",
        ),
    ),
    ("ind_", ("Indicador {}", "Indicator {}", "Indicador {}")),
    (
        "nat_",
        (
            "Descrição da natureza {}",
            "Description of the nature {}",
            "Descripción de la naturaleza {}",
        ),
    ),
]

# Expansions for the abbreviated remainders SICOM uses, as (pt, en, es).
PARTS: dict[str, tuple[str, str, str]] = {
    "abertura": ("de abertura", "of opening", "de apertura"),
    "material_servico": (
        "de material ou serviço",
        "of material or service",
        "de material o servicio",
    ),
    "restos_pagar_processado": (
        "de restos a pagar processados",
        "of processed carried-over commitments",
        "de residuos pasivos procesados",
    ),
    "restos_pagar_nao_processado": (
        "de restos a pagar não processados",
        "of unprocessed carried-over commitments",
        "de residuos pasivos no procesados",
    ),
    "percentual_desconto": (
        "percentual de desconto",
        "discount percentage",
        "porcentaje de descuento",
    ),
    "percentual_taxa_administracao": (
        "percentual da taxa de administração",
        "administration fee percentage",
        "porcentaje de la tasa de administración",
    ),
    "credor": ("do credor", "of the creditor", "del acreedor"),
    "alteracao_orcamentaria": (
        "da alteração orçamentária",
        "of the budget amendment",
        "de la modificación presupuestaria",
    ),
    "contrato_apostilamento": (
        "do apostilamento do contrato",
        "of the contract endorsement",
        "del apostillado del contrato",
    ),
    "contrato_contabilizacao": (
        "da contabilização do contrato",
        "of the contract accounting entry",
        "de la contabilización del contrato",
    ),
    "contrato_credito": (
        "do crédito orçamentário do contrato",
        "of the contract's budget credit",
        "del crédito presupuestario del contrato",
    ),
    "contrato_item": (
        "do item do contrato",
        "of the contract item",
        "del ítem del contrato",
    ),
    "contrato_rescisao": (
        "da rescisão do contrato",
        "of the contract termination",
        "de la rescisión del contrato",
    ),
    "contrato_termo_aditivo": (
        "do termo aditivo do contrato",
        "of the contract amendment",
        "del anexo del contrato",
    ),
    "contrato_termo_aditivo_item": (
        "do item do termo aditivo do contrato",
        "of the contract amendment item",
        "del ítem del anexo del contrato",
    ),
    "dispensa_cotacao": (
        "da cotação da dispensa",
        "of the waiver quotation",
        "de la cotización de la dispensa",
    ),
    "dispensa_credenciado": (
        "do credenciado da dispensa",
        "of the waiver's accredited supplier",
        "del acreditado de la dispensa",
    ),
    "dispensa_dotacao": (
        "da dotação orçamentária da dispensa",
        "of the waiver's budget appropriation",
        "de la partida presupuestaria de la dispensa",
    ),
    "dispensa_fornecedor": (
        "do fornecedor da dispensa",
        "of the waiver supplier",
        "del proveedor de la dispensa",
    ),
    "dispensa_item": (
        "do item da dispensa",
        "of the waiver item",
        "del ítem de la dispensa",
    ),
    "dispensa_responsavel": (
        "do responsável pela dispensa",
        "of the officer responsible for the waiver",
        "del responsable de la dispensa",
    ),
    "lei_decreto": (
        "da lei que autoriza o decreto",
        "of the law authorising the decree",
        "de la ley que autoriza el decreto",
    ),
    "licitacao_comissao": (
        "do membro da comissão de licitação",
        "of the procurement committee member",
        "del miembro de la comisión de licitación",
    ),
    "licitacao_cotacao": (
        "da cotação do item da licitação",
        "of the procurement item quotation",
        "de la cotización del ítem de la licitación",
    ),
    "licitacao_dotacao": (
        "da dotação orçamentária da licitação",
        "of the procurement's budget appropriation",
        "de la partida presupuestaria de la licitación",
    ),
    "licitacao_homologacao": (
        "da homologação do item da licitação",
        "of the procurement item award",
        "de la homologación del ítem de la licitación",
    ),
    "licitacao_item": (
        "do item da licitação",
        "of the procurement item",
        "del ítem de la licitación",
    ),
    "licitacao_julgamento": (
        "do julgamento do item da licitação",
        "of the procurement item adjudication",
        "del juzgamiento del ítem de la licitación",
    ),
    "licitacao_parecer": (
        "do parecer da licitação",
        "of the procurement opinion",
        "del dictamen de la licitación",
    ),
    "licitacao_participante": (
        "do participante da licitação",
        "of the procurement bidder",
        "del participante de la licitación",
    ),
    "licitacao_quadro_societario": (
        "do sócio do participante da licitação",
        "of the bidder's shareholder",
        "del socio del participante de la licitación",
    ),
    "licitacao_responsavel": (
        "do responsável pela licitação",
        "of the officer responsible for the procurement",
        "del responsable de la licitación",
    ),
    "liquidacao_fonte": (
        "da fonte de recursos da liquidação",
        "of the verification's funding source",
        "de la fuente de recursos de la liquidación",
    ),
    "liquidacao_nota_fiscal": (
        "da nota fiscal da liquidação",
        "of the verification's invoice",
        "de la factura de la liquidación",
    ),
    "nota_fiscal_item": (
        "do item da nota fiscal",
        "of the invoice item",
        "del ítem de la factura",
    ),
    "registro_preco_adesao_cotacao": (
        "da cotação da adesão a registro de preços",
        "of the price-registry adhesion quotation",
        "de la cotización de la adhesión al registro de precios",
    ),
    "registro_preco_adesao_item": (
        "do item da adesão a registro de preços",
        "of the price-registry adhesion item",
        "del ítem de la adhesión al registro de precios",
    ),
    "registro_preco_adesao_vencedor": (
        "do vencedor da adesão a registro de preços",
        "of the price-registry adhesion winner",
        "del ganador de la adhesión al registro de precios",
    ),
    "restos_pagar_credor": (
        "do credor dos restos a pagar",
        "of the carried-over commitment creditor",
        "del acreedor de los residuos pasivos",
    ),
    "restos_pagar_movimentacao": (
        "da movimentação de restos a pagar",
        "of the carried-over commitment movement",
        "del movimiento de residuos pasivos",
    ),
    "restos_pagar_movimentacao_credor": (
        "do credor da movimentação de restos a pagar",
        "of the creditor of the carried-over commitment movement",
        "del acreedor del movimiento de residuos pasivos",
    ),
    "restos_pagar_movimentacao_fonte": (
        "da fonte da movimentação de restos a pagar",
        "of the funding source of the carried-over commitment movement",
        "de la fuente del movimiento de residuos pasivos",
    ),
    "acres_red": (
        "de acréscimo ou redução",
        "of increase or reduction",
        "de incremento o reducción",
    ),
    "acrescimo": ("do acréscimo", "of the increase", "del incremento"),
    "alien_bens": (
        "de alienação de bens",
        "of asset disposal",
        "de enajenación de bienes",
    ),
    "alt": ("de alteração", "of amendment", "de modificación"),
    "alteracao": ("da alteração", "of the amendment", "de la modificación"),
    "anu_liq_fonte": (
        "de anulação da liquidação por fonte",
        "of verification cancellation by funding source",
        "de anulación de la liquidación por fuente",
    ),
    "anu_liquidacao": (
        "de anulação da liquidação",
        "of verification cancellation",
        "de anulación de la liquidación",
    ),
    "anu_outras_baixas": (
        "de anulação de outras baixas",
        "of cancellation of other write-offs",
        "de anulación de otras bajas",
    ),
    "anu_pagamento": (
        "de anulação do pagamento",
        "of payment cancellation",
        "de anulación del pago",
    ),
    "anulacao_emp": (
        "de anulação do empenho",
        "of commitment cancellation",
        "de anulación del compromiso",
    ),
    "ano_contrato": (
        "do ano do contrato",
        "of the contract year",
        "del año del contrato",
    ),
    "ano_emp_origem": (
        "do ano do empenho de origem",
        "of the originating commitment's year",
        "del año del compromiso de origen",
    ),
    "ano_processo": (
        "do ano do processo",
        "of the process year",
        "del año del proceso",
    ),
    "ano_referencia": (
        "do ano de referência",
        "of the reference year",
        "del año de referencia",
    ),
    "aplicacao": ("da aplicação", "of the investment", "de la aplicación"),
    "apost": ("de apostilamento", "of endorsement", "de apostillado"),
    "apostilamento": (
        "do apostilamento",
        "of the endorsement",
        "del apostillado",
    ),
    "ass_decreto": (
        "de assinatura do decreto",
        "of decree signature",
        "de firma del decreto",
    ),
    "ass_termo": (
        "de assinatura do termo",
        "of amendment signature",
        "de firma del anexo",
    ),
    "assinatura": ("de assinatura", "of signature", "de firma"),
    "ata_reg_preco": (
        "da ata de registro de preços",
        "of the price registry",
        "del acta de registro de precios",
    ),
    "ato_nomeacao": (
        "do ato de nomeação",
        "of the appointment act",
        "del acto de nombramiento",
    ),
    "atrib": ("de atribuição", "of assignment", "de atribución"),
    "aut_impressao": (
        "da autorização de impressão",
        "of the printing authorisation",
        "de la autorización de impresión",
    ),
    "aberto": ("em aberto", "outstanding", "pendiente"),
    "aberto_lei": ("aberto por lei", "opened by law", "abierto por ley"),
    "bruto": ("bruto", "gross", "bruto"),
    "cep_mun_emitente": (
        "do CEP do município do emitente",
        "of the postcode of the issuer's municipality",
        "del código postal del municipio del emisor",
    ),
    "cert_fgts": (
        "da certidão do FGTS",
        "of the FGTS certificate",
        "del certificado del FGTS",
    ),
    "cert_negativa": (
        "da certidão negativa",
        "of the clearance certificate",
        "del certificado de no deuda",
    ),
    "certidao_inss": (
        "da certidão do INSS",
        "of the INSS certificate",
        "del certificado del INSS",
    ),
    "cgc_habilitado": (
        "do CNPJ do habilitado",
        "of the qualified bidder's CNPJ",
        "del CNPJ del habilitado",
    ),
    "chave_mun_nota_fis": (
        "da chave municipal da nota fiscal",
        "of the municipal invoice key",
        "de la clave municipal de la factura",
    ),
    "chave_nota_fis": (
        "da chave da nota fiscal",
        "of the invoice key",
        "de la clave de la factura",
    ),
    "cndt": (
        "da Certidão Negativa de Débitos Trabalhistas",
        "of the labour debt clearance certificate",
        "del certificado de no deuda laboral",
    ),
    "com_licitacao": (
        "da comissão de licitação",
        "of the procurement committee",
        "de la comisión de licitación",
    ),
    "comissao": ("de comissão", "of the committee", "de comisión"),
    "contrato": ("do contrato", "of the contract", "del contrato"),
    "contratado": ("do contratado", "of the contractor", "del contratado"),
    "cot_dispensa": (
        "da cotação da dispensa",
        "of the waiver quotation",
        "de la cotización de la dispensa",
    ),
    "cot_licitacao": (
        "da cotação da licitação",
        "of the procurement quotation",
        "de la cotización de la licitación",
    ),
    "cot_preco_unit": (
        "do preço unitário cotado",
        "of the quoted unit price",
        "del precio unitario cotizado",
    ),
    "cot_reg_adesao": (
        "da cotação da adesão a registro de preços",
        "of the price-registry adhesion quotation",
        "de la cotización de la adhesión al registro de precios",
    ),
    "cotacao": ("da cotação", "of the quotation", "de la cotización"),
    "cred_dispensa": (
        "do credenciado da dispensa",
        "of the waiver's accredited supplier",
        "del acreditado de la dispensa",
    ),
    "cred_mov_rsp": (
        "do credor da movimentação de restos a pagar",
        "of the creditor of the carried-over commitment movement",
        "del acreedor del movimiento de residuos pasivos",
    ),
    "credenciado": (
        "do credenciado",
        "of the accredited supplier",
        "del acreditado",
    ),
    "credenciamento": (
        "de credenciamento",
        "of accreditation",
        "de acreditación",
    ),
    "credito_contrato": (
        "do crédito do contrato",
        "of the contract credit",
        "del crédito del contrato",
    ),
    "credor_rsp": (
        "do credor de restos a pagar",
        "of the carried-over commitment creditor",
        "del acreedor de residuos pasivos",
    ),
    "decreto": ("do decreto", "of the decree", "del decreto"),
    "decreto_alt": (
        "do decreto de alteração",
        "of the amending decree",
        "del decreto de modificación",
    ),
    "deducao": ("de dedução", "of deduction", "de deducción"),
    "desconto": ("de desconto", "of discount", "de descuento"),
    "despesa_dotacao": (
        "da dotação de despesa",
        "of the expenditure appropriation",
        "de la partida de gasto",
    ),
    "dispensa": ("da dispensa", "of the waiver", "de la dispensa"),
    "doc": ("do documento", "of the document", "del documento"),
    "doc_credenciado": (
        "do documento do credenciado",
        "of the accredited supplier's document",
        "del documento del acreditado",
    ),
    "doc_credor": (
        "do documento do credor",
        "of the creditor's document",
        "del documento del acreedor",
    ),
    "doc_emitente": (
        "do documento do emitente",
        "of the issuer's document",
        "del documento del emisor",
    ),
    "doc_fornecedor": (
        "do documento do fornecedor",
        "of the supplier's document",
        "del documento del proveedor",
    ),
    "doc_licitante": (
        "do documento do licitante",
        "of the bidder's document",
        "del documento del licitante",
    ),
    "doc_livre": (
        "do documento livre",
        "of the free-form document",
        "del documento libre",
    ),
    "doc_representante": (
        "do documento do representante",
        "of the representative's document",
        "del documento del representante",
    ),
    "doc_resp": (
        "do documento do responsável",
        "of the responsible officer's document",
        "del documento del responsable",
    ),
    "doc_resp_parecer": (
        "do documento do responsável pelo parecer",
        "of the document of the officer responsible for the opinion",
        "del documento del responsable del dictamen",
    ),
    "doc_responsavel": (
        "do documento do responsável",
        "of the responsible officer's document",
        "del documento del responsable",
    ),
    "doc_signatario": (
        "do documento do signatário",
        "of the signatory's document",
        "del documento del firmante",
    ),
    "doc_vencedor": (
        "do documento do vencedor",
        "of the winner's document",
        "del documento del ganador",
    ),
    "documento": ("do documento", "of the document", "del documento"),
    "emi_cert_fgts": (
        "de emissão da certidão do FGTS",
        "of issue of the FGTS certificate",
        "de emisión del certificado del FGTS",
    ),
    "emi_cert_inss": (
        "de emissão da certidão do INSS",
        "of issue of the INSS certificate",
        "de emisión del certificado del INSS",
    ),
    "emi_cert_negativa": (
        "de emissão da certidão negativa",
        "of issue of the clearance certificate",
        "de emisión del certificado de no deuda",
    ),
    "emi_cndt": (
        "de emissão da CNDT",
        "of issue of the labour debt clearance certificate",
        "de emisión del certificado de no deuda laboral",
    ),
    "emissao": ("de emissão", "of issue", "de emisión"),
    "emitente": ("do emitente", "of the issuer", "del emisor"),
    "emp_credor": (
        "do credor do empenho",
        "of the commitment creditor",
        "del acreedor del compromiso",
    ),
    "empenhado": ("empenhado", "committed", "comprometido"),
    "empenho": ("do empenho", "of the commitment", "del compromiso"),
    "empenho_credor": (
        "do credor do empenho",
        "of the commitment creditor",
        "del acreedor del compromiso",
    ),
    "empenho_fonte": (
        "da fonte do empenho",
        "of the commitment's funding source",
        "de la fuente del compromiso",
    ),
    "empenho_origem": (
        "do empenho de origem",
        "of the originating commitment",
        "del compromiso de origen",
    ),
    "est_credor": (
        "do estado do credor",
        "of the creditor's state",
        "del estado del acreedor",
    ),
    "fim_vigencia": (
        "de fim da vigência",
        "of end of validity",
        "de fin de vigencia",
    ),
    "fimvigencia": (
        "de fim da vigência",
        "of end of validity",
        "de fin de vigencia",
    ),
    "fornecedor": ("do fornecedor", "of the supplier", "del proveedor"),
    "forn_dispensa": (
        "do fornecedor da dispensa",
        "of the waiver supplier",
        "del proveedor de la dispensa",
    ),
    "global": ("global", "overall", "global"),
    "hab_licitacao": (
        "do habilitado da licitação",
        "of the qualified bidder",
        "del habilitado de la licitación",
    ),
    "hom_licitacao": (
        "da homologação da licitação",
        "of the procurement award",
        "de la homologación de la licitación",
    ),
    "ini_vigencia": (
        "de início da vigência",
        "of start of validity",
        "de inicio de vigencia",
    ),
    "iniciovigencia": (
        "de início da vigência",
        "of start of validity",
        "de inicio de vigencia",
    ),
    "inscr_est_emitente": (
        "da inscrição estadual do emitente",
        "of the issuer's state registration",
        "de la inscripción estatal del emisor",
    ),
    "inscr_estadual": (
        "da inscrição estadual",
        "of the state registration",
        "de la inscripción estatal",
    ),
    "inscr_mun_emitente": (
        "da inscrição municipal do emitente",
        "of the issuer's municipal registration",
        "de la inscripción municipal del emisor",
    ),
    "item": ("do item", "of the item", "del ítem"),
    "item_contrato": (
        "do item do contrato",
        "of the contract item",
        "del ítem del contrato",
    ),
    "item_dispensa": (
        "do item da dispensa",
        "of the waiver item",
        "del ítem de la dispensa",
    ),
    "item_licitacao": (
        "do item da licitação",
        "of the procurement item",
        "del ítem de la licitación",
    ),
    "item_nota": (
        "do item da nota fiscal",
        "of the invoice item",
        "del ítem de la factura",
    ),
    "item_planilha": (
        "do item na planilha",
        "of the item on the schedule",
        "del ítem en la planilla",
    ),
    "item_reg_adesao": (
        "do item da adesão a registro de preços",
        "of the price-registry adhesion item",
        "del ítem de la adhesión al registro de precios",
    ),
    "item_sicro": (
        "do item na tabela SICRO",
        "of the item in the SICRO table",
        "del ítem en la tabla SICRO",
    ),
    "item_sinapi": (
        "do item na tabela SINAPI",
        "of the item in the SINAPI table",
        "del ítem en la tabla SINAPI",
    ),
    "item_termo": (
        "do item do termo aditivo",
        "of the amendment item",
        "del ítem del anexo",
    ),
    "julgamento": ("do julgamento", "of the adjudication", "del juzgamiento"),
    "lei": ("da lei", "of the law", "de la ley"),
    "lei_alt": (
        "da lei de alteração",
        "of the amending law",
        "de la ley de modificación",
    ),
    "lei_alteracao": (
        "da lei de alteração",
        "of the amending law",
        "de la ley de modificación",
    ),
    "licitacao": (
        "da licitação",
        "of the procurement process",
        "de la licitación",
    ),
    "licitante": ("do licitante", "of the bidder", "del licitante"),
    "liq_fonte": (
        "da liquidação por fonte",
        "of the verification by funding source",
        "de la liquidación por fuente",
    ),
    "liq_nota_fiscal": (
        "da nota fiscal da liquidação",
        "of the verification's invoice",
        "de la factura de la liquidación",
    ),
    "liquidacao": (
        "da liquidação",
        "of the verification",
        "de la liquidación",
    ),
    "liquidado": ("liquidado", "verified", "liquidado"),
    "liquido": ("líquido", "net", "neto"),
    "lote": ("do lote", "of the lot", "del lote"),
    "mat_serv": (
        "de material ou serviço",
        "of material or service",
        "de material o servicio",
    ),
    "material_serv": (
        "de material ou serviço",
        "of material or service",
        "de material o servicio",
    ),
    "mes_referencia": (
        "do mês de referência",
        "of the reference month",
        "del mes de referencia",
    ),
    "min_alien_bens": (
        "mínimo de alienação de bens",
        "minimum for asset disposal",
        "mínimo de enajenación de bienes",
    ),
    "modalidade": ("da modalidade", "of the modality", "de la modalidad"),
    "mov_fonte": (
        "da movimentação por fonte",
        "of the movement by funding source",
        "del movimiento por fuente",
    ),
    "mov_pagamento": (
        "da movimentação do pagamento",
        "of the payment movement",
        "del movimiento del pago",
    ),
    "mov_rsp": (
        "da movimentação de restos a pagar",
        "of the carried-over commitment movement",
        "del movimiento de residuos pasivos",
    ),
    "mov_rsp_fonte": (
        "da fonte da movimentação de restos a pagar",
        "of the funding source of the carried-over commitment movement",
        "de la fuente del movimiento de residuos pasivos",
    ),
    "movimentacao": ("da movimentação", "of the movement", "del movimiento"),
    "nao_processado": ("não processado", "unprocessed", "no procesado"),
    "nat_cargo": (
        "da natureza do cargo",
        "of the nature of the role",
        "de la naturaleza del cargo",
    ),
    "nota": ("da nota fiscal", "of the invoice", "de la factura"),
    "nota_fiscal": ("da nota fiscal", "of the invoice", "de la factura"),
    "nomeacao": ("de nomeação", "of appointment", "de nombramiento"),
    "novo_termino": (
        "do novo término",
        "of the new end date",
        "del nuevo término",
    ),
    "origem": ("de origem", "of origin", "de origen"),
    "original": ("original", "original", "original"),
    "outras_baixas": (
        "de outras baixas",
        "of other write-offs",
        "de otras bajas",
    ),
    "pagamento": ("do pagamento", "of the payment", "del pago"),
    "pagamento_movimento": (
        "da movimentação do pagamento",
        "of the payment movement",
        "del movimiento del pago",
    ),
    "pago": ("pago", "paid", "pagado"),
    "parecer": ("do parecer", "of the opinion", "del dictamen"),
    "parecer_licit": (
        "do parecer da licitação",
        "of the procurement opinion",
        "del dictamen de la licitación",
    ),
    "participacao": (
        "de participação",
        "of participation",
        "de participación",
    ),
    "pct_desconto": (
        "percentual de desconto",
        "discount percentage",
        "porcentaje de descuento",
    ),
    "pct_tax_adm": (
        "percentual da taxa de administração",
        "administration fee percentage",
        "porcentaje de la tasa de administración",
    ),
    "perc_desconto": (
        "percentual de desconto",
        "discount percentage",
        "porcentaje de descuento",
    ),
    "perc_taxa_adm": (
        "percentual da taxa de administração",
        "administration fee percentage",
        "porcentaje de la tasa de administración",
    ),
    "percentual": ("percentual", "percentage", "porcentual"),
    "pessoa": ("da pessoa", "of the person", "de la persona"),
    "pessoa_resp": (
        "da pessoa responsável",
        "of the responsible person",
        "de la persona responsable",
    ),
    "preco_unit": ("unitário", "unit", "unitario"),
    "preco_unitario": ("unitário", "unit", "unitario"),
    "previsto": ("previsto", "planned", "previsto"),
    "processado": ("processado", "processed", "procesado"),
    "processo": ("do processo", "of the process", "del proceso"),
    "pub_aviso_inst": (
        "de publicação do aviso do instrumento",
        "of publication of the instrument notice",
        "de publicación del aviso del instrumento",
    ),
    "pub_lei": (
        "de publicação da lei",
        "of publication of the law",
        "de publicación de la ley",
    ),
    "pub_lei_alt": (
        "de publicação da lei de alteração",
        "of publication of the amending law",
        "de publicación de la ley de modificación",
    ),
    "pub_termo": (
        "de publicação do termo",
        "of publication of the amendment",
        "de publicación del anexo",
    ),
    "publicacao": ("de publicação", "of publication", "de publicación"),
    "quadro_soc": (
        "do quadro societário",
        "of the shareholding structure",
        "del cuadro societario",
    ),
    "quant_acres_decres": (
        "da quantidade acrescida ou reduzida",
        "of the quantity added or removed",
        "de la cantidad incrementada o reducida",
    ),
    "quant_aderido": (
        "da quantidade aderida",
        "of the adhered quantity",
        "de la cantidad adherida",
    ),
    "quant_item": (
        "da quantidade do item",
        "of the item quantity",
        "de la cantidad del ítem",
    ),
    "quant_item_cotado": (
        "da quantidade do item cotado",
        "of the quoted item quantity",
        "de la cantidad del ítem cotizado",
    ),
    "quant_licitado": (
        "da quantidade licitada",
        "of the tendered quantity",
        "de la cantidad licitada",
    ),
    "rec_dispensa": (
        "do recurso orçamentário da dispensa",
        "of the waiver's budget resource",
        "del recurso presupuestario de la dispensa",
    ),
    "rec_licitacao": (
        "do recurso orçamentário da licitação",
        "of the procurement's budget resource",
        "del recurso presupuestario de la licitación",
    ),
    "recurso": (
        "do recurso orçamentário",
        "of the budget resource",
        "del recurso presupuestario",
    ),
    "reforco_emp": (
        "de reforço do empenho",
        "of commitment reinforcement",
        "de refuerzo del compromiso",
    ),
    "reg_adesao": (
        "da adesão a registro de preços",
        "of the price-registry adhesion",
        "de la adhesión al registro de precios",
    ),
    "registro_preco_adesao": (
        "da adesão a registro de preços",
        "of the price-registry adhesion",
        "de la adhesión al registro de precios",
    ),
    "representante": (
        "do representante",
        "of the representative",
        "del representante",
    ),
    "rescicao": ("de rescisão", "of termination", "de rescisión"),
    "rescisao": ("da rescisão", "of the termination", "de la rescisión"),
    "rescisao_contrato": (
        "da rescisão do contrato",
        "of the contract termination",
        "de la rescisión del contrato",
    ),
    "resp": (
        "do responsável",
        "of the responsible officer",
        "del responsable",
    ),
    "resp_dispensa": (
        "do responsável pela dispensa",
        "of the officer responsible for the waiver",
        "del responsable de la dispensa",
    ),
    "resp_licitacao": (
        "do responsável pela licitação",
        "of the officer responsible for the procurement",
        "del responsable de la licitación",
    ),
    "resp_parecer": (
        "do responsável pelo parecer",
        "of the officer responsible for the opinion",
        "del responsable del dictamen",
    ),
    "responsavel": (
        "do responsável",
        "of the responsible officer",
        "del responsable",
    ),
    "restos_pagar": (
        "de restos a pagar",
        "of carried-over commitments",
        "de residuos pasivos",
    ),
    "rsp": (
        "de restos a pagar",
        "of carried-over commitments",
        "de residuos pasivos",
    ),
    "rsp_nao_proc": (
        "de restos a pagar não processados",
        "of unprocessed carried-over commitments",
        "de residuos pasivos no procesados",
    ),
    "rsp_nao_processado": (
        "de restos a pagar não processados",
        "of unprocessed carried-over commitments",
        "de residuos pasivos no procesados",
    ),
    "rsp_proc": (
        "de restos a pagar processados",
        "of processed carried-over commitments",
        "de residuos pasivos procesados",
    ),
    "rsp_processado": (
        "de restos a pagar processados",
        "of processed carried-over commitments",
        "de residuos pasivos procesados",
    ),
    "rspnprocessado": (
        "de restos a pagar não processados",
        "of unprocessed carried-over commitments",
        "de residuos pasivos no procesados",
    ),
    "rspprocessado": (
        "de restos a pagar processados",
        "of processed carried-over commitments",
        "de residuos pasivos procesados",
    ),
    "serie_nota_fiscal": (
        "da série da nota fiscal",
        "of the invoice series",
        "de la serie de la factura",
    ),
    "signatario": ("do signatário", "of the signatory", "del firmante"),
    "termo": ("do termo aditivo", "of the amendment", "del anexo"),
    "termo_aditivo": ("do termo aditivo", "of the amendment", "del anexo"),
    "unidade_gestora": (
        "da unidade gestora",
        "of the managing unit",
        "de la unidad gestora",
    ),
    "unitario": ("unitário", "unit", "unitario"),
    "val_cert_fgts": (
        "de validade da certidão do FGTS",
        "of validity of the FGTS certificate",
        "de validez del certificado del FGTS",
    ),
    "val_cert_inss": (
        "de validade da certidão do INSS",
        "of validity of the INSS certificate",
        "de validez del certificado del INSS",
    ),
    "val_cert_negativa": (
        "de validade da certidão negativa",
        "of validity of the clearance certificate",
        "de validez del certificado de no deuda",
    ),
    "val_cndt": (
        "de validade da CNDT",
        "of validity of the labour debt clearance certificate",
        "de validez del certificado de no deuda laboral",
    ),
    "validade": ("de validade", "of validity", "de validez"),
    "vencedor": ("do vencedor", "of the winner", "del ganador"),
    "vencimento": ("de vencimento", "of maturity", "de vencimiento"),
    "venc_reg_adesao": (
        "do vencedor da adesão a registro de preços",
        "of the price-registry adhesion winner",
        "del ganador de la adhesión al registro de precios",
    ),
}

_BD_KEY: tuple[str, str, str] = (
    "Código único de identificação {}, construído por Data Basis",
    "Unique identifier {}, built by Data Basis",
    "Código único de identificación {}, construido por Data Basis",
)

_MISSES: set[str] = set()


def build_description(column: str, lang: str = "pt") -> str:
    """Return the description for one published column, in `lang`."""
    idx = LANGS.index(lang)
    if column in TERMS:
        return TERMS[column][idx]
    if column.endswith("_bd"):
        entity = column[len("id_") : -len("_bd")]
        part = PARTS.get(entity)
        if part is None:
            _MISSES.add(column)
            return _BD_KEY[idx].format(entity.replace("_", " "))
        return _BD_KEY[idx].format(part[idx])
    for prefix, template in PREFIXES:
        if column.startswith(prefix):
            rest = column[len(prefix) :]
            part = PARTS.get(rest)
            if part is None:
                _MISSES.add(column)
                return template[idx].format(rest.replace("_", " "))
            return template[idx].format(part[idx])
    _MISSES.add(column)
    return column.replace("_", " ").capitalize()


def unresolved() -> list[str]:
    """Columns that fell through to the mechanical fallback."""
    return sorted(_MISSES)
