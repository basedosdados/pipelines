"""Generate the br_pncp architecture CSVs.

The column specs live here rather than in the CSVs so that the trilingual
descriptions, types and directory links stay in one reviewable place. Run this
to regenerate `<table>.csv`; the CSVs are the artefact the rest of the
onboarding reads (cleaning code, dbt models, metadata upload).

Types follow the arithmetic-meaning rule: INT64/FLOAT64 only where summing or
averaging is meaningful and a unit can be named. PNCP's coded fields
(modalidade, situação, esfera, poder, categoria) are STRING and covered by the
dicionario table; its genuine JSON booleans stay BOOLEAN so NULL survives.
"""

from __future__ import annotations

import csv
from pathlib import Path

HERE = Path(__file__).parent

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

DIR_ANO = "br_bd_diretorios_data_tempo.ano:ano"
# The UF directory calls its key `sigla`, not `sigla_uf` — pointing the FK at
# `sigla_uf` compiles but fails at run time with "Unrecognized name: sigla_uf".
DIR_UF = "br_bd_diretorios_brasil.uf:sigla"
DIR_MUN = "br_bd_diretorios_brasil.municipio:id_municipio"


def col(
    name,
    typ,
    pt,
    en,
    es,
    *,
    dic="no",
    directory="",
    unit="",
    sensitive="no",
    obs="",
    original="",
    coverage="",
):
    return {
        "name": name,
        "bigquery_type": typ,
        "description": pt,
        "temporal_coverage": coverage,
        "covered_by_dictionary": dic,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": sensitive,
        "observations": obs,
        "original_name": original,
        "description_en": en,
        "description_es": es,
    }


def concat(*groups: list) -> list:
    """Flatten reusable column blocks into one ordered list."""
    return [column for group in groups for column in group]


# --------------------------------------------------------------------------
# Blocks reused across tables. PNCP nests órgão/unidade identically in the
# contratação, contrato and ata payloads, so the flattened columns match.
# --------------------------------------------------------------------------


def ano_col(original, coverage="2021(1)2026"):
    return col(
        "ano",
        "INT64",
        "Ano de publicação do registro no PNCP",
        "Year the record was published on PNCP",
        "Año de publicación del registro en el PNCP",
        directory=DIR_ANO,
        unit="year",
        obs="Coluna de partição, derivada da data de publicação no PNCP",
        original=original,
        coverage=coverage,
    )


GEO_COLS = [
    col(
        "sigla_uf",
        "STRING",
        "Sigla da unidade da federação da unidade administrativa responsável",
        "Abbreviation of the federative unit of the responsible administrative unit",
        "Sigla de la unidad federativa de la unidad administrativa responsable",
        directory=DIR_UF,
        original="unidadeOrgao.ufSigla",
    ),
    col(
        "id_municipio",
        "STRING",
        "Código IBGE de sete dígitos do município da unidade administrativa responsável",
        "Seven-digit IBGE code of the municipality of the responsible administrative unit",
        "Código IBGE de siete dígitos del municipio de la unidad administrativa responsable",
        directory=DIR_MUN,
        original="unidadeOrgao.codigoIbge",
    ),
]


def orgao_cols(
    prefix_original="orgaoEntidade", unidade_original="unidadeOrgao"
):
    return [
        col(
            "cnpj_orgao",
            "STRING",
            "CNPJ do órgão ou entidade contratante",
            "Taxpayer registry number (CNPJ) of the contracting agency or entity",
            "Número de registro fiscal (CNPJ) del órgano o entidad contratante",
            original=f"{prefix_original}.cnpj",
        ),
        col(
            "nome_orgao",
            "STRING",
            "Razão social do órgão ou entidade contratante",
            "Legal name of the contracting agency or entity",
            "Razón social del órgano o entidad contratante",
            original=f"{prefix_original}.razaoSocial",
        ),
        col(
            "id_esfera",
            "STRING",
            "Código da esfera de governo do órgão contratante",
            "Code of the government sphere of the contracting agency",
            "Código de la esfera de gobierno del órgano contratante",
            dic="yes",
            original=f"{prefix_original}.esferaId",
            obs="F federal, E estadual, M municipal, D distrital, N não se aplica",
        ),
        col(
            "id_poder",
            "STRING",
            "Código do poder ao qual o órgão contratante pertence",
            "Code of the branch of government the contracting agency belongs to",
            "Código del poder al que pertenece el órgano contratante",
            dic="yes",
            original=f"{prefix_original}.poderId",
        ),
        col(
            "codigo_unidade",
            "STRING",
            "Código da unidade administrativa responsável dentro do órgão",
            "Code of the responsible administrative unit within the agency",
            "Código de la unidad administrativa responsable dentro del órgano",
            original=f"{unidade_original}.codigoUnidade",
        ),
        col(
            "nome_unidade",
            "STRING",
            "Nome da unidade administrativa responsável dentro do órgão",
            "Name of the responsible administrative unit within the agency",
            "Nombre de la unidad administrativa responsable dentro del órgano",
            original=f"{unidade_original}.nomeUnidade",
        ),
    ]


SUBROGADO_COLS = [
    col(
        "cnpj_orgao_subrogado",
        "STRING",
        "CNPJ do órgão sub-rogado, quando a competência foi transferida",
        "CNPJ of the subrogated agency, when responsibility was transferred",
        "CNPJ del órgano subrogado, cuando la competencia fue transferida",
        original="orgaoSubRogado.cnpj",
    ),
    col(
        "nome_orgao_subrogado",
        "STRING",
        "Razão social do órgão sub-rogado",
        "Legal name of the subrogated agency",
        "Razón social del órgano subrogado",
        original="orgaoSubRogado.razaoSocial",
    ),
    col(
        "codigo_unidade_subrogada",
        "STRING",
        "Código da unidade administrativa sub-rogada",
        "Code of the subrogated administrative unit",
        "Código de la unidad administrativa subrogada",
        original="unidadeSubRogada.codigoUnidade",
    ),
    col(
        "nome_unidade_subrogada",
        "STRING",
        "Nome da unidade administrativa sub-rogada",
        "Name of the subrogated administrative unit",
        "Nombre de la unidad administrativa subrogada",
        original="unidadeSubRogada.nomeUnidade",
    ),
]

ATUALIZACAO_COLS = [
    col(
        "data_publicacao",
        "DATE",
        "Data de publicação do registro no PNCP",
        "Date the record was published on PNCP",
        "Fecha de publicación del registro en el PNCP",
        original="dataPublicacaoPncp",
    ),
    col(
        "data_atualizacao",
        "DATE",
        "Data da última atualização global do registro no PNCP",
        "Date of the last global update of the record on PNCP",
        "Fecha de la última actualización global del registro en el PNCP",
        original="dataAtualizacaoGlobal",
        obs="Usada para desduplicar múltiplas versões do mesmo registro, mantendo a mais recente",
    ),
]


# --------------------------------------------------------------------------
# contratacao
# --------------------------------------------------------------------------

CONTRATACAO = concat(
    [ano_col("dataPublicacaoPncp")],
    GEO_COLS,
    [
        col(
            "id_contratacao_pncp",
            "STRING",
            "Número de controle da contratação no PNCP, identificador único do registro",
            "PNCP control number of the tender, the record's unique identifier",
            "Número de control de la contratación en el PNCP, identificador único del registro",
            original="numeroControlePNCP",
        ),
        col(
            "ano_compra",
            "INT64",
            "Ano da compra atribuído pelo órgão contratante",
            "Purchase year assigned by the contracting agency",
            "Año de la compra asignado por el órgano contratante",
            unit="year",
            original="anoCompra",
        ),
        col(
            "sequencial_compra",
            "STRING",
            "Número sequencial da compra dentro do órgão e ano",
            "Sequential number of the purchase within the agency and year",
            "Número secuencial de la compra dentro del órgano y año",
            original="sequencialCompra",
        ),
        col(
            "numero_compra",
            "STRING",
            "Número da compra atribuído pelo órgão contratante",
            "Purchase number assigned by the contracting agency",
            "Número de la compra asignado por el órgano contratante",
            original="numeroCompra",
        ),
        col(
            "numero_processo",
            "STRING",
            "Número do processo administrativo da contratação",
            "Administrative process number of the tender",
            "Número del proceso administrativo de la contratación",
            original="processo",
        ),
    ],
    orgao_cols(),
    SUBROGADO_COLS,
    [
        col(
            "id_modalidade",
            "STRING",
            "Código da modalidade de contratação",
            "Code of the tender procedure type",
            "Código de la modalidad de contratación",
            dic="yes",
            original="modalidadeId",
        ),
        col(
            "modalidade",
            "STRING",
            "Nome da modalidade de contratação",
            "Name of the tender procedure type",
            "Nombre de la modalidad de contratación",
            original="modalidadeNome",
        ),
        col(
            "id_modo_disputa",
            "STRING",
            "Código do modo de disputa da contratação",
            "Code of the bidding dispute mode",
            "Código del modo de disputa de la contratación",
            dic="yes",
            original="modoDisputaId",
        ),
        col(
            "modo_disputa",
            "STRING",
            "Nome do modo de disputa da contratação",
            "Name of the bidding dispute mode",
            "Nombre del modo de disputa de la contratación",
            original="modoDisputaNome",
        ),
        col(
            "id_situacao_compra",
            "STRING",
            "Código da situação da contratação",
            "Code of the tender status",
            "Código de la situación de la contratación",
            dic="yes",
            original="situacaoCompraId",
        ),
        col(
            "situacao_compra",
            "STRING",
            "Nome da situação da contratação",
            "Name of the tender status",
            "Nombre de la situación de la contratación",
            original="situacaoCompraNome",
        ),
        col(
            "id_tipo_instrumento_convocatorio",
            "STRING",
            "Código do tipo de instrumento convocatório",
            "Code of the type of calling instrument",
            "Código del tipo de instrumento convocatorio",
            dic="yes",
            original="tipoInstrumentoConvocatorioCodigo",
        ),
        col(
            "tipo_instrumento_convocatorio",
            "STRING",
            "Nome do tipo de instrumento convocatório",
            "Name of the type of calling instrument",
            "Nombre del tipo de instrumento convocatorio",
            original="tipoInstrumentoConvocatorioNome",
        ),
        col(
            "codigo_amparo_legal",
            "STRING",
            "Código do amparo legal invocado para a contratação",
            "Code of the legal basis invoked for the tender",
            "Código del amparo legal invocado para la contratación",
            dic="yes",
            original="amparoLegal.codigo",
        ),
        col(
            "nome_amparo_legal",
            "STRING",
            "Nome do dispositivo legal que ampara a contratação",
            "Name of the legal provision supporting the tender",
            "Nombre del dispositivo legal que ampara la contratación",
            original="amparoLegal.nome",
        ),
        col(
            "objeto_compra",
            "STRING",
            "Descrição do objeto da contratação",
            "Description of the object of the tender",
            "Descripción del objeto de la contratación",
            original="objetoCompra",
        ),
        col(
            "informacao_complementar",
            "STRING",
            "Informação complementar registrada pelo órgão contratante",
            "Additional information recorded by the contracting agency",
            "Información complementaria registrada por el órgano contratante",
            original="informacaoComplementar",
        ),
        col(
            "justificativa_presencial",
            "STRING",
            "Justificativa apresentada para a realização da contratação em formato presencial",
            "Justification given for holding the tender in person",
            "Justificación presentada para realizar la contratación de forma presencial",
            original="justificativaPresencial",
        ),
        col(
            "indicador_srp",
            "BOOLEAN",
            "Indica se a contratação usa sistema de registro de preços",
            "Whether the tender uses the price registration system",
            "Indica si la contratación utiliza el sistema de registro de precios",
            original="srp",
        ),
        col(
            "indicador_emenda_parlamentar",
            "BOOLEAN",
            "Indica se a contratação envolve recursos de emenda parlamentar",
            "Whether the tender involves congressional amendment funds",
            "Indica si la contratación involucra recursos de enmienda parlamentaria",
            original="emendaParlamentar",
        ),
        col(
            "data_abertura_proposta",
            "DATE",
            "Data de abertura do recebimento de propostas",
            "Date proposals began to be received",
            "Fecha de apertura de la recepción de propuestas",
            original="dataAberturaProposta",
        ),
        col(
            "data_encerramento_proposta",
            "DATE",
            "Data de encerramento do recebimento de propostas",
            "Date proposals stopped being received",
            "Fecha de cierre de la recepción de propuestas",
            original="dataEncerramentoProposta",
        ),
    ],
    ATUALIZACAO_COLS,
    [
        col(
            "valor_total_estimado",
            "FLOAT64",
            "Valor total estimado da contratação",
            "Total estimated value of the tender",
            "Valor total estimado de la contratación",
            unit="BRL",
            original="valorTotalEstimado",
        ),
        col(
            "valor_total_homologado",
            "FLOAT64",
            "Valor total homologado da contratação",
            "Total awarded value of the tender",
            "Valor total homologado de la contratación",
            unit="BRL",
            original="valorTotalHomologado",
            obs="Nulo enquanto a contratação não foi homologada",
        ),
        col(
            "link_sistema_origem",
            "STRING",
            "Endereço do registro no sistema de origem do órgão contratante",
            "Address of the record in the contracting agency's source system",
            "Dirección del registro en el sistema de origen del órgano contratante",
            original="linkSistemaOrigem",
        ),
    ],
)


# --------------------------------------------------------------------------
# contrato (includes empenhos, discriminated by id_tipo_contrato)
# --------------------------------------------------------------------------

CONTRATO = concat(
    [ano_col("dataPublicacaoPncp")],
    GEO_COLS,
    [
        col(
            "id_contrato_pncp",
            "STRING",
            "Número de controle do contrato no PNCP, identificador único do registro",
            "PNCP control number of the contract, the record's unique identifier",
            "Número de control del contrato en el PNCP, identificador único del registro",
            original="numeroControlePNCP",
        ),
        col(
            "id_contratacao_pncp",
            "STRING",
            "Número de controle no PNCP da contratação que originou o contrato",
            "PNCP control number of the tender that originated the contract",
            "Número de control en el PNCP de la contratación que originó el contrato",
            original="numeroControlePncpCompra",
            obs="Chave de ligação com a tabela contratacao",
        ),
        col(
            "id_ata_pncp",
            "STRING",
            "Número de controle no PNCP da ata de registro de preços vinculada",
            "PNCP control number of the linked price registration record",
            "Número de control en el PNCP del acta de registro de precios vinculada",
            original="numeroControlePncpAta",
            obs="Chave de ligação com a tabela ata_registro_preco; nulo quando o contrato não deriva de ata",
        ),
        col(
            "ano_contrato",
            "INT64",
            "Ano do contrato atribuído pelo órgão contratante",
            "Contract year assigned by the contracting agency",
            "Año del contrato asignado por el órgano contratante",
            unit="year",
            original="anoContrato",
        ),
        col(
            "sequencial_contrato",
            "STRING",
            "Número sequencial do contrato dentro do órgão e ano",
            "Sequential number of the contract within the agency and year",
            "Número secuencial del contrato dentro del órgano y año",
            original="sequencialContrato",
        ),
        col(
            "numero_contrato_empenho",
            "STRING",
            "Número do contrato ou do empenho atribuído pelo órgão contratante",
            "Contract or commitment number assigned by the contracting agency",
            "Número del contrato o del compromiso asignado por el órgano contratante",
            original="numeroContratoEmpenho",
        ),
        col(
            "numero_processo",
            "STRING",
            "Número do processo administrativo do contrato",
            "Administrative process number of the contract",
            "Número del proceso administrativo del contrato",
            original="processo",
        ),
    ],
    orgao_cols(),
    SUBROGADO_COLS,
    [
        col(
            "id_tipo_contrato",
            "STRING",
            "Código do tipo de contrato",
            "Code of the contract type",
            "Código del tipo de contrato",
            dic="yes",
            original="tipoContrato.id",
            obs=(
                "Discrimina contratos de empenhos: o tipo Empenho responde por cerca de "
                "40% dos registros da tabela"
            ),
        ),
        col(
            "tipo_contrato",
            "STRING",
            "Nome do tipo de contrato",
            "Name of the contract type",
            "Nombre del tipo de contrato",
            original="tipoContrato.nome",
        ),
        col(
            "id_categoria_processo",
            "STRING",
            "Código da categoria do objeto contratado",
            "Code of the category of the contracted object",
            "Código de la categoría del objeto contratado",
            dic="yes",
            original="categoriaProcesso.id",
        ),
        col(
            "categoria_processo",
            "STRING",
            "Nome da categoria do objeto contratado",
            "Name of the category of the contracted object",
            "Nombre de la categoría del objeto contratado",
            original="categoriaProcesso.nome",
        ),
        col(
            "id_fornecedor",
            "STRING",
            "Número de inscrição do fornecedor contratado, CNPJ para pessoa jurídica e CPF para pessoa física",
            "Registry number of the contracted supplier, CNPJ for legal entities and CPF for individuals",
            "Número de inscripción del proveedor contratado, CNPJ para persona jurídica y CPF para persona física",
            original="niFornecedor",
            sensitive="yes",
            obs="Contém CPF quando o fornecedor é pessoa física, conforme publicado pelo PNCP",
        ),
        col(
            "nome_fornecedor",
            "STRING",
            "Nome ou razão social do fornecedor contratado",
            "Name or legal name of the contracted supplier",
            "Nombre o razón social del proveedor contratado",
            original="nomeRazaoSocialFornecedor",
            sensitive="yes",
        ),
        col(
            "tipo_pessoa_fornecedor",
            "STRING",
            "Tipo de pessoa do fornecedor contratado",
            "Person type of the contracted supplier",
            "Tipo de persona del proveedor contratado",
            dic="yes",
            original="tipoPessoa",
            obs="PJ pessoa jurídica, PF pessoa física, PE pessoa estrangeira",
        ),
        col(
            "codigo_pais_fornecedor",
            "STRING",
            "Código do país do fornecedor contratado",
            "Country code of the contracted supplier",
            "Código del país del proveedor contratado",
            original="codigoPaisFornecedor",
        ),
        col(
            "id_fornecedor_subcontratado",
            "STRING",
            "Número de inscrição do fornecedor subcontratado",
            "Registry number of the subcontracted supplier",
            "Número de inscripción del proveedor subcontratado",
            original="niFornecedorSubContratado",
            sensitive="yes",
        ),
        col(
            "nome_fornecedor_subcontratado",
            "STRING",
            "Nome ou razão social do fornecedor subcontratado",
            "Name or legal name of the subcontracted supplier",
            "Nombre o razón social del proveedor subcontratado",
            original="nomeFornecedorSubContratado",
            sensitive="yes",
        ),
        col(
            "objeto_contrato",
            "STRING",
            "Descrição do objeto do contrato",
            "Description of the object of the contract",
            "Descripción del objeto del contrato",
            original="objetoContrato",
        ),
        col(
            "informacao_complementar",
            "STRING",
            "Informação complementar registrada pelo órgão contratante",
            "Additional information recorded by the contracting agency",
            "Información complementaria registrada por el órgano contratante",
            original="informacaoComplementar",
        ),
        col(
            "indicador_receita",
            "BOOLEAN",
            "Indica se o contrato gera receita para a administração em vez de despesa",
            "Whether the contract generates revenue for the administration rather than expenditure",
            "Indica si el contrato genera ingresos para la administración en lugar de gasto",
            original="receita",
        ),
        col(
            "indicador_emenda_parlamentar",
            "BOOLEAN",
            "Indica se o contrato envolve recursos de emenda parlamentar",
            "Whether the contract involves congressional amendment funds",
            "Indica si el contrato involucra recursos de enmienda parlamentaria",
            original="emendaParlamentar",
        ),
        col(
            "indicador_fruto_adesao",
            "BOOLEAN",
            "Indica se o contrato resulta de adesão a ata de registro de preços de outro órgão",
            "Whether the contract results from joining another agency's price registration record",
            "Indica si el contrato resulta de la adhesión al acta de registro de precios de otro órgano",
            original="frutoAdesao",
        ),
        col(
            "numero_retificacao",
            "INT64",
            "Número de retificações aplicadas ao registro do contrato",
            "Number of rectifications applied to the contract record",
            "Número de rectificaciones aplicadas al registro del contrato",
            unit="unit",
            original="numeroRetificacao",
        ),
        col(
            "numero_parcelas",
            "INT64",
            "Número de parcelas previstas para a execução do contrato",
            "Number of instalments planned for contract execution",
            "Número de cuotas previstas para la ejecución del contrato",
            unit="unit",
            original="numeroParcelas",
        ),
        col(
            "data_assinatura",
            "DATE",
            "Data de assinatura do contrato",
            "Date the contract was signed",
            "Fecha de firma del contrato",
            original="dataAssinatura",
        ),
        col(
            "data_vigencia_inicio",
            "DATE",
            "Data de início da vigência do contrato",
            "Start date of the contract term",
            "Fecha de inicio de la vigencia del contrato",
            original="dataVigenciaInicio",
        ),
        col(
            "data_vigencia_fim",
            "DATE",
            "Data de término da vigência do contrato",
            "End date of the contract term",
            "Fecha de término de la vigencia del contrato",
            original="dataVigenciaFim",
        ),
    ],
    ATUALIZACAO_COLS,
    [
        col(
            "valor_inicial",
            "FLOAT64",
            "Valor inicial do contrato",
            "Initial value of the contract",
            "Valor inicial del contrato",
            unit="BRL",
            original="valorInicial",
        ),
        col(
            "valor_parcela",
            "FLOAT64",
            "Valor de cada parcela do contrato",
            "Value of each contract instalment",
            "Valor de cada cuota del contrato",
            unit="BRL",
            original="valorParcela",
        ),
        col(
            "valor_global",
            "FLOAT64",
            "Valor global do contrato",
            "Global value of the contract",
            "Valor global del contrato",
            unit="BRL",
            original="valorGlobal",
        ),
        col(
            "valor_acumulado",
            "FLOAT64",
            "Valor acumulado do contrato após termos aditivos e reajustes",
            "Accumulated contract value after amendments and adjustments",
            "Valor acumulado del contrato tras adiciones y reajustes",
            unit="BRL",
            original="valorAcumulado",
        ),
    ],
)


# --------------------------------------------------------------------------
# ata_registro_preco
# --------------------------------------------------------------------------

ATA = concat(
    [ano_col("dataPublicacaoPncp")],
    [
        col(
            "id_ata_pncp",
            "STRING",
            "Número de controle da ata no PNCP, identificador único do registro",
            "PNCP control number of the price registration record, its unique identifier",
            "Número de control del acta en el PNCP, identificador único del registro",
            original="numeroControlePNCPAta",
        ),
        col(
            "id_contratacao_pncp",
            "STRING",
            "Número de controle no PNCP da contratação que originou a ata",
            "PNCP control number of the tender that originated the record",
            "Número de control en el PNCP de la contratación que originó el acta",
            original="numeroControlePNCPCompra",
            obs="Chave de ligação com a tabela contratacao",
        ),
        col(
            "numero_ata",
            "STRING",
            "Número da ata de registro de preços atribuído pelo órgão",
            "Number of the price registration record assigned by the agency",
            "Número del acta de registro de precios asignado por el órgano",
            original="numeroAtaRegistroPreco",
        ),
        col(
            "ano_ata",
            "INT64",
            "Ano da ata de registro de preços",
            "Year of the price registration record",
            "Año del acta de registro de precios",
            unit="year",
            original="anoAta",
        ),
        col(
            "cnpj_orgao",
            "STRING",
            "CNPJ do órgão ou entidade responsável pela ata",
            "CNPJ of the agency or entity responsible for the record",
            "CNPJ del órgano o entidad responsable del acta",
            original="cnpjOrgao",
        ),
        col(
            "nome_orgao",
            "STRING",
            "Nome do órgão ou entidade responsável pela ata",
            "Name of the agency or entity responsible for the record",
            "Nombre del órgano o entidad responsable del acta",
            original="nomeOrgao",
        ),
        col(
            "codigo_unidade",
            "STRING",
            "Código da unidade administrativa responsável pela ata",
            "Code of the administrative unit responsible for the record",
            "Código de la unidad administrativa responsable del acta",
            original="codigoUnidadeOrgao",
        ),
        col(
            "nome_unidade",
            "STRING",
            "Nome da unidade administrativa responsável pela ata",
            "Name of the administrative unit responsible for the record",
            "Nombre de la unidad administrativa responsable del acta",
            original="nomeUnidadeOrgao",
        ),
        col(
            "cnpj_orgao_subrogado",
            "STRING",
            "CNPJ do órgão sub-rogado responsável pela ata",
            "CNPJ of the subrogated agency responsible for the record",
            "CNPJ del órgano subrogado responsable del acta",
            original="cnpjOrgaoSubrogado",
        ),
        col(
            "nome_orgao_subrogado",
            "STRING",
            "Nome do órgão sub-rogado responsável pela ata",
            "Name of the subrogated agency responsible for the record",
            "Nombre del órgano subrogado responsable del acta",
            original="nomeOrgaoSubrogado",
        ),
        col(
            "codigo_unidade_subrogada",
            "STRING",
            "Código da unidade administrativa sub-rogada",
            "Code of the subrogated administrative unit",
            "Código de la unidad administrativa subrogada",
            original="codigoUnidadeOrgaoSubrogado",
        ),
        col(
            "nome_unidade_subrogada",
            "STRING",
            "Nome da unidade administrativa sub-rogada",
            "Name of the subrogated administrative unit",
            "Nombre de la unidad administrativa subrogada",
            original="nomeUnidadeOrgaoSubrogado",
        ),
        col(
            "objeto_contratacao",
            "STRING",
            "Descrição do objeto da contratação registrada na ata",
            "Description of the object of the tender recorded in the price registration record",
            "Descripción del objeto de la contratación registrada en el acta",
            original="objetoContratacao",
        ),
        col(
            "indicador_cancelado",
            "BOOLEAN",
            "Indica se a ata foi cancelada",
            "Whether the record has been cancelled",
            "Indica si el acta fue cancelada",
            original="cancelado",
        ),
        col(
            "indicador_possibilidade_adesao",
            "BOOLEAN",
            "Indica se a ata admite adesão por outros órgãos",
            "Whether other agencies may join the record",
            "Indica si el acta admite adhesión por otros órganos",
            original="possibilidadeAdesao",
            obs=(
                "PNCP only began populating this field during 2026: sampled "
                "non-null in 0/13 records for 2022-09-08 and 0/500 for "
                "2025-03-11, against 347/500 for 2026-06-10. Expect it empty "
                "for historical years"
            ),
        ),
        col(
            "data_assinatura",
            "DATE",
            "Data de assinatura da ata",
            "Date the record was signed",
            "Fecha de firma del acta",
            original="dataAssinatura",
        ),
        col(
            "data_vigencia_inicio",
            "DATE",
            "Data de início da vigência da ata",
            "Start date of the record's term",
            "Fecha de inicio de la vigencia del acta",
            original="vigenciaInicio",
        ),
        col(
            "data_vigencia_fim",
            "DATE",
            "Data de término da vigência da ata",
            "End date of the record's term",
            "Fecha de término de la vigencia del acta",
            original="vigenciaFim",
        ),
        col(
            "data_cancelamento",
            "DATE",
            "Data de cancelamento da ata",
            "Date the record was cancelled",
            "Fecha de cancelación del acta",
            original="dataCancelamento",
        ),
    ],
    ATUALIZACAO_COLS,
)


# --------------------------------------------------------------------------
# instrumento_cobranca
# --------------------------------------------------------------------------

INSTRUMENTO_COBRANCA = [
    ano_col("dataInclusao", coverage="2023(1)2026"),
    col(
        "cnpj_orgao",
        "STRING",
        "CNPJ do órgão ou entidade ao qual o instrumento de cobrança se refere",
        "CNPJ of the agency or entity the billing instrument refers to",
        "CNPJ del órgano o entidad al que se refiere el instrumento de cobro",
        original="cnpj",
    ),
    col(
        "ano_contrato",
        "INT64",
        "Ano do contrato ao qual o instrumento de cobrança está vinculado",
        "Year of the contract the billing instrument is linked to",
        "Año del contrato al que está vinculado el instrumento de cobro",
        unit="year",
        original="ano",
    ),
    col(
        "sequencial_contrato",
        "STRING",
        "Número sequencial do contrato ao qual o instrumento de cobrança está vinculado",
        "Sequential number of the contract the billing instrument is linked to",
        "Número secuencial del contrato al que está vinculado el instrumento de cobro",
        original="sequencialContrato",
    ),
    col(
        "sequencial_instrumento_cobranca",
        "STRING",
        "Número sequencial do instrumento de cobrança dentro do contrato",
        "Sequential number of the billing instrument within the contract",
        "Número secuencial del instrumento de cobro dentro del contrato",
        original="sequencialInstrumentoCobranca",
    ),
    col(
        "id_contrato_pncp",
        "STRING",
        "Número de controle no PNCP do contrato ao qual o instrumento de cobrança se vincula",
        "PNCP control number of the contract the billing instrument is linked to",
        "Número de control en el PNCP del contrato al que se vincula el instrumento de cobro",
        original="recuperarContratoDTO.numeroControlePNCP",
        obs="Chave de ligação com a tabela contrato",
    ),
    col(
        "id_tipo_instrumento_cobranca",
        "STRING",
        "Código do tipo de instrumento de cobrança",
        "Code of the billing instrument type",
        "Código del tipo de instrumento de cobro",
        dic="yes",
        original="tipoInstrumentoCobranca.id",
    ),
    col(
        "tipo_instrumento_cobranca",
        "STRING",
        "Nome do tipo de instrumento de cobrança",
        "Name of the billing instrument type",
        "Nombre del tipo de instrumento de cobro",
        original="tipoInstrumentoCobranca.nome",
    ),
    col(
        "numero_instrumento_cobranca",
        "STRING",
        "Número do instrumento de cobrança",
        "Number of the billing instrument",
        "Número del instrumento de cobro",
        original="numeroInstrumentoCobranca",
    ),
    col(
        "chave_nfe",
        "STRING",
        "Chave de acesso da nota fiscal eletrônica associada",
        "Access key of the associated electronic invoice",
        "Clave de acceso de la factura electrónica asociada",
        original="chaveNFe",
    ),
    col(
        "numero_nfe",
        "STRING",
        "Número da nota fiscal eletrônica associada",
        "Number of the associated electronic invoice",
        "Número de la factura electrónica asociada",
        original="notaFiscalEletronica.numero",
    ),
    col(
        "serie_nfe",
        "STRING",
        "Série da nota fiscal eletrônica associada",
        "Series of the associated electronic invoice",
        "Serie de la factura electrónica asociada",
        original="notaFiscalEletronica.serie",
    ),
    col(
        "id_emitente_nfe",
        "STRING",
        "Número de inscrição do emitente da nota fiscal eletrônica",
        "Registry number of the issuer of the electronic invoice",
        "Número de inscripción del emisor de la factura electrónica",
        original="notaFiscalEletronica.niEmitente",
        sensitive="yes",
    ),
    col(
        "nome_emitente_nfe",
        "STRING",
        "Nome do emitente da nota fiscal eletrônica",
        "Name of the issuer of the electronic invoice",
        "Nombre del emisor de la factura electrónica",
        original="notaFiscalEletronica.nomeEmitente",
        sensitive="yes",
    ),
    col(
        "nome_municipio_emitente_nfe",
        "STRING",
        "Nome do município do emitente da nota fiscal eletrônica",
        "Name of the municipality of the issuer of the electronic invoice",
        "Nombre del municipio del emisor de la factura electrónica",
        original="notaFiscalEletronica.nomeMunicipioEmitente",
    ),
    col(
        "codigo_orgao_destinatario_nfe",
        "STRING",
        "Código do órgão destinatário da nota fiscal eletrônica",
        "Code of the agency receiving the electronic invoice",
        "Código del órgano destinatario de la factura electrónica",
        original="notaFiscalEletronica.codigoOrgaoDestinatario",
    ),
    col(
        "nome_orgao_destinatario_nfe",
        "STRING",
        "Nome do órgão destinatário da nota fiscal eletrônica",
        "Name of the agency receiving the electronic invoice",
        "Nombre del órgano destinatario de la factura electrónica",
        original="notaFiscalEletronica.nomeOrgaoDestinatario",
    ),
    col(
        "tipo_evento_recente_nfe",
        "STRING",
        "Tipo do evento mais recente registrado na nota fiscal eletrônica",
        "Type of the most recent event recorded on the electronic invoice",
        "Tipo del evento más reciente registrado en la factura electrónica",
        original="notaFiscalEletronica.tipoEventoMaisRecente",
    ),
    col(
        "observacao",
        "STRING",
        "Observação registrada sobre o instrumento de cobrança",
        "Note recorded about the billing instrument",
        "Observación registrada sobre el instrumento de cobro",
        original="observacao",
    ),
    col(
        "data_emissao",
        "DATE",
        "Data de emissão do documento de cobrança",
        "Issue date of the billing document",
        "Fecha de emisión del documento de cobro",
        original="dataEmissaoDocumento",
    ),
    col(
        "data_inclusao",
        "DATE",
        "Data de inclusão do instrumento de cobrança no PNCP",
        "Date the billing instrument was recorded on PNCP",
        "Fecha de inclusión del instrumento de cobro en el PNCP",
        original="dataInclusao",
    ),
    col(
        "data_atualizacao",
        "DATE",
        "Data da última atualização do instrumento de cobrança no PNCP",
        "Date of the last update of the billing instrument on PNCP",
        "Fecha de la última actualización del instrumento de cobro en el PNCP",
        original="dataAtualizacao",
    ),
    col(
        "valor_nota_fiscal",
        "FLOAT64",
        "Valor total da nota fiscal eletrônica associada",
        "Total value of the associated electronic invoice",
        "Valor total de la factura electrónica asociada",
        unit="BRL",
        original="notaFiscalEletronica.valorNotaFiscal",
    ),
]


# --------------------------------------------------------------------------
# plano_contratacao_anual, at item grain
# --------------------------------------------------------------------------

PCA = [
    col(
        "ano",
        "INT64",
        "Ano do plano de contratações anual",
        "Year of the annual procurement plan",
        "Año del plan anual de contrataciones",
        directory=DIR_ANO,
        unit="year",
        obs="Coluna de partição, correspondente ao ano de referência do plano",
        original="anoPca",
        coverage="2023(1)2026",
    ),
    col(
        "id_pca_pncp",
        "STRING",
        "Identificador do plano de contratações anual no PNCP",
        "Identifier of the annual procurement plan on PNCP",
        "Identificador del plan anual de contrataciones en el PNCP",
        original="idPcaPncp",
    ),
    col(
        "numero_item",
        "STRING",
        "Número do item dentro do plano de contratações anual",
        "Number of the item within the annual procurement plan",
        "Número del ítem dentro del plan anual de contrataciones",
        original="itens.numeroItem",
    ),
    col(
        "cnpj_orgao",
        "STRING",
        "CNPJ do órgão ou entidade responsável pelo plano",
        "CNPJ of the agency or entity responsible for the plan",
        "CNPJ del órgano o entidad responsable del plan",
        original="orgaoEntidadeCnpj",
    ),
    col(
        "nome_orgao",
        "STRING",
        "Razão social do órgão ou entidade responsável pelo plano",
        "Legal name of the agency or entity responsible for the plan",
        "Razón social del órgano o entidad responsable del plan",
        original="orgaoEntidadeRazaoSocial",
    ),
    col(
        "codigo_unidade",
        "STRING",
        "Código da unidade administrativa responsável pelo plano",
        "Code of the administrative unit responsible for the plan",
        "Código de la unidad administrativa responsable del plan",
        original="codigoUnidade",
    ),
    col(
        "nome_unidade",
        "STRING",
        "Nome da unidade administrativa responsável pelo plano",
        "Name of the administrative unit responsible for the plan",
        "Nombre de la unidad administrativa responsable del plan",
        original="nomeUnidade",
    ),
    col(
        "unidade_requisitante",
        "STRING",
        "Nome da unidade requisitante do item",
        "Name of the unit requesting the item",
        "Nombre de la unidad solicitante del ítem",
        original="itens.unidadeRequisitante",
    ),
    col(
        "codigo_item",
        "STRING",
        "Código do item no catálogo de materiais e serviços",
        "Code of the item in the materials and services catalogue",
        "Código del ítem en el catálogo de materiales y servicios",
        original="itens.codigoItem",
    ),
    col(
        "descricao_item",
        "STRING",
        "Descrição do item planejado",
        "Description of the planned item",
        "Descripción del ítem planificado",
        original="itens.descricaoItem",
    ),
    col(
        "id_classificacao_catalogo",
        "STRING",
        "Código da classificação do catálogo à qual o item pertence",
        "Code of the catalogue classification the item belongs to",
        "Código de la clasificación del catálogo a la que pertenece el ítem",
        dic="yes",
        original="itens.classificacaoCatalogoId",
    ),
    col(
        "nome_classificacao_catalogo",
        "STRING",
        "Nome da classificação do catálogo à qual o item pertence",
        "Name of the catalogue classification the item belongs to",
        "Nombre de la clasificación del catálogo a la que pertenece el ítem",
        original="itens.nomeClassificacaoCatalogo",
    ),
    col(
        "codigo_classificacao_superior",
        "STRING",
        "Código da classificação superior do item no catálogo",
        "Code of the item's higher-level classification in the catalogue",
        "Código de la clasificación superior del ítem en el catálogo",
        original="itens.classificacaoSuperiorCodigo",
    ),
    col(
        "nome_classificacao_superior",
        "STRING",
        "Nome da classificação superior do item no catálogo",
        "Name of the item's higher-level classification in the catalogue",
        "Nombre de la clasificación superior del ítem en el catálogo",
        original="itens.classificacaoSuperiorNome",
    ),
    col(
        "codigo_pdm",
        "STRING",
        "Código do padrão descritivo de material do item",
        "Code of the item's material description standard",
        "Código del estándar descriptivo de material del ítem",
        original="itens.pdmCodigo",
    ),
    col(
        "descricao_pdm",
        "STRING",
        "Descrição do padrão descritivo de material do item",
        "Description of the item's material description standard",
        "Descripción del estándar descriptivo de material del ítem",
        original="itens.pdmDescricao",
    ),
    col(
        "codigo_grupo_contratacao",
        "STRING",
        "Código do grupo de contratação ao qual o item pertence",
        "Code of the procurement group the item belongs to",
        "Código del grupo de contratación al que pertenece el ítem",
        original="itens.grupoContratacaoCodigo",
    ),
    col(
        "nome_grupo_contratacao",
        "STRING",
        "Nome do grupo de contratação ao qual o item pertence",
        "Name of the procurement group the item belongs to",
        "Nombre del grupo de contratación al que pertenece el ítem",
        original="itens.grupoContratacaoNome",
    ),
    col(
        "categoria_item",
        "STRING",
        "Nome da categoria do item no plano de contratações",
        "Name of the item category in the procurement plan",
        "Nombre de la categoría del ítem en el plan de contrataciones",
        original="itens.categoriaItemPcaNome",
    ),
    col(
        "unidade_fornecimento",
        "STRING",
        "Unidade de fornecimento do item",
        "Supply unit of the item",
        "Unidad de suministro del ítem",
        original="itens.unidadeFornecimento",
    ),
    col(
        "data_desejada",
        "DATE",
        "Data desejada para a contratação do item",
        "Desired date for procuring the item",
        "Fecha deseada para la contratación del ítem",
        original="itens.dataDesejada",
    ),
    col(
        "data_publicacao",
        "DATE",
        "Data de publicação do plano no PNCP",
        "Date the plan was published on PNCP",
        "Fecha de publicación del plan en el PNCP",
        original="dataPublicacaoPNCP",
    ),
    col(
        "data_atualizacao",
        "DATE",
        "Data da última atualização global do plano no PNCP",
        "Date of the last global update of the plan on PNCP",
        "Fecha de la última actualización global del plan en el PNCP",
        original="dataAtualizacaoGlobalPCA",
        obs="Usada para desduplicar múltiplas versões do mesmo plano, mantendo a mais recente",
    ),
    col(
        "quantidade_estimada",
        "FLOAT64",
        "Quantidade estimada do item a ser contratada",
        "Estimated quantity of the item to be procured",
        "Cantidad estimada del ítem a contratar",
        unit="unit",
        original="itens.quantidadeEstimada",
    ),
    col(
        "valor_unitario",
        "FLOAT64",
        "Valor unitário estimado do item",
        "Estimated unit value of the item",
        "Valor unitario estimado del ítem",
        unit="BRL",
        original="itens.valorUnitario",
    ),
    col(
        "valor_total",
        "FLOAT64",
        "Valor total estimado do item",
        "Estimated total value of the item",
        "Valor total estimado del ítem",
        unit="BRL",
        original="itens.valorTotal",
    ),
    col(
        "valor_orcamento_exercicio",
        "FLOAT64",
        "Valor orçado para o item no exercício financeiro",
        "Amount budgeted for the item in the financial year",
        "Valor presupuestado para el ítem en el ejercicio financiero",
        unit="BRL",
        original="itens.valorOrcamentoExercicio",
    ),
]


# --------------------------------------------------------------------------
# dicionario
# --------------------------------------------------------------------------

DICIONARIO = [
    col(
        "id_tabela",
        "STRING",
        "Nome da tabela à qual a chave se refere",
        "Name of the table the key refers to",
        "Nombre de la tabla a la que se refiere la clave",
    ),
    col(
        "nome_coluna",
        "STRING",
        "Nome da coluna à qual a chave se refere",
        "Name of the column the key refers to",
        "Nombre de la columna a la que se refiere la clave",
    ),
    col(
        "chave",
        "STRING",
        "Valor codificado presente na coluna",
        "Coded value present in the column",
        "Valor codificado presente en la columna",
    ),
    col(
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal da chave",
        "Temporal coverage of the key",
        "Cobertura temporal de la clave",
    ),
    col(
        "valor",
        "STRING",
        "Significado da chave codificada",
        "Meaning of the coded key",
        "Significado de la clave codificada",
    ),
]


TABLES = {
    "contratacao": CONTRATACAO,
    "contrato": CONTRATO,
    "ata_registro_preco": ATA,
    "instrumento_cobranca": INSTRUMENTO_COBRANCA,
    "plano_contratacao_anual": PCA,
    "dicionario": DICIONARIO,
}


def main() -> None:
    for table, cols in TABLES.items():
        names = [c["name"] for c in cols]
        duplicates = {n for n in names if names.count(n) > 1}
        if duplicates:
            raise SystemExit(
                f"{table}: duplicate column names {sorted(duplicates)}"
            )
        for c in cols:
            if (
                c["bigquery_type"] in ("INT64", "FLOAT64")
                and not c["measurement_unit"]
            ):
                raise SystemExit(
                    f"{table}.{c['name']}: numeric column without measurement_unit"
                )
            if (
                c["covered_by_dictionary"] == "yes"
                and c["bigquery_type"] != "STRING"
            ):
                raise SystemExit(
                    f"{table}.{c['name']}: dictionary-covered column must be STRING"
                )
            if c["description"].endswith("."):
                raise SystemExit(
                    f"{table}.{c['name']}: description ends with a period"
                )
        path = HERE / f"{table}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=HEADER, lineterminator="\n")
            writer.writeheader()
            writer.writerows(cols)
        print(f"{path.name}: {len(cols)} columns")


if __name__ == "__main__":
    main()
