"""Constantes de br_senado_dados_abertos_administrativos.

Um Enum do conjunto e um por tabela.
"""

from enum import Enum

from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
    PartBdpro,
    YearMonth,
    YearOnly,
)


class constants(Enum):
    """Configuração da API, do agendamento e das regras comuns às tabelas."""

    DATASET_ID = "br_senado_dados_abertos_administrativos"
    ANCHOR_TABLE = "senador"

    API_URL = "https://adm.senado.gov.br/adm-dadosabertos/api/v1"
    LEGIS_API_URL = "https://legis.senado.leg.br/dadosabertos"
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    )
    HEADERS = {"Accept": "application/json", "User-Agent": USER_AGENT}
    FETCH_WORKERS = 8
    FETCH_FAIL_RATIO = 0.01

    DAILY_CRON = "17 6 * * 0,2,3,4,5,6"
    WEEKLY_CRON = "17 6 * * 1"
    REFRESH_PRIOR_YEARS = 1

    FIRST_YEAR_CEAPS = 2008
    FIRST_YEAR_REMUNERACAO = 2013
    FIRST_YEAR_HORA_EXTRA = 2013
    FIRST_YEAR_SUPRIDOS = 2013

    LEGISLATURA_START = 53
    LEGISLATURA_END = 57
    CROSSWALK_OVERRIDES = {"NELSINHO TRAD FILHO": "Nelsinho Trad"}

    CONTRATO_STATUSES = ["VIGENTE", "EM_RENOVACAO", "ENCERRADO"]
    TIPOS_CONTRATACAO = ["contratos", "notas_empenho", "atas_registro_preco"]

    NULL_VALUES = {"", "-", "---", "N/A", "NAO INFORMADO", "NÃO INFORMADO"}
    DATE_ISO_PATTERN = r"^\d{4}-\d{2}-\d{2}"
    DATE_BR_PATTERN = r"^(\d{2})/(\d{2})/(\d{4})$"
    MONTH_YEAR_PATTERN = r"^(\d{2})/(\d{4})$"
    HOURS_PATTERN = r"^(\d+)h(\d{2})$"
    WEEKLY_HOURS_PATTERN = r"^(\d+)"

    SNAPSHOTS = [
        "senador_gabinete",
        "senador_escritorio_apoio",
        "senador_auxilio_moradia",
        "senador_aposentado_pensionista",
        "servidor",
        "servidor_ativo",
        "servidor_aposentado",
        "servidor_exonerado",
        "servidor_cedido",
        "pensionista",
        "contratacao",
        "contratacao_orgao_gestor",
        "contratacao_item",
        "contratacao_garantia",
        "contratacao_pagamento",
        "contratacao_documento_fiscal",
        "contratacao_pagamento_empenho",
        "contrato_aditivo",
        "ata_acionamento",
        "licitacao",
        "licitacao_detalhamento",
        "empresa",
        "terceirizado",
        "menor_aprendiz",
        "estagiario",
        "quadro_pessoal",
        "diretor_coordenador",
        "previsao_aposentadoria",
    ]
    PARTITIONED = [
        "despesa_ceaps",
        "servidor_remuneracao",
        "servidor_hora_extra",
        "servidor_hora_extra_dia",
        "suprido_ato_concessao",
        "suprido_empenho",
        "suprido_transacao",
        "suprido_transacao_objeto",
        "suprido_movimentacao",
        "suprido_movimentacao_subtipo",
    ]
    UNPARTITIONED = ["senador", "dicionario"]
    ALL_TABLES = UNPARTITIONED + SNAPSHOTS + PARTITIONED
    STATIC_TABLES = ["dicionario"]


class constants_senador(Enum):
    """Configuração da tabela senador."""

    TABLE_ID = "senador"
    COVERAGE = None
    COLUMNS = [
        "id_senador",
        "nome_parlamentar",
        "nome_completo",
        "sexo",
        "sigla_uf",
        "sigla_partido",
        "titular_suplente",
        "mandato",
        "data_nascimento",
        "email",
        "indicador_em_exercicio",
    ]


class constants_despesa_ceaps(Enum):
    """Configuração da tabela despesa_ceaps."""

    TABLE_ID = "despesa_ceaps"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "ano",
        "mes",
        "id_despesa",
        "id_senador",
        "tipo_despesa",
        "tipo_documento",
        "documento",
        "data",
        "cpf_cnpj_fornecedor",
        "nome_fornecedor",
        "detalhamento",
        "valor_reembolsado",
    ]


class constants_senador_gabinete(Enum):
    """Configuração da tabela senador_gabinete."""

    TABLE_ID = "senador_gabinete"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "id_senador",
        "endereco",
        "telefones",
        "fax",
        "chefe_gabinete",
    ]


class constants_senador_escritorio_apoio(Enum):
    """Configuração da tabela senador_escritorio_apoio."""

    TABLE_ID = "senador_escritorio_apoio"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "id_senador",
        "nome_escritorio",
        "sigla_setor",
        "endereco",
        "telefone",
    ]


class constants_senador_auxilio_moradia(Enum):
    """Configuração da tabela senador_auxilio_moradia."""

    TABLE_ID = "senador_auxilio_moradia"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "id_senador",
        "indicador_auxilio_moradia",
        "indicador_imovel_funcional",
    ]


class constants_senador_aposentado_pensionista(Enum):
    """Configuração da tabela senador_aposentado_pensionista."""

    TABLE_ID = "senador_aposentado_pensionista"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "nome",
        "tipo_beneficio",
        "regime",
        "tipo",
        "data_inicio",
        "data_fim",
        "valor_remuneracao",
    ]


class constants_servidor(Enum):
    """Configuração da tabela servidor."""

    TABLE_ID = "servidor"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "id_servidor",
        "nome",
        "vinculo",
        "situacao",
        "cargo",
        "especialidade",
        "padrao",
        "codigo_categoria",
        "categoria",
        "codigo_funcao",
        "funcao",
        "sigla_lotacao",
        "lotacao",
        "tipo_cessao",
        "orgao_origem",
        "orgao_destino",
        "ano_admissao",
    ]


class constants_servidor_ativo(Enum):
    """Configuração da tabela servidor_ativo."""

    TABLE_ID = "servidor_ativo"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "nome",
        "tipo_vinculo",
        "cargo",
        "categoria",
        "funcao",
        "data_admissao",
        "jornada_semanal_horas",
        "afastamento",
        "isencao_ponto",
        "sigla_lotacao",
        "lotacao",
        "nivel_lotacao",
        "hierarquia_lotacao",
    ]


class constants_servidor_remuneracao(Enum):
    """Configuração da tabela servidor_remuneracao."""

    TABLE_ID = "servidor_remuneracao"
    COVERAGE = PartBdpro(
        date_column=YearMonth(year="ano", month="mes"),
        date_format=DateFormat.YEAR_MONTH,
    )
    COLUMNS = [
        "ano",
        "mes",
        "id_remuneracao",
        "nome",
        "tipo_folha",
        "remuneracao_basica",
        "vantagens_pessoais",
        "funcao_comissionada",
        "gratificacao_natalina",
        "horas_extras",
        "auxilios",
        "diarias",
        "vantagens_indenizatorias",
        "outras_eventuais",
        "abono_permanencia",
        "faltas",
        "previdencia",
        "imposto_renda",
        "reversao_teto_constitucional",
        "remuneracao_liquida",
    ]


class constants_servidor_hora_extra(Enum):
    """Configuração da tabela servidor_hora_extra."""

    TABLE_ID = "servidor_hora_extra"
    COVERAGE = PartBdpro(
        date_column=YearMonth(year="ano", month="mes"),
        date_format=DateFormat.YEAR_MONTH,
    )
    COLUMNS = [
        "ano",
        "mes",
        "id_hora_extra",
        "nome",
        "mes_ano_prestacao",
        "mes_ano_pagamento",
        "valor_total",
    ]


class constants_servidor_hora_extra_dia(Enum):
    """Configuração da tabela servidor_hora_extra_dia."""

    TABLE_ID = "servidor_hora_extra_dia"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "ano",
        "mes",
        "id_hora_extra",
        "mes_ano_prestacao",
        "data",
        "quantidade_horas",
        "sigla_setor_prestacao",
        "setor_prestacao",
    ]


class constants_servidor_aposentado(Enum):
    """Configuração da tabela servidor_aposentado."""

    TABLE_ID = "servidor_aposentado"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "matricula",
        "tipo_quadro",
        "nome",
        "categoria",
        "cargo",
        "data_aposentadoria",
        "tipo_aposentadoria",
    ]


class constants_servidor_exonerado(Enum):
    """Configuração da tabela servidor_exonerado."""

    TABLE_ID = "servidor_exonerado"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "matricula",
        "nome",
        "forma_vacancia",
        "data_exercicio",
        "data_vacancia",
    ]


class constants_servidor_cedido(Enum):
    """Configuração da tabela servidor_cedido."""

    TABLE_ID = "servidor_cedido"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "matricula",
        "tipo_cessao",
        "nome",
        "orgao_origem",
        "orgao_destino",
        "cargo",
        "categoria",
        "lotacao",
        "data_exercicio",
    ]
    SENADO_FEDERAL = "SENADO FEDERAL"
    CESSOES = [
        (
            "/servidores/cedidos/para-senado",
            "cedido para o senado",
            "in",
            "orgaoOrigem",
        ),
        (
            "/servidores/cedidos/pelo-senado",
            "cedido pelo senado",
            "out",
            "orgao",
        ),
        (
            "/servidores/cedidos/infraero-para-senado",
            "cedido da infraero",
            "in",
            None,
        ),
        (
            "/servidores/exercicio-provisorio",
            "exercicio provisorio",
            "in",
            "orgaoOrigem",
        ),
    ]
    INFRAERO = "INFRAERO"


class constants_pensionista(Enum):
    """Configuração da tabela pensionista."""

    TABLE_ID = "pensionista"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "id_pensionista",
        "nome",
        "vinculo",
        "fundamento",
        "nome_instituidor",
        "codigo_categoria",
        "categoria",
        "cargo",
        "codigo_funcao",
        "funcao",
        "ano_exercicio",
        "data_obito",
        "data_inicio_pensao",
    ]


class constants_contratacao(Enum):
    """Configuração da tabela contratacao."""

    TABLE_ID = "contratacao"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "tipo_contratacao",
        "id_contratacao",
        "numero",
        "numero_formatado",
        "status",
        "sigla_sub_especie",
        "sub_especie",
        "objeto",
        "cpf_cnpj_empresa",
        "nome_empresa",
        "numero_licitacao",
        "modalidade_licitacao",
        "sigla_unidade_gestora",
        "unidade_gestora",
        "fundamentacao_legal",
        "processo_principal",
        "indicador_mao_de_obra",
        "data_assinatura",
        "data_publicacao",
        "data_inicio_vigencia",
        "data_fim_vigencia",
        "data_ultima_atualizacao",
    ]


class constants_contratacao_orgao_gestor(Enum):
    """Configuração da tabela contratacao_orgao_gestor."""

    TABLE_ID = "contratacao_orgao_gestor"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "tipo_contratacao",
        "id_contratacao",
        "sigla_orgao_gestor",
        "orgao_gestor",
        "tipo_gestao",
    ]


class constants_contratacao_item(Enum):
    """Configuração da tabela contratacao_item."""

    TABLE_ID = "contratacao_item"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "tipo_contratacao",
        "id_contratacao",
        "id_item",
        "numero_item",
        "descricao",
        "quantidade",
        "data_atualizacao",
    ]


class constants_contratacao_garantia(Enum):
    """Configuração da tabela contratacao_garantia."""

    TABLE_ID = "contratacao_garantia"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "tipo_contratacao",
        "id_contratacao",
        "id_garantia",
        "tipo",
        "valor",
        "data_vencimento",
    ]


class constants_contratacao_pagamento(Enum):
    """Configuração da tabela contratacao_pagamento."""

    TABLE_ID = "contratacao_pagamento"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "tipo_contratacao",
        "id_contratacao",
        "id_pagamento",
        "descricao_despesa",
        "valor_cobrado",
        "multa",
        "glosa",
        "observacao",
    ]


class constants_contratacao_documento_fiscal(Enum):
    """Configuração da tabela contratacao_documento_fiscal."""

    TABLE_ID = "contratacao_documento_fiscal"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "tipo_contratacao",
        "id_contratacao",
        "id_documento_fiscal",
        "numero",
        "data_emissao",
        "data_vencimento",
    ]


class constants_contratacao_pagamento_empenho(Enum):
    """Configuração da tabela contratacao_pagamento_empenho."""

    TABLE_ID = "contratacao_pagamento_empenho"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "tipo_contratacao",
        "id_contratacao",
        "id_pagamento",
        "id_empenho",
        "natureza_despesa",
        "valor_empenhado",
        "valor_liquidado",
        "saldo",
    ]


class constants_contrato_aditivo(Enum):
    """Configuração da tabela contrato_aditivo."""

    TABLE_ID = "contrato_aditivo"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "id_contratacao",
        "id_aditivo",
        "numero",
        "valor",
        "data_assinatura",
        "data_publicacao",
        "data_atualizacao",
    ]


class constants_ata_acionamento(Enum):
    """Configuração da tabela ata_acionamento."""

    TABLE_ID = "ata_acionamento"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "id_contratacao",
        "id_acionamento",
        "numero",
        "objeto",
    ]


class constants_licitacao(Enum):
    """Configuração da tabela licitacao."""

    TABLE_ID = "licitacao"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "id_licitacao",
        "numero",
        "modalidade",
        "situacao",
        "objeto",
        "data_abertura",
        "indicador_registro_preco",
        "orgao_origem",
        "edital",
    ]


class constants_licitacao_detalhamento(Enum):
    """Configuração da tabela licitacao_detalhamento."""

    TABLE_ID = "licitacao_detalhamento"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "id_licitacao",
        "id_detalhamento",
        "tipo",
        "descricao",
        "data_criacao",
        "link",
    ]


class constants_empresa(Enum):
    """Configuração da tabela empresa."""

    TABLE_ID = "empresa"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "id_empresa",
        "cpf_cnpj",
        "nome",
    ]


class constants_terceirizado(Enum):
    """Configuração da tabela terceirizado."""

    TABLE_ID = "terceirizado"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "cpf",
        "nome",
        "id_contrato",
        "numero_contrato",
        "id_item_contrato",
        "situacao",
        "empresa",
        "sigla_lotacao",
        "lotacao",
    ]


class constants_menor_aprendiz(Enum):
    """Configuração da tabela menor_aprendiz."""

    TABLE_ID = "menor_aprendiz"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "cpf",
        "nome",
        "situacao",
        "fornecedor",
        "sigla_orgao",
        "orgao",
    ]


class constants_estagiario(Enum):
    """Configuração da tabela estagiario."""

    TABLE_ID = "estagiario"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "nome",
        "curso",
        "sigla_orgao",
        "orgao",
    ]


class constants_suprido_ato_concessao(Enum):
    """Configuração da tabela suprido_ato_concessao."""

    TABLE_ID = "suprido_ato_concessao"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "ano",
        "codigo_ato_concessao",
        "codigo_suprido",
        "nome_suprido",
        "codigo_orgao",
        "sigla_orgao",
        "orgao",
        "matricula_solicitante",
        "numero_processo",
        "indicador_regime_especial",
        "data",
        "data_publicacao_basf",
        "prazo_aplicacao",
        "prazo_comprovacao",
        "data_aplicacao",
        "data_comprovacao",
        "valor_total_elementos_despesa",
        "valor_total_empenhos",
        "valor_total_transacoes",
        "valor_total_movimentacoes",
        "data_processamento",
    ]


class constants_suprido_empenho(Enum):
    """Configuração da tabela suprido_empenho."""

    TABLE_ID = "suprido_empenho"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "ano",
        "codigo_ato_concessao",
        "numero",
        "rubrica",
        "descricao",
        "valor_concedido",
        "valor_executado",
        "data",
        "data_processamento",
    ]


class constants_suprido_transacao(Enum):
    """Configuração da tabela suprido_transacao."""

    TABLE_ID = "suprido_transacao"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "ano",
        "id_transacao",
        "codigo_ato_concessao",
        "tipo",
        "numero",
        "tipo_inscricao",
        "inscricao",
        "fornecedor",
        "rubricas",
        "valor",
        "data",
        "data_processamento",
    ]


class constants_suprido_transacao_objeto(Enum):
    """Configuração da tabela suprido_transacao_objeto."""

    TABLE_ID = "suprido_transacao_objeto"
    COVERAGE = AllFree(
        date_column=YearOnly(col="ano"),
        date_format=DateFormat.YEAR,
    )
    COLUMNS = [
        "ano",
        "id_transacao",
        "id_objeto",
        "descricao_objeto",
        "tipo_despesa",
        "subtipo_despesa",
        "rubrica",
        "quantidade",
        "valor_unitario",
        "valor_total",
    ]


class constants_suprido_movimentacao(Enum):
    """Configuração da tabela suprido_movimentacao."""

    TABLE_ID = "suprido_movimentacao"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "ano",
        "id_movimentacao",
        "codigo_ato_concessao",
        "tipo",
        "numero",
        "tipo_inscricao",
        "inscricao",
        "fornecedor",
        "rubricas",
        "valor",
        "data",
        "data_processamento",
    ]


class constants_suprido_movimentacao_subtipo(Enum):
    """Configuração da tabela suprido_movimentacao_subtipo."""

    TABLE_ID = "suprido_movimentacao_subtipo"
    COVERAGE = AllFree(
        date_column=YearOnly(col="ano"),
        date_format=DateFormat.YEAR,
    )
    COLUMNS = [
        "ano",
        "id_movimentacao",
        "id_subtipo",
        "tipo_despesa",
        "subtipo_despesa",
        "rubrica",
        "valor",
    ]


class constants_quadro_pessoal(Enum):
    """Configuração da tabela quadro_pessoal."""

    TABLE_ID = "quadro_pessoal"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "data_referencia",
        "quadro",
        "periodo",
        "categoria",
        "classe",
        "grupo",
        "cargo",
        "nivel",
        "especialidade",
        "referencia",
        "tabela_vencimento",
        "plano_carreira",
        "nivel_escolaridade",
        "padrao_nivel_referencia",
        "quantidade_cargos",
        "quantidade_ocupados",
        "quantidade_vagos",
        "quantidade_subtotal",
        "quantidade_total",
        "quantidade_com_opcao",
        "quantidade_sem_opcao",
        "quantidade_sem_vinculo",
        "quantidade_estaveis",
        "quantidade_nao_estaveis",
        "quantidade_total_ativo",
        "quantidade_aposentados",
        "quantidade_instituidores_pensao",
        "quantidade_beneficiarios_pensao",
        "quantidade_total_inativo",
    ]
    DIMENSIONS = [
        "categoria",
        "classe",
        "grupo",
        "cargo",
        "nivel",
        "especialidade",
        "referencia",
        "tabela_vencimento",
        "plano_carreira",
        "nivel_escolaridade",
        "padrao_nivel_referencia",
    ]
    MEASURES = [
        "quantidade_cargos",
        "quantidade_ocupados",
        "quantidade_vagos",
        "quantidade_subtotal",
        "quantidade_total",
        "quantidade_com_opcao",
        "quantidade_sem_opcao",
        "quantidade_sem_vinculo",
        "quantidade_estaveis",
        "quantidade_nao_estaveis",
        "quantidade_total_ativo",
        "quantidade_aposentados",
        "quantidade_instituidores_pensao",
        "quantidade_beneficiarios_pensao",
        "quantidade_total_inativo",
    ]
    QUADROS = [
        (
            "/gestao/quadro-cargos-efetivos",
            "cargo_efetivo",
            {
                "categoria": "categoria",
                "nivel": "nivel",
                "especialidade": "especialidade",
            },
            {
                "ATUAL": {
                    "quantidade_cargos": "totalCargosHoje",
                    "quantidade_ocupados": "totalOcupadosHoje",
                    "quantidade_vagos": "totalVagosHoje",
                },
                "ANTERIOR": {
                    "quantidade_ocupados": "totalOcupadosAnt",
                    "quantidade_vagos": "totalVagosAnt",
                },
            },
        ),
        (
            "/gestao/quadro-funcoes-comissionadas",
            "funcao_comissionada",
            {"referencia": "referencia"},
            {
                "ATUAL": {
                    "quantidade_total": "totalHoje",
                    "quantidade_ocupados": "ocupadasEBloqueadasHoje",
                    "quantidade_vagos": "livreHoje",
                },
                "ANTERIOR": {"quantidade_total": "totalAnoAnterior"},
            },
        ),
        (
            "/servidores/quadro-servidores-estaveis-e-nao-estaveis",
            "servidor_estavel",
            {"cargo": "cargo"},
            {
                "ATUAL": {
                    "quantidade_estaveis": "qtdEstaveisHoje",
                    "quantidade_nao_estaveis": "qtdNaoEstaveisHoje",
                },
                "ANTERIOR": {
                    "quantidade_estaveis": "qtdEstaveisAnt",
                    "quantidade_nao_estaveis": "qtdNaoEstaveisAnt",
                },
            },
        ),
        (
            "/servidores/quantitativos/pessoal",
            "pessoal",
            {
                "classe": "classe",
                "grupo": "grupo",
                "plano_carreira": "plano_carreira",
                "nivel_escolaridade": "nivel_escolaridade",
                "padrao_nivel_referencia": "padrao_nivel_referencia",
            },
            {
                "ATUAL": {
                    "quantidade_estaveis": "estaveis",
                    "quantidade_nao_estaveis": "nao_estaveis",
                    "quantidade_subtotal": "subtotal",
                    "quantidade_vagos": "vagos",
                    "quantidade_total_ativo": "total_ativo",
                    "quantidade_aposentados": "aposentados",
                    "quantidade_instituidores_pensao": "instituidores_pensao",
                    "quantidade_beneficiarios_pensao": "beneficiarios_pensao",
                    "quantidade_total_inativo": "total_inativo",
                },
            },
        ),
        (
            "/servidores/quantitativos/cargos-funcoes",
            "cargo_funcao",
            {"nivel": "nivel", "tabela_vencimento": "tabVenc"},
            {
                "ATUAL": {
                    "quantidade_subtotal": "subtotal",
                    "quantidade_vagos": "vago",
                    "quantidade_total": "total",
                    "quantidade_com_opcao": "com_opcao",
                    "quantidade_sem_opcao": "sem_opcao",
                    "quantidade_sem_vinculo": "sem_vinculo",
                },
            },
        ),
        (
            "/senadores/quantitativos/senadores",
            "senador",
            {"cargo": "cargo"},
            {
                "ATUAL": {
                    "quantidade_ocupados": "ocupados_ativo",
                    "quantidade_vagos": "vagos_ativo",
                    "quantidade_total_ativo": "total_ativo",
                    "quantidade_aposentados": "aposentados",
                    "quantidade_instituidores_pensao": "instituidores_pensao",
                    "quantidade_beneficiarios_pensao": "beneficiarios_pensao",
                    "quantidade_total_inativo": "total_inativo",
                },
            },
        ),
    ]


class constants_diretor_coordenador(Enum):
    """Configuração da tabela diretor_coordenador."""

    TABLE_ID = "diretor_coordenador"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "sigla_setor",
        "setor",
        "sigla_setor_superior",
        "setor_superior",
        "matricula_titular",
        "nome_titular",
        "cargo_titular",
        "email_titular",
        "referencia_chefia",
        "matricula_substituto",
        "nome_substituto",
        "data_inicio_substituicao",
        "data_fim_substituicao",
        "telefone",
        "endereco",
    ]


class constants_previsao_aposentadoria(Enum):
    """Configuração da tabela previsao_aposentadoria."""

    TABLE_ID = "previsao_aposentadoria"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "data_extracao",
        "cargo",
        "categoria",
        "ano_direito",
        "mes_direito",
        "quantidade",
    ]


class constants_dicionario(Enum):
    """Configuração da tabela dicionario."""

    TABLE_ID = "dicionario"
    COVERAGE = None
    COLUMNS = [
        "id_tabela",
        "nome_coluna",
        "chave",
        "cobertura_temporal",
        "valor",
    ]
    SIM_NAO = {"sim": "Sim", "nao": "Não"}
    STATIC_VALUES = {
        ("senador", "indicador_em_exercicio"): SIM_NAO,
        ("senador_auxilio_moradia", "indicador_auxilio_moradia"): SIM_NAO,
        ("senador_auxilio_moradia", "indicador_imovel_funcional"): SIM_NAO,
        ("contratacao", "indicador_mao_de_obra"): SIM_NAO,
        ("licitacao", "indicador_registro_preco"): SIM_NAO,
        ("suprido_ato_concessao", "indicador_regime_especial"): SIM_NAO,
        ("senador_aposentado_pensionista", "tipo_beneficio"): {
            "aposentadoria": "Aposentadoria de ex-senador",
            "pensao": "Pensão de beneficiário de ex-senador",
        },
        ("senador_aposentado_pensionista", "regime"): {
            "geral": "Regime geral de aposentadoria de ex-senadores",
            "ipc": "Instituto de Previdência dos Congressistas",
            "pssc": "Plano de Seguridade Social dos Congressistas",
        },
        ("servidor_aposentado", "tipo_quadro"): {
            "efetivo": "Quadro de servidores efetivos",
            "comissionado": "Quadro de servidores comissionados",
        },
        ("servidor_cedido", "tipo_cessao"): {
            "cedido para o senado": (
                "Servidor de outro órgão cedido ao Senado Federal"
            ),
            "cedido pelo senado": (
                "Servidor do Senado Federal cedido a outro órgão"
            ),
            "cedido da infraero": (
                "Servidor da Infraero cedido ao Senado Federal"
            ),
            "exercicio provisorio": (
                "Servidor em exercício provisório no Senado Federal"
            ),
        },
        ("quadro_pessoal", "quadro"): {
            "cargo_efetivo": "Quadro de cargos efetivos",
            "funcao_comissionada": "Quadro de funções comissionadas",
            "servidor_estavel": "Quadro de servidores estáveis e não estáveis",
            "pessoal": "Quantitativo de pessoal",
            "cargo_funcao": "Quantitativo de cargos e funções",
            "senador": "Quantitativo de senadores",
        },
        ("quadro_pessoal", "periodo"): {
            "ATUAL": "Quantitativo na data de extração",
            "ANTERIOR": "Quantitativo no período anterior publicado pela fonte",
        },
    }
    TIPO_CONTRATACAO_VALUES = {
        "contratos": "Contrato",
        "notas_empenho": "Nota de empenho",
        "atas_registro_preco": "Ata de registro de preço",
    }
    TIPO_CONTRATACAO_TABLES = [
        "contratacao",
        "contratacao_orgao_gestor",
        "contratacao_item",
        "contratacao_garantia",
        "contratacao_pagamento",
        "contratacao_documento_fiscal",
        "contratacao_pagamento_empenho",
    ]
    RUBRICA_SOURCE_TABLE = "suprido_empenho"
    RUBRICA_TABLES = [
        "suprido_empenho",
        "suprido_transacao_objeto",
        "suprido_movimentacao_subtipo",
    ]


TABLES_CONSTANTS = {
    "senador": constants_senador,
    "despesa_ceaps": constants_despesa_ceaps,
    "senador_gabinete": constants_senador_gabinete,
    "senador_escritorio_apoio": constants_senador_escritorio_apoio,
    "senador_auxilio_moradia": constants_senador_auxilio_moradia,
    "senador_aposentado_pensionista": constants_senador_aposentado_pensionista,
    "servidor": constants_servidor,
    "servidor_ativo": constants_servidor_ativo,
    "servidor_remuneracao": constants_servidor_remuneracao,
    "servidor_hora_extra": constants_servidor_hora_extra,
    "servidor_hora_extra_dia": constants_servidor_hora_extra_dia,
    "servidor_aposentado": constants_servidor_aposentado,
    "servidor_exonerado": constants_servidor_exonerado,
    "servidor_cedido": constants_servidor_cedido,
    "pensionista": constants_pensionista,
    "contratacao": constants_contratacao,
    "contratacao_orgao_gestor": constants_contratacao_orgao_gestor,
    "contratacao_item": constants_contratacao_item,
    "contratacao_garantia": constants_contratacao_garantia,
    "contratacao_pagamento": constants_contratacao_pagamento,
    "contratacao_documento_fiscal": constants_contratacao_documento_fiscal,
    "contratacao_pagamento_empenho": constants_contratacao_pagamento_empenho,
    "contrato_aditivo": constants_contrato_aditivo,
    "ata_acionamento": constants_ata_acionamento,
    "licitacao": constants_licitacao,
    "licitacao_detalhamento": constants_licitacao_detalhamento,
    "empresa": constants_empresa,
    "terceirizado": constants_terceirizado,
    "menor_aprendiz": constants_menor_aprendiz,
    "estagiario": constants_estagiario,
    "suprido_ato_concessao": constants_suprido_ato_concessao,
    "suprido_empenho": constants_suprido_empenho,
    "suprido_transacao": constants_suprido_transacao,
    "suprido_transacao_objeto": constants_suprido_transacao_objeto,
    "suprido_movimentacao": constants_suprido_movimentacao,
    "suprido_movimentacao_subtipo": constants_suprido_movimentacao_subtipo,
    "quadro_pessoal": constants_quadro_pessoal,
    "diretor_coordenador": constants_diretor_coordenador,
    "previsao_aposentadoria": constants_previsao_aposentadoria,
    "dicionario": constants_dicionario,
}
