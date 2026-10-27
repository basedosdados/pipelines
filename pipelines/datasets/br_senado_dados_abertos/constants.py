"""Constantes de br_senado_dados_abertos: um Enum do conjunto e um por tabela."""

from datetime import date
from enum import Enum

from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    NonHistorical,
    PartBdpro,
)


class constants(Enum):
    """Configuração da API e das regras comuns a todas as tabelas."""

    DATASET_ID = "br_senado_dados_abertos"

    API_URL = "https://legis.senado.leg.br/dadosabertos"
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    )
    JSON_HEADERS = {"Accept": "application/json", "User-Agent": USER_AGENT}
    XML_HEADERS = {"Accept": "application/xml", "User-Agent": USER_AGENT}
    FETCH_WORKERS = 8

    REFRESH_PRIOR_YEARS = 1
    NO_NEW_DATA_DATE = date(1900, 1, 1)

    CURRENT_LEG_REFERENCE = 57
    CURRENT_LEG_REFERENCE_YEAR = 2023
    LEG_START = 30

    DIMS = [
        "senador",
        "partido",
        "bloco",
        "lideranca",
        "comissao",
        "mesa",
        "senador_mandato",
        "senador_filiacao",
        "senador_comissao",
        "senador_cargo",
    ]
    PARTITIONED = [
        "votacao",
        "votacao_parlamentar",
        "votacao_orientacao_bancada",
        "processo",
        "relatoria",
        "votacao_comissao",
        "votacao_comissao_parlamentar",
        "discurso",
    ]
    ALL_TABLES = DIMS + PARTITIONED

    VOTO_MAP = {
        "Sim": "voto_sim",
        "Não": "voto_nao",
        "Nao": "voto_nao",
        "Abstenção": "voto_abstencao",
    }
    IDENTIFICACAO_PATTERN = r"^\s*(\S+)\s+([\w.]+)/(\d{4})"


class constants_senador(Enum):
    """Configuração da tabela senador."""

    TABLE_ID = "senador"
    COVERAGE = NonHistorical()
    COLUMNS = [
        "id_senador",
        "nome",
        "nome_completo",
        "sexo",
        "forma_tratamento",
        "sigla_partido",
        "sigla_uf",
        "email",
        "url_foto",
        "url_pagina",
        "url_pagina_particular",
        "id_publico_legislatura_atual",
    ]


class constants_partido(Enum):
    """Configuração da tabela partido."""

    TABLE_ID = "partido"
    COVERAGE = NonHistorical()
    COLUMNS = [
        "id_partido",
        "sigla_partido",
        "nome_partido",
        "data_criacao",
        "data_extincao",
    ]


class constants_bloco(Enum):
    """Configuração da tabela bloco."""

    TABLE_ID = "bloco"
    COVERAGE = NonHistorical()
    COLUMNS = [
        "id_bloco",
        "nome_bloco",
        "nome_apelido",
        "data_criacao",
        "data_extincao",
    ]


class constants_lideranca(Enum):
    """Configuração da tabela lideranca."""

    TABLE_ID = "lideranca"
    COVERAGE = NonHistorical()
    COLUMNS = [
        "id_lideranca",
        "casa",
        "id_senador",
        "nome_parlamentar",
        "sigla_tipo_lideranca",
        "descricao_tipo_lideranca",
        "sigla_tipo_unidade_lideranca",
        "descricao_tipo_unidade_lideranca",
        "id_partido_filiacao",
        "sigla_partido_filiacao",
        "nome_partido_filiacao",
        "id_bloco",
        "sigla_bloco",
        "nome_bloco",
        "numero_ordem_vice_lider",
        "data_designacao",
    ]


class constants_comissao(Enum):
    """Configuração da tabela comissao."""

    TABLE_ID = "comissao"
    COVERAGE = NonHistorical()
    COLUMNS = [
        "id_comissao",
        "sigla_comissao",
        "nome_comissao",
        "sigla_casa",
        "id_tipo_colegiado",
        "sigla_tipo_colegiado",
        "descricao_tipo_colegiado",
        "publica",
        "data_inicio",
    ]


class constants_mesa(Enum):
    """Configuração da tabela mesa."""

    TABLE_ID = "mesa"
    COVERAGE = NonHistorical()
    COLUMNS = [
        "id_colegiado",
        "sigla_colegiado",
        "nome_colegiado",
        "cargo",
        "id_senador",
        "nome_parlamentar",
        "bancada",
        "ordem",
        "origem",
    ]


class constants_senador_mandato(Enum):
    """Configuração da tabela senador_mandato."""

    TABLE_ID = "senador_mandato"
    COVERAGE = NonHistorical()
    COLUMNS = [
        "id_senador",
        "id_mandato",
        "sigla_uf",
        "participacao",
        "numero_legislatura_1",
        "data_inicio_legislatura_1",
        "data_fim_legislatura_1",
        "numero_legislatura_2",
        "data_inicio_legislatura_2",
        "data_fim_legislatura_2",
    ]


class constants_senador_filiacao(Enum):
    """Configuração da tabela senador_filiacao."""

    TABLE_ID = "senador_filiacao"
    COVERAGE = NonHistorical()
    COLUMNS = [
        "id_senador",
        "id_partido",
        "sigla_partido",
        "data_filiacao",
        "data_desfiliacao",
    ]


class constants_senador_comissao(Enum):
    """Configuração da tabela senador_comissao."""

    TABLE_ID = "senador_comissao"
    COVERAGE = NonHistorical()
    COLUMNS = [
        "id_senador",
        "id_comissao",
        "sigla_comissao",
        "sigla_casa",
        "participacao",
        "data_inicio",
        "data_fim",
    ]


class constants_senador_cargo(Enum):
    """Configuração da tabela senador_cargo."""

    TABLE_ID = "senador_cargo"
    COVERAGE = NonHistorical()
    COLUMNS = [
        "id_senador",
        "id_comissao",
        "sigla_comissao",
        "id_cargo",
        "descricao_cargo",
        "data_inicio",
        "data_fim",
    ]


class constants_votacao(Enum):
    """Configuração da tabela votacao."""

    TABLE_ID = "votacao"
    START_YEAR = 1991
    DATE_COLUMN = "data_sessao"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_sessao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "ano",
        "id_votacao",
        "data_sessao",
        "casa",
        "id_processo",
        "codigo_materia",
        "identificacao_materia",
        "sigla_materia",
        "numero_materia",
        "ano_materia",
        "id_sessao",
        "id_sessao_legislativa",
        "numero_sessao",
        "sequencial_sessao",
        "sequencial_votacao",
        "sigla_tipo_sessao",
        "descricao_votacao",
        "ementa",
        "resultado_votacao",
        "votacao_secreta",
        "data_apresentacao",
        "sigla_colegiado",
        "nome_colegiado",
        "voto_sim",
        "voto_nao",
        "voto_abstencao",
        "voto_outro",
    ]


class constants_votacao_parlamentar(Enum):
    """Configuração da tabela votacao_parlamentar."""

    TABLE_ID = "votacao_parlamentar"
    DATE_COLUMN = "data_sessao"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_sessao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "ano",
        "id_votacao",
        "data_sessao",
        "id_senador",
        "sigla_partido",
        "sigla_uf",
        "voto",
        "descricao_voto",
    ]


class constants_votacao_orientacao_bancada(Enum):
    """Configuração da tabela votacao_orientacao_bancada."""

    TABLE_ID = "votacao_orientacao_bancada"
    DATE_COLUMN = "data_votacao"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_votacao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "ano",
        "id_votacao_sve",
        "sequencial_votacao",
        "data_votacao",
        "sigla_materia",
        "numero_materia",
        "ano_materia",
        "bancada",
        "orientacao",
        "data_hora_orientacao",
    ]


class constants_processo(Enum):
    """Configuração da tabela processo."""

    TABLE_ID = "processo"
    START_YEAR = 1900
    DATE_COLUMN = "data_apresentacao"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_apresentacao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "ano",
        "id_processo",
        "codigo_materia",
        "identificacao",
        "sigla",
        "numero",
        "autoria",
        "ementa",
        "objetivo",
        "tipo_documento",
        "tipo_conteudo",
        "situacao_atual",
        "sigla_tipo_deliberacao",
        "ente_identificador",
        "casa_identificadora",
        "norma_gerada",
        "apelido",
        "tramitando",
        "data_apresentacao",
        "data_deliberacao",
        "data_situacao_atual",
        "data_ultima_atualizacao",
        "ultima_informacao_atualizada",
        "url_documento",
    ]


class constants_relatoria(Enum):
    """Configuração da tabela relatoria."""

    TABLE_ID = "relatoria"
    START_YEAR = 2015
    DATE_COLUMN = "data_designacao"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_designacao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "ano",
        "id_relatoria",
        "id_processo",
        "codigo_materia",
        "identificacao_processo",
        "id_senador",
        "sigla_casa_relator",
        "id_colegiado",
        "sigla_colegiado",
        "nome_colegiado",
        "id_tipo_colegiado",
        "tipo_relator",
        "id_tipo_relator",
        "numero_autuacao",
        "tipo_encerramento",
        "data_designacao",
        "data_destituicao",
        "tramitando",
    ]


class constants_votacao_comissao(Enum):
    """Configuração da tabela votacao_comissao."""

    TABLE_ID = "votacao_comissao"
    DATE_COLUMN = "data_reuniao"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_reuniao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "ano",
        "id_votacao",
        "id_comissao",
        "sigla_colegiado",
        "nome_colegiado",
        "sigla_casa_colegiado",
        "id_reuniao",
        "numero_reuniao",
        "tipo_reuniao",
        "data_reuniao",
        "identificacao_materia",
        "sigla_materia",
        "numero_materia",
        "ano_materia",
        "descricao",
        "id_senador_presidente",
        "voto_sim",
        "voto_nao",
        "voto_abstencao",
    ]


class constants_votacao_comissao_parlamentar(Enum):
    """Configuração da tabela votacao_comissao_parlamentar."""

    TABLE_ID = "votacao_comissao_parlamentar"
    DATE_COLUMN = "data_reuniao"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_reuniao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "ano",
        "id_votacao",
        "data_reuniao",
        "id_senador",
        "sigla_partido",
        "sigla_casa",
        "voto",
        "voto_presidente",
    ]


class constants_discurso(Enum):
    """Configuração da tabela discurso."""

    TABLE_ID = "discurso"
    START_YEAR = 1997
    DATE_COLUMN = "data_sessao"
    COVERAGE = PartBdpro(
        date_column=DateOnly(col="data_sessao"),
        date_format=DateFormat.YEAR_MD,
    )
    COLUMNS = [
        "ano",
        "id_pronunciamento",
        "id_sessao",
        "data_sessao",
        "sigla_casa",
        "sigla_tipo_sessao",
        "numero_sessao",
        "id_senador",
        "tipo_autor",
        "sigla_partido",
        "sigla_uf",
        "sigla_tipo_uso_palavra",
        "descricao_tipo_uso_palavra",
        "resumo",
        "indexacao",
        "url_texto",
    ]


DIMS_CONSTANTS = {
    "senador": constants_senador,
    "partido": constants_partido,
    "bloco": constants_bloco,
    "lideranca": constants_lideranca,
    "comissao": constants_comissao,
    "mesa": constants_mesa,
    "senador_mandato": constants_senador_mandato,
    "senador_filiacao": constants_senador_filiacao,
    "senador_comissao": constants_senador_comissao,
    "senador_cargo": constants_senador_cargo,
}
PARTITIONED_CONSTANTS = {
    "votacao": constants_votacao,
    "votacao_parlamentar": constants_votacao_parlamentar,
    "votacao_orientacao_bancada": constants_votacao_orientacao_bancada,
    "processo": constants_processo,
    "relatoria": constants_relatoria,
    "votacao_comissao": constants_votacao_comissao,
    "votacao_comissao_parlamentar": constants_votacao_comissao_parlamentar,
    "discurso": constants_discurso,
}
TABLES_CONSTANTS = DIMS_CONSTANTS | PARTITIONED_CONSTANTS
