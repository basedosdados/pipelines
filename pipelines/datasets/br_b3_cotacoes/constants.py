# -*- coding: utf-8 -*-
from enum import Enum
from datetime import datetime, timedelta


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_b3_cotacoes project
    """

    B3_PATH_INPUT_TXT = "/tmp/input/br_b3_cotacoes/{}_NEGOCIOSAVISTA.txt"
    B3_URL = "https://arquivos.b3.com.br/apinegocios/tickercsv/{}"
    B3_PATH_INPUT = "/tmp/input/br_b3_cotacoes"
    B3_PATH_OUTPUT = "/tmp/output/br_b3_cotacoes"

    RENAME = rename = {
        "DataReferencia": "data_referencia",
        "CodigoInstrumento": "codigo_instrumento",
        "AcaoAtualizacao": "acao_atualizacao",
        "PrecoNegocio": "preco_negocio",
        "QuantidadeNegociada": "quantidade_negociada",
        "HoraFechamento": "hora_fechamento",
        "CodigoIdentificadorNegocio": "codigo_identificador_negocio",
        "TipoSessaoPregao": "tipo_sessao_pregao",
        "DataNegocio": "data_negocio",
        "CodigoParticipanteComprador": "codigo_participante_comprador",
        "CodigoParticipanteVendedor": "codigo_participante_vendedor",
    }

    ORDEM = [
        "data_referencia",
        "tipo_sessao_pregao",
        "codigo_instrumento",
        "acao_atualizacao",
        "data_negocio",
        "codigo_identificador_negocio",
        "preco_negocio",
        "quantidade_negociada",
        "hora_fechamento",
        "codigo_participante_comprador",
        "codigo_participante_vendedor",
    ]
