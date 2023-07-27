# -*- coding: utf-8 -*- dataset
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