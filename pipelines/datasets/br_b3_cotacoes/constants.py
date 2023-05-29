# -*- coding: utf-8 -*-
from enum import Enum
from datetime import datetime, timedelta


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_b3_cotacoes project
    """

    # data = datetime.now().strftime("%d-%m-%Y")
    # data_nova = datetime.now().strf("%Y-%m-%d")
    B3_URL = "https://arquivos.b3.com.br/apinegocios/tickercsv/2023-05-26"
    B3_PATH_INPUT = "/tmp/input/br_b3_cotacoes"
    B3_PATH_OUTPUT_DF = "/tmp/input/br_b3_cotacoes/26-05-2023_NEGOCIOSAVISTA.txt"
    B3_PATH_OUTPUT = "/tmp/output/br_b3_cotacoes"
