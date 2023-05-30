# -*- coding: utf-8 -*-
from enum import Enum
from datetime import datetime, timedelta


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_b3_cotacoes project
    """

    #data = datetime.now().strftime("%d-%m-%Y")
    #ontem = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")
    #ontem_url = datetime.strptime(ontem, "%d-%m-%Y").strftime("%Y-%m-%d")
    B3_URL = "https://arquivos.b3.com.br/apinegocios/tickercsv/2023-05-29"
    B3_PATH_INPUT = "/tmp/input/br_b3_cotacoes"
    B3_PATH_OUTPUT_DF = "/tmp/input/br_b3_cotacoes/29-05-2023_NEGOCIOSAVISTA.txt"
    B3_PATH_OUTPUT = "/tmp/output/br_b3_cotacoes"
